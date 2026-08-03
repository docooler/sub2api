#!/usr/bin/env python3
"""
kiro_model_sync — 半自动同步 Kiro 可用模型到 sub2api 账号的 model_mapping。

背景
====
native sub2api 走 runtime.*.kiro.dev 端点，该端点不提供 ListAvailableModels，
因此无法"列出"模型。本脚本改用 **探测(probe)** 方式：对一批候选 modelId 各发
一个极小的 generateAssistantResponse 请求，能正常出流(200)的即视为可用，返回
INVALID_MODEL_ID 的即不可用。把新发现的可用模型写进各 kiro 账号的
credentials.model_mapping，并投递 scheduler_outbox 事件触发调度器重建。

- /v1/models 和调度器的账号可用性都从 model_mapping 的 key 生成，所以写进去即生效。
- 探测只读取响应前 256 字节即断开，尽量少生成、少耗额度。
- 同一 profileArn 下模型可用性一致，故按 internal_id 缓存探测结果，跨账号复用。
- 已在某账号 mapping 里的模型视为已知可用，不再重复探测。
- 明确判定为 invalid 的候选写入状态文件，默认 7 天内不再重复探测（省额度）。

用法
====
  python3 kiro_model_sync.py --dry-run      # 只探测并打印将要写入的改动
  python3 kiro_model_sync.py                # 探测并写入 DB + 触发调度器
  python3 kiro_model_sync.py --recheck-invalid-after 0   # 强制重探所有 invalid 候选

依赖：仅标准库 + 宿主机 docker/psql（通过 docker exec 访问 sub2api-postgres）。
"""

import argparse
import datetime
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

# ---- 环境相关默认值（按需用命令行覆盖）----------------------------------------
PG_CONTAINER = "sub2api-postgres"
PG_USER = "sub2api"
PG_DB = "sub2api"
REDIS_CONTAINER = "sub2api-redis"

KIRO_IDE_VERSION = "0.7.45"
FINGERPRINT = "modelsync"
PROBE_CONV_ID = "00000000-0000-4000-8000-00000000ms01"
PROBE_TIMEOUT = 30
PROBE_DELAY = 0.3  # 每次探测间隔，避免限流
STATE_FILE = __file__.rsplit("/", 1)[0] + "/kiro_model_sync.state.json"

# ---- 候选模型生成 --------------------------------------------------------------
# Claude 命名约定：
#   internal(点号) claude-{family}-{major}[.{minor}]   例: claude-sonnet-4.5 / claude-sonnet-5
#   exposed(横线)  把点换成横线                          例: claude-sonnet-4-5 / claude-sonnet-5
CLAUDE_FAMILIES = ["sonnet", "opus", "haiku"]
CLAUDE_MAJORS = [4, 5, 6]
CLAUDE_MINORS = [None, 1, 2, 3, 4, 5, 6, 7, 8, 9]

# OpenAI GPT 命名约定：gpt-{major}.{minor}-{tier}，tier 用形象代号(sol/terra/luna)，
# 无 thinking 变体。internal(点号) 与 exposed 同名，另加全横线别名。
GPT_VERSIONS = ["5.4", "5.6", "5.7", "6.0"]
GPT_TIERS = ["sol", "terra", "luna"]

# 开源模型命名不规则(如 deepseek 前缀 v)，无法从版本号推导，手工维护候选。
# 形如 exposed -> internal
OPEN_MODELS = {
    "deepseek-v3.2": "deepseek-3.2",
    "deepseek-v3.3": "deepseek-3.3",
    "glm-5": "glm-5",
    "glm-6": "glm-6",
    "minimax-m2.1": "minimax-m2.1",
    "minimax-m2.5": "minimax-m2.5",
    "minimax-m3": "minimax-m3",
    "qwen3-coder-next": "qwen3-coder-next",
}


def build_candidates():
    """返回 {exposed_id: internal_id}，覆盖 Claude 各家族版本 + 开源模型。"""
    out = {}
    for fam in CLAUDE_FAMILIES:
        for major in CLAUDE_MAJORS:
            for minor in CLAUDE_MINORS:
                internal = (
                    f"claude-{fam}-{major}"
                    if minor is None
                    else f"claude-{fam}-{major}.{minor}"
                )
                exposed = internal.replace(".", "-")
                out[exposed] = internal
    # OpenAI GPT tiers: internal 用点号，exposed 同名，另加全横线别名。
    for ver in GPT_VERSIONS:
        for tier in GPT_TIERS:
            internal = f"gpt-{ver}-{tier}"
            out[internal] = internal
            out[internal.replace(".", "-")] = internal
    out.update(OPEN_MODELS)
    return out


def thinking_variant(exposed, internal):
    """Claude 模型追加 -thinking 别名，沿用现有 mapping 约定。"""
    if exposed.startswith("claude-"):
        return f"{exposed}-thinking", internal
    return None


# ---- postgres 访问（经 docker exec psql）---------------------------------------
def psql(sql, args):
    cmd = [
        "docker", "exec", "-i", args.pg_container,
        "psql", "-U", args.pg_user, "-d", args.pg_db,
        "-v", "ON_ERROR_STOP=1", "-t", "-A", "-F", "\t",
    ]
    r = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"psql 失败: {r.stderr.strip()}")
    return r.stdout


def load_accounts(args):
    sql = """
    select a.id,
           a.credentials->>'access_token',
           a.credentials->>'profile_arn',
           coalesce(a.credentials->>'region','us-east-1'),
           coalesce(a.credentials->'model_mapping','{}'::jsonb)::text,
           coalesce((select json_agg(ag.group_id order by ag.group_id)
                     from account_groups ag where ag.account_id=a.id),'[]')::text,
           coalesce(a.credentials->>'refresh_token',''),
           coalesce(a.credentials->>'client_id',''),
           coalesce(a.credentials->>'client_secret',''),
           coalesce(nullif(a.credentials->>'sso_region',''),
                    coalesce(a.credentials->>'region','us-east-1')),
           coalesce(a.credentials->>'expires_at','')
    from accounts a
    where a.platform='kiro' and a.status='active'
    order by a.id;
    """
    rows = []
    for line in psql(sql, args).splitlines():
        if not line.strip():
            continue
        (acc_id, token, arn, region, mapping, groups,
         refresh_tok, client_id, client_secret, sso_region, expires_at) = line.split("\t")
        rows.append({
            "id": int(acc_id),
            "token": token,
            "arn": arn,
            "region": region,
            "mapping": json.loads(mapping),
            "group_ids": json.loads(groups),
            "refresh_token": refresh_tok,
            "client_id": client_id,
            "client_secret": client_secret,
            "sso_region": sso_region,
            "expires_at": expires_at,
        })
    return rows


# ---- token 刷新（复刻 backend/internal/pkg/kiro/auth.go 的两条刷新路径）--------
REFRESH_LOCK_TTL_MS = 60_000


def _redis_cli(*cmd):
    r = subprocess.run(["docker", "exec", REDIS_CONTAINER, "redis-cli", *cmd],
                       capture_output=True, text=True)
    return r.stdout.strip()


def _token_expired(acc, margin=300):
    """expires_at 缺失或 margin 秒内到期即视为需要刷新。"""
    if not acc["expires_at"]:
        return True
    try:
        exp = datetime.datetime.fromisoformat(acc["expires_at"].replace("Z", "+00:00"))
    except ValueError:
        return True
    now = datetime.datetime.now(datetime.timezone.utc)
    return (exp - now).total_seconds() < margin


def _http_refresh(acc):
    """执行一次 HTTP 刷新，返回 (access_token, refresh_token或'', expires_at_rfc3339)。"""
    if acc["client_id"] and acc["client_secret"]:
        # AWS SSO OIDC（camelCase JSON）
        url = f"https://oidc.{acc['sso_region']}.amazonaws.com/token"
        body = {"grantType": "refresh_token", "clientId": acc["client_id"],
                "clientSecret": acc["client_secret"], "refreshToken": acc["refresh_token"]}
    else:
        # Kiro Desktop
        url = f"https://prod.{acc['region']}.auth.desktop.kiro.dev/refreshToken"
        body = {"refreshToken": acc["refresh_token"]}
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.load(r)
    access = data.get("accessToken", "")
    if not access:
        raise RuntimeError(f"刷新响应缺少 accessToken: {json.dumps(data)[:200]}")
    expires_in = data.get("expiresIn") or 3600
    exp = (datetime.datetime.now(datetime.timezone.utc)
           + datetime.timedelta(seconds=expires_in - 60))
    return access, data.get("refreshToken", ""), exp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _persist_tokens(acc, access, new_refresh, expires_at, args):
    """与网关 OnRefresh 相同的写回：access/refresh/expires_at/_token_version。"""
    patch = {"access_token": access, "expires_at": expires_at,
             "_token_version": int(time.time() * 1000)}
    if new_refresh:
        patch["refresh_token"] = new_refresh
    patch_json = json.dumps(patch).replace("'", "''")
    psql(f"""
    UPDATE accounts
    SET credentials = credentials || '{patch_json}'::jsonb
    WHERE id = {acc['id']};
    """, args)


def ensure_fresh_token(acc, args, force=False):
    """探测前保证 access_token 可用；过期则加分布式锁刷新并写回 DB。

    锁 key 与网关一致（oauth:refresh_lock:kiro:account:<id>），拿不到锁说明网关
    正在刷新，等待后从 DB 重读即可。刷新出的 refreshToken（若轮换）必须写回，
    否则会使网关侧的旧 refresh token 失效。
    """
    if not force and not _token_expired(acc):
        return
    if not acc["refresh_token"]:
        print(f"[warn] 账号 {acc['id']} token 已过期且无 refresh_token，探测可能 403", file=sys.stderr)
        return
    lock_key = f"oauth:refresh_lock:kiro:account:{acc['id']}"
    got = _redis_cli("SET", lock_key, "modelsync", "NX", "PX", str(REFRESH_LOCK_TTL_MS))
    if got != "OK":
        print(f"账号 {acc['id']} 刷新锁被占用，等待 5s 后从 DB 重读 token")
        time.sleep(5)
    else:
        try:
            access, new_refresh, exp = _http_refresh(acc)
            _persist_tokens(acc, access, new_refresh, exp, args)
            acc.update(token=access, expires_at=exp)
            if new_refresh:
                acc["refresh_token"] = new_refresh
            print(f"账号 {acc['id']} access_token 已刷新（有效期至 {exp}）")
            return
        finally:
            _redis_cli("DEL", lock_key)
    # 锁被别人持有：采用对方写回的最新凭据
    fresh = [a for a in load_accounts(args) if a["id"] == acc["id"]]
    if fresh:
        acc.update(token=fresh[0]["token"], expires_at=fresh[0]["expires_at"],
                   refresh_token=fresh[0]["refresh_token"])


def apply_changes(acc, new_pairs, args):
    """把 new_pairs 合并进账号 mapping，并投递 account_changed 事件。"""
    add_json = json.dumps(new_pairs).replace("'", "''")
    groups_json = json.dumps({"group_ids": acc["group_ids"]}).replace("'", "''")
    sql = f"""
    UPDATE accounts
    SET credentials = jsonb_set(
          credentials, '{{model_mapping}}',
          coalesce(credentials->'model_mapping','{{}}'::jsonb) || '{add_json}'::jsonb)
    WHERE id = {acc['id']};
    INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)
    VALUES ('account_changed', {acc['id']}, NULL, '{groups_json}'::jsonb);
    """
    psql(sql, args)


# ---- 探测 ----------------------------------------------------------------------
def probe(internal_id, token, arn, region):
    """返回 'valid' / 'invalid' / 'error:...'。只读前 256 字节即断开。"""
    url = f"https://runtime.{region}.kiro.dev/generateAssistantResponse"
    payload = {
        "profileArn": arn,
        "conversationState": {
            "chatTriggerType": "MANUAL",
            "conversationId": PROBE_CONV_ID,
            "currentMessage": {
                "userInputMessage": {
                    "content": "hi",
                    "modelId": internal_id,
                    "origin": "AI_EDITOR",
                }
            },
        },
    }
    ua = (f"aws-sdk-js/1.0.27 ua/2.1 os/win32#10.0.19044 lang/js "
          f"md/nodejs#22.21.1 api/codewhispererstreaming#1.0.27 m/E "
          f"KiroIDE-{KIRO_IDE_VERSION}-{FINGERPRINT}")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-amz-json-1.0",
        "x-amz-target": "AmazonCodeWhispererStreamingService.GenerateAssistantResponse",
        "User-Agent": ua,
        "x-amz-user-agent": f"aws-sdk-js/1.0.27 KiroIDE-{KIRO_IDE_VERSION}-{FINGERPRINT}",
        "x-amzn-codewhisperer-optout": "true",
        "x-amzn-kiro-agent-mode": "vibe",
    }
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT) as r:
            r.read(256)
            return "valid"
    except urllib.error.HTTPError as e:
        body = e.read(1024).decode("utf-8", "replace")
        if "INVALID_MODEL_ID" in body:
            return "invalid"
        return f"error:{e.code} {body[:120]}"
    except Exception as e:
        return f"error:{e}"


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[warn] 无法写状态文件 {STATE_FILE}: {e}", file=sys.stderr)


# ---- 主流程 --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="同步 Kiro 可用模型到 sub2api model_mapping")
    ap.add_argument("--dry-run", action="store_true", help="只探测并打印，不写库")
    ap.add_argument("--recheck-invalid-after", type=int, default=7 * 24 * 3600,
                    help="距上次判定 invalid 超过该秒数才重探（默认 7 天；0=每次都探）")
    ap.add_argument("--pg-container", default=PG_CONTAINER)
    ap.add_argument("--pg-user", default=PG_USER)
    ap.add_argument("--pg-db", default=PG_DB)
    args = ap.parse_args()

    accounts = load_accounts(args)
    if not accounts:
        sys.exit("没有 active 的 kiro 账号")
    print(f"发现 {len(accounts)} 个 kiro 账号: {[a['id'] for a in accounts]}")

    candidates = build_candidates()
    # 各账号 mapping 已有的 exposed key（已知可用，跳过探测）。
    known_exposed = set()
    for a in accounts:
        known_exposed.update(a["mapping"].keys())

    state = load_state()
    now = int(time.time())
    probe_cache = {}   # internal_id -> 'valid'/'invalid'
    probe_acc = accounts[0]  # 同 profile 共享可用性，用首个账号探测
    ensure_fresh_token(probe_acc, args)

    # 需要探测的 = 候选里、尚未出现在任何账号 mapping、且没被近期判定 invalid 的。
    to_probe = {}
    for exposed, internal in candidates.items():
        if exposed in known_exposed:
            probe_cache[internal] = "valid"  # 已在库中即认为可用
            continue
        st = state.get(internal)
        if (st and st.get("result") == "invalid"
                and now - st.get("checked_at", 0) < args.recheck_invalid_after):
            continue  # 近期已判定不可用，跳过省额度
        to_probe[internal] = exposed

    print(f"待探测候选 {len(to_probe)} 个（已跳过 {len(candidates) - len(to_probe)} 个已知/近期无效）")
    refreshed_on_403 = False
    for internal in sorted(to_probe):
        res = probe(internal, probe_acc["token"], probe_acc["arn"], probe_acc["region"])
        if res.startswith("error:403") and not refreshed_on_403:
            # token 探测中途失效：强制刷新一次并重试当前候选
            refreshed_on_403 = True
            ensure_fresh_token(probe_acc, args, force=True)
            res = probe(internal, probe_acc["token"], probe_acc["arn"], probe_acc["region"])
        probe_cache[internal] = "valid" if res == "valid" else res
        state[internal] = {"result": ("valid" if res == "valid" else "invalid"
                                      if res == "invalid" else "error"),
                           "checked_at": now}
        flag = "✅" if res == "valid" else ("—" if res == "invalid" else "⚠️")
        print(f"  {flag} {internal:<28} {res}")
        time.sleep(PROBE_DELAY)

    valid_internal = {i for i, r in probe_cache.items() if r == "valid"}

    # 逐账号计算需要补的 pairs（含 -thinking 变体）。
    total_changes = 0
    for acc in accounts:
        new_pairs = {}
        for exposed, internal in candidates.items():
            if internal not in valid_internal:
                continue
            if exposed not in acc["mapping"]:
                new_pairs[exposed] = internal
            tv = thinking_variant(exposed, internal)
            if tv and tv[0] not in acc["mapping"]:
                new_pairs[tv[0]] = tv[1]
        if not new_pairs:
            continue
        total_changes += len(new_pairs)
        print(f"\n账号 {acc['id']} 新增 {len(new_pairs)} 项:")
        for k, v in sorted(new_pairs.items()):
            print(f"    {k} -> {v}")
        if not args.dry_run:
            apply_changes(acc, new_pairs, args)
            print(f"  已写入并触发调度器 (group_ids={acc['group_ids']})")

    if not args.dry_run:
        save_state(state)

    if total_changes == 0:
        print("\n无新增模型，mapping 已是最新。")
    elif args.dry_run:
        print(f"\n[dry-run] 共将新增 {total_changes} 项。去掉 --dry-run 即写入。")
    else:
        print(f"\n完成：共新增 {total_changes} 项。")


if __name__ == "__main__":
    main()
