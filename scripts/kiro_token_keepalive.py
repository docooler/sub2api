#!/usr/bin/env python3
"""周期性刷新所有 kiro 账号的 access/refresh token（含未参与调度的账号）。

背景：网关只在转发请求时按需刷新 token。账号一旦因积分耗尽被自动移出调度
（rate_limit_reset_at 未到）、或被手工置为不可调度，就再也没有请求经过它，
token 停止轮换；AWS SSO OIDC 的 refresh token 闲置若干天后失效，等积分恢复
时账号已经无法自动刷新，只能重新登录取 token（2026-09-01 kiro-laiyuan 即如此）。

本脚本与 kiro_model_sync.py 共用刷新实现：同一把 Redis 分布式锁
（oauth:refresh_lock:kiro:account:<id>），写回 access_token/refresh_token/
expires_at/_token_version，网关据 _token_version 失效其内存缓存。
账号筛选只看 status='active'，刻意不看 schedulable / rate_limit_reset_at。

建议 cron：*/30 * * * *
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.error

import kiro_model_sync as ks

DEFAULT_MARGIN = 1800  # 剩余有效期低于该秒数即刷新（token 有效期 1h）


def _set_error(acc_id, msg, args):
    """把刷新失败原因写到 accounts.error_message，便于在后台页面发现。"""
    esc = msg.replace("'", "''")[:500]
    ks.psql(f"UPDATE accounts SET error_message='{esc}', updated_at=now() WHERE id={acc_id};", args)


def _clear_error(acc_id, args):
    ks.psql(f"UPDATE accounts SET error_message=NULL WHERE id={acc_id} "
            f"AND error_message IS NOT NULL;", args)


def refresh_one(acc, args):
    """返回 'skipped' / 'refreshed' / 'locked' / 'failed'。"""
    if not args.force and not ks._token_expired(acc, margin=args.margin):
        return "skipped"
    if not acc["refresh_token"]:
        _set_error(acc["id"], "no refresh_token stored; re-login required", args)
        return "failed"

    lock_key = f"oauth:refresh_lock:kiro:account:{acc['id']}"
    if ks._redis_cli("SET", lock_key, "keepalive", "NX", "PX",
                     str(ks.REFRESH_LOCK_TTL_MS)) != "OK":
        # 网关正在刷新，本轮跳过即可，下一轮 cron 会看到新的 expires_at。
        return "locked"
    try:
        access, new_refresh, exp = ks._http_refresh(acc)
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        _set_error(acc["id"], f"token refresh failed ({e.code}): {body}", args)
        print(f"[error] 账号 {acc['id']} 刷新失败 {e.code}: {body}", file=sys.stderr)
        return "failed"
    except Exception as e:  # 网络等临时故障，不写 error_message
        print(f"[error] 账号 {acc['id']} 刷新异常: {e}", file=sys.stderr)
        return "failed"
    finally:
        ks._redis_cli("DEL", lock_key)

    ks._persist_tokens(acc, access, new_refresh, exp, args)
    _clear_error(acc["id"], args)
    print(f"账号 {acc['id']} 已刷新，有效期至 {exp}"
          f"{'（refresh_token 已轮换）' if new_refresh else ''}")
    return "refreshed"


def main():
    ap = argparse.ArgumentParser(description="周期刷新 kiro 账号 token（含未调度账号）")
    ap.add_argument("--margin", type=int, default=DEFAULT_MARGIN,
                    help=f"剩余有效期低于该秒数才刷新（默认 {DEFAULT_MARGIN}）")
    ap.add_argument("--force", action="store_true", help="忽略 margin，强制刷新全部账号")
    ap.add_argument("--only", type=int, nargs="*", help="只处理指定账号 id")
    ap.add_argument("--pg-container", default=ks.PG_CONTAINER)
    ap.add_argument("--pg-user", default=ks.PG_USER)
    ap.add_argument("--pg-db", default=ks.PG_DB)
    args = ap.parse_args()

    accounts = ks.load_accounts(args)
    if args.only:
        accounts = [a for a in accounts if a["id"] in args.only]
    if not accounts:
        sys.exit("没有匹配的 active kiro 账号")

    stamp = time.strftime("%Y-%m-%d %H:%M:%S%z")
    tally = {}
    for acc in accounts:
        res = refresh_one(acc, args)
        tally[res] = tally.get(res, 0) + 1
    summary = " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
    print(f"[{stamp}] 共 {len(accounts)} 个账号: {summary}")
    sys.exit(1 if tally.get("failed") else 0)


if __name__ == "__main__":
    main()
