package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/Wei-Shaw/sub2api/internal/pkg/httpclient"
	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// KiroGatewayService 转发请求到原生 Kiro 平台（Amazon Q Developer / AWS CodeWhisperer）。
//
// 它将入站的 Anthropic /v1/messages 请求转换为 Kiro conversationState，
// 调用 generateAssistantResponse，解析文本模式事件流，并把结果写回客户端
// （非流式 JSON 或 Anthropic SSE）。账号凭据（refresh_token / profile_arn /
// region / client_id / client_secret）存于 oauth 类型账号的 credentials map。
type KiroGatewayService struct {
	accountRepo AccountRepository
	// tokenCache 提供跨实例分布式刷新锁（复用 Gemini/Antigravity 等使用的 Redis
	// 实现）。可为 nil（无 Redis 时降级为仅进程内互斥锁）。
	tokenCache GeminiTokenCache
}

// NewKiroGatewayService 构造 Kiro 网关服务。tokenCache 可为 nil。
func NewKiroGatewayService(accountRepo AccountRepository, tokenCache GeminiTokenCache) *KiroGatewayService {
	return &KiroGatewayService{accountRepo: accountRepo, tokenCache: tokenCache}
}

// Credential keys stored on the account's credentials map.
const (
	kiroCredRefreshToken = "refresh_token"
	kiroCredAccessToken  = "access_token"
	kiroCredProfileArn   = "profile_arn"
	kiroCredRegion       = "region"
	kiroCredSSORegion    = "sso_region"
	kiroCredClientID     = "client_id"
	kiroCredClientSecret = "client_secret"
	kiroCredExpiresAt    = "expires_at"
)

// Streaming first-token wait/retry tuning, mirroring kiro-gateway config.py
// (FIRST_TOKEN_TIMEOUT / FIRST_TOKEN_MAX_RETRIES).
const (
	kiroFirstTokenTimeout    = 15 * time.Second
	kiroFirstTokenMaxRetries = 3
)

// Forward 执行一次转发。body 是原始的 Anthropic Messages 请求体。
func (s *KiroGatewayService) Forward(ctx context.Context, c *gin.Context, account *Account, body []byte) (*ForwardResult, error) {
	startTime := time.Now()
	reqLog := logger.L().With(zap.String("component", "service.kiro.forward"), zap.Int64("account_id", account.ID))

	var req antigravity.ClaudeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("kiro: parse request: %w", err)
	}

	messages, systemPrompt, tools := translateClaudeRequest(&req)
	modelID := s.resolveModel(account, req.Model)
	inputTokens := estimateKiroInputTokens(messages, systemPrompt, tools)

	auth, err := s.buildAuthManager(ctx, account)
	if err != nil {
		return nil, err
	}
	client := s.buildClient(auth, account)

	// makeRequest 打开一次上游请求并校验 200。每次（重试）调用都重建 payload，
	// conversationID 每次新生成，与 kiro-gateway 的 make_request 语义一致。
	makeRequest := func(reqCtx context.Context) (*http.Response, error) {
		conversationID := uuid.NewString()
		payload, _ := kiro.BuildKiroPayload(messages, systemPrompt, modelID, tools, conversationID, auth.ProfileArn())
		resp, rerr := client.GenerateAssistantResponse(reqCtx, payload)
		if rerr != nil {
			return nil, fmt.Errorf("kiro: upstream request failed: %w", rerr)
		}
		if resp.StatusCode != http.StatusOK {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			reqLog.Warn("kiro.upstream_error", zap.Int("status", resp.StatusCode), zap.ByteString("body", errBody))
			return nil, &UpstreamFailoverError{StatusCode: resp.StatusCode, ResponseBody: errBody, ResponseHeaders: resp.Header}
		}
		return resp, nil
	}

	resp, err := makeRequest(ctx)
	if err != nil {
		return nil, err
	}

	if req.Stream {
		return s.streamResponse(ctx, c, resp, makeRequest, req.Model, inputTokens, startTime, reqLog)
	}
	defer resp.Body.Close()
	return s.nonStreamResponse(c, resp, req.Model, inputTokens, startTime)
}

// nonStreamResponse 读取完整流，转换成 Anthropic Messages JSON 响应。
func (s *KiroGatewayService) nonStreamResponse(c *gin.Context, resp *http.Response, model string, inputTokens int, startTime time.Time) (*ForwardResult, error) {
	parser := kiro.NewParser()
	var text string

	buf := make([]byte, 16*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			for _, ev := range parser.Feed(buf[:n]) {
				if ev.Type == "content" {
					text += ev.Text
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("kiro: read stream: %w", err)
		}
	}

	toolCalls := parser.ToolCalls()

	msgID := "msg_" + uuid.NewString()
	content := buildClaudeContentItems(text, toolCalls)
	stopReason := "end_turn"
	if len(toolCalls) > 0 {
		stopReason = "tool_use"
	}

	// Kiro 的 usage 事件只是一个 credit 数字，不含 token 拆分，因此输入/输出
	// token 始终由 prompt/生成文本估算（与 Gemini/Antigravity 缺数时口径一致）。
	outTokens := estimateKiroOutputTokens(text, toolCalls)

	response := antigravity.ClaudeResponse{
		ID:         msgID,
		Type:       "message",
		Role:       "assistant",
		Model:      model,
		Content:    content,
		StopReason: stopReason,
		Usage:      antigravity.ClaudeUsage{InputTokens: inputTokens, OutputTokens: outTokens},
	}

	c.Header("Content-Type", "application/json")
	c.JSON(http.StatusOK, response)

	return &ForwardResult{
		RequestID:     msgID,
		Model:         model,
		UpstreamModel: model,
		Stream:        false,
		Duration:      time.Since(startTime),
		Usage:         ClaudeUsage{InputTokens: inputTokens, OutputTokens: outTokens},
	}, nil
}

// streamResponse 把 Kiro 事件流转换为 Anthropic SSE。
//
// 在写出任何 SSE 字节之前，先用 first-token 超时 + 重试逻辑等待首个内容/工具事件
// （复刻 kiro-gateway 的 stream_with_first_token_retry）：若模型在超时内无响应，
// 取消该请求并重开一个新请求，最多重试 kiroFirstTokenMaxRetries 次。一旦确认首个
// 事件到达（或全部重试失败前），才开始写 SSE，保证重试期间客户端尚未收到任何字节。
func (s *KiroGatewayService) streamResponse(
	ctx context.Context,
	c *gin.Context,
	resp *http.Response,
	makeRequest func(context.Context) (*http.Response, error),
	model string,
	inputTokens int,
	startTime time.Time,
	reqLog *zap.Logger,
) (*ForwardResult, error) {
	// 等待首事件（带超时+重试）。pending 是已经从 parser 解析出、需要在首块之后
	// 立即重放的事件。
	stream, pending, firstTokenMs, err := s.awaitFirstEvent(ctx, resp, makeRequest, startTime, reqLog)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	parser := stream.parser
	body := stream.body

	setSSEHeaders(c)
	w := c.Writer

	msgID := "msg_" + uuid.NewString()
	if err := writeSSEMessageStart(w, msgID, model); err != nil {
		return nil, err
	}

	em := newKiroStreamEmitter(w)
	var text string

	// 重放等待首事件阶段已解析出的事件，保持到达顺序。
	for _, ev := range pending {
		if derr := em.emit(ev, &text); derr != nil {
			return nil, derr
		}
	}

	buf := make([]byte, 16*1024)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			for _, ev := range parser.Feed(buf[:n]) {
				if derr := em.emit(ev, &text); derr != nil {
					return nil, derr
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, fmt.Errorf("kiro: read stream: %w", rerr)
		}
	}
	// 末尾未收到显式 stop 的在飞工具调用，按到达顺序补发。
	for _, ev := range parser.Flush() {
		if derr := em.emit(ev, &text); derr != nil {
			return nil, derr
		}
	}

	if err := em.close(); err != nil {
		return nil, err
	}

	stopReason := "end_turn"
	if em.toolCount > 0 {
		stopReason = "tool_use"
	}
	outTokens := estimateKiroOutputTokens(text, em.toolCalls)

	if err := flushSSEJSON(w, "message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": stopReason, "stop_sequence": nil},
		"usage": map[string]int{"output_tokens": outTokens},
	}); err != nil {
		return nil, err
	}
	if err := flushSSEJSON(w, "message_stop", map[string]string{"type": "message_stop"}); err != nil {
		return nil, err
	}

	return &ForwardResult{
		RequestID:     msgID,
		Model:         model,
		UpstreamModel: model,
		Stream:        true,
		Duration:      time.Since(startTime),
		FirstTokenMs:  firstTokenMs,
		Usage:         ClaudeUsage{InputTokens: inputTokens, OutputTokens: outTokens},
	}, nil
}

// awaitFirstEvent waits for the first content/tool event with a per-attempt
// timeout, retrying by re-issuing the upstream request. It returns the live
// stream (with its parser), any events already parsed (to replay in order), and
// the first-token latency. No SSE bytes have been written when this returns.
func (s *KiroGatewayService) awaitFirstEvent(
	parentCtx context.Context,
	initialResp *http.Response,
	makeRequest func(context.Context) (*http.Response, error),
	startTime time.Time,
	reqLog *zap.Logger,
) (*streamWithParser, []kiro.Event, *int, error) {
	var lastErr error
	resp := initialResp

	for attempt := 0; attempt < kiroFirstTokenMaxRetries; attempt++ {
		if attempt > 0 {
			reqLog.Warn("kiro.first_token_retry", zap.Int("attempt", attempt+1), zap.Int("max", kiroFirstTokenMaxRetries))
			newResp, err := makeRequest(parentCtx)
			if err != nil {
				// Non-timeout upstream error: propagate for failover.
				return nil, nil, nil, err
			}
			resp = newResp
		}

		parser := kiro.NewParser()
		attemptCtx, cancel := context.WithTimeout(parentCtx, kiroFirstTokenTimeout)
		pending, ok, err := readUntilFirstEvent(attemptCtx, resp.Body, parser)
		cancel()

		if ok {
			ms := int(time.Since(startTime).Milliseconds())
			return &streamWithParser{body: resp.Body, parser: parser}, pending, &ms, nil
		}

		// No first token within timeout (or stream closed early). Close and retry.
		resp.Body.Close()
		if err != nil && parentCtx.Err() != nil {
			// Parent context canceled (client disconnect): stop retrying.
			return nil, nil, nil, parentCtx.Err()
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("kiro: model did not respond within %s after %d attempts", kiroFirstTokenTimeout, kiroFirstTokenMaxRetries)
	}
	reqLog.Warn("kiro.first_token_exhausted", zap.Int("attempts", kiroFirstTokenMaxRetries), zap.Error(lastErr))
	// Treat first-token exhaustion as a failover-eligible upstream error so the
	// account scheduler can rotate to another account (mirrors antigravity 5xx).
	return nil, nil, nil, &UpstreamFailoverError{StatusCode: http.StatusGatewayTimeout, ResponseBody: []byte(lastErr.Error())}
}

// streamWithParser pairs a live response body with its parser. It is returned by
// awaitFirstEvent so the caller can keep reading from where the first-token loop
// left off without re-parsing.
type streamWithParser struct {
	body   io.ReadCloser
	parser *kiro.Parser
}

func (s *streamWithParser) Close() error { return s.body.Close() }

// readUntilFirstEvent reads from body (honoring ctx for the first-token timeout)
// until at least one content/tool_use/thinking event is parsed. It returns the
// events parsed so far (to be replayed), whether a first event was seen, and any
// read error. Reading runs in a goroutine so ctx cancellation (timeout) unblocks
// the wait even if the underlying Read does not observe the deadline.
func readUntilFirstEvent(ctx context.Context, body io.Reader, parser *kiro.Parser) ([]kiro.Event, bool, error) {
	type chunk struct {
		events []kiro.Event
		err    error
	}
	ch := make(chan chunk, 1)
	buf := make([]byte, 16*1024)
	var pending []kiro.Event

	for {
		go func() {
			n, err := body.Read(buf)
			var evs []kiro.Event
			if n > 0 {
				evs = parser.Feed(buf[:n])
			}
			ch <- chunk{events: evs, err: err}
		}()

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case res := <-ch:
			pending = append(pending, res.events...)
			// A first "real" event is content or tool_use (usage/context_usage alone
			// do not count as the model having started responding).
			if hasFirstEvent(res.events) {
				return pending, true, nil
			}
			if res.err == io.EOF {
				// Stream ended without any content: empty response, not a timeout.
				return pending, len(pending) > 0, res.err
			}
			if res.err != nil {
				return pending, false, res.err
			}
			// Got some non-first events (e.g. usage) but no content yet: keep reading.
			// Note: buf is reused next iteration; events are already materialized.
		}
	}
}

func hasFirstEvent(events []kiro.Event) bool {
	for _, ev := range events {
		if ev.Type == "content" || ev.Type == "tool_use" || ev.Type == "thinking" {
			return true
		}
	}
	return false
}

// kiroStreamEmitter converts parsed kiro events into Anthropic SSE blocks while
// preserving content/tool arrival order. Text deltas go into a single text block
// (index 0) that is opened lazily and reopened after an interleaved tool block.
type kiroStreamEmitter struct {
	w         http.ResponseWriter
	nextIndex int
	textOpen  bool
	textIndex int
	toolCount int
	toolCalls []kiro.ToolCall
}

func newKiroStreamEmitter(w http.ResponseWriter) *kiroStreamEmitter {
	return &kiroStreamEmitter{w: w}
}

func (e *kiroStreamEmitter) emit(ev kiro.Event, text *string) error {
	switch ev.Type {
	case "content":
		if ev.Text == "" {
			return nil
		}
		if err := e.ensureTextOpen(); err != nil {
			return err
		}
		*text += ev.Text
		return flushSSEJSON(e.w, "content_block_delta", map[string]any{
			"type": "content_block_delta", "index": e.textIndex,
			"delta": map[string]string{"type": "text_delta", "text": ev.Text},
		})
	case "thinking":
		// Passthrough of any native reasoning content Kiro emits. Kiro's text
		// stream currently carries no native thinking events (fake-reasoning
		// injection is intentionally OFF), so this branch is a forward-compatible
		// no-op in practice; real thinking deltas are surfaced as a thinking block.
		if ev.Text == "" {
			return nil
		}
		if err := e.ensureTextOpen(); err != nil {
			return err
		}
		return flushSSEJSON(e.w, "content_block_delta", map[string]any{
			"type": "content_block_delta", "index": e.textIndex,
			"delta": map[string]string{"type": "thinking_delta", "thinking": ev.Text},
		})
	case "tool_use":
		if ev.Tool == nil {
			return nil
		}
		// Close the open text block so the tool block lands in arrival order; a
		// subsequent text delta will lazily open a fresh text block.
		if err := e.closeText(); err != nil {
			return err
		}
		e.toolCount++
		e.toolCalls = append(e.toolCalls, *ev.Tool)
		return e.writeToolUse(*ev.Tool)
	}
	return nil
}

func (e *kiroStreamEmitter) ensureTextOpen() error {
	if e.textOpen {
		return nil
	}
	e.textIndex = e.nextIndex
	e.nextIndex++
	e.textOpen = true
	return flushSSEJSON(e.w, "content_block_start", map[string]any{
		"type": "content_block_start", "index": e.textIndex,
		"content_block": map[string]any{"type": "text", "text": ""},
	})
}

func (e *kiroStreamEmitter) closeText() error {
	if !e.textOpen {
		return nil
	}
	e.textOpen = false
	return flushSSEJSON(e.w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": e.textIndex})
}

// close flushes any open text block at end-of-stream.
func (e *kiroStreamEmitter) close() error {
	return e.closeText()
}

func (e *kiroStreamEmitter) writeToolUse(tc kiro.ToolCall) error {
	index := e.nextIndex
	e.nextIndex++

	var input any
	if tc.Arguments != "" {
		if err := json.Unmarshal([]byte(tc.Arguments), &input); err != nil {
			input = map[string]any{}
		}
	} else {
		input = map[string]any{}
	}
	if err := flushSSEJSON(e.w, "content_block_start", map[string]any{
		"type": "content_block_start", "index": index,
		"content_block": map[string]any{
			"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": map[string]any{},
		},
	}); err != nil {
		return err
	}
	b, _ := json.Marshal(input)
	if err := flushSSEJSON(e.w, "content_block_delta", map[string]any{
		"type": "content_block_delta", "index": index,
		"delta": map[string]any{"type": "input_json_delta", "partial_json": string(b)},
	}); err != nil {
		return err
	}
	return flushSSEJSON(e.w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": index})
}

// resolveModel 解析对外模型名到 Kiro modelId（账号映射优先，否则用默认映射）。
func (s *KiroGatewayService) resolveModel(account *Account, requested string) string {
	if mapped, matched := account.ResolveMappedModel(requested); matched && mapped != "" {
		return mapped
	}
	if mapped, ok := kiroDefaultModelMapping()[requested]; ok {
		return mapped
	}
	return requested
}

// buildAuthManager 从账号凭据构造 AuthManager，并配置刷新回写 + 跨实例分布式锁。
func (s *KiroGatewayService) buildAuthManager(ctx context.Context, account *Account) (*kiro.AuthManager, error) {
	creds := kiro.Credentials{
		RefreshToken: account.GetCredential(kiroCredRefreshToken),
		AccessToken:  account.GetCredential(kiroCredAccessToken),
		ProfileArn:   account.GetCredential(kiroCredProfileArn),
		Region:       account.GetCredential(kiroCredRegion),
		SSORegion:    account.GetCredential(kiroCredSSORegion),
		ClientID:     account.GetCredential(kiroCredClientID),
		ClientSecret: account.GetCredential(kiroCredClientSecret),
		ExpiresAt:    account.GetCredentialAsTime(kiroCredExpiresAt),
	}
	if creds.RefreshToken == "" && creds.AccessToken == "" {
		return nil, fmt.Errorf("kiro: account %d missing refresh_token", account.ID)
	}

	httpClient, err := httpclient.GetClient(httpclient.Options{
		ProxyURL: resolveAccountProxyURL(account),
		Timeout:  5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("kiro: build http client: %w", err)
	}

	auth := kiro.NewAuthManager(creds, httpClient)

	// 跨实例分布式刷新锁（单飞）：在 refresh+writeback 周围串行化，避免多实例并发
	// 刷新互相失效 refresh token（集成文档坑点 3）。复用 Gemini/Antigravity 使用的
	// Redis GeminiTokenCache.AcquireRefreshLock/ReleaseRefreshLock。
	if s.tokenCache != nil {
		lockKey := KiroTokenCacheKey(account)
		auth.RefreshGuard = func(refreshCtx context.Context, refresh func() error) error {
			acquired, lockErr := s.tokenCache.AcquireRefreshLock(refreshCtx, lockKey, defaultRefreshLockTTL)
			if lockErr != nil {
				// Redis 错误：降级为仅进程内互斥锁（AuthManager 自带），仍执行刷新。
				logger.L().With(zap.String("component", "service.kiro.refresh")).
					Warn("kiro.refresh_lock_failed_degraded", zap.Int64("account_id", account.ID), zap.Error(lockErr))
				return refresh()
			}
			if !acquired {
				// 锁被其它实例持有：跳过本次刷新，由 AuthManager 经 ReloadCreds 采用
				// 对方已写回的最新凭据（单飞语义）。
				return kiro.ErrRefreshLockHeld
			}
			defer func() { _ = s.tokenCache.ReleaseRefreshLock(refreshCtx, lockKey) }()
			return refresh()
		}
		// ReloadCreds：锁被持有时从 DB 重读最新凭据。
		auth.ReloadCreds = func(reloadCtx context.Context) (kiro.Credentials, bool) {
			fresh, err := s.accountRepo.GetByID(reloadCtx, account.ID)
			if err != nil || fresh == nil {
				return kiro.Credentials{}, false
			}
			return kiro.Credentials{
				AccessToken:  fresh.GetCredential(kiroCredAccessToken),
				RefreshToken: fresh.GetCredential(kiroCredRefreshToken),
				ProfileArn:   fresh.GetCredential(kiroCredProfileArn),
				ExpiresAt:    fresh.GetCredentialAsTime(kiroCredExpiresAt),
			}, true
		}
	}

	repo := s.accountRepo
	acc := account
	auth.OnRefresh = func(r kiro.RefreshResult) {
		newCreds := cloneCredentials(acc.Credentials)
		newCreds[kiroCredAccessToken] = r.AccessToken
		if r.RefreshToken != "" {
			newCreds[kiroCredRefreshToken] = r.RefreshToken
		}
		if r.ProfileArn != "" {
			newCreds[kiroCredProfileArn] = r.ProfileArn
		}
		newCreds[kiroCredExpiresAt] = r.ExpiresAt.UTC().Format(time.RFC3339)
		// 标记 token 版本，与其他平台的 _token_version 约定一致。
		newCreds["_token_version"] = time.Now().UnixMilli()
		if err := persistAccountCredentials(ctx, repo, acc, newCreds); err != nil {
			logger.L().With(zap.String("component", "service.kiro.refresh")).
				Warn("kiro.persist_credentials_failed", zap.Int64("account_id", acc.ID), zap.Error(err))
		}
	}
	return auth, nil
}

func (s *KiroGatewayService) buildClient(auth *kiro.AuthManager, account *Account) *kiro.Client {
	httpClient, err := httpclient.GetClient(httpclient.Options{
		ProxyURL: resolveAccountProxyURL(account),
		Timeout:  5 * time.Minute,
	})
	if err != nil {
		httpClient = http.DefaultClient
	}
	return kiro.NewClient(auth, httpClient)
}

// KiroTokenCacheKey 返回 Kiro 账号用于分布式刷新锁的缓存键。
func KiroTokenCacheKey(account *Account) string {
	return "kiro:account:" + strconv.FormatInt(account.ID, 10)
}
