package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
}

// NewKiroGatewayService 构造 Kiro 网关服务。
func NewKiroGatewayService(accountRepo AccountRepository) *KiroGatewayService {
	return &KiroGatewayService{accountRepo: accountRepo}
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

	auth, err := s.buildAuthManager(ctx, account)
	if err != nil {
		return nil, err
	}
	client := s.buildClient(auth, account)

	conversationID := uuid.NewString()
	payload, _ := kiro.BuildKiroPayload(messages, systemPrompt, modelID, tools, conversationID, auth.ProfileArn())

	resp, err := client.GenerateAssistantResponse(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("kiro: upstream request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		reqLog.Warn("kiro.upstream_error", zap.Int("status", resp.StatusCode), zap.ByteString("body", errBody))
		return nil, &UpstreamFailoverError{StatusCode: resp.StatusCode, ResponseBody: errBody, ResponseHeaders: resp.Header}
	}

	if req.Stream {
		return s.streamResponse(c, resp, req.Model, startTime)
	}
	return s.nonStreamResponse(c, resp, req.Model, startTime)
}

// nonStreamResponse 读取完整流，转换成 Anthropic Messages JSON 响应。
func (s *KiroGatewayService) nonStreamResponse(c *gin.Context, resp *http.Response, model string, startTime time.Time) (*ForwardResult, error) {
	parser := kiro.NewParser()
	var text string
	var usage kiro.Usage
	usageSeen := false

	buf := make([]byte, 16*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			for _, ev := range parser.Feed(buf[:n]) {
				switch ev.Type {
				case "content":
					text += ev.Text
				case "usage":
					usage.OutputTokens = int(ev.UsageValue)
					usageSeen = true
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
	_ = usageSeen

	toolCalls := parser.ToolCalls()

	msgID := "msg_" + uuid.NewString()
	content := buildClaudeContentItems(text, toolCalls)
	stopReason := "end_turn"
	if len(toolCalls) > 0 {
		stopReason = "tool_use"
	}

	outTokens := usage.OutputTokens
	if outTokens == 0 {
		outTokens = estimateTokens(text)
	}

	response := antigravity.ClaudeResponse{
		ID:         msgID,
		Type:       "message",
		Role:       "assistant",
		Model:      model,
		Content:    content,
		StopReason: stopReason,
		Usage:      antigravity.ClaudeUsage{InputTokens: usage.InputTokens, OutputTokens: outTokens},
	}

	c.Header("Content-Type", "application/json")
	c.JSON(http.StatusOK, response)

	return &ForwardResult{
		RequestID:     msgID,
		Model:         model,
		UpstreamModel: model,
		Stream:        false,
		Duration:      time.Since(startTime),
		Usage:         ClaudeUsage{InputTokens: usage.InputTokens, OutputTokens: outTokens},
	}, nil
}

// streamResponse 把 Kiro 事件流转换为 Anthropic SSE。
func (s *KiroGatewayService) streamResponse(c *gin.Context, resp *http.Response, model string, startTime time.Time) (*ForwardResult, error) {
	parser := kiro.NewParser()
	setSSEHeaders(c)
	w := c.Writer

	msgID := "msg_" + uuid.NewString()
	if err := writeSSEMessageStart(w, msgID, model); err != nil {
		return nil, err
	}

	// content_block_start for the text block (index 0).
	if err := flushSSEJSON(w, "content_block_start", map[string]any{
		"type": "content_block_start", "index": 0,
		"content_block": map[string]any{"type": "text", "text": ""},
	}); err != nil {
		return nil, err
	}

	var text string
	var usage kiro.Usage
	var firstTokenMs *int

	buf := make([]byte, 16*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			for _, ev := range parser.Feed(buf[:n]) {
				switch ev.Type {
				case "content":
					if ev.Text == "" {
						continue
					}
					if firstTokenMs == nil {
						ms := int(time.Since(startTime).Milliseconds())
						firstTokenMs = &ms
					}
					text += ev.Text
					if derr := flushSSEJSON(w, "content_block_delta", map[string]any{
						"type": "content_block_delta", "index": 0,
						"delta": map[string]string{"type": "text_delta", "text": ev.Text},
					}); derr != nil {
						return nil, derr
					}
				case "usage":
					usage.OutputTokens = int(ev.UsageValue)
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

	// Close the text block.
	if err := flushSSEJSON(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}); err != nil {
		return nil, err
	}

	// Emit any tool_use blocks after the text block.
	toolCalls := parser.ToolCalls()
	index := 1
	for _, tc := range toolCalls {
		if err := s.writeSSEToolUse(w, tc, index); err != nil {
			return nil, err
		}
		index++
	}

	stopReason := "end_turn"
	if len(toolCalls) > 0 {
		stopReason = "tool_use"
	}
	outTokens := usage.OutputTokens
	if outTokens == 0 {
		outTokens = estimateTokens(text)
	}

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
		Usage:         ClaudeUsage{OutputTokens: outTokens},
	}, nil
}

func (s *KiroGatewayService) writeSSEToolUse(w http.ResponseWriter, tc kiro.ToolCall, index int) error {
	var input any
	if tc.Arguments != "" {
		if err := json.Unmarshal([]byte(tc.Arguments), &input); err != nil {
			input = map[string]any{}
		}
	} else {
		input = map[string]any{}
	}
	if err := flushSSEJSON(w, "content_block_start", map[string]any{
		"type": "content_block_start", "index": index,
		"content_block": map[string]any{
			"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": map[string]any{},
		},
	}); err != nil {
		return err
	}
	// Send the full input as a single input_json_delta.
	b, _ := json.Marshal(input)
	if err := flushSSEJSON(w, "content_block_delta", map[string]any{
		"type": "content_block_delta", "index": index,
		"delta": map[string]any{"type": "input_json_delta", "partial_json": string(b)},
	}); err != nil {
		return err
	}
	return flushSSEJSON(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": index})
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

// buildAuthManager 从账号凭据构造 AuthManager，并配置刷新回写。
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
	// 刷新后回写到账号 credentials（DB）。注意多实例并发刷新竞争见集成文档坑点 3。
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
