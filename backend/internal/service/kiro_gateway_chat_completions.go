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

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
)

// ForwardAsChatCompletions 接受一个 OpenAI Chat Completions 请求体，转换为 Anthropic
// 格式后转发到原生 Kiro 上游，再把响应转换回 Chat Completions 格式。这让以 OpenAI
// /v1/chat/completions 协议接入的客户端（DeepSeek / GLM / MiniMax / Qwen 等三方模型，
// 以及 Claude 模型）也能走 Kiro 平台。
//
// 与 /v1/messages 主路径的区别：Kiro 上游本身始终是流式的，这里把事件流缓冲为完整
// 的 Anthropic Messages 响应后再转 CC（复刻 GatewayService 的 buffered CC 语义），
// 因此对 stream=true 的客户端是「先收齐再以 SSE 回放」，而非逐 token 增量。上游非 200
// 仍返回 *UpstreamFailoverError，保证 handler 的 failover 行为与主路径一致。
func (s *KiroGatewayService) ForwardAsChatCompletions(ctx context.Context, c *gin.Context, account *Account, body []byte) (*ForwardResult, error) {
	startTime := time.Now()

	// 1. 解析 Chat Completions 请求
	var ccReq apicompat.ChatCompletionsRequest
	if err := json.Unmarshal(body, &ccReq); err != nil {
		return nil, fmt.Errorf("kiro cc: parse chat completions request: %w", err)
	}
	originalModel := ccReq.Model
	clientStream := ccReq.Stream
	includeUsage := ccReq.StreamOptions != nil && ccReq.StreamOptions.IncludeUsage

	// 2. CC → Responses → Anthropic（链式转换，与 GatewayService 主路径同一套 apicompat）
	responsesReq, err := apicompat.ChatCompletionsToResponses(&ccReq)
	if err != nil {
		return nil, fmt.Errorf("kiro cc: convert chat completions to responses: %w", err)
	}
	anthReq, err := apicompat.ResponsesToAnthropicRequest(responsesReq)
	if err != nil {
		return nil, fmt.Errorf("kiro cc: convert responses to anthropic: %w", err)
	}
	// 保留客户端请求的原始 model 名，交给 resolveModel 做 Kiro 目录映射。
	anthReq.Model = originalModel
	anthBody, err := json.Marshal(anthReq)
	if err != nil {
		return nil, fmt.Errorf("kiro cc: marshal anthropic request: %w", err)
	}

	// 3. 复用 Forward 的上游调用逻辑（鉴权 / 模型映射 / failover 完全一致）
	var req antigravity.ClaudeRequest
	if err := json.Unmarshal(anthBody, &req); err != nil {
		return nil, fmt.Errorf("kiro cc: parse anthropic request: %w", err)
	}
	modelID, inputTokens, makeRequest, err := s.prepareUpstream(ctx, account, &req)
	if err != nil {
		return nil, err
	}
	resp, err := makeRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 4. 缓冲 Kiro 事件流为文本 + 工具调用
	parser := kiro.NewParser()
	var text string
	buf := make([]byte, 16*1024)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			for _, ev := range parser.Feed(buf[:n]) {
				if ev.Type == "content" {
					text += ev.Text
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, fmt.Errorf("kiro cc: read stream: %w", rerr)
		}
	}
	toolCalls := parser.ToolCalls()
	outTokens := estimateKiroOutputTokens(text, toolCalls)

	// 5. 组装 Anthropic Messages 响应，再 Anthropic → Responses → CC
	msgID := "msg_" + uuid.NewString()
	stopReason := "end_turn"
	if len(toolCalls) > 0 {
		stopReason = "tool_use"
	}
	anthResp := &apicompat.AnthropicResponse{
		ID:         msgID,
		Type:       "message",
		Role:       "assistant",
		Model:      originalModel,
		Content:    buildKiroAnthropicContent(text, toolCalls),
		StopReason: stopReason,
		Usage:      apicompat.AnthropicUsage{InputTokens: inputTokens, OutputTokens: outTokens},
	}
	ccResp := apicompat.ResponsesToChatCompletions(apicompat.AnthropicToResponsesResponse(anthResp), originalModel)

	usage := ClaudeUsage{InputTokens: inputTokens, OutputTokens: outTokens}

	// 6. 输出：流式客户端以 SSE 回放，非流式直接回 JSON
	if clientStream {
		writeKiroCCStream(c, ccResp, includeUsage)
	} else {
		c.Header("Content-Type", "application/json; charset=utf-8")
		c.JSON(http.StatusOK, ccResp)
	}

	return &ForwardResult{
		RequestID:     msgID,
		Model:         originalModel,
		UpstreamModel: modelID,
		Stream:        clientStream,
		Duration:      time.Since(startTime),
		Usage:         usage,
	}, nil
}

// buildKiroAnthropicContent 把缓冲得到的文本 + 工具调用转成 Anthropic content blocks。
func buildKiroAnthropicContent(text string, toolCalls []kiro.ToolCall) []apicompat.AnthropicContentBlock {
	var blocks []apicompat.AnthropicContentBlock
	if text != "" {
		blocks = append(blocks, apicompat.AnthropicContentBlock{Type: "text", Text: text})
	}
	for _, tc := range toolCalls {
		input := tc.Arguments
		if input == "" || !json.Valid([]byte(input)) {
			input = "{}"
		}
		blocks = append(blocks, apicompat.AnthropicContentBlock{
			Type:  "tool_use",
			ID:    tc.ID,
			Name:  tc.Name,
			Input: json.RawMessage(input),
		})
	}
	if len(blocks) == 0 {
		blocks = append(blocks, apicompat.AnthropicContentBlock{Type: "text", Text: ""})
	}
	return blocks
}

// writeKiroCCStream 把一个完整的 Chat Completions 响应回放为合法的 SSE 流：
// role chunk → content chunk → tool_calls chunk(s) → 终止 chunk(带 finish_reason)
// →（可选）usage chunk → data: [DONE]。这是「先收齐再回放」，非逐 token 增量，
// 但帧格式与 OpenAI 兼容，满足 stream=true 客户端。
func writeKiroCCStream(c *gin.Context, ccResp *apicompat.ChatCompletionsResponse, includeUsage bool) {
	setSSEHeaders(c)
	w := c.Writer

	id := ccResp.ID
	model := ccResp.Model
	created := ccResp.Created

	send := func(chunk apicompat.ChatCompletionsChunk) {
		if sse, err := apicompat.ChatChunkToSSE(chunk); err == nil {
			_, _ = fmt.Fprint(w, sse)
			w.Flush()
		}
	}
	emit := func(delta apicompat.ChatDelta, finish *string) {
		send(apicompat.ChatCompletionsChunk{
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   model,
			Choices: []apicompat.ChatChunkChoice{{Index: 0, Delta: delta, FinishReason: finish}},
		})
	}

	finishReason := "stop"
	var msg apicompat.ChatMessage
	if len(ccResp.Choices) > 0 {
		msg = ccResp.Choices[0].Message
		if ccResp.Choices[0].FinishReason != "" {
			finishReason = ccResp.Choices[0].FinishReason
		}
	}

	// role
	emit(apicompat.ChatDelta{Role: "assistant"}, nil)

	// reasoning_content（如有）
	if msg.ReasoningContent != "" {
		rc := msg.ReasoningContent
		emit(apicompat.ChatDelta{ReasoningContent: &rc}, nil)
	}

	// content（msg.Content 是 JSON 字符串）
	if len(msg.Content) > 0 {
		var contentStr string
		if err := json.Unmarshal(msg.Content, &contentStr); err == nil && contentStr != "" {
			emit(apicompat.ChatDelta{Content: &contentStr}, nil)
		}
	}

	// tool_calls（每个调用一帧，带 index）
	for i, tc := range msg.ToolCalls {
		idx := i
		tc.Index = &idx
		emit(apicompat.ChatDelta{ToolCalls: []apicompat.ChatToolCall{tc}}, nil)
	}

	// 终止 chunk
	emit(apicompat.ChatDelta{}, &finishReason)

	// usage chunk：OpenAI 约定在 include_usage 时单独发一个 choices 为空、仅带 usage
	// 的 chunk（不重复 finish_reason）。
	if includeUsage && ccResp.Usage != nil {
		send(apicompat.ChatCompletionsChunk{
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   model,
			Choices: []apicompat.ChatChunkChoice{},
			Usage:   ccResp.Usage,
		})
	}

	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	w.Flush()
}
