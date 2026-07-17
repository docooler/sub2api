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

// ForwardAsResponses 接受一个 OpenAI Responses API 请求体，转换为 Anthropic 格式后转发
// 到原生 Kiro 上游，再把响应转换回 Responses 格式。这让以 /v1/responses 协议接入的客户端
// （Codex CLI、Arize Phoenix playground 等）也能走 Kiro 平台。
//
// 与 ForwardAsChatCompletions 的关系：那条路径是 CC → Responses → Anthropic → 上游
// → Anthropic → Responses → CC，本方法即掐掉两端 CC 转换后的同一条链路，因此共用
// prepareUpstream（鉴权 / 模型映射 / 上游调用）与 apicompat 的转换实现。
//
// 与 GatewayService.ForwardAsResponses 的区别：Kiro 上游是自有事件流协议而非 Anthropic
// SSE，无法边收边转，这里沿用 ForwardAsChatCompletions 的 buffered 语义——先把事件流收全，
// 再合成完整响应。对 stream=true 的客户端是「先收齐再以 SSE 回放」，而非逐 token 增量。
func (s *KiroGatewayService) ForwardAsResponses(ctx context.Context, c *gin.Context, account *Account, body []byte) (*ForwardResult, error) {
	startTime := time.Now()

	// 1. 解析 Responses 请求
	var responsesReq apicompat.ResponsesRequest
	if err := json.Unmarshal(body, &responsesReq); err != nil {
		return nil, fmt.Errorf("kiro responses: parse responses request: %w", err)
	}
	originalModel := responsesReq.Model
	clientStream := responsesReq.Stream

	// 2. Responses → Anthropic
	anthReq, err := apicompat.ResponsesToAnthropicRequest(&responsesReq)
	if err != nil {
		return nil, fmt.Errorf("kiro responses: convert responses to anthropic: %w", err)
	}
	// 保留客户端请求的原始 model 名，交给 resolveModel 做 Kiro 目录映射。
	anthReq.Model = originalModel
	anthBody, err := json.Marshal(anthReq)
	if err != nil {
		return nil, fmt.Errorf("kiro responses: marshal anthropic request: %w", err)
	}

	// 3. 复用 Forward 的上游调用逻辑（鉴权 / 模型映射 / failover 完全一致）
	var req antigravity.ClaudeRequest
	if err := json.Unmarshal(anthBody, &req); err != nil {
		return nil, fmt.Errorf("kiro responses: parse anthropic request: %w", err)
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
			return nil, fmt.Errorf("kiro responses: read stream: %w", rerr)
		}
	}
	toolCalls := parser.ToolCalls()
	outTokens := estimateKiroOutputTokens(text, toolCalls)

	// 5. 组装 Anthropic Messages 响应
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

	usage := ClaudeUsage{InputTokens: inputTokens, OutputTokens: outTokens}

	// 6. 输出：流式客户端以 SSE 回放，非流式直接回 JSON
	if clientStream {
		writeKiroResponsesStream(c, anthResp, originalModel)
	} else {
		responsesResp := apicompat.AnthropicToResponsesResponse(anthResp)
		responsesResp.Model = originalModel
		c.Header("Content-Type", "application/json; charset=utf-8")
		c.JSON(http.StatusOK, responsesResp)
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

// writeKiroResponsesStream 把一个完整的 Anthropic 响应回放为合法的 Responses SSE 流。
//
// 实现方式是把缓冲结果反向合成 Anthropic 事件序列（message_start → content_block_* →
// message_delta → message_stop），再喂给 apicompat 既有的 Anthropic→Responses 流式状态机。
// 这样 Responses 的事件语义（sequence_number、output_index、item 生命周期）完全由那套
// 已被 GatewayService 主路径与单测覆盖的实现决定，无需在此重写一遍协议。
func writeKiroResponsesStream(c *gin.Context, anthResp *apicompat.AnthropicResponse, originalModel string) {
	setSSEHeaders(c)
	w := c.Writer

	state := apicompat.NewAnthropicEventToResponsesState()
	state.Model = originalModel

	emit := func(evt *apicompat.AnthropicStreamEvent) {
		for _, out := range apicompat.AnthropicEventToResponsesEvents(evt, state) {
			if sse, err := apicompat.ResponsesEventToSSE(out); err == nil {
				_, _ = fmt.Fprint(w, sse)
			}
		}
		w.Flush()
	}

	// message_start：携带 ID / model / 输入侧 usage
	emit(&apicompat.AnthropicStreamEvent{
		Type: "message_start",
		Message: &apicompat.AnthropicResponse{
			ID:    anthResp.ID,
			Type:  "message",
			Role:  "assistant",
			Model: originalModel,
			Usage: apicompat.AnthropicUsage{InputTokens: anthResp.Usage.InputTokens},
		},
	})

	// 每个 content block 合成 start → delta → stop
	for i := range anthResp.Content {
		block := anthResp.Content[i]
		idx := i

		start := block
		var delta *apicompat.AnthropicDelta
		switch block.Type {
		case "text":
			start.Text = "" // start 事件不带正文，正文走 delta
			delta = &apicompat.AnthropicDelta{Type: "text_delta", Text: block.Text}
		case "tool_use":
			start.Input = nil // 同上：参数走 input_json_delta
			args := string(block.Input)
			if args == "" {
				args = "{}"
			}
			delta = &apicompat.AnthropicDelta{Type: "input_json_delta", PartialJSON: args}
		default:
			continue
		}

		emit(&apicompat.AnthropicStreamEvent{Type: "content_block_start", Index: &idx, ContentBlock: &start})
		if delta != nil {
			emit(&apicompat.AnthropicStreamEvent{Type: "content_block_delta", Index: &idx, Delta: delta})
		}
		emit(&apicompat.AnthropicStreamEvent{Type: "content_block_stop", Index: &idx})
	}

	// message_delta：携带 stop_reason 与输出侧 usage
	emit(&apicompat.AnthropicStreamEvent{
		Type:  "message_delta",
		Delta: &apicompat.AnthropicDelta{},
		Usage: &apicompat.AnthropicUsage{
			InputTokens:  anthResp.Usage.InputTokens,
			OutputTokens: anthResp.Usage.OutputTokens,
		},
	})

	// message_stop：关闭未结束的 item 并发出 response.completed
	emit(&apicompat.AnthropicStreamEvent{Type: "message_stop"})
}
