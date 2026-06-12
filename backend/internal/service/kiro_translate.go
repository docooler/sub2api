package service

import (
	"encoding/json"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
)

// kiroDefaultModelMapping 返回 Kiro 平台默认模型映射（来自 domain 常量）。
func kiroDefaultModelMapping() map[string]string {
	return domain.DefaultKiroModelMapping
}

// translateClaudeRequest 把入站 Anthropic Messages 请求转换为 Kiro 统一格式。
// 返回 (messages, systemPrompt, tools)。
func translateClaudeRequest(req *antigravity.ClaudeRequest) ([]kiro.UnifiedMessage, string, []kiro.UnifiedTool) {
	system := extractClaudeSystem(req.System)

	messages := make([]kiro.UnifiedMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		um := translateClaudeMessage(m)
		messages = append(messages, um)
	}

	tools := translateClaudeTools(req.Tools)
	return messages, system, tools
}

// extractClaudeSystem 解析 system 字段（string 或 []SystemBlock）。
func extractClaudeSystem(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []antigravity.SystemBlock
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var out string
		for i, b := range blocks {
			if b.Text == "" {
				continue
			}
			if i > 0 && out != "" {
				out += "\n\n"
			}
			out += b.Text
		}
		return out
	}
	return ""
}

// translateClaudeMessage 把单条 Claude 消息转换为 Kiro 统一消息。
func translateClaudeMessage(m antigravity.ClaudeMessage) kiro.UnifiedMessage {
	um := kiro.UnifiedMessage{Role: m.Role}

	// content 可能是 string 或 []ContentBlock。
	var asString string
	if err := json.Unmarshal(m.Content, &asString); err == nil {
		um.Content = asString
		return um
	}

	var blocks []antigravity.ContentBlock
	if err := json.Unmarshal(m.Content, &blocks); err != nil {
		return um
	}

	var textBlocks []map[string]any
	for _, b := range blocks {
		switch b.Type {
		case "text":
			textBlocks = append(textBlocks, map[string]any{"type": "text", "text": b.Text})
		case "image":
			if b.Source != nil {
				textBlocks = append(textBlocks, map[string]any{
					"type": "image",
					"source": map[string]any{
						"type":       b.Source.Type,
						"media_type": b.Source.MediaType,
						"data":       b.Source.Data,
					},
				})
			}
		case "tool_use":
			um.ToolCalls = append(um.ToolCalls, map[string]any{
				"id": b.ID,
				"function": map[string]any{
					"name":      b.Name,
					"arguments": b.Input,
				},
			})
		case "tool_result":
			um.ToolResults = append(um.ToolResults, map[string]any{
				"tool_use_id": b.ToolUseID,
				"content":     toolResultContentToString(b.Content),
			})
		}
	}
	if len(textBlocks) > 0 {
		um.Content = textBlocks
	}
	return um
}

// toolResultContentToString 把 tool_result 的 content（string 或 块数组）规整为字符串。
func toolResultContentToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []antigravity.ContentBlock
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var out string
		for _, b := range blocks {
			if b.Type == "text" {
				out += b.Text
			}
		}
		return out
	}
	return string(raw)
}

// translateClaudeTools 把 Claude 工具定义转换为 Kiro 统一工具。
func translateClaudeTools(tools []antigravity.ClaudeTool) []kiro.UnifiedTool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]kiro.UnifiedTool, 0, len(tools))
	for _, t := range tools {
		name := t.Name
		desc := t.Description
		schema := t.InputSchema
		// Custom (MCP) 格式
		if t.Custom != nil {
			if desc == "" {
				desc = t.Custom.Description
			}
			if schema == nil {
				schema = t.Custom.InputSchema
			}
		}
		out = append(out, kiro.UnifiedTool{Name: name, Description: desc, InputSchema: schema})
	}
	return out
}

// buildClaudeContentItems 把响应文本和工具调用组装为 Anthropic content 数组。
func buildClaudeContentItems(text string, toolCalls []kiro.ToolCall) []antigravity.ClaudeContentItem {
	var items []antigravity.ClaudeContentItem
	if text != "" {
		items = append(items, antigravity.ClaudeContentItem{Type: "text", Text: text})
	}
	for _, tc := range toolCalls {
		var input any
		if tc.Arguments != "" {
			if err := json.Unmarshal([]byte(tc.Arguments), &input); err != nil {
				input = map[string]any{}
			}
		} else {
			input = map[string]any{}
		}
		items = append(items, antigravity.ClaudeContentItem{
			Type:  "tool_use",
			ID:    tc.ID,
			Name:  tc.Name,
			Input: input,
		})
	}
	if len(items) == 0 {
		items = append(items, antigravity.ClaudeContentItem{Type: "text", Text: ""})
	}
	return items
}

// estimateTokens 是一个粗略的输出 token 估算（约 4 字符/token），
// 仅在上游未返回 usage 时作为兜底。
func estimateTokens(text string) int {
	if text == "" {
		return 0
	}
	n := len([]rune(text)) / 4
	if n < 1 {
		n = 1
	}
	return n
}
