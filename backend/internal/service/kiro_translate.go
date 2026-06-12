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

// estimateKiroOutputTokens 估算生成文本的输出 token 数。
// Kiro 的 usage 事件只返回一个 credit 数字，不含 input/output token 拆分，
// 因此输出 token 始终由生成文本估算。复用与 Gemini/Antigravity 等缺乏精确计数
// 的平台相同的 estimateTokensForText（ASCII≈4字符/token，CJK≈1 rune/token）。
func estimateKiroOutputTokens(text string, toolCalls []kiro.ToolCall) int {
	total := estimateTokensForText(text)
	// 工具调用的 JSON 参数也属于模型生成内容，计入输出 token。
	for _, tc := range toolCalls {
		total += estimateTokensForText(tc.Name)
		total += estimateTokensForText(tc.Arguments)
	}
	return total
}

// estimateKiroInputTokens 估算组装后 prompt 的输入 token 数。
// Kiro 上游不返回 input token，因此从入站消息（system + 历史消息 + tool 结果）
// 与工具定义中估算，复用 estimateTokensForText 保持与其他平台一致的口径。
func estimateKiroInputTokens(messages []kiro.UnifiedMessage, systemPrompt string, tools []kiro.UnifiedTool) int {
	total := estimateTokensForText(systemPrompt)

	for _, m := range messages {
		total += estimateUnifiedMessageTokens(m)
	}

	for _, t := range tools {
		total += estimateTokensForText(t.Name)
		total += estimateTokensForText(t.Description)
		if t.InputSchema != nil {
			if b, err := json.Marshal(t.InputSchema); err == nil {
				total += estimateTokensForText(string(b))
			}
		}
	}
	return total
}

// estimateUnifiedMessageTokens 估算单条统一消息的 token 数（文本 + 工具调用 + 工具结果）。
func estimateUnifiedMessageTokens(m kiro.UnifiedMessage) int {
	total := 0
	switch c := m.Content.(type) {
	case string:
		total += estimateTokensForText(c)
	case []map[string]any:
		for _, block := range c {
			if t, ok := block["text"].(string); ok {
				total += estimateTokensForText(t)
			}
		}
	}
	for _, tc := range m.ToolCalls {
		if b, err := json.Marshal(tc); err == nil {
			total += estimateTokensForText(string(b))
		}
	}
	for _, tr := range m.ToolResults {
		if content, ok := tr["content"].(string); ok {
			total += estimateTokensForText(content)
		}
	}
	return total
}
