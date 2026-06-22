// Package kiro implements a native client for the Kiro API
// (Amazon Q Developer / AWS CodeWhisperer), ported from the Python
// kiro-gateway reference implementation.
//
// It provides:
//   - auth.go:    token refresh (Kiro Desktop + AWS SSO OIDC), in-memory cache
//   - convert.go: unified messages -> Kiro conversationState payload
//   - parser.go:  text-pattern event parsing of the streamed response
//   - client.go:  POST generateAssistantResponse with retry + headers
//
// The package intentionally keeps its own minimal message/tool types so it does
// not depend on the rest of sub2api; callers translate their inbound formats
// (Anthropic / OpenAI) into these unified types.
package kiro

// UnifiedMessage is the API-agnostic message representation consumed by the
// converter. Roles are "user" or "assistant" (others are normalized to user).
type UnifiedMessage struct {
	Role string
	// Content is either a plain string or a []map[string]any of content blocks
	// (Anthropic-style: {"type":"text","text":...} / {"type":"image",...} /
	// {"type":"tool_use",...} / {"type":"tool_result",...}).
	Content any
	// ToolCalls holds assistant tool calls in the OpenAI-ish unified shape:
	//   {"id":..., "function": {"name":..., "arguments": string|map}}
	ToolCalls []map[string]any
	// ToolResults holds user tool results in the unified shape:
	//   {"tool_use_id":..., "content": string|[]block}
	ToolResults []map[string]any
	// Images holds unified images: {"media_type":"image/png", "data":"base64"}
	Images []map[string]any
}

// UnifiedTool is the API-agnostic tool definition.
type UnifiedTool struct {
	Name        string
	Description string
	InputSchema map[string]any
}

// ThinkingConfig controls fake-reasoning tag injection (disabled by default in
// this native port; kept for parity / future use).
type ThinkingConfig struct {
	Enabled      bool
	BudgetTokens *int
}

// Usage captures token accounting parsed from the Kiro response stream.
type Usage struct {
	InputTokens  int
	OutputTokens int
}

// ToolCall is a finalized tool call extracted from the response stream.
// Arguments is a JSON-encoded string (OpenAI function-call convention).
type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}
