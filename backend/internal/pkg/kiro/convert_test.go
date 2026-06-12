package kiro

import (
	"encoding/json"
	"testing"
)

// helper: navigate the payload conversationState.
func convState(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	cs, ok := payload["conversationState"].(map[string]any)
	if !ok {
		t.Fatalf("payload missing conversationState: %#v", payload)
	}
	return cs
}

func currentUserInput(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	cs := convState(t, payload)
	cm, ok := cs["currentMessage"].(map[string]any)
	if !ok {
		t.Fatalf("missing currentMessage")
	}
	uim, ok := cm["userInputMessage"].(map[string]any)
	if !ok {
		t.Fatalf("missing userInputMessage")
	}
	return uim
}

func historyOf(payload map[string]any) []map[string]any {
	cs, _ := payload["conversationState"].(map[string]any)
	raw, _ := cs["history"].([]map[string]any)
	return raw
}

func TestBuildKiroPayload_BasicUserMessage(t *testing.T) {
	msgs := []UnifiedMessage{{Role: "user", Content: "Hello"}}
	payload, _ := BuildKiroPayload(msgs, "", "claude-sonnet-4-5", nil, "conv-1", "arn:profile")

	cs := convState(t, payload)
	if cs["chatTriggerType"] != "MANUAL" {
		t.Errorf("chatTriggerType = %v, want MANUAL", cs["chatTriggerType"])
	}
	if cs["conversationId"] != "conv-1" {
		t.Errorf("conversationId = %v", cs["conversationId"])
	}
	if payload["profileArn"] != "arn:profile" {
		t.Errorf("profileArn = %v", payload["profileArn"])
	}
	uim := currentUserInput(t, payload)
	if uim["content"] != "Hello" {
		t.Errorf("content = %v, want Hello", uim["content"])
	}
	if uim["modelId"] != "claude-sonnet-4-5" {
		t.Errorf("modelId = %v", uim["modelId"])
	}
	if uim["origin"] != "AI_EDITOR" {
		t.Errorf("origin = %v", uim["origin"])
	}
	if len(historyOf(payload)) != 0 {
		t.Errorf("expected empty history for single message")
	}
}

func TestBuildKiroPayload_SystemPromptInjectionNoHistory(t *testing.T) {
	msgs := []UnifiedMessage{{Role: "user", Content: "Hi"}}
	payload, _ := BuildKiroPayload(msgs, "You are helpful.", "m", nil, "c", "")
	uim := currentUserInput(t, payload)
	want := "You are helpful.\n\nHi"
	if uim["content"] != want {
		t.Errorf("content = %q, want %q", uim["content"], want)
	}
}

func TestBuildKiroPayload_SystemPromptInjectionFirstHistoryUser(t *testing.T) {
	msgs := []UnifiedMessage{
		{Role: "user", Content: "First"},
		{Role: "assistant", Content: "Reply"},
		{Role: "user", Content: "Second"},
	}
	payload, _ := BuildKiroPayload(msgs, "SYS", "m", nil, "c", "")
	hist := historyOf(payload)
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	firstUser, _ := hist[0]["userInputMessage"].(map[string]any)
	want := "SYS\n\nFirst"
	if firstUser["content"] != want {
		t.Errorf("first history content = %q, want %q", firstUser["content"], want)
	}
	// current message should be "Second" un-prefixed
	uim := currentUserInput(t, payload)
	if uim["content"] != "Second" {
		t.Errorf("current content = %q, want Second", uim["content"])
	}
}

func TestBuildKiroPayload_ForceFirstUser(t *testing.T) {
	// First message is assistant -> synthetic user placeholder prepended.
	msgs := []UnifiedMessage{
		{Role: "assistant", Content: "Hello"},
		{Role: "user", Content: "Hi"},
	}
	payload, _ := BuildKiroPayload(msgs, "", "m", nil, "c", "")
	hist := historyOf(payload)
	if len(hist) == 0 {
		t.Fatalf("expected non-empty history")
	}
	firstUser, ok := hist[0]["userInputMessage"].(map[string]any)
	if !ok {
		t.Fatalf("first history entry should be userInputMessage, got %#v", hist[0])
	}
	if firstUser["content"] != emptyPlaceholder {
		t.Errorf("synthetic first user content = %q, want %q", firstUser["content"], emptyPlaceholder)
	}
}

func TestBuildKiroPayload_AlternatingRolesInserted(t *testing.T) {
	// Consecutive user messages separated by an assistant exercise alternation.
	// Pipeline (merge runs first): u(First) a(Reply) u(Second) u(Third).
	// merge keeps them distinct (no adjacency among the two trailing users until
	// the assistant separates the first pair); the trailing two users are
	// adjacent and merge into one, so we use a non-mergeable layout instead.
	msgs := []UnifiedMessage{
		{Role: "user", Content: "First"},
		{Role: "assistant", Content: "Reply"},
		{Role: "user", Content: "Second"},
		// current message is appended by the harness below
		{Role: "user", Content: "Third"},
	}
	payload, _ := BuildKiroPayload(msgs, "", "m", nil, "c", "")
	// merge: u(First) a(Reply) u(Second\nThird). history = u, a. current = u.
	hist := historyOf(payload)
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	if _, ok := hist[0]["userInputMessage"]; !ok {
		t.Errorf("hist[0] should be userInputMessage")
	}
	if _, ok := hist[1]["assistantResponseMessage"]; !ok {
		t.Errorf("hist[1] should be assistantResponseMessage")
	}
}

func TestBuildKiroPayload_ConsecutiveUsersMerge(t *testing.T) {
	// Three consecutive user messages merge into one (Kiro forbids consecutive
	// same-role); the merged single user becomes the current message.
	msgs := []UnifiedMessage{
		{Role: "user", Content: "First"},
		{Role: "user", Content: "Second"},
		{Role: "user", Content: "Third"},
	}
	payload, _ := BuildKiroPayload(msgs, "", "m", nil, "c", "")
	if len(historyOf(payload)) != 0 {
		t.Fatalf("expected empty history after merge to single user message")
	}
	uim := currentUserInput(t, payload)
	if uim["content"] != "First\nSecond\nThird" {
		t.Errorf("merged content = %q, want %q", uim["content"], "First\nSecond\nThird")
	}
}

func TestEnsureAlternatingRoles_Synthetic(t *testing.T) {
	// Unit test of the alternation helper directly (consecutive users get a
	// synthetic assistant placeholder between them).
	msgs := []UnifiedMessage{
		{Role: "user", Content: "First"},
		{Role: "user", Content: "Second"},
	}
	out := ensureAlternatingRoles(msgs)
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3", len(out))
	}
	if out[1].Role != "assistant" || out[1].Content != emptyPlaceholder {
		t.Errorf("synthetic = %#v", out[1])
	}
}

func TestBuildKiroPayload_EmptyContentPlaceholder(t *testing.T) {
	msgs := []UnifiedMessage{{Role: "user", Content: ""}}
	payload, _ := BuildKiroPayload(msgs, "", "m", nil, "c", "")
	uim := currentUserInput(t, payload)
	if uim["content"] != emptyPlaceholder {
		t.Errorf("content = %q, want %q", uim["content"], emptyPlaceholder)
	}
}

func TestBuildKiroPayload_MergeAdjacentSameRole(t *testing.T) {
	msgs := []UnifiedMessage{
		{Role: "user", Content: "A"},
		{Role: "assistant", Content: "x"},
		{Role: "assistant", Content: "y"},
		{Role: "user", Content: "B"},
	}
	payload, _ := BuildKiroPayload(msgs, "", "m", nil, "c", "")
	hist := historyOf(payload)
	// merged: user A, assistant "x\ny", user B(current). history len 2.
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	asst, _ := hist[1]["assistantResponseMessage"].(map[string]any)
	if asst["content"] != "x\ny" {
		t.Errorf("merged assistant content = %q, want %q", asst["content"], "x\ny")
	}
}

func TestBuildKiroPayload_ToolsConversion(t *testing.T) {
	tools := []UnifiedTool{{
		Name:        "get_weather",
		Description: "Get the weather",
		InputSchema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{"city": map[string]any{"type": "string"}},
			"required":             []any{}, // empty -> stripped
			"additionalProperties": false,   // -> stripped
		},
	}}
	msgs := []UnifiedMessage{{Role: "user", Content: "weather?"}}
	payload, _ := BuildKiroPayload(msgs, "", "m", tools, "c", "")
	uim := currentUserInput(t, payload)
	ctx, ok := uim["userInputMessageContext"].(map[string]any)
	if !ok {
		t.Fatalf("missing userInputMessageContext with tools")
	}
	ktools, ok := ctx["tools"].([]map[string]any)
	if !ok || len(ktools) != 1 {
		t.Fatalf("tools = %#v", ctx["tools"])
	}
	spec, _ := ktools[0]["toolSpecification"].(map[string]any)
	if spec["name"] != "get_weather" {
		t.Errorf("tool name = %v", spec["name"])
	}
	inputSchema, _ := spec["inputSchema"].(map[string]any)
	jsonSchema, _ := inputSchema["json"].(map[string]any)
	if _, present := jsonSchema["required"]; present {
		t.Errorf("empty required should be stripped: %#v", jsonSchema)
	}
	if _, present := jsonSchema["additionalProperties"]; present {
		t.Errorf("additionalProperties should be stripped: %#v", jsonSchema)
	}
}

func TestBuildKiroPayload_ToolResultsAndUses(t *testing.T) {
	tools := []UnifiedTool{{Name: "search", Description: "search"}}
	msgs := []UnifiedMessage{
		{Role: "user", Content: "do it"},
		{Role: "assistant", Content: "calling", ToolCalls: []map[string]any{
			{"id": "call_1", "function": map[string]any{"name": "search", "arguments": `{"q":"x"}`}},
		}},
		{Role: "user", ToolResults: []map[string]any{
			{"tool_use_id": "call_1", "content": "result text"},
		}},
	}
	payload, _ := BuildKiroPayload(msgs, "", "m", tools, "c", "")

	// Assistant tool use should be in history.
	hist := historyOf(payload)
	var foundUse bool
	for _, h := range hist {
		if asst, ok := h["assistantResponseMessage"].(map[string]any); ok {
			if uses, ok := asst["toolUses"].([]map[string]any); ok && len(uses) > 0 {
				if uses[0]["name"] == "search" && uses[0]["toolUseId"] == "call_1" {
					foundUse = true
				}
			}
		}
	}
	if !foundUse {
		t.Errorf("expected assistant toolUses in history, hist=%#v", hist)
	}

	// Current user message carries toolResults.
	uim := currentUserInput(t, payload)
	ctx, _ := uim["userInputMessageContext"].(map[string]any)
	ktr, ok := ctx["toolResults"].([]map[string]any)
	if !ok || len(ktr) != 1 {
		t.Fatalf("toolResults = %#v", ctx["toolResults"])
	}
	if ktr[0]["toolUseId"] != "call_1" || ktr[0]["status"] != "success" {
		t.Errorf("toolResult = %#v", ktr[0])
	}
}

func TestBuildKiroPayload_StripToolsWhenNoToolsDefined(t *testing.T) {
	// No tools defined: tool calls/results must be folded to text, no toolResults key.
	msgs := []UnifiedMessage{
		{Role: "user", Content: "do it"},
		{Role: "assistant", Content: "calling", ToolCalls: []map[string]any{
			{"id": "call_1", "function": map[string]any{"name": "search", "arguments": `{"q":"x"}`}},
		}},
		{Role: "user", ToolResults: []map[string]any{
			{"tool_use_id": "call_1", "content": "result text"},
		}},
	}
	payload, _ := BuildKiroPayload(msgs, "", "m", nil, "c", "")
	uim := currentUserInput(t, payload)
	if ctx, ok := uim["userInputMessageContext"].(map[string]any); ok {
		if _, present := ctx["toolResults"]; present {
			t.Errorf("toolResults should not be present when no tools defined")
		}
	}
	// Tool result text must appear in the current content.
	content, _ := uim["content"].(string)
	if content == "" || !contains(content, "result text") {
		t.Errorf("expected tool result folded into content, got %q", content)
	}
}

func TestBuildKiroPayload_Images(t *testing.T) {
	msgs := []UnifiedMessage{{
		Role: "user",
		Content: []map[string]any{
			{"type": "text", "text": "look"},
			{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": "abc123"}},
		},
	}}
	payload, _ := BuildKiroPayload(msgs, "", "m", nil, "c", "")
	uim := currentUserInput(t, payload)
	imgs, ok := uim["images"].([]map[string]any)
	if !ok || len(imgs) != 1 {
		t.Fatalf("images = %#v", uim["images"])
	}
	if imgs[0]["format"] != "png" {
		t.Errorf("image format = %v, want png", imgs[0]["format"])
	}
	src, _ := imgs[0]["source"].(map[string]any)
	if src["bytes"] != "abc123" {
		t.Errorf("image bytes = %v", src["bytes"])
	}
	if uim["content"] != "look" {
		t.Errorf("content = %v, want look", uim["content"])
	}
}

func TestExtractTextContent(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, ""},
		{"string", "Hello", "Hello"},
		{"blocks", []map[string]any{{"type": "text", "text": "World"}}, "World"},
		{"mixed", []map[string]any{
			{"type": "text", "text": "A"},
			{"type": "image", "source": map[string]any{}},
			{"type": "text", "text": "B"},
		}, "AB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractTextContent(tc.in); got != tc.want {
				t.Errorf("extractTextContent(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestBuildKiroPayload_ValidJSON(t *testing.T) {
	msgs := []UnifiedMessage{{Role: "user", Content: "Hello"}}
	payload, _ := BuildKiroPayload(msgs, "sys", "m", nil, "c", "arn")
	if _, err := json.Marshal(payload); err != nil {
		t.Fatalf("payload not JSON-serializable: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
