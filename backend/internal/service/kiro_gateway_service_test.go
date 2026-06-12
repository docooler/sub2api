package service

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
)

func TestEstimateKiroInputTokens(t *testing.T) {
	messages := []kiro.UnifiedMessage{
		{Role: "user", Content: "Hello there, how are you doing today?"},
		{Role: "assistant", Content: []map[string]any{{"type": "text", "text": "I am fine"}}},
		{Role: "user", ToolResults: []map[string]any{{"tool_use_id": "x", "content": "tool output here"}}},
	}
	tools := []kiro.UnifiedTool{
		{Name: "get_weather", Description: "Gets the weather", InputSchema: map[string]any{"type": "object"}},
	}
	got := estimateKiroInputTokens(messages, "You are a helpful assistant.", tools)
	if got <= 0 {
		t.Fatalf("expected positive input token estimate, got %d", got)
	}
	// Sanity: dropping all content should reduce the estimate.
	empty := estimateKiroInputTokens(nil, "", nil)
	if empty != 0 {
		t.Fatalf("expected 0 for empty input, got %d", empty)
	}
	if got <= empty {
		t.Fatalf("expected non-trivial estimate (%d) > empty (%d)", got, empty)
	}
}

func TestEstimateKiroOutputTokens(t *testing.T) {
	text := "This is the generated assistant reply."
	calls := []kiro.ToolCall{{Name: "search", Arguments: `{"q":"golang"}`}}
	withTools := estimateKiroOutputTokens(text, calls)
	textOnly := estimateKiroOutputTokens(text, nil)
	if withTools <= textOnly {
		t.Fatalf("tool-call args should add to output tokens: with=%d textOnly=%d", withTools, textOnly)
	}
	if estimateKiroOutputTokens("", nil) != 0 {
		t.Fatal("empty output should estimate 0 tokens")
	}
}

// TestKiroStreamEmitter_InterleavedOrder verifies that text and tool_use blocks
// are emitted in arrival order, and that text reopens a fresh block after an
// interleaved tool block (rather than dumping all tools after the text).
func TestKiroStreamEmitter_InterleavedOrder(t *testing.T) {
	rec := httptest.NewRecorder()
	em := newKiroStreamEmitter(rec)
	var text string

	events := []kiro.Event{
		{Type: "content", Text: "before "},
		{Type: "tool_use", Tool: &kiro.ToolCall{ID: "id1", Name: "t1", Arguments: `{"a":1}`}},
		{Type: "content", Text: "after"},
		{Type: "tool_use", Tool: &kiro.ToolCall{ID: "id2", Name: "t2", Arguments: `{"b":2}`}},
	}
	for _, ev := range events {
		if err := em.emit(ev, &text); err != nil {
			t.Fatalf("emit error: %v", err)
		}
	}
	if err := em.close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	if text != "before after" {
		t.Fatalf("accumulated text = %q, want %q", text, "before after")
	}
	if em.toolCount != 2 {
		t.Fatalf("toolCount = %d, want 2", em.toolCount)
	}

	body := rec.Body.String()
	// Expected block index ordering: text(0) -> tool(1) -> text(2) -> tool(3).
	wantOrder := []string{
		`"index":0`, // first text block start
		`"index":1`, // first tool block
		`"index":2`, // reopened text block after tool
		`"index":3`, // second tool block
	}
	last := -1
	for _, marker := range wantOrder {
		idx := strings.Index(body[last+1:], marker)
		if idx < 0 {
			t.Fatalf("marker %q not found in arrival order; body=\n%s", marker, body)
		}
		last = last + 1 + idx
	}

	// t1 must appear before t2 in the stream (arrival order preserved).
	if strings.Index(body, `"name":"t1"`) > strings.Index(body, `"name":"t2"`) {
		t.Fatalf("tool t1 should precede t2 in stream; body=\n%s", body)
	}
}

func TestKiroTokenCacheKey(t *testing.T) {
	acc := &Account{}
	acc.ID = 42
	if got := KiroTokenCacheKey(acc); got != "kiro:account:42" {
		t.Fatalf("KiroTokenCacheKey = %q, want kiro:account:42", got)
	}
}
