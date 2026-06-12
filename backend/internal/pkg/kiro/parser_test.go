package kiro

import "testing"

func TestFindMatchingBrace(t *testing.T) {
	cases := []struct {
		text  string
		start int
		want  int
	}{
		{`{"a": {"b": 1}}`, 0, 14},
		{`{"a": "{}"}`, 0, 10},
		{`{"a": "\""}`, 0, 10},
		{`{"incomplete":`, 0, -1},
		{`not a brace`, 0, -1},
	}
	for _, tc := range cases {
		if got := findMatchingBrace(tc.text, tc.start); got != tc.want {
			t.Errorf("findMatchingBrace(%q, %d) = %d, want %d", tc.text, tc.start, got, tc.want)
		}
	}
}

func TestParser_ContentEvents(t *testing.T) {
	p := NewParser()
	events := p.Feed([]byte(`some framing{"content":"Hello"}more{"content":" world"}`))
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %#v", len(events), events)
	}
	if events[0].Type != "content" || events[0].Text != "Hello" {
		t.Errorf("event[0] = %#v", events[0])
	}
	if events[1].Text != " world" {
		t.Errorf("event[1] = %#v", events[1])
	}
}

func TestParser_ContentDeduplication(t *testing.T) {
	p := NewParser()
	events := p.Feed([]byte(`{"content":"X"}{"content":"X"}{"content":"Y"}`))
	// The duplicate consecutive "X" should be dropped.
	var texts []string
	for _, e := range events {
		if e.Type == "content" {
			texts = append(texts, e.Text)
		}
	}
	if len(texts) != 2 || texts[0] != "X" || texts[1] != "Y" {
		t.Errorf("dedup texts = %#v, want [X Y]", texts)
	}
}

func TestParser_IncompleteJSONBuffered(t *testing.T) {
	p := NewParser()
	events := p.Feed([]byte(`{"content":"Hel`))
	if len(events) != 0 {
		t.Fatalf("expected no events for incomplete JSON, got %#v", events)
	}
	events = p.Feed([]byte(`lo"}`))
	if len(events) != 1 || events[0].Text != "Hello" {
		t.Errorf("after completion events = %#v", events)
	}
}

func TestParser_ToolCallStartStop(t *testing.T) {
	p := NewParser()
	// tool_start with full input and stop flag in one event.
	p.Feed([]byte(`{"name":"get_weather","toolUseId":"call_abc","input":{"city":"London"},"stop":true}`))
	calls := p.ToolCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d calls, want 1: %#v", len(calls), calls)
	}
	if calls[0].Name != "get_weather" || calls[0].ID != "call_abc" {
		t.Errorf("call = %#v", calls[0])
	}
	if calls[0].Arguments != `{"city":"London"}` {
		t.Errorf("args = %q, want %q", calls[0].Arguments, `{"city":"London"}`)
	}
}

func TestParser_ToolCallStreamingInput(t *testing.T) {
	p := NewParser()
	// tool_start with empty input, then input fragments, then stop.
	p.Feed([]byte(`{"name":"search","toolUseId":"call_1","input":{}}`))
	p.Feed([]byte(`{"input":"{\"q\":"}`))
	p.Feed([]byte(`{"input":"\"hello\"}"}`))
	p.Feed([]byte(`{"stop":true}`))
	calls := p.ToolCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d calls, want 1: %#v", len(calls), calls)
	}
	// Arguments accumulated to {"q":"hello"} and re-serialized.
	if calls[0].Arguments != `{"q":"hello"}` {
		t.Errorf("args = %q, want %q", calls[0].Arguments, `{"q":"hello"}`)
	}
}

func TestParser_UsageEvent(t *testing.T) {
	p := NewParser()
	events := p.Feed([]byte(`{"usage":42}`))
	if len(events) != 1 || events[0].Type != "usage" {
		t.Fatalf("usage events = %#v", events)
	}
	if events[0].UsageValue != 42 {
		t.Errorf("usage value = %v, want 42", events[0].UsageValue)
	}
}

func TestParser_ContextUsageEvent(t *testing.T) {
	p := NewParser()
	events := p.Feed([]byte(`{"contextUsagePercentage":12.5}`))
	if len(events) != 1 || events[0].Type != "context_usage" {
		t.Fatalf("context_usage events = %#v", events)
	}
	if events[0].ContextUsage != 12.5 {
		t.Errorf("context usage = %v, want 12.5", events[0].ContextUsage)
	}
}

func TestParser_FollowupPromptSkipped(t *testing.T) {
	p := NewParser()
	// content event carrying a followupPrompt should be skipped.
	events := p.Feed([]byte(`{"content":"ignored","followupPrompt":{"x":1}}`))
	for _, e := range events {
		if e.Type == "content" {
			t.Errorf("followupPrompt content should be skipped, got %#v", e)
		}
	}
}

func TestParser_OrderedToolUseEvents(t *testing.T) {
	p := NewParser()
	// text, then a tool that completes (stop), then more text, then a trailing
	// tool that only completes on Flush. Arrival order must be preserved.
	var events []Event
	events = append(events, p.Feed([]byte(`{"content":"before"}`))...)
	events = append(events, p.Feed([]byte(`{"name":"t1","toolUseId":"id1","input":{"a":1},"stop":true}`))...)
	events = append(events, p.Feed([]byte(`{"content":"after"}`))...)
	events = append(events, p.Feed([]byte(`{"name":"t2","toolUseId":"id2","input":{"b":2}}`))...)
	events = append(events, p.Flush()...)

	var order []string
	for _, e := range events {
		switch e.Type {
		case "content":
			order = append(order, "content:"+e.Text)
		case "tool_use":
			if e.Tool == nil {
				t.Fatalf("tool_use event missing Tool: %#v", e)
			}
			order = append(order, "tool:"+e.Tool.Name)
		}
	}
	want := []string{"content:before", "tool:t1", "content:after", "tool:t2"}
	if len(order) != len(want) {
		t.Fatalf("order = %#v, want %#v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d] = %q, want %q (full=%#v)", i, order[i], want[i], order)
		}
	}
}

func TestParser_FlushFinalizesTrailingToolOnce(t *testing.T) {
	p := NewParser()
	// Tool with no explicit stop: must surface via Flush, exactly once.
	p.Feed([]byte(`{"name":"only","toolUseId":"idX","input":{"x":1}}`))
	flushed := p.Flush()
	if len(flushed) != 1 || flushed[0].Type != "tool_use" || flushed[0].Tool == nil {
		t.Fatalf("flush = %#v, want one tool_use", flushed)
	}
	if flushed[0].Tool.Name != "only" || flushed[0].Tool.Arguments != `{"x":1}` {
		t.Fatalf("flushed tool = %#v", flushed[0].Tool)
	}
	// Flush again is a no-op (already finalized).
	if again := p.Flush(); len(again) != 0 {
		t.Fatalf("second flush = %#v, want empty", again)
	}
}

func TestParser_MultipleToolCallsDeduplicated(t *testing.T) {
	p := NewParser()
	// Same id appears twice; the one with richer args should win.
	p.Feed([]byte(`{"name":"f","toolUseId":"id1","input":{},"stop":true}`))
	p.Feed([]byte(`{"name":"f","toolUseId":"id1","input":{"a":1},"stop":true}`))
	calls := p.ToolCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d calls, want 1 after dedup: %#v", len(calls), calls)
	}
	if calls[0].Arguments != `{"a":1}` {
		t.Errorf("args = %q, want richer %q", calls[0].Arguments, `{"a":1}`)
	}
}
