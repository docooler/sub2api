package kiro

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// Event is a parsed streaming event emitted by the parser.
type Event struct {
	// Type is one of: "content", "thinking", "tool_use", "usage", "context_usage".
	// Tool calls are also accumulated internally and surfaced via ToolCalls() for
	// the non-streaming path; streaming consumers use the "tool_use" events to
	// preserve content/tool arrival order.
	Type string
	// Text carries incremental content text for Type=="content" or the reasoning
	// text for Type=="thinking".
	Text string
	// Tool carries a finalized tool call for Type=="tool_use".
	Tool *ToolCall
	// Usage carries token usage for Type=="usage" (raw value as float).
	UsageValue float64
	// ContextUsage carries the context usage percentage for Type=="context_usage".
	ContextUsage float64
}

// findMatchingBrace returns the index of the closing brace matching the opening
// brace at startPos, accounting for strings and escapes. Returns -1 if not found
// (i.e. the JSON object is not yet complete in the buffer).
// Mirrors parsers.find_matching_brace.
func findMatchingBrace(text string, startPos int) int {
	if startPos >= len(text) || text[startPos] != '{' {
		return -1
	}
	braceCount := 0
	inString := false
	escapeNext := false
	for i := startPos; i < len(text); i++ {
		ch := text[i]
		if escapeNext {
			escapeNext = false
			continue
		}
		if ch == '\\' && inString {
			escapeNext = true
			continue
		}
		if ch == '"' {
			inString = !inString
			continue
		}
		if !inString {
			switch ch {
			case '{':
				braceCount++
			case '}':
				braceCount--
				if braceCount == 0 {
					return i
				}
			}
		}
	}
	return -1
}

type eventPattern struct {
	pattern   string
	eventType string
}

// eventPatterns mirror parsers.AwsEventStreamParser.EVENT_PATTERNS order.
var eventPatterns = []eventPattern{
	{`{"content":`, "content"},
	{`{"name":`, "tool_start"},
	{`{"input":`, "tool_input"},
	{`{"stop":`, "tool_stop"},
	{`{"followupPrompt":`, "followup"},
	{`{"usage":`, "usage"},
	{`{"contextUsagePercentage":`, "context_usage"},
}

// Parser scans the decoded UTF-8 response stream for JSON events using text
// pattern matching (no binary AWS event-stream framing). Mirrors
// parsers.AwsEventStreamParser.
type Parser struct {
	buffer          string
	lastContent     *string
	currentToolCall *ToolCall
	toolCalls       []ToolCall
}

// NewParser creates an empty parser.
func NewParser() *Parser { return &Parser{} }

// Feed appends a chunk to the buffer and returns any complete events parsed.
func (p *Parser) Feed(chunk []byte) []Event {
	p.buffer += string(chunk)
	var events []Event
	for {
		earliestPos := -1
		earliestType := ""
		for _, ep := range eventPatterns {
			pos := strings.Index(p.buffer, ep.pattern)
			if pos != -1 && (earliestPos == -1 || pos < earliestPos) {
				earliestPos = pos
				earliestType = ep.eventType
			}
		}
		if earliestPos == -1 {
			break
		}
		jsonEnd := findMatchingBrace(p.buffer, earliestPos)
		if jsonEnd == -1 {
			break // incomplete JSON, wait for more data
		}
		jsonStr := p.buffer[earliestPos : jsonEnd+1]
		p.buffer = p.buffer[jsonEnd+1:]

		var data map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			continue
		}
		// Snapshot the finalized tool-call count so we can surface any tool calls
		// finalized while processing this event as ordered "tool_use" events.
		before := len(p.toolCalls)
		ev, ok := p.processEvent(data, earliestType)
		if ok {
			events = append(events, ev)
		}
		for _, tc := range p.toolCalls[before:] {
			finalized := tc
			events = append(events, Event{Type: "tool_use", Tool: &finalized})
		}
	}
	return events
}

func (p *Parser) processEvent(data map[string]any, eventType string) (Event, bool) {
	switch eventType {
	case "content":
		return p.processContentEvent(data)
	case "tool_start":
		p.processToolStartEvent(data)
		return Event{}, false
	case "tool_input":
		p.processToolInputEvent(data)
		return Event{}, false
	case "tool_stop":
		p.processToolStopEvent(data)
		return Event{}, false
	case "usage":
		return Event{Type: "usage", UsageValue: toFloat(data["usage"])}, true
	case "context_usage":
		return Event{Type: "context_usage", ContextUsage: toFloat(data["contextUsagePercentage"])}, true
	}
	return Event{}, false
}

func (p *Parser) processContentEvent(data map[string]any) (Event, bool) {
	if fp, ok := data["followupPrompt"]; ok && fp != nil {
		return Event{}, false
	}
	content, _ := data["content"].(string)
	if p.lastContent != nil && *p.lastContent == content {
		return Event{}, false
	}
	c := content
	p.lastContent = &c
	return Event{Type: "content", Text: content}, true
}

func (p *Parser) processToolStartEvent(data map[string]any) {
	if p.currentToolCall != nil {
		p.finalizeToolCall()
	}
	id, _ := data["toolUseId"].(string)
	if id == "" {
		id = generateToolCallID()
	}
	name, _ := data["name"].(string)
	p.currentToolCall = &ToolCall{
		ID:        id,
		Name:      name,
		Arguments: inputToString(data["input"]),
	}
	if stop, ok := data["stop"].(bool); ok && stop {
		p.finalizeToolCall()
	}
}

func (p *Parser) processToolInputEvent(data map[string]any) {
	if p.currentToolCall != nil {
		p.currentToolCall.Arguments += inputToString(data["input"])
	}
}

func (p *Parser) processToolStopEvent(data map[string]any) {
	if p.currentToolCall != nil {
		if stop, ok := data["stop"].(bool); ok && stop {
			p.finalizeToolCall()
		}
	}
}

func (p *Parser) finalizeToolCall() {
	if p.currentToolCall == nil {
		return
	}
	args := strings.TrimSpace(p.currentToolCall.Arguments)
	if args != "" {
		var parsed any
		if err := json.Unmarshal([]byte(args), &parsed); err == nil {
			if b, err := json.Marshal(parsed); err == nil {
				p.currentToolCall.Arguments = string(b)
			} else {
				p.currentToolCall.Arguments = "{}"
			}
		} else {
			p.currentToolCall.Arguments = "{}"
		}
	} else {
		p.currentToolCall.Arguments = "{}"
	}
	p.toolCalls = append(p.toolCalls, *p.currentToolCall)
	p.currentToolCall = nil
}

// ToolCalls finalizes any in-flight tool call and returns deduplicated calls.
// Use this for the non-streaming path where ordering is reconstructed at the end.
func (p *Parser) ToolCalls() []ToolCall {
	if p.currentToolCall != nil {
		p.finalizeToolCall()
	}
	return deduplicateToolCalls(p.toolCalls)
}

// Flush finalizes any in-flight tool call at end-of-stream and returns the
// newly finalized calls as ordered "tool_use" events. Streaming consumers call
// this after the response body is fully read so the trailing tool call (which
// never received an explicit stop event) is still emitted in arrival order.
func (p *Parser) Flush() []Event {
	if p.currentToolCall == nil {
		return nil
	}
	before := len(p.toolCalls)
	p.finalizeToolCall()
	var events []Event
	for _, tc := range p.toolCalls[before:] {
		finalized := tc
		events = append(events, Event{Type: "tool_use", Tool: &finalized})
	}
	return events
}

// Reset clears parser state.
func (p *Parser) Reset() {
	p.buffer = ""
	p.lastContent = nil
	p.currentToolCall = nil
	p.toolCalls = nil
}

// inputToString normalizes a tool "input" field (string or object) to a string
// fragment, matching the Python behavior.
func inputToString(input any) string {
	switch v := input.(type) {
	case nil:
		return ""
	case string:
		return v
	case map[string]any:
		if len(v) == 0 {
			return ""
		}
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// deduplicateToolCalls removes duplicates by id (keeping richer args) and by
// name+arguments. Mirrors parsers.deduplicate_tool_calls.
func deduplicateToolCalls(toolCalls []ToolCall) []ToolCall {
	byID := map[string]ToolCall{}
	for _, tc := range toolCalls {
		if tc.ID == "" {
			continue
		}
		existing, ok := byID[tc.ID]
		if !ok {
			byID[tc.ID] = tc
			continue
		}
		existingArgs := existing.Arguments
		if existingArgs == "" {
			existingArgs = "{}"
		}
		curArgs := tc.Arguments
		if curArgs == "" {
			curArgs = "{}"
		}
		if curArgs != "{}" && (existingArgs == "{}" || len(curArgs) > len(existingArgs)) {
			byID[tc.ID] = tc
		}
	}
	// Preserve order: first id'd calls (in original order), then id-less.
	var withID []ToolCall
	seenID := map[string]bool{}
	for _, tc := range toolCalls {
		if tc.ID == "" {
			continue
		}
		if seenID[tc.ID] {
			continue
		}
		seenID[tc.ID] = true
		withID = append(withID, byID[tc.ID])
	}
	var withoutID []ToolCall
	for _, tc := range toolCalls {
		if tc.ID == "" {
			withoutID = append(withoutID, tc)
		}
	}

	seen := map[string]bool{}
	var unique []ToolCall
	for _, tc := range append(withID, withoutID...) {
		args := tc.Arguments
		if args == "" {
			args = "{}"
		}
		key := tc.Name + "-" + args
		if !seen[key] {
			seen[key] = true
			unique = append(unique, tc)
		}
	}
	return unique
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

// generateToolCallID returns an id in the form "call_xxxxxxxx".
func generateToolCallID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "call_00000000"
	}
	return "call_" + hex.EncodeToString(b)
}
