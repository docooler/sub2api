package kiro

import (
	"encoding/json"
	"strings"
)

// emptyPlaceholder is the minimal valid content Kiro requires for empty messages.
// Kept byte-identical to the Python reference ("(empty placeholder)").
const emptyPlaceholder = "(empty placeholder)"

// emptyToolResult is the placeholder for empty tool-result content.
const emptyToolResult = "(empty result)"

// toolDescriptionMaxLength mirrors kiro-gateway TOOL_DESCRIPTION_MAX_LENGTH default.
const toolDescriptionMaxLength = 10000

// extractTextContent extracts plain text from a string or a list of content blocks.
// Mirrors converters_core.extract_text_content.
func extractTextContent(content any) string {
	switch v := content.(type) {
	case nil:
		return ""
	case string:
		return v
	case []map[string]any:
		var parts []string
		for _, item := range v {
			parts = appendBlockText(parts, item)
		}
		return strings.Join(parts, "")
	case []any:
		var parts []string
		for _, raw := range v {
			if item, ok := raw.(map[string]any); ok {
				parts = appendBlockText(parts, item)
			} else if s, ok := raw.(string); ok {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "")
	default:
		return ""
	}
}

func appendBlockText(parts []string, item map[string]any) []string {
	t, _ := item["type"].(string)
	switch t {
	case "image", "image_url", "tool_reference":
		return parts // handled separately
	case "text":
		if s, ok := item["text"].(string); ok {
			parts = append(parts, s)
		}
		return parts
	}
	if s, ok := item["text"].(string); ok {
		parts = append(parts, s)
	}
	return parts
}

// asBlockList normalizes content into a []map[string]any if it is list-shaped.
func asBlockList(content any) ([]map[string]any, bool) {
	switch v := content.(type) {
	case []map[string]any:
		return v, true
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, raw := range v {
			if m, ok := raw.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out, true
	default:
		return nil, false
	}
}

// extractImagesFromContent pulls images from content blocks into unified format.
// Mirrors converters_core.extract_images_from_content (base64 sources only).
func extractImagesFromContent(content any) []map[string]any {
	blocks, ok := asBlockList(content)
	if !ok {
		return nil
	}
	var images []map[string]any
	for _, item := range blocks {
		t, _ := item["type"].(string)
		switch t {
		case "image_url":
			urlObj, _ := item["image_url"].(map[string]any)
			url, _ := urlObj["url"].(string)
			if strings.HasPrefix(url, "data:") {
				if idx := strings.Index(url, ","); idx >= 0 {
					header := url[:idx]
					data := url[idx+1:]
					mediaPart := strings.SplitN(header, ";", 2)[0]
					mediaType := strings.TrimPrefix(mediaPart, "data:")
					if data != "" {
						images = append(images, map[string]any{"media_type": mediaType, "data": data})
					}
				}
			}
		case "image":
			source, _ := item["source"].(map[string]any)
			if source == nil {
				continue
			}
			if st, _ := source["type"].(string); st == "base64" {
				mediaType, _ := source["media_type"].(string)
				if mediaType == "" {
					mediaType = "image/jpeg"
				}
				data, _ := source["data"].(string)
				if data != "" {
					images = append(images, map[string]any{"media_type": mediaType, "data": data})
				}
			}
		}
	}
	return images
}

// sanitizeJSONSchema strips fields Kiro rejects: empty required arrays and
// additionalProperties (recursively). Mirrors converters_core.sanitize_json_schema.
func sanitizeJSONSchema(schema map[string]any) map[string]any {
	if len(schema) == 0 {
		return map[string]any{}
	}
	result := make(map[string]any, len(schema))
	for key, value := range schema {
		if key == "required" {
			if arr, ok := value.([]any); ok && len(arr) == 0 {
				continue
			}
		}
		if key == "additionalProperties" {
			continue
		}
		if key == "properties" {
			if props, ok := value.(map[string]any); ok {
				newProps := make(map[string]any, len(props))
				for pn, pv := range props {
					if pm, ok := pv.(map[string]any); ok {
						newProps[pn] = sanitizeJSONSchema(pm)
					} else {
						newProps[pn] = pv
					}
				}
				result[key] = newProps
				continue
			}
		}
		switch v := value.(type) {
		case map[string]any:
			result[key] = sanitizeJSONSchema(v)
		case []any:
			arr := make([]any, len(v))
			for i, item := range v {
				if im, ok := item.(map[string]any); ok {
					arr[i] = sanitizeJSONSchema(im)
				} else {
					arr[i] = item
				}
			}
			result[key] = arr
		default:
			result[key] = value
		}
	}
	return result
}

// processToolsWithLongDescriptions moves over-long descriptions into system-prompt
// documentation. Mirrors converters_core.process_tools_with_long_descriptions.
func processToolsWithLongDescriptions(tools []UnifiedTool) ([]UnifiedTool, string) {
	if len(tools) == 0 {
		return nil, ""
	}
	var docParts []string
	processed := make([]UnifiedTool, 0, len(tools))
	for _, tool := range tools {
		desc := tool.Description
		if len(desc) <= toolDescriptionMaxLength {
			processed = append(processed, tool)
			continue
		}
		docParts = append(docParts, "## Tool: "+tool.Name+"\n\n"+desc)
		processed = append(processed, UnifiedTool{
			Name:        tool.Name,
			Description: "[Full documentation in system prompt under '## Tool: " + tool.Name + "']",
			InputSchema: tool.InputSchema,
		})
	}
	doc := ""
	if len(docParts) > 0 {
		doc = "\n\n---\n# Tool Documentation\nThe following tools have detailed documentation that couldn't fit in the tool definition.\n\n" +
			strings.Join(docParts, "\n\n---\n\n")
	}
	return processed, doc
}

// convertToolsToKiroFormat converts unified tools to Kiro toolSpecification entries.
func convertToolsToKiroFormat(tools []UnifiedTool) []map[string]any {
	if len(tools) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		params := sanitizeJSONSchema(tool.InputSchema)
		desc := tool.Description
		if strings.TrimSpace(desc) == "" {
			desc = "Tool: " + tool.Name
		}
		out = append(out, map[string]any{
			"toolSpecification": map[string]any{
				"name":        tool.Name,
				"description": desc,
				"inputSchema": map[string]any{"json": params},
			},
		})
	}
	return out
}

// convertImagesToKiroFormat converts unified images to Kiro image entries.
func convertImagesToKiroFormat(images []map[string]any) []map[string]any {
	if len(images) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(images))
	for _, img := range images {
		mediaType, _ := img["media_type"].(string)
		if mediaType == "" {
			mediaType = "image/jpeg"
		}
		data, _ := img["data"].(string)
		if data == "" {
			continue
		}
		if strings.HasPrefix(data, "data:") {
			if idx := strings.Index(data, ","); idx >= 0 {
				header := data[:idx]
				actual := data[idx+1:]
				mediaPart := strings.SplitN(header, ";", 2)[0]
				if extracted := strings.TrimPrefix(mediaPart, "data:"); extracted != "" {
					mediaType = extracted
				}
				data = actual
			}
		}
		format := mediaType
		if idx := strings.LastIndex(mediaType, "/"); idx >= 0 {
			format = mediaType[idx+1:]
		}
		out = append(out, map[string]any{
			"format": format,
			"source": map[string]any{"bytes": data},
		})
	}
	return out
}

// convertToolResultsToKiroFormat converts unified tool results to Kiro format.
func convertToolResultsToKiroFormat(toolResults []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(toolResults))
	for _, tr := range toolResults {
		contentText := toolResultContentText(tr["content"])
		if contentText == "" {
			contentText = emptyToolResult
		}
		toolUseID, _ := tr["tool_use_id"].(string)
		out = append(out, map[string]any{
			"content":   []map[string]any{{"text": contentText}},
			"status":    "success",
			"toolUseId": toolUseID,
		})
	}
	return out
}

func toolResultContentText(content any) string {
	if s, ok := content.(string); ok {
		return s
	}
	return extractTextContent(content)
}

// extractToolResultsFromContent pulls tool_result blocks from content (Anthropic).
func extractToolResultsFromContent(content any) []map[string]any {
	blocks, ok := asBlockList(content)
	if !ok {
		return nil
	}
	var out []map[string]any
	for _, item := range blocks {
		if t, _ := item["type"].(string); t == "tool_result" {
			text := extractTextContent(item["content"])
			if text == "" {
				text = emptyToolResult
			}
			toolUseID, _ := item["tool_use_id"].(string)
			out = append(out, map[string]any{
				"content":   []map[string]any{{"text": text}},
				"status":    "success",
				"toolUseId": toolUseID,
			})
		}
	}
	return out
}

// extractToolUsesFromMessage pulls tool uses from tool_calls and content blocks.
func extractToolUsesFromMessage(content any, toolCalls []map[string]any) []map[string]any {
	var out []map[string]any
	for _, tc := range toolCalls {
		fn, _ := tc["function"].(map[string]any)
		var input any
		switch args := fn["arguments"].(type) {
		case string:
			if args != "" {
				var parsed any
				if err := json.Unmarshal([]byte(args), &parsed); err == nil {
					input = parsed
				} else {
					input = map[string]any{}
				}
			} else {
				input = map[string]any{}
			}
		case nil:
			input = map[string]any{}
		default:
			input = args
		}
		name, _ := fn["name"].(string)
		id, _ := tc["id"].(string)
		out = append(out, map[string]any{"name": name, "input": input, "toolUseId": id})
	}
	if blocks, ok := asBlockList(content); ok {
		for _, item := range blocks {
			if t, _ := item["type"].(string); t == "tool_use" {
				name, _ := item["name"].(string)
				id, _ := item["id"].(string)
				input := item["input"]
				if input == nil {
					input = map[string]any{}
				}
				out = append(out, map[string]any{"name": name, "input": input, "toolUseId": id})
			}
		}
	}
	return out
}

// toolCallsToText renders tool calls as readable text (used when no tools defined).
func toolCallsToText(toolCalls []map[string]any) string {
	if len(toolCalls) == 0 {
		return ""
	}
	var parts []string
	for _, tc := range toolCalls {
		fn, _ := tc["function"].(map[string]any)
		name, _ := fn["name"].(string)
		if name == "" {
			name = "unknown"
		}
		arguments := stringifyArgs(fn["arguments"])
		id, _ := tc["id"].(string)
		if id != "" {
			parts = append(parts, "[Tool: "+name+" ("+id+")]\n"+arguments)
		} else {
			parts = append(parts, "[Tool: "+name+"]\n"+arguments)
		}
	}
	return strings.Join(parts, "\n\n")
}

func stringifyArgs(args any) string {
	switch v := args.(type) {
	case string:
		if v == "" {
			return "{}"
		}
		return v
	case nil:
		return "{}"
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "{}"
		}
		return string(b)
	}
}

// toolResultsToText renders tool results as readable text.
func toolResultsToText(toolResults []map[string]any) string {
	if len(toolResults) == 0 {
		return ""
	}
	var parts []string
	for _, tr := range toolResults {
		contentText := toolResultContentText(tr["content"])
		if contentText == "" {
			contentText = emptyToolResult
		}
		id, _ := tr["tool_use_id"].(string)
		if id != "" {
			parts = append(parts, "[Tool Result ("+id+")]\n"+contentText)
		} else {
			parts = append(parts, "[Tool Result]\n"+contentText)
		}
	}
	return strings.Join(parts, "\n\n")
}

// stripAllToolContent converts all tool content to text. Used when no tools defined.
func stripAllToolContent(messages []UnifiedMessage) []UnifiedMessage {
	if len(messages) == 0 {
		return messages
	}
	result := make([]UnifiedMessage, 0, len(messages))
	for _, msg := range messages {
		hasToolCalls := len(msg.ToolCalls) > 0
		hasToolResults := len(msg.ToolResults) > 0
		if !hasToolCalls && !hasToolResults {
			result = append(result, msg)
			continue
		}
		var contentParts []string
		if existing := extractTextContent(msg.Content); existing != "" {
			contentParts = append(contentParts, existing)
		}
		if hasToolCalls {
			if t := toolCallsToText(msg.ToolCalls); t != "" {
				contentParts = append(contentParts, t)
			}
		}
		if hasToolResults {
			if t := toolResultsToText(msg.ToolResults); t != "" {
				contentParts = append(contentParts, t)
			}
		}
		content := emptyPlaceholder
		if len(contentParts) > 0 {
			content = strings.Join(contentParts, "\n\n")
		}
		result = append(result, UnifiedMessage{Role: msg.Role, Content: content, Images: msg.Images})
	}
	return result
}

// ensureAssistantBeforeToolResults converts orphaned tool_results (no preceding
// assistant with tool_calls) to text. Mirrors the Python function.
func ensureAssistantBeforeToolResults(messages []UnifiedMessage) []UnifiedMessage {
	if len(messages) == 0 {
		return messages
	}
	result := make([]UnifiedMessage, 0, len(messages))
	for _, msg := range messages {
		if len(msg.ToolResults) > 0 {
			hasPreceding := len(result) > 0 &&
				result[len(result)-1].Role == "assistant" &&
				len(result[len(result)-1].ToolCalls) > 0
			if !hasPreceding {
				toolText := toolResultsToText(msg.ToolResults)
				original := extractTextContent(msg.Content)
				var newContent string
				switch {
				case original != "" && toolText != "":
					newContent = original + "\n\n" + toolText
				case toolText != "":
					newContent = toolText
				default:
					newContent = original
				}
				result = append(result, UnifiedMessage{
					Role:      msg.Role,
					Content:   newContent,
					ToolCalls: msg.ToolCalls,
					Images:    msg.Images,
				})
				continue
			}
		}
		result = append(result, msg)
	}
	return result
}

// mergeAdjacentMessages merges consecutive same-role messages.
func mergeAdjacentMessages(messages []UnifiedMessage) []UnifiedMessage {
	if len(messages) == 0 {
		return messages
	}
	merged := make([]UnifiedMessage, 0, len(messages))
	for _, msg := range messages {
		if len(merged) == 0 {
			merged = append(merged, msg)
			continue
		}
		last := &merged[len(merged)-1]
		if msg.Role != last.Role {
			merged = append(merged, msg)
			continue
		}
		// Merge content. To keep behavior simple and robust, fold to text.
		lastText := extractTextContent(last.Content)
		curText := extractTextContent(msg.Content)
		last.Content = lastText + "\n" + curText
		if msg.Role == "assistant" && len(msg.ToolCalls) > 0 {
			last.ToolCalls = append(last.ToolCalls, msg.ToolCalls...)
		}
		if msg.Role == "user" && len(msg.ToolResults) > 0 {
			last.ToolResults = append(last.ToolResults, msg.ToolResults...)
		}
		if len(msg.Images) > 0 {
			last.Images = append(last.Images, msg.Images...)
		}
	}
	return merged
}

// ensureFirstMessageIsUser prepends a synthetic user message if needed.
func ensureFirstMessageIsUser(messages []UnifiedMessage) []UnifiedMessage {
	if len(messages) == 0 {
		return messages
	}
	if messages[0].Role != "user" {
		synthetic := UnifiedMessage{Role: "user", Content: emptyPlaceholder}
		return append([]UnifiedMessage{synthetic}, messages...)
	}
	return messages
}

// normalizeMessageRoles maps unknown roles to "user".
func normalizeMessageRoles(messages []UnifiedMessage) []UnifiedMessage {
	if len(messages) == 0 {
		return messages
	}
	out := make([]UnifiedMessage, 0, len(messages))
	for _, msg := range messages {
		if msg.Role != "user" && msg.Role != "assistant" {
			msg.Role = "user"
		}
		out = append(out, msg)
	}
	return out
}

// ensureAlternatingRoles inserts synthetic assistant placeholders between
// consecutive user messages.
func ensureAlternatingRoles(messages []UnifiedMessage) []UnifiedMessage {
	if len(messages) < 2 {
		return messages
	}
	result := make([]UnifiedMessage, 0, len(messages))
	result = append(result, messages[0])
	for _, msg := range messages[1:] {
		prevRole := result[len(result)-1].Role
		if msg.Role == "user" && prevRole == "user" {
			result = append(result, UnifiedMessage{Role: "assistant", Content: emptyPlaceholder})
		}
		result = append(result, msg)
	}
	return result
}

// buildKiroHistory converts unified messages to the Kiro history array.
func buildKiroHistory(messages []UnifiedMessage, modelID string) []map[string]any {
	history := make([]map[string]any, 0, len(messages))
	for _, msg := range messages {
		switch msg.Role {
		case "user":
			content := extractTextContent(msg.Content)
			if content == "" {
				content = emptyPlaceholder
			}
			userInput := map[string]any{
				"content": content,
				"modelId": modelID,
				"origin":  "AI_EDITOR",
			}
			images := msg.Images
			if len(images) == 0 {
				images = extractImagesFromContent(msg.Content)
			}
			if kimg := convertImagesToKiroFormat(images); len(kimg) > 0 {
				userInput["images"] = kimg
			}
			ctx := map[string]any{}
			if len(msg.ToolResults) > 0 {
				if ktr := convertToolResultsToKiroFormat(msg.ToolResults); len(ktr) > 0 {
					ctx["toolResults"] = ktr
				}
			} else if tr := extractToolResultsFromContent(msg.Content); len(tr) > 0 {
				ctx["toolResults"] = tr
			}
			if len(ctx) > 0 {
				userInput["userInputMessageContext"] = ctx
			}
			history = append(history, map[string]any{"userInputMessage": userInput})
		case "assistant":
			content := extractTextContent(msg.Content)
			if content == "" {
				content = emptyPlaceholder
			}
			assistant := map[string]any{"content": content}
			if uses := extractToolUsesFromMessage(msg.Content, msg.ToolCalls); len(uses) > 0 {
				assistant["toolUses"] = uses
			}
			history = append(history, map[string]any{"assistantResponseMessage": assistant})
		}
	}
	return history
}

// BuildKiroPayload assembles the complete Kiro generateAssistantResponse payload
// from unified messages. Mirrors converters_core.build_kiro_payload.
//
// It returns the payload map (ready to JSON-encode) and the tool documentation
// string that was folded into the system prompt (for diagnostics).
func BuildKiroPayload(
	messages []UnifiedMessage,
	systemPrompt string,
	modelID string,
	tools []UnifiedTool,
	conversationID string,
	profileArn string,
) (map[string]any, string) {
	processedTools, toolDoc := processToolsWithLongDescriptions(tools)

	fullSystemPrompt := systemPrompt
	if toolDoc != "" {
		if fullSystemPrompt != "" {
			fullSystemPrompt += toolDoc
		} else {
			fullSystemPrompt = strings.TrimSpace(toolDoc)
		}
	}

	var pipeline []UnifiedMessage
	if len(tools) == 0 {
		pipeline = stripAllToolContent(messages)
	} else {
		pipeline = ensureAssistantBeforeToolResults(messages)
	}

	pipeline = mergeAdjacentMessages(pipeline)
	pipeline = ensureFirstMessageIsUser(pipeline)
	pipeline = normalizeMessageRoles(pipeline)
	pipeline = ensureAlternatingRoles(pipeline)

	if len(pipeline) == 0 {
		// Fallback: a single empty user message keeps the request valid.
		pipeline = []UnifiedMessage{{Role: "user", Content: emptyPlaceholder}}
	}

	var historyMessages []UnifiedMessage
	if len(pipeline) > 1 {
		historyMessages = pipeline[:len(pipeline)-1]
	}

	if fullSystemPrompt != "" && len(historyMessages) > 0 {
		first := &historyMessages[0]
		if first.Role == "user" {
			original := extractTextContent(first.Content)
			first.Content = fullSystemPrompt + "\n\n" + original
		}
	}

	history := buildKiroHistory(historyMessages, modelID)

	currentMessage := pipeline[len(pipeline)-1]
	currentContent := extractTextContent(currentMessage.Content)

	if fullSystemPrompt != "" && len(history) == 0 {
		currentContent = fullSystemPrompt + "\n\n" + currentContent
	}

	if currentMessage.Role == "assistant" {
		history = append(history, map[string]any{
			"assistantResponseMessage": map[string]any{"content": currentContent},
		})
		currentContent = emptyPlaceholder
	}

	if currentContent == "" {
		currentContent = emptyPlaceholder
	}

	images := currentMessage.Images
	if len(images) == 0 {
		images = extractImagesFromContent(currentMessage.Content)
	}
	kiroImages := convertImagesToKiroFormat(images)

	userInputContext := map[string]any{}
	if ktools := convertToolsToKiroFormat(processedTools); len(ktools) > 0 {
		userInputContext["tools"] = ktools
	}
	if len(currentMessage.ToolResults) > 0 {
		if ktr := convertToolResultsToKiroFormat(currentMessage.ToolResults); len(ktr) > 0 {
			userInputContext["toolResults"] = ktr
		}
	} else if tr := extractToolResultsFromContent(currentMessage.Content); len(tr) > 0 {
		userInputContext["toolResults"] = tr
	}

	userInputMessage := map[string]any{
		"content": currentContent,
		"modelId": modelID,
		"origin":  "AI_EDITOR",
	}
	if len(kiroImages) > 0 {
		userInputMessage["images"] = kiroImages
	}
	if len(userInputContext) > 0 {
		userInputMessage["userInputMessageContext"] = userInputContext
	}

	conversationState := map[string]any{
		"chatTriggerType": "MANUAL",
		"conversationId":  conversationID,
		"currentMessage": map[string]any{
			"userInputMessage": userInputMessage,
		},
	}
	if len(history) > 0 {
		conversationState["history"] = history
	}

	payload := map[string]any{"conversationState": conversationState}
	if profileArn != "" {
		payload["profileArn"] = profileArn
	}

	return payload, toolDoc
}
