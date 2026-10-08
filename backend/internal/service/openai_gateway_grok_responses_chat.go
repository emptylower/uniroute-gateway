package service

import (
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// An operator-selected CC transport cannot silently discard native Responses
// state or tools. Reject these requests before making any upstream call.
func validateGrokResponsesChatRequest(c *gin.Context, body []byte, req *apicompat.ResponsesRequest, tools []apicompat.ResponsesTool) error {
	unsupported := func(feature string) error {
		return fmt.Errorf("grok Chat Completions transport does not support %s", feature)
	}
	if isOpenAIResponsesCompactPath(c) {
		return unsupported("/responses/compact")
	}
	if strings.TrimSpace(req.PreviousResponseID) != "" {
		return unsupported("previous_response_id; send complete conversation input instead")
	}
	for _, field := range []string{"background", "store"} {
		value := gjson.GetBytes(body, field)
		if value.Exists() && value.Type != gjson.Null && value.Type != gjson.True && value.Type != gjson.False {
			return unsupported("non-boolean " + field)
		}
		if value.Bool() {
			return unsupported(field + "=true")
		}
	}
	if len(req.Include) > 0 {
		return unsupported("include; encrypted reasoning is unavailable on this transport")
	}
	if value := gjson.GetBytes(body, "truncation"); value.Exists() && value.Type != gjson.Null && value.String() != "disabled" {
		return unsupported("truncation")
	}
	for _, field := range []string{"conversation", "context_management", "max_tool_calls", "stream_options"} {
		if value := gjson.GetBytes(body, field); value.Exists() && value.Type != gjson.Null {
			return unsupported(field)
		}
	}
	for _, tool := range tools {
		switch tool.Type {
		case "function", "custom":
		case "tool_search":
			// Existing conversion exposes this as a client-executable search proxy.
		case "namespace":
			children := tool.Tools
			if len(children) == 0 {
				children = tool.Children
			}
			for _, child := range children {
				if child.Type != "function" {
					return unsupported("non-function namespace tools")
				}
			}
		default:
			return unsupported("tool type " + tool.Type)
		}
	}
	rawTools := gjson.GetBytes(body, "tools").Array()
	for _, item := range gjson.GetBytes(body, "input").Array() {
		if item.Get("type").String() == "additional_tools" {
			rawTools = append(rawTools, item.Get("tools").Array()...)
		}
	}
	for _, tool := range rawTools {
		if tool.Get("type").String() == "tool_search" && tool.Get("execution").Exists() && tool.Get("execution").String() != "client" {
			return unsupported("server-side tool_search")
		}
	}
	input := gjson.ParseBytes(req.Input)
	if input.Type == gjson.String || input.Type == gjson.Null {
		return nil
	}
	if !input.IsArray() {
		return unsupported("non-array input")
	}
	for _, item := range input.Array() {
		if item.Type == gjson.String || item.Type == gjson.Null {
			continue
		}
		if !item.IsObject() {
			return unsupported("non-object input item")
		}
		switch item.Get("type").String() {
		case "", "message":
			content := item.Get("content")
			if content.Type == gjson.String || content.Type == gjson.Null {
				continue
			}
			if !content.IsArray() {
				return unsupported("non-text message content")
			}
			for _, part := range content.Array() {
				switch part.Get("type").String() {
				case "input_text", "output_text", "text":
				default:
					return unsupported("content type " + part.Get("type").String())
				}
			}
		case "input_text", "text", "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output", "tool_search_call", "tool_search_output", "additional_tools":
		case "reasoning":
			if strings.TrimSpace(item.Get("encrypted_content").String()) != "" {
				return unsupported("encrypted reasoning history")
			}
			// The converter supports plain reasoning text and summaries, not hidden state.
			if !item.Get("summary").IsArray() && item.Get("text").String() == "" {
				return unsupported("reasoning history without text")
			}
		default:
			return unsupported("input item type " + item.Get("type").String())
		}
	}
	return nil
}
