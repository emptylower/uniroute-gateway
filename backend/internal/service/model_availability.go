package service

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
)

const ModelTemporarilyUnavailableCode = "MODEL_TEMPORARILY_UNAVAILABLE"
const ModelTemporarilyUnavailableMessage = "模型暂时下架 / Model temporarily unavailable."

var ErrModelTemporarilyUnavailable = infraerrors.New(http.StatusServiceUnavailable, ModelTemporarilyUnavailableCode, ModelTemporarilyUnavailableMessage)

// The explicit compliance suspension applies to model IDs and their public
// slugs. Other Kling and Wan models remain available.
func IsModelTemporarilyUnavailable(model string) bool {
	if strings.Contains(model, "://") {
		if parsed, err := url.Parse(model); err == nil {
			model = parsed.EscapedPath()
		}
	}
	if decoded, err := url.PathUnescape(model); err == nil {
		model = decoded
	}
	model = strings.Trim(strings.TrimSpace(model), "/")
	parts := strings.Split(model, "/")
	if len(parts) > 1 && (parts[0] == "en" || parts[0] == "zh" || strings.HasPrefix(parts[0], "en-") || strings.HasPrefix(parts[0], "zh-")) {
		parts = parts[1:]
	}
	if len(parts) > 1 && (parts[0] == "models" || parts[0] == "model" || parts[0] == "market") {
		model = strings.Join(parts[1:], "/")
	}
	model = strings.ReplaceAll(strings.ToLower(strings.TrimSpace(model)), "/", "-")
	model = strings.NewReplacer("1.5", "1-5", "2.2", "2-2").Replace(model)
	switch model {
	case "kling-ai-avatar-pro", "kling-ai-avatar-standard", "kling-ai-avatar",
		"omnihuman-1-5", "omnihuman-1-5-human-identification", "omnihuman-1-5-subject-detection",
		"infinitalk-from-audio", "infinitalk", "volcengine-video-to-video-lip-sync",
		"wan-2-2-a14b-speech-to-video-turbo", "wan-speech-to-video-turbo", "wan-2-2-animate-replace":
		return true
	default:
		return false
	}
}

func CheckModelAvailability(models ...string) error {
	for _, model := range models {
		if IsModelTemporarilyUnavailable(model) {
			return ErrModelTemporarilyUnavailable
		}
	}
	return nil
}

// CheckJSONModelAvailability inspects every top-level model occurrence without
// collapsing duplicate keys. Keys and string values are decoded so escaped
// forms cannot differ from the provider's JSON parser. Nested input/tool data
// is skipped as a complete value and is not a generation model selector.
func CheckJSONModelAvailability(body []byte) error {
	return checkJSONModelSelectors(body, nil, false, false)
}

// CheckWebSocketModelAvailability also checks session.model on session.update
// events, including repeated type/session keys and account model aliases.
func CheckWebSocketModelAvailability(body []byte, account *Account) error {
	return checkJSONModelSelectors(body, account, true, true)
}

// CheckLiveCallModelAvailability checks the session object in a Live create
// request. Other protocols' nested prompt/tool objects are not selectors.
func CheckLiveCallModelAvailability(body []byte) error {
	return checkJSONModelSelectors(body, nil, true, false)
}

func checkJSONModelSelectors(body []byte, account *Account, scanSessions, sessionUpdateOnly bool) error {
	// Multipart and binary bodies are not JSON model selectors. Avoid scanning
	// or normalizing their file bytes; keep the same leading-BOM behavior.
	prefix := bytes.TrimLeft(bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf}), " \t\r\n")
	if len(prefix) == 0 || prefix[0] != '{' {
		return nil
	}
	// Match the lenient HTTP request parser, while leaving the forwarded body
	// untouched. Six bytes per input byte bounds the normalizer's expansion;
	// the caller remains responsible for its existing request size limit.
	if normalized, err := pkghttputil.NormalizeLenientJSONRequestBody(body, int64(len(body))*6); err == nil {
		body = normalized
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil // Existing request parsers own malformed/unsupported JSON.
	}
	var sessions []json.RawMessage
	sessionUpdate := !sessionUpdateOnly
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			break
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			break
		}
		name, ok := key.(string)
		if !ok {
			continue
		}
		if scanSessions && strings.EqualFold(name, "session") {
			sessions = append(sessions, value)
			continue
		}
		if scanSessions && strings.EqualFold(name, "type") {
			var eventType string
			if json.Unmarshal(value, &eventType) == nil && strings.TrimSpace(eventType) == "session.update" {
				sessionUpdate = true
			}
			continue
		}
		if !strings.EqualFold(name, "model") {
			continue
		}
		var model string
		if json.Unmarshal(value, &model) == nil {
			if err := checkMappedModelAvailability(account, model); err != nil {
				return err
			}
		}
	}
	if sessionUpdate {
		for _, session := range sessions {
			if err := checkJSONModelSelectors(session, account, false, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkMappedModelAvailability(account *Account, models ...string) error {
	if err := CheckModelAvailability(models...); err != nil {
		return err
	}
	if account != nil {
		for _, model := range models {
			if err := CheckModelAvailability(account.GetMappedModel(model)); err != nil {
				return err
			}
		}
	}
	return nil
}
