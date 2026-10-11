package service

import (
	"net/http"
	"net/url"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
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
