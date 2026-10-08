package service

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const mediaPricingVersion = "kie-playground-v1"
const mediaUnitsPerUSD int64 = 100000000

type MediaParamOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}
type MediaParam struct {
	Kind        string             `json:"kind"`
	Options     []MediaParamOption `json:"options"`
	OptionTiers map[string]string  `json:"optionTiers,omitempty"`
}
type MediaModel struct {
	Slug           string            `json:"slug"`
	ModelID        string            `json:"modelId"`
	Name           string            `json:"name"`
	Provider       string            `json:"provider"`
	MediaKind      string            `json:"mediaKind"`
	Unit           string            `json:"unit"`
	PriceUSD       string            `json:"price_usd"`
	OptionPriceUSD map[string]string `json:"option_price_usd"`
	Param          MediaParam        `json:"param"`
	InputFields    []string          `json:"input_fields,omitempty"`
	prices         map[string]int64
	// fields maps the colon-separated segments of an option value onto
	// provider input fields; fixed holds provider inputs the browser cannot
	// choose. A nil fields keeps the legacy single-parameter mapping.
	fields []mediaField
	fixed  map[string]any
	// upstream is the provider model id when several catalog entries share one
	// provider model (Suno versions differ only by a fixed input field).
	upstream string
	// promptMax caps the prompt length of models whose price is flat per
	// request but whose provider cost grows with input length (speech).
	promptMax int
	// build replaces the generic option mapping for provider inputs that nest
	// the prompt (speech models); it returns the complete provider input.
	build func(prompt, option string) map[string]any
}
type mediaField struct {
	name    string
	integer bool
}
type mediaOption struct {
	value, label string
	price        int64
}

// Prices are exact ten-thousandths of USD per unit. Only verified provider
// inputs are exposed; browser fields cannot change model price or quantity.
func mediaModel(id, name, provider, kind, param string, price int64, options ...string) MediaModel {
	m := MediaModel{Slug: strings.ReplaceAll(id, "/", "-"), ModelID: id, Name: name, Provider: provider, MediaKind: kind, Param: MediaParam{Kind: param}, prices: map[string]int64{}, OptionPriceUSD: map[string]string{}}
	m.Unit = "per_image"
	if kind == "video" {
		m.Unit = "per_second"
	}
	if kind == "music" {
		m.Unit = "per_track"
	}
	m.PriceUSD = mediaUSD(price * (mediaUnitsPerUSD / 10000))
	for _, option := range options {
		label := option
		if param == "duration" {
			label += "s"
		}
		m.Param.Options = append(m.Param.Options, MediaParamOption{option, label})
		m.prices[option] = price
		m.OptionPriceUSD[option] = mediaUSD(price * (mediaUnitsPerUSD / 10000))
	}
	return m
}

func MediaTaskCatalog() []MediaModel {
	models := []MediaModel{
		mediaModel("nano-banana-2-lite", "Nano Banana 2 Lite", "Google", "image", "aspect_ratio", 273, "1:1", "16:9", "9:16"),
		mediaModel("google/imagen4-fast", "Google Imagen 4 Fast", "Google", "image", "aspect_ratio", 200, "1:1", "16:9"),
		mediaModel("google/nano-banana", "Google Nano Banana", "Google", "image", "aspect_ratio", 273, "1:1"),
		mediaModel("nano-banana-pro", "Nano Banana Pro", "Google", "image", "aspect_ratio", 938, "1:1"),
		mediaModel("bytedance/seedream", "ByteDance Seedream", "ByteDance", "image", "size", 245, "1024x1024", "1280x720"),
		mediaModel("flux-2/pro-text-to-image", "Flux 2 Pro Text to Image", "Black Forest Labs", "image", "resolution", 300, "1K:16:9", "2K:1:1"),
		mediaModel("bytedance/seedance-2-mini", "ByteDance Seedance 2 Mini", "ByteDance", "video", "duration", 215, "5", "10"),
		mediaModel("grok-imagine/text-to-video", "Grok Imagine Text to Video", "Grok", "video", "duration", 350, "6"),
		mediaModel("hailuo/02-text-to-video-standard", "Hailuo 02 Text to Video Standard", "Hailuo", "video", "duration", 327, "6"),
		mediaModel("veo-3-1", "Veo 3.1", "Google", "video", "duration", 350, "4", "6", "8"),
	}
	slugs := map[string]string{"nano-banana-2-lite": "nano-banana-2-lite", "google/imagen4-fast": "google-imagen4-fast", "google/nano-banana": "google-nano-banana", "nano-banana-pro": "nano-banana-pro", "bytedance/seedream": "bytedance-seedream", "flux-2/pro-text-to-image": "flux-2-pro-text-to-image", "bytedance/seedance-2-mini": "bytedance-seedance-2-mini", "grok-imagine/text-to-video": "grok-imagine-text-to-video", "hailuo/02-text-to-video-standard": "hailuo-02-text-to-video-standard", "veo-3-1": "veo-3-1"}
	for i := range models {
		if slug, ok := slugs[models[i].ModelID]; ok {
			models[i].Slug = slug
		}
	}

	models[5].prices["2K:1:1"] = 1200
	models[5].OptionPriceUSD["2K:1:1"] = "0.12"
	models = append(models, musicMediaModels()...)
	models = append(models, extendedMediaModels()...)
	for i := range models {
		if models[i].ModelID == "google/gemini-omni-flash-1-1" {
			models[i].InputFields = []string{"first_frame_url", "last_frame_url", "image_urls", "aspect_ratio", "seed"}
		}
	}
	return models
}

// mediaOptionSeconds reads the billable duration from a video option, which is
// either a bare duration ("6") or ends with one ("720p:10").
func mediaOptionSeconds(option string) int64 {
	if i := strings.LastIndex(option, ":"); i >= 0 {
		option = option[i+1:]
	}
	n, _ := strconv.ParseInt(option, 10, 64)
	return n
}

// mediaFields parses "name" (string) and "name:int" (integer) field specs.
func mediaFields(specs ...string) []mediaField {
	out := make([]mediaField, 0, len(specs))
	for _, spec := range specs {
		name, kind, _ := strings.Cut(spec, ":")
		out = append(out, mediaField{name: name, integer: kind == "int"})
	}
	return out
}

func mediaOpt(value, label string, price int64) mediaOption {
	return mediaOption{value: value, label: label, price: price}
}

// mediaSpecModel builds a model whose options map onto provider input fields
// and may each carry their own price. Prices are ten-thousandths of USD per
// unit (per image, per video second or per track).
func mediaSpecModel(id, slug, name, provider, kind, param string, fields []mediaField, fixed map[string]any, options ...mediaOption) MediaModel {
	m := MediaModel{Slug: slug, ModelID: id, Name: name, Provider: provider, MediaKind: kind, Param: MediaParam{Kind: param}, prices: map[string]int64{}, OptionPriceUSD: map[string]string{}, fields: fields, fixed: fixed}
	m.Unit = "per_image"
	if kind == "video" {
		m.Unit = "per_second"
	}
	if kind == "music" {
		m.Unit = "per_track"
	}
	lowest := options[0].price
	for _, option := range options {
		m.Param.Options = append(m.Param.Options, MediaParamOption{option.value, option.label})
		m.prices[option.value] = option.price
		m.OptionPriceUSD[option.value] = mediaUSD(option.price * (mediaUnitsPerUSD / 10000))
		if option.price < lowest {
			lowest = option.price
		}
	}
	m.PriceUSD = mediaUSD(lowest * (mediaUnitsPerUSD / 10000))
	return m
}

type MediaCreateInput struct {
	Model     string         `json:"model"`
	Prompt    string         `json:"prompt"`
	Option    string         `json:"option"`
	MediaType string         `json:"media_type,omitempty"`
	Input     map[string]any `json:"input,omitempty"`
}
type MediaQuote struct {
	Model         string `json:"model"`
	Option        string `json:"option"`
	Currency      string `json:"currency"`
	QuotedUSD     string `json:"quoted_usd"`
	PolicyVersion string `json:"policy_version"`
}

func resolveMediaQuote(model, option string) (MediaModel, string, int64, error) {
	for _, m := range MediaTaskCatalog() {
		if m.ModelID != strings.TrimSpace(model) {
			continue
		}
		if option == "" {
			option = m.Param.Options[0].Value
		}
		price, ok := m.prices[option]
		if !ok {
			return MediaModel{}, "", 0, infraerrors.BadRequest("INVALID_OPTION", "invalid media option")
		}
		quantity := int64(1)
		if m.MediaKind == "video" {
			quantity = mediaOptionSeconds(option)
			if quantity <= 0 {
				return MediaModel{}, "", 0, infraerrors.BadRequest("INVALID_OPTION", "invalid media option")
			}
		}
		return m, option, price * quantity * (mediaUnitsPerUSD / 10000), nil
	}
	return MediaModel{}, "", 0, infraerrors.BadRequest("UNKNOWN_MODEL", "unknown media model")
}
func mediaUSD(units int64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%d.%08d", units/mediaUnitsPerUSD, units%mediaUnitsPerUSD), "0"), ".")
}
func mediaQuote(model, option string) (MediaQuote, error) {
	m, o, units, err := resolveMediaQuote(model, option)
	if err != nil {
		return MediaQuote{}, err
	}
	return MediaQuote{m.ModelID, o, "USD", mediaUSD(units), "usd-wallet-v1"}, nil
}
func normalizeMediaCreate(in MediaCreateInput) (MediaCreateInput, MediaModel, int64, map[string]any, error) {
	m, option, units, err := resolveMediaQuote(in.Model, in.Option)
	if err != nil {
		return in, m, 0, nil, err
	}
	in.Model = m.ModelID
	in.Option = option
	in.Prompt = strings.TrimSpace(in.Prompt)
	minPrompt, maxPrompt := 3, 2000
	if m.ModelID == "google/gemini-omni-flash-1-1" {
		minPrompt, maxPrompt = 1, 20000
	}
	if m.promptMax > 0 {
		maxPrompt = m.promptMax
	}
	if n := utf8.RuneCountInString(in.Prompt); n < minPrompt || n > maxPrompt {
		return in, m, 0, nil, infraerrors.BadRequest("INVALID_PROMPT", fmt.Sprintf("prompt must contain %d to %d characters", minPrompt, maxPrompt))
	}
	if in.MediaType != "" && in.MediaType != m.MediaKind {
		return in, m, 0, nil, infraerrors.BadRequest("INVALID_MEDIA_TYPE", "model and media type disagree")
	}
	in.MediaType = m.MediaKind
	input := map[string]any{"prompt": in.Prompt}
	if m.build != nil {
		input = m.build(in.Prompt, option)
		for k, v := range m.fixed {
			input[k] = v
		}
		return finishMediaInput(in, m, units, input)
	}
	for k, v := range m.fixed {
		input[k] = v
	}
	if m.fields != nil {
		parts := strings.SplitN(option, ":", len(m.fields))
		if len(parts) != len(m.fields) {
			return in, m, 0, nil, infraerrors.BadRequest("INVALID_OPTION", "invalid media option")
		}
		for i, field := range m.fields {
			if field.integer {
				n, err := strconv.Atoi(parts[i])
				if err != nil {
					return in, m, 0, nil, infraerrors.BadRequest("INVALID_OPTION", "invalid media option")
				}
				input[field.name] = n
			} else {
				input[field.name] = parts[i]
			}
		}
		return finishMediaInput(in, m, units, input)
	}
	switch m.Param.Kind {
	case "duration":
		input["duration"], _ = strconv.Atoi(option)
	case "resolution":
		bits := strings.SplitN(option, ":", 2)
		input["resolution"] = bits[0]
		input["aspect_ratio"] = bits[1]
	default:
		input[m.Param.Kind] = option
	}
	return finishMediaInput(in, m, units, input)
}

// Additional form fields are model-specific. The priced option remains the
// sole source of duration, resolution, quantity and all fixed billing flags.
func finishMediaInput(in MediaCreateInput, m MediaModel, units int64, input map[string]any) (MediaCreateInput, MediaModel, int64, map[string]any, error) {
	invalid := func(message string) (MediaCreateInput, MediaModel, int64, map[string]any, error) {
		return in, m, 0, nil, infraerrors.BadRequest("INVALID_MEDIA_INPUT", message)
	}
	allowed := make(map[string]bool, len(m.InputFields))
	for _, name := range m.InputFields {
		allowed[name] = true
	}
	for name, value := range in.Input {
		if !allowed[name] {
			return invalid("unsupported model input: " + name)
		}
		switch name {
		case "first_frame_url", "last_frame_url":
			url, ok := value.(string)
			if !ok || !validMediaURL(url) || len(url) > 4096 {
				return invalid("frame URL must be a valid HTTPS URL")
			}
			input[name] = url
		case "image_urls":
			urls, ok := value.([]any)
			if !ok || len(urls) > 7 {
				return invalid("image_urls must contain at most 7 HTTPS URLs")
			}
			for _, raw := range urls {
				url, ok := raw.(string)
				if !ok || !validMediaURL(url) || len(url) > 4096 {
					return invalid("image_urls must contain valid HTTPS URLs")
				}
			}
			input[name] = urls
		case "aspect_ratio":
			if value != "16:9" && value != "9:16" {
				return invalid("aspect_ratio must be 16:9 or 9:16")
			}
			input[name] = value
		case "seed":
			n, ok := value.(float64)
			if !ok || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 2147483647 || n != math.Trunc(n) {
				return invalid("seed must be an integer from 0 to 2147483647")
			}
			input[name] = int64(n)
		}
	}
	first, _ := input["first_frame_url"].(string)
	last, _ := input["last_frame_url"].(string)
	urls, _ := input["image_urls"].([]any)
	if last != "" && first == "" {
		return invalid("last_frame_url requires first_frame_url")
	}
	if first != "" && len(urls) > 0 {
		return invalid("frame input and image_urls cannot be combined")
	}
	return in, m, units, input, nil
}

// mediaUpstreamModel maps a catalog id onto the provider model it runs on.
func mediaUpstreamModel(model string) string {
	for _, m := range MediaTaskCatalog() {
		if m.ModelID == model && m.upstream != "" {
			return m.upstream
		}
	}
	return model
}
