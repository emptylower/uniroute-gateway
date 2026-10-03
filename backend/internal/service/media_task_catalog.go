package service

import (
	"fmt"
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
	prices         map[string]int64
	// fields maps the colon-separated segments of an option value onto
	// provider input fields; fixed holds provider inputs the browser cannot
	// choose. A nil fields keeps the legacy single-parameter mapping.
	fields []mediaField
	fixed  map[string]any
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
		mediaModel("ai-music-api/generate", "AI Music API Generate", "Suno", "music", "tags", 840, "pop, upbeat", "lofi, chill, mellow", "cinematic, orchestral, epic", "electronic, synth, dance", "acoustic, folk, warm", "jazz, smooth, brass"),
	}
	slugs := map[string]string{"nano-banana-2-lite": "nano-banana-2-lite", "google/imagen4-fast": "google-imagen4-fast", "google/nano-banana": "google-nano-banana", "nano-banana-pro": "nano-banana-pro", "bytedance/seedream": "bytedance-seedream", "flux-2/pro-text-to-image": "flux-2-pro-text-to-image", "bytedance/seedance-2-mini": "bytedance-seedance-2-mini", "grok-imagine/text-to-video": "grok-imagine-text-to-video", "hailuo/02-text-to-video-standard": "hailuo-02-text-to-video-standard", "veo-3-1": "veo-3-1", "ai-music-api/generate": "ai-music-api-generate"}
	for i := range models {
		if slug, ok := slugs[models[i].ModelID]; ok {
			models[i].Slug = slug
		}
	}

	models[5].prices["2K:1:1"] = 1200
	models[5].OptionPriceUSD["2K:1:1"] = "0.12"
	return append(models, extendedMediaModels()...)
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
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	Option    string `json:"option"`
	MediaType string `json:"media_type,omitempty"`
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
	if n := utf8.RuneCountInString(in.Prompt); n < 3 || n > 2000 {
		return in, m, 0, nil, infraerrors.BadRequest("INVALID_PROMPT", "prompt must contain 3 to 2000 characters")
	}
	if in.MediaType != "" && in.MediaType != m.MediaKind {
		return in, m, 0, nil, infraerrors.BadRequest("INVALID_MEDIA_TYPE", "model and media type disagree")
	}
	in.MediaType = m.MediaKind
	input := map[string]any{"prompt": in.Prompt}
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
		return in, m, units, input, nil
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
	return in, m, units, input, nil
}
