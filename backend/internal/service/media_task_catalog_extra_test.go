//go:build unit

package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// mediaKIECostTenThousandths is the KIE list price (USD, ten-thousandths per
// image or per video second) of every option of the extended catalog, read from
// KIE's /api/v1/models pricing text on 2026-10-03. Retail must stay above it.
var mediaKIECostTenThousandths = map[string]map[string]int64{
	"google/imagen4":                       {"1:1": 400, "16:9": 400, "9:16": 400},
	"google/imagen4-ultra":                 {"1:1": 600, "16:9": 600, "9:16": 600},
	"nano-banana-2":                        {"1K:1:1": 250, "1K:16:9": 250, "2K:1:1": 400, "2K:16:9": 400, "4K:1:1": 600, "4K:16:9": 600},
	"flux-2/flex-text-to-image":            {"1K:1:1": 700, "1K:16:9": 700, "1K:9:16": 700, "2K:1:1": 1200, "2K:16:9": 1200, "2K:9:16": 1200},
	"gpt-image/1.5-text-to-image":          {"medium:1:1": 200, "medium:3:2": 200, "medium:2:3": 200, "high:1:1": 1100, "high:3:2": 1100, "high:2:3": 1100},
	"gpt-image-2-5-flare-text-to-image":    {"1K:1:1": 150, "1K:16:9": 150, "2K:1:1": 500, "2K:16:9": 500, "4K:1:1": 800, "4K:16:9": 800},
	"gpt-image-2-5-sunburst-text-to-image": {"1K:1:1": 150, "1K:16:9": 150, "2K:1:1": 500, "2K:16:9": 500, "4K:1:1": 800, "4K:16:9": 800},
	"gpt-image-2-text-to-image":            {"1K:1:1": 150, "1K:16:9": 150, "2K:1:1": 250, "2K:16:9": 250, "4K:1:1": 400, "4K:16:9": 400},
	"grok-imagine-image-2-0/text-to-image": {"1:1": 200, "16:9": 200, "9:16": 200, "2:3": 200, "3:2": 200},
	"grok-imagine/text-to-image":           {"1:1": 200, "16:9": 200, "9:16": 200, "2:3": 200, "3:2": 200},
	"ideogram/v3-text-to-image":            {"square_hd": 175, "landscape_16_9": 175, "portrait_16_9": 175},
	"qwen2-1/text-to-image":                {"1K:1:1": 200, "1K:16:9": 200, "1K:9:16": 200, "2K:1:1": 400, "2K:16:9": 400, "2K:9:16": 400},
	"qwen3/text-to-image":                  {"1:1": 240, "16:9": 240, "9:16": 240},
	"qwen3/pro-text-to-image":              {"1K:1:1": 320, "1K:16:9": 320, "1K:9:16": 320, "2K:1:1": 600, "2K:16:9": 600, "2K:9:16": 600},
	"qwen/text-to-image":                   {"square_hd": 200, "landscape_16_9": 200, "portrait_16_9": 200},
	"seedream/4.5-text-to-image":           {"1:1": 325, "16:9": 325, "9:16": 325},
	"seedream/5-lite-text-to-image":        {"1:1": 275, "16:9": 275, "9:16": 275},
	"seedream/5-pro-text-to-image":         {"basic:1:1": 350, "basic:16:9": 350, "basic:9:16": 350, "high:1:1": 700, "high:16:9": 700, "high:9:16": 700},
	"seedream/5-flash-text-to-image":       {"1:1": 162, "16:9": 162, "9:16": 162},
	"bytedance/seedream-v4-text-to-image":  {"square_hd": 250, "landscape_16_9": 250, "portrait_16_9": 250},
	"wan/2-7-image":                        {"1:1": 240, "16:9": 240, "9:16": 240},
	"wan/2-7-image-pro":                    {"1:1": 600, "16:9": 600, "9:16": 600},
	"z-image":                              {"1:1": 40, "16:9": 40, "9:16": 40},
	"bytedance/v1-pro-text-to-video":       {"480p:5": 140, "480p:10": 140, "720p:5": 300, "720p:10": 300, "1080p:5": 700, "1080p:10": 700},
	"bytedance/seedance-2-5":               {"480p:5": 1400, "480p:10": 1400, "720p:5": 3150, "720p:10": 3150, "1080p:5": 7900, "1080p:10": 7900},
	"bytedance/seedance-2-fast":            {"480p:5": 590, "480p:10": 590, "720p:5": 1240, "720p:10": 1240},
	"bytedance/seedance-2":                 {"480p:5": 950, "480p:10": 950, "720p:5": 2050, "720p:10": 2050, "1080p:5": 5100, "1080p:10": 5100},
	"gemini-omni-video":                    {"720p:4": 562, "720p:8": 562, "1080p:4": 562, "1080p:8": 562},
	"google/gemini-omni-flash-1-1":         {"720p:4": 562, "720p:8": 562, "1080p:4": 562, "1080p:8": 562},
	"hailuo/02-text-to-video-pro":          {"6": 475},
	"happyhorse-1-1/text-to-video":         {"720p:5": 1125, "720p:10": 1125, "1080p:5": 1450, "1080p:10": 1450},
	"happyhorse/text-to-video":             {"720p:5": 1400, "720p:10": 1400, "1080p:5": 2400, "1080p:10": 2400},
	"kling-3.0/video":                      {"std:5": 700, "std:10": 700, "pro:5": 900, "pro:10": 900},
	"kling-2.6/text-to-video":              {"5": 560, "10": 560},
	"kling/v2-1-master-text-to-video":      {"5": 1600, "10": 1600},
	"kling/v2-5-turbo-text-to-video-pro":   {"5": 420, "10": 420},
	"kling-3.0-omni/text-to-video":         {"720p:5": 700, "720p:10": 700, "1080p:5": 900, "1080p:10": 900},
	"kling/v3-turbo-text-to-video":         {"720p:5": 900, "720p:10": 900, "1080p:5": 1125, "1080p:10": 1125},
	"minimax-h3/text-to-video":             {"768P:5": 400, "768P:10": 400, "2K:5": 650, "2K:10": 650},
	"pixverse-v6/text-to-video":            {"360p:5": 200, "360p:10": 200, "540p:5": 280, "540p:10": 280, "720p:5": 360, "720p:10": 360, "1080p:5": 720, "1080p:10": 720},
	"wan/2-5-text-to-video":                {"720p:5": 600, "720p:10": 600, "1080p:5": 1000, "1080p:10": 1000},
	"wan/2-6-text-to-video":                {"720p:5": 700, "720p:10": 700, "1080p:5": 1045, "1080p:10": 1045},
	"wan/2-7-text-to-video":                {"720p:5": 800, "720p:10": 800, "1080p:5": 1200, "1080p:10": 1200},
	"wan/3-0-video":                        {"480P:5": 400, "480P:10": 400, "720P:5": 800, "720P:10": 800, "1080P:5": 1600, "1080P:10": 1600},
	"wan/3-0-video-prime":                  {"480P:5": 612, "480P:10": 612, "720P:5": 1260, "720P:10": 1260, "1080P:5": 2520, "1080P:10": 2520},
}

func TestExtendedMediaCatalogShape(t *testing.T) {
	extended := extendedMediaModels()
	require.Len(t, extended, len(mediaKIECostTenThousandths))
	ids, slugs := map[string]bool{}, map[string]bool{}
	for _, m := range MediaTaskCatalog() {
		require.False(t, ids[m.ModelID], "duplicate model %s", m.ModelID)
		require.False(t, slugs[m.Slug], "duplicate slug %s", m.Slug)
		ids[m.ModelID], slugs[m.Slug] = true, true
	}
	for _, m := range extended {
		t.Run(m.ModelID, func(t *testing.T) {
			require.Contains(t, []string{"image", "video"}, m.MediaKind)
			require.Contains(t, []string{"aspect_ratio", "duration", "spec"}, m.Param.Kind)
			require.NotEmpty(t, m.Param.Options)
			costs := mediaKIECostTenThousandths[m.ModelID]
			require.Len(t, costs, len(m.Param.Options))
			seen := map[string]bool{}
			for _, option := range m.Param.Options {
				require.False(t, seen[option.Value], "duplicate option %s", option.Value)
				seen[option.Value] = true
				require.NotEmpty(t, option.Label)
				if len(m.fields) > 1 || (len(m.fields) == 1 && m.Param.Kind == "spec") {
					require.Len(t, strings.SplitN(option.Value, ":", len(m.fields)), len(m.fields))
				}
				unitPrice := m.prices[option.Value]
				cost, ok := costs[option.Value]
				require.True(t, ok, "no KIE cost for %s", option.Value)
				require.Greater(t, unitPrice, cost, "option %s must sell above KIE cost", option.Value)

				quote, err := mediaQuote(m.ModelID, option.Value)
				require.NoError(t, err)
				quantity := int64(1)
				if m.MediaKind == "video" {
					quantity = mediaOptionSeconds(option.Value)
					require.Greater(t, quantity, int64(0))
				}
				require.Equal(t, mediaUSD(unitPrice*quantity*(mediaUnitsPerUSD/10000)), quote.QuotedUSD)
				require.Equal(t, mediaUSD(unitPrice*(mediaUnitsPerUSD/10000)), m.OptionPriceUSD[option.Value])

				_, _, units, input, err := normalizeMediaCreate(MediaCreateInput{Model: m.ModelID, Option: option.Value, Prompt: "a colorful garden"})
				require.NoError(t, err)
				require.Equal(t, unitPrice*quantity*(mediaUnitsPerUSD/10000), units)
				require.Equal(t, "a colorful garden", input["prompt"])
				for key := range m.fixed {
					require.Contains(t, input, key)
				}
				for _, field := range m.fields {
					require.Contains(t, input, field.name)
				}
				raw, err := json.Marshal(input)
				require.NoError(t, err)
				require.NotContains(t, string(raw), "null")
			}
		})
	}
}

func TestExtendedMediaCatalogProviderInputs(t *testing.T) {
	cases := []struct {
		model, option string
		price         string
		input         map[string]any
	}{
		{"kling-3.0/video", "pro:5", "0.56", map[string]any{"prompt": "hello", "mode": "pro", "duration": "5", "sound": false, "multi_shots": false, "multi_prompt": []any{}, "aspect_ratio": "16:9"}},
		{"gpt-image/1.5-text-to-image", "high:3:2", "0.154", map[string]any{"prompt": "hello", "quality": "high", "aspect_ratio": "3:2"}},
		{"pixverse-v6/text-to-video", "540p:10", "0.392", map[string]any{"prompt": "hello", "quality": "540p", "duration": 10, "aspect_ratio": "16:9", "generate_audio_switch": false, "generate_multi_clip_switch": false}},
		{"ideogram/v3-text-to-image", "landscape_16_9", "0.0245", map[string]any{"prompt": "hello", "image_size": "landscape_16_9", "rendering_speed": "TURBO"}},
		{"hailuo/02-text-to-video-pro", "6", "0.3432", map[string]any{"prompt": "hello"}},
		{"wan/2-7-image", "1:1", "0.0336", map[string]any{"prompt": "hello", "aspect_ratio": "1:1", "n": 1, "resolution": "2K"}},
		{"wan/3-0-video", "1080P:10", "1.65", map[string]any{"prompt": "hello", "resolution": "1080P", "duration": 10, "aspect_ratio": "16:9", "audio": false}},
	}
	for _, c := range cases {
		t.Run(c.model+"/"+c.option, func(t *testing.T) {
			quote, err := mediaQuote(c.model, c.option)
			require.NoError(t, err)
			require.Equal(t, c.price, quote.QuotedUSD)
			_, _, _, input, err := normalizeMediaCreate(MediaCreateInput{Model: c.model, Option: c.option, Prompt: "hello"})
			require.NoError(t, err)
			require.Equal(t, c.input, input)
		})
	}
}

func TestExtendedMediaCatalogRejectsBadOptions(t *testing.T) {
	for _, c := range [][2]string{{"kling-3.0/video", "pro:99"}, {"kling-3.0/video", "pro"}, {"pixverse-v6/text-to-video", "540p:x"}, {"z-image", "4:3"}, {"hailuo/02-text-to-video-pro", "10"}} {
		_, err := mediaQuote(c[0], c[1])
		require.Error(t, err, "%v", c)
	}
	require.Equal(t, int64(10), mediaOptionSeconds("720p:10"))
	require.Equal(t, int64(6), mediaOptionSeconds("6"))
	require.Equal(t, int64(0), mediaOptionSeconds("720p"))
}
