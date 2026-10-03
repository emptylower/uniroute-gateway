//go:build unit

package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeminiMarketReferencesPreserveAuthoritativeQuote(t *testing.T) {
	var in MediaCreateInput
	require.NoError(t, json.Unmarshal([]byte(`{"model":"google/gemini-omni-flash-1-1","option":"720p:8","prompt":"A dog enters the scene","input":{"image_urls":["https://example.com/reference.png"],"aspect_ratio":"9:16","seed":23}}`), &in))
	_, model, units, input, err := normalizeMediaCreate(in)
	require.NoError(t, err)
	require.Equal(t, int64(63040000), units)
	require.Equal(t, "720p", input["resolution"])
	require.Equal(t, "8", input["duration"])
	require.Equal(t, "9:16", input["aspect_ratio"])
	require.Equal(t, int64(23), input["seed"])
	require.Equal(t, []any{"https://example.com/reference.png"}, input["image_urls"])
	require.Contains(t, model.InputFields, "image_urls")
}

func TestGeminiMarketInputRejectsUnpricedOrMalformedInputs(t *testing.T) {
	for _, raw := range []string{
		`{"duration":"10"}`, `{"resolution":"4k"}`, `{"n":100}`,
		`{"audio_ids":["audio"]}`, `{"character_ids":["character"]}`, `{"video_list":[{"url":"https://example.com/video.mp4"}]}`,
		`{"image_urls":["data:image/png;base64,a"]}`, `{"image_urls":["http://example.com/input.png"]}`,
		`{"last_frame_url":"https://example.com/last.png"}`,
		`{"first_frame_url":"https://example.com/first.png","image_urls":["https://example.com/image.png"]}`,
		`{"seed":-1}`, `{"seed":2147483648}`, `{"seed":1.5}`, `{"seed":"1"}`, `{"aspect_ratio":"1:1"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var input map[string]any
			require.NoError(t, json.Unmarshal([]byte(raw), &input))
			_, _, units, provider, err := normalizeMediaCreate(MediaCreateInput{Model: "google/gemini-omni-flash-1-1", Option: "720p:8", Prompt: "hello", Input: input})
			require.Error(t, err)
			require.Zero(t, units)
			require.Nil(t, provider)
		})
	}
	for _, option := range []string{"720p:6", "720p:10", "4k:8"} {
		_, _, _, _, err := normalizeMediaCreate(MediaCreateInput{Model: "google/gemini-omni-flash-1-1", Option: option, Prompt: "hello"})
		require.Error(t, err)
	}
}

func TestGeminiMarketFramesAndLongPrompt(t *testing.T) {
	in := MediaCreateInput{Model: "google/gemini-omni-flash-1-1", Option: "1080p:4", Prompt: strings.Repeat("x", 20000), Input: map[string]any{"first_frame_url": "https://example.com/first.png", "last_frame_url": "https://example.com/last.png"}}
	_, _, units, provider, err := normalizeMediaCreate(in)
	require.NoError(t, err)
	require.Equal(t, int64(31520000), units)
	require.Equal(t, in.Input["last_frame_url"], provider["last_frame_url"])
	in.Prompt += "x"
	_, _, _, _, err = normalizeMediaCreate(in)
	require.Error(t, err)
	in.Model, in.Option, in.Prompt = "google/imagen4-fast", "1:1", "hello"
	_, _, _, _, err = normalizeMediaCreate(in)
	require.Error(t, err, "reference inputs must not leak onto other model schemas")
}
