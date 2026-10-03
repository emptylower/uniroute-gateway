//go:build unit

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMusicCatalogListsSunoVersionsAndSpeech(t *testing.T) {
	byID := map[string]MediaModel{}
	slugs := map[string]bool{}
	for _, m := range MediaTaskCatalog() {
		byID[m.ModelID] = m
		if m.MediaKind == "music" {
			require.False(t, slugs[m.Slug], "duplicate slug %s", m.Slug)
			slugs[m.Slug] = true
			require.Equal(t, "per_track", m.Unit)
			require.NotEmpty(t, m.Param.Options)
		}
	}
	for _, id := range []string{
		"ai-music-api/generate", "ai-music-api/generate-v6-mini", "ai-music-api/generate-v6-wild",
		"google/gemini-3-8-flash-lite-tts", "google/gemini-3-8-flash-tts", "google/gemini-3-1-flash-tts", "google/gemini-2-5-pro-tts",
	} {
		require.Equal(t, "music", byID[id].MediaKind, id)
	}
}

func TestSunoVersionsSendCurrentProviderInput(t *testing.T) {
	for id, version := range map[string]string{
		"ai-music-api/generate":         "V6",
		"ai-music-api/generate-v6-mini": "V6_MINI",
		"ai-music-api/generate-v6-wild": "V6_WILD",
	} {
		_, _, _, input, err := normalizeMediaCreate(MediaCreateInput{Model: id, Option: "lofi, chill, mellow", Prompt: "a rainy afternoon"})
		require.NoError(t, err, id)
		require.Equal(t, map[string]any{"prompt": "a rainy afternoon", "style": "lofi, chill, mellow", "custom_mode": false, "instrumental": false, "model": version}, input, id)
		quote, err := mediaQuote(id, "lofi, chill, mellow")
		require.NoError(t, err)
		require.Equal(t, "0.084", quote.QuotedUSD)
	}
	// All three versions run on one provider model; the version is an input.
	require.Equal(t, "ai-music-api/generate", mediaUpstreamModel("ai-music-api/generate-v6-mini"))
	require.Equal(t, "ai-music-api/generate", mediaUpstreamModel("ai-music-api/generate-v6-wild"))
	require.Equal(t, "ai-music-api/generate", mediaUpstreamModel("ai-music-api/generate"))
	require.Equal(t, "veo-3-1", mediaUpstreamModel("veo-3-1"))
}

func TestGeminiSpeechNestsPromptAndBoundsLength(t *testing.T) {
	_, _, _, input, err := normalizeMediaCreate(MediaCreateInput{Model: "google/gemini-3-8-flash-lite-tts", Option: "Fola", Prompt: "Hello there."})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"speakers":       []any{map[string]any{"speaker_id": "Speaker 1", "voice_name": "Fola"}},
		"dialogue_turns": []any{map[string]any{"speaker_id": "Speaker 1", "text": "Hello there."}},
	}, input)

	long := make([]rune, geminiSpeechPromptMax+1)
	for i := range long {
		long[i] = 'a'
	}
	_, _, _, _, err = normalizeMediaCreate(MediaCreateInput{Model: "google/gemini-2-5-pro-tts", Option: "Kore", Prompt: string(long)})
	require.Error(t, err)
	_, _, _, _, err = normalizeMediaCreate(MediaCreateInput{Model: "google/gemini-2-5-pro-tts", Option: "Fola", Prompt: "Hello."})
	require.Error(t, err, "Fola is only offered by the 3.8 family")
}
