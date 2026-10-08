package service

// musicMediaModels lists the KIE audio models that take a bare text prompt:
// the Suno generations of the AI Music API and the Gemini speech models.
// Everything goes through /api/v1/jobs/createTask and was checked against live
// KIE jobs on 2026-10-03.
//
// ElevenLabs speech models (text-to-speech-multilingual-v2, -turbo-2-5 and
// text-to-dialogue-v3) are not listed: every createTask for them ended in
// "Internal Error" at KIE on 2026-10-03, so they would only sell failures.
// Add them once KIE serves them again.
//
// Prices are ten-thousandths of USD per track. Suno is flat per request at KIE
// (12 credits). Speech is billed by KIE per token, so retail is flat per
// request and the prompt is capped to bound cost: 1000 characters is about a
// minute of audio, roughly 1.6k output tokens.
func musicMediaModels() []MediaModel {
	suno := func(id, slug, name, version string) MediaModel {
		m := mediaSpecModel(id, slug, name, "Suno", "music", "tags",
			mediaFields("style"), map[string]any{"custom_mode": false, "instrumental": false, "model": version},
			mediaOpt("pop, upbeat", "Pop", 840),
			mediaOpt("lofi, chill, mellow", "Lo-fi", 840),
			mediaOpt("cinematic, orchestral, epic", "Cinematic", 840),
			mediaOpt("electronic, synth, dance", "Electronic", 840),
			mediaOpt("acoustic, folk, warm", "Acoustic", 840),
			mediaOpt("jazz, smooth, brass", "Jazz", 840),
		)
		m.upstream = "ai-music-api/generate"
		return m
	}
	v6 := suno("ai-music-api/generate", "ai-music-api-generate", "Suno V6", "V6")
	v6.upstream = ""
	return []MediaModel{
		v6,
		suno("ai-music-api/generate-v6-mini", "ai-music-api-generate-v6-mini", "Suno V6 Mini", "V6_MINI"),
		suno("ai-music-api/generate-v6-wild", "ai-music-api-generate-v6-wild", "Suno V6 Wild", "V6_WILD"),
		geminiSpeechModel("google/gemini-3-8-flash-lite-tts", "google-gemini-3-8-flash-lite-tts", "Gemini 3.8 Flash Lite Text to Speech", 120, geminiVoicesWide),
		geminiSpeechModel("google/gemini-3-8-flash-tts", "google-gemini-3-8-flash-tts", "Gemini 3.8 Flash Text to Speech", 200, geminiVoicesWide),
		geminiSpeechModel("google/gemini-3-1-flash-tts", "google-gemini-3-1-flash-tts", "Gemini 3.1 Flash Text to Speech", 400, geminiVoices),
		geminiSpeechModel("google/gemini-2-5-pro-tts", "google-gemini-2-5-pro-tts", "Gemini 2.5 Pro Text to Speech", 400, geminiVoices),
	}
}

const geminiSpeechPromptMax = 1000

// Voices accepted by every Gemini speech model; the 3.8 family also takes the
// larger voice set, whose default voice is Fola.
var (
	geminiVoices     = []string{"Kore", "Puck", "Charon", "Zephyr", "Fenrir", "Aoede"}
	geminiVoicesWide = []string{"Fola", "Kore", "Puck", "Zephyr", "Aoede", "Charon"}
)

func geminiSpeechModel(id, slug, name string, price int64, voices []string) MediaModel {
	options := make([]mediaOption, 0, len(voices))
	for _, voice := range voices {
		options = append(options, mediaOpt(voice, voice, price))
	}
	m := mediaSpecModel(id, slug, name, "Google", "music", "voice", nil, nil, options...)
	m.promptMax = geminiSpeechPromptMax
	m.build = func(prompt, voice string) map[string]any {
		return map[string]any{
			"speakers":       []any{map[string]any{"speaker_id": "Speaker 1", "voice_name": voice}},
			"dialogue_turns": []any{map[string]any{"speaker_id": "Speaker 1", "text": prompt}},
		}
	}
	return m
}
