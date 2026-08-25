package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeOpenAICompatiblePlatform_VendorPassThrough(t *testing.T) {
	for _, p := range []string{PlatformDeepseek, PlatformGLM, PlatformKimi, PlatformQwen, PlatformLongcat, PlatformBytedance, PlatformMinimax, PlatformGrok} {
		require.Equal(t, p, normalizeOpenAICompatiblePlatform(p), "platform=%s", p)
	}
	require.Equal(t, PlatformOpenAI, normalizeOpenAICompatiblePlatform(PlatformOpenAI))
	require.Equal(t, PlatformOpenAI, normalizeOpenAICompatiblePlatform(PlatformAnthropic))
	require.Equal(t, PlatformOpenAI, normalizeOpenAICompatiblePlatform(""))
}

func TestIsOpenAICompatible_VendorPlatforms(t *testing.T) {
	for _, p := range []string{PlatformOpenAI, PlatformGrok, PlatformDeepseek, PlatformGLM, PlatformKimi, PlatformQwen, PlatformLongcat, PlatformBytedance, PlatformMinimax} {
		acc := &Account{Platform: p}
		require.True(t, acc.IsOpenAICompatible(), "platform=%s", p)
	}
	require.False(t, (&Account{Platform: PlatformAnthropic}).IsOpenAICompatible())
	require.False(t, (&Account{Platform: PlatformGemini}).IsOpenAICompatible())
}
