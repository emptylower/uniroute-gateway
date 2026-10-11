//go:build unit

package service

import (
	"context"
	"net/http"
	"strings"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

var suspendedTestModels = []string{
	"kling/ai-avatar-pro", "kling/ai-avatar-standard", "omnihuman-1-5",
	"omnihuman-1-5/human-identification", "omnihuman-1-5/subject-detection",
	"infinitalk/from-audio", "volcengine/video-to-video-lip-sync",
	"wan/2-2-a14b-speech-to-video-turbo", "wan/2-2-animate-replace",
}

func requireSuspendedModel(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, infraerrors.Code(err))
	require.Equal(t, ModelTemporarilyUnavailableCode, infraerrors.Reason(err))
}

func TestModelSuspensionNamedIDsSlugsAndAliases(t *testing.T) {
	for _, id := range suspendedTestModels {
		for _, model := range []string{id, strings.ReplaceAll(id, "/", "-"), " " + strings.ToUpper(id) + " ", "https://uniroute.app/zh/models/" + strings.ReplaceAll(id, "/", "-")} {
			t.Run(model, func(t *testing.T) {
				requireSuspendedModel(t, CheckModelAvailability(model))
				_, _, units, err := resolveMediaQuote(model, "")
				requireSuspendedModel(t, err)
				require.Zero(t, units)
				// No user, database, wallet or provider is needed to reject create.
				_, err = (&MediaTaskService{}).Create(context.Background(), 1, "existing-key", MediaCreateInput{Model: model})
				requireSuspendedModel(t, err)
				// A stale queued task cannot reach the provider or create a write.
				handle, _ := newAuthorizationHandle("enforce")
				id, rejected, err := (&kieMediaProvider{}).Create(context.Background(), model, nil, handle)
				requireSuspendedModel(t, err)
				require.True(t, rejected)
				require.Empty(t, id)
				require.Empty(t, handle.Writes())
			})
		}
	}
	for _, model := range []string{"kling-ai-avatar", "infinitalk", "wan-speech-to-video-turbo", "omnihuman-1.5", "wan/2.2-animate-replace", "kling%2Fai-avatar-pro"} {
		requireSuspendedModel(t, CheckModelAvailability(model))
	}
	for _, model := range []string{"kling/2.6/text-to-video", "wan/2-2-animate-move", "wan-2-2-animate", "google/nano-banana", "gpt-5", "unrelated/kling/ai-avatar-pro", "kling/ai-avatar-pro-extra"} {
		require.NoError(t, CheckModelAvailability(model), model)
	}
	quote, err := mediaQuote("google/nano-banana", "1:1")
	require.NoError(t, err)
	require.Equal(t, "0.0273", quote.QuotedUSD)
}

func TestModelSuspensionAccountAliasesBeforeBillingOrSend(t *testing.T) {
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"avatar-alias": "kling/ai-avatar-pro"}}}
	ctx := context.Background()
	_, err := (&GatewayService{}).FreezeBillingSnapshot(ctx, nil, account, nil, "avatar-alias", "avatar-alias", false, 0, 0)
	requireSuspendedModel(t, err)
	_, err = (&OpenAIGatewayService{}).FreezeBillingSnapshot(ctx, nil, account, nil, "safe-public", "avatar-alias", false)
	requireSuspendedModel(t, err)
	_, err = (&OpenAIGatewayService{}).ForwardImages(ctx, nil, account, nil, &OpenAIImagesRequest{Model: "safe-public"}, "avatar-alias")
	requireSuspendedModel(t, err)
	account.Platform = PlatformGrok
	_, err = (&OpenAIGatewayService{}).ForwardGrokMedia(ctx, nil, account, GrokMediaEndpointVideosGenerations, "", []byte(`{"model":"avatar-alias"}`), "application/json")
	requireSuspendedModel(t, err)
	// Billing off remains unchanged for allowed models.
	snap, err := (&GatewayService{}).FreezeBillingSnapshot(ctx, nil, account, nil, "gpt-5", "gpt-5", false, 0, 0)
	require.NoError(t, err)
	require.Nil(t, snap)
}
