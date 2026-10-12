package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestPublicMediaSignedModerationFailClosed(t *testing.T) {
	for _, tt := range []struct {
		name, body string
		status     int
		allow      bool
	}{{"allowed", `{"code":0,"data":{"allowed":true}}`, 200, true}, {"false", `{"code":0,"data":{"allowed":false}}`, 200, false}, {"empty", `{}`, 200, false}, {"blocked", `{"code":403}`, 403, false}, {"unavailable", `{}`, 503, false}, {"malformed", `garbage`, 200, false}} {
		t.Run(tt.name, func(t *testing.T) {
			secret := strings.Repeat("s", 32)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/internal/v2/media/moderation", r.URL.Path)
				token, err := jwt.Parse(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), func(token *jwt.Token) (any, error) { return []byte(secret), nil })
				require.NoError(t, err)
				require.True(t, token.Valid)
				claims, ok := token.Claims.(jwt.MapClaims)
				require.True(t, ok, "signed moderation claims must be MapClaims")
				require.Equal(t, "media:moderation", claims["scope"])
				require.Equal(t, "sub2api-gateway", claims["sub"])
				var body map[string]string
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				require.Equal(t, "real-user", body["user_id"])
				require.Equal(t, "image", body["media_type"])
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			client := newCanonicalWalletHTTPClient(config.CanonicalWalletConfig{ControlPlaneURL: server.URL, Issuer: "gateway", Audience: "console", Secret: secret, Version: "v1"}, server.Client())
			svc := &MediaTaskService{bridge: &CanonicalWalletBridge{control: client}}
			err := svc.screenMedia(context.Background(), "real-user", MediaCreateInput{Model: "nano-banana-2-lite", MediaType: "image", Prompt: "a red flower"})
			if tt.allow {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	svc := &MediaTaskService{}
	require.NoError(t, svc.screenMedia(context.Background(), "real-user", MediaCreateInput{MediaType: "music"}))
	require.Error(t, svc.screenMedia(context.Background(), "real-user", MediaCreateInput{MediaType: "video"}))
}
func TestPublicMediaModerationRespectsRequestCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer server.Close()
	client := newCanonicalWalletHTTPClient(config.CanonicalWalletConfig{ControlPlaneURL: server.URL, Secret: strings.Repeat("s", 32)}, server.Client())
	svc := &MediaTaskService{bridge: &CanonicalWalletBridge{control: client}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.Error(t, svc.screenMedia(ctx, "u", MediaCreateInput{MediaType: "image"}))
	require.Less(t, time.Since(start), time.Second)
}
