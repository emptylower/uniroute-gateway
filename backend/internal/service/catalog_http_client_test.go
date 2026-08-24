package service

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCatalogHTTPClientValidateURL(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "openrouter ok", url: "https://openrouter.ai/api/v1/models"},
		{name: "github api ok", url: "https://api.github.com/repos/foo/bar/contents/x"},
		{name: "github raw ok", url: "https://raw.githubusercontent.com/foo/bar/main/models.json"},
		{name: "explicit 443 ok", url: "https://openrouter.ai:443/api/v1/models"},
		{name: "case insensitive host ok", url: "https://OPENROUTER.AI/api/v1/models"},
		{name: "http scheme rejected", url: "http://openrouter.ai/api/v1/models", wantErr: true},
		{name: "userinfo rejected", url: "https://user:pass@openrouter.ai/api/v1/models", wantErr: true},
		{name: "ipv4 literal rejected", url: "https://127.0.0.1/api/v1/models", wantErr: true},
		{name: "ipv6 literal rejected", url: "https://[::1]/models.json", wantErr: true},
		{name: "public ipv4 literal rejected", url: "https://93.184.216.34/models.json", wantErr: true},
		{name: "non-allowlisted host rejected", url: "https://evil.example.com/models.json", wantErr: true},
		{name: "suffix spoof rejected", url: "https://openrouter.ai.evil.com/models.json", wantErr: true},
		{name: "prefix spoof rejected", url: "https://openrouter.ai.local/models.json", wantErr: true},
		{name: "nonstandard port rejected", url: "https://openrouter.ai:8443/models.json", wantErr: true},
		{name: "empty url rejected", url: "", wantErr: true},
		{name: "garbage url rejected", url: "://not a url", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCatalogURL(tc.url)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCatalogHTTPClientAllowlistedHosts(t *testing.T) {
	require.True(t, IsAllowedCatalogHost("openrouter.ai"))
	require.True(t, IsAllowedCatalogHost("models.dev"))
	require.True(t, IsAllowedCatalogHost("api.github.com"))
	require.True(t, IsAllowedCatalogHost("raw.githubusercontent.com"))
	require.False(t, IsAllowedCatalogHost(""))
	require.False(t, IsAllowedCatalogHost("example.com"))
}

func TestCatalogHTTPClientCheckRedirect(t *testing.T) {
	client := NewCatalogHTTPClient(time.Second, 1024)

	mustReq := func(raw string) *http.Request {
		req, err := http.NewRequest(http.MethodGet, raw, nil)
		require.NoError(t, err)
		return req
	}

	// Within budget and allowlisted target: allowed.
	err := client.checkRedirect(mustReq("https://raw.githubusercontent.com/a/b/main/c.json"), []*http.Request{
		mustReq("https://api.github.com/x"),
	})
	require.NoError(t, err)

	// Too many redirects: rejected.
	via := []*http.Request{
		mustReq("https://api.github.com/1"),
		mustReq("https://api.github.com/2"),
		mustReq("https://api.github.com/3"),
	}
	err = client.checkRedirect(mustReq("https://raw.githubusercontent.com/a/b/main/c.json"), via)
	require.Error(t, err, "more than 3 redirects must be rejected")

	// Redirect off the allowlist: rejected even within budget.
	err = client.checkRedirect(mustReq("https://evil.example.com/payload"), nil)
	require.Error(t, err)

	// Downgrade to http: rejected.
	err = client.checkRedirect(mustReq("http://openrouter.ai/api/v1/models"), nil)
	require.Error(t, err)
}

func TestCatalogHTTPClientDialControl(t *testing.T) {
	rejected := []string{
		"192.168.1.10:443",
		"127.0.0.1:443",
		"10.0.0.5:443",
		"172.16.0.9:443",
		"169.254.169.254:443",
		"[fd00::1]:443",
		"[::1]:443",
		"0.0.0.0:443",
		"224.0.0.1:443",
	}
	for _, addr := range rejected {
		require.Error(t, catalogDialControl("tcp", addr, nil), "private/reserved address %s must be rejected", addr)
	}

	allowed := []string{
		"93.184.216.34:443",
		"[2606:2800:220:1:248:1893:25c8:1946]:443",
	}
	for _, addr := range allowed {
		require.NoError(t, catalogDialControl("tcp", addr, nil), "public address %s must be allowed", addr)
	}
}

func TestCatalogHTTPClientReadBodyLimit(t *testing.T) {
	body := bytes.Repeat([]byte("a"), 1024)
	got, err := readCatalogBody(strings.NewReader(string(body)), 2048)
	require.NoError(t, err)
	require.Equal(t, len(body), len(got))

	_, err = readCatalogBody(strings.NewReader(string(body)), 512)
	require.Error(t, err, "payload over limit must be rejected")
}

func TestCatalogHTTPClientConstants(t *testing.T) {
	require.Equal(t, 20*time.Second, DefaultCatalogRequestTimeout)
	require.Equal(t, int64(32*1024*1024), DefaultCatalogMaxPayloadBytes)
	require.Equal(t, 50000, CatalogMaxItemsDefault)
	require.Equal(t, 3, maxCatalogRedirects)
}

func TestCatalogHTTPClientGetRejectsDisallowedURL(t *testing.T) {
	client := NewCatalogHTTPClient(time.Second, 1024)
	_, err := client.Get(context.Background(), "https://evil.example.com/models.json")
	require.Error(t, err)
}
