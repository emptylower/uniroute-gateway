//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

// transportUpstream is an HTTPUpstream that sends through a real transport, so the
// httptrace hooks the decorator installs actually fire. Only the decorator test uses it.
type transportUpstream struct {
	transport http.RoundTripper
	calls     int
	lastCtx   context.Context
}

func (u *transportUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls++
	u.lastCtx = req.Context()
	return u.transport.RoundTrip(req)
}

func (u *transportUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, accountConcurrency)
}

func modeFn(mode string) func() string { return func() string { return mode } }

func newRequest(t *testing.T, ctx context.Context, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(`{"input":"x"}`))
	require.NoError(t, err)
	return req
}

func TestAuthorizingUpstreamDisabledModeIsAPassThrough(t *testing.T) {
	resetAuthorizationMetricsForTest()
	inner := &transportUpstream{transport: http.DefaultTransport}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeDisabled))
	ctx := context.Background()
	req := newRequest(t, ctx, srv.URL)
	resp, err := dec.Do(req, "", 1, 1)
	require.NoError(t, err)
	_ = resp.Body.Close()
	// context.Background() is a struct value, not a pointer — require.Same would fail its
	// pointer precondition (testify v1.11.1). Interface equality is the right test: the
	// decorator's mode check precedes any req.Context() read, so the value is identical.
	require.True(t, ctx == inner.lastCtx, "disabled mode must not touch the request context")
	require.Equal(t, int64(0), AuthorizationMetricsSnapshot().WritesUnmarked, "disabled mode does not classify")
}

func TestAuthorizingUpstreamThreeOutcomeClasses(t *testing.T) {
	resetAuthorizationMetricsForTest()
	inner := &transportUpstream{transport: &http.Transport{}}
	dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeShadow))

	// (1) result
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body); w.WriteHeader(200) }))
	defer ok.Close()
	h, _ := newAuthorizationHandle("shadow")
	resp, err := dec.Do(newRequest(t, WithAuthorizationHandle(context.Background(), h), ok.URL), "", 1, 1)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, AuthorizationOutcomeResult, h.Writes()[0].Outcome)
	require.Equal(t, h.ID+".1", h.SettledToken())

	// (2) not written: nothing listens on the port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dead := "http://" + l.Addr().String()
	_ = l.Close()
	h2, _ := newAuthorizationHandle("shadow")
	_, err = dec.Do(newRequest(t, WithAuthorizationHandle(context.Background(), h2), dead), "", 1, 1)
	require.Error(t, err)
	require.Equal(t, AuthorizationOutcomeNotWritten, h2.Writes()[0].Outcome)
	require.Equal(t, "", h2.SettledToken())

	// (3) indeterminate: the server hijacks and closes after reading the request
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer hang.Close()
	h3, _ := newAuthorizationHandle("shadow")
	_, err = dec.Do(newRequest(t, WithAuthorizationHandle(context.Background(), h3), hang.URL), "", 1, 1)
	require.Error(t, err)
	require.Equal(t, AuthorizationOutcomeIndeterminate, h3.Writes()[0].Outcome)

	m := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(3), m.WritesAuthorized)
	require.Equal(t, int64(1), m.OutcomeResult)
	require.Equal(t, int64(1), m.OutcomeNotWritten)
	require.Equal(t, int64(1), m.OutcomeIndeterminate)
}

func TestAuthorizingUpstreamEachRetryMintsADistinctToken(t *testing.T) {
	inner := &transportUpstream{transport: http.DefaultTransport}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeShadow))
	h, _ := newAuthorizationHandle("shadow")
	ctx := WithAuthorizationHandle(context.Background(), h)
	for i := 0; i < 3; i++ {
		resp, err := dec.Do(newRequest(t, ctx, srv.URL), "", 1, 1)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}
	writes := h.Writes()
	require.Len(t, writes, 3)
	require.NotEqual(t, writes[0].Token, writes[1].Token)
	require.NotEqual(t, writes[1].Token, writes[2].Token)
	require.Equal(t, writes[2].Token, h.SettledToken())
}

func TestAuthorizingUpstreamClassification(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	inner := &transportUpstream{transport: http.DefaultTransport}

	t.Run("shadow admits and counts an unmarked write", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeShadow))
		resp, err := dec.Do(newRequest(t, context.Background(), srv.URL), "", 1, 1)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesUnmarked)
		require.Equal(t, int64(0), AuthorizationMetricsSnapshot().WritesRefused)
	})
	t.Run("enforce refuses an unmarked write before it is sent", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		before := inner.calls
		dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeEnforce))
		resp, err := dec.Do(newRequest(t, context.Background(), srv.URL), "", 1, 1)
		require.Nil(t, resp)
		require.True(t, errors.Is(err, ErrAuthorizationRefused))
		refused, _ := AsAuthorizationRefused(err)
		require.Equal(t, AuthorizationRefusalUnmarkedWrite, refused.Reason)
		require.Contains(t, refused.Detail, "POST")
		require.Equal(t, before, inner.calls, "the inner port is never called")
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesRefused)
	})
	t.Run("enforce admits a non-billable mark", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeEnforce))
		resp, err := dec.Do(newRequest(t, WithNonBillableUpstream(context.Background(), NonBillableProbe), srv.URL), "", 1, 1)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().WritesNonBillable)
	})
	t.Run("enforce refuses a write whose handle was refused at Authorize", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeEnforce))
		h, _ := newAuthorizationHandle("enforce")
		h.Refusal = &AuthorizationRefusedError{Reason: AuthorizationRefusalBalanceShortfall, AuthorizationID: h.ID}
		_, err := dec.Do(newRequest(t, WithAuthorizationHandle(context.Background(), h), srv.URL), "", 1, 1)
		require.True(t, errors.Is(err, ErrAuthorizationRefused))
		require.Empty(t, h.Writes(), "no token is minted for a refused handle")
	})
	t.Run("a misplaced mark is counted and the handle wins", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeEnforce))
		h, _ := newAuthorizationHandle("enforce")
		ctx := WithNonBillableUpstream(WithAuthorizationHandle(context.Background(), h), NonBillableProbe)
		resp, err := dec.Do(newRequest(t, ctx, srv.URL), "", 1, 1)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, int64(1), AuthorizationMetricsSnapshot().MisplacedMarks)
		require.Len(t, h.Writes(), 1)
	})
	t.Run("DoWithTLS classifies exactly as Do", func(t *testing.T) {
		resetAuthorizationMetricsForTest()
		dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeEnforce))
		_, err := dec.DoWithTLS(newRequest(t, context.Background(), srv.URL), "", 1, 1, nil)
		require.True(t, errors.Is(err, ErrAuthorizationRefused))
	})
	t.Run("the test-only refusal delay is honoured", func(t *testing.T) {
		dec := newAuthorizingHTTPUpstreamWithMode(inner, modeFn(config.CanonicalWalletModeEnforce))
		dec.refusalDelayForTest = 20 * time.Millisecond
		start := time.Now()
		_, err := dec.Do(newRequest(t, context.Background(), srv.URL), "", 1, 1)
		require.True(t, errors.Is(err, ErrAuthorizationRefused))
		require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	})
}
