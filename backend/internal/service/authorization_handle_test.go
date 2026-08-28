//go:build unit

package service

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorizationHandleMintsDistinctTokensPerWrite(t *testing.T) {
	resetAuthorizationMetricsForTest()
	h, err := newAuthorizationHandle("shadow")
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`^auth_[0-9a-f]{32}$`), h.ID)
	t1 := h.MintWriteToken()
	t2 := h.MintWriteToken()
	require.NotEqual(t, t1, t2)
	require.Equal(t, h.ID+".1", t1)
	require.Equal(t, h.ID+".2", t2)
	require.Equal(t, t2, h.LastWriteToken())
	require.Equal(t, "", h.SettledToken(), "no write obtained a result yet")
	h.RecordOutcome(t1, AuthorizationOutcomeNotWritten, errors.New("dial"))
	h.RecordOutcome(t2, AuthorizationOutcomeResult, nil)
	require.Equal(t, t2, h.SettledToken())
	writes := h.Writes()
	require.Len(t, writes, 2)
	require.Equal(t, AuthorizationOutcomeNotWritten, writes[0].Outcome)
	require.Equal(t, AuthorizationOutcomeResult, writes[1].Outcome)
	require.False(t, writes[1].EndedAt.IsZero())
}

func TestAuthorizationHandleIsSafeForConcurrentWrites(t *testing.T) {
	h, err := newAuthorizationHandle("shadow")
	require.NoError(t, err)
	var wg sync.WaitGroup
	seen := make(chan string, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok := h.MintWriteToken()
			h.RecordOutcome(tok, AuthorizationOutcomeResult, nil)
			seen <- tok
		}()
	}
	wg.Wait()
	close(seen)
	uniq := map[string]struct{}{}
	for tok := range seen {
		uniq[tok] = struct{}{}
	}
	require.Len(t, uniq, 64)
	require.Len(t, h.Writes(), 64)
}

func TestAuthorizationHandleNilSafeHelpers(t *testing.T) {
	var h *AuthorizationHandle
	require.Equal(t, "", h.MintWriteToken())
	require.Equal(t, "", AuthorizationTokenOf(h))
	require.Equal(t, "", AuthorizationIDOf(h))
	h.RecordOutcome("x", AuthorizationOutcomeResult, nil) // must not panic
	h.MarkAbandoned("worker_pool_dropped")
	require.Nil(t, h.Writes())
}

func TestAuthorizationHandleContextRoundTrip(t *testing.T) {
	h, err := newAuthorizationHandle("enforce")
	require.NoError(t, err)
	ctx := WithAuthorizationHandle(context.Background(), h)
	require.Same(t, h, AuthorizationHandleFromContext(ctx))
	require.Nil(t, AuthorizationHandleFromContext(context.Background()))
	require.Nil(t, AuthorizationHandleFromContext(nil)) //nolint:staticcheck // nil ctx is the documented no-op
	require.Same(t, ctx, WithAuthorizationHandle(ctx, nil), "a nil handle attaches nothing")
}

func TestNonBillableMarkContextRoundTrip(t *testing.T) {
	ctx := WithNonBillableUpstream(context.Background(), NonBillableCountTokens)
	reason, ok := NonBillableUpstreamFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, NonBillableCountTokens, reason)
	_, ok = NonBillableUpstreamFromContext(context.Background())
	require.False(t, ok)
	// The mark and the transport profile are independent keys (spec §2.0: it cannot be the profile).
	ctx = WithHTTPUpstreamProfile(ctx, HTTPUpstreamProfileOpenAI)
	_, ok = NonBillableUpstreamFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, HTTPUpstreamProfileOpenAI, HTTPUpstreamProfileFromContext(ctx))
}

func TestAuthorizationHandleAbandonedIsRecordedOnce(t *testing.T) {
	resetAuthorizationMetricsForTest()
	h, err := newAuthorizationHandle("shadow")
	require.NoError(t, err)
	h.MarkAbandoned("worker_pool_dropped")
	h.MarkAbandoned("panic")
	require.Equal(t, "worker_pool_dropped", h.Abandoned())
	require.Equal(t, int64(1), AuthorizationMetricsSnapshot().Abandoned)
}
