//go:build unit

package service

import (
	"context"
	"testing"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type mockLiveFrameConnForAuth struct {
	fakeWSConn
	writes [][]byte
}

func (m *mockLiveFrameConnForAuth) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	m.writes = append(m.writes, append([]byte(nil), payload...))
	return nil
}

func (m *mockLiveFrameConnForAuth) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	<-ctx.Done()
	return 0, nil, ctx.Err()
}

func TestOpenAILiveSidebandWritesAreMarkedNonBillable(t *testing.T) {
	resetAuthorizationMetricsForTest()

	raw := &mockLiveFrameConnForAuth{}
	authConn := &authorizingOpenAIWSClientConn{
		inner: raw,
		mode:  func() string { return config.CanonicalWalletModeEnforce },
	}

	// Verify that authConn implements liveFrameConn
	liveConn, ok := any(authConn).(liveFrameConn)
	require.True(t, ok, "authorizing conn must implement liveFrameConn")

	// Sideband Write 1: attestation / client sideband frame
	ctx1 := WithNonBillableUpstream(context.Background(), NonBillableLiveSideband)
	err := liveConn.WriteFrame(ctx1, coderws.MessageText, []byte(`{"type":"attestation.response"}`))
	require.NoError(t, err)

	// Sideband Write 2: session update / session close
	ctx2 := WithNonBillableUpstream(context.Background(), NonBillableLiveSideband)
	err = liveConn.WriteFrame(ctx2, coderws.MessageText, []byte(`{"type":"session.close"}`))
	require.NoError(t, err)

	require.Len(t, raw.writes, 2)

	// Verify authorization metrics: exactly 2 non-billable writes, 0 unmarked/refused/authorized/misplaced
	metrics := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(2), metrics.WritesNonBillable, "both sideband writes must be counted as non-billable")
	require.Equal(t, int64(0), metrics.WritesAuthorized, "sideband writes must not be counted as authorized turns")
	require.Equal(t, int64(0), metrics.WritesUnmarked, "sideband writes must not be unmarked")
	require.Equal(t, int64(0), metrics.WritesRefused, "sideband writes must not be refused")
	require.Equal(t, int64(0), metrics.MisplacedMarks, "sideband marks on live conn are valid")
}

func TestOpenAILiveSidebandInProxyLiveSidebandMarksFramesNonBillable(t *testing.T) {
	resetAuthorizationMetricsForTest()

	raw := &mockLiveFrameConnForAuth{}
	authConn := &authorizingOpenAIWSClientConn{
		inner: raw,
		mode:  func() string { return config.CanonicalWalletModeEnforce },
	}
	liveConn := any(authConn).(liveFrameConn)

	// Simulate ProxyLiveSideband context and write
	proxyCtx := WithNonBillableUpstream(context.Background(), NonBillableLiveSideband)
	err := liveConn.WriteFrame(proxyCtx, coderws.MessageText, []byte(`{"type":"input_audio_buffer.append","audio":"base64..."}`))
	require.NoError(t, err)

	metrics := AuthorizationMetricsSnapshot()
	require.Equal(t, int64(1), metrics.WritesNonBillable)
	require.Equal(t, int64(0), metrics.WritesUnmarked)
	require.Equal(t, int64(0), metrics.WritesRefused)
}
