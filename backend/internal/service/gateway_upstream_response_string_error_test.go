package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractUpstreamErrorMessage_StringErrorField(t *testing.T) {
	require.Equal(t, "余额不足，请前往充值", ExtractUpstreamErrorMessage([]byte(`{"error":"余额不足，请前往充值"}`)))
	require.Equal(t, "quota exceeded", ExtractUpstreamErrorMessage([]byte(`{"error":{"message":"quota exceeded"}}`)))
	require.Equal(t, "detail msg", ExtractUpstreamErrorMessage([]byte(`{"detail":"detail msg"}`)))
	require.Equal(t, "top msg", ExtractUpstreamErrorMessage([]byte(`{"message":"top msg"}`)))
	require.Equal(t, "", ExtractUpstreamErrorMessage([]byte(`{}`)))
}
