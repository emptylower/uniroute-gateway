//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestPassthroughFinancialHandoffPersistsBeforeSealAndDispatch(t *testing.T) {
	var mu sync.Mutex
	var order []string
	harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, func(turn int, _ EstimateInput) (*AuthorizationHandle, error) {
		h, err := newAuthorizationHandle("enforce")
		if err != nil {
			return nil, err
		}
		h.sealEvidence = func(context.Context, string) error {
			mu.Lock()
			order = append(order, fmt.Sprintf("seal%d", turn))
			mu.Unlock()
			return nil
		}
		h.dispatchEvidence = func(context.Context) error {
			mu.Lock()
			order = append(order, fmt.Sprintf("dispatch%d", turn))
			mu.Unlock()
			return nil
		}
		return h, nil
	}, nil, func(hooks *OpenAIWSIngressHooks) {
		hooks.AfterTurn = func(turn int, result *OpenAIForwardResult, err error) {
			if err != nil || result == nil {
				return
			}
			mu.Lock()
			order = append(order, fmt.Sprintf("record%d", turn))
			mu.Unlock()
		}
	})
	defer harness.server.Close()
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
	harness.waitCall(t)
	harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	_, err := harness.readClient(t)
	require.NoError(t, err)
	harness.writeClient(t, `{"type":"response.create","model":"gpt-5.1","previous_response_id":"resp_1"}`)
	harness.waitCall(t)
	harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.created","response":{"id":"resp_2"}}`)
	_, err = harness.readClient(t)
	require.NoError(t, err)
	// Duplicate old terminal must not bill, count or disarm the new handle.
	harness.upstream.Send(`{"type":"response.done","response":{"id":"resp_1","usage":{"input_tokens":999,"output_tokens":999}}}`)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_2","usage":{"input_tokens":2,"output_tokens":1}}}`)
	event, err := harness.readClient(t)
	require.NoError(t, err)
	require.Equal(t, "resp_2", gjson.GetBytes(event, "response.id").String())
	harness.closeClientAndWait(t)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"record1", "seal1", "dispatch1", "record2", "seal2", "dispatch2"}, order)
}

func TestPassthroughFinancialPanicFinalizesRetainedHandleAfterReaderJoin(t *testing.T) {
	finalized := make(chan string, 2)
	var harness *passthroughAuthHarness
	harness = newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, func(int, EstimateInput) (*AuthorizationHandle, error) {
		h, err := newAuthorizationHandle("enforce")
		if err != nil {
			return nil, err
		}
		h.sealEvidence = func(_ context.Context, reason string) error {
			select {
			case <-harness.upstream.closed:
			default:
				return errors.New("reader not stopped before seal")
			}
			finalized <- reason
			return nil
		}
		h.dispatchEvidence = func(context.Context) error { finalized <- "dispatch"; return nil }
		return h, nil
	}, nil, func(hooks *OpenAIWSIngressHooks) {
		hooks.AfterTurn = func(int, *OpenAIForwardResult, error) { panic("durable stage failure") }
	})
	defer harness.server.Close()
	harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
	harness.waitCall(t)
	harness.waitUpstreamWrite(t)
	harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_panic","usage":{"input_tokens":2,"output_tokens":1}}}`)
	select {
	case err := <-harness.serverErrCh:
		require.ErrorContains(t, err, "durable stage failure")
	case <-time.After(3 * time.Second):
		t.Fatal("panic recovery did not finish the session")
	}
	require.Equal(t, "ws_reader_joined", <-finalized)
	require.Equal(t, "dispatch", <-finalized)
	_ = harness.clientConn.CloseNow()
}

func TestPassthroughFinancialMetadataDoesNotTurnMissingMalformedIntoZero(t *testing.T) {
	for _, tc := range []struct {
		name, usage        string
		present, malformed bool
	}{
		{"missing", "", false, false},
		{"malformed", `,"usage":{"input_tokens":0}`, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results := make(chan *OpenAIForwardResult, 1)
			harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, nil, nil, func(hooks *OpenAIWSIngressHooks) {
				hooks.AfterTurn = func(_ int, result *OpenAIForwardResult, _ error) { results <- result }
			})
			defer harness.server.Close()
			harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
			harness.waitCall(t)
			harness.waitUpstreamWrite(t)
			harness.upstream.Send(`{"type":"response.completed","response":{"id":"resp_metadata"` + tc.usage + `}}`)
			_, err := harness.readClient(t)
			require.NoError(t, err)
			result := <-results
			require.Equal(t, tc.present, result.UsagePresent)
			require.False(t, result.UsageValid)
			require.Equal(t, tc.malformed, result.UsageMalformed)
			harness.closeClientAndWait(t)
		})
	}
}

func TestPassthroughFinancialRiskRefusalPreservesStatusAndRetryBeforeWrite(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			harness := newPassthroughAuthHarness(t, config.CanonicalWalletModeEnforce, func(int, EstimateInput) (*AuthorizationHandle, error) {
				return nil, &WalletRiskRefusedError{Status: status, RetryAfter: 17}
			}, nil)
			defer harness.server.Close()
			harness.dial(t, `{"type":"response.create","model":"gpt-5.1"}`)
			harness.waitCall(t)
			_, err := harness.readClient(t)
			require.Equal(t, coderws.StatusTryAgainLater, coderws.CloseStatus(err))
			var closeErr coderws.CloseError
			require.ErrorAs(t, err, &closeErr)
			require.Contains(t, closeErr.Reason, fmt.Sprintf("status=%d", status))
			require.Contains(t, closeErr.Reason, "Retry-After=17")
			select {
			case payload := <-harness.upstream.writes:
				t.Fatalf("risk refusal wrote upstream: %s", payload)
			default:
			}
			<-harness.serverErrCh
		})
	}
}

func TestPassthroughFinancialRiskMappingPrecedesGenericAuthorization(t *testing.T) {
	err := fmt.Errorf("%w: %w", ErrAuthorizationRefused, &WalletRiskRefusedError{Status: 503, RetryAfter: 3})
	status, reason, ok := openAIWSPassthroughRelayClientClose(openaiwsv2.RelayExit{Err: err}, 0)
	require.True(t, ok)
	require.Equal(t, coderws.StatusTryAgainLater, status)
	require.Contains(t, reason, "status=503 Retry-After=3")
	svc := &OpenAIGatewayService{}
	mapped := svc.mapOpenAIWSPassthroughDialError(errors.New("upstream unavailable"), 503, http.Header{"Retry-After": {"8"}})
	var closeErr *OpenAIWSClientCloseError
	require.ErrorAs(t, mapped, &closeErr)
	require.Equal(t, coderws.StatusTryAgainLater, closeErr.StatusCode())
	require.Contains(t, closeErr.Reason(), "status=503 Retry-After=8")
}

func TestIngressFinancialHandoffAndCrossTurnTerminalDedup(t *testing.T) {
	f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
	f.setCaptureEvents(t, [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_ingress_1","usage":{"input_tokens":2,"output_tokens":1}}}`),
		[]byte(`{"type":"response.completed","response":{"id":"resp_ingress_1","usage":{"input_tokens":999,"output_tokens":999}}}`),
		[]byte(`{"type":"response.completed","response":{"id":"resp_ingress_2","usage":{"input_tokens":4,"output_tokens":2}}}`),
	})
	var mu sync.Mutex
	var order []string
	results := make(chan *OpenAIForwardResult, 2)
	harness := f.startWSSession(t, func(hooks *OpenAIWSIngressHooks) {
		authorize := hooks.AuthorizeTurn
		hooks.AuthorizeTurn = func(turn int, estimate EstimateInput) (*AuthorizationHandle, error) {
			h, err := authorize(turn, estimate)
			if err != nil {
				return nil, err
			}
			h.sealEvidence = func(context.Context, string) error {
				mu.Lock()
				order = append(order, fmt.Sprintf("seal%d", turn))
				mu.Unlock()
				return nil
			}
			h.dispatchEvidence = func(context.Context) error {
				mu.Lock()
				order = append(order, fmt.Sprintf("dispatch%d", turn))
				mu.Unlock()
				return nil
			}
			return h, nil
		}
		hooks.AfterTurn = func(turn int, result *OpenAIForwardResult, err error) {
			if result == nil || err != nil {
				return
			}
			mu.Lock()
			order = append(order, fmt.Sprintf("record%d", turn))
			mu.Unlock()
			results <- result
		}
	})
	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false}`)
	harness.waitCall(t)
	harness.readEvent(t)
	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false}`)
	harness.waitCall(t)
	event := harness.readEvent(t)
	require.Equal(t, "resp_ingress_2", gjson.GetBytes(event, "response.id").String())
	harness.closeAndWait(t)
	first, second := <-results, <-results
	require.True(t, first.UsageValid)
	require.Equal(t, 4, second.Usage.InputTokens)
	require.Equal(t, 2, second.Usage.OutputTokens)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"record1", "seal1", "dispatch1", "record2", "seal2", "dispatch2"}, order)
}

func TestIngressFinancialMalformedUsageRetainsUnknownMetadata(t *testing.T) {
	f := newIngressAuthFixture(t, config.CanonicalWalletModeEnforce)
	f.setCaptureEvents(t, [][]byte{[]byte(`{"type":"response.completed","response":{"id":"resp_bad","usage":{"input_tokens":0,"output_tokens":-1}}}`)})
	results := make(chan *OpenAIForwardResult, 1)
	harness := f.startWSSession(t, func(hooks *OpenAIWSIngressHooks) {
		hooks.AfterTurn = func(_ int, result *OpenAIForwardResult, _ error) {
			if result != nil {
				results <- result
			}
		}
	})
	harness.writeMessage(t, `{"type":"response.create","model":"gpt-5.1","stream":false}`)
	harness.waitCall(t)
	harness.readEvent(t)
	harness.closeAndWait(t)
	result := <-results
	require.True(t, result.UsagePresent)
	require.False(t, result.UsageValid)
	require.True(t, result.UsageMalformed)
	require.Zero(t, result.Usage.OutputTokens)
}
