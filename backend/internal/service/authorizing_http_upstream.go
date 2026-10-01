package service

import (
	"net/http"
	"net/http/httptrace"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

// AuthorizingHTTPUpstream is the authorization boundary on the HTTP port (spec
// §2.0). It wraps the ONE HTTPUpstream the DI graph constructs, so every one of
// the 58 Do/DoWithTLS call sites — repository-layer users included — is covered
// without an inventory. It classifies each write off req.Context():
//
//	handle present            → billable, authorized: mint a token, send, record the outcome
//	non-billable mark present → send unchanged
//	neither                   → UNMARKED: refused in enforce mode, counted+logged in shadow
//
// It never touches a request in disabled mode. It arms no hold.
type AuthorizingHTTPUpstream struct {
	inner HTTPUpstream
	mode  func() string
	calls atomic.Int64
	// refusalDelayForTest lets the first-output header-guard test trip the guard
	// before the refusal returns (Task 8). Zero in production.
	refusalDelayForTest time.Duration
}

// NewAuthorizingHTTPUpstream is what repository.NewHTTPUpstream returns.
func NewAuthorizingHTTPUpstream(inner HTTPUpstream, cfg *config.Config) HTTPUpstream {
	mode := func() string {
		if cfg == nil {
			return config.CanonicalWalletModeDisabled
		}
		return cfg.CanonicalWallet.Mode
	}
	return newAuthorizingHTTPUpstreamWithMode(inner, mode)
}

func newAuthorizingHTTPUpstreamWithMode(inner HTTPUpstream, mode func() string) *AuthorizingHTTPUpstream {
	return &AuthorizingHTTPUpstream{inner: inner, mode: mode}
}

// Calls counts every entry through the decorator (Task 8's tests assert exactly one).
func (a *AuthorizingHTTPUpstream) Calls() int64 { return a.calls.Load() }

func (a *AuthorizingHTTPUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	return a.write(req, func(r *http.Request) (*http.Response, error) {
		return a.inner.Do(r, proxyURL, accountID, accountConcurrency)
	})
}

func (a *AuthorizingHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return a.write(req, func(r *http.Request) (*http.Response, error) {
		return a.inner.DoWithTLS(r, proxyURL, accountID, accountConcurrency, profile)
	})
}

func (a *AuthorizingHTTPUpstream) write(req *http.Request, send func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	a.calls.Add(1)
	mode := a.mode()
	if req == nil || mode == "" || mode == config.CanonicalWalletModeDisabled {
		return send(req)
	}
	ctx := req.Context()
	handle := AuthorizationHandleFromContext(ctx)
	_, marked := NonBillableUpstreamFromContext(ctx)
	switch {
	case handle != nil:
		if marked {
			// A non-billable mark on a billable write is the one silent-and-free
			// mistake (spec §2.0); the static egress test is what fails it. At
			// runtime the handle wins so the write is authorized, and it is counted.
			authorizationMetrics.misplacedMarks.Add(1)
		}
		return a.authorizedWrite(req, handle, send)
	case marked:
		authorizationMetrics.writesNonBillable.Add(1)
		return send(req)
	default:
		authorizationMetrics.writesUnmarked.Add(1)
		if mode == config.CanonicalWalletModeEnforce {
			authorizationMetrics.writesRefused.Add(1)
			if a.refusalDelayForTest > 0 {
				time.Sleep(a.refusalDelayForTest)
			}
			return nil, &AuthorizationRefusedError{Reason: AuthorizationRefusalUnmarkedWrite, Detail: describeUpstreamRequest(req)}
		}
		logger.LegacyPrintf("service.authorization", "shadow: unmarked upstream write admitted: %s from %s", describeUpstreamRequest(req), callerOutsideDecorator())
		return send(req)
	}
}

func (a *AuthorizingHTTPUpstream) authorizedWrite(req *http.Request, handle *AuthorizationHandle, send func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	var err error
	handle, err = handle.nextHTTPAttempt(req.Context())
	if err != nil {
		return nil, err
	}
	req = req.WithContext(WithAuthorizationHandle(req.Context(), handle))
	if handle.Refusal != nil {
		authorizationMetrics.writesRefused.Add(1)
		return nil, handle.Refusal
	}
	token := handle.MintWriteToken()
	if err := handle.prepareWrite(req.Context(), token); err != nil {
		return nil, err
	}
	authorizationMetrics.writesAuthorized.Add(1)
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteHeaders:         func() { wrote.Store(true) },
		WroteRequest:         func(httptrace.WroteRequestInfo) { wrote.Store(true) },
		GotFirstResponseByte: func() { wrote.Store(true) },
	}
	// WithContext derives from req.Context(), so the handle, the profile and every
	// other value survive; WithClientTrace composes with any trace already present.
	traced := req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := send(traced)
	// Residual, named: net/http can return from roundTrip on a closed connection or a
	// cancelled context before writeLoop fires WroteHeaders, so a request that did reach
	// the wire can classify as not_written. Nothing in 3.3 acts on the class; 3.4b's
	// reaper must cross-check the class against the error before treating not_written
	// as "nothing left the process" (3.4b deliverable, recorded here so it is not lost).
	switch {
	case err == nil && resp != nil && handle.beforeWrite != nil && (resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusRequestEntityTooLarge || resp.StatusCode == http.StatusUnsupportedMediaType || resp.StatusCode == http.StatusUnprocessableEntity):
		handle.RecordOutcome(token, AuthorizationOutcomeRejected, nil)
	case err == nil:
		authorizationMetrics.outcomeResult.Add(1)
		handle.RecordOutcome(token, AuthorizationOutcomeResult, nil)
	case wrote.Load():
		authorizationMetrics.outcomeIndeterminate.Add(1)
		handle.RecordOutcome(token, AuthorizationOutcomeIndeterminate, err)
	default:
		authorizationMetrics.outcomeNotWritten.Add(1)
		handle.RecordOutcome(token, AuthorizationOutcomeNotWritten, err)
	}
	return resp, err
}

func describeUpstreamRequest(req *http.Request) string {
	if req == nil || req.URL == nil {
		return "<nil request>"
	}
	return req.Method + " " + req.URL.Host + req.URL.EscapedPath()
}

// callerOutsideDecorator names the first frame outside this file — the call site
// of the unmarked write — for the shadow-mode log line only.
func callerOutsideDecorator() string {
	pcs := make([]uintptr, 16)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if !strings.HasSuffix(f.File, "authorizing_http_upstream.go") && f.File != "" {
			return f.File + ":" + strconv.Itoa(f.Line)
		}
		if !more {
			return "?"
		}
	}
}
