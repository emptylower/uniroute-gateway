package service

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// External catalog fetch limits and the exact host allowlist.
// Requests to any other host are rejected before dialing.
const (
	DefaultCatalogRequestTimeout  = 20 * time.Second
	DefaultCatalogMaxPayloadBytes = int64(32 << 20) // 32MiB
	CatalogMaxItemsDefault        = 50000
	maxCatalogRedirects           = 3

	catalogHostOpenRouter    = "openrouter.ai"
	catalogHostModelsDev     = "models.dev"
	catalogHostGitHubAPI     = "api.github.com"
	catalogHostGitHubRawData = "raw.githubusercontent.com"
)

// IsAllowedCatalogHost reports whether host is one of the allowlisted
// catalog sources. Comparison is case-insensitive; ports are not part of the
// host allowlist.
func IsAllowedCatalogHost(host string) bool {
	switch strings.ToLower(host) {
	case catalogHostOpenRouter, catalogHostModelsDev, catalogHostGitHubAPI, catalogHostGitHubRawData:
		return true
	default:
		return false
	}
}

// ValidateCatalogURL enforces HTTPS-only, no userinfo, no IP-literal hosts,
// allowlisted hostname, and standard port only. It is applied to the initial
// URL and to every redirect target.
func ValidateCatalogURL(rawURL string) error {
	if strings.TrimSpace(rawURL) == "" {
		return fmt.Errorf("catalog url is required")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid catalog url: %w", err)
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("catalog url must use https: %q", parsed.Scheme)
	}
	if parsed.User != nil {
		return fmt.Errorf("catalog url must not carry userinfo")
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("catalog url host is required")
	}
	if net.ParseIP(host) != nil {
		return fmt.Errorf("catalog url host must be a DNS name, not an IP literal")
	}
	if !IsAllowedCatalogHost(host) {
		return fmt.Errorf("catalog host %q is not allowlisted", host)
	}
	port := parsed.Port()
	if port != "" && port != "443" {
		return fmt.Errorf("catalog url port %q is not allowed", port)
	}
	return nil
}

// catalogDialControl runs after DNS resolution for every connection attempt and
// rejects private, loopback, link-local, multicast, and unspecified targets so a
// rebinding DNS answer cannot reach internal networks.
func catalogDialControl(network, address string, _ syscall.RawConn) error {
	if !strings.HasPrefix(network, "tcp") {
		return fmt.Errorf("catalog dial network %q is not allowed", network)
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid dial address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("dial target %q did not resolve to an IP", host)
	}
	switch {
	case ip.IsLoopback(),
		ip.IsPrivate(),
		ip.IsLinkLocalUnicast(),
		ip.IsLinkLocalMulticast(),
		ip.IsMulticast(),
		ip.IsUnspecified():
		return fmt.Errorf("dial target %q resolves to a non-public address", host)
	}
	return nil
}

// CatalogHTTPClient performs bounded fetches from the allowlisted catalog hosts.
type CatalogHTTPClient struct {
	http            *http.Client
	maxPayloadBytes int64
}

// NewCatalogHTTPClient builds the bounded client. Non-positive timeout or limit
// falls back to the defaults.
func NewCatalogHTTPClient(timeout time.Duration, maxPayloadBytes int64) *CatalogHTTPClient {
	if timeout <= 0 {
		timeout = DefaultCatalogRequestTimeout
	}
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = DefaultCatalogMaxPayloadBytes
	}
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   catalogDialControl,
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: time.Second,
		MaxIdleConns:          4,
		MaxConnsPerHost:       2,
		ForceAttemptHTTP2:     true,
	}
	return &CatalogHTTPClient{
		http: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return checkCatalogRedirect(req, via)
			},
		},
		maxPayloadBytes: maxPayloadBytes,
	}
}

// checkCatalogRedirect allows at most maxCatalogRedirects hops, each validated
// against the same URL rules as the initial request.
func checkCatalogRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxCatalogRedirects {
		return fmt.Errorf("catalog request exceeded %d redirects", maxCatalogRedirects)
	}
	if err := ValidateCatalogURL(req.URL.String()); err != nil {
		return fmt.Errorf("catalog redirect target rejected: %w", err)
	}
	return nil
}

// checkRedirect is the client-scoped entry point for redirect validation.
func (c *CatalogHTTPClient) checkRedirect(req *http.Request, via []*http.Request) error {
	return checkCatalogRedirect(req, via)
}

// Get fetches rawURL and returns its body, enforcing the payload byte limit.
func (c *CatalogHTTPClient) Get(ctx context.Context, rawURL string) ([]byte, error) {
	if c == nil || c.http == nil {
		return nil, fmt.Errorf("catalog http client is not configured")
	}
	if err := ValidateCatalogURL(rawURL); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("catalog fetch %s returned status %s", rawURL, strconv.Itoa(resp.StatusCode))
	}
	return readCatalogBody(resp.Body, c.maxPayloadBytes)
}

// readCatalogBody reads r fully but fails when it exceeds limit bytes.
func readCatalogBody(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("catalog payload exceeds limit of %d bytes", limit)
	}
	return body, nil
}
