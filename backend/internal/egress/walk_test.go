package egress

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeFixture builds a mini source tree under dir with the given file contents.
func writeFixture(t *testing.T, dir string, files map[string]string) string {
	t.Helper()
	for name, src := range files {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestWalkFindsConstructsFactoriesAndWriteSites(t *testing.T) {
	root := writeFixture(t, t.TempDir(), map[string]string{
		"internal/client.go": `package internal

import "net/http"

func newClient() *http.Client {
	return &http.Client{}
}

func newTransport() http.RoundTripper {
	return http.DefaultTransport
}
`,
		"internal/writes.go": `package internal

import (
	"net/http"

	"example.com/service"
)

type svc struct{ httpUpstream service.HTTPUpstream }

func (s *svc) bothWrites(ctx context.Context) {
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
	service.WithNonBillableUpstream(ctx, service.NonBillableProbe)
	resp, err := s.httpUpstream.Do(req, "", 1, 1)
	_ = resp
	_ = err
	resp2, err2 := s.httpUpstream.DoWithTLS(req, "", 1, 1, nil)
	_ = resp2
	_ = err2
}

func (s *svc) shortCircuit(ctx context.Context) {
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
	resp, err := s.httpUpstream.Do(req, "", 1, 1)
	if _, ok := service.AsAuthorizationRefused(err); ok {
		return
	}
	_ = resp
}

func fetch(ctx context.Context) error {
	_, err := http.Get("http://example.com")
	return err
}
`,
		"internal/testdata/ignored.go": `package testdata

import "net/http"

func inTestdata() {
	_ = &http.Client{}
}
`,
	})

	entries, sites, err := Walk(root)
	require.NoError(t, err)

	entryKeys := map[string]bool{}
	for _, e := range entries {
		entryKeys[e.Key()] = true
	}
	require.True(t, entryKeys["internal/client.go|construct|newClient: &http.Client{}"])
	require.True(t, entryKeys["internal/client.go|factory|newTransport"])
	require.True(t, entryKeys["internal/writes.go|construct|fetch: http.Get"])

	// bothWrites hosts two Do-family writes: ordinals 1 and 2, mark set.
	found := map[string]WriteSite{}
	for _, s := range sites {
		found[s.Key()] = s
	}
	w1, ok := found["internal/writes.go|svc.bothWrites|1"]
	require.True(t, ok)
	require.Equal(t, "Do", w1.Method)
	w2, ok := found["internal/writes.go|svc.bothWrites|2"]
	require.True(t, ok)
	require.Equal(t, "DoWithTLS", w2.Method)
	require.True(t, w1.EnclosingFuncCarriesNonBillableMark)
	require.False(t, w1.EnclosingFuncShortCircuitsRefusal)

	// shortCircuit hosts one write whose function short-circuits refusals.
	s1, ok := found["internal/writes.go|svc.shortCircuit|1"]
	require.True(t, ok)
	require.True(t, s1.EnclosingFuncShortCircuitsRefusal)
	require.False(t, s1.EnclosingFuncCarriesNonBillableMark)

	// The walker must not ingest its own testdata tree.
	for _, e := range entries {
		require.NotContains(t, e.File, "testdata")
	}
	for _, s := range sites {
		require.NotContains(t, s.File, "testdata")
	}
}

func TestWalkParsesWholeRepoWithoutError(t *testing.T) {
	fset := token.NewFileSet()
	require.NotNil(t, fset)
	_, err := parser.ParseFile(fset, "walk.go", nil, parser.ParseComments)
	require.NoError(t, err)
}

// Review note M1: direct branch coverage for the two expression helpers the
// walker uses to classify refusal checks and upstream receivers.
func TestIsSelectorOrIdentNamed(t *testing.T) {
	for _, expr := range []string{`ErrAuthorizationRefused`, `pkg.ErrAuthorizationRefused`, `somethingElse`} {
		e, err := parser.ParseExpr(expr)
		require.NoError(t, err)
		require.True(t, isSelectorOrIdentNamed(e, "ErrAuthorizationRefused") == (expr != `somethingElse`))
	}
	idx, err := parser.ParseExpr(`m["k"]`)
	require.NoError(t, err)
	require.False(t, isSelectorOrIdentNamed(idx, "ErrAuthorizationRefused"))
}

func TestReceiverIsHTTPUpstream(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want bool
	}{{`httpUpstream`, true}, {`s.httpUpstream`, true}, {`s.accountTestService.httpUpstream`, true}, {`client`, false}, {`client.Do`, false}, {`m["k"]`, false}} {
		e, err := parser.ParseExpr(tc.expr)
		require.NoError(t, err)
		require.Equal(t, tc.want, receiverIsHTTPUpstream(e), tc.expr)
	}
}
