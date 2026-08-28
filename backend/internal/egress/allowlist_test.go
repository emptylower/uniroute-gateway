//go:build unit

package egress

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const updateEnv = "EGRESS_ALLOWLIST_UPDATE"

func TestEgressAllowlistIsPinned(t *testing.T) {
	root := repoRoot(t)
	entries, sites, err := Walk(root)
	require.NoError(t, err)
	if os.Getenv(updateEnv) == "1" {
		writePinned(t, filepath.Join("testdata", "egress_allowlist.json"), mergeEntries(loadEntries(t), entries))
		writePinned(t, filepath.Join("testdata", "upstream_write_sites.json"), mergeSites(loadSites(t), sites))
		t.Skip("pinned lists regenerated; re-run without " + updateEnv)
	}
	pinned := loadEntries(t)
	byKey := map[string]EgressEntry{}
	for _, e := range pinned {
		byKey[e.Key()] = e
		require.Contains(t, []Annotation{AnnotationPort, AnnotationNonBillable, AnnotationBillable, AnnotationBillableOutsideThisWallet}, e.Annotation, "%s has no annotation — annotate it in testdata/egress_allowlist.json", e.Key())
	}
	for _, e := range entries {
		if _, ok := byKey[e.Key()]; !ok {
			t.Errorf("new egress construct not on the allowlist — a deliberate act is required (spec §2.0): %s", e.Key())
		}
		delete(byKey, e.Key())
	}
	for k := range byKey {
		t.Errorf("allowlist entry no longer exists — remove it: %s", k)
	}

	pinnedSites := loadSites(t)
	found := map[string]WriteSite{}
	for _, s := range sites {
		found[s.Key()] = s
	}
	for _, s := range pinnedSites {
		f, ok := found[s.Key()]
		require.True(t, ok, "pinned write site missing: %s", s.Key())
		switch s.Annotation {
		case AnnotationNonBillable:
			require.True(t, f.EnclosingFuncCarriesNonBillableMark, "non-billable site %s: its function carries no WithNonBillableUpstream mark", s.Key())
		case AnnotationBillable:
			require.False(t, f.EnclosingFuncCarriesNonBillableMark, "BILLABLE site %s carries a non-billable mark — the one silent-and-free mistake (spec §2.0)", s.Key())
		default:
			t.Errorf("write site %s: annotation must be billable or non-billable", s.Key())
		}
		delete(found, s.Key())
	}
	for k := range found {
		t.Errorf("new upstream write site not pinned — annotate it: %s", k)
	}
}

// Exit: the allowlist test "fails on an injected new importing file or client
// factory" and "a billable write carrying a non-billable mark fails the test".
func TestEgressAllowlistFailsOnInjectedNewConstruct(t *testing.T) {
	root := writeFixture(t, t.TempDir(), map[string]string{
		"internal/sneaky_client.go": `package internal

import "net/http"

func sneakyNewClient() *http.Client {
	return &http.Client{}
}
`,
	})
	entries, _, err := Walk(root)
	require.NoError(t, err)
	pinned := loadEntries(t)
	byKey := map[string]EgressEntry{}
	for _, e := range pinned {
		byKey[e.Key()] = e
	}
	found := false
	for _, e := range entries {
		if _, ok := byKey[e.Key()]; !ok {
			found = true
		}
	}
	require.True(t, found, "an injected new client factory must be detected as un-pinned")
}

func TestEgressAllowlistFailsOnBillableSiteWithNonBillableMark(t *testing.T) {
	root := writeFixture(t, t.TempDir(), map[string]string{
		"internal/marked_billable.go": `package internal

import (
	"context"
	"net/http"

	"example.com/service"
)

type marked struct{ httpUpstream service.HTTPUpstream }

func (m *marked) write(ctx context.Context) {
	ctx = service.WithNonBillableUpstream(ctx, service.NonBillableProbe)
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
	_, _ = m.httpUpstream.Do(req, "", 1, 1)
}
`,
	})
	_, sites, err := Walk(root)
	require.NoError(t, err)
	found := map[string]WriteSite{}
	for _, s := range sites {
		found[s.Key()] = s
	}
	pinned := []WriteSite{{
		File: "internal/marked_billable.go", Func: "marked.write", Ordinal: 1, Method: "Do",
		Annotation: AnnotationBillable,
	}}
	for _, s := range pinned {
		f, ok := found[s.Key()]
		require.True(t, ok, "pinned write site missing: %s", s.Key())
		// The walker DOES see the mark on this billable-annotated write — which
		// is exactly the condition TestEgressAllowlistIsPinned's
		// `require.False(..., "BILLABLE site … carries a non-billable mark …")`
		// turns into a failure.
		require.True(t, f.EnclosingFuncCarriesNonBillableMark,
			"a billable write carrying WithNonBillableUpstream must be detected")
	}
}
