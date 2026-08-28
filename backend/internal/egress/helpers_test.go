package egress

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// repoRoot walks up from this file's directory to the directory containing go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file location")
	}
	dir := filepath.Dir(thisFile)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above test directory")
		}
		dir = parent
	}
}

// loadEntries reads the pinned egress allowlist; an absent file yields an empty
// slice (the first EGRESS_ALLOWLIST_UPDATE=1 run).
func loadEntries(t *testing.T) []EgressEntry {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "egress_allowlist.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var entries []EgressEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatal(err)
	}
	return entries
}

// loadSites reads the pinned upstream write sites; an absent file yields an
// empty slice (the first EGRESS_ALLOWLIST_UPDATE=1 run).
func loadSites(t *testing.T) []WriteSite {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "upstream_write_sites.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var sites []WriteSite
	if err := json.Unmarshal(raw, &sites); err != nil {
		t.Fatal(err)
	}
	return sites
}

// mustWalkWriteSites runs Walk and keys the found sites by Key().
func mustWalkWriteSites(t *testing.T, root string) map[string]WriteSite {
	t.Helper()
	_, sites, err := Walk(root)
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[string]WriteSite, len(sites))
	for _, s := range sites {
		found[s.Key()] = s
	}
	return found
}

// mustLoadWriteSites is loadSites with a hard failure on malformed pinned data.
func mustLoadWriteSites(t *testing.T) []WriteSite {
	t.Helper()
	return loadSites(t)
}

// writePinned marshals entries as indented JSON into testdata.
func writePinned(t *testing.T, name string, entries any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// mergeEntries keeps existing annotations and notes for keys already pinned,
// adds new keys with an empty annotation (the next plain run fails on them until
// a human annotates), and drops vanished keys.
func mergeEntries(pinned []EgressEntry, found []EgressEntry) []EgressEntry {
	byKey := make(map[string]EgressEntry, len(pinned))
	for _, e := range pinned {
		byKey[e.Key()] = e
	}
	merged := make([]EgressEntry, 0, len(found))
	for _, e := range found {
		if old, ok := byKey[e.Key()]; ok {
			e.Annotation, e.Note = old.Annotation, old.Note
		}
		merged = append(merged, e)
	}
	return merged
}

// mergeSites is mergeEntries for write sites.
func mergeSites(pinned []WriteSite, found []WriteSite) []WriteSite {
	byKey := make(map[string]WriteSite, len(pinned))
	for _, s := range pinned {
		byKey[s.Key()] = s
	}
	merged := make([]WriteSite, 0, len(found))
	for _, s := range found {
		if old, ok := byKey[s.Key()]; ok {
			s.Annotation, s.Note = old.Annotation, old.Note
		}
		merged = append(merged, s)
	}
	return merged
}
