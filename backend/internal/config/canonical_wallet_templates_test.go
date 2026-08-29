//go:build unit

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// TestCanonicalWalletTemplatesCarryEveryKey (Phase 3.8-G Task 2, redesign
// §14.3 b): every canonical_wallet.* key added since 3.4 must be deployable
// from the templates — a deployment that follows config.example.yaml and the
// ShipAny compose cannot set a key the templates omit. The reflection walks
// CanonicalWalletConfig's mapstructure tags (the env_reachability_test.go
// pattern) and asserts each key appears as `  <key>:` in the yaml block and
// as `CANONICAL_WALLET_<UPPER>:` in the compose's sub2api.environment
// (viper's AutomaticEnv + "."→"_" replacer, config.go's env setup, maps the
// compose line onto the key).
//
// The deploy files live at the repository root's deploy/ — read relative to
// the package as the migration tests read their SQL:
// filepath.Join("..", "..", "..", "deploy", …) — the exact relative path is
// recorded in the completion record.
func TestCanonicalWalletTemplatesCarryEveryKey(t *testing.T) {
	keys := canonicalWalletMapstructureKeys(t)

	yamlKeys := canonicalWalletBlockKeys(t, filepath.Join("..", "..", "..", "deploy", "config.example.yaml"))
	composeKeys := canonicalWalletComposeKeys(t, filepath.Join("..", "..", "..", "deploy", "docker-compose.shipany.yml"))

	var missingYAML, missingCompose []string
	for _, key := range keys {
		if !yamlKeys[key] {
			missingYAML = append(missingYAML, key)
		}
		if !composeKeys[strings.ToUpper("CANONICAL_WALLET_"+key)] {
			missingCompose = append(missingCompose, key)
		}
	}
	sort.Strings(missingYAML)
	sort.Strings(missingCompose)
	if len(missingYAML) > 0 {
		t.Errorf("deploy/config.example.yaml's canonical_wallet: block is missing keys: %s", strings.Join(missingYAML, ", "))
	}
	if len(missingCompose) > 0 {
		t.Errorf("deploy/docker-compose.shipany.yml's sub2api.environment is missing CANONICAL_WALLET_* entries for keys: %s", strings.Join(missingCompose, ", "))
	}
}

func canonicalWalletMapstructureKeys(t *testing.T) []string {
	t.Helper()
	rt := reflect.TypeOf(CanonicalWalletConfig{})
	var keys []string
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("mapstructure")
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		keys = append(keys, name)
	}
	if len(keys) == 0 {
		t.Fatal("no mapstructure keys found on CanonicalWalletConfig — the reflection is broken")
	}
	return keys
}

// canonicalWalletBlockKeys extracts the two-space-indented `  key:` lines of
// the canonical_wallet: block (comments and blank lines skipped; the block
// ends at the first non-indented, non-comment line).
func canonicalWalletBlockKeys(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	keys := make(map[string]bool)
	inBlock := false
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if !inBlock {
			if trimmed == "canonical_wallet:" {
				inBlock = true
			}
			continue
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if !strings.HasPrefix(line, "  ") {
			break // the block ended
		}
		if strings.HasPrefix(line, "    ") {
			continue // a nested mapping's continuation
		}
		field := strings.TrimRight(strings.TrimPrefix(line, "  "), " ")
		name, _, _ := strings.Cut(field, ":")
		if name != "" {
			keys[name] = true
		}
	}
	if len(keys) == 0 {
		t.Fatalf("no canonical_wallet block found in %s", path)
	}
	return keys
}

// canonicalWalletComposeKeys extracts the CANONICAL_WALLET_* environment
// entries from the compose file's sub2api service.
func canonicalWalletComposeKeys(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	keys := make(map[string]bool)
	for _, line := range strings.Split(string(raw), "\n") {
		trimmedLeft := strings.TrimLeft(line, " ")
		if !strings.HasPrefix(trimmedLeft, "CANONICAL_WALLET_") {
			continue
		}
		name, _, _ := strings.Cut(trimmedLeft, ":")
		keys[name] = true
	}
	if len(keys) == 0 {
		t.Fatalf("no CANONICAL_WALLET_* entries found in %s", path)
	}
	return keys
}
