package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeCatalogFetcher serves canned responses and records requested URLs.
type fakeCatalogFetcher struct {
	responses map[string][]byte
	err       error
	requested []string
}

func (f *fakeCatalogFetcher) Get(ctx context.Context, rawURL string) ([]byte, error) {
	f.requested = append(f.requested, rawURL)
	if f.err != nil {
		return nil, f.err
	}
	body, ok := f.responses[rawURL]
	if !ok {
		return nil, fmt.Errorf("unexpected url %s", rawURL)
	}
	return body, nil
}

const fakeCommitSHA = "0123456789abcdef0123456789abcdef01234567"

func TestGitHubCommitResolvePinsBranch(t *testing.T) {
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", ModelsDevOwner, ModelsDevRepo, ModelsDevBranch)
	fetcher := &fakeCatalogFetcher{responses: map[string][]byte{
		apiURL: []byte(fmt.Sprintf(`{"sha": %q, "commit": {"message": "update data"}}`, fakeCommitSHA)),
	}}
	resolver := NewGitHubCatalogCommitResolver(fetcher)

	commit, err := resolver.ResolveCommit(context.Background(), ModelsDevOwner, ModelsDevRepo, ModelsDevBranch)
	require.NoError(t, err)
	require.Equal(t, fakeCommitSHA, commit)

	// The resolution request went to the allowlisted API host with the branch ref.
	require.Len(t, fetcher.requested, 1)
	require.Contains(t, fetcher.requested[0], "https://api.github.com/repos/models-dev/models.dev/commits/main")
}

func TestGitHubCommitRejectsUnresolved(t *testing.T) {
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", LiteLLMOwner, LiteLLMRepo, LiteLLMBranch)
	fetcher := &fakeCatalogFetcher{responses: map[string][]byte{
		apiURL: []byte(`{"sha": ""}`),
	}}
	resolver := NewGitHubCatalogCommitResolver(fetcher)

	// Empty sha: unresolved commit must be rejected.
	_, err := resolver.ResolveCommit(context.Background(), LiteLLMOwner, LiteLLMRepo, LiteLLMBranch)
	require.Error(t, err)

	// Non-hex garbage sha: rejected.
	fetcher.responses[apiURL] = []byte(`{"sha": "../etc/passwd"}`)
	_, err = resolver.ResolveCommit(context.Background(), LiteLLMOwner, LiteLLMRepo, LiteLLMBranch)
	require.Error(t, err)

	// Fetch failure propagates.
	fetcher.err = fmt.Errorf("github down")
	_, err = resolver.ResolveCommit(context.Background(), LiteLLMOwner, LiteLLMRepo, LiteLLMBranch)
	require.Error(t, err)

	// Malformed JSON: rejected.
	fetcher.err = nil
	fetcher.responses[apiURL] = []byte(`not-json`)
	_, err = resolver.ResolveCommit(context.Background(), LiteLLMOwner, LiteLLMRepo, LiteLLMBranch)
	require.Error(t, err)
}

func TestRawGitHubURLPinnedToCommit(t *testing.T) {
	url, err := RawGitHubURL(ModelsDevOwner, ModelsDevRepo, fakeCommitSHA, ModelsDevDataPath)
	require.NoError(t, err)
	require.Equal(
		t,
		fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", ModelsDevOwner, ModelsDevRepo, fakeCommitSHA, ModelsDevDataPath),
		url,
	)
	// The pinned URL must satisfy the catalog URL rules (allowlisted host).
	require.NoError(t, ValidateCatalogURL(url))

	// Mutable branch refs are not acceptable as pin targets here; callers pass commit SHAs.
	_, err = RawGitHubURL(ModelsDevOwner, ModelsDevRepo, "main", ModelsDevDataPath)
	require.Error(t, err, "branch ref must not be used as an immutable pin")

	// Traversal segments rejected.
	_, err = RawGitHubURL(ModelsDevOwner, "..", fakeCommitSHA, ModelsDevDataPath)
	require.Error(t, err)
	_, err = RawGitHubURL(ModelsDevOwner, ModelsDevRepo, fakeCommitSHA, "a/../../etc/passwd")
	require.Error(t, err)

	// Unknown owner/repo combos are still constructible only when the resulting
	// host is allowlisted; path/host validation covers abuse otherwise.
	url, err = RawGitHubURL(LiteLLMOwner, LiteLLMRepo, fakeCommitSHA, LiteLLMDataPath)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(url, "https://raw.githubusercontent.com/"))
}
