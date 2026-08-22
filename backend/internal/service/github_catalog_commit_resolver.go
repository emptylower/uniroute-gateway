package service

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// models.dev and LiteLLM publish on mutable branches; ingestion must resolve
// the branch to an immutable commit before fetching raw payloads.
const (
	ModelsDevOwner    = "models-dev"
	ModelsDevRepo     = "models.dev"
	ModelsDevBranch   = "main"
	ModelsDevDataPath = "api.json"

	LiteLLMOwner    = "BerriAI"
	LiteLLMRepo     = "litellm"
	LiteLLMBranch   = "main"
	LiteLLMDataPath = "model_prices_and_context_window.json"

	githubCommitSHALen = 40
)

var (
	segmentPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	commitPattern  = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// CatalogFetcher is the bounded fetch capability used by catalog tooling.
type CatalogFetcher interface {
	Get(ctx context.Context, rawURL string) ([]byte, error)
}

// GitHubCatalogCommitResolver pins mutable branch refs to immutable commit SHAs
// through the allowlisted api.github.com host.
type GitHubCatalogCommitResolver struct {
	fetcher CatalogFetcher
}

// NewGitHubCatalogCommitResolver builds the resolver on top of a bounded fetcher.
func NewGitHubCatalogCommitResolver(fetcher CatalogFetcher) *GitHubCatalogCommitResolver {
	return &GitHubCatalogCommitResolver{fetcher: fetcher}
}

// ResolveCommit returns the current commit SHA for owner/repo/ref.
func (r *GitHubCatalogCommitResolver) ResolveCommit(ctx context.Context, owner, repo, ref string) (string, error) {
	if r == nil || r.fetcher == nil {
		return "", fmt.Errorf("github commit resolver is not configured")
	}
	for _, segment := range []string{owner, repo, ref} {
		if !segmentPattern.MatchString(segment) {
			return "", fmt.Errorf("invalid github ref segment %q", segment)
		}
	}
	rawURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", owner, repo, ref)
	body, err := r.fetcher.Get(ctx, rawURL)
	if err != nil {
		return "", fmt.Errorf("resolve github commit: %w", err)
	}
	var payload struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("invalid github commit response: %w", err)
	}
	sha := strings.TrimSpace(payload.SHA)
	if !commitPattern.MatchString(sha) {
		return "", fmt.Errorf("unresolved github commit for %s/%s@%s", owner, repo, ref)
	}
	return sha, nil
}

// RawGitHubURL builds the pinned raw content URL for one immutable commit and
// validates it against the catalog URL rules. The ref argument must be a full
// commit SHA — branch names are rejected because they are mutable.
func RawGitHubURL(owner, repo, commit, path string) (string, error) {
	for _, segment := range []string{owner, repo} {
		if !segmentPattern.MatchString(segment) || segment == "." || segment == ".." {
			return "", fmt.Errorf("invalid github path segment %q", segment)
		}
	}
	if !commitPattern.MatchString(commit) {
		return "", fmt.Errorf("ref %q is not an immutable commit sha", commit)
	}
	cleanPath := strings.TrimPrefix(path, "/")
	for _, segment := range strings.Split(cleanPath, "/") {
		if segment == "" || segment == "." || segment == ".." || !segmentPattern.MatchString(segment) {
			return "", fmt.Errorf("invalid github data path segment %q", segment)
		}
	}
	rawURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", owner, repo, commit, cleanPath)
	if err := ValidateCatalogURL(rawURL); err != nil {
		return "", err
	}
	return rawURL, nil
}
