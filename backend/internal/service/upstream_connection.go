package service

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// UpstreamConnection mirrors the persisted connection identity (Phase 4).
type UpstreamConnection struct {
	ID                int64
	Kind              string
	Provider          *GovernanceProvider
	BaseURL           string
	CredentialVersion int64
	ProxyID           *int64
	Status            string
	EvidenceRef       *string
}

type AccountProtocol string

const (
	AccountProtocolAnthropic AccountProtocol = "anthropic"
	AccountProtocolOpenAI    AccountProtocol = "openai"
	AccountProtocolGemini    AccountProtocol = "gemini"
)

func ValidAccountProtocol(p AccountProtocol) bool {
	switch p {
	case AccountProtocolAnthropic, AccountProtocolOpenAI, AccountProtocolGemini:
		return true
	default:
		return false
	}
}

// UpstreamConnectionRepository persists encrypted connections.
type UpstreamConnectionRepository interface {
	Create(ctx context.Context, conn *UpstreamConnection, encryptedCredential string) error
	GetByID(ctx context.Context, id int64) (*UpstreamConnection, string, error)
	UpdateCredential(ctx context.Context, id int64, expectedVersion int64, encryptedCredential string) (int64, error)
	BatchGetByIDs(ctx context.Context, ids []int64) (map[int64]*UpstreamConnection, error)
	ListAll(ctx context.Context) ([]*UpstreamConnection, error)
	UpdateStatus(ctx context.Context, id int64, status string) error
}

// AggregatorDesignationRepository extends connection repo with designation support.
type AggregatorDesignationRepository interface {
	UpstreamConnectionRepository
	FindEventByIdempotencyKey(ctx context.Context, key string) (bool, error)
	TransitionToAggregator(ctx context.Context, id int64, expectedVersion int64, evidenceRef, actorID, idempotencyKey string) error
	CountAccountsByConnectionID(ctx context.Context, connectionID int64) (int, error)
}

// UpstreamConnectionService enforces kind/provider affinity, URL normalization, encryption, and DTO redaction.
type UpstreamConnectionService struct {
	repo      UpstreamConnectionRepository
	encryptor SecretEncryptor
}

func NewUpstreamConnectionService(repo UpstreamConnectionRepository, encryptor SecretEncryptor) *UpstreamConnectionService {
	return &UpstreamConnectionService{repo: repo, encryptor: encryptor}
}

func (s *UpstreamConnectionService) ValidateKindProvider(kind string, provider *GovernanceProvider) error {
	switch kind {
	case "first_party":
		if provider == nil || !ValidGovernanceProvider(*provider) {
			return fmt.Errorf("first_party requires a governed provider")
		}
	case "aggregator":
		if provider != nil {
			return fmt.Errorf("aggregator must have null provider")
		}
	default:
		return fmt.Errorf("invalid kind %q", kind)
	}
	return nil
}

func NormalizeBaseURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("base_url is empty")
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid base_url: %w", err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("base_url must be http or https")
	}
	if u.Host == "" {
		return "", fmt.Errorf("base_url missing host")
	}
	// Normalize: trim trailing slash, lower scheme/host
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	path := strings.TrimRight(u.Path, "/")
	if path == "" {
		path = ""
	}
	u.Path = path
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func NormalizeEndpointPath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("endpoint path is empty")
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	// Remove trailing slash except root
	if len(trimmed) > 1 {
		trimmed = strings.TrimRight(trimmed, "/")
	}
	// Lowercase for deterministic matching (endpoints are case-sensitive per spec? keep case but normalize)
	return trimmed, nil
}

func (s *UpstreamConnectionService) Create(ctx context.Context, kind string, provider *GovernanceProvider, baseURL string, credential string, proxyID *int64) (*UpstreamConnection, error) {
	if err := s.ValidateKindProvider(kind, provider); err != nil {
		return nil, err
	}
	normalizedURL, err := NormalizeBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(credential) == "" {
		return nil, fmt.Errorf("credential is empty")
	}
	if s.encryptor == nil {
		return nil, fmt.Errorf("encryptor not configured")
	}
	enc, err := s.encryptor.Encrypt(credential)
	if err != nil {
		return nil, fmt.Errorf("encrypt credential: %w", err)
	}
	conn := &UpstreamConnection{
		Kind:              kind,
		Provider:          provider,
		BaseURL:           normalizedURL,
		CredentialVersion: 1,
		ProxyID:           proxyID,
		Status:            "active",
	}
	if s.repo != nil {
		if err := s.repo.Create(ctx, conn, enc); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (s *UpstreamConnectionService) RotateCredential(ctx context.Context, id int64, expectedVersion int64, newCredential string) (int64, error) {
	if strings.TrimSpace(newCredential) == "" {
		return 0, fmt.Errorf("credential is empty")
	}
	if s.encryptor == nil {
		return 0, fmt.Errorf("encryptor not configured")
	}
	enc, err := s.encryptor.Encrypt(newCredential)
	if err != nil {
		return 0, fmt.Errorf("encrypt credential: %w", err)
	}
	if s.repo == nil {
		return expectedVersion + 1, nil
	}
	newVersion, err := s.repo.UpdateCredential(ctx, id, expectedVersion, enc)
	if err != nil {
		return 0, err
	}
	_ = enc
	return newVersion, nil
}

// RedactedDTO hides encrypted credential.
type UpstreamConnectionDTO struct {
	ID                int64             `json:"id"`
	Kind              string            `json:"kind"`
	Provider          *GovernanceProvider `json:"provider"`
	BaseURL           string            `json:"base_url"`
	CredentialVersion int64             `json:"credential_version"`
	ProxyID           *int64            `json:"proxy_id"`
	Status            string            `json:"status"`
	CredentialRedacted bool             `json:"credential_redacted"`
}

func (s *UpstreamConnectionService) ToDTO(conn *UpstreamConnection) *UpstreamConnectionDTO {
	if conn == nil {
		return nil
	}
	return &UpstreamConnectionDTO{
		ID:                conn.ID,
		Kind:              conn.Kind,
		Provider:          conn.Provider,
		BaseURL:           conn.BaseURL,
		CredentialVersion: conn.CredentialVersion,
		ProxyID:           conn.ProxyID,
		Status:            conn.Status,
		CredentialRedacted: true,
	}
}

// BatchLoadConnections retains legacy fallback only for unmigrated rows (connection_id == nil).
func (s *UpstreamConnectionService) BatchLoad(ctx context.Context, accountConnectionIDs map[int64]*int64) (map[int64]*UpstreamConnection, error) {
	ids := make([]int64, 0)
	for _, cid := range accountConnectionIDs {
		if cid != nil {
			ids = append(ids, *cid)
		}
	}
	if len(ids) == 0 || s.repo == nil {
		return map[int64]*UpstreamConnection{}, nil
	}
	return s.repo.BatchGetByIDs(ctx, ids)
}
