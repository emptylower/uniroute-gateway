package service

import "testing"

func TestAPIKeyService_RejectsV10AuthSnapshotWithoutModelsListConfig(t *testing.T) {
	groupID := int64(9)
	svc := &APIKeyService{}

	apiKey, ok, err := svc.applyAuthCacheEntry("k-legacy-models-list", &APIKeyAuthCacheEntry{
		Snapshot: &APIKeyAuthSnapshot{
			Version:  10,
			APIKeyID: 1,
			UserID:   2,
			GroupID:  &groupID,
			Status:   StatusActive,
			User: APIKeyAuthUserSnapshot{
				ID:          2,
				Status:      StatusActive,
				Role:        RoleUser,
				Balance:     10,
				Concurrency: 3,
			},
			Group: &APIKeyAuthGroupSnapshot{
				ID:               groupID,
				Name:             "openai",
				Platform:         PlatformOpenAI,
				Status:           StatusActive,
				SubscriptionType: SubscriptionTypeStandard,
				RateMultiplier:   1,
			},
		},
	})

	if err != nil {
		t.Fatalf("expected stale snapshot to be ignored without error, got %v", err)
	}
	if ok {
		t.Fatalf("expected v10 auth snapshot to be rejected after models_list_config was added")
	}
	if apiKey != nil {
		t.Fatalf("expected no API key from stale snapshot, got %#v", apiKey)
	}
}

func TestAPIKeyService_RejectsV15AuthSnapshotWithoutReasoningEffortPolicy(t *testing.T) {
	svc := &APIKeyService{}

	apiKey, ok, err := svc.applyAuthCacheEntry("k-legacy-reasoning-mappings", &APIKeyAuthCacheEntry{
		Snapshot: &APIKeyAuthSnapshot{Version: 15},
	})

	if err != nil {
		t.Fatalf("expected stale snapshot to be ignored without error, got %v", err)
	}
	if ok {
		t.Fatal("expected v15 auth snapshot to be rejected after reasoning effort policy was added")
	}
	if apiKey != nil {
		t.Fatalf("expected no API key from stale snapshot, got %#v", apiKey)
	}
}

func TestAPIKeyService_AuthSnapshotPreservesBillingCurrencyAndCurrencyRates(t *testing.T) {
	cny, usd := 0.2, 0.4
	svc := &APIKeyService{}
	snapshot := svc.snapshotFromAPIKey(t.Context(), &APIKey{
		ID: 1, UserID: 2, Status: StatusActive,
		User: &User{ID: 2, Status: StatusActive, BillingCurrency: CurrencyCNY},
		Group: &Group{
			ID: 3, Status: StatusActive, RateMultiplier: 1,
			RateMultiplierCNY: &cny, RateMultiplierUSD: &usd,
		},
	})

	if snapshot == nil {
		t.Fatal("expected snapshot")
	}
	if snapshot.Version != apiKeyAuthSnapshotVersion {
		t.Fatalf("expected version %d, got %d", apiKeyAuthSnapshotVersion, snapshot.Version)
	}
	restored := svc.snapshotToAPIKey("key", snapshot)
	if restored.User.BillingCurrency != CurrencyCNY {
		t.Fatalf("expected CNY billing currency, got %q", restored.User.BillingCurrency)
	}
	if got := restored.Group.RateMultiplierForCurrency(CurrencyCNY); got != cny {
		t.Fatalf("expected CNY multiplier %v, got %v", cny, got)
	}
	if got := restored.Group.RateMultiplierForCurrency(CurrencyUSD); got != usd {
		t.Fatalf("expected USD multiplier %v, got %v", usd, got)
	}
}
