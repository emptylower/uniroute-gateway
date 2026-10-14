package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// PublicMediaPriceSnapshot is a read-only projection of the actual create/quote
// catalog, including its producer mode. No user identity, provider key or
// supplier price participates in this default-retail discovery endpoint.
type PublicMediaPriceSnapshot struct {
	Models          []MediaModel `json:"models"`
	Currency        string       `json:"currency"`
	PriceScope      string       `json:"price_scope"`
	GeneratedAt     time.Time    `json:"generated_at"`
	SnapshotVersion string       `json:"snapshot_version"`
}

func NewPublicMediaPriceSnapshot(now time.Time) PublicMediaPriceSnapshot {
	models := PublicMediaTaskCatalog()
	if models == nil {
		models = []MediaModel{}
	}
	content, _ := json.Marshal(models)
	hash := sha256.Sum256(content)
	return PublicMediaPriceSnapshot{Models: models, Currency: CurrencyUSD, PriceScope: "default_retail_media_quotes", GeneratedAt: now.UTC(), SnapshotVersion: "sha256:" + hex.EncodeToString(hash[:])}
}
