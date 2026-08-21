package service

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// GovernanceModeProvider determines effective governance mode from DB activations.
type GovernanceModeProvider interface {
	IsEnforce(ctx context.Context) bool
	CurrentMode(ctx context.Context) string
}

// dbGovernanceModeProvider reads latest model_authorization_activations row.
type dbGovernanceModeProvider struct {
	db    *sql.DB
	cfg   string // boot cap: "off" or "shadow"; never "enforce" via config
	mu    sync.RWMutex
	cachedMode string
	cachedAt   time.Time
	ttl        time.Duration
}

func NewGovernanceModeProvider(db *sql.DB, bootMode string) GovernanceModeProvider {
	if bootMode != "off" && bootMode != "shadow" {
		bootMode = "off"
	}
	return &dbGovernanceModeProvider{db: db, cfg: bootMode, ttl: 5 * time.Second}
}

func (p *dbGovernanceModeProvider) CurrentMode(ctx context.Context) string {
	if p.db == nil {
		return p.cfg
	}
	// Fast path: cached
	p.mu.RLock()
	if time.Since(p.cachedAt) < p.ttl && p.cachedMode != "" {
		mode := p.cachedMode
		p.mu.RUnlock()
		return mode
	}
	p.mu.RUnlock()

	var modeAfter sql.NullString
	err := p.db.QueryRowContext(ctx, `SELECT mode_after FROM model_authorization_activations ORDER BY created_at DESC, id DESC LIMIT 1`).Scan(&modeAfter)
	mode := p.cfg
	if err == nil && modeAfter.Valid && modeAfter.String != "" {
		// Only enforce is allowed via activation; off/shadow via activation overrides boot cap.
		switch modeAfter.String {
		case "enforce", "shadow", "off":
			mode = modeAfter.String
		}
	}
	// Cache
	p.mu.Lock()
	p.cachedMode = mode
	p.cachedAt = time.Now()
	p.mu.Unlock()
	return mode
}

func (p *dbGovernanceModeProvider) IsEnforce(ctx context.Context) bool {
	return p.CurrentMode(ctx) == "enforce"
}

func (p *dbGovernanceModeProvider) Invalidate() {
	p.mu.Lock()
	p.cachedMode = ""
	p.cachedAt = time.Time{}
	p.mu.Unlock()
}
