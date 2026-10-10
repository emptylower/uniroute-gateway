package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

const walletReaderJournalBound = 32 * 1024

var errWalletReaderFenced = errors.New("wallet reader is durably fenced")

type walletReaderJournalOwner struct {
	dir, host, id string
	conn          *sql.Conn
}
type walletReaderJournalRecord struct {
	Version           int                        `json:"version"`
	Host              string                     `json:"host"`
	OwnerID           string                     `json:"owner_id"`
	AuthorizationID   string                     `json:"authorization_id"`
	Token             string                     `json:"token"`
	SnapshotID        string                     `json:"snapshot_id"`
	Normalization     *WalletReaderNormalization `json:"normalization,omitempty"`
	LegacyStatus      int                        `json:"legacy_status"`
	Started           bool                       `json:"started"`
	Joined            bool                       `json:"joined"`
	Sealed            bool                       `json:"sealed"`
	PersistenceFailed bool                       `json:"persistence_failed"`
	Evidence          WalletReaderEvidence       `json:"evidence"`
	PendingUsage      json.RawMessage            `json:"pending_usage,omitempty"`
	PendingSource     string                     `json:"pending_source,omitempty"`
}
type walletReaderJournal struct {
	dir, path string
	mu        sync.Mutex
	lock      *os.File
	record    walletReaderJournalRecord
}

// fsync both the new file and its containing directory. Atomic rename keeps a
// crash from replacing the previous complete evidence with a truncated record.
func writeWalletReaderJournal(dir, path string, record any) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(raw) > walletReaderJournalBound {
		return errors.New("wallet journal exceeds evidence bound")
	}
	file, err := os.CreateTemp(dir, ".wallet-reader-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer func() { _ = os.Remove(name) }()
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func walletReaderJournalDirectory(dir string) (string, error) {
	if dir == "" || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return "", errors.New("wallet reader durable directory is not configured")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("wallet reader directory must be a real mounted directory")
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".directory.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return "", err
	}
	defer func() { _ = lock.Close() }()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return "", err
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	path := filepath.Join(dir, ".identity.json")
	var identity struct {
		Version int    `json:"version"`
		Host    string `json:"host"`
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		identity.Version = 1
		identity.Host = uuid.NewString()
		err = writeWalletReaderJournal(dir, path, identity)
	} else if err == nil {
		err = json.Unmarshal(raw, &identity)
	}
	if err != nil {
		return "", err
	}
	if identity.Version != 1 || identity.Host == "" || len(identity.Host) > 128 {
		return "", errors.New("wallet reader volume identity is corrupt")
	}
	// Read-only/full disks must refuse new money before the first hold.
	probe, err := os.CreateTemp(dir, ".prehold-probe-*")
	if err != nil {
		return "", err
	}
	probePath := probe.Name()
	defer func() { _ = os.Remove(probePath) }()
	_, err = probe.Write([]byte(identity.Host))
	if err == nil {
		err = probe.Sync()
	}
	closeErr := probe.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return "", err
	}
	defer func() { _ = directory.Close() }()
	if err = directory.Sync(); err != nil {
		return "", err
	}
	return identity.Host, nil
}

func (b *CanonicalWalletBridge) ensureWalletReaderJournal(ctx context.Context) error {
	b.readerJournalMu.Lock()
	defer b.readerJournalMu.Unlock()
	refuse := func(err error) error {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: err}
	}
	host, err := walletReaderJournalDirectory(b.cfg.ReaderJournalDirectory)
	if err != nil {
		return refuse(err)
	}
	if err = b.verifyWalletReaderJournalVolume(ctx, host); err != nil {
		return refuse(err)
	}
	if b.readerJournalOwner != nil {
		if host != b.readerJournalOwner.host || b.cfg.ReaderJournalDirectory != b.readerJournalOwner.dir {
			return refuse(errors.New("wallet reader volume changed"))
		}
		if err = b.readerJournalOwner.conn.PingContext(ctx); err == nil {
			return nil
		}
		_ = b.readerJournalOwner.conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = b.readerJournalOwner.conn.Close()
		b.readerJournalOwner = nil
	}
	if b.outboxDB == nil {
		return refuse(errors.New("wallet reader owner database missing"))
	}
	conn, err := b.outboxDB.Conn(ctx)
	if err != nil {
		return refuse(err)
	}
	owner := &walletReaderJournalOwner{dir: b.cfg.ReaderJournalDirectory, host: host, id: uuid.NewString(), conn: conn}
	if _, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1,0))`, "wallet-reader-owner:"+owner.id); err != nil {
		_ = conn.Close()
		return refuse(err)
	}
	b.readerJournalOwner = owner
	return nil
}

// A Gateway database owns one durable reader volume. All instances may share
// that volume and its OS locks, while a fresh or different mount must never
// replace the registered UUID merely because a container was recreated.
func (b *CanonicalWalletBridge) verifyWalletReaderJournalVolume(ctx context.Context, host string) error {
	if b.outboxDB == nil {
		return errors.New("wallet reader owner database missing")
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('wallet-reader-volume-registry',0))`); err != nil {
		return err
	}
	var historicalMismatch bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment WHERE reader_journal_host IS NOT NULL AND reader_journal_host<>$1 AND (terminal_sealed_at IS NULL OR evidence_pending OR fee_pending OR state<>'finished'))`, host).Scan(&historicalMismatch); err != nil {
		return err
	}
	if historicalMismatch {
		return errors.New("wallet reader volume differs from unfinished durable readers")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO wallet_reader_journal_volume(singleton,volume_id) VALUES(true,$1) ON CONFLICT(singleton) DO NOTHING`, host); err != nil {
		return err
	}
	var registered string
	if err = tx.QueryRowContext(ctx, `SELECT volume_id FROM wallet_reader_journal_volume WHERE singleton=true`).Scan(&registered); err != nil {
		return err
	}
	if registered != host {
		return errors.New("wallet reader mounted volume does not match the Gateway database")
	}
	return tx.Commit()
}

func walletReaderJournalPath(dir, parent, token string) string {
	sum := sha256.Sum256([]byte(parent + "\x00" + token))
	return filepath.Join(dir, hex.EncodeToString(sum[:])+".json")
}
func (b *CanonicalWalletBridge) startWalletReaderJournal(ctx context.Context, h *AuthorizationHandle, token string) error {
	if b.ImmediateWalletReleaseMode("llm") != "enabled" || h.AttemptKind != "llm" {
		return nil
	}
	if err := b.ensureWalletReaderJournal(ctx); err != nil {
		return err
	}
	b.readerJournalMu.Lock()
	owner := b.readerJournalOwner
	record := walletReaderJournalRecord{Version: 1, Host: owner.host, OwnerID: owner.id, AuthorizationID: h.ID, Token: token, SnapshotID: h.SnapshotID}
	dir := owner.dir
	b.readerJournalMu.Unlock()
	h.mu.Lock()
	if h.readerNormalization != nil {
		facts := *h.readerNormalization
		record.Normalization = &facts
	}
	h.mu.Unlock()
	journal := &walletReaderJournal{dir: dir, path: walletReaderJournalPath(dir, h.ID, token), record: record}
	if err := journal.acquire(false); err != nil {
		return err
	}
	defer journal.release()
	if _, err := os.Stat(journal.path); err == nil {
		return errors.New("wallet reader journal identity already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := journal.saveLocked(); err != nil {
		return err
	}
	h.mu.Lock()
	h.readerJournal = journal
	h.mu.Unlock()
	return nil
}

func (j *walletReaderJournal) acquire(nonblocking bool) error {
	j.mu.Lock()
	file, err := os.OpenFile(j.path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		j.mu.Unlock()
		return err
	}
	flags := syscall.LOCK_EX
	if nonblocking {
		flags |= syscall.LOCK_NB
	}
	if err = syscall.Flock(int(file.Fd()), flags); err != nil {
		_ = file.Close()
		j.mu.Unlock()
		return err
	}
	j.lock = file
	return nil
}
func (j *walletReaderJournal) release() {
	if j.lock != nil {
		_ = syscall.Flock(int(j.lock.Fd()), syscall.LOCK_UN)
		_ = j.lock.Close()
		j.lock = nil
	}
	j.mu.Unlock()
}
func (j *walletReaderJournal) loadLocked() error {
	file, err := os.Open(j.path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	raw, err := io.ReadAll(io.LimitReader(file, walletReaderJournalBound+1))
	if err != nil {
		return err
	}
	if len(raw) > walletReaderJournalBound {
		return errors.New("wallet journal oversized")
	}
	var record walletReaderJournalRecord
	if err = json.Unmarshal(raw, &record); err != nil {
		return err
	}
	if record.Version != 1 || record.Host != j.record.Host || record.OwnerID != j.record.OwnerID || record.AuthorizationID != j.record.AuthorizationID || record.Token != j.record.Token || record.SnapshotID != j.record.SnapshotID {
		return errors.New("wallet journal identity mismatch")
	}
	j.record = record
	return nil
}
func (j *walletReaderJournal) saveLocked() error {
	return writeWalletReaderJournal(j.dir, j.path, j.record)
}
func (j *walletReaderJournal) beginRead() error {
	if err := j.acquire(false); err != nil {
		return err
	}
	if err := j.loadLocked(); err != nil {
		j.release()
		return err
	}
	if j.record.Sealed || j.record.Joined {
		j.release()
		return errWalletReaderFenced
	}
	j.record.Started = true
	if err := j.saveLocked(); err != nil {
		j.release()
		return err
	}
	return nil
}
func (j *walletReaderJournal) observeLocked(e WalletReaderEvidence, failed bool) error {
	if j.record.Sealed {
		return errWalletReaderFenced
	}
	// Keep the normalized cumulative maximum and positive failure latch.
	j.record.Evidence = e
	j.record.PendingUsage = nil
	j.record.PendingSource = ""
	j.record.PersistenceFailed = j.record.PersistenceFailed || failed
	return j.saveLocked()
}

// Checkpoint only fields understood by the selected parser, before parsing a
// positive value or forwarding it. Output text, credentials, arbitrary source,
// raw credits and raw-token estimates are never journaled or priced.
func selectedWalletUsageFrame(raw []byte, facts ...*WalletReaderNormalization) ([]byte, error) {
	if !gjson.ValidBytes(raw) {
		return nil, nil
	}
	root := gjson.ParseBytes(raw)
	kind := ""
	if len(facts) > 0 && facts[0] != nil {
		kind = facts[0].CountKind
	}
	counts := selectedWalletImageCounts(raw, kind)
	path, usage := walletSelectedUsage(root)
	if path == "" && counts == nil {
		return nil, nil
	}
	var selected any
	if path != "" && !usage.IsObject() {
		selected = json.RawMessage(usage.Raw)
	} else {
		fields := map[string]any{}
		for _, field := range []string{"input_tokens", "output_tokens", "prompt_tokens", "completion_tokens", "total_tokens", "completion_tokens_details.reasoning_tokens", "inputTokens", "outputTokens", "cache_creation_input_tokens", "cache_read_input_tokens", "image_input_tokens", "image_output_tokens", "promptTokenCount", "candidatesTokenCount", "cachedContentTokenCount", "thoughtsTokenCount", "totalTokenCount", "toolUsePromptTokenCount", "input_tokens_details.cached_tokens", "prompt_tokens_details.cached_tokens", "input_tokens_details.cache_write_tokens", "prompt_tokens_details.cache_write_tokens", "input_tokens_details.cache_creation_tokens", "prompt_tokens_details.cache_creation_tokens", "cache_write_tokens", "cache_write_input_tokens", "cache_creation_tokens", "input_tokens_details.image_tokens", "output_tokens_details.image_tokens", "cache_creation.ephemeral_5m_input_tokens", "cache_creation.ephemeral_1h_input_tokens"} {
			value := usage.Get(field)
			if !value.Exists() {
				continue
			}
			parts := strings.Split(field, ".")
			if len(parts) == 1 {
				fields[field] = json.RawMessage(value.Raw)
			} else {
				nested, _ := fields[parts[0]].(map[string]any)
				if nested == nil {
					nested = map[string]any{}
					fields[parts[0]] = nested
				}
				nested[parts[1]] = json.RawMessage(value.Raw)
			}
		}
		selected = fields
	}
	envelope := map[string]any{"type": root.Get("type").String()}
	for _, id := range []string{"id", "response_id", "responseId"} {
		if value := root.Get(id); value.Type == gjson.String {
			envelope[id] = value.String()
		}
	}
	if counts != nil {
		envelope["_wallet_selected_counts"] = counts
	}
	if path == "response.usage" {
		envelope["response"] = map[string]any{"id": root.Get("response.id").String(), "usage": selected}
	} else if path == "message.usage" {
		envelope["message"] = map[string]any{"usage": selected}
	} else if path == "usageMetadata" || path == "response.usageMetadata" {
		native := root
		if path == "response.usageMetadata" {
			native = root.Get("response")
		}
		body := map[string]any{"usageMetadata": selected}
		if id := native.Get("id"); id.Type == gjson.String {
			body["id"] = id.String()
		}
		if id := native.Get("responseId"); id.Type == gjson.String {
			body["responseId"] = id.String()
		}
		if block := native.Get("promptFeedback.blockReason"); block.Exists() {
			body["promptFeedback"] = map[string]any{"blockReason": json.RawMessage(block.Raw)}
		}
		if candidates := native.Get("candidates"); candidates.IsArray() {
			var terminal []map[string]any
			for _, candidate := range candidates.Array() {
				proof := map[string]any{}
				if finish := candidate.Get("finishReason"); finish.Exists() {
					proof["finishReason"] = json.RawMessage(finish.Raw)
				}
				terminal = append(terminal, proof)
			}
			body["candidates"] = terminal
		}
		if path == "response.usageMetadata" {
			envelope["response"] = body
		} else {
			for field, value := range body {
				envelope[field] = value
			}
		}
	} else if path != "" {
		envelope[path] = selected
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(encoded) > 8*1024 {
		return nil, errors.New("selected wallet usage checkpoint exceeds bound")
	}
	return encoded, nil
}

func (j *walletReaderJournal) checkpointLocked(raw []byte, source string) error {
	if j.record.Sealed || j.record.Joined {
		return errWalletReaderFenced
	}
	selected, err := selectedWalletUsageFrame(raw, j.record.Normalization)
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		return nil
	}
	if source != "llm_http_usage" && source != "llm_ws_usage" {
		return errors.New("wallet checkpoint parser source invalid")
	}
	j.record.PendingUsage = selected
	j.record.PendingSource = source
	return j.saveLocked()
}

func (j *walletReaderJournal) replayCheckpointLocked() error {
	if len(j.record.PendingUsage) == 0 {
		return nil
	}
	if j.record.PendingSource != "llm_http_usage" && j.record.PendingSource != "llm_ws_usage" {
		return errors.New("wallet checkpoint source is corrupt")
	}
	if !gjson.ValidBytes(j.record.PendingUsage) {
		return errors.New("wallet checkpoint usage is corrupt")
	}
	evidence := j.record.Evidence
	evidence.Source = j.record.PendingSource
	if j.record.PendingSource == "llm_ws_usage" {
		observeWalletWSUsage(j.record.PendingUsage, &evidence)
	} else {
		observeWalletUsage(j.record.PendingUsage, &evidence, j.record.Normalization)
	}
	if err := replayWalletReaderCounts(j.record.PendingUsage, &evidence); err != nil {
		return err
	}
	j.record.Evidence = evidence
	j.record.PendingUsage = nil
	j.record.PendingSource = ""
	return j.saveLocked()
}
func (j *walletReaderJournal) updateHeader(status int) error {
	if err := j.acquire(false); err != nil {
		return err
	}
	defer j.release()
	if err := j.loadLocked(); err != nil {
		return err
	}
	if err := j.replayCheckpointLocked(); err != nil {
		return err
	}
	if j.record.Sealed {
		return errWalletReaderFenced
	}
	j.record.LegacyStatus = status
	return j.saveLocked()
}
func (j *walletReaderJournal) transfer(e WalletReaderEvidence, fn func(WalletReaderEvidence) error) error {
	if err := j.acquire(false); err != nil {
		return err
	}
	defer j.release()
	pending := append(json.RawMessage(nil), j.record.PendingUsage...)
	pendingSource := j.record.PendingSource
	if err := j.loadLocked(); err != nil {
		return err
	}
	if j.record.Sealed {
		return errWalletReaderFenced
	}
	// A checkpoint write may have failed before parsing. The live owner can
	// retry that selected raw checkpoint, never replace it with an empty zero.
	if len(pending) > 0 && len(j.record.PendingUsage) == 0 {
		j.record.PendingUsage = pending
		j.record.PendingSource = pendingSource
		if err := j.saveLocked(); err != nil {
			return err
		}
	}
	replayedGemini := false
	if facts := j.record.Normalization; facts != nil && facts.TokenOnly && facts.CountKind == "" && j.record.PendingSource == "llm_http_usage" && (facts.ProviderPlatform == PlatformGemini || facts.ProviderPlatform == PlatformAntigravity) {
		path, _ := walletSelectedUsage(gjson.ParseBytes(j.record.PendingUsage))
		replayedGemini = path == "usageMetadata" || path == "response.usageMetadata"
	}
	if err := j.replayCheckpointLocked(); err != nil {
		return err
	}
	if j.record.Sealed {
		return errWalletReaderFenced
	}
	if j.record.Evidence.ObservedPositive && !e.ObservedPositive || replayedGemini && !e.Malformed && (e.Counts == nil || !e.Counts.Malformed) {
		// The failed checkpoint precedes live terminal parsing. Its replay owns
		// the terminal proof and cumulative native cache split, even if the live
		// partial was already positive. Keep the live reader completion flag.
		complete := e.Complete
		e = j.record.Evidence
		e.Complete = e.Complete || complete
	}
	if err := mergeWalletReaderCounts(&e, j.record.Evidence.Counts); err != nil {
		return err
	}
	e.Tokens.InputTokens = max(e.Tokens.InputTokens, j.record.Evidence.Tokens.InputTokens)
	e.RawInputTokens = max(e.RawInputTokens, j.record.Evidence.RawInputTokens)
	e.Tokens.OutputTokens = max(e.Tokens.OutputTokens, j.record.Evidence.Tokens.OutputTokens)
	e.Tokens.CacheCreationTokens = max(e.Tokens.CacheCreationTokens, j.record.Evidence.Tokens.CacheCreationTokens)
	e.Tokens.CacheReadTokens = max(e.Tokens.CacheReadTokens, j.record.Evidence.Tokens.CacheReadTokens)
	e.Tokens.ImageInputTokens = max(e.Tokens.ImageInputTokens, j.record.Evidence.Tokens.ImageInputTokens)
	e.Tokens.ImageOutputTokens = max(e.Tokens.ImageOutputTokens, j.record.Evidence.Tokens.ImageOutputTokens)
	e.Tokens.CacheCreation5mTokens = max(e.Tokens.CacheCreation5mTokens, j.record.Evidence.Tokens.CacheCreation5mTokens)
	e.Tokens.CacheCreation1hTokens = max(e.Tokens.CacheCreation1hTokens, j.record.Evidence.Tokens.CacheCreation1hTokens)
	j.record.Evidence = e
	j.record.Joined = true
	if err := j.saveLocked(); err != nil {
		return err
	}
	if err := fn(e); err != nil {
		j.record.PersistenceFailed = true
		_ = j.saveLocked()
		return err
	}
	j.record.PersistenceFailed = false
	return j.saveLocked()
}

func (b *CanonicalWalletBridge) closeWalletReaderJournalOwner() {
	b.readerJournalMu.Lock()
	defer b.readerJournalMu.Unlock()
	if b.readerJournalOwner != nil {
		_, err := b.readerJournalOwner.conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1,0))`, "wallet-reader-owner:"+b.readerJournalOwner.id)
		if err != nil {
			_ = b.readerJournalOwner.conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = b.readerJournalOwner.conn.Close()
		b.readerJournalOwner = nil
	}
}

// An advisory owner session is per bridge/process, not per reader. A recovery
// transaction proves that session no longer owns the attempt, then takes the
// same OS lock used across raw Read+fsync. It fences the next Read before moving
// journal evidence into PG. No elapsed heartbeat is terminal evidence.
func (b *CanonicalWalletBridge) recoverWalletReaderJournals(ctx context.Context) {
	if b.cfg.ReaderJournalDirectory == "" {
		return
	}
	host, err := walletReaderJournalDirectory(b.cfg.ReaderJournalDirectory)
	if err != nil {
		return
	}
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT parent_authorization_id,platform_user_id,billing_snapshot_id,authorization_token,reader_owner_id,reader_journal_host FROM wallet_authorization_segment WHERE kind='llm' AND ordinal=0 AND terminal_sealed_at IS NULL AND reader_owner_id IS NOT NULL AND authorization_token IS NOT NULL ORDER BY reader_journal_scan_at NULLS FIRST,parent_authorization_id LIMIT 32`)
	if err != nil {
		return
	}
	type candidate struct{ parent, user, snapshot, token, owner, host string }
	items := []candidate{}
	for rows.Next() {
		var item candidate
		if rows.Scan(&item.parent, &item.user, &item.snapshot, &item.token, &item.owner, &item.host) != nil {
			break
		}
		items = append(items, item)
	}
	_ = rows.Close()
	for _, item := range items {
		_, _ = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET reader_journal_scan_at=now() WHERE parent_authorization_id=$1 AND ordinal=0`, item.parent)
		if item.host != host {
			continue
		}
		tx, beginErr := b.outboxDB.BeginTx(ctx, nil)
		if beginErr != nil {
			continue
		}
		var orphaned bool
		if tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "wallet-reader-owner:"+item.owner).Scan(&orphaned) != nil || !orphaned {
			_ = tx.Rollback()
			continue
		}
		journal := &walletReaderJournal{dir: b.cfg.ReaderJournalDirectory, path: walletReaderJournalPath(b.cfg.ReaderJournalDirectory, item.parent, item.token), record: walletReaderJournalRecord{Version: 1, Host: item.host, OwnerID: item.owner, AuthorizationID: item.parent, Token: item.token, SnapshotID: item.snapshot}}
		if journal.acquire(true) != nil {
			_ = tx.Rollback()
			continue
		}
		if journal.loadLocked() != nil {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		if journal.replayCheckpointLocked() != nil {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		evidence := journal.record.Evidence
		// This latch records a PG-only failure after the normalized record was
		// fsynced. With an intact owner-bound WAL, recovery can retry transfer.
		if _, err = lockWalletAttempt(ctx, tx, item.parent); err != nil {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		raw, marshalErr := json.Marshal(evidence)
		if marshalErr != nil {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		// Mark the local fence first; a PG failure leaves a recoverable sealed WAL.
		journal.record.Sealed = true
		journal.record.Joined = true
		if journal.saveLocked() != nil {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		var normalization any
		if journal.record.Normalization != nil {
			facts, factsErr := json.Marshal(journal.record.Normalization)
			if factsErr != nil {
				journal.release()
				_ = tx.Rollback()
				continue
			}
			normalization = string(facts)
		}
		_, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET reader_evidence=$3::jsonb,reader_fee_normalization=COALESCE(reader_fee_normalization,$7::jsonb),reader_handoff_at=COALESCE(reader_handoff_at,now()),write_ended_at=COALESCE(write_ended_at,now()),write_active_until=NULL,evidence_pending=true,fee_pending=fee_pending OR $4,legacy_zero_candidate=COALESCE(legacy_zero_candidate,NULLIF($5,0)) WHERE parent_authorization_id=$1 AND authorization_token=$2 AND reader_owner_id=$6 AND terminal_sealed_at IS NULL AND zero_intent_at IS NULL`, item.parent, item.token, string(raw), walletReaderEvidenceNeedsFeeRecovery(evidence), journal.record.LegacyStatus, item.owner, normalization)
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		journal.release()
		if err != nil {
			continue
		}
	}
	b.observeWalletReaderJournal(ctx)
	b.cleanupWalletReaderJournals(ctx)
}

func (b *CanonicalWalletBridge) observeWalletReaderJournal(ctx context.Context) {
	b.readerJournalMu.Lock()
	if time.Since(b.readerJournalLastAlert) < time.Minute {
		b.readerJournalMu.Unlock()
		return
	}
	b.readerJournalLastAlert = time.Now()
	b.readerJournalMu.Unlock()
	var pending, count int64
	var oldest sql.NullFloat64
	if b.outboxDB.QueryRowContext(ctx, `SELECT count(*) FILTER(WHERE terminal_sealed_at IS NULL),count(*) FILTER(WHERE reader_journal_cleanup_at IS NULL),EXTRACT(epoch FROM now()-min(first_write_at) FILTER(WHERE terminal_sealed_at IS NULL)) FROM wallet_authorization_segment WHERE ordinal=0 AND reader_owner_id IS NOT NULL`).Scan(&pending, &count, &oldest) != nil {
		return
	}
	if oldest.Valid && oldest.Float64 >= 30 {
		slog.Warn("wallet reader journal recovery pending", "reason_code", "reader_journal_pending", "pending_count", pending, "retained_count", count, "oldest_age_seconds", oldest.Float64)
	}
}

func (b *CanonicalWalletBridge) cleanupWalletReaderJournals(ctx context.Context) {
	retention := b.retentionDays()
	if retention < 45 {
		retention = 45
	}
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT a.parent_authorization_id,a.billing_snapshot_id,a.authorization_token,a.reader_owner_id,a.reader_journal_host FROM wallet_authorization_segment a WHERE a.ordinal=0 AND a.reader_owner_id IS NOT NULL AND a.reader_journal_cleanup_at IS NULL AND a.terminal_sealed_at IS NOT NULL AND COALESCE(a.zero_released_at,a.expiry_released_at,a.terminal_sealed_at)<now()-($1 * interval '1 day') AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment s WHERE s.parent_authorization_id=a.parent_authorization_id AND (s.evidence_pending OR s.fee_pending OR NOT ((s.state='finished' AND (s.actual_units>0 OR s.zero_ack_at IS NOT NULL)) OR (s.expiry_intent_version=2 AND s.expiry_ack_at IS NOT NULL AND s.expiry_cleanup_at IS NOT NULL)))) ORDER BY a.terminal_sealed_at,a.parent_authorization_id LIMIT 32`, retention)
	if err != nil {
		return
	}
	type candidate struct{ parent, snapshot, token, owner, host string }
	items := []candidate{}
	for rows.Next() {
		var item candidate
		if rows.Scan(&item.parent, &item.snapshot, &item.token, &item.owner, &item.host) != nil {
			break
		}
		items = append(items, item)
	}
	_ = rows.Close()
	host, err := walletReaderJournalDirectory(b.cfg.ReaderJournalDirectory)
	if err != nil {
		return
	}
	for _, item := range items {
		if item.host != host {
			continue
		}
		tx, beginErr := b.outboxDB.BeginTx(ctx, nil)
		if beginErr != nil {
			continue
		}
		var orphaned bool
		if tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "wallet-reader-owner:"+item.owner).Scan(&orphaned) != nil || !orphaned {
			_ = tx.Rollback()
			continue
		}
		journal := &walletReaderJournal{dir: b.cfg.ReaderJournalDirectory, path: walletReaderJournalPath(b.cfg.ReaderJournalDirectory, item.parent, item.token), record: walletReaderJournalRecord{Version: 1, Host: item.host, OwnerID: item.owner, AuthorizationID: item.parent, Token: item.token, SnapshotID: item.snapshot}}
		if journal.acquire(true) != nil {
			_ = tx.Rollback()
			continue
		}
		loadErr := journal.loadLocked()
		if loadErr != nil && !errors.Is(loadErr, os.ErrNotExist) {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		if loadErr == nil && !journal.record.Joined && !journal.record.Sealed {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		if loadErr == nil {
			journal.record.Sealed = true
			if journal.saveLocked() != nil {
				journal.release()
				_ = tx.Rollback()
				continue
			}
		}
		removeErr := os.Remove(journal.path)
		if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		// Preserve the lock inode until the PG owner session is gone and no raw
		// Read holds it. A waiting reader will fail the absent-record check before
		// touching the socket, even if it opened the old inode before unlink.
		_ = os.Remove(journal.path + ".lock")
		directory, openErr := os.Open(journal.dir)
		if openErr != nil {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		syncErr := directory.Sync()
		_ = directory.Close()
		if syncErr != nil {
			journal.release()
			_ = tx.Rollback()
			continue
		}
		_, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET reader_journal_cleanup_at=COALESCE(reader_journal_cleanup_at,now()) WHERE parent_authorization_id=$1`, item.parent)
		if err == nil {
			// A failed commit leaves the cleanup marker unset; the next pass retries.
			_ = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		journal.release()
	}
}
