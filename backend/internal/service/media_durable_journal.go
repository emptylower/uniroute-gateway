package service

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// Only normalized identity and the first authoritative success are persisted;
// prompts, credentials and provider response bodies never enter this journal.
type mediaDurableJournalRecord struct {
	Version       int                  `json:"version"`
	Volume        string               `json:"volume"`
	Owner         string               `json:"owner"`
	Task          string               `json:"task"`
	Authorization string               `json:"authorization"`
	Token         string               `json:"token"`
	User          string               `json:"user"`
	Provider      string               `json:"provider"`
	Snapshot      string               `json:"snapshot"`
	Quote         int64                `json:"quote"`
	WriteOwner    string               `json:"write_owner,omitempty"`
	Operation     string               `json:"operation"`
	Joined        bool                 `json:"joined"`
	NotSent       bool                 `json:"not_sent,omitempty"`
	Rejected      bool                 `json:"rejected,omitempty"`
	WriteEndedAt  *time.Time           `json:"write_ended_at,omitempty"`
	ProviderID    string               `json:"provider_id,omitempty"`
	Success       *MediaProviderResult `json:"success,omitempty"`
	SuccessAt     *time.Time           `json:"success_at,omitempty"`
	Failure       *MediaProviderResult `json:"failure,omitempty"`
	UpdatedAt     time.Time            `json:"updated_at"`
}

type mediaDurableJournal struct {
	dir, path string
	lock      *os.File
	record    mediaDurableJournalRecord
}

func (s *MediaTaskService) ensureMediaJournal(ctx context.Context) (*walletReaderJournalOwner, error) {
	s.journalMu.Lock()
	defer s.journalMu.Unlock()
	if s.bridge == nil || s.db == nil {
		return nil, errors.New("media durable journal database unavailable")
	}
	host, err := walletReaderJournalDirectory(s.bridge.cfg.ReaderJournalDirectory)
	if err != nil {
		return nil, err
	}
	if err = s.bridge.verifyWalletReaderJournalVolume(ctx, host); err != nil {
		return nil, err
	}
	if o := s.journalOwner; o != nil {
		if o.host != host || o.dir != s.bridge.cfg.ReaderJournalDirectory {
			return nil, errors.New("media durable volume changed")
		}
		if err = o.conn.PingContext(ctx); err == nil {
			return o, nil
		}
		_ = o.conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = o.conn.Close()
		s.journalOwner = nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	o := &walletReaderJournalOwner{dir: s.bridge.cfg.ReaderJournalDirectory, host: host, id: uuid.NewString(), conn: conn}
	if _, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1,0))`, "media-journal-owner:"+o.id); err != nil {
		_ = conn.Close()
		return nil, err
	}
	s.journalOwner = o
	return o, nil
}

func (s *MediaTaskService) closeMediaJournalOwner() {
	s.journalMu.Lock()
	defer s.journalMu.Unlock()
	if o := s.journalOwner; o != nil {
		_ = o.conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = o.conn.Close()
		s.journalOwner = nil
	}
}

func openMediaJournal(owner *walletReaderJournalOwner, r *mediaTaskRecord, nonblocking bool) (*mediaDurableJournal, error) {
	dir := filepath.Join(owner.dir, "media")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("media journal directory is not a mounted directory")
	}
	parent, err := os.Open(owner.dir)
	if err != nil {
		return nil, err
	}
	err = parent.Sync()
	_ = parent.Close()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(r.ID))
	j := &mediaDurableJournal{dir: dir, path: filepath.Join(dir, hex.EncodeToString(sum[:])+".json")}
	j.lock, err = os.OpenFile(j.path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	flags := syscall.LOCK_EX
	if nonblocking {
		flags |= syscall.LOCK_NB
	}
	if err = syscall.Flock(int(j.lock.Fd()), flags); err != nil {
		_ = j.lock.Close()
		return nil, err
	}
	j.record = mediaDurableJournalRecord{Version: 1, Volume: owner.host, Owner: owner.id, Task: r.ID, Authorization: r.AuthorizationID, Token: r.AuthorizationToken, User: r.PlatformUserID, Provider: "kie", Snapshot: r.SnapshotID, Quote: r.QuotedUnits, ProviderID: r.ProviderTaskID, WriteOwner: r.WriteOwner, WriteEndedAt: r.WriteEndedAt}
	file, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return j, nil
	}
	if err != nil {
		j.close()
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, walletReaderJournalBound+1))
	_ = file.Close()
	if err != nil || len(raw) > walletReaderJournalBound {
		j.close()
		return nil, errors.New("media durable journal unreadable")
	}
	var record mediaDurableJournalRecord
	if err = json.Unmarshal(raw, &record); err != nil {
		j.close()
		return nil, err
	}
	// Initial WAL fsync precedes the atomic PG send-start. Its exact first token
	// may therefore be absent from PG after a crash. Loading that owning record
	// is not terminal proof: recovery still fences its actual PG owner and locks
	// every unstarted funding segment before declaring it not sent.
	unboundWrite := record.Operation == "write" && record.Token == r.AuthorizationID+".1" && record.Owner != "" && record.WriteOwner != "" && (!record.Joined || record.NotSent) && record.ProviderID == "" && record.Success == nil && record.Failure == nil && r.ProviderTaskID == "" && r.WriteStartedAt == nil && r.WriteEndedAt == nil && r.AuthorizationToken == ""
	if record.Version != 1 || record.Volume != owner.host || record.Task != r.ID || record.Authorization != r.AuthorizationID || record.Token != r.AuthorizationToken && !unboundWrite || record.User != r.PlatformUserID || record.Provider != "kie" || record.Snapshot != r.SnapshotID || record.Quote != r.QuotedUnits || record.ProviderID != "" && r.ProviderTaskID != "" && record.ProviderID != r.ProviderTaskID {
		j.close()
		return nil, errors.New("media durable journal identity conflict")
	}
	j.record = record
	return j, nil
}

func (j *mediaDurableJournal) close() {
	if j != nil && j.lock != nil {
		_ = syscall.Flock(int(j.lock.Fd()), syscall.LOCK_UN)
		_ = j.lock.Close()
		j.lock = nil
	}
}
func (j *mediaDurableJournal) save() error {
	j.record.UpdatedAt = time.Now().UTC()
	return writeWalletReaderJournal(j.dir, j.path, j.record)
}

func (s *MediaTaskService) beginMediaJournal(ctx context.Context, r *mediaTaskRecord, operation string) error {
	o, err := s.ensureMediaJournal(ctx)
	if err != nil {
		return err
	}
	j, err := openMediaJournal(o, r, false)
	if err != nil {
		return err
	}
	if j.record.Success != nil || j.record.Failure != nil || j.record.Operation != "" && !j.record.Joined {
		j.close()
		return errors.New("media durable owner handoff must recover before another provider operation")
	}
	j.record.Owner, j.record.Operation, j.record.Joined = o.id, operation, false
	if operation == "write" {
		j.record.WriteOwner = r.ClaimedBy
	}
	if err = j.save(); err != nil {
		j.close()
		return err
	}
	r.journal = j
	return nil
}

// Mark the durable operation before reading an authoritative result. A failed
// first fee transaction leaves this PG barrier in place until journal recovery.
func (s *MediaTaskService) beginMediaQuery(ctx context.Context, r *mediaTaskRecord) error {
	if err := s.beginMediaJournal(ctx, r, "query"); err != nil {
		return err
	}
	j := r.journal
	res, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET media_journal_volume=$3,media_journal_owner=$4,media_journal_pending=true,next_journal_recovery_at=now() WHERE id=$1 AND claimed_by=$2 AND claim_until>now() AND provider_task_id=$5 AND NOT media_journal_pending AND fee_pending_at IS NULL`, r.ID, r.ClaimedBy, j.record.Volume, j.record.Owner, r.ProviderTaskID)
	if err == nil {
		var n int64
		n, err = res.RowsAffected()
		if err == nil && n != 1 {
			err = errors.New("media query owner fence refused")
		}
	}
	if err != nil {
		j.record.Joined = true
		_ = j.save()
		j.close()
		r.journal = nil
	}
	return err
}

func normalizedMediaSuccess(result MediaProviderResult) MediaProviderResult {
	// Delivery metadata is bounded independently from the immutable fee facts.
	out := MediaProviderResult{ProviderTaskID: result.ProviderTaskID, Status: "success", URLs: []MediaURL{}}
	for _, u := range result.URLs {
		if len(out.URLs) < 8 && len(u.Kind) <= 16 && len(u.URL) <= 2048 && validMediaURL(u.URL) {
			out.URLs = append(out.URLs, u)
		}
	}
	if len(result.Title) <= 256 {
		out.Title = result.Title
	}
	if len(result.CoverURL) <= 2048 && validMediaURL(result.CoverURL) {
		out.CoverURL = result.CoverURL
	}
	// JSON escaping can expand otherwise short URLs. Drop only delivery fields
	// until the normalized result leaves room for its immutable journal identity.
	for {
		raw, _ := json.Marshal(out)
		if len(raw) <= 16*1024 || len(out.URLs) == 0 {
			break
		}
		out.URLs = out.URLs[:len(out.URLs)-1]
	}
	if raw, _ := json.Marshal(out); len(raw) > 16*1024 {
		out.CoverURL, out.Title = "", ""
	}
	return out
}

func (s *MediaTaskService) completeMediaQuery(ctx context.Context, r *mediaTaskRecord, result MediaProviderResult) error {
	j := r.journal
	if j == nil {
		return errors.New("media query has no durable owner")
	}
	j.record.Joined = true
	if mediaTrustedResult(r, result) && result.Status == "success" && j.record.Success == nil {
		first := normalizedMediaSuccess(result)
		at := time.Now().UTC()
		j.record.Success, j.record.SuccessAt = &first, &at
	}
	if mediaTrustedResult(r, result) && result.Status == "failed" && j.record.Success == nil && j.record.Failure == nil {
		failed := MediaProviderResult{ProviderTaskID: r.ProviderTaskID, Status: "failed", URLs: []MediaURL{}}
		if len(result.ErrorCode) <= 128 {
			failed.ErrorCode = result.ErrorCode
		}
		if len(result.ErrorMessage) <= 512 {
			failed.ErrorMessage = result.ErrorMessage
		}
		j.record.Failure = &failed
	}
	if err := j.save(); err != nil {
		return err
	}
	if j.record.Success != nil {
		r.Result = *j.record.Success
		return s.persistMediaSuccess(ctx, r)
	}
	if j.record.Failure != nil {
		r.Result = *j.record.Failure
		return s.persistMediaFailure(ctx, r)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET media_journal_pending=false WHERE id=$1 AND media_journal_volume=$2 AND media_journal_owner=$3`, r.ID, j.record.Volume, j.record.Owner)
	return err
}

func (s *MediaTaskService) mediaJournalOwnerGone(ctx context.Context, owner string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var gone bool
	err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "media-journal-owner:"+owner).Scan(&gone)
	return gone, err
}

// Recovery never relies on the 90-second polling claim. A joined record proves
// an actual return; an interrupted record also requires the live PG owner to be
// gone and acquisition of its OS lock before its owner can be declared ended.
func (s *MediaTaskService) recoverMediaJournal(ctx context.Context, r *mediaTaskRecord) error {
	o, err := s.ensureMediaJournal(ctx)
	if err != nil {
		return err
	}
	j, err := openMediaJournal(o, r, true)
	if err != nil {
		return err
	}
	defer j.close()
	var volume, owner string
	var pending bool
	if err = s.db.QueryRowContext(ctx, `SELECT COALESCE(media_journal_volume,''),COALESCE(media_journal_owner,''),media_journal_pending FROM gateway_media_task WHERE id=$1`, r.ID).Scan(&volume, &owner, &pending); err != nil {
		return err
	}
	if !pending {
		if j.record.Operation != "write" || j.record.Token != r.AuthorizationID+".1" || j.record.ProviderID != "" || j.record.Success != nil || j.record.Failure != nil || r.ProviderTaskID != "" || r.WriteStartedAt != nil || r.WriteEndedAt != nil || r.AuthorizationToken != "" {
			return nil
		}
		if !j.record.Joined {
			// The OS lock is held above. A killed writer between WAL fsync and
			// PG start can never send without that atomic start transaction. Prove
			// its session gone and the entire original group unstarted together;
			// neither an expired poll claim nor absence of a provider ID suffices.
			tx, beginErr := s.db.BeginTx(ctx, nil)
			if beginErr != nil {
				return beginErr
			}
			defer func() { _ = tx.Rollback() }()
			var gone bool
			if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "media-journal-owner:"+j.record.Owner).Scan(&gone); err != nil || !gone {
				return err
			}
			count, lockErr := lockWalletAttempt(ctx, tx, r.AuthorizationID)
			if lockErr != nil || count == 0 {
				return errors.New("media unstarted owner funding group unavailable")
			}
			var taskMatches, segmentsMatch bool
			err = tx.QueryRowContext(ctx, `SELECT authorization_id=$2 AND platform_user_id=$3 AND billing_snapshot_id=$4 AND quoted_units=$5 AND held_units=$6 AND status IN ('submitting','indeterminate') AND financial_state='held' AND authorization_token IS NULL AND write_started_at IS NULL AND write_ended_at IS NULL AND provider_task_id IS NULL AND NOT media_journal_pending AND media_journal_volume IS NULL AND media_journal_owner IS NULL AND financial_terminal_proof IS NULL AND fee_pending_at IS NULL AND (actual_units IS NULL OR actual_units=0) FROM gateway_media_task WHERE id=$1 FOR UPDATE`, r.ID, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.QuotedUnits, r.HeldUnits).Scan(&taskMatches)
			if err != nil || !taskMatches {
				return errors.New("media unstarted task owner identity conflict")
			}
			err = tx.QueryRowContext(ctx, `SELECT bool_and(kind='media' AND platform_user_id=$2 AND billing_snapshot_id=$3 AND authorization_token IS NULL AND first_write_at IS NULL AND write_ended_at IS NULL AND state IN ('prepared','held') AND pin_state='active' AND terminal_sealed_at IS NULL AND known_fee_units IS NULL AND zero_intent_at IS NULL AND expiry_intent_version=0 AND actual_units=0 AND NOT evidence_pending AND NOT fee_pending AND settlement_payload IS NULL AND remainder_payload IS NULL) AND sum(held_units)=$4 FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.HeldUnits).Scan(&segmentsMatch)
			if err != nil || !segmentsMatch {
				return errors.New("media unstarted funding owner identity conflict")
			}
			at := time.Now().UTC()
			j.record.Joined, j.record.NotSent, j.record.WriteEndedAt = true, true, &at
			if err = j.save(); err != nil {
				return err
			}
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		if !j.record.NotSent || j.record.WriteEndedAt == nil {
			return nil
		}
		// This owner really returned before the decorated send. Resolve that
		// direct return or fenced atomic-no-start proof as signed zero. Do not
		// invent a started provider write for its prepared funding segments.
		r.AuthorizationToken, r.WriteOwner, r.journal = j.record.Token, j.record.WriteOwner, j
		defer func() { r.journal = nil }()
		r.Segments, err = s.bridge.authorizationSegments(ctx, r.AuthorizationID)
		if err != nil {
			return err
		}
		claimed, err := s.prepareMediaFinancialTerminal(ctx, r, true, true)
		if err != nil || !claimed {
			return err
		}
		return s.finishMediaZero(ctx, r)
	}
	if j.record.Operation == "" {
		return errors.New("media durable pending record missing")
	}
	if volume != j.record.Volume || owner != j.record.Owner {
		return errors.New("media durable PG owner identity conflict")
	}
	if !j.record.Joined {
		gone, e := s.mediaJournalOwnerGone(ctx, j.record.Owner)
		if e != nil {
			return e
		}
		if !gone {
			return nil
		}
		j.record.Joined = true
		if j.record.Operation == "write" && j.record.WriteEndedAt == nil {
			at := time.Now().UTC()
			j.record.WriteEndedAt = &at
		}
		if err = j.save(); err != nil {
			return err
		}
	}
	r.journal = j
	defer func() { r.journal = nil }()
	if j.record.Success != nil {
		r.ProviderTaskID = j.record.ProviderID
		r.Result = *j.record.Success
		return s.persistMediaSuccess(ctx, r)
	}
	if j.record.Failure != nil {
		r.Result = *j.record.Failure
		return s.persistMediaFailure(ctx, r)
	}
	if j.record.Operation == "write" {
		return s.persistMediaWrite(ctx, r, j)
	}
	_, err = s.db.ExecContext(ctx, `UPDATE gateway_media_task SET media_journal_pending=false,claimed_by=NULL,claim_until=NULL,next_poll_at=now() WHERE id=$1 AND media_journal_volume=$2 AND media_journal_owner=$3 AND media_journal_pending`, r.ID, j.record.Volume, j.record.Owner)
	return err
}

func (s *MediaTaskService) persistMediaFailure(ctx context.Context, r *mediaTaskRecord) error {
	r.ErrorCode, r.ErrorMessage = r.Result.ErrorCode, r.Result.ErrorMessage
	if r.FinancialState == "unknown_pending" {
		// An earlier immutable unknown intent must receive its signed ACK;
		// provider failure cannot rewrite that intent or reacquire the hold.
		segments, err := s.bridge.authorizationSegments(ctx, r.AuthorizationID)
		if err != nil {
			return err
		}
		r.Segments = segments
		if err = s.recoverMediaFinancial(ctx, r); err != nil {
			return err
		}
		refreshed, err := s.store.get(ctx, r.UserID, r.ID)
		if err != nil {
			return err
		}
		if refreshed.FinancialState != "released_unknown" {
			return errors.New("media original unknown ACK remains pending")
		}
		refreshed.journal, refreshed.Result = r.journal, r.Result
		return s.persistMediaFailure(ctx, refreshed)
	}
	if r.FinancialState == "released_unknown" || r.FinancialState == "released_zero" {
		raw, err := json.Marshal(r.Result)
		if err != nil {
			return err
		}
		_, err = s.db.ExecContext(ctx, `UPDATE gateway_media_task SET status='failed',result=$2::jsonb,error_code=NULLIF($3,''),error_message=NULLIF($4,''),media_journal_pending=false,claimed_by=NULL,claim_until=NULL WHERE id=$1 AND financial_state IN ('released_unknown','released_zero') AND media_journal_volume=$5 AND media_journal_owner=$6`, r.ID, string(raw), r.ErrorCode, r.ErrorMessage, r.journal.record.Volume, r.journal.record.Owner)
		return err
	}
	claimed, err := s.prepareMediaFinancialTerminal(ctx, r, true)
	if err != nil {
		return err
	}
	if !claimed {
		return errors.New("media durable zero intent deferred")
	}
	_, err = s.db.ExecContext(ctx, `UPDATE gateway_media_task SET claimed_by=NULL,claim_until=NULL,next_financial_recovery_at=now() WHERE id=$1 AND financial_state='zero_pending'`, r.ID)
	return err
}

func (s *MediaTaskService) recoverMediaJournals(ctx context.Context) {
	// Rotate before attempting a read: a slow/corrupt/live owner does not stay
	// permanently at the head of this lane, nor block fee/finance/query lanes.
	rows, err := s.db.QueryContext(ctx, `UPDATE gateway_media_task SET next_journal_recovery_at=now()+interval '1 second' WHERE id IN (SELECT id FROM gateway_media_task WHERE (media_journal_pending OR (status IN ('submitting','indeterminate') AND write_started_at IS NULL AND financial_state='held')) AND next_journal_recovery_at<=now() ORDER BY next_journal_recovery_at,id LIMIT 8 FOR UPDATE SKIP LOCKED) RETURNING `+mediaTaskColumns)
	if err != nil {
		slog.Warn("media durable handoff discovery failed", "error", err)
		return
	}
	var records []*mediaTaskRecord
	for rows.Next() {
		r, e := scanMediaTask(rows)
		if e != nil {
			err = e
			break
		}
		records = append(records, r)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		slog.Warn("media durable handoff rows unavailable", "error", err)
		return
	}
	for _, r := range records {
		opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = s.recoverMediaJournal(opCtx, r)
		cancel()
		if err != nil && !errors.Is(err, syscall.EWOULDBLOCK) {
			slog.Warn("media durable handoff deferred", "task_id", r.ID, "error", err)
		}
	}
}

// One transaction contains the task and every segment's actual owner end. A
// failed intermediate segment handoff cannot acknowledge only half the owner.
func (s *MediaTaskService) persistMediaWrite(ctx context.Context, r *mediaTaskRecord, j *mediaDurableJournal) error {
	if j.record.WriteEndedAt == nil || !j.record.Joined {
		return errors.New("media durable write owner has not ended")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	count, err := lockWalletAttempt(ctx, tx, r.AuthorizationID)
	if err != nil || count == 0 {
		if err == nil {
			err = errors.New("media write handoff has no funding segments")
		}
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE gateway_media_task SET write_ended_at=COALESCE(write_ended_at,$5),provider_task_id=COALESCE(provider_task_id,NULLIF($4,'')),status=CASE WHEN financial_state IN ('held','unknown_pending','released_unknown') THEN CASE WHEN NULLIF($4,'') IS NOT NULL THEN 'processing' ELSE 'indeterminate' END ELSE status END,error_code=CASE WHEN NULLIF($4,'') IS NULL THEN 'PROVIDER_CREATE_UNKNOWN' ELSE NULL END,error_message=CASE WHEN NULLIF($4,'') IS NULL THEN 'The provider submission could not be confirmed. Reconciliation continues.' ELSE NULL END,write_proven_not_sent=write_proven_not_sent OR $8,write_proven_zero=write_proven_zero OR $9,media_journal_pending=false,next_poll_at=now(),next_financial_recovery_at=now(),claimed_by=NULL,claim_until=NULL,updated_at=now() WHERE id=$1 AND write_owner=$2 AND authorization_token=$3 AND (provider_task_id IS NULL OR provider_task_id=NULLIF($4,'')) AND media_journal_volume=$6 AND media_journal_owner=$7 AND media_journal_pending AND authorization_id=$10 AND platform_user_id=$11 AND billing_snapshot_id=$12 AND quoted_units=$13`, r.ID, j.record.WriteOwner, j.record.Token, j.record.ProviderID, *j.record.WriteEndedAt, j.record.Volume, j.record.Owner, j.record.NotSent, j.record.Rejected, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.QuotedUnits)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("media write handoff identity conflict")
	}
	res, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET write_ended_at=COALESCE(write_ended_at,$3),write_active_until=NULL WHERE parent_authorization_id=$1 AND authorization_token=$2 AND kind='media' AND platform_user_id=$4 AND billing_snapshot_id=$5 AND first_write_at IS NOT NULL`, r.AuthorizationID, j.record.Token, *j.record.WriteEndedAt, r.PlatformUserID, r.SnapshotID)
	if err != nil {
		return err
	}
	n, err = res.RowsAffected()
	if err != nil || n != int64(count) {
		return errors.New("media write handoff funding segment identity conflict")
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	r.ProviderTaskID, r.WriteEndedAt = j.record.ProviderID, j.record.WriteEndedAt
	if r.ProviderTaskID != "" {
		r.Status = "processing"
	} else {
		r.Status = "indeterminate"
	}
	return nil
}
