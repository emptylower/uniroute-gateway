package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// This deadline is funding policy, not an HTTP stream timeout. Genuine active
// owners are checked separately and do not modify the immutable deadline.
func (b *CanonicalWalletBridge) poolExpiryGrace() time.Duration {
	seconds := b.cfg.PoolExpiryGraceSeconds
	if seconds == 0 {
		seconds = 1800
	}
	if b.callerSlotTTLSeconds > seconds {
		seconds = b.callerSlotTTLSeconds
	}
	if b.cfg.OrphanGraceSeconds > seconds {
		seconds = b.cfg.OrphanGraceSeconds
	}
	return time.Duration(seconds) * time.Second
}

func (b *CanonicalWalletBridge) startPoolWriteOwner(ctx context.Context, h *AuthorizationHandle, token string) {
	if h.AttemptKind != "llm" {
		return
	}
	done := make(chan struct{})
	var once sync.Once
	end := func() {
		once.Do(func() {
			close(done)
			markCtx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
			defer cancel()
			_, _ = b.outboxDB.ExecContext(markCtx, `UPDATE wallet_authorization_segment SET write_ended_at=COALESCE(write_ended_at,now()),write_active_until=NULL WHERE parent_authorization_id=$1 AND authorization_token=$2`, h.ID, token)
		})
	}
	h.mu.Lock()
	h.writeEnded = end
	h.mu.Unlock()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				end()
				return
			case <-b.stop:
				end()
				return
			case <-ticker.C:
				heartbeatCtx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
				result, err := b.outboxDB.ExecContext(heartbeatCtx, `UPDATE wallet_authorization_segment SET write_active_until=now()+interval '90 seconds' WHERE parent_authorization_id=$1 AND authorization_token=$2 AND state='indeterminate' AND write_ended_at IS NULL AND write_active_until>now()`, h.ID, token)
				cancel()
				if err == nil {
					n, e := result.RowsAffected()
					if e == nil && n == 0 {
						end()
						return
					}
				}
			}
		}
	}()
}

func (h *AuthorizationHandle) completeWrite() {
	if h == nil {
		return
	}
	h.mu.Lock()
	end := h.writeEnded
	h.mu.Unlock()
	if end != nil {
		end()
	}
}

type walletResponseBody struct {
	io.ReadCloser
	handle      *AuthorizationHandle
	status      int
	stream      bool
	jsonLimit   int64
	streamLimit int64
	mu          sync.Mutex
	started     bool
	ended       bool
	buffer      []byte
	evidence    WalletReaderEvidence
	once        sync.Once
}

func (b *walletResponseBody) Read(p []byte) (int, error) {
	if b.handle.consumeHTTP != nil {
		return b.readEvidence(p)
	}
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.handle.completeWrite()
	}
	return n, err
}
func (b *walletResponseBody) Close() error {
	err := walletSafeReaderClose(b.ReadCloser)
	if b.handle.consumeHTTP != nil {
		b.mu.Lock()
		b.ended = true
		if parseErr := b.parseEvidence(nil, true, false); parseErr != nil {
			b.handle.recordReaderEvidence(b.evidence, parseErr)
			if err == nil {
				err = parseErr
			}
		}
		evidence := b.evidence
		b.mu.Unlock()
		b.finishEvidence(evidence, err)
		return err
	}
	b.handle.completeWrite()
	return err
}

func (b *walletResponseBody) readEvidence(p []byte) (int, error) {
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return 0, io.EOF
	}
	if !b.started {
		b.started = true
		if b.handle.readerStarted != nil {
			if err := b.handle.readerStarted(); err != nil {
				b.handle.recordReaderEvidence(b.evidence, err)
				b.mu.Unlock()
				return 0, err
			}
		}
	}
	if b.handle.readerJournal != nil {
		if err := b.handle.readerJournal.beginRead(); err != nil {
			b.mu.Unlock()
			b.handle.recordReaderEvidence(b.evidence, err)
			return 0, err
		}
	}
	n, err := walletSafeReaderRead(b.ReadCloser, p)
	parseErr := b.parseEvidence(p[:n], err != nil, true)
	if parseErr != nil {
		b.handle.recordReaderEvidence(b.evidence, parseErr)
		err = parseErr
	}
	if err != nil {
		b.evidence.Complete = errors.Is(err, io.EOF)
		finishWalletReaderCounts(b.handle.readerNormalization, &b.evidence, b.status, b.evidence.Complete)
	}
	if b.handle.readerJournal != nil && parseErr == nil {
		if saveErr := b.handle.readerJournal.observeLocked(b.evidence, false); saveErr != nil {
			b.handle.recordReaderEvidence(b.evidence, saveErr)
			if err == nil {
				err = saveErr
			}
		}
	}
	if parseErr == nil && (b.evidence.Present || b.evidence.Counts != nil) && b.handle.readerObserved != nil {
		if saveErr := b.handle.readerObserved(b.evidence); saveErr != nil {
			b.handle.recordReaderEvidence(b.evidence, saveErr)
			if b.handle.readerJournal != nil {
				_ = b.handle.readerJournal.observeLocked(b.evidence, true)
			}
		}
	}
	if err != nil {
		b.ended = true
		b.evidence.Complete = errors.Is(err, io.EOF)
	}
	evidence := b.evidence
	if b.handle.readerJournal != nil {
		b.handle.readerJournal.release()
	}
	b.mu.Unlock()
	if err != nil {
		b.finishEvidence(evidence, err)
	}
	return n, err
}

func walletSafeReaderRead(reader io.Reader, p []byte) (n int, err error) {
	defer func() {
		if recover() != nil {
			n = 0
			err = errors.New("wallet upstream reader panicked")
		}
	}()
	n, err = reader.Read(p)
	if n < 0 || n > len(p) {
		return 0, errors.New("wallet upstream reader returned invalid length")
	}
	return n, err
}
func walletSafeReaderClose(reader io.Closer) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("wallet upstream reader close panicked")
		}
	}()
	return reader.Close()
}
func (b *walletResponseBody) parseEvidence(raw []byte, final bool, journalLocked bool) error {
	if bytes.HasPrefix(bytes.TrimSpace(b.buffer), []byte("data:")) || bytes.HasPrefix(bytes.TrimSpace(b.buffer), []byte("event:")) || len(b.buffer) == 0 && (bytes.HasPrefix(bytes.TrimSpace(raw), []byte("data:")) || bytes.HasPrefix(bytes.TrimSpace(raw), []byte("event:"))) {
		b.stream = true
	}
	bound := b.jsonLimit
	if bound <= 0 {
		bound = defaultUpstreamResponseReadMaxBytes
	}
	if b.stream {
		bound = b.streamLimit
		if bound <= 0 {
			bound = defaultMaxLineSize
		}
	}
	if !b.stream && int64(len(b.buffer))+int64(len(raw)) > bound {
		b.evidence.Malformed = true
		b.buffer = nil
		return nil
	}
	b.buffer = append(b.buffer, raw...)
	for {
		idx := bytes.IndexByte(b.buffer, '\n')
		if idx < 0 {
			break
		}
		line := b.buffer[:idx]
		if b.stream {
			if int64(len(line)) > bound {
				b.evidence.Malformed = true
				b.buffer = b.buffer[idx+1:]
				continue
			}
			if bytes.HasPrefix(bytes.TrimSpace(line), []byte("data:")) {
				if err := b.observeEvidence(walletTrimSSEData(line), journalLocked); err != nil {
					return err
				}
			}
			b.buffer = b.buffer[idx+1:]
			continue
		}
		// Ordinary JSON can contain newlines; retain it until reader completion.
		break
	}
	if b.stream && int64(len(b.buffer)) > bound {
		b.evidence.Malformed = true
		b.buffer = nil
	}
	if final && len(b.buffer) > 0 {
		if err := b.observeEvidence(walletTrimSSEData(b.buffer), journalLocked); err != nil {
			return err
		}
		b.buffer = nil
	}
	return nil
}

func (b *walletResponseBody) observeEvidence(raw []byte, journalLocked bool) error {
	journal := b.handle.readerJournal
	if journal != nil {
		if !journalLocked {
			if err := journal.acquire(false); err != nil {
				return err
			}
			defer journal.release()
			if err := journal.loadLocked(); err != nil {
				return err
			}
		}
		if err := journal.checkpointLocked(raw, "llm_http_usage"); err != nil {
			return err
		}
	}
	observeWalletUsage(raw, &b.evidence, b.handle.readerNormalization)
	if err := observeWalletReaderCounts(raw, &b.evidence, b.handle.readerNormalization); err != nil {
		return err
	}
	b.evidence.Source = "llm_http_usage"
	if journal != nil {
		return journal.observeLocked(b.evidence, false)
	}
	return nil
}
func (b *walletResponseBody) finishEvidence(evidence WalletReaderEvidence, err error) {
	b.once.Do(func() {
		b.handle.recordReaderEvidence(evidence, nil)
		b.handle.completeWrite()
		b.handle.consumeHTTP(b.status, evidence, err)
	})
}
