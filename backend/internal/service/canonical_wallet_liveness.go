package service

import (
	"context"
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
	handle *AuthorizationHandle
}

func (b *walletResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.handle.completeWrite()
	}
	return n, err
}
func (b *walletResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.handle.completeWrite()
	return err
}
