//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestMediaDurableJournalCompletionRequiresEveryBoundSegment(t *testing.T) {
	for _, affected := range []int64{0, 1, 2} {
		t.Run(fmt.Sprint(affected), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			at := time.Now().UTC()
			r := &mediaTaskRecord{ID: "task", AuthorizationID: "auth", AuthorizationToken: "auth.1", PlatformUserID: "user", SnapshotID: "frozen"}
			j := &mediaDurableJournal{record: mediaDurableJournalRecord{Joined: true, WriteEndedAt: &at, WriteOwner: "write-owner", Token: r.AuthorizationToken, ProviderID: "provider-id", Volume: "volume", Owner: "live-owner"}}
			s := &MediaTaskService{db: db}
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT authorization_id FROM wallet_authorization_segment").WithArgs(r.AuthorizationID).WillReturnRows(sqlmock.NewRows([]string{"authorization_id"}).AddRow("auth").AddRow("auth-share-2"))
			mock.ExpectExec("UPDATE gateway_media_task SET write_ended_at").WithArgs(r.ID, "write-owner", r.AuthorizationToken, "provider-id", at, "volume", "live-owner", false, false, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.QuotedUnits).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE wallet_authorization_segment SET write_ended_at").WithArgs(r.AuthorizationID, r.AuthorizationToken, at, r.PlatformUserID, r.SnapshotID).WillReturnResult(sqlmock.NewResult(0, affected))
			if affected == 2 {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			err = s.persistMediaWrite(context.Background(), r, j)
			if affected == 2 {
				require.NoError(t, err)
				require.Equal(t, "provider-id", r.ProviderTaskID)
				require.Equal(t, &at, r.WriteEndedAt)
			} else {
				require.ErrorContains(t, err, "funding segment identity conflict")
				require.Empty(t, r.ProviderTaskID)
				require.Nil(t, r.WriteEndedAt, "task owner completion cannot commit without all original bound segments")
			}
			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMediaDurableJournalBindsImmutableIdentityAndVolume(t *testing.T) {
	dir := t.TempDir()
	volume, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	owner := &walletReaderJournalOwner{dir: dir, host: volume, id: "media-live-owner"}
	r := &mediaTaskRecord{ID: "task", AuthorizationID: "auth", AuthorizationToken: "auth.1", PlatformUserID: "user", SnapshotID: "frozen-snapshot", QuotedUnits: 2730000, ProviderTaskID: "provider-task"}
	j, err := openMediaJournal(owner, r, false)
	require.NoError(t, err)
	j.record.Operation, j.record.Joined = "query", true
	require.NoError(t, j.save())
	j.close()
	for _, mutate := range []func(*mediaTaskRecord){
		func(r *mediaTaskRecord) { r.AuthorizationID = "other" },
		func(r *mediaTaskRecord) { r.AuthorizationToken = "auth.2" },
		func(r *mediaTaskRecord) { r.PlatformUserID = "other" },
		func(r *mediaTaskRecord) { r.SnapshotID = "other" },
		func(r *mediaTaskRecord) { r.QuotedUnits++ },
		func(r *mediaTaskRecord) { r.ProviderTaskID = "other" },
	} {
		changed := *r
		mutate(&changed)
		_, err = openMediaJournal(owner, &changed, true)
		require.Error(t, err)
	}
	wrong := *owner
	wrong.host = "replacement-volume"
	_, err = openMediaJournal(&wrong, r, true)
	require.Error(t, err)
}

func TestMediaDurableJournalOSLockFencesAnUnfinishedLiveProviderOwner(t *testing.T) {
	dir := t.TempDir()
	volume, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	owner := &walletReaderJournalOwner{dir: dir, host: volume, id: "actual-owner"}
	r := &mediaTaskRecord{ID: "task", AuthorizationID: "auth", AuthorizationToken: "auth.1", PlatformUserID: "user", SnapshotID: "frozen", QuotedUnits: 2730000}
	live, err := openMediaJournal(owner, r, false)
	require.NoError(t, err)
	live.record.Operation = "write"
	require.NoError(t, live.save())
	_, err = openMediaJournal(owner, r, true)
	require.True(t, errors.Is(err, syscall.EWOULDBLOCK))
	live.close()
	recovery, err := openMediaJournal(owner, r, true)
	require.NoError(t, err)
	defer recovery.close()
	require.False(t, recovery.record.Joined, "acquiring an OS lock is not itself proof that the live PG owner ended")
}

func TestMediaDurableJournalUnboundFirstWriteRetainsImmutableIdentity(t *testing.T) {
	dir := t.TempDir()
	volume, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	owner := &walletReaderJournalOwner{dir: dir, host: volume, id: "actual-owner"}
	r := &mediaTaskRecord{ID: "task", AuthorizationID: "auth", AuthorizationToken: "auth.1", PlatformUserID: "user", SnapshotID: "frozen", QuotedUnits: 2730000, WriteOwner: "actual-writer"}
	j, err := openMediaJournal(owner, r, false)
	require.NoError(t, err)
	j.record.Operation = "write"
	require.NoError(t, j.save())
	j.close()
	unbound := *r
	unbound.AuthorizationToken = ""
	recoveryOwner := *owner
	recoveryOwner.id = "recovery-owner"
	recovery, err := openMediaJournal(&recoveryOwner, &unbound, true)
	require.NoError(t, err)
	require.Equal(t, "auth.1", recovery.record.Token)
	require.Equal(t, owner.id, recovery.record.Owner)
	require.False(t, recovery.record.Joined || recovery.record.NotSent, "loading an unbound WAL is not a terminal or not-sent proof")
	recovery.close()
	for _, mutate := range []func(*mediaTaskRecord){
		func(r *mediaTaskRecord) { r.AuthorizationID = "other" },
		func(r *mediaTaskRecord) { r.AuthorizationToken = "auth.2" },
		func(r *mediaTaskRecord) { r.PlatformUserID = "other" },
		func(r *mediaTaskRecord) { r.SnapshotID = "other" },
		func(r *mediaTaskRecord) { r.QuotedUnits++ },
		func(r *mediaTaskRecord) { r.ProviderTaskID = "provider-id" },
		func(r *mediaTaskRecord) { at := time.Now(); r.WriteStartedAt = &at },
	} {
		changed := unbound
		mutate(&changed)
		_, err = openMediaJournal(&recoveryOwner, &changed, true)
		require.Error(t, err)
	}
	j, err = openMediaJournal(owner, r, false)
	require.NoError(t, err)
	j.record.Token = "auth.2"
	require.NoError(t, j.save())
	j.close()
	_, err = openMediaJournal(&recoveryOwner, &unbound, true)
	require.Error(t, err, "only the owning first media token can precede the atomic PG start")
}

func TestMediaDurableJournalNormalizesBoundedFirstSuccess(t *testing.T) {
	result := MediaProviderResult{ProviderTaskID: "provider", Status: "success", Title: strings.Repeat("t", 300), ErrorMessage: "provider response body", CoverURL: "http://unsafe"}
	for i := 0; i < 50; i++ {
		result.URLs = append(result.URLs, MediaURL{Kind: "image", URL: "https://example.test/" + strings.Repeat("a", 1500)})
	}
	first := normalizedMediaSuccess(result)
	require.Len(t, first.URLs, 8)
	require.Empty(t, first.Title)
	require.Empty(t, first.ErrorMessage)
	require.Empty(t, first.CoverURL)
	require.Equal(t, "provider", first.ProviderTaskID)
	require.Equal(t, "success", first.Status)
	for i := range result.URLs {
		result.URLs[i].URL = "https://example.test/" + strings.Repeat("<&", 700)
	}
	raw, err := json.Marshal(normalizedMediaSuccess(result))
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), 16*1024, "escaped delivery fields cannot discard the durable fee fact")
}
