//go:build unit

package service

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestMediaFeePlanReplaysSevenFieldPayloadWithLockedOriginalIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		locked        int
		taskToken     string
		segmentColumn int
		segmentValue  driver.Value
		payloadField  string
		affected      int64
		wantErr       string
	}{
		{name: "original-replay", locked: 2, affected: 1},
		{name: "no-segment", locked: 0, wantErr: "segment identity conflict"},
		{name: "missing-segment", locked: 1, wantErr: "segment identity conflict"},
		{name: "task-token", locked: 2, taskToken: "other-token", wantErr: "task identity conflict"},
		{name: "parent", locked: 2, segmentColumn: 2, segmentValue: "other-parent", wantErr: "segment identity conflict"},
		{name: "platform-user", locked: 2, segmentColumn: 3, segmentValue: "other-user", wantErr: "segment identity conflict"},
		{name: "snapshot", locked: 2, segmentColumn: 4, segmentValue: "other-snapshot", wantErr: "segment identity conflict"},
		{name: "token", locked: 2, segmentColumn: 5, segmentValue: "other-token", wantErr: "segment identity conflict"},
		{name: "kind", locked: 2, segmentColumn: 6, segmentValue: "llm", wantErr: "segment identity conflict"},
		{name: "event", locked: 2, segmentColumn: 7, segmentValue: "other-event", wantErr: "segment identity conflict"},
		{name: "lease", locked: 2, segmentColumn: 8, segmentValue: "other-lease", wantErr: "segment identity conflict"},
		{name: "held", locked: 2, segmentColumn: 9, segmentValue: int64(99), wantErr: "segment identity conflict"},
		{name: "ordinal", locked: 2, segmentColumn: 10, segmentValue: int64(1), wantErr: "segment identity conflict"},
		{name: "payload-user", locked: 2, payloadField: "platform_user_id", wantErr: "original settlement payload conflict"},
		{name: "payload-lease", locked: 2, payloadField: "lease_id", wantErr: "original settlement payload conflict"},
		{name: "payload-currency", locked: 2, payloadField: "currency", wantErr: "original settlement payload conflict"},
		{name: "payload-time", locked: 2, payloadField: "occurred_at", wantErr: "original settlement payload conflict"},
		{name: "no-row-updated", locked: 2, affected: 0, wantErr: "segment update conflict"},
		{name: "multiple-rows-updated", locked: 2, affected: 2, wantErr: "segment update conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			r := &mediaTaskRecord{ID: "task", AuthorizationID: "auth", AuthorizationToken: "auth.1", PlatformUserID: "user", SnapshotID: "original-snapshot", QuotedUnits: 150, CreatedAt: time.Unix(1700000000, 0).UTC(), Segments: []AuthorizationSegment{
				{AuthorizationID: "auth", EventID: "event-0", LeaseID: "lease-0", HeldUnits: 100},
				{AuthorizationID: "auth-share-1", EventID: "event-1", LeaseID: "lease-1", HeldUnits: 100},
			}}
			s := &MediaTaskService{db: db}
			replays := 1
			if tc.wantErr == "" {
				replays = 2
			}
			for replay := 0; replay < replays; replay++ {
				mock.ExpectBegin()
				locked := sqlmock.NewRows([]string{"authorization_id"})
				for i := 0; i < tc.locked; i++ {
					locked.AddRow(r.Segments[i].AuthorizationID)
				}
				mock.ExpectQuery("SELECT authorization_id FROM wallet_authorization_segment").WithArgs(r.AuthorizationID).WillReturnRows(locked)
				if tc.locked != 2 {
					mock.ExpectRollback()
					continue
				}
				token := r.AuthorizationToken
				if tc.taskToken != "" {
					token = tc.taskToken
				}
				mock.ExpectQuery("SELECT authorization_id,authorization_token,billing_snapshot_id,platform_user_id,quoted_units,created_at FROM gateway_media_task").WithArgs(r.ID).WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "authorization_token", "billing_snapshot_id", "platform_user_id", "quoted_units", "created_at"}).AddRow(r.AuthorizationID, token, r.SnapshotID, r.PlatformUserID, r.QuotedUnits, r.CreatedAt))
				if tc.taskToken != "" {
					mock.ExpectRollback()
					continue
				}
				for ordinal, segment := range r.Segments {
					actual, lease, version := int64(100), segment.LeaseID, 0
					if ordinal == 1 {
						actual, lease, version = 50, "", 2
					}
					event := CanonicalWalletSettlementEvent{EventID: segment.EventID, GatewayRequestID: r.ID, PlatformUserID: r.PlatformUserID, LeaseID: lease, Currency: CurrencyUSD, AmountUnits: actual, OccurredAt: r.CreatedAt, AuthorizationID: segment.AuthorizationID, AuthorizationToken: r.AuthorizationToken, BillingSnapshotID: r.SnapshotID}
					raw, err := json.Marshal(event)
					require.NoError(t, err)
					var fields map[string]any
					require.NoError(t, json.Unmarshal(raw, &fields))
					require.Len(t, fields, 7, "internal owner metadata cannot change the established wire/hash contract")
					require.NotContains(t, fields, "authorization_token")
					require.NotContains(t, fields, "authorization_id")
					require.NotContains(t, fields, "billing_snapshot_id")
					if ordinal == 0 && tc.payloadField != "" {
						fields[tc.payloadField] = "other"
						if tc.payloadField == "occurred_at" {
							fields[tc.payloadField] = r.CreatedAt.Add(time.Second).Format(time.RFC3339Nano)
						}
						raw, err = json.Marshal(fields)
						require.NoError(t, err)
					}
					row := []driver.Value{version, raw, r.AuthorizationID, r.PlatformUserID, r.SnapshotID, r.AuthorizationToken, "media", segment.EventID, segment.LeaseID, segment.HeldUnits, ordinal}
					if ordinal == 0 && tc.segmentValue != nil {
						row[tc.segmentColumn] = tc.segmentValue
					}
					mock.ExpectQuery("SELECT expiry_intent_version,settlement_payload,parent_authorization_id").WithArgs(segment.AuthorizationID).WillReturnRows(sqlmock.NewRows([]string{"version", "payload", "parent", "user", "snapshot", "token", "kind", "event", "lease", "held", "ordinal"}).AddRow(row...))
					if ordinal == 0 && (tc.segmentValue != nil || tc.payloadField != "") {
						mock.ExpectRollback()
						break
					}
					affected := int64(1)
					if ordinal == 0 {
						affected = tc.affected
					}
					mock.ExpectExec("UPDATE wallet_authorization_segment SET actual_units").WithArgs(segment.AuthorizationID, actual, "settling", string(raw)).WillReturnResult(sqlmock.NewResult(0, affected))
					if affected != 1 {
						mock.ExpectRollback()
						break
					}
				}
				if tc.wantErr == "" {
					mock.ExpectExec("INSERT INTO usage_logs").WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectExec("UPDATE gateway_media_task SET fee_plan_at").WithArgs(r.ID).WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectCommit()
				}
			}
			for replay := 0; replay < replays; replay++ {
				err = s.persistMediaFeePlan(context.Background(), r)
				if tc.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tc.wantErr)
				}
			}
			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet(), "wrong immutable identity must roll back before publishing any fee plan")
		})
	}
}
