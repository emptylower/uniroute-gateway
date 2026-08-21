//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestMigrationsRunner_ConcurrentInstancesSerializeOnSessionLock(t *testing.T) {
	const instances = 2
	errorsByInstance := make([]error, instances)
	var wg sync.WaitGroup
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errorsByInstance[index] = ApplyMigrations(ctx, integrationDB)
		}(i)
	}
	wg.Wait()
	for i, err := range errorsByInstance {
		require.NoErrorf(t, err, "migration instance %d", i)
	}
}

func TestMigrationsRunner_IsIdempotent_AndSchemaIsUpToDate(t *testing.T) {
	tx := testTx(t)

	// Re-apply migrations to verify idempotency (no errors, no duplicate rows).
	require.NoError(t, ApplyMigrations(context.Background(), integrationDB))

	// schema_migrations should have at least the current migration set.
	var applied int
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM schema_migrations").Scan(&applied))
	require.GreaterOrEqual(t, applied, 7, "expected schema_migrations to contain applied migrations")

	// users: columns required by repository queries
	requireColumn(t, tx, "users", "username", "character varying", 100, false)
	requireColumn(t, tx, "users", "notes", "text", 0, false)
	requireColumn(t, tx, "users", "billing_currency", "character varying", 3, false)
	requireColumn(t, tx, "users", "platform_user_id", "character varying", 128, true)
	requireIndex(t, tx, "users", "idx_users_platform_user_id")

	// accounts: schedulable and rate-limit fields
	requireColumn(t, tx, "accounts", "notes", "text", 0, true)
	requireColumn(t, tx, "accounts", "schedulable", "boolean", 0, false)
	requireColumn(t, tx, "accounts", "rate_limited_at", "timestamp with time zone", 0, true)
	requireColumn(t, tx, "accounts", "rate_limit_reset_at", "timestamp with time zone", 0, true)
	requireColumn(t, tx, "accounts", "overload_until", "timestamp with time zone", 0, true)
	requireColumn(t, tx, "accounts", "session_window_status", "character varying", 20, true)
	requireIndex(t, tx, "accounts", "idx_accounts_autopause_expiry_due")

	// groups: OpenAI Live 默认关闭，管理员显式开启后才可访问。
	requireColumn(t, tx, "groups", "allow_live", "boolean", 0, false)
	requireColumn(t, tx, "groups", "rate_multiplier_cny", "numeric", 0, true)
	requireColumn(t, tx, "groups", "rate_multiplier_usd", "numeric", 0, true)

	// api_keys: key length should be 128
	requireColumn(t, tx, "api_keys", "key", "character varying", 128, false)
	requireColumn(t, tx, "api_keys", "routing_mode", "character varying", 20, false)
	requireColumnDefaultContains(t, tx, "api_keys", "routing_mode", "legacy_group")
	requireConstraintDefinitionContains(
		t,
		tx,
		"api_keys",
		"chk_api_keys_routing_mode",
		"legacy_group",
		"channels",
		"auto_channels",
	)
	requireForeignKeyOnDelete(t, tx, "api_key_channels", "api_key_id", "api_keys", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "api_key_channels", "channel_id", "channels", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "user_default_channels", "user_id", "users", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "user_default_channels", "channel_id", "channels", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "user_disabled_routing_groups", "user_id", "users", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "user_disabled_routing_groups", "group_id", "groups", "CASCADE")

	// redeem_codes: subscription fields
	requireColumn(t, tx, "redeem_codes", "group_id", "bigint", 0, true)
	requireColumn(t, tx, "redeem_codes", "validity_days", "integer", 0, false)

	// usage_logs: billing_type used by filters/stats
	requireColumn(t, tx, "usage_logs", "billing_type", "smallint", 0, false)
	requireColumn(t, tx, "usage_logs", "request_type", "smallint", 0, false)
	requireColumn(t, tx, "usage_logs", "openai_ws_mode", "boolean", 0, false)
	requireColumn(t, tx, "usage_logs", "image_input_size", "character varying", 32, true)
	requireColumn(t, tx, "usage_logs", "image_output_size", "character varying", 32, true)
	requireColumn(t, tx, "usage_logs", "image_size_source", "character varying", 16, true)
	requireColumn(t, tx, "usage_logs", "image_size_breakdown", "jsonb", 0, true)
	requireColumn(t, tx, "usage_logs", "video_count", "integer", 0, false)
	requireColumn(t, tx, "usage_logs", "video_resolution", "character varying", 10, true)
	requireColumn(t, tx, "usage_logs", "video_duration_seconds", "integer", 0, true)
	requireColumn(t, tx, "usage_logs", "source_currency", "character varying", 3, false)
	requireColumn(t, tx, "usage_logs", "settlement_currency", "character varying", 3, false)
	requireColumn(t, tx, "usage_logs", "exchange_rate", "numeric", 0, false)
	requireColumn(t, tx, "usage_logs", "exchange_rate_source", "character varying", 64, false)
	requireColumn(t, tx, "usage_logs", "exchange_rate_as_of", "timestamp with time zone", 0, true)
	requireColumn(t, tx, "usage_logs", "source_cost", "numeric", 0, false)
	requireColumn(t, tx, "usage_logs", "base_cost", "numeric", 0, false)
	requireConstraintDefinitionContains(
		t,
		tx,
		"usage_logs",
		"usage_logs_image_size_source_check",
		"image_size_source",
		"'output'",
		"'input'",
		"'default'",
		"'legacy'",
	)
	requireConstraintDefinitionContains(
		t,
		tx,
		"usage_logs",
		"usage_logs_image_billing_size_check",
		"image_count",
		"billing_mode",
		"'video'",
		"video_count",
		"image_size IS NOT NULL",
		"'1K'",
		"'2K'",
		"'4K'",
		"'mixed'",
	)

	// usage_billing_dedup: billing idempotency narrow table
	var usageBillingDedupRegclass sql.NullString
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT to_regclass('public.usage_billing_dedup')").Scan(&usageBillingDedupRegclass))
	require.True(t, usageBillingDedupRegclass.Valid, "expected usage_billing_dedup table to exist")
	requireColumn(t, tx, "usage_billing_dedup", "request_fingerprint", "character varying", 64, false)
	requireIndex(t, tx, "usage_billing_dedup", "idx_usage_billing_dedup_request_api_key")
	requireIndex(t, tx, "usage_billing_dedup", "idx_usage_billing_dedup_created_at_brin")

	var usageBillingDedupArchiveRegclass sql.NullString
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT to_regclass('public.usage_billing_dedup_archive')").Scan(&usageBillingDedupArchiveRegclass))
	require.True(t, usageBillingDedupArchiveRegclass.Valid, "expected usage_billing_dedup_archive table to exist")
	requireColumn(t, tx, "usage_billing_dedup_archive", "request_fingerprint", "character varying", 64, false)
	requireIndex(t, tx, "usage_billing_dedup_archive", "usage_billing_dedup_archive_pkey")

	// settings table should exist
	var settingsRegclass sql.NullString
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT to_regclass('public.settings')").Scan(&settingsRegclass))
	require.True(t, settingsRegclass.Valid, "expected settings table to exist")

	// security_secrets table should exist
	var securitySecretsRegclass sql.NullString
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT to_regclass('public.security_secrets')").Scan(&securitySecretsRegclass))
	require.True(t, securitySecretsRegclass.Valid, "expected security_secrets table to exist")

	// scheduler_outbox pending dedup support
	requireColumn(t, tx, "scheduler_outbox", "dedup_key", "text", 0, true)
	requireIndex(t, tx, "scheduler_outbox", "idx_scheduler_outbox_pending_dedup_key")

	// ops_system_logs: API key id index for operational log triage
	requireColumn(t, tx, "ops_system_logs", "api_key_id", "bigint", 0, true)
	requireIndex(t, tx, "ops_system_logs", "idx_ops_system_logs_api_key_id_created_at")

	// Bounded ingress rejection security aggregates.
	requireColumn(t, tx, "ops_ingress_reject_aggregates", "bucket_start", "timestamp with time zone", 0, false)
	requireColumn(t, tx, "ops_ingress_reject_aggregates", "client_ip", "inet", 0, false)
	requireColumn(t, tx, "ops_ingress_reject_aggregates", "request_count", "bigint", 0, false)
	requireIndex(t, tx, "ops_ingress_reject_aggregates", "idx_ops_ingress_reject_aggregates_bucket")
	requireIndex(t, tx, "ops_ingress_reject_aggregates", "idx_ops_ingress_reject_aggregates_ip_bucket")

	// user_allowed_groups table should exist
	var uagRegclass sql.NullString
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT to_regclass('public.user_allowed_groups')").Scan(&uagRegclass))
	require.True(t, uagRegclass.Valid, "expected user_allowed_groups table to exist")

	// user_subscriptions: deleted_at for soft delete support (migration 012)
	requireColumn(t, tx, "user_subscriptions", "deleted_at", "timestamp with time zone", 0, true)

	// orphan_allowed_groups_audit table should exist (migration 013)
	var orphanAuditRegclass sql.NullString
	require.NoError(t, tx.QueryRowContext(context.Background(), "SELECT to_regclass('public.orphan_allowed_groups_audit')").Scan(&orphanAuditRegclass))
	require.True(t, orphanAuditRegclass.Valid, "expected orphan_allowed_groups_audit table to exist")

	// account_groups: created_at should be timestamptz
	requireColumn(t, tx, "account_groups", "created_at", "timestamp with time zone", 0, false)

	// user_allowed_groups: created_at should be timestamptz
	requireColumn(t, tx, "user_allowed_groups", "created_at", "timestamp with time zone", 0, false)
}

func TestMigrationsRunner_AuthIdentityAndPaymentSchemaStayAligned(t *testing.T) {
	tx := testTx(t)

	requireColumn(t, tx, "auth_identity_migration_reports", "report_type", "character varying", 80, false)
	requireColumn(t, tx, "users", "signup_source", "character varying", 20, false)
	requireColumnDefaultContains(t, tx, "users", "signup_source", "email")
	requireConstraintDefinitionContains(
		t,
		tx,
		"users",
		"users_signup_source_check",
		"signup_source",
		"'email'",
		"'linuxdo'",
		"'wechat'",
		"'oidc'",
		"'platform'",
	)

	requireForeignKeyOnDelete(t, tx, "auth_identities", "user_id", "users", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "auth_identity_channels", "identity_id", "auth_identities", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "pending_auth_sessions", "target_user_id", "users", "SET NULL")
	requireForeignKeyOnDelete(t, tx, "identity_adoption_decisions", "pending_auth_session_id", "pending_auth_sessions", "CASCADE")
	requireForeignKeyOnDelete(t, tx, "identity_adoption_decisions", "identity_id", "auth_identities", "SET NULL")

	requireIndex(t, tx, "payment_orders", "paymentorder_out_trade_no")
	requirePartialUniqueIndexDefinition(t, tx, "payment_orders", "paymentorder_out_trade_no", "out_trade_no", "WHERE")
	requireIndexAbsent(t, tx, "payment_orders", "paymentorder_out_trade_no_unique")
}

func TestMigrationsRunner_ModelGovernanceFoundationSchema(t *testing.T) {
	tx := testTx(t)

	for _, table := range []string{
		"account_incarnation_ids",
		"model_registry",
		"model_registry_aliases",
		"model_registry_events",
		"model_classification_batches",
		"model_observations",
		"model_observation_events",
		"model_inventory_runs",
		"model_inventory_items",
	} {
		requireTable(t, tx, table)
	}

	requireColumn(t, tx, "model_observations", "connection_id", "bigint", 0, true)
	requireColumn(t, tx, "account_incarnation_ids", "account_id", "bigint", 0, false)
	requireColumn(t, tx, "account_incarnation_ids", "issued_at", "timestamp with time zone", 0, false)
	var accountsMissingFromLedger int
	require.NoError(t, tx.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM accounts a
		LEFT JOIN account_incarnation_ids ledger ON ledger.account_id = a.id
		WHERE ledger.account_id IS NULL
	`).Scan(&accountsMissingFromLedger))
	require.Zero(t, accountsMissingFromLedger, "migration 200 must backfill every account that existed when it was applied")
	requireColumn(t, tx, "model_registry", "canonical_id", "text", 0, false)
	requireColumn(t, tx, "model_registry_aliases", "alias", "text", 0, false)
	requireColumn(t, tx, "model_observations", "upstream_model_id", "text", 0, false)
	requireColumn(t, tx, "model_observations", "snapshot_batch_id", "character varying", 64, false)
	requireColumn(t, tx, "model_observation_events", "account_id", "bigint", 0, false)
	requireColumn(t, tx, "model_observation_events", "upstream_model_id", "text", 0, false)
	requireColumn(t, tx, "model_observation_events", "snapshot_batch_id", "character varying", 64, false)
	requireColumn(t, tx, "usage_logs", "governance_target_platform", "character varying", 32, true)
	requireColumn(t, tx, "model_inventory_items", "target_platform", "character varying", 32, false)
	requireColumn(t, tx, "model_inventory_items", "upstream_model_id", "text", 0, false)
	requireColumnAbsent(t, tx, "model_observations", "raw_snapshot")
	requireConstraintDefinitionContains(t, tx, "model_registry", "chk_model_registry_provider", "anthropic", "openai", "gemini", "grok")
	requireConstraintDefinitionContains(t, tx, "model_registry", "chk_model_registry_modality", "text", "image", "audio", "video", "embedding", "other")
	requireConstraintDefinitionContains(t, tx, "model_registry", "chk_model_registry_lifecycle", "active", "deprecated", "retired")
	requireConstraintDefinitionContains(t, tx, "model_observations", "chk_model_observations_classification", "discovered", "approved", "cross_provider", "unknown", "ignored")
	requireConstraintDefinitionContains(t, tx, "model_observations", "chk_model_observations_presence", "present", "missing")
	requireNoUniqueConstraint(t, tx, "model_registry", "canonical_id")
	requireNoUniqueConstraint(t, tx, "model_registry_aliases", "alias")
	requireNoUniqueConstraint(t, tx, "model_observations", "account_id", "upstream_model_id")
	for _, index := range []struct{ table, name string }{
		{"model_registry", "idx_model_registry_canonical_bucket"},
		{"model_registry_aliases", "idx_model_registry_aliases_alias_bucket"},
		{"model_observations", "idx_model_observations_identity_bucket"},
		{"model_inventory_items", "idx_model_inventory_items_identity_bucket"},
	} {
		requireIndex(t, tx, index.table, index.name)
	}
	requireIndexAbsent(t, tx, "model_inventory_items", "uq_model_inventory_items_dimensions")
	requireAppendOnlyTable(t, tx, "model_registry_events")
	requireAppendOnlyTable(t, tx, "account_incarnation_ids")
	requireAppendOnlyTable(t, tx, "model_classification_batches")
	requireAppendOnlyTable(t, tx, "model_observation_events")
	requireGovernanceTriggerMetadata(t, tx)
	requireNoForeignKey(t, tx, "model_classification_batches", "account_id")
	requireNoForeignKey(t, tx, "model_registry_events", "registry_id")
	requireNoForeignKey(t, tx, "model_observation_events", "observation_id")
	requireForeignKeyOnDelete(t, tx, "model_observations", "snapshot_batch_id", "model_classification_batches", "RESTRICT")
	requireForeignKeyOnDelete(t, tx, "model_observation_events", "batch_id", "model_classification_batches", "RESTRICT")
	requireForeignKeyOnDelete(t, tx, "model_observation_events", "snapshot_batch_id", "model_classification_batches", "RESTRICT")
	requireExactModelTables(t, tx)
	requireNoActivationTables(t, tx)
}

func TestMigrationsRunner_ModelGovernanceFoundationExactChecks(t *testing.T) {
	tx := testTx(t)

	requireCheckValues(t, tx, "model_registry", "chk_model_registry_provider", []string{"anthropic", "gemini", "grok", "openai"})
	requireCheckValues(t, tx, "model_registry", "chk_model_registry_modality", []string{"audio", "embedding", "image", "other", "text", "video"})
	requireCheckValues(t, tx, "model_registry", "chk_model_registry_lifecycle", []string{"active", "deprecated", "retired"})
	requireCheckValues(t, tx, "model_observations", "chk_model_observations_classification", []string{"approved", "cross_provider", "discovered", "ignored", "unknown"})
	requireCheckValues(t, tx, "model_observations", "chk_model_observations_presence", []string{"missing", "present"})
	requireCheckValues(t, tx, "model_observation_events", "chk_model_observation_events_classification", []string{"approved", "cross_provider", "discovered", "ignored", "unknown"})
	requireCheckValues(t, tx, "model_observation_events", "chk_model_observation_events_presence", []string{"missing", "present"})
	requireCheckValues(t, tx, "model_inventory_items", "chk_model_inventory_items_classification", []string{"approved", "cross_provider", "discovered", "ignored", "unknown"})
	requireCheckValues(t, tx, "usage_logs", "chk_usage_logs_governance_target_platform", []string{"anthropic", "antigravity", "gemini", "grok", "openai"})
}

func TestMigrationsRunner_ModelGovernanceFoundationPreservesEventsWhenAccountDeleted(t *testing.T) {
	tx := testTx(t)
	suffix := time.Now().UnixNano()

	var accountID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO accounts (name, platform, type)
VALUES ($1, 'anthropic', 'apikey')
RETURNING id
`, fmt.Sprintf("governance-delete-%d", suffix)).Scan(&accountID))

	batchID := fmt.Sprintf("batch-%d", suffix)
	_, err := tx.ExecContext(context.Background(), `
INSERT INTO model_classification_batches (batch_id, idempotency_key, account_id, observed_at)
VALUES ($1, $2, $3, NOW())
`, batchID, fmt.Sprintf("batch-key-%d", suffix), accountID)
	require.NoError(t, err)

	var observationID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
VALUES ($1, $2, $3, NOW(), NOW())
RETURNING id
`, accountID, fmt.Sprintf("model-%d", suffix), batchID).Scan(&observationID))

	var eventID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_observation_events (
	observation_id, batch_id, snapshot_batch_id, account_id, upstream_model_id,
	event_type, classification, classification_reason, upstream_presence
)
VALUES ($1, $2, $2, $3, 'immutable-model', 'discovered', 'discovered', 'awaiting_registry_classification', 'present')
RETURNING id
`, observationID, batchID, accountID).Scan(&eventID))

	_, err = tx.ExecContext(context.Background(), "DELETE FROM accounts WHERE id = $1", accountID)
	require.NoError(t, err, "existing account deletion must not mutate append-only governance events")

	var storedObservationID int64
	var storedBatchID string
	var storedAccountID int64
	var storedModelID string
	require.NoError(t, tx.QueryRowContext(context.Background(), `
SELECT observation_id, batch_id, account_id, upstream_model_id
FROM model_observation_events WHERE id = $1
`, eventID).Scan(&storedObservationID, &storedBatchID, &storedAccountID, &storedModelID))
	require.Equal(t, observationID, storedObservationID)
	require.Equal(t, batchID, storedBatchID)
	require.Equal(t, accountID, storedAccountID)
	require.Equal(t, "immutable-model", storedModelID)
}

func TestMigrationsRunner_ModelGovernanceFoundationRejectsHistoricalAccountIDReuse(t *testing.T) {
	ctx := context.Background()
	tx := testTx(t)
	suffix := time.Now().UnixNano()
	explicitID := suffix

	_, err := tx.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
	`, explicitID, fmt.Sprintf("governance-first-incarnation-%d", suffix))
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE accounts SET name = $2 WHERE id = $1`, explicitID, fmt.Sprintf("governance-updated-%d", suffix))
	require.NoError(t, err, "updates must not invoke the incarnation insert guard")
	_, err = tx.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, explicitID)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `SAVEPOINT account_id_reuse`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
	`, explicitID, fmt.Sprintf("governance-reused-%d", suffix))
	var pqErr *pq.Error
	require.ErrorAs(t, err, &pqErr)
	require.Equal(t, pq.ErrorCode("23505"), pqErr.Code)
	require.Equal(t, "account_id_incarnation_not_reused", pqErr.Constraint)
	require.ErrorContains(t, err, "account ID is a permanent incarnation identifier")
	_, rollbackErr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT account_id_reuse`)
	require.NoError(t, rollbackErr)
	var ledgerCount int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id = $1`, explicitID).Scan(&ledgerCount))
	require.Equal(t, 1, ledgerCount, "hard deletion must retain the permanent account ID ledger entry")
	var accountCount int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts WHERE id = $1`, explicitID).Scan(&accountCount))
	require.Zero(t, accountCount, "the rejected historical-ID insertion must roll back the proposed account row")

	var generatedID int64
	require.NoError(t, tx.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'openai', 'apikey') RETURNING id
	`, fmt.Sprintf("governance-generated-%d", suffix)).Scan(&generatedID))
	require.NotEqual(t, explicitID, generatedID)
}

func TestMigrationsRunner_ModelGovernanceFoundationConcurrentHistoricalAccountIDReuseFailsClosed(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'openai', 'apikey') RETURNING id
	`, fmt.Sprintf("governance-concurrent-incarnation-%d", suffix)).Scan(&accountID))
	_, err := integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, insertErr := integrationDB.ExecContext(ctx, `
				INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
			`, accountID, fmt.Sprintf("governance-concurrent-reuse-%d", suffix))
			errs <- insertErr
		}()
	}
	close(start)
	for range 2 {
		var pqErr *pq.Error
		require.ErrorAs(t, <-errs, &pqErr)
		require.Equal(t, pq.ErrorCode("23505"), pqErr.Code)
		require.Equal(t, "account_id_incarnation_not_reused", pqErr.Constraint)
	}
}

func TestMigrationsRunner_ModelGovernanceFoundationInstallLockClosesBackfillTriggerGap(t *testing.T) {
	ctx := context.Background()
	raw, err := os.ReadFile("../../migrations/200_model_governance_foundation.sql")
	require.NoError(t, err)
	lockStatement := "LOCK TABLE accounts IN SHARE ROW EXCLUSIVE MODE"
	require.Contains(t, string(raw), lockStatement,
		"the live lock-semantics test must execute the production migration lock statement")

	lockTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = lockTx.Rollback() }()
	_, err = lockTx.ExecContext(ctx, lockStatement)
	require.NoError(t, err)

	insertConn, err := integrationDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, insertConn.Close()) }()
	var insertPID int
	require.NoError(t, insertConn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&insertPID))

	explicitID := time.Now().UnixNano()
	insertResult := make(chan error, 1)
	go func() {
		_, insertErr := insertConn.ExecContext(ctx, `
			INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
		`, explicitID, fmt.Sprintf("governance-install-lock-%d", explicitID))
		insertResult <- insertErr
	}()

	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := integrationDB.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE pid = $1
				  AND locktype = 'relation'
				  AND relation = 'accounts'::regclass
				  AND NOT granted
			)
		`, insertPID).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond,
		"concurrent account insert must wait until the migration backfill/trigger transaction commits")

	require.NoError(t, lockTx.Commit())
	require.NoError(t, <-insertResult)
	var ledgerCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id = $1
	`, explicitID).Scan(&ledgerCount))
	require.Equal(t, 1, ledgerCount, "the insert released after migration commit must be claimed by the ledger trigger")
}

func TestMigrationsRunner_ModelGovernanceFoundationFailedAccountInsertDoesNotConsumeID(t *testing.T) {
	ctx := context.Background()
	explicitID := time.Now().UnixNano()
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, NULL, 'openai', 'apikey')
	`, explicitID)
	var pqErr *pq.Error
	require.ErrorAs(t, err, &pqErr)
	require.Equal(t, pq.ErrorCode("23502"), pqErr.Code)

	var ledgerCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id = $1
	`, explicitID).Scan(&ledgerCount))
	require.Zero(t, ledgerCount, "the ledger claim in a failed account insert must roll back with its transaction")
}

func TestMigrationsRunner_ModelGovernanceFoundationConflictProposalsDoNotClaimAccountIDs(t *testing.T) {
	requireAccountConflictProposalsDoNotClaimIDs(t, "focused-conflict")
}

func TestMigrationsRunner_ModelGovernanceFoundationOnConflictIDUsesAfterInsertSemantics(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	existingID := suffix
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
	`, existingID, fmt.Sprintf("governance-id-conflict-%d", suffix))
	require.NoError(t, err)

	result, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
		ON CONFLICT (id) DO NOTHING
	`, existingID, fmt.Sprintf("governance-id-nothing-%d", suffix))
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, rows, "DO NOTHING must skip the AFTER INSERT trigger")

	updatedName := fmt.Sprintf("governance-id-update-%d", suffix)
	result, err = integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
	`, existingID, updatedName)
	require.NoError(t, err)
	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)

	var storedName string
	var ledgerCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT name FROM accounts WHERE id = $1`, existingID).Scan(&storedName))
	require.Equal(t, updatedName, storedName)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id = $1`, existingID).Scan(&ledgerCount))
	require.Equal(t, 1, ledgerCount, "conflict update must retain the existing incarnation claim without creating another")
}

func TestMigrationsRunner_ModelGovernanceFoundationSequenceIDsAreClaimed(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	var firstID, secondID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'openai', 'apikey') RETURNING id
	`, fmt.Sprintf("governance-sequence-first-%d", suffix)).Scan(&firstID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'openai', 'apikey') RETURNING id
	`, fmt.Sprintf("governance-sequence-second-%d", suffix)).Scan(&secondID))
	require.Greater(t, secondID, firstID)

	var claimed int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id IN ($1, $2)
	`, firstID, secondID).Scan(&claimed))
	require.Equal(t, 2, claimed)
}

func TestMigrationsRunner_ModelGovernanceFoundationRejectsAccountIDUpdate(t *testing.T) {
	requireAccountIDUpdateRejected(t, "focused-id-update")
}

func TestMigrationsRunner_ModelGovernanceFoundationRejectsAppendOnlyTruncatesAndPreservesRows(t *testing.T) {
	requireAppendOnlyTruncatesRejectedAndRowsPreserved(t, "focused-truncate")
}

func requireAppendOnlyTruncatesRejectedAndRowsPreserved(t *testing.T, label string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	accountID := requireUnusedAccountID(t)
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
	`, accountID, fmt.Sprintf("governance-%s-%d", label, suffix))
	require.NoError(t, err)
	registryKey := fmt.Sprintf("%s-registry-%d", label, suffix)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO model_registry_events (idempotency_key, event_type, registry_version, actor_id)
		VALUES ($1, 'created', $2, 'integration-test')
	`, registryKey, suffix)
	require.NoError(t, err)
	batchID := fmt.Sprintf("%s-batch-%d", label, suffix)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO model_classification_batches (batch_id, idempotency_key, account_id, observed_at)
		VALUES ($1, $1, $2, NOW())
	`, batchID, accountID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO model_observation_events (
			observation_id, batch_id, snapshot_batch_id, account_id, upstream_model_id,
			event_type, classification, classification_reason, upstream_presence
		) VALUES ($1, $2, $2, $1, 'truncate-model', 'observed', 'discovered', 'integration_test', 'present')
	`, accountID, batchID)
	require.NoError(t, err)

	truncates := []struct {
		name            string
		query           string
		appendOnlyError bool
	}{
		{name: "ledger direct", query: `TRUNCATE account_incarnation_ids`, appendOnlyError: true},
		{name: "registry events direct", query: `TRUNCATE model_registry_events`, appendOnlyError: true},
		{name: "classification batches direct", query: `TRUNCATE model_classification_batches`},
		{name: "observation events direct", query: `TRUNCATE model_observation_events`, appendOnlyError: true},
		{name: "classification batches cascade", query: `TRUNCATE model_classification_batches CASCADE`, appendOnlyError: true},
		{name: "explicit multi-table", query: `TRUNCATE account_incarnation_ids, model_registry_events, model_observation_events`, appendOnlyError: true},
	}
	for _, tt := range truncates {
		t.Run(tt.name, func(t *testing.T) {
			tx, beginErr := integrationDB.BeginTx(ctx, nil)
			require.NoError(t, beginErr)
			_, truncateErr := tx.ExecContext(ctx, tt.query)
			require.NoError(t, tx.Rollback())
			var pqErr *pq.Error
			require.ErrorAs(t, truncateErr, &pqErr)
			if tt.appendOnlyError {
				require.Equal(t, pq.ErrorCode("P0001"), pqErr.Code)
				require.ErrorContains(t, truncateErr, "append-only; TRUNCATE is not allowed")
			} else {
				require.Equal(t, pq.ErrorCode("0A000"), pqErr.Code)
				require.ErrorContains(t, truncateErr, "cannot truncate a table referenced in a foreign key constraint")
			}
		})
	}

	for _, check := range []struct {
		query string
		arg   any
	}{
		{query: `SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id = $1`, arg: accountID},
		{query: `SELECT COUNT(*) FROM model_registry_events WHERE idempotency_key = $1`, arg: registryKey},
		{query: `SELECT COUNT(*) FROM model_classification_batches WHERE batch_id = $1`, arg: batchID},
		{query: `SELECT COUNT(*) FROM model_observation_events WHERE batch_id = $1`, arg: batchID},
	} {
		var count int
		require.NoError(t, integrationDB.QueryRowContext(ctx, check.query, check.arg).Scan(&count))
		require.Equal(t, 1, count, "rejected truncate must preserve the seeded row")
	}
}

func TestMigrationsRunner_ModelGovernanceFoundationEventsRejectActualMutations(t *testing.T) {
	tx := testTx(t)
	suffix := time.Now().UnixNano()
	var accountID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'openai', 'apikey') RETURNING id
	`, fmt.Sprintf("governance-ledger-append-only-%d", suffix)).Scan(&accountID))
	_, err := tx.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
	require.NoError(t, err)

	var registryEventID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_registry_events (idempotency_key, event_type, registry_version, actor_id)
VALUES ($1, 'created', $2, 'integration-test')
RETURNING id
`, fmt.Sprintf("registry-event-%d", suffix), suffix).Scan(&registryEventID))

	var observationEventID int64
	mutationBatchID := fmt.Sprintf("mutation-batch-%d", suffix)
	var mutationBatchRowID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_classification_batches (batch_id, idempotency_key, account_id, observed_at)
VALUES ($1, $2, $3, NOW())
RETURNING id
`, mutationBatchID, fmt.Sprintf("mutation-batch-key-%d", suffix), suffix).Scan(&mutationBatchRowID))
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_observation_events (
	observation_id, batch_id, snapshot_batch_id, account_id, upstream_model_id,
	event_type, classification, classification_reason, upstream_presence
)
VALUES ($1, $2, $2, $1, 'mutation-model', 'discovered', 'discovered', 'integration_test', 'present')
RETURNING id
`, suffix, mutationBatchID).Scan(&observationEventID))

	for _, mutation := range []struct {
		name string
		sql  string
		id   int64
	}{
		{name: "update account incarnation ledger", sql: "UPDATE account_incarnation_ids SET issued_at = issued_at WHERE account_id = $1", id: accountID},
		{name: "delete account incarnation ledger", sql: "DELETE FROM account_incarnation_ids WHERE account_id = $1", id: accountID},
		{name: "update registry event", sql: "UPDATE model_registry_events SET actor_id = 'changed' WHERE id = $1", id: registryEventID},
		{name: "delete registry event", sql: "DELETE FROM model_registry_events WHERE id = $1", id: registryEventID},
		{name: "update classification batch", sql: "UPDATE model_classification_batches SET raw_snapshot = raw_snapshot WHERE id = $1", id: mutationBatchRowID},
		{name: "delete classification batch", sql: "DELETE FROM model_classification_batches WHERE id = $1", id: mutationBatchRowID},
		{name: "update observation event", sql: "UPDATE model_observation_events SET classification_reason = 'changed' WHERE id = $1", id: observationEventID},
		{name: "delete observation event", sql: "DELETE FROM model_observation_events WHERE id = $1", id: observationEventID},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			_, err := tx.ExecContext(context.Background(), "SAVEPOINT append_only_mutation")
			require.NoError(t, err)
			_, err = tx.ExecContext(context.Background(), mutation.sql, mutation.id)
			require.ErrorContains(t, err, "append-only")
			_, rollbackErr := tx.ExecContext(context.Background(), "ROLLBACK TO SAVEPOINT append_only_mutation")
			require.NoError(t, rollbackErr)
		})
	}
}

func TestMigrationsRunner_ModelGovernanceFoundationInventoryUniquenessTreatsNullsAsEqual(t *testing.T) {
	tx := testTx(t)
	suffix := time.Now().UnixNano()

	var accountID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO accounts (name, platform, type)
VALUES ($1, 'anthropic', 'apikey')
RETURNING id
`, fmt.Sprintf("governance-inventory-%d", suffix)).Scan(&accountID))

	runID := fmt.Sprintf("inventory-%d", suffix)
	_, err := tx.ExecContext(context.Background(), `
INSERT INTO model_inventory_runs (run_id, cutoff_7d, cutoff_30d)
VALUES ($1, NOW() - INTERVAL '7 days', NOW() - INTERVAL '30 days')
`, runID)
	require.NoError(t, err)

	insert := `
INSERT INTO model_inventory_items (
	run_id, account_id, group_id, channel_id, target_platform, upstream_model_id, classification, billing_currency
)
VALUES ($1, $2, NULL, NULL, 'anthropic', 'claude-test', 'approved', 'USD')
`
	_, err = tx.ExecContext(context.Background(), insert, runID, accountID)
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), `
INSERT INTO model_inventory_items (
	run_id, account_id, group_id, channel_id, target_platform, upstream_model_id, classification, billing_currency
)
VALUES ($1, $2, NULL, NULL, 'gemini', 'claude-test', 'approved', 'USD')
`, runID, accountID)
	require.NoError(t, err, "the same model under a different target platform is a distinct inventory identity")
	_, err = tx.ExecContext(context.Background(), "SAVEPOINT duplicate_inventory_item")
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), insert, runID, accountID)
	require.Error(t, err, "duplicate account-level inventory rows with NULL group/channel must be rejected")
	_, rollbackErr := tx.ExecContext(context.Background(), "ROLLBACK TO SAVEPOINT duplicate_inventory_item")
	require.NoError(t, rollbackErr)

	_, err = tx.ExecContext(context.Background(), `
INSERT INTO groups (id, name, status) VALUES (0, $1, 'active')
`, fmt.Sprintf("governance-zero-group-%d", suffix))
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), `
INSERT INTO channels (id, name, status) VALUES (0, $1, 'active')
`, fmt.Sprintf("governance-zero-channel-%d", suffix))
	require.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), `
INSERT INTO model_inventory_items (
	run_id, account_id, group_id, channel_id, target_platform, upstream_model_id, classification, billing_currency
)
VALUES ($1, $2, 0, 0, 'anthropic', 'claude-test', 'approved', 'USD')
`, runID, accountID)
	require.NoError(t, err, "real zero dimensions must not collide with NULL dimensions")
}

func TestMigrationsRunner_ModelGovernanceExactIdentityWritesRequireReadCommitted(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'openai', 'apikey') RETURNING id
	`, fmt.Sprintf("governance-isolation-%d", suffix)).Scan(&accountID))
	batchID := fmt.Sprintf("isolation-batch-%d", suffix)
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO model_classification_batches (batch_id, idempotency_key, account_id, observed_at)
		VALUES ($1, $1, $2, NOW())
	`, batchID, accountID)
	require.NoError(t, err)
	var registryID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_registry (canonical_id, provider, modality, decided_by)
		VALUES ($1, 'openai', 'text', 'integration-test') RETURNING id
	`, fmt.Sprintf("isolation-registry-base-%d", suffix)).Scan(&registryID))
	var observationID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
		VALUES ($1, $2, $3, NOW(), NOW()) RETURNING id
	`, accountID, fmt.Sprintf("isolation-observation-base-%d", suffix), batchID).Scan(&observationID))
	runID := fmt.Sprintf("isolation-run-%d", suffix)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO model_inventory_runs (run_id, cutoff_7d, cutoff_30d)
		VALUES ($1, NOW() - INTERVAL '7 days', NOW() - INTERVAL '30 days')
	`, runID)
	require.NoError(t, err)

	readOnlyTx, err := integrationDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	var registryCount int
	require.NoError(t, readOnlyTx.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_registry`).Scan(&registryCount))
	require.NoError(t, readOnlyTx.Commit(), "read-only repeatable-read transactions do not invoke write triggers")

	tests := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "registry insert",
			sql:  `INSERT INTO model_registry (canonical_id, provider, modality, decided_by) VALUES ($1, 'openai', 'text', 'integration-test')`,
			args: []any{fmt.Sprintf("isolation-registry-%d", suffix)},
		},
		{
			name: "alias insert",
			sql:  `INSERT INTO model_registry_aliases (registry_id, alias) VALUES ($1, $2)`,
			args: []any{registryID, fmt.Sprintf("isolation-alias-%d", suffix)},
		},
		{
			name: "observation insert",
			sql: `INSERT INTO model_observations
				(account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
				VALUES ($1, $2, $3, NOW(), NOW())`,
			args: []any{accountID, fmt.Sprintf("isolation-observation-%d", suffix), batchID},
		},
		{
			name: "observation identity update",
			sql:  `UPDATE model_observations SET upstream_model_id = $1 WHERE id = $2`,
			args: []any{fmt.Sprintf("isolation-observation-update-%d", suffix), observationID},
		},
		{
			name: "inventory insert",
			sql: `INSERT INTO model_inventory_items
				(run_id, account_id, group_id, channel_id, target_platform, upstream_model_id, classification, billing_currency)
				VALUES ($1, $2, NULL, NULL, 'openai', $3, 'approved', 'USD')`,
			args: []any{runID, accountID, fmt.Sprintf("isolation-inventory-%d", suffix)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, beginErr := integrationDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
			require.NoError(t, beginErr)
			_, execErr := tx.ExecContext(ctx, tt.sql, tt.args...)
			var pqErr *pq.Error
			require.ErrorAs(t, execErr, &pqErr)
			require.Equal(t, pq.ErrorCode("0A000"), pqErr.Code)
			require.ErrorContains(t, execErr, "READ COMMITTED")
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestMigrationsRunner_ModelGovernanceInventoryConcurrentNullableDuplicateAtReadCommitted(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'anthropic', 'apikey') RETURNING id
	`, fmt.Sprintf("governance-inventory-concurrent-%d", suffix)).Scan(&accountID))
	runID := fmt.Sprintf("inventory-concurrent-%d", suffix)
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO model_inventory_runs (run_id, cutoff_7d, cutoff_30d)
		VALUES ($1, NOW() - INTERVAL '7 days', NOW() - INTERVAL '30 days')
	`, runID)
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			tx, beginErr := integrationDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if beginErr != nil {
				errs <- beginErr
				return
			}
			_, insertErr := tx.ExecContext(ctx, `
				INSERT INTO model_inventory_items
					(run_id, account_id, group_id, channel_id, target_platform, upstream_model_id, classification, billing_currency)
				VALUES ($1, $2, NULL, NULL, 'anthropic', 'concurrent-nullable-model', 'approved', 'USD')
			`, runID, accountID)
			if insertErr != nil {
				_ = tx.Rollback()
				errs <- insertErr
				return
			}
			errs <- tx.Commit()
		}()
	}
	close(start)
	var successes, uniqueViolations int
	for range 2 {
		insertErr := <-errs
		if insertErr == nil {
			successes++
		} else if pqErr, ok := insertErr.(*pq.Error); ok && pqErr.Code == "23505" {
			uniqueViolations++
		} else {
			require.NoError(t, insertErr)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, uniqueViolations)
}

func TestMigrationsRunner_ModelGovernanceRegistryAndAliasConcurrentDuplicatesAtReadCommitted(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	var registryID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_registry (canonical_id, provider, modality, decided_by)
		VALUES ($1, 'openai', 'text', 'integration-test') RETURNING id
	`, fmt.Sprintf("concurrent-alias-registry-%d", suffix)).Scan(&registryID))

	tests := []struct {
		name string
		exec func(*sql.Tx) error
	}{
		{
			name: "registry canonical id",
			exec: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `
					INSERT INTO model_registry (canonical_id, provider, modality, decided_by)
					VALUES ($1, 'openai', 'text', 'integration-test')
				`, fmt.Sprintf("concurrent-registry-%d", suffix))
				return err
			},
		},
		{
			name: "registry alias",
			exec: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `
					INSERT INTO model_registry_aliases (registry_id, alias) VALUES ($1, $2)
				`, registryID, fmt.Sprintf("concurrent-alias-%d", suffix))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := make(chan struct{})
			errs := make(chan error, 2)
			for range 2 {
				go func() {
					<-start
					tx, beginErr := integrationDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
					if beginErr != nil {
						errs <- beginErr
						return
					}
					if execErr := tt.exec(tx); execErr != nil {
						_ = tx.Rollback()
						errs <- execErr
						return
					}
					errs <- tx.Commit()
				}()
			}
			close(start)
			var successes, uniqueViolations int
			for range 2 {
				execErr := <-errs
				if execErr == nil {
					successes++
				} else if pqErr, ok := execErr.(*pq.Error); ok && pqErr.Code == "23505" {
					uniqueViolations++
				} else {
					require.NoError(t, execErr)
				}
			}
			require.Equal(t, 1, successes)
			require.Equal(t, 1, uniqueViolations)
		})
	}
}

func TestMigrationsRunner_ModelGovernanceFoundationUnboundedExactIdentityUniqueness(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	longID := "vendor/" + strings.Repeat("exact-model-segment-", 220)
	distinctLongID := longID + "-distinct"
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'openai', 'apikey') RETURNING id
	`, fmt.Sprintf("long-exact-%d", suffix)).Scan(&accountID))
	batchID := fmt.Sprintf("long-exact-batch-%d", suffix)
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO model_classification_batches (batch_id, idempotency_key, account_id, observed_at)
		VALUES ($1, $1, $2, NOW())
	`, batchID, accountID)
	require.NoError(t, err)

	var registryID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_registry (canonical_id, provider, modality, decided_by)
		VALUES ($1, 'openai', 'text', 'integration-test') RETURNING id
	`, longID).Scan(&registryID))
	var distinctRegistryID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_registry (canonical_id, provider, modality, decided_by)
		VALUES ($1, 'openai', 'text', 'integration-test') RETURNING id
	`, distinctLongID).Scan(&distinctRegistryID))
	requireUniqueViolation(t, integrationDB, `
		INSERT INTO model_registry (canonical_id, provider, modality, decided_by)
		VALUES ($1, 'openai', 'text', 'integration-test')
	`, longID)
	requireUniqueViolation(t, integrationDB, `UPDATE model_registry SET canonical_id = $1 WHERE id = $2`, longID, distinctRegistryID)

	_, err = integrationDB.ExecContext(ctx, `INSERT INTO model_registry_aliases (registry_id, alias) VALUES ($1, $2)`, registryID, longID)
	require.NoError(t, err)
	var distinctAliasID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_registry_aliases (registry_id, alias) VALUES ($1, $2) RETURNING id
	`, registryID, distinctLongID).Scan(&distinctAliasID))
	requireUniqueViolation(t, integrationDB, `INSERT INTO model_registry_aliases (registry_id, alias) VALUES ($1, $2)`, registryID, longID)
	requireUniqueViolation(t, integrationDB, `UPDATE model_registry_aliases SET alias = $1 WHERE id = $2`, longID, distinctAliasID)

	var observationID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
		VALUES ($1, $2, $3, NOW(), NOW()) RETURNING id
	`, accountID, longID, batchID).Scan(&observationID))
	var distinctObservationID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
		VALUES ($1, $2, $3, NOW(), NOW()) RETURNING id
	`, accountID, distinctLongID, batchID).Scan(&distinctObservationID))
	requireUniqueViolation(t, integrationDB, `
		INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
		VALUES ($1, $2, $3, NOW(), NOW())
	`, accountID, longID, batchID)
	requireUniqueViolation(t, integrationDB, `
		UPDATE model_observations SET upstream_model_id = $1 WHERE id = $2
	`, longID, distinctObservationID)

	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO model_observation_events (
			observation_id, batch_id, snapshot_batch_id, account_id, upstream_model_id,
			event_type, classification, classification_reason, upstream_presence
		) VALUES ($1, $2, $2, $3, $4, 'observed', 'discovered', 'integration_test', 'present')
	`, observationID, batchID, accountID, longID)
	require.NoError(t, err)

	runID := fmt.Sprintf("long-exact-run-%d", suffix)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO model_inventory_runs (run_id, cutoff_7d, cutoff_30d)
		VALUES ($1, NOW() - INTERVAL '7 days', NOW() - INTERVAL '30 days')
	`, runID)
	require.NoError(t, err)
	insertInventory := `
		INSERT INTO model_inventory_items (
			run_id, account_id, group_id, channel_id, target_platform, upstream_model_id, classification, billing_currency
		) VALUES ($1, $2, NULL, NULL, 'openai', $3, 'approved', 'USD')
	`
	_, err = integrationDB.ExecContext(ctx, insertInventory, runID, accountID, longID)
	require.NoError(t, err)
	var distinctInventoryID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO model_inventory_items (
			run_id, account_id, group_id, channel_id, target_platform, upstream_model_id, classification, billing_currency
		) VALUES ($1, $2, NULL, NULL, 'openai', $3, 'approved', 'USD') RETURNING id
	`, runID, accountID, distinctLongID).Scan(&distinctInventoryID))
	requireUniqueViolation(t, integrationDB, insertInventory, runID, accountID, longID)
	requireUniqueViolation(t, integrationDB, `
		UPDATE model_inventory_items SET upstream_model_id = $1 WHERE id = $2
	`, longID, distinctInventoryID)

	concurrentID := longID + "-concurrent"
	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, insertErr := integrationDB.ExecContext(ctx, `
				INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
				VALUES ($1, $2, $3, NOW(), NOW())
			`, accountID, concurrentID, batchID)
			errs <- insertErr
		}()
	}
	close(start)
	var successes, uniqueViolations int
	for range 2 {
		if insertErr := <-errs; insertErr == nil {
			successes++
		} else if pqErr, ok := insertErr.(*pq.Error); ok && pqErr.Code == "23505" {
			uniqueViolations++
		} else {
			require.NoError(t, insertErr)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, uniqueViolations)

	blockedID := longID + "-blocked-concurrent"
	firstTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = firstTx.ExecContext(ctx, `
		INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
		VALUES ($1, $2, $3, NOW(), NOW())
	`, accountID, blockedID, batchID)
	require.NoError(t, err)
	secondConn, err := integrationDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, secondConn.Close()) }()
	var secondPID int
	require.NoError(t, secondConn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&secondPID))
	blockedResult := make(chan error, 1)
	go func() {
		_, insertErr := secondConn.ExecContext(ctx, `
			INSERT INTO model_observations (account_id, upstream_model_id, snapshot_batch_id, first_seen_at, last_seen_at)
			VALUES ($1, $2, $3, NOW(), NOW())
		`, accountID, blockedID, batchID)
		blockedResult <- insertErr
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := integrationDB.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE pid = $1 AND locktype = 'advisory' AND NOT granted
			)
		`, secondPID).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond, "second insert must wait on the exact-identity advisory lock")
	require.NoError(t, firstTx.Commit())
	requireUniqueViolationError(t, <-blockedResult)
}

func TestMigrationsRunner_ModelGovernanceFoundationSQLRerunsDirectly(t *testing.T) {
	raw, err := os.ReadFile("../../migrations/200_model_governance_foundation.sql")
	require.NoError(t, err)

	_, err = integrationDB.ExecContext(context.Background(), string(raw))
	require.NoError(t, err, "first direct SQL rerun on an already migrated schema")
	_, err = integrationDB.ExecContext(context.Background(), string(raw))
	require.NoError(t, err, "second direct SQL rerun on the same schema")

	tx := testTx(t)
	requireGovernanceTriggerMetadata(t, tx)
	require.NoError(t, tx.Commit())
	requireAppendOnlyTruncatesRejectedAndRowsPreserved(t, "rerun-truncate")
	requireAccountConflictProposalsDoNotClaimIDs(t, "rerun-conflict")
	requireHistoricalAccountIDReuseRejected(t, "rerun-historical")
	requireAccountIDUpdateRejected(t, "rerun-id-update")
}

func requireTable(t *testing.T, tx *sql.Tx, table string) {
	t.Helper()

	var exists bool
	err := tx.QueryRowContext(context.Background(), "SELECT to_regclass('public.' || $1) IS NOT NULL", table).Scan(&exists)
	require.NoError(t, err, "query table %s", table)
	require.True(t, exists, "expected table %s to exist", table)
}

func requireUniqueConstraint(t *testing.T, tx *sql.Tx, table string, columns ...string) {
	t.Helper()

	var exists bool
	err := tx.QueryRowContext(context.Background(), `
SELECT EXISTS (
	SELECT 1
	FROM pg_constraint c
	JOIN pg_class tbl ON tbl.oid = c.conrelid
	JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
	WHERE ns.nspname = 'public'
	  AND tbl.relname = $1
	  AND c.contype = 'u'
	  AND ARRAY(
		SELECT attr.attname::text
		FROM unnest(c.conkey) WITH ORDINALITY AS key(attnum, ord)
		JOIN pg_attribute attr ON attr.attrelid = tbl.oid AND attr.attnum = key.attnum
		ORDER BY key.ord
	  ) = $2::text[]
)
`, table, pq.Array(columns)).Scan(&exists)
	require.NoError(t, err, "query unique constraint on %s", table)
	require.True(t, exists, "expected unique constraint on %s(%v)", table, columns)
}

func requireNoUniqueConstraint(t *testing.T, tx *sql.Tx, table string, columns ...string) {
	t.Helper()
	var exists bool
	require.NoError(t, tx.QueryRowContext(context.Background(), `
SELECT EXISTS (
	SELECT 1
	FROM pg_constraint c
	JOIN pg_class tbl ON tbl.oid = c.conrelid
	JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
	WHERE ns.nspname = 'public' AND tbl.relname = $1 AND c.contype = 'u'
	  AND ARRAY(
		SELECT attr.attname::text
		FROM unnest(c.conkey) WITH ORDINALITY AS key(attnum, ord)
		JOIN pg_attribute attr ON attr.attrelid = tbl.oid AND attr.attnum = key.attnum
		ORDER BY key.ord
	  ) = $2::text[]
)
`, table, pq.Array(columns)).Scan(&exists))
	require.False(t, exists, "unexpected unique constraint on %s(%v)", table, columns)
}

func requireUniqueViolation(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), query, args...)
	requireUniqueViolationError(t, err)
}

func requireUniqueViolationError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var pqErr *pq.Error
	require.ErrorAs(t, err, &pqErr)
	require.Equal(t, pq.ErrorCode("23505"), pqErr.Code)
}

func requireNullsNotDistinctUniqueConstraint(t *testing.T, tx *sql.Tx, table string, columns ...string) {
	t.Helper()

	var exists bool
	err := tx.QueryRowContext(context.Background(), `
SELECT EXISTS (
	SELECT 1
	FROM pg_constraint c
	JOIN pg_class tbl ON tbl.oid = c.conrelid
	JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
	JOIN pg_index idx ON idx.indexrelid = c.conindid
	WHERE ns.nspname = 'public'
	  AND tbl.relname = $1
	  AND c.contype = 'u'
	  AND idx.indnullsnotdistinct
	  AND ARRAY(
		SELECT attr.attname::text
		FROM unnest(c.conkey) WITH ORDINALITY AS key(attnum, ord)
		JOIN pg_attribute attr ON attr.attrelid = tbl.oid AND attr.attnum = key.attnum
		ORDER BY key.ord
	  ) = $2::text[]
)
`, table, pq.Array(columns)).Scan(&exists)
	require.NoError(t, err, "query NULLS NOT DISTINCT unique constraint on %s", table)
	require.True(t, exists, "expected NULLS NOT DISTINCT unique constraint on %s(%v)", table, columns)
}

func requireNoForeignKey(t *testing.T, tx *sql.Tx, table, column string) {
	t.Helper()

	var exists bool
	err := tx.QueryRowContext(context.Background(), `
SELECT EXISTS (
	SELECT 1
	FROM pg_constraint c
	JOIN pg_class tbl ON tbl.oid = c.conrelid
	JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
	JOIN pg_attribute attr ON attr.attrelid = tbl.oid AND attr.attnum = ANY(c.conkey)
	WHERE ns.nspname = 'public'
	  AND tbl.relname = $1
	  AND attr.attname = $2
	  AND c.contype = 'f'
)
`, table, column).Scan(&exists)
	require.NoError(t, err, "query foreign key on %s.%s", table, column)
	require.False(t, exists, "append-only lineage %s.%s must remain scalar evidence", table, column)
}

func requireExactModelTables(t *testing.T, tx *sql.Tx) {
	t.Helper()

	rows, err := tx.QueryContext(context.Background(), `
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename LIKE 'model_%'
ORDER BY tablename
`)
	require.NoError(t, err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{
		"model_classification_batches",
		"model_inventory_items",
		"model_inventory_runs",
		"model_observation_events",
		"model_observations",
		"model_registry",
		"model_registry_aliases",
		"model_registry_events",
		"model_shadow_decisions",
	}, tables)
}

func requireNoActivationTables(t *testing.T, tx *sql.Tx) {
	t.Helper()

	var count int
	err := tx.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename LIKE '%activation%'
`).Scan(&count)
	require.NoError(t, err, "query activation tables")
	require.Zero(t, count, "migration 200 must not add activation infrastructure")
}

func requireCheckValues(t *testing.T, tx *sql.Tx, table, constraint string, expected []string) {
	t.Helper()

	var definition string
	err := tx.QueryRowContext(context.Background(), `
SELECT pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class tbl ON tbl.oid = c.conrelid
JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
WHERE ns.nspname = 'public'
  AND tbl.relname = $1
  AND c.conname = $2
  AND c.contype = 'c'
`, table, constraint).Scan(&definition)
	require.NoError(t, err, "query check constraint %s.%s", table, constraint)

	matches := regexp.MustCompile(`'([^']+)'`).FindAllStringSubmatch(definition, -1)
	actual := make([]string, 0, len(matches))
	for _, match := range matches {
		actual = append(actual, match[1])
	}
	sort.Strings(actual)
	require.Equal(t, expected, actual, "exact allowed values mismatch for %s.%s", table, constraint)
}

func requireAppendOnlyTable(t *testing.T, tx *sql.Tx, table string) {
	t.Helper()

	var triggerCount int
	err := tx.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM pg_trigger trigger
JOIN pg_class tbl ON tbl.oid = trigger.tgrelid
JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
WHERE ns.nspname = 'public'
  AND tbl.relname = $1
  AND NOT trigger.tgisinternal
  AND (trigger.tgtype & 2) = 2
  AND (trigger.tgtype & 16) = 16
  AND (trigger.tgtype & 8) = 8
`, table).Scan(&triggerCount)
	require.NoError(t, err, "query append-only trigger on %s", table)
	require.Equal(t, 1, triggerCount, "expected one UPDATE/DELETE trigger on %s", table)
}

func requireGovernanceTriggerMetadata(t *testing.T, tx *sql.Tx) {
	t.Helper()
	for _, trigger := range []struct {
		table string
		name  string
	}{
		{table: "account_incarnation_ids", name: "trg_account_incarnation_ids_reject_truncate"},
		{table: "model_registry_events", name: "trg_model_registry_events_reject_truncate"},
		{table: "model_classification_batches", name: "trg_model_classification_batches_reject_truncate"},
		{table: "model_observation_events", name: "trg_model_observation_events_reject_truncate"},
	} {
		requireTriggerDefinition(t, tx, trigger.table, trigger.name, "BEFORE TRUNCATE", "FOR EACH STATEMENT")
	}
	requireTriggerDefinition(t, tx, "accounts", "trg_account_incarnation_id_not_reused", "AFTER INSERT", "FOR EACH ROW")
	requireTriggerDefinition(t, tx, "accounts", "trg_account_incarnation_id_immutable", "BEFORE UPDATE OF id", "FOR EACH ROW")
}

func requireTriggerDefinition(t *testing.T, tx *sql.Tx, table, triggerName string, fragments ...string) {
	t.Helper()
	rows, err := tx.QueryContext(context.Background(), `
SELECT pg_get_triggerdef(trigger.oid)
FROM pg_trigger trigger
JOIN pg_class tbl ON tbl.oid = trigger.tgrelid
JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
WHERE ns.nspname = 'public'
  AND tbl.relname = $1
  AND trigger.tgname = $2
  AND NOT trigger.tgisinternal
`, table, triggerName)
	require.NoError(t, err)
	defer rows.Close()

	var definitions []string
	for rows.Next() {
		var definition string
		require.NoError(t, rows.Scan(&definition))
		definitions = append(definitions, definition)
	}
	require.NoError(t, rows.Err())
	require.Len(t, definitions, 1, "expected exactly one trigger %s on %s", triggerName, table)
	for _, fragment := range fragments {
		require.Contains(t, definitions[0], fragment)
	}
}

func requireAccountAndLedgerAbsent(t *testing.T, accountID int64) {
	t.Helper()
	var accountCount, ledgerCount int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM accounts WHERE id = $1`, accountID).Scan(&accountCount))
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id = $1`, accountID).Scan(&ledgerCount))
	require.Zero(t, accountCount)
	require.Zero(t, ledgerCount)
}

func requireAccountConflictProposalsDoNotClaimIDs(t *testing.T, label string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	parentID := requireUnusedAccountID(t)
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'oauth')
	`, parentID, fmt.Sprintf("governance-%s-parent-%d", label, suffix))
	require.NoError(t, err)
	existingShadowID := requireUnusedAccountID(t, parentID)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type, parent_account_id, quota_dimension)
		VALUES ($1, $2, 'openai', 'oauth', $3, 'spark')
	`, existingShadowID, fmt.Sprintf("governance-%s-existing-shadow-%d", label, suffix), parentID)
	require.NoError(t, err)

	proposedNothingID := requireUnusedAccountID(t, existingShadowID)
	result, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type, parent_account_id, quota_dimension)
		VALUES ($1, $2, 'openai', 'oauth', $3, 'spark')
		ON CONFLICT (parent_account_id)
		WHERE parent_account_id IS NOT NULL AND quota_dimension = 'spark' AND deleted_at IS NULL
		DO NOTHING
	`, proposedNothingID, fmt.Sprintf("governance-%s-shadow-nothing-%d", label, suffix), parentID)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, rows)
	requireAccountAndLedgerAbsent(t, proposedNothingID)

	proposedUpdateID := requireUnusedAccountID(t, existingShadowID, proposedNothingID)
	var updatedID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (id, name, platform, type, parent_account_id, quota_dimension)
		VALUES ($1, $2, 'openai', 'oauth', $3, 'spark')
		ON CONFLICT (parent_account_id)
		WHERE parent_account_id IS NOT NULL AND quota_dimension = 'spark' AND deleted_at IS NULL
		DO UPDATE SET name = EXCLUDED.name
		RETURNING id
	`, proposedUpdateID, fmt.Sprintf("governance-%s-shadow-update-%d", label, suffix), parentID).Scan(&updatedID))
	require.Equal(t, existingShadowID, updatedID)
	requireAccountAndLedgerAbsent(t, proposedUpdateID)
}

func requireHistoricalAccountIDReuseRejected(t *testing.T, label string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	explicitID := requireUnusedAccountID(t)
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
	`, explicitID, fmt.Sprintf("governance-%s-first-%d", label, suffix))
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, explicitID)
	require.NoError(t, err)

	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
	`, explicitID, fmt.Sprintf("governance-%s-reuse-%d", label, suffix))
	var pqErr *pq.Error
	require.ErrorAs(t, err, &pqErr)
	require.Equal(t, pq.ErrorCode("23505"), pqErr.Code)
	require.Equal(t, "account_id_incarnation_not_reused", pqErr.Constraint)

	var accountCount, ledgerCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts WHERE id = $1`, explicitID).Scan(&accountCount))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_incarnation_ids WHERE account_id = $1`, explicitID).Scan(&ledgerCount))
	require.Zero(t, accountCount)
	require.Equal(t, 1, ledgerCount)
}

func requireAccountIDUpdateRejected(t *testing.T, label string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	accountID := requireUnusedAccountID(t)
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO accounts (id, name, platform, type) VALUES ($1, $2, 'openai', 'apikey')
	`, accountID, fmt.Sprintf("governance-%s-%d", label, suffix))
	require.NoError(t, err)
	proposedID := requireUnusedAccountID(t, accountID)
	require.NotEqual(t, accountID, proposedID)

	_, err = integrationDB.ExecContext(ctx, `UPDATE accounts SET id = $1 WHERE id = $2`, proposedID, accountID)
	var pqErr *pq.Error
	require.ErrorAs(t, err, &pqErr)
	require.Equal(t, pq.ErrorCode("P0001"), pqErr.Code)
	require.ErrorContains(t, err, "account incarnation ID is immutable")
	requireAccountAndLedgerAbsent(t, proposedID)
}

func requireUnusedAccountID(t *testing.T, distinctFrom ...int64) int64 {
	t.Helper()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT GREATEST(
			COALESCE((SELECT MAX(id) FROM accounts), 0),
			COALESCE((SELECT MAX(account_id) FROM account_incarnation_ids), 0)
		) + 1
	`).Scan(&accountID))
	for {
		distinct := true
		for _, other := range distinctFrom {
			if accountID == other {
				accountID++
				distinct = false
				break
			}
		}
		if distinct {
			break
		}
	}
	requireAccountAndLedgerAbsent(t, accountID)
	return accountID
}

func requireIndex(t *testing.T, tx *sql.Tx, table, index string) {
	t.Helper()

	var exists bool
	err := tx.QueryRowContext(context.Background(), `
SELECT EXISTS (
	SELECT 1
	FROM pg_indexes
	WHERE schemaname = 'public'
	  AND tablename = $1
	  AND indexname = $2
)
`, table, index).Scan(&exists)
	require.NoError(t, err, "query pg_indexes for %s.%s", table, index)
	require.True(t, exists, "expected index %s on %s", index, table)
}

func requireIndexAbsent(t *testing.T, tx *sql.Tx, table, index string) {
	t.Helper()

	var exists bool
	err := tx.QueryRowContext(context.Background(), `
SELECT EXISTS (
	SELECT 1
	FROM pg_indexes
	WHERE schemaname = 'public'
	  AND tablename = $1
	  AND indexname = $2
)
`, table, index).Scan(&exists)
	require.NoError(t, err, "query pg_indexes for %s.%s", table, index)
	require.False(t, exists, "expected index %s on %s to be absent", index, table)
}

func requirePartialUniqueIndexDefinition(t *testing.T, tx *sql.Tx, table, index string, fragments ...string) {
	t.Helper()

	var (
		unique bool
		def    string
	)

	err := tx.QueryRowContext(context.Background(), `
SELECT
	i.indisunique,
	pg_get_indexdef(i.indexrelid)
FROM pg_class idx
JOIN pg_index i ON i.indexrelid = idx.oid
JOIN pg_class tbl ON tbl.oid = i.indrelid
JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
WHERE ns.nspname = 'public'
  AND tbl.relname = $1
  AND idx.relname = $2
`, table, index).Scan(&unique, &def)
	require.NoError(t, err, "query index definition for %s.%s", table, index)
	require.True(t, unique, "expected index %s on %s to be unique", index, table)

	for _, fragment := range fragments {
		require.Contains(t, def, fragment, "expected index definition for %s.%s to contain %q", table, index, fragment)
	}
}

func requireForeignKeyOnDelete(t *testing.T, tx *sql.Tx, table, column, refTable, expected string) {
	t.Helper()

	var actual string
	err := tx.QueryRowContext(context.Background(), `
SELECT CASE c.confdeltype
	WHEN 'a' THEN 'NO ACTION'
	WHEN 'r' THEN 'RESTRICT'
	WHEN 'c' THEN 'CASCADE'
	WHEN 'n' THEN 'SET NULL'
	WHEN 'd' THEN 'SET DEFAULT'
END
FROM pg_constraint c
JOIN pg_class tbl ON tbl.oid = c.conrelid
JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
JOIN pg_class ref_tbl ON ref_tbl.oid = c.confrelid
JOIN pg_attribute attr ON attr.attrelid = tbl.oid AND attr.attnum = ANY(c.conkey)
WHERE ns.nspname = 'public'
  AND c.contype = 'f'
  AND tbl.relname = $1
  AND attr.attname = $2
  AND ref_tbl.relname = $3
LIMIT 1
`, table, column, refTable).Scan(&actual)
	require.NoError(t, err, "query foreign key action for %s.%s -> %s", table, column, refTable)
	require.Equal(t, expected, actual, "unexpected ON DELETE action for %s.%s -> %s", table, column, refTable)
}

func requireConstraintDefinitionContains(t *testing.T, tx *sql.Tx, table, constraint string, fragments ...string) {
	t.Helper()

	var def string
	err := tx.QueryRowContext(context.Background(), `
SELECT pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class tbl ON tbl.oid = c.conrelid
JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
WHERE ns.nspname = 'public'
  AND tbl.relname = $1
  AND c.conname = $2
`, table, constraint).Scan(&def)
	require.NoError(t, err, "query constraint definition for %s.%s", table, constraint)

	for _, fragment := range fragments {
		require.Contains(t, def, fragment, "expected constraint definition for %s.%s to contain %q", table, constraint, fragment)
	}
}

func requireColumnDefaultContains(t *testing.T, tx *sql.Tx, table, column string, fragments ...string) {
	t.Helper()

	var columnDefault sql.NullString
	err := tx.QueryRowContext(context.Background(), `
SELECT column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = $1
  AND column_name = $2
`, table, column).Scan(&columnDefault)
	require.NoError(t, err, "query column_default for %s.%s", table, column)
	require.True(t, columnDefault.Valid, "expected column_default for %s.%s", table, column)

	for _, fragment := range fragments {
		require.Contains(t, columnDefault.String, fragment, "expected default for %s.%s to contain %q", table, column, fragment)
	}
}

func requireColumn(t *testing.T, tx *sql.Tx, table, column, dataType string, maxLen int, nullable bool) {
	t.Helper()

	var row struct {
		DataType string
		MaxLen   sql.NullInt64
		Nullable string
	}

	err := tx.QueryRowContext(context.Background(), `
SELECT
  data_type,
  character_maximum_length,
  is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = $1
  AND column_name = $2
`, table, column).Scan(&row.DataType, &row.MaxLen, &row.Nullable)
	require.NoError(t, err, "query information_schema.columns for %s.%s", table, column)
	require.Equal(t, dataType, row.DataType, "data_type mismatch for %s.%s", table, column)

	if maxLen > 0 {
		require.True(t, row.MaxLen.Valid, "expected maxLen for %s.%s", table, column)
		require.Equal(t, int64(maxLen), row.MaxLen.Int64, "maxLen mismatch for %s.%s", table, column)
	}

	if nullable {
		require.Equal(t, "YES", row.Nullable, "nullable mismatch for %s.%s", table, column)
	} else {
		require.Equal(t, "NO", row.Nullable, "nullable mismatch for %s.%s", table, column)
	}
}

func requireColumnAbsent(t *testing.T, tx *sql.Tx, table, column string) {
	t.Helper()
	var exists bool
	require.NoError(t, tx.QueryRowContext(context.Background(), `
SELECT EXISTS (
	SELECT 1 FROM information_schema.columns
	WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
)
`, table, column).Scan(&exists))
	require.False(t, exists, "expected %s.%s to be absent", table, column)
}
