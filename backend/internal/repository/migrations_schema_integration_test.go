//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
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
	requireColumn(t, tx, "model_observation_events", "account_id", "bigint", 0, false)
	requireColumn(t, tx, "model_observation_events", "upstream_model_id", "character varying", 255, false)
	requireConstraintDefinitionContains(t, tx, "model_registry", "chk_model_registry_provider", "anthropic", "openai", "gemini", "grok")
	requireConstraintDefinitionContains(t, tx, "model_registry", "chk_model_registry_modality", "text", "image", "audio", "video", "embedding", "other")
	requireConstraintDefinitionContains(t, tx, "model_registry", "chk_model_registry_lifecycle", "active", "deprecated", "retired")
	requireConstraintDefinitionContains(t, tx, "model_observations", "chk_model_observations_classification", "discovered", "approved", "cross_provider", "unknown", "ignored")
	requireConstraintDefinitionContains(t, tx, "model_observations", "chk_model_observations_presence", "present", "missing")
	requireUniqueConstraint(t, tx, "model_observations", "account_id", "upstream_model_id")
	requirePartialUniqueIndexDefinition(t, tx, "model_inventory_items", "uq_model_inventory_items_dimensions",
		"UNIQUE", "run_id", "account_id", "(group_id IS NULL)", "COALESCE(group_id, (0)::bigint)",
		"(channel_id IS NULL)", "COALESCE(channel_id, (0)::bigint)", "upstream_model_id")
	requireAppendOnlyTable(t, tx, "model_registry_events")
	requireAppendOnlyTable(t, tx, "model_observation_events")
	requireNoForeignKey(t, tx, "model_registry_events", "registry_id")
	requireNoForeignKey(t, tx, "model_observation_events", "observation_id")
	requireNoForeignKey(t, tx, "model_observation_events", "batch_id")
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
INSERT INTO model_observations (account_id, upstream_model_id, first_seen_at, last_seen_at)
VALUES ($1, $2, NOW(), NOW())
RETURNING id
`, accountID, fmt.Sprintf("model-%d", suffix)).Scan(&observationID))

	var eventID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_observation_events (
	observation_id, batch_id, account_id, upstream_model_id,
	event_type, classification, classification_reason, upstream_presence
)
VALUES ($1, $2, $3, 'immutable-model', 'discovered', 'discovered', 'awaiting_registry_classification', 'present')
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

func TestMigrationsRunner_ModelGovernanceFoundationEventsRejectActualMutations(t *testing.T) {
	tx := testTx(t)
	suffix := time.Now().UnixNano()

	var registryEventID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_registry_events (idempotency_key, event_type, registry_version, actor_id)
VALUES ($1, 'created', 1, 'integration-test')
RETURNING id
`, fmt.Sprintf("registry-event-%d", suffix)).Scan(&registryEventID))

	var observationEventID int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `
INSERT INTO model_observation_events (
	observation_id, account_id, upstream_model_id,
	event_type, classification, classification_reason, upstream_presence
)
VALUES ($1, $1, 'mutation-model', 'discovered', 'discovered', 'integration_test', 'present')
RETURNING id
`, suffix).Scan(&observationEventID))

	for _, mutation := range []struct {
		name string
		sql  string
		id   int64
	}{
		{name: "update registry event", sql: "UPDATE model_registry_events SET actor_id = 'changed' WHERE id = $1", id: registryEventID},
		{name: "delete registry event", sql: "DELETE FROM model_registry_events WHERE id = $1", id: registryEventID},
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
	run_id, account_id, group_id, channel_id, upstream_model_id, classification, billing_currency
)
VALUES ($1, $2, NULL, NULL, 'claude-test', 'approved', 'USD')
`
	_, err = tx.ExecContext(context.Background(), insert, runID, accountID)
	require.NoError(t, err)
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
	run_id, account_id, group_id, channel_id, upstream_model_id, classification, billing_currency
)
VALUES ($1, $2, 0, 0, 'claude-test', 'approved', 'USD')
`, runID, accountID)
	require.NoError(t, err, "real zero dimensions must not collide with NULL dimensions")
}

func TestMigrationsRunner_ModelGovernanceFoundationSQLRerunsDirectly(t *testing.T) {
	raw, err := os.ReadFile("../../migrations/200_model_governance_foundation.sql")
	require.NoError(t, err)

	_, err = integrationDB.ExecContext(context.Background(), string(raw))
	require.NoError(t, err, "first direct SQL rerun on an already migrated schema")
	_, err = integrationDB.ExecContext(context.Background(), string(raw))
	require.NoError(t, err, "second direct SQL rerun on the same schema")
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
