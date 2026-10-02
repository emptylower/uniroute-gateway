package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type channelPreferenceRepository struct {
	db *sql.DB
}

func NewChannelPreferenceRepository(db *sql.DB) service.ChannelPreferenceRepository {
	return &channelPreferenceRepository{db: db}
}

func scanInt64Rows(rows *sql.Rows) (ids []int64, err error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *channelPreferenceRepository) GetAPIKeyChannelIDs(ctx context.Context, apiKeyID, userID int64) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT akc.channel_id
		FROM api_key_channels akc
		JOIN api_keys ak ON ak.id = akc.api_key_id
		WHERE akc.api_key_id = $1 AND ak.user_id = $2 AND ak.deleted_at IS NULL
		ORDER BY akc.channel_id`, apiKeyID, userID)
	if err != nil {
		return nil, err
	}
	return scanInt64Rows(rows)
}

func (r *channelPreferenceRepository) ReplaceAPIKeyChannels(ctx context.Context, apiKeyID, userID, anchorGroupID int64, channelIDs []int64) (string, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()

	var key string
	if err := tx.QueryRowContext(ctx, `
		SELECT key FROM api_keys
		WHERE id = $1 AND user_id = $2 AND deleted_at IS NULL
		FOR UPDATE`, apiKeyID, userID).Scan(&key); err != nil {
		if err == sql.ErrNoRows {
			return "", service.ErrAPIKeyNotFound
		}
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM api_key_channels WHERE api_key_id = $1`, apiKeyID); err != nil {
		return "", err
	}
	for _, channelID := range channelIDs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO api_key_channels (api_key_id, channel_id)
			VALUES ($1, $2)`, apiKeyID, channelID); err != nil {
			return "", fmt.Errorf("insert api key channel: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE api_keys
		SET routing_mode = 'channels', group_id = $1, updated_at = NOW()
		WHERE id = $2`, anchorGroupID, apiKeyID); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return key, nil
}

func (r *channelPreferenceRepository) GetUserDefaultChannelIDs(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT channel_id FROM user_default_channels
		WHERE user_id = $1 ORDER BY channel_id`, userID)
	if err != nil {
		return nil, err
	}
	return scanInt64Rows(rows)
}

func (r *channelPreferenceRepository) ReplaceUserDefaultChannels(ctx context.Context, userID int64, channelIDs []int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM user_default_channels WHERE user_id = $1`, userID); err != nil {
		return err
	}
	for _, channelID := range channelIDs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO user_default_channels (user_id, channel_id)
			VALUES ($1, $2)`, userID, channelID); err != nil {
			return fmt.Errorf("insert user default channel: %w", err)
		}
	}
	return tx.Commit()
}

func (r *channelPreferenceRepository) GetUserDisabledGroupIDs(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT group_id FROM user_disabled_routing_groups
		WHERE user_id = $1 ORDER BY group_id`, userID)
	if err != nil {
		return nil, err
	}
	return scanInt64Rows(rows)
}

func (r *channelPreferenceRepository) ReplaceUserDisabledGroups(ctx context.Context, userID int64, groupIDs []int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM user_disabled_routing_groups WHERE user_id = $1`, userID); err != nil {
		return err
	}
	for _, groupID := range groupIDs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO user_disabled_routing_groups (user_id, group_id)
			VALUES ($1, $2)`, userID, groupID); err != nil {
			return fmt.Errorf("insert user disabled routing group: %w", err)
		}
	}
	return tx.Commit()
}

func (r *channelPreferenceRepository) GetUserModelChannelPreferences(ctx context.Context, userID int64) (map[string]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT model_id, channel FROM user_model_channel_preferences WHERE user_id = $1 ORDER BY model_id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	preferences := make(map[string]string)
	for rows.Next() {
		var model, channel string
		if err := rows.Scan(&model, &channel); err != nil {
			return nil, err
		}
		preferences[model] = channel
	}
	return preferences, rows.Err()
}

func (r *channelPreferenceRepository) GetUserModelChannelPreference(ctx context.Context, userID int64, modelID string) (string, error) {
	var channel string
	err := r.db.QueryRowContext(ctx, `SELECT channel FROM user_model_channel_preferences WHERE user_id = $1 AND model_id = $2`, userID, modelID).Scan(&channel)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return channel, err
}

func (r *channelPreferenceRepository) UpsertUserModelChannelPreference(ctx context.Context, userID int64, modelID, channel string) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO user_model_channel_preferences (user_id, model_id, channel)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_id, model_id) DO UPDATE
		SET channel = EXCLUDED.channel, updated_at = NOW()`, userID, modelID, channel)
	return err
}
