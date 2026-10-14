package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// The generic Veo name changed its default engine. Compare only frozen tasks
// whose public engine/resolution/mode match the current exact reference SKU.
// Kie's Veo API includes background audio by default; an explicit audio override
// is not the verified default tariff and must remain incomparable.
// SQL returns a boolean, never request JSON or user input.
const usageMediaReferenceVerified = `CASE WHEN t.model='veo-3-1' THEN COALESCE(t.request_payload->>'model'='veo3_fast' AND t.request_payload->>'resolution'='720p' AND t.request_payload->>'generation_type'='TEXT_2_VIDEO' AND NOT t.request_payload ?| ARRAY['generate_audio','generateAudio','audio','audio_enabled','sound'],false) ELSE true END`

// Project only the exact task identity and chosen option. Prompts, provider IDs,
// supplier prices and raw billing/request JSON never enter a user's usage DTO.
func (r *usageLogRepository) hydrateUsageMediaPriceContext(ctx context.Context, logs []service.UsageLog) error {
	const batchSize = 100
	for start := 0; start < len(logs); start += batchSize {
		end := min(start+batchSize, len(logs))
		conditions := []string{}
		args := []any{}
		for _, log := range logs[start:end] {
			if log.MediaType == nil || log.RequestID == "" || log.UserID <= 0 || log.APIKeyID <= 0 {
				continue
			}
			index := len(args) + 1
			conditions = append(conditions, fmt.Sprintf("(ul.id=$%d AND t.user_id=$%d AND t.api_key_id=$%d AND t.id=$%d)", index, index+1, index+2, index+3))
			args = append(args, log.ID, log.UserID, log.APIKeyID, log.RequestID)
		}
		if len(conditions) == 0 {
			continue
		}
		query := `SELECT ul.id,t.model,t.option,` + usageMediaReferenceVerified + ` FROM usage_logs ul JOIN gateway_media_task t ON t.id=ul.request_id AND t.user_id=ul.user_id AND t.api_key_id=ul.api_key_id AND t.model=ul.model AND t.media_type=ul.media_type WHERE ` + strings.Join(conditions, " OR ")
		rows, err := r.sql.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		contexts := make(map[int64]*service.UsageMediaPriceContext)
		for rows.Next() {
			var id int64
			var value service.UsageMediaPriceContext
			if err = rows.Scan(&id, &value.ModelID, &value.Option, &value.ReferenceSKUVerified); err != nil {
				_ = rows.Close()
				return err
			}
			contexts[id] = &value
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		for i := start; i < end; i++ {
			logs[i].MediaPriceContext = contexts[logs[i].ID]
		}
	}
	return nil
}
