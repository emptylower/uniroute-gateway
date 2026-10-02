package handler

import (
	"context"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Native protocols and WebSocket sessions bind before mapping or freezing a
// billing snapshot. Existing handlers then schedule and settle the same group.
func bindFirstModelChannel(c *gin.Context, selector *service.ChannelRoutingSelector, keys *service.APIKeyService, key *service.APIKey, model, family string, diagnosers ...service.ModelAvailabilityDiagnoser) (*service.APIKey, error) {
	if key == nil || !service.IsChannelRoutingMode(key.RoutingMode) {
		return key, nil
	}
	if selector == nil {
		return nil, service.ErrNoChannelRoutingCandidate
	}
	if !selector.UsesChannelRouting(key) {
		return key, nil
	}
	candidates, err := selector.AllowedCandidates(c.Request.Context(), key, model, family, time.Now())
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, service.ErrNoChannelRoutingCandidate
	}
	anchor, _ := middleware.GetSubscriptionFromContext(c)
	for _, candidate := range candidates {
		bound := candidate.Apply(key)
		platform := bound.Group.Platform
		if platform == service.PlatformComposite {
			if detected, ok := service.DetectModelPlatform(model); ok {
				platform = detected
			} else if resolved, ok := service.ResolvedTargetPlatformFromContext(c.Request.Context()); ok {
				platform = resolved
			}
		}
		if len(diagnosers) > 0 && diagnosers[0] != nil {
			availability := diagnosers[0].DiagnoseModelAvailabilityForPlatform(c.Request.Context(), bound.GroupID, model, platform)
			if !availability.HasModelSupport {
				continue
			}
		}
		subscription, err := routedCandidateSubscription(c.Request.Context(), keys, bound, anchor)
		if err != nil {
			if isRecoverableChannelBillingError(err) || errors.Is(err, service.ErrSubscriptionNotFound) {
				continue
			}
			return nil, err
		}
		applyRoutedCandidateContext(c, bound, model)
		if bound.Group.Platform == service.PlatformComposite && platform != service.PlatformComposite {
			c.Request = c.Request.WithContext(service.WithResolvedTargetPlatform(c.Request.Context(), platform))
		}
		c.Set(string(middleware.ContextKeySubscription), subscription)
		return bound, nil
	}
	return nil, service.ErrNoChannelRoutingCandidate
}

// An established upstream session cannot migrate groups. Check every turn
// before price freezing; changed preferences require a reconnect, not a charge
// on a group the user has now excluded. The same check guards native fallbacks.
func modelChannelGroupAllowed(ctx context.Context, selector *service.ChannelRoutingSelector, key *service.APIKey, model, family string, groupID int64) error {
	if key == nil || !service.IsChannelRoutingMode(key.RoutingMode) {
		return nil
	}
	if selector == nil {
		return service.ErrNoChannelRoutingCandidate
	}
	if !selector.UsesChannelRouting(key) {
		return nil
	}
	candidates, err := selector.AllowedCandidates(ctx, key, model, family, time.Now())
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.Group.ID == groupID {
			return nil
		}
	}
	return service.ErrNoChannelRoutingCandidate
}
