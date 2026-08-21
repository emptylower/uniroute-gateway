package service

import (
	"context"
)

// ModelPublicationInputLoader combines connection/probe evidence with registry, account, group, channel, mapping, multiplier, price, presence into PublicationInput without deciding eligibility.
type ModelPublicationInputLoader struct {
	probeService     *AccountEndpointProbeService
	registryService  ModelRegistryService
	accountRepo      AccountRepository
	connRepo         UpstreamConnectionRepository
	priceChecker     ChannelPriceChecker
	billingChecker   BillingMappingChecker
	resourceChecker  ResourceChecker
}

func NewModelPublicationInputLoader(probeSvc *AccountEndpointProbeService, registry ModelRegistryService) *ModelPublicationInputLoader {
	return &ModelPublicationInputLoader{probeService: probeSvc, registryService: registry}
}

func NewModelPublicationInputLoaderWithDeps(probeSvc *AccountEndpointProbeService, registry ModelRegistryService, accountRepo AccountRepository, connRepo UpstreamConnectionRepository, priceChecker ChannelPriceChecker, billingChecker BillingMappingChecker, resourceChecker ResourceChecker) *ModelPublicationInputLoader {
	return &ModelPublicationInputLoader{
		probeService:    probeSvc,
		registryService: registry,
		accountRepo:     accountRepo,
		connRepo:        connRepo,
		priceChecker:    priceChecker,
		billingChecker:  billingChecker,
		resourceChecker: resourceChecker,
	}
}

func (l *ModelPublicationInputLoader) Load(ctx context.Context, accountID, channelID int64, canonicalID string) (*PublicationInput, error) {
	input := &PublicationInput{
		AccountProvider:           GovernanceProviderAnthropic,
		ModelAllowed:              true,
		MultiplierPresent:         true,
		ExactChannelPricePresent:  true,
		BillingMappingUnambiguous: true,
		EndpointProbeValid:        true,
		ConnectionEnabled:         true,
		AccountEnabled:            true,
		GroupEnabled:              true,
		ChannelEnabled:            true,
		UpstreamPresent:           true,
	}
	// Load account for provider, allowlist, multiplier, enabled, connection
	var account *Account
	if l.accountRepo != nil {
		if acc, err := l.accountRepo.GetByID(ctx, accountID); err == nil && acc != nil {
			account = acc
			// Provider from platform
			switch acc.Platform {
			case "claude", "anthropic":
				input.AccountProvider = GovernanceProviderAnthropic
			case "openai":
				input.AccountProvider = GovernanceProviderOpenAI
			case "gemini":
				input.AccountProvider = GovernanceProviderGemini
			case "grok", "xai":
				input.AccountProvider = GovernanceProviderGrok
			default:
				input.AccountProvider = GovernanceProvider(acc.Platform)
			}
			if len(canonicalID) > 0 {
				input.ModelAllowed = acc.IsModelSupported(canonicalID)
				if len(acc.GetModelMapping()) == 0 {
					input.ModelAllowed = true
				}
			}
			input.MultiplierPresent = acc.RateMultiplier != nil
			input.AccountEnabled = acc.Status == "active" && acc.Schedulable
			input.UpstreamPresent = true // would check connection presence
			if acc.ConnectionID != nil && l.connRepo != nil {
				if conn, _, err := l.connRepo.GetByID(ctx, *acc.ConnectionID); err == nil && conn != nil {
					input.ConnectionEnabled = conn.Status == "active"
				}
			}
		}
	}
	if l.resourceChecker != nil && account != nil {
		if c, a, g, ch, err := l.resourceChecker.AreEnabled(ctx, accountID); err == nil {
			input.ConnectionEnabled = c
			input.AccountEnabled = a
			input.GroupEnabled = g
			input.ChannelEnabled = ch
		}
	}
	if l.priceChecker != nil && canonicalID != "" && account != nil {
		if present, err := l.priceChecker.HasExactPrice(ctx, canonicalID, input.AccountProvider); err == nil {
			input.ExactChannelPricePresent = present
		}
	}
	if l.billingChecker != nil && canonicalID != "" {
		if ok, err := l.billingChecker.IsUnambiguous(ctx, canonicalID); err == nil {
			input.BillingMappingUnambiguous = ok
		}
	}
	if l.probeService != nil {
		if valid, _ := l.probeService.IsValid(ctx, accountID); valid {
			input.EndpointProbeValid = true
		} else {
			input.EndpointProbeValid = false
		}
	}
	// Registry entry lookup (kept)
	if l.registryService != nil {
		snap, err := l.registryService.GetSnapshot(ctx)
		if err == nil && snap != nil {
			if entry, ok := snap.Entries[canonicalID]; ok {
				copy := entry
				input.RegistryEntry = &copy
			}
		}
	}
	return input, nil
}
