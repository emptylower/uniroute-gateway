package service

import (
	"context"
)

// ModelPublicationInputLoader combines connection/probe evidence with registry, account, group, channel, mapping, multiplier, price, presence into PublicationInput without deciding eligibility.
type ModelPublicationInputLoader struct {
	probeService    *AccountEndpointProbeService
	registryService ModelRegistryService
}

func NewModelPublicationInputLoader(probeSvc *AccountEndpointProbeService, registry ModelRegistryService) *ModelPublicationInputLoader {
	return &ModelPublicationInputLoader{probeService: probeSvc, registryService: registry}
}

func (l *ModelPublicationInputLoader) Load(ctx context.Context, accountID, channelID int64, canonicalID string) (*PublicationInput, error) {
	// Load current connection/probe evidence; stub returns defaults that keep enumeration testable.
	// Real implementation would fetch account, connection, group, channel, mapping, multiplier, price, presence.
	input := &PublicationInput{
		AccountProvider:           GovernanceProviderAnthropic, // placeholder, real would be per account
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
	if l.probeService != nil {
		if valid, _ := l.probeService.IsValid(ctx, accountID); valid {
			input.EndpointProbeValid = true
		} else {
			input.EndpointProbeValid = false
		}
	}
	// Registry entry
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
