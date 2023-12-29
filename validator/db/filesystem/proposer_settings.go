package filesystem

import (
	"context"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"

	fieldparams "github.com/prysmaticlabs/prysm/v4/config/fieldparams"
	validatorServiceConfig "github.com/prysmaticlabs/prysm/v4/config/validator/service"
	validatorpb "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1/validator-client"
)

// ErrNoProposerSettingsFound is an error thrown when no settings are found.
var ErrNoProposerSettingsFound = errors.New("no proposer settings found in bucket")

// ProposerSettings returns the proposer settings.
func (s *Store) ProposerSettings(_ context.Context) (*validatorServiceConfig.ProposerSettings, error) {
	// Get configuration
	configuration, err := s.configuration()
	if err != nil {
		return nil, errors.Wrap(err, "could not get configuration")
	}

	// Return on error if config file does not exist.
	if configuration == nil || configuration.ProposerSettings == nil {
		return nil, ErrNoProposerSettingsFound
	}

	// Convert proposer settings to validator service config.
	proposerSettings, err := validatorServiceConfig.ToSettings(configuration.ProposerSettings)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert proposer settings")
	}

	return proposerSettings, nil
}

// ProposerSettingsExists returns true if proposer settings exists, false otherwise.
func (s *Store) ProposerSettingsExists(_ context.Context) (bool, error) {
	// Get configuration.
	configuration, err := s.configuration()
	if err != nil {
		return false, errors.Wrap(err, "could not get configuration")
	}

	// If configuration is nil, return false.
	if configuration == nil {
		return false, nil
	}

	// Return true if proposer settings exists, false otherwise.
	exists := configuration.ProposerSettings != nil
	return exists, nil
}

// UpdateProposerSettingsDefault updates the default proposer settings.
func (s *Store) UpdateProposerSettingsDefault(_ context.Context, option *validatorServiceConfig.ProposerOption) error {
	// If option is nil, return nil.
	if option == nil {
		return nil
	}

	// Populate proposer option payload.
	proposerOptionPayload := &validatorpb.ProposerOptionPayload{}

	if option.FeeRecipientConfig != nil {
		feeRecipientHex := hexutil.Encode(option.FeeRecipientConfig.FeeRecipient[:])
		proposerOptionPayload.FeeRecipient = feeRecipientHex
	}

	if option.BuilderConfig != nil {
		builderConfigPayload := option.BuilderConfig.ToPayload()
		proposerOptionPayload.Builder = builderConfigPayload
	}

	// Get configuration.
	configuration, err := s.configuration()
	if err != nil {
		return errors.Wrap(err, "could not get configuration")
	}

	if configuration == nil {
		// If configuration is nil, create a new one.
		configuration = &Configuration{
			ProposerSettings: &validatorpb.ProposerSettingsPayload{
				DefaultConfig: proposerOptionPayload,
			},
		}

		// Save the configuration.
		if err := s.saveConfiguration(configuration); err != nil {
			return errors.Wrap(err, "could not save configuration")
		}

		return nil
	}

	if configuration.ProposerSettings == nil {
		// If proposer settings is nil, create a new one.
		configuration.ProposerSettings = &validatorpb.ProposerSettingsPayload{
			DefaultConfig: proposerOptionPayload,
		}

		// Save the configuration.
		if err := s.saveConfiguration(configuration); err != nil {
			return errors.Wrap(err, "could not save configuration")
		}

		return nil
	}

	// Modify the value of proposer settings.
	configuration.ProposerSettings.DefaultConfig = proposerOptionPayload

	// Save the configuration.
	if err := s.saveConfiguration(configuration); err != nil {
		return errors.Wrap(err, "could not save configuration")
	}

	return nil
}

// UpdateProposerSettingsForPubkey updates the proposer settings for a given pubkey.
func (s *Store) UpdateProposerSettingsForPubkey(_ context.Context, pubkey [fieldparams.BLSPubkeyLength]byte, proposerOption *validatorServiceConfig.ProposerOption) error {
	// If proposer option is nil, return nil.
	if proposerOption == nil {
		return nil
	}

	// Get configuration.
	configuration, err := s.configuration()
	if err != nil {
		return errors.Wrap(err, "could not get configuration")
	}

	// Convert pubkey to string.
	pubkeyHex := hexutil.Encode(pubkey[:])

	// Populate proposer option payload.
	proposerOptionPayload := &validatorpb.ProposerOptionPayload{}

	if proposerOption.FeeRecipientConfig != nil {
		feeRecipientHex := hexutil.Encode(proposerOption.FeeRecipientConfig.FeeRecipient[:])
		proposerOptionPayload.FeeRecipient = feeRecipientHex
	}

	if proposerOption.BuilderConfig != nil {
		builderConfigPayload := proposerOption.BuilderConfig.ToPayload()
		proposerOptionPayload.Builder = builderConfigPayload
	}

	if configuration == nil {
		// If configuration is nil, create a new one.
		configuration = &Configuration{
			ProposerSettings: &validatorpb.ProposerSettingsPayload{
				ProposerConfig: map[string]*validatorpb.ProposerOptionPayload{
					pubkeyHex: proposerOptionPayload,
				},
			},
		}

		// Save the configuration
		if err := s.saveConfiguration(configuration); err != nil {
			return errors.Wrap(err, "could not save configuration")
		}

		return nil
	}

	if configuration.ProposerSettings == nil {
		// If proposer settings is nil, create a new one.
		configuration.ProposerSettings = &validatorpb.ProposerSettingsPayload{
			ProposerConfig: map[string]*validatorpb.ProposerOptionPayload{
				pubkeyHex: proposerOptionPayload,
			},
		}

		// Save the configuration.
		if err := s.saveConfiguration(configuration); err != nil {
			return errors.Wrap(err, "could not save configuration")
		}

		return nil
	}

	// Modify the value of proposer settings.
	configuration.ProposerSettings.ProposerConfig[pubkeyHex] = proposerOptionPayload

	// Save the configuration.
	if err := s.saveConfiguration(configuration); err != nil {
		return errors.Wrap(err, "could not save configuration")
	}

	return nil
}

// SaveProposerSettings saves the proposer settings.
func (s *Store) SaveProposerSettings(_ context.Context, proposerSettings *validatorServiceConfig.ProposerSettings) error {
	// If proposer settings is nil, return nil.
	if proposerSettings == nil {
		return nil
	}

	// Convert proposer settings to payload.
	proposerSettingsPayload := proposerSettings.ToPayload()

	// Get configuration.
	configuration, err := s.configuration()
	if err != nil {
		return errors.Wrap(err, "could not get configuration")
	}

	if configuration == nil {
		// If configuration is nil, create new config.
		configuration = &Configuration{
			ProposerSettings: proposerSettingsPayload,
		}

		// Save the configuration.
		if err := s.saveConfiguration(configuration); err != nil {
			return errors.Wrap(err, "could not save configuration")
		}

		return nil
	}

	// Modify the value of proposer settings.
	configuration.ProposerSettings = proposerSettingsPayload

	// Save the configuration.
	if err := s.saveConfiguration(configuration); err != nil {
		return errors.Wrap(err, "could not save configuration")
	}

	return nil
}
