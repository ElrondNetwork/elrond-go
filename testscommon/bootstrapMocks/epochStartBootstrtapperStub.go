package bootstrapMocks

import (
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/epochStart/bootstrap"
)

// EpochStartBootstrapperStub -
type EpochStartBootstrapperStub struct {
	TrieHolder      state.TriesHolder
	StorageManagers map[string]data.StorageManager
	BootstrapCalled func() (bootstrap.Parameters, error)
}

// GetTriesComponents -
func (esbs *EpochStartBootstrapperStub) GetTriesComponents() (state.TriesHolder, map[string]data.StorageManager) {
	return esbs.TrieHolder, esbs.StorageManagers
}

// Bootstrap -
func (esbs *EpochStartBootstrapperStub) Bootstrap() (bootstrap.Parameters, error) {
	if esbs.BootstrapCalled != nil {
		return esbs.BootstrapCalled()
	}

	return bootstrap.Parameters{}, nil
}

// EpochStartBootstrapperStub -
func (esbs *EpochStartBootstrapperStub) IsInterfaceNil() bool {
	return esbs == nil
}

// Close -
func (esbs *EpochStartBootstrapperStub) Close() error {
	return nil
}
