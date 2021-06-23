package mock

import "github.com/ElrondNetwork/elrond-go/update"

// AccountsDBSyncerStub -
type AccountsDBSyncerStub struct {
	SyncAccountsCalled    func(rootHash []byte, shardId uint32) error
	GetTrieExporterCalled func() update.TrieExporter
}

// SyncAccounts -
func (a *AccountsDBSyncerStub) SyncAccounts(rootHash []byte, shardId uint32) error {
	if a.SyncAccountsCalled != nil {
		return a.SyncAccountsCalled(rootHash, shardId)
	}
	return nil
}

// GetTrieExporter -
func (a *AccountsDBSyncerStub) GetTrieExporter() update.TrieExporter {
	if a.GetTrieExporterCalled != nil {
		return a.GetTrieExporterCalled()
	}
	return nil
}

// IsInterfaceNil -
func (a *AccountsDBSyncerStub) IsInterfaceNil() bool {
	return a == nil
}
