package mock

import (
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
)

// SyncStateStub -
type SyncStateStub struct {
	GetEpochStartMetaBlockCalled  func() (*block.MetaBlock, error)
	GetUnFinishedMetaBlocksCalled func() (map[string]*block.MetaBlock, error)
	SyncAllStateCalled            func(epoch uint32, ownShardId uint32) error
	GetAllTransactionsCalled      func() (map[string]data.TransactionHandler, error)
	GetAllMiniBlocksCalled        func() (map[string]*block.MiniBlock, error)
}

// GetEpochStartMetaBlock -
func (sss *SyncStateStub) GetEpochStartMetaBlock() (*block.MetaBlock, error) {
	if sss.GetEpochStartMetaBlockCalled != nil {
		return sss.GetEpochStartMetaBlockCalled()
	}
	return nil, nil
}

// GetUnFinishedMetaBlocks -
func (sss *SyncStateStub) GetUnFinishedMetaBlocks() (map[string]*block.MetaBlock, error) {
	if sss.GetUnFinishedMetaBlocksCalled != nil {
		return sss.GetUnFinishedMetaBlocksCalled()
	}
	return nil, nil
}

// SyncAllState -
func (sss *SyncStateStub) SyncAllState(epoch uint32, ownShardId uint32) error {
	if sss.SyncAllStateCalled != nil {
		return sss.SyncAllStateCalled(epoch, ownShardId)
	}
	return nil
}

// GetAllTransactions -
func (sss *SyncStateStub) GetAllTransactions() (map[string]data.TransactionHandler, error) {
	if sss.GetAllTransactionsCalled != nil {
		return sss.GetAllTransactionsCalled()
	}
	return nil, nil
}

// GetAllMiniBlocks -
func (sss *SyncStateStub) GetAllMiniBlocks() (map[string]*block.MiniBlock, error) {
	if sss.GetAllMiniBlocksCalled != nil {
		return sss.GetAllMiniBlocksCalled()
	}
	return nil, nil
}

// IsInterfaceNil -
func (sss *SyncStateStub) IsInterfaceNil() bool {
	return sss == nil
}
