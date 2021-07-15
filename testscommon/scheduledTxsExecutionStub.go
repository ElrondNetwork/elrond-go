package testscommon

import (
	"time"

	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/process"
)

// ScheduledTxsExecutionStub -
type ScheduledTxsExecutionStub struct {
	InitCalled                      func()
	AddCalled                       func([]byte, data.TransactionHandler) bool
	ExecuteCalled                   func([]byte) error
	ExecuteAllCalled                func(func() time.Duration) error
	GetScheduledSCRsCalled          func() map[block.Type][]data.TransactionHandler
	SetScheduledSCRsCalled          func(map[block.Type][]data.TransactionHandler)
	GetScheduledRootHashCalled      func() []byte
	SetScheduledRootHashCalled      func([]byte)
	SetTransactionProcessorCalled   func(process.TransactionProcessor)
	SetTransactionCoordinatorCalled func(process.TransactionCoordinator)
}

// Init -
func (stes *ScheduledTxsExecutionStub) Init() {
	if stes.InitCalled != nil {
		stes.InitCalled()
	}
}

// Add -
func (stes *ScheduledTxsExecutionStub) Add(txHash []byte, tx data.TransactionHandler) bool {
	if stes.AddCalled != nil {
		return stes.AddCalled(txHash, tx)
	}
	return true
}

// Execute -
func (stes *ScheduledTxsExecutionStub) Execute(txHash []byte) error {
	if stes.ExecuteCalled != nil {
		return stes.ExecuteCalled(txHash)
	}
	return nil
}

// ExecuteAll -
func (stes *ScheduledTxsExecutionStub) ExecuteAll(haveTime func() time.Duration) error {
	if stes.ExecuteAllCalled != nil {
		return stes.ExecuteAllCalled(haveTime)
	}
	return nil
}

// GetScheduledSCRs -
func (stes *ScheduledTxsExecutionStub) GetScheduledSCRs() map[block.Type][]data.TransactionHandler {
	if stes.GetScheduledSCRsCalled != nil {
		return stes.GetScheduledSCRsCalled()
	}
	return nil
}

// SetScheduledSCRs -
func (stes *ScheduledTxsExecutionStub) SetScheduledSCRs(mapScheduledSCRs map[block.Type][]data.TransactionHandler) {
	if stes.SetScheduledSCRsCalled != nil {
		stes.SetScheduledSCRsCalled(mapScheduledSCRs)
	}
}

// GetScheduledRootHash -
func (stes *ScheduledTxsExecutionStub) GetScheduledRootHash() []byte {
	if stes.GetScheduledRootHashCalled != nil {
		return stes.GetScheduledRootHashCalled()
	}

	return make([]byte, 0)
}

// SetScheduledRootHash -
func (stes *ScheduledTxsExecutionStub) SetScheduledRootHash(rootHash []byte) {
	if stes.SetScheduledRootHashCalled != nil {
		stes.SetScheduledRootHashCalled(rootHash)
	}
}

// SetTransactionProcessor -
func (stes *ScheduledTxsExecutionStub) SetTransactionProcessor(txProcessor process.TransactionProcessor) {
	if stes.SetTransactionProcessorCalled != nil {
		stes.SetTransactionProcessorCalled(txProcessor)
	}
}

// SetTransactionCoordinator -
func (stes *ScheduledTxsExecutionStub) SetTransactionCoordinator(txCoordinator process.TransactionCoordinator) {
	if stes.SetTransactionCoordinatorCalled != nil {
		stes.SetTransactionCoordinatorCalled(txCoordinator)
	}
}

// IsInterfaceNil -
func (stes *ScheduledTxsExecutionStub) IsInterfaceNil() bool {
	return stes == nil
}
