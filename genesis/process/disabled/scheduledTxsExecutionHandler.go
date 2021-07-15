package disabled

import (
	"time"

	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/process"
)

// ScheduledTxsExecutionHandler implements ScheduledTxsExecutionHandler interface but does nothing as it is a disabled component
type ScheduledTxsExecutionHandler struct {
}

// Init does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) Init() {
}

// Add does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) Add(_ []byte, _ data.TransactionHandler) bool {
	return true
}

// Execute does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) Execute(_ []byte) error {
	return nil
}

// ExecuteAll does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) ExecuteAll(_ func() time.Duration) error {
	return nil
}

// GetScheduledSCRs does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) GetScheduledSCRs() map[block.Type][]data.TransactionHandler {
	return make(map[block.Type][]data.TransactionHandler)
}

// SetScheduledSCRs does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) SetScheduledSCRs(_ map[block.Type][]data.TransactionHandler) {
}

// GetScheduledRootHash does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) GetScheduledRootHash() []byte {
	return make([]byte, 0)
}

// SetScheduledRootHash does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) SetScheduledRootHash(_ []byte) {
}

// SetTransactionProcessor does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) SetTransactionProcessor(_ process.TransactionProcessor) {
}

// SetTransactionCoordinator does nothing as it is a disabled component
func (steh *ScheduledTxsExecutionHandler) SetTransactionCoordinator(_ process.TransactionCoordinator) {
}

// IsInterfaceNil returns true if underlying object is nil
func (steh *ScheduledTxsExecutionHandler) IsInterfaceNil() bool {
	return steh == nil
}
