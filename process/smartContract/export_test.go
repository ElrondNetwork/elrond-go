package smartContract

import (
	"math/big"

	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/ElrondNetwork/elrond-vm-common"
)

func (sc *scProcessor) CreateVMCallInput(tx *transaction.Transaction) (*vmcommon.ContractCallInput, error) {
	return sc.createVMCallInput(tx)
}

func (sc *scProcessor) CreateVMDeployInput(tx *transaction.Transaction) (*vmcommon.ContractCreateInput, error) {
	return sc.createVMDeployInput(tx)
}

func (sc *scProcessor) CreateVMInput(tx *transaction.Transaction) (*vmcommon.VMInput, error) {
	return sc.createVMInput(tx)
}

func (sc *scProcessor) ProcessVMOutput(vmOutput *vmcommon.VMOutput, tx *transaction.Transaction, acntSnd, acntDst state.AccountHandler, round uint32) error {
	return sc.processVMOutput(vmOutput, tx, acntSnd, acntDst, round)
}

func (sc *scProcessor) RefundGasToSender(gasRefund *big.Int, tx *transaction.Transaction, acntSnd state.AccountHandler) error {
	return sc.refundGasToSender(gasRefund, tx, acntSnd)
}

func (sc *scProcessor) ProcessSCOutputAccounts(outputAccounts []*vmcommon.OutputAccount) error {
	return sc.processSCOutputAccounts(outputAccounts)
}

func (sc *scProcessor) DeleteAccounts(deletedAccounts [][]byte) error {
	return sc.deleteAccounts(deletedAccounts)
}

func (sc *scProcessor) GetAccountFromAddress(address []byte) (state.AccountHandler, error) {
	return sc.getAccountFromAddress(address)
}

func (sc *scProcessor) SaveSCOutputToCurrentState(output *vmcommon.VMOutput, round uint32, txHash []byte) error {
	return sc.saveSCOutputToCurrentState(output, round, txHash)
}

func (sc *scProcessor) SaveReturnData(returnData []*big.Int, round uint32, txHash []byte) error {
	return sc.saveReturnData(returnData, round, txHash)
}

func (sc *scProcessor) SaveReturnCode(returnCode vmcommon.ReturnCode, round uint32, txHash []byte) error {
	return sc.saveReturnCode(returnCode, round, txHash)
}

func (sc *scProcessor) SaveLogsIntoState(logs []*vmcommon.LogEntry, round uint32, txHash []byte) error {
	return sc.saveLogsIntoState(logs, round, txHash)
}
