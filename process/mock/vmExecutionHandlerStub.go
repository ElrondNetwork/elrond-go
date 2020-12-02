package mock

import (
	"math/big"

	"github.com/ElrondNetwork/elrond-go/core/vmcommon"
)

// VMExecutionHandlerStub -
type VMExecutionHandlerStub struct {
	RunSmartContractCreateCalled func(input *vmcommon.ContractCreateInput) (*vmcommon.VMOutput, error)
	RunSmartContractCallCalled   func(input *vmcommon.ContractCallInput) (*vmcommon.VMOutput, error)
	GasScheduleChangeCalled      func(gasSchedule map[string]map[string]uint64)
}

// RunSmartContractCreate computes how a smart contract creation should be performed
func (vm *VMExecutionHandlerStub) RunSmartContractCreate(input *vmcommon.ContractCreateInput) (*vmcommon.VMOutput, error) {
	if vm.RunSmartContractCreateCalled == nil {
		return &vmcommon.VMOutput{
			GasRefund:    big.NewInt(0),
			GasRemaining: 0,
		}, nil
	}

	return vm.RunSmartContractCreateCalled(input)
}

// RunSmartContractCall Computes the result of a smart contract call and how the system must change after the execution
func (vm *VMExecutionHandlerStub) RunSmartContractCall(input *vmcommon.ContractCallInput) (*vmcommon.VMOutput, error) {
	if vm.RunSmartContractCallCalled == nil {
		return &vmcommon.VMOutput{
			GasRefund:    big.NewInt(0),
			GasRemaining: 0,
		}, nil
	}

	return vm.RunSmartContractCallCalled(input)
}

// GasScheduleChange -
func (vm *VMExecutionHandlerStub) GasScheduleChange(gasSchedule map[string]map[string]uint64) {
	if vm.GasScheduleChangeCalled != nil {
		vm.GasScheduleChangeCalled(gasSchedule)
	}
}

// IsInterfaceNil returns true if there is no value under the interface
func (vm *VMExecutionHandlerStub) IsInterfaceNil() bool {
	return vm == nil
}
