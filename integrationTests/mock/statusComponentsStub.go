package mock

import (
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/outport"
)

// StatusComponentsStub -
type StatusComponentsStub struct {
	TPSBench             statistics.TPSBenchmark
	Outport              outport.OutportHandler
	SoftwareVersionCheck statistics.SoftwareVersionChecker
	AppStatusHandler     core.AppStatusHandler
}

// Create -
func (scs *StatusComponentsStub) Create() error {
	return nil
}

// Close -
func (scs *StatusComponentsStub) Close() error {
	return nil
}

// CheckSubcomponents -
func (scs *StatusComponentsStub) CheckSubcomponents() error {
	return nil
}

// TpsBenchmark -
func (scs *StatusComponentsStub) TpsBenchmark() statistics.TPSBenchmark {
	return scs.TPSBench
}

// OutportHandler -
func (scs *StatusComponentsStub) OutportHandler() outport.OutportHandler {
	return scs.Outport
}

// SoftwareVersionChecker -
func (scs *StatusComponentsStub) SoftwareVersionChecker() statistics.SoftwareVersionChecker {
	return scs.SoftwareVersionCheck
}

// IsInterfaceNil -
func (scs *StatusComponentsStub) IsInterfaceNil() bool {
	return scs == nil
}
