package mock

import (
	"math/big"

	"github.com/ElrondNetwork/elrond-go/process"
)

// EconomicsHandlerStub -
type EconomicsHandlerStub struct {
	MaxGasLimitPerBlockCalled                    func() uint64
	SetMinGasPriceCalled                         func(minGasPrice uint64)
	SetMinGasLimitCalled                         func(minGasLimit uint64)
	ComputeGasLimitCalled                        func(tx process.TransactionWithFeeHandler) uint64
	ComputeMoveBalanceFeeCalled                  func(tx process.TransactionWithFeeHandler) *big.Int
	ComputeTxFeeCalled                           func(tx process.TransactionWithFeeHandler) *big.Int
	CheckValidityTxValuesCalled                  func(tx process.TransactionWithFeeHandler) error
	DeveloperPercentageCalled                    func() float64
	MinGasPriceCalled                            func() uint64
	LeaderPercentageCalled                       func() float64
	ProtocolSustainabilityPercentageCalled       func() float64
	ProtocolSustainabilityAddressCalled          func() string
	MinInflationRateCalled                       func() float64
	MaxInflationRateCalled                       func(year uint32) float64
	GasPriceModifierCalled                       func() float64
	ComputeFeeForProcessingCalled                func(tx process.TransactionWithFeeHandler, gasToUse uint64) *big.Int
	GenesisTotalSupplyCalled                     func() *big.Int
	RewardsTopUpGradientPointCalled              func() *big.Int
	RewardsTopUpFactorCalled                     func() float64
	SplitTxGasInCategoriesCalled                 func(tx process.TransactionWithFeeHandler) (uint64, uint64)
	GasPriceForProcessingCalled                  func(tx process.TransactionWithFeeHandler) uint64
	GasPriceForMoveCalled                        func(tx process.TransactionWithFeeHandler) uint64
	MinGasPriceForProcessingCalled               func() uint64
	ComputeGasUsedAndFeeBasedOnRefundValueCalled func(tx process.TransactionWithFeeHandler, refundValue *big.Int) (uint64, *big.Int)
	ComputeTxFeeBasedOnGasUsedCalled             func(tx process.TransactionWithFeeHandler, gasUsed uint64) *big.Int
	ComputeGasLimitBasedOnBalanceCalled          func(tx process.TransactionWithFeeHandler, balance *big.Int) (uint64, error)
}

// ComputeGasLimitBasedOnBalance -
func (ehs *EconomicsHandlerStub) ComputeGasLimitBasedOnBalance(tx process.TransactionWithFeeHandler, balance *big.Int) (uint64, error) {
	if ehs.ComputeGasLimitBasedOnBalanceCalled != nil {
		return ehs.ComputeGasLimitBasedOnBalanceCalled(tx, balance)
	}
	return 0, nil
}

// ComputeFeeForProcessing -
func (ehs *EconomicsHandlerStub) ComputeFeeForProcessing(tx process.TransactionWithFeeHandler, gasToUse uint64) *big.Int {
	if ehs.ComputeFeeForProcessingCalled != nil {
		return ehs.ComputeFeeForProcessingCalled(tx, gasToUse)
	}
	return big.NewInt(0)
}

// GasPriceModifier -
func (ehs *EconomicsHandlerStub) GasPriceModifier() float64 {
	if ehs.GasPriceModifierCalled != nil {
		return ehs.GasPriceModifierCalled()
	}
	return 1.0
}

// MinGasPrice -
func (ehs *EconomicsHandlerStub) MinGasPrice() uint64 {
	if ehs.MinGasPriceCalled != nil {
		return ehs.MinGasPriceCalled()
	}
	return 0
}

// MinGasLimit will return min gas limit
func (ehs *EconomicsHandlerStub) MinGasLimit() uint64 {
	return 0
}

// GasPerDataByte -
func (ehs *EconomicsHandlerStub) GasPerDataByte() uint64 {
	return 0
}

// DeveloperPercentage -
func (ehs *EconomicsHandlerStub) DeveloperPercentage() float64 {
	return ehs.DeveloperPercentageCalled()
}

// MaxGasLimitPerBlock -
func (ehs *EconomicsHandlerStub) MaxGasLimitPerBlock(uint32) uint64 {
	return ehs.MaxGasLimitPerBlockCalled()
}

// ComputeGasLimit -
func (ehs *EconomicsHandlerStub) ComputeGasLimit(tx process.TransactionWithFeeHandler) uint64 {
	return ehs.ComputeGasLimitCalled(tx)
}

// ComputeMoveBalanceFee -
func (ehs *EconomicsHandlerStub) ComputeMoveBalanceFee(tx process.TransactionWithFeeHandler) *big.Int {
	return ehs.ComputeMoveBalanceFeeCalled(tx)
}

// ComputeTxFee -
func (ehs *EconomicsHandlerStub) ComputeTxFee(tx process.TransactionWithFeeHandler) *big.Int {
	return ehs.ComputeTxFeeCalled(tx)
}

// CheckValidityTxValues -
func (ehs *EconomicsHandlerStub) CheckValidityTxValues(tx process.TransactionWithFeeHandler) error {
	if ehs.CheckValidityTxValuesCalled != nil {
		return ehs.CheckValidityTxValuesCalled(tx)
	}

	return nil
}

// LeaderPercentage -
func (ehs *EconomicsHandlerStub) LeaderPercentage() float64 {
	if ehs.LeaderPercentageCalled != nil {
		return ehs.LeaderPercentageCalled()
	}

	return 1
}

// ProtocolSustainabilityPercentage will return the protocol sustainability percentage value
func (ehs *EconomicsHandlerStub) ProtocolSustainabilityPercentage() float64 {
	if ehs.ProtocolSustainabilityPercentageCalled != nil {
		return ehs.ProtocolSustainabilityPercentageCalled()
	}

	return 0.1
}

// ProtocolSustainabilityAddress will return the protocol sustainability address
func (ehs *EconomicsHandlerStub) ProtocolSustainabilityAddress() string {
	if ehs.ProtocolSustainabilityAddressCalled != nil {
		return ehs.ProtocolSustainabilityAddressCalled()
	}

	return "1111"
}

// MinInflationRate -
func (ehs *EconomicsHandlerStub) MinInflationRate() float64 {
	if ehs.MinInflationRateCalled != nil {
		return ehs.MinInflationRateCalled()
	}

	return 1
}

// MaxInflationRate -
func (ehs *EconomicsHandlerStub) MaxInflationRate(year uint32) float64 {
	if ehs.MaxInflationRateCalled != nil {
		return ehs.MaxInflationRateCalled(year)
	}

	return 1000000
}

// GenesisTotalSupply -
func (ehs *EconomicsHandlerStub) GenesisTotalSupply() *big.Int {
	if ehs.GenesisTotalSupplyCalled != nil {
		return ehs.GenesisTotalSupplyCalled()
	}

	return big.NewInt(0)
}

// RewardsTopUpGradientPoint -
func (ehs *EconomicsHandlerStub) RewardsTopUpGradientPoint() *big.Int {
	if ehs.RewardsTopUpGradientPointCalled != nil {
		return ehs.RewardsTopUpGradientPointCalled()
	}
	return big.NewInt(0)
}

// RewardsTopUpFactor -
func (ehs *EconomicsHandlerStub) RewardsTopUpFactor() float64 {
	if ehs.RewardsTopUpFactorCalled != nil {
		return ehs.RewardsTopUpFactorCalled()
	}
	return 0
}

// SplitTxGasInCategories -
func (ehs *EconomicsHandlerStub) SplitTxGasInCategories(tx process.TransactionWithFeeHandler) (uint64, uint64) {
	if ehs.SplitTxGasInCategoriesCalled != nil {
		return ehs.SplitTxGasInCategoriesCalled(tx)
	}
	return 0, 0
}

// GasPriceForProcessing -
func (ehs *EconomicsHandlerStub) GasPriceForProcessing(tx process.TransactionWithFeeHandler) uint64 {
	if ehs.GasPriceForProcessingCalled != nil {
		return ehs.GasPriceForProcessingCalled(tx)
	}
	return 0
}

// GasPriceForMove -
func (ehs *EconomicsHandlerStub) GasPriceForMove(tx process.TransactionWithFeeHandler) uint64 {
	if ehs.GasPriceForMoveCalled != nil {
		return ehs.GasPriceForMoveCalled(tx)
	}
	return 0
}

// MinGasPriceForProcessing -
func (ehs *EconomicsHandlerStub) MinGasPriceForProcessing() uint64 {
	if ehs.MinGasPriceForProcessingCalled != nil {
		return ehs.MinGasPriceForProcessingCalled()
	}
	return 0
}

// ComputeGasUsedAndFeeBasedOnRefundValue -
func (ehs *EconomicsHandlerStub) ComputeGasUsedAndFeeBasedOnRefundValue(tx process.TransactionWithFeeHandler, refundValue *big.Int) (uint64, *big.Int) {
	if ehs.ComputeGasUsedAndFeeBasedOnRefundValueCalled != nil {
		return ehs.ComputeGasUsedAndFeeBasedOnRefundValueCalled(tx, refundValue)
	}
	return 0, big.NewInt(0)
}

// ComputeTxFeeBasedOnGasUsed -
func (ehs *EconomicsHandlerStub) ComputeTxFeeBasedOnGasUsed(tx process.TransactionWithFeeHandler, gasUsed uint64) *big.Int {
	if ehs.ComputeTxFeeBasedOnGasUsedCalled != nil {
		return ehs.ComputeTxFeeBasedOnGasUsedCalled(tx, gasUsed)
	}
	return big.NewInt(0)
}

// IsInterfaceNil returns true if there is no value under the interface
func (ehs *EconomicsHandlerStub) IsInterfaceNil() bool {
	return ehs == nil
}
