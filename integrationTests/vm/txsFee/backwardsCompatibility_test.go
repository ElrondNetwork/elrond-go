package txsFee

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/receipt"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minGasPrice = 1, gasPerDataByte = 1, minGasLimit = 1
func TestMoveBalanceSelfShouldWorkAndConsumeTxFeeWhenAllFlagsAreDisabled(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(
		vm.ArgEnableEpoch{
			PenalizedTooMuchGasEnableEpoch: 100,
			BuiltinEnableEpoch:             100,
			DeployEnableEpoch:              100,
			MetaProtectionEnableEpoch:      100,
			RelayedTxEnableEpoch:           100,
		})
	require.Nil(t, err)
	defer testContext.Close()

	sndAddr := []byte("12345678901234567890123456789012")
	senderNonce := uint64(0)
	senderBalance := big.NewInt(10000)
	gasPrice := uint64(10)
	gasLimit := uint64(100)

	_, _ = vm.CreateAccount(testContext.Accounts, sndAddr, 0, senderBalance)
	tx := vm.CreateTransaction(senderNonce, big.NewInt(100), sndAddr, sndAddr, gasPrice, gasLimit, []byte("aaaa"))

	_, err = testContext.TxProcessor.ProcessTransaction(tx)
	require.Nil(t, err)
	require.Nil(t, testContext.GetLatestError())

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	// 10_000 - gasPrice(10)*gasLimit(1) + gasPerDataByte(1)*gasPrice(10)*4 = 10000 - 50 = 9950
	expectedBalance := big.NewInt(9950)
	vm.TestAccount(t, testContext.Accounts, sndAddr, senderNonce+1, expectedBalance)

	//check receipts
	require.Equal(t, 1, len(testContext.GetIntermediateTransactions(t)))
	rcpt := testContext.GetIntermediateTransactions(t)[0].(*receipt.Receipt)
	assert.Equal(t, "950", rcpt.Value.String())

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(50), accumulatedFees)

	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, nil)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(5), indexerTx.GasUsed)
	require.Equal(t, "50", indexerTx.Fee)
}

// minGasPrice = 1, gasPerDataByte = 1, minGasLimit = 1
func TestMoveBalanceAllFlagsDisabledLessBalanceThanGasLimitMulGasPrice(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(
		vm.ArgEnableEpoch{
			PenalizedTooMuchGasEnableEpoch: 100,
			BuiltinEnableEpoch:             100,
			DeployEnableEpoch:              100,
			MetaProtectionEnableEpoch:      100,
			RelayedTxEnableEpoch:           100,
		})
	require.Nil(t, err)
	defer testContext.Close()

	sndAddr := []byte("12345678901234567890123456789012")
	senderNonce := uint64(0)
	senderBalance := big.NewInt(10000)
	gasPrice := uint64(10)
	gasLimit := uint64(10000)

	_, _ = vm.CreateAccount(testContext.Accounts, sndAddr, 0, senderBalance)
	tx := vm.CreateTransaction(senderNonce, big.NewInt(100), sndAddr, sndAddr, gasPrice, gasLimit, []byte("aaaa"))

	_, err = testContext.TxProcessor.ProcessTransaction(tx)
	require.True(t, errors.Is(err, process.ErrInsufficientFee))
}

func TestMoveBalanceSelfShouldWorkAndConsumeTxFeeWhenSomeFlagsAreDisabled(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(
		vm.ArgEnableEpoch{
			PenalizedTooMuchGasEnableEpoch: 0,
			BuiltinEnableEpoch:             100,
			DeployEnableEpoch:              100,
			MetaProtectionEnableEpoch:      100,
			RelayedTxEnableEpoch:           100,
		})
	require.Nil(t, err)
	defer testContext.Close()

	sndAddr := []byte("12345678901234567890123456789012")
	senderNonce := uint64(0)
	senderBalance := big.NewInt(10000)
	gasPrice := uint64(10)
	gasLimit := uint64(100)

	_, _ = vm.CreateAccount(testContext.Accounts, sndAddr, 0, senderBalance)
	tx := vm.CreateTransaction(senderNonce, big.NewInt(100), sndAddr, sndAddr, gasPrice, gasLimit, []byte("aaaa"))

	_, err = testContext.TxProcessor.ProcessTransaction(tx)
	require.Nil(t, err)
	require.Nil(t, testContext.GetLatestError())

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	// 10_000 - gasPrice(10)*gasLimit(1) + gasPerDataByte(1)*gasPrice(10)*4 = 10000 - 50 = 9950
	expectedBalance := big.NewInt(9950)
	vm.TestAccount(t, testContext.Accounts, sndAddr, senderNonce+1, expectedBalance)

	//check receipts
	require.Equal(t, 1, len(testContext.GetIntermediateTransactions(t)))
	rcpt := testContext.GetIntermediateTransactions(t)[0].(*receipt.Receipt)
	assert.Equal(t, "950", rcpt.Value.String())

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(50), accumulatedFees)

	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, nil)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(5), indexerTx.GasUsed)
	require.Equal(t, "50", indexerTx.Fee)
}
