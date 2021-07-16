// +build !race

// TODO remove build condition above to allow -race -short, after Arwen fix

package txsFee

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/arwen"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/txsFee/utils"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	"github.com/stretchr/testify/require"
)

func TestRelayedScDeployShouldWork(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	relayerAddr := []byte("12345678901234567890123456789033")
	sndAddr := []byte("12345678901234567890123456789012")

	senderNonce := uint64(0)
	senderBalance := big.NewInt(0)
	gasPrice := uint64(10)
	gasLimit := uint64(1000)

	_, _ = vm.CreateAccount(testContext.Accounts, sndAddr, 0, senderBalance)
	_, _ = vm.CreateAccount(testContext.Accounts, relayerAddr, 0, big.NewInt(50000))

	scCode := arwen.GetSCCode("../arwen/testdata/misc/fib_arwen/output/fib_arwen.wasm")
	userTx := vm.CreateTransaction(senderNonce, big.NewInt(0), sndAddr, vm.CreateEmptyAddress(), gasPrice, gasLimit, []byte(arwen.CreateDeployTxData(scCode)))

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, big.NewInt(0), relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, err := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	expectedBalanceRelayer := big.NewInt(28440)
	vm.TestAccount(t, testContext.Accounts, relayerAddr, 1, expectedBalanceRelayer)

	// check balance inner tx sender
	vm.TestAccount(t, testContext.Accounts, sndAddr, 1, big.NewInt(0))

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(21560), accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "23070", indexerTx.Fee)
}

func TestRelayedScDeployInvalidCodeShouldConsumeGas(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	relayerAddr := []byte("12345678901234567890123456789033")
	sndAddr := []byte("12345678901234567890123456789012")

	senderNonce := uint64(0)
	senderBalance := big.NewInt(0)
	gasPrice := uint64(10)
	gasLimit := uint64(500)

	_, _ = vm.CreateAccount(testContext.Accounts, sndAddr, 0, senderBalance)
	_, _ = vm.CreateAccount(testContext.Accounts, relayerAddr, 0, big.NewInt(50000))

	scCode := arwen.GetSCCode("../arwen/testdata/misc/fib_arwen/output/fib_arwen.wasm")
	scCodeBytes := []byte(arwen.CreateDeployTxData(scCode))
	scCodeBytes = append(scCodeBytes, []byte("aaaaa")...)
	userTx := vm.CreateTransaction(senderNonce, big.NewInt(0), sndAddr, vm.CreateEmptyAddress(), gasPrice, gasLimit, scCodeBytes)

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, big.NewInt(0), relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, _ := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.UserError, retCode)
	require.Nil(t, testContext.GetLatestError())

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	expectedBalanceRelayer := big.NewInt(31830)
	vm.TestAccount(t, testContext.Accounts, relayerAddr, 1, expectedBalanceRelayer)

	// check balance inner tx sender
	vm.TestAccount(t, testContext.Accounts, sndAddr, 1, big.NewInt(0))

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(18170), accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "18170", indexerTx.Fee)
}

func TestRelayedScDeployInsufficientGasLimitShouldConsumeGas(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	relayerAddr := []byte("12345678901234567890123456789033")
	sndAddr := []byte("12345678901234567890123456789012")

	senderNonce := uint64(0)
	senderBalance := big.NewInt(0)
	gasPrice := uint64(10)
	gasLimit := uint64(500)

	_, _ = vm.CreateAccount(testContext.Accounts, sndAddr, 0, senderBalance)
	_, _ = vm.CreateAccount(testContext.Accounts, relayerAddr, 0, big.NewInt(50000))

	scCode := arwen.GetSCCode("../arwen/testdata/misc/fib_arwen/output/fib_arwen.wasm")
	userTx := vm.CreateTransaction(senderNonce, big.NewInt(0), sndAddr, vm.CreateEmptyAddress(), gasPrice, gasLimit, []byte(arwen.CreateDeployTxData(scCode)))

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, big.NewInt(0), relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, _ := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.UserError, retCode)
	require.Nil(t, testContext.GetLatestError())

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	expectedBalanceRelayer := big.NewInt(31930)
	vm.TestAccount(t, testContext.Accounts, relayerAddr, 1, expectedBalanceRelayer)

	// check balance inner tx sender
	vm.TestAccount(t, testContext.Accounts, sndAddr, 1, big.NewInt(0))

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(18070), accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "18070", indexerTx.Fee)
}

func TestRelayedScDeployOutOfGasShouldConsumeGas(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	relayerAddr := []byte("12345678901234567890123456789033")
	sndAddr := []byte("12345678901234567890123456789012")

	senderNonce := uint64(0)
	senderBalance := big.NewInt(0)
	gasPrice := uint64(10)
	gasLimit := uint64(570)

	_, _ = vm.CreateAccount(testContext.Accounts, sndAddr, 0, senderBalance)
	_, _ = vm.CreateAccount(testContext.Accounts, relayerAddr, 0, big.NewInt(50000))

	scCode := arwen.GetSCCode("../arwen/testdata/misc/fib_arwen/output/fib_arwen.wasm")
	userTx := vm.CreateTransaction(senderNonce, big.NewInt(0), sndAddr, vm.CreateEmptyAddress(), gasPrice, gasLimit, []byte(arwen.CreateDeployTxData(scCode)))

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, big.NewInt(0), relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	code, err := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.UserError, code)
	require.Nil(t, err)
	require.Equal(t, errors.New("out of gas"), testContext.GetLatestError())

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	expectedBalanceRelayer := big.NewInt(31230)
	vm.TestAccount(t, testContext.Accounts, relayerAddr, 1, expectedBalanceRelayer)

	// check balance inner tx sender
	vm.TestAccount(t, testContext.Accounts, sndAddr, 1, big.NewInt(0))

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(18770), accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "18770", indexerTx.Fee)
}
