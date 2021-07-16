// +build !race

// TODO remove build condition above to allow -race -short, after Arwen fix

package txsFee

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/txsFee/utils"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	"github.com/stretchr/testify/require"
)

func TestRelayedAsyncESDTCallShouldWork(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	egldBalance := big.NewInt(100000000)
	ownerAddr := []byte("12345678901234567890123456789010")
	_, _ = vm.CreateAccount(testContext.Accounts, ownerAddr, 0, egldBalance)

	// create an address with ESDT token
	relayerAddr := []byte("12345678901234567890123456789033")
	sndAddr := []byte("12345678901234567890123456789012")

	esdtBalance := big.NewInt(100000000)
	token := []byte("miiutoken")
	utils.CreateAccountWithESDTBalance(t, testContext.Accounts, sndAddr, big.NewInt(0), token, esdtBalance)
	_, _ = vm.CreateAccount(testContext.Accounts, relayerAddr, 0, egldBalance)

	// deploy 2 contracts
	gasPrice := uint64(10)
	ownerAccount, _ := testContext.Accounts.LoadAccount(ownerAddr)
	deployGasLimit := uint64(50000)

	argsSecond := [][]byte{[]byte(hex.EncodeToString(token))}
	secondSCAddress := utils.DoDeploySecond(t, testContext, "../esdt/testdata/second-contract.wasm", ownerAccount, gasPrice, deployGasLimit, argsSecond, big.NewInt(0))

	args := [][]byte{[]byte(hex.EncodeToString(token)), []byte(hex.EncodeToString(secondSCAddress))}
	ownerAccount, _ = testContext.Accounts.LoadAccount(ownerAddr)
	firstSCAddress := utils.DoDeploySecond(t, testContext, "../esdt/testdata/first-contract.wasm", ownerAccount, gasPrice, deployGasLimit, args, big.NewInt(0))

	testContext.TxFeeHandler.CreateBlockStarted()
	utils.CleanAccumulatedIntermediateTransactions(t, testContext)

	gasLimit := uint64(500000)
	innerTx := utils.CreateESDTTransferTx(0, sndAddr, firstSCAddress, token, big.NewInt(5000), gasPrice, gasLimit)
	innerTx.Data = []byte(string(innerTx.Data) + "@" + hex.EncodeToString([]byte("transferToSecondContractHalf")))

	rtxData := utils.PrepareRelayerTxData(innerTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, innerTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, err := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	utils.CheckESDTBalance(t, testContext, firstSCAddress, token, big.NewInt(2500))
	utils.CheckESDTBalance(t, testContext, secondSCAddress, token, big.NewInt(2500))

	expectedSenderBalance := big.NewInt(94996430)
	utils.TestAccount(t, testContext.Accounts, relayerAddr, 1, expectedSenderBalance)

	expectedAccumulatedFees := big.NewInt(5003570)
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, expectedAccumulatedFees, accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "5003570", indexerTx.Fee)
}

func TestRelayedAsyncESDTCall_InvalidCallFirstContract(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	egldBalance := big.NewInt(100000000)
	ownerAddr := []byte("12345678901234567890123456789010")
	_, _ = vm.CreateAccount(testContext.Accounts, ownerAddr, 0, egldBalance)

	// create an address with ESDT token
	relayerAddr := []byte("12345678901234567890123456789033")
	sndAddr := []byte("12345678901234567890123456789012")

	esdtBalance := big.NewInt(100000000)
	token := []byte("miiutoken")
	utils.CreateAccountWithESDTBalance(t, testContext.Accounts, sndAddr, big.NewInt(0), token, esdtBalance)
	_, _ = vm.CreateAccount(testContext.Accounts, relayerAddr, 0, egldBalance)

	// deploy 2 contracts
	gasPrice := uint64(10)
	ownerAccount, _ := testContext.Accounts.LoadAccount(ownerAddr)
	deployGasLimit := uint64(50000)

	argsSecond := [][]byte{[]byte(hex.EncodeToString(token))}
	secondSCAddress := utils.DoDeploySecond(t, testContext, "../esdt/testdata/second-contract.wasm", ownerAccount, gasPrice, deployGasLimit, argsSecond, big.NewInt(0))

	args := [][]byte{[]byte(hex.EncodeToString(token)), []byte(hex.EncodeToString(secondSCAddress))}
	ownerAccount, _ = testContext.Accounts.LoadAccount(ownerAddr)
	firstSCAddress := utils.DoDeploySecond(t, testContext, "../esdt/testdata/first-contract.wasm", ownerAccount, gasPrice, deployGasLimit, args, big.NewInt(0))

	testContext.TxFeeHandler.CreateBlockStarted()
	utils.CleanAccumulatedIntermediateTransactions(t, testContext)

	gasLimit := uint64(500000)
	innerTx := utils.CreateESDTTransferTx(0, sndAddr, firstSCAddress, token, big.NewInt(5000), gasPrice, gasLimit)
	innerTx.Data = []byte(string(innerTx.Data) + "@" + hex.EncodeToString([]byte("transferToSecondContractRejected")))

	rtxData := utils.PrepareRelayerTxData(innerTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, innerTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, err := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	utils.CheckESDTBalance(t, testContext, firstSCAddress, token, big.NewInt(5000))
	utils.CheckESDTBalance(t, testContext, secondSCAddress, token, big.NewInt(0))

	expectedSenderBalance := big.NewInt(95996260)
	utils.TestAccount(t, testContext.Accounts, relayerAddr, 1, expectedSenderBalance)

	expectedAccumulatedFees := big.NewInt(4003740)
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, expectedAccumulatedFees, accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "5003730", indexerTx.Fee)
}

func TestRelayedAsyncESDTCall_InvalidOutOfGas(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMs(vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	egldBalance := big.NewInt(100000000)
	ownerAddr := []byte("12345678901234567890123456789010")
	_, _ = vm.CreateAccount(testContext.Accounts, ownerAddr, 0, egldBalance)

	// create an address with ESDT token
	relayerAddr := []byte("12345678901234567890123456789033")
	sndAddr := []byte("12345678901234567890123456789012")

	esdtBalance := big.NewInt(100000000)
	token := []byte("miiutoken")
	utils.CreateAccountWithESDTBalance(t, testContext.Accounts, sndAddr, big.NewInt(0), token, esdtBalance)
	_, _ = vm.CreateAccount(testContext.Accounts, relayerAddr, 0, egldBalance)

	// deploy 2 contracts
	gasPrice := uint64(10)
	ownerAccount, _ := testContext.Accounts.LoadAccount(ownerAddr)
	deployGasLimit := uint64(50000)

	argsSecond := [][]byte{[]byte(hex.EncodeToString(token))}
	secondSCAddress := utils.DoDeploySecond(t, testContext, "../esdt/testdata/second-contract.wasm", ownerAccount, gasPrice, deployGasLimit, argsSecond, big.NewInt(0))

	args := [][]byte{[]byte(hex.EncodeToString(token)), []byte(hex.EncodeToString(secondSCAddress))}
	ownerAccount, _ = testContext.Accounts.LoadAccount(ownerAddr)
	firstSCAddress := utils.DoDeploySecond(t, testContext, "../esdt/testdata/first-contract.wasm", ownerAccount, gasPrice, deployGasLimit, args, big.NewInt(0))

	testContext.TxFeeHandler.CreateBlockStarted()
	utils.CleanAccumulatedIntermediateTransactions(t, testContext)

	gasLimit := uint64(2000)
	innerTx := utils.CreateESDTTransferTx(0, sndAddr, firstSCAddress, token, big.NewInt(5000), gasPrice, gasLimit)
	innerTx.Data = []byte(string(innerTx.Data) + "@" + hex.EncodeToString([]byte("transferToSecondContractHalf")))

	rtxData := utils.PrepareRelayerTxData(innerTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, innerTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, err := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.UserError, retCode)
	require.Nil(t, err)

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	utils.CheckESDTBalance(t, testContext, firstSCAddress, token, big.NewInt(0))
	utils.CheckESDTBalance(t, testContext, secondSCAddress, token, big.NewInt(0))

	expectedSenderBalance := big.NewInt(99976450)
	utils.TestAccount(t, testContext.Accounts, relayerAddr, 1, expectedSenderBalance)

	expectedAccumulatedFees := big.NewInt(23550)
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, expectedAccumulatedFees, accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "23550", indexerTx.Fee)
}
