// +build !race

// TODO remove build condition above to allow -race -short, after Arwen fix

package multiShard

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

func TestAsyncESDTTransferWithSCCallShouldWork(t *testing.T) {
	testContextSender, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSender.Close()

	testContextFirstContract, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextFirstContract.Close()

	testContextSecondContract, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(2, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSecondContract.Close()

	senderAddr := []byte("12345678901234567890123456789030")
	require.Equal(t, uint32(0), testContextSender.ShardCoordinator.ComputeId(senderAddr))

	firstContractOwner := []byte("12345678901234567890123456789011")
	require.Equal(t, uint32(1), testContextSender.ShardCoordinator.ComputeId(firstContractOwner))

	secondContractOwner := []byte("12345678901234567890123456789012")
	require.Equal(t, uint32(2), testContextSender.ShardCoordinator.ComputeId(secondContractOwner))

	token := []byte("miiutoken")
	egldBalance := big.NewInt(10000000)
	esdtBalance := big.NewInt(10000000)
	utils.CreateAccountWithESDTBalance(t, testContextSender.Accounts, senderAddr, egldBalance, token, esdtBalance)

	// create accounts for owners
	_, _ = vm.CreateAccount(testContextFirstContract.Accounts, firstContractOwner, 0, egldBalance)
	_, _ = vm.CreateAccount(testContextSecondContract.Accounts, secondContractOwner, 0, egldBalance)

	// deploy contracts on shard 1 and shard 2
	gasPrice := uint64(10)
	gasLimitDeploy := uint64(50000)

	secondContractOwnerAcc, _ := testContextSecondContract.Accounts.LoadAccount(secondContractOwner)
	argsSecond := [][]byte{[]byte(hex.EncodeToString(token))}
	secondSCAddress := utils.DoDeploySecond(t, testContextSecondContract, "../../esdt/testdata/second-contract.wasm", secondContractOwnerAcc, gasPrice, gasLimitDeploy, argsSecond, big.NewInt(0))
	testContextSecondContract.TxFeeHandler.CreateBlockStarted()

	firstContractOwnerAcc, _ := testContextFirstContract.Accounts.LoadAccount(firstContractOwner)
	args := [][]byte{[]byte(hex.EncodeToString(token)), []byte(hex.EncodeToString(secondSCAddress))}
	firstSCAddress := utils.DoDeploySecond(t, testContextFirstContract, "../../esdt/testdata/first-contract.wasm", firstContractOwnerAcc, gasPrice, gasLimitDeploy, args, big.NewInt(0))
	testContextFirstContract.TxFeeHandler.CreateBlockStarted()

	require.Equal(t, uint32(1), testContextSender.ShardCoordinator.ComputeId(firstSCAddress))
	require.Equal(t, uint32(2), testContextSender.ShardCoordinator.ComputeId(secondSCAddress))

	utils.CleanAccumulatedIntermediateTransactions(t, testContextFirstContract)
	utils.CleanAccumulatedIntermediateTransactions(t, testContextSecondContract)

	gasLimit := uint64(500000)
	tx := utils.CreateESDTTransferTx(0, senderAddr, firstSCAddress, token, big.NewInt(5000), gasPrice, gasLimit)
	tx.Data = []byte(string(tx.Data) + "@" + hex.EncodeToString([]byte("transferToSecondContractHalf")))

	// execute on the source shard
	retCode, err := testContextSender.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSender.GetLatestError())

	_, err = testContextSender.Accounts.Commit()
	require.Nil(t, err)

	expectedAccumulatedFees := big.NewInt(950)
	require.Equal(t, expectedAccumulatedFees, testContextSender.TxFeeHandler.GetAccumulatedFees())

	testIndexer := vm.CreateTestIndexer(t, testContextSender.ShardCoordinator, testContextSender.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, nil)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(94), indexerTx.GasUsed)
	require.Equal(t, "940", indexerTx.Fee)

	// execute on the destination shard
	retCode, err = testContextFirstContract.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextFirstContract.GetLatestError())

	_, err = testContextSender.Accounts.Commit()
	require.Nil(t, err)

	expectedAccumulatedFees = big.NewInt(190290)
	require.Equal(t, expectedAccumulatedFees, testContextFirstContract.TxFeeHandler.GetAccumulatedFees())

	developerFees := big.NewInt(19029)
	require.Equal(t, developerFees, testContextFirstContract.TxFeeHandler.GetDeveloperFees())

	utils.CheckESDTBalance(t, testContextFirstContract, firstSCAddress, token, big.NewInt(2500))

	intermediateTxs := testContextFirstContract.GetIntermediateTransactions(t)
	testIndexer = vm.CreateTestIndexer(t, testContextFirstContract.ShardCoordinator, testContextFirstContract.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(500000), indexerTx.GasUsed)
	require.Equal(t, "5000000", indexerTx.Fee)

	scrForSecondContract := intermediateTxs[1]
	require.Equal(t, scrForSecondContract.GetSndAddr(), firstSCAddress)
	require.Equal(t, scrForSecondContract.GetRcvAddr(), secondSCAddress)
	utils.ProcessSCRResult(t, testContextSecondContract, scrForSecondContract, vmcommon.Ok, nil)

	utils.CheckESDTBalance(t, testContextSecondContract, secondSCAddress, token, big.NewInt(2500))

	accumulatedFee := big.NewInt(62420)
	require.Equal(t, accumulatedFee, testContextSecondContract.TxFeeHandler.GetAccumulatedFees())

	developerFees = big.NewInt(6242)
	require.Equal(t, developerFees, testContextSecondContract.TxFeeHandler.GetDeveloperFees())

	intermediateTxs = testContextSecondContract.GetIntermediateTransactions(t)
	require.NotNil(t, intermediateTxs)

	utils.ProcessSCRResult(t, testContextFirstContract, intermediateTxs[1], vmcommon.Ok, nil)

	require.Equal(t, big.NewInt(4936620), testContextFirstContract.TxFeeHandler.GetAccumulatedFees())
}

func TestAsyncESDTTransferWithSCCallSecondContractAnotherToken(t *testing.T) {
	testContextSender, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSender.Close()

	testContextFirstContract, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextFirstContract.Close()

	testContextSecondContract, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(2, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSecondContract.Close()

	senderAddr := []byte("12345678901234567890123456789030")
	require.Equal(t, uint32(0), testContextSender.ShardCoordinator.ComputeId(senderAddr))

	firstContractOwner := []byte("12345678901234567890123456789011")
	require.Equal(t, uint32(1), testContextSender.ShardCoordinator.ComputeId(firstContractOwner))

	secondContractOwner := []byte("12345678901234567890123456789012")
	require.Equal(t, uint32(2), testContextSender.ShardCoordinator.ComputeId(secondContractOwner))

	token := []byte("miiutoken")
	egldBalance := big.NewInt(10000000)
	esdtBalance := big.NewInt(10000000)
	utils.CreateAccountWithESDTBalance(t, testContextSender.Accounts, senderAddr, egldBalance, token, esdtBalance)

	// create accounts for owners
	_, _ = vm.CreateAccount(testContextFirstContract.Accounts, firstContractOwner, 0, egldBalance)
	_, _ = vm.CreateAccount(testContextSecondContract.Accounts, secondContractOwner, 0, egldBalance)

	// deploy contracts on shard 1 and shard 2
	gasPrice := uint64(10)
	gasLimitDeploy := uint64(50000)

	secondContractOwnerAcc, _ := testContextSecondContract.Accounts.LoadAccount(secondContractOwner)
	argsSecond := [][]byte{[]byte(hex.EncodeToString(append(token, []byte("aaa")...)))}
	secondSCAddress := utils.DoDeploySecond(t, testContextSecondContract, "../../esdt/testdata/second-contract.wasm", secondContractOwnerAcc, gasPrice, gasLimitDeploy, argsSecond, big.NewInt(0))
	testContextSecondContract.TxFeeHandler.CreateBlockStarted()

	firstContractOwnerAcc, _ := testContextFirstContract.Accounts.LoadAccount(firstContractOwner)
	args := [][]byte{[]byte(hex.EncodeToString(token)), []byte(hex.EncodeToString(secondSCAddress))}
	firstSCAddress := utils.DoDeploySecond(t, testContextFirstContract, "../../esdt/testdata/first-contract.wasm", firstContractOwnerAcc, gasPrice, gasLimitDeploy, args, big.NewInt(0))
	testContextFirstContract.TxFeeHandler.CreateBlockStarted()

	require.Equal(t, uint32(1), testContextSender.ShardCoordinator.ComputeId(firstSCAddress))
	require.Equal(t, uint32(2), testContextSender.ShardCoordinator.ComputeId(secondSCAddress))

	utils.CleanAccumulatedIntermediateTransactions(t, testContextFirstContract)
	utils.CleanAccumulatedIntermediateTransactions(t, testContextSecondContract)

	gasLimit := uint64(500000)
	tx := utils.CreateESDTTransferTx(0, senderAddr, firstSCAddress, token, big.NewInt(5000), gasPrice, gasLimit)
	tx.Data = []byte(string(tx.Data) + "@" + hex.EncodeToString([]byte("transferToSecondContractHalf")))

	// execute on the source shard
	retCode, err := testContextSender.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSender.GetLatestError())

	_, err = testContextSender.Accounts.Commit()
	require.Nil(t, err)

	expectedAccumulatedFees := big.NewInt(950)
	require.Equal(t, expectedAccumulatedFees, testContextSender.TxFeeHandler.GetAccumulatedFees())

	testIndexer := vm.CreateTestIndexer(t, testContextSender.ShardCoordinator, testContextSender.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, nil)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(94), indexerTx.GasUsed)
	require.Equal(t, "940", indexerTx.Fee)

	// execute on the destination shard
	retCode, err = testContextFirstContract.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextFirstContract.GetLatestError())

	_, err = testContextSender.Accounts.Commit()
	require.Nil(t, err)

	expectedAccumulatedFees = big.NewInt(190290)
	require.Equal(t, expectedAccumulatedFees, testContextFirstContract.TxFeeHandler.GetAccumulatedFees())

	developerFees := big.NewInt(19029)
	require.Equal(t, developerFees, testContextFirstContract.TxFeeHandler.GetDeveloperFees())

	utils.CheckESDTBalance(t, testContextFirstContract, firstSCAddress, token, big.NewInt(2500))

	intermediateTxs := testContextFirstContract.GetIntermediateTransactions(t)
	testIndexer = vm.CreateTestIndexer(t, testContextFirstContract.ShardCoordinator, testContextFirstContract.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(500000), indexerTx.GasUsed)
	require.Equal(t, "5000000", indexerTx.Fee)

	scrForSecondContract := intermediateTxs[1]
	require.Equal(t, scrForSecondContract.GetSndAddr(), firstSCAddress)
	require.Equal(t, scrForSecondContract.GetRcvAddr(), secondSCAddress)
	utils.ProcessSCRResult(t, testContextSecondContract, scrForSecondContract, vmcommon.UserError, nil)

	utils.CheckESDTBalance(t, testContextSecondContract, secondSCAddress, token, big.NewInt(0))

	accumulatedFee := big.NewInt(3720360)
	require.Equal(t, accumulatedFee, testContextSecondContract.TxFeeHandler.GetAccumulatedFees())

	developerFees = big.NewInt(0)
	require.Equal(t, developerFees, testContextSecondContract.TxFeeHandler.GetDeveloperFees())
	// consumed fee 5 000 000 = 950 + 3 740 770 + 1 258 270 + 10 (built in function call)
	intermediateTxs = testContextSecondContract.GetIntermediateTransactions(t)
	require.NotNil(t, intermediateTxs)

	utils.ProcessSCRResult(t, testContextFirstContract, intermediateTxs[0], vmcommon.Ok, nil)

	require.Equal(t, big.NewInt(1278680), testContextFirstContract.TxFeeHandler.GetAccumulatedFees())
}
