// +build !race

package multiShard

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/txsFee/utils"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	"github.com/stretchr/testify/require"
)

func TestAsyncCallShouldWork(t *testing.T) {
	// TODO reinstate test after Arwen pointer fix
	if testing.Short() {
		t.Skip("cannot run with -race -short; requires Arwen fix")
	}

	testContextFirstContract, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextFirstContract.Close()

	testContextSecondContract, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSecondContract.Close()

	testContextSender, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(2, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSender.Close()

	firstContractOwner := []byte("12345678901234567890123456789010")
	require.Equal(t, uint32(0), testContextSender.ShardCoordinator.ComputeId(firstContractOwner))

	secondContractOwner := []byte("12345678901234567890123456789011")
	require.Equal(t, uint32(1), testContextSender.ShardCoordinator.ComputeId(secondContractOwner))

	senderAddr := []byte("12345678901234567890123456789032")
	require.Equal(t, uint32(2), testContextSender.ShardCoordinator.ComputeId(senderAddr))

	egldBalance := big.NewInt(1000000000)

	_, _ = vm.CreateAccount(testContextSender.Accounts, senderAddr, 0, egldBalance)
	_, _ = vm.CreateAccount(testContextFirstContract.Accounts, firstContractOwner, 0, egldBalance)
	_, _ = vm.CreateAccount(testContextSecondContract.Accounts, secondContractOwner, 0, egldBalance)

	gasPrice := uint64(10)
	deployGasLimit := uint64(50000)
	firstAccount, _ := testContextFirstContract.Accounts.LoadAccount(firstContractOwner)

	pathToContract := "../testdata/first/first.wasm"
	firstScAddress := utils.DoDeploySecond(t, testContextFirstContract, pathToContract, firstAccount, gasPrice, deployGasLimit, nil, big.NewInt(50))

	args := [][]byte{[]byte(hex.EncodeToString(firstScAddress))}
	secondAccount, _ := testContextSecondContract.Accounts.LoadAccount(secondContractOwner)
	pathToContract = "../testdata/second/output/async.wasm"
	secondSCAddress := utils.DoDeploySecond(t, testContextSecondContract, pathToContract, secondAccount, gasPrice, deployGasLimit, args, big.NewInt(50))

	utils.CleanAccumulatedIntermediateTransactions(t, testContextFirstContract)
	utils.CleanAccumulatedIntermediateTransactions(t, testContextSecondContract)
	testContextFirstContract.TxFeeHandler.CreateBlockStarted()
	testContextSecondContract.TxFeeHandler.CreateBlockStarted()

	gasLimit := uint64(5000000)
	tx := vm.CreateTransaction(0, big.NewInt(0), senderAddr, secondSCAddress, gasPrice, gasLimit, []byte("doSomething"))

	// execute on the sender shard
	retCode, err := testContextSender.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSender.GetLatestError())

	require.Equal(t, big.NewInt(120), testContextSender.TxFeeHandler.GetAccumulatedFees())

	testIndexer := vm.CreateTestIndexer(t, testContextSender.ShardCoordinator, testContextSender.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, nil)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(12), indexerTx.GasUsed)
	require.Equal(t, "120", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	utils.TestAccount(t, testContextSender.Accounts, senderAddr, 1, big.NewInt(950000000))

	// execute on the destination shard
	retCode, err = testContextSecondContract.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSender.GetLatestError())

	require.Equal(t, big.NewInt(3830), testContextSecondContract.TxFeeHandler.GetAccumulatedFees())
	require.Equal(t, big.NewInt(383), testContextSecondContract.TxFeeHandler.GetDeveloperFees())

	intermediateTxs := testContextSecondContract.GetIntermediateTransactions(t)
	testIndexer = vm.CreateTestIndexer(t, testContextSecondContract.ShardCoordinator, testContextSecondContract.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, tx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "50000000", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	// execute async call first contract shard
	scr := intermediateTxs[0]
	utils.ProcessSCRResult(t, testContextFirstContract, scr, vmcommon.Ok, nil)

	res := vm.GetIntValueFromSC(nil, testContextFirstContract.Accounts, firstScAddress, "numCalled")
	require.Equal(t, big.NewInt(1), res)

	require.Equal(t, big.NewInt(2900), testContextFirstContract.TxFeeHandler.GetAccumulatedFees())
	require.Equal(t, big.NewInt(290), testContextFirstContract.TxFeeHandler.GetDeveloperFees())

	intermediateTxs = testContextFirstContract.GetIntermediateTransactions(t)
	require.NotNil(t, intermediateTxs)

	testContextSecondContract.TxFeeHandler.CreateBlockStarted()
	scr = intermediateTxs[0]
	utils.ProcessSCRResult(t, testContextSecondContract, scr, vmcommon.Ok, nil)

	require.Equal(t, big.NewInt(49993150), testContextSecondContract.TxFeeHandler.GetAccumulatedFees())
	require.Equal(t, big.NewInt(4999315), testContextSecondContract.TxFeeHandler.GetDeveloperFees())

	intermediateTxs = testContextSecondContract.GetIntermediateTransactions(t)
	require.NotNil(t, intermediateTxs)

	// 50 000 000 fee = 120 + 2011170 + 2900 + 290
}
