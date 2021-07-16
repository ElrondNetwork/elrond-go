// +build !race

// TODO remove build condition above to allow -race -short, after Arwen fix

package multiShard

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/txsFee/utils"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	"github.com/stretchr/testify/require"
)

func TestScCallExecuteOnSourceAndDstShardShouldWork(t *testing.T) {
	testContextSource, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSource.Close()

	testContextDst, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextDst.Close()

	pathToContract := "../../arwen/testdata/counter/output/counter.wasm"
	scAddr, owner := utils.DoDeploy(t, testContextDst, pathToContract)
	utils.CleanAccumulatedIntermediateTransactions(t, testContextDst)
	testContextDst.TxFeeHandler.CreateBlockStarted()

	require.Equal(t, uint32(1), testContextDst.ShardCoordinator.ComputeId(scAddr))
	require.Equal(t, uint32(1), testContextDst.ShardCoordinator.ComputeId(owner))

	sndAddr := []byte("12345678901234567890123456789010")
	shardID := testContextDst.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(0), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(500)

	_, _ = vm.CreateAccount(testContextSource.Accounts, sndAddr, 0, big.NewInt(10000))

	tx := vm.CreateTransaction(0, big.NewInt(0), sndAddr, scAddr, gasPrice, gasLimit, []byte("increment"))

	// execute on source shard
	retCode, err := testContextSource.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSource.GetLatestError())

	_, err = testContextSource.Accounts.Commit()
	require.Nil(t, err)

	expectedBalance := big.NewInt(5000)
	vm.TestAccount(t, testContextSource.Accounts, sndAddr, 1, expectedBalance)

	// check accumulated fees
	accumulatedFees := testContextSource.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(100), accumulatedFees)

	developerFees := testContextSource.TxFeeHandler.GetDeveloperFees()
	require.Equal(t, big.NewInt(0), developerFees)

	intermediateTxs := testContextSource.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContextSource.ShardCoordinator, testContextSource.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(10), indexerTx.GasUsed)
	require.Equal(t, "100", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	//execute on destination shard
	retCode, err = testContextDst.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextDst.GetLatestError())

	ret := vm.GetIntValueFromSC(nil, testContextDst.Accounts, scAddr, "get")
	require.Equal(t, big.NewInt(2), ret)

	// check accumulated fees dest
	accumulatedFees = testContextDst.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(3770), accumulatedFees)

	developerFees = testContextDst.TxFeeHandler.GetDeveloperFees()
	require.Equal(t, big.NewInt(377), developerFees)

	// execute sc result with gas refund
	txs := testContextDst.GetIntermediateTransactions(t)

	testIndexer = vm.CreateTestIndexer(t, testContextDst.ShardCoordinator, testContextDst.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, txs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(387), indexerTx.GasUsed)
	require.Equal(t, "3870", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	scr := txs[0]

	utils.ProcessSCRResult(t, testContextSource, scr, vmcommon.Ok, nil)

	// check sender balance after refund
	expectedBalance = big.NewInt(6130)
	vm.TestAccount(t, testContextSource.Accounts, sndAddr, 1, expectedBalance)

	// check accumulated fees
	accumulatedFees = testContextSource.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(100), accumulatedFees)
}

func TestScCallExecuteOnSourceAndDstShardInvalidOnDst(t *testing.T) {
	testContextSource, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSource.Close()

	testContextDst, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextDst.Close()

	pathToContract := "../../arwen/testdata/counter/output/counter.wasm"
	scAddr, owner := utils.DoDeploy(t, testContextDst, pathToContract)
	testContextDst.TxFeeHandler.CreateBlockStarted()

	require.Equal(t, uint32(1), testContextDst.ShardCoordinator.ComputeId(scAddr))
	require.Equal(t, uint32(1), testContextDst.ShardCoordinator.ComputeId(owner))

	sndAddr := []byte("12345678901234567890123456789010")
	shardID := testContextDst.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(0), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(500)

	_, _ = vm.CreateAccount(testContextSource.Accounts, sndAddr, 0, big.NewInt(10000))

	// execute on source shard
	tx := vm.CreateTransaction(0, big.NewInt(0), sndAddr, scAddr, gasPrice, gasLimit, []byte("incremena"))
	retCode, err := testContextSource.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSource.GetLatestError())

	_, err = testContextSource.Accounts.Commit()
	require.Nil(t, err)

	expectedBalance := big.NewInt(5000)
	vm.TestAccount(t, testContextSource.Accounts, sndAddr, 1, expectedBalance)

	// check accumulated fees
	accumulatedFees := testContextSource.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(100), accumulatedFees)

	developerFees := testContextSource.TxFeeHandler.GetDeveloperFees()
	require.Equal(t, big.NewInt(0), developerFees)

	intermediateTxs := testContextSource.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContextSource.ShardCoordinator, testContextSource.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(10), indexerTx.GasUsed)
	require.Equal(t, "100", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	//execute on destination shard
	utils.CleanAccumulatedIntermediateTransactions(t, testContextDst)
	retCode, err = testContextDst.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.UserError, retCode)
	require.Nil(t, err)
	require.Equal(t, fmt.Errorf("function not found"), testContextDst.GetLatestError())

	ret := vm.GetIntValueFromSC(nil, testContextDst.Accounts, scAddr, "get")
	require.Equal(t, big.NewInt(1), ret)

	// check accumulated fees dest
	accumulatedFees = testContextDst.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(4900), accumulatedFees)

	developerFees = testContextDst.TxFeeHandler.GetDeveloperFees()
	require.Equal(t, big.NewInt(0), developerFees)

	// execute sc result with gas refund
	txs := testContextDst.GetIntermediateTransactions(t)

	testIndexer = vm.CreateTestIndexer(t, testContextDst.ShardCoordinator, testContextDst.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, txs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, tx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "5000", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusFail.String(), indexerTx.Status)

	scr := txs[0]

	utils.ProcessSCRResult(t, testContextSource, scr, vmcommon.Ok, nil)

	// check sender balance after refund
	expectedBalance = big.NewInt(5000)
	vm.TestAccount(t, testContextSource.Accounts, sndAddr, 1, expectedBalance)

	// check accumulated fees
	accumulatedFees = testContextSource.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(100), accumulatedFees)
}
