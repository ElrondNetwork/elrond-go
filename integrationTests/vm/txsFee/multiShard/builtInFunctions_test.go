package multiShard

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/txsFee/utils"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	"github.com/stretchr/testify/require"
)

// Test scenario
// 1. Do a SC deploy on shard 1
// 2. Do a ChangeOwnerAddress (owner of the deployed contract will be moved in shard 0)
// 3. Do a ClaimDeveloperReward (cross shard call , the transaction will be executed on the source shard and the destination shard)
// 4. Execute SCR from context destination on context source ( the new owner will receive the developer rewards)
func TestBuiltInFunctionExecuteOnSourceAndDestinationShouldWork(t *testing.T) {
	// TODO reinstate test after Arwen pointer fix
	if testing.Short() {
		t.Skip("cannot run with -race -short; requires Arwen fix")
	}

	testContextSource, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(
		0,
		vm.ArgEnableEpoch{
			PenalizedTooMuchGasEnableEpoch: 100,
		})
	require.Nil(t, err)
	defer testContextSource.Close()

	testContextDst, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(
		1,
		vm.ArgEnableEpoch{
			PenalizedTooMuchGasEnableEpoch: 100,
		})
	require.Nil(t, err)
	defer testContextDst.Close()

	pathToContract := "../../arwen/testdata/counter/output/counter.wasm"
	scAddr, owner := utils.DoDeploy(t, testContextDst, pathToContract)
	require.Equal(t, uint32(1), testContextDst.ShardCoordinator.ComputeId(scAddr))
	require.Equal(t, uint32(1), testContextDst.ShardCoordinator.ComputeId(owner))
	testContextDst.TxFeeHandler.CreateBlockStarted()
	utils.CleanAccumulatedIntermediateTransactions(t, testContextDst)

	newOwner := []byte("12345678901234567890123456789110")
	require.Equal(t, uint32(0), testContextDst.ShardCoordinator.ComputeId(newOwner))

	gasPrice := uint64(10)
	gasLimit := uint64(1000)

	txData := []byte(core.BuiltInFunctionChangeOwnerAddress + "@" + hex.EncodeToString(newOwner))
	tx := vm.CreateTransaction(1, big.NewInt(0), owner, scAddr, gasPrice, gasLimit, txData)
	_, err = testContextDst.TxProcessor.ProcessTransaction(tx)
	require.Nil(t, err)
	require.Nil(t, testContextDst.GetLatestError())

	_, err = testContextDst.Accounts.Commit()
	require.Nil(t, err)

	utils.CheckOwnerAddr(t, testContextDst, scAddr, newOwner)

	accumulatedFees := testContextDst.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(850), accumulatedFees)

	testIndexer := vm.CreateTestIndexer(t, testContextDst.ShardCoordinator, testContextDst.EconomicsData, false)
	testIndexer.SaveTransaction(tx, block.TxBlock, nil)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(84), indexerTx.GasUsed)
	require.Equal(t, "840", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	utils.CleanAccumulatedIntermediateTransactions(t, testContextDst)

	// do a sc call intra shard
	sndAddr := []byte("12345678901234567890123456789111")
	shardID := testContextDst.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(1), shardID)

	scStateAcc, _ := testContextDst.Accounts.GetExistingAccount(scAddr)
	scUserAcc := scStateAcc.(state.UserAccountHandler)
	currentSCDevBalance := scUserAcc.GetDeveloperReward()

	gasLimit = uint64(500)
	_, _ = vm.CreateAccount(testContextDst.Accounts, sndAddr, 0, big.NewInt(10000))
	tx = vm.CreateTransaction(0, big.NewInt(0), sndAddr, scAddr, gasPrice, gasLimit, []byte("increment"))

	retCode, err := testContextDst.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextDst.GetLatestError())

	_, err = testContextDst.Accounts.Commit()
	require.Nil(t, err)

	expectedBalance := big.NewInt(6130)
	vm.TestAccount(t, testContextDst.Accounts, sndAddr, 1, expectedBalance)

	accumulatedFees = testContextDst.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(4720), accumulatedFees)

	developerFees := testContextDst.TxFeeHandler.GetDeveloperFees()
	require.Equal(t, big.NewInt(377), developerFees)

	intermediateTxs := testContextDst.GetIntermediateTransactions(t)
	testIndexer = vm.CreateTestIndexer(t, testContextDst.ShardCoordinator, testContextDst.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(387), indexerTx.GasUsed)
	require.Equal(t, "3870", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	// call get developer rewards
	gasLimit = 500
	_, _ = vm.CreateAccount(testContextSource.Accounts, newOwner, 0, big.NewInt(10000))
	txData = []byte(core.BuiltInFunctionClaimDeveloperRewards)
	tx = vm.CreateTransaction(0, big.NewInt(0), newOwner, scAddr, gasPrice, gasLimit, txData)

	// execute claim on source shard
	retCode, err = testContextSource.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSource.GetLatestError())

	expectedBalance = big.NewInt(9770)
	utils.TestAccount(t, testContextSource.Accounts, newOwner, 1, expectedBalance)

	accumulatedFees = testContextSource.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(230), accumulatedFees)

	developerFees = testContextSource.TxFeeHandler.GetDeveloperFees()
	require.Equal(t, big.NewInt(0), developerFees)

	intermediateTxs = testContextSource.GetIntermediateTransactions(t)
	testIndexer = vm.CreateTestIndexer(t, testContextSource.ShardCoordinator, testContextSource.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(23), indexerTx.GasUsed)
	require.Equal(t, "230", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	utils.CleanAccumulatedIntermediateTransactions(t, testContextDst)

	// execute claim on destination shard
	retCode, err = testContextDst.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextDst.GetLatestError())

	txs := testContextDst.GetIntermediateTransactions(t)
	scr := txs[0]

	testIndexer = vm.CreateTestIndexer(t, testContextDst.ShardCoordinator, testContextDst.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, txs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(500), indexerTx.GasUsed)
	require.Equal(t, "5000", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	utils.ProcessSCRResult(t, testContextSource, scr, vmcommon.Ok, nil)

	expectedBalance = big.NewInt(9771 + 376 + currentSCDevBalance.Int64())
	utils.TestAccount(t, testContextSource.Accounts, newOwner, 1, expectedBalance)

}
