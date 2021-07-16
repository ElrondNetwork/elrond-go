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

func TestRelayedMoveBalanceRelayerShard0InnerTxSenderAndReceiverShard1ShouldWork(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	relayerAddr := []byte("12345678901234567890123456789030")
	shardID := testContext.ShardCoordinator.ComputeId(relayerAddr)
	require.Equal(t, uint32(0), shardID)

	sndAddr := []byte("12345678901234567890123456789011")
	shardID = testContext.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(1), shardID)

	rcvAddr := []byte("12345678901234567890123456789021")
	shardID = testContext.ShardCoordinator.ComputeId(rcvAddr)
	require.Equal(t, uint32(1), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(100)

	userTx := vm.CreateTransaction(0, big.NewInt(100), sndAddr, rcvAddr, gasPrice, gasLimit, []byte("aaaa"))

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, userTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, err := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	// check balance inner tx sender
	utils.TestAccount(t, testContext.Accounts, sndAddr, 1, big.NewInt(0))

	// check balance inner tx receiver
	utils.TestAccount(t, testContext.Accounts, rcvAddr, 0, big.NewInt(100))

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(1000), accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "2750", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)
}

func TestRelayedMoveBalanceRelayerAndInnerTxSenderShard0ReceiverShard1(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	relayerAddr := []byte("12345678901234567890123456789030")
	shardID := testContext.ShardCoordinator.ComputeId(relayerAddr)
	require.Equal(t, uint32(0), shardID)

	sndAddr := []byte("12345678901234567890123456789011")
	shardID = testContext.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(1), shardID)

	scAddress := "00000000000000000000dbb53e4b23392b0d6f36cce32deb2d623e9625ab3132"
	scAddrBytes, _ := hex.DecodeString(scAddress)
	scAddrBytes[31] = 1
	shardID = testContext.ShardCoordinator.ComputeId(scAddrBytes)
	require.Equal(t, uint32(1), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(100)

	userTx := vm.CreateTransaction(0, big.NewInt(100), sndAddr, scAddrBytes, gasPrice, gasLimit, nil)

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, userTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	retCode, err := testContext.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.UserError, retCode)
	require.Nil(t, err)
	require.Nil(t, testContext.GetLatestError())

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	// check inner tx receiver
	account, err := testContext.Accounts.GetExistingAccount(scAddrBytes)
	require.Nil(t, account)
	require.NotNil(t, err)

	// check accumulated fees
	accumulatedFees := testContext.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(1000), accumulatedFees)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "2630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)
}

func TestRelayedMoveBalanceExecuteOnSourceAndDestination(t *testing.T) {
	testContextSource, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSource.Close()

	testContextDst, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextDst.Close()

	relayerAddr := []byte("12345678901234567890123456789030")
	shardID := testContextSource.ShardCoordinator.ComputeId(relayerAddr)
	require.Equal(t, uint32(0), shardID)

	sndAddr := []byte("12345678901234567890123456789011")
	shardID = testContextSource.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(1), shardID)

	scAddress := "00000000000000000000dbb53e4b23392b0d6f36cce32deb2d623e9625ab3132"
	scAddrBytes, _ := hex.DecodeString(scAddress)
	scAddrBytes[31] = 1
	shardID = testContextSource.ShardCoordinator.ComputeId(scAddrBytes)
	require.Equal(t, uint32(1), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(100)

	_, _ = vm.CreateAccount(testContextSource.Accounts, relayerAddr, 0, big.NewInt(100000))

	userTx := vm.CreateTransaction(0, big.NewInt(100), sndAddr, scAddrBytes, gasPrice, gasLimit, nil)

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, userTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	// execute on source shard
	retCode, err := testContextSource.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSource.GetLatestError())

	// check relayed balance
	utils.TestAccount(t, testContextSource.Accounts, relayerAddr, 1, big.NewInt(97270))

	// check accumulated fees
	accumulatedFees := testContextSource.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(1630), accumulatedFees)

	intermediateTxs := testContextSource.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContextSource.ShardCoordinator, testContextSource.EconomicsData, false)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(163), indexerTx.GasUsed)
	require.Equal(t, "1630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	// execute on destination shard
	retCode, err = testContextDst.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.UserError, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextDst.GetLatestError())

	_, err = testContextDst.Accounts.Commit()
	require.Nil(t, err)

	// check inner tx receiver
	account, err := testContextDst.Accounts.GetExistingAccount(scAddrBytes)
	require.Nil(t, account)
	require.NotNil(t, err)

	// check accumulated fees
	accumulatedFees = testContextDst.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(1000), accumulatedFees)

	intermediateTxs = testContextDst.GetIntermediateTransactions(t)
	testIndexer = vm.CreateTestIndexer(t, testContextDst.ShardCoordinator, testContextDst.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "2630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)
}

func TestRelayedMoveBalanceExecuteOnSourceAndDestinationRelayerAndInnerTxSenderShard0InnerTxReceiverShard1ShouldWork(t *testing.T) {
	testContextSource, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSource.Close()

	testContextDst, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextDst.Close()

	relayerAddr := []byte("12345678901234567890123456789030")
	shardID := testContextSource.ShardCoordinator.ComputeId(relayerAddr)
	require.Equal(t, uint32(0), shardID)

	sndAddr := []byte("12345678901234567890123456789010")
	shardID = testContextSource.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(0), shardID)

	rcvAddr := []byte("12345678901234567890123456789011")
	shardID = testContextSource.ShardCoordinator.ComputeId(rcvAddr)
	require.Equal(t, uint32(1), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(100)

	_, _ = vm.CreateAccount(testContextSource.Accounts, relayerAddr, 0, big.NewInt(100000))

	userTx := vm.CreateTransaction(0, big.NewInt(100), sndAddr, rcvAddr, gasPrice, gasLimit, nil)

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, userTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	// execute on source shard
	retCode, err := testContextSource.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSource.GetLatestError())

	// check relayed balance
	utils.TestAccount(t, testContextSource.Accounts, relayerAddr, 1, big.NewInt(97270))
	// check inner tx sender
	utils.TestAccount(t, testContextSource.Accounts, sndAddr, 1, big.NewInt(0))

	// check accumulated fees
	accumulatedFees := testContextSource.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(2630), accumulatedFees)

	intermediateTxs := testContextSource.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContextSource.ShardCoordinator, testContextSource.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "2630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	// get scr for destination shard
	txs := testContextSource.GetIntermediateTransactions(t)
	scr := txs[0]

	utils.ProcessSCRResult(t, testContextDst, scr, vmcommon.Ok, nil)

	// check balance receiver
	utils.TestAccount(t, testContextDst.Accounts, rcvAddr, 0, big.NewInt(100))

	// check accumulated fess
	accumulatedFees = testContextDst.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(0), accumulatedFees)
}

func TestRelayedMoveBalanceRelayerAndInnerTxReceiverShard0SenderShard1(t *testing.T) {
	testContextSource, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextSource.Close()

	testContextDst, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextDst.Close()

	relayerAddr := []byte("12345678901234567890123456789030")
	shardID := testContextSource.ShardCoordinator.ComputeId(relayerAddr)
	require.Equal(t, uint32(0), shardID)

	sndAddr := []byte("12345678901234567890123456789011")
	shardID = testContextSource.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(1), shardID)

	rcvAddr := []byte("12345678901234567890123456789010")
	shardID = testContextSource.ShardCoordinator.ComputeId(rcvAddr)
	require.Equal(t, uint32(0), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(100)

	_, _ = vm.CreateAccount(testContextSource.Accounts, relayerAddr, 0, big.NewInt(100000))

	innerTx := vm.CreateTransaction(0, big.NewInt(100), sndAddr, rcvAddr, gasPrice, gasLimit, nil)

	rtxData := utils.PrepareRelayerTxData(innerTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, innerTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	// execute on relayer shard
	retCode, err := testContextSource.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextSource.GetLatestError())

	// check relayed balance
	utils.TestAccount(t, testContextSource.Accounts, relayerAddr, 1, big.NewInt(97270))

	// check inner Tx receiver
	innerTxSenderAccount, err := testContextSource.Accounts.GetExistingAccount(sndAddr)
	require.Nil(t, innerTxSenderAccount)
	require.NotNil(t, err)

	//check accumulated fees
	accumulatedFees := testContextSource.TxFeeHandler.GetAccumulatedFees()
	expectedAccFees := big.NewInt(1630)
	require.Equal(t, expectedAccFees, accumulatedFees)

	intermediateTxs := testContextSource.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContextSource.ShardCoordinator, testContextSource.EconomicsData, false)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(163), indexerTx.GasUsed)
	require.Equal(t, "1630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	// execute on destination shard
	retCode, err = testContextDst.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextDst.GetLatestError())

	utils.TestAccount(t, testContextDst.Accounts, sndAddr, 1, big.NewInt(0))

	//check accumulated fees
	accumulatedFees = testContextDst.TxFeeHandler.GetAccumulatedFees()
	expectedAccFees = big.NewInt(1000)
	require.Equal(t, expectedAccFees, accumulatedFees)

	txs := testContextDst.GetIntermediateTransactions(t)
	scr := txs[0]

	testIndexer = vm.CreateTestIndexer(t, testContextDst.ShardCoordinator, testContextDst.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, txs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "2630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	// execute generated SCR from shard1 on shard 0
	utils.ProcessSCRResult(t, testContextSource, scr, vmcommon.Ok, nil)

	// check receiver balance
	utils.TestAccount(t, testContextSource.Accounts, rcvAddr, 0, big.NewInt(100))
}

func TestMoveBalanceRelayerShard0InnerTxSenderShard1InnerTxReceiverShard2ShouldWork(t *testing.T) {
	testContextRelayer, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(0, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextRelayer.Close()

	testContextInnerSource, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextInnerSource.Close()

	testContextDst, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(2, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextDst.Close()

	relayerAddr := []byte("12345678901234567890123456789030")
	shardID := testContextRelayer.ShardCoordinator.ComputeId(relayerAddr)
	require.Equal(t, uint32(0), shardID)

	sndAddr := []byte("12345678901234567890123456789011")
	shardID = testContextRelayer.ShardCoordinator.ComputeId(sndAddr)
	require.Equal(t, uint32(1), shardID)

	rcvAddr := []byte("12345678901234567890123456789012")
	shardID = testContextRelayer.ShardCoordinator.ComputeId(rcvAddr)
	require.Equal(t, uint32(2), shardID)

	gasPrice := uint64(10)
	gasLimit := uint64(100)

	_, _ = vm.CreateAccount(testContextRelayer.Accounts, relayerAddr, 0, big.NewInt(100000))

	innerTx := vm.CreateTransaction(0, big.NewInt(100), sndAddr, rcvAddr, gasPrice, gasLimit, nil)

	rtxData := utils.PrepareRelayerTxData(innerTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, innerTx.Value, relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	// execute on relayer shard
	retCode, err := testContextRelayer.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextRelayer.GetLatestError())

	// check relayed balance
	utils.TestAccount(t, testContextRelayer.Accounts, relayerAddr, 1, big.NewInt(97270))

	// check inner Tx receiver
	innerTxSenderAccount, err := testContextRelayer.Accounts.GetExistingAccount(sndAddr)
	require.Nil(t, innerTxSenderAccount)
	require.NotNil(t, err)

	//check accumulated fees
	accumulatedFees := testContextRelayer.TxFeeHandler.GetAccumulatedFees()
	expectedAccFees := big.NewInt(1630)
	require.Equal(t, expectedAccFees, accumulatedFees)

	intermediateTxs := testContextRelayer.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContextRelayer.ShardCoordinator, testContextRelayer.EconomicsData, false)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(163), indexerTx.GasUsed)
	require.Equal(t, "1630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	// execute on inner tx sender shard
	retCode, err = testContextInnerSource.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContextInnerSource.GetLatestError())

	utils.TestAccount(t, testContextInnerSource.Accounts, sndAddr, 1, big.NewInt(0))

	//check accumulated fees
	accumulatedFees = testContextInnerSource.TxFeeHandler.GetAccumulatedFees()
	expectedAccFees = big.NewInt(1000)
	require.Equal(t, expectedAccFees, accumulatedFees)

	// execute on inner tx receiver shard
	txs := testContextInnerSource.GetIntermediateTransactions(t)
	scr := txs[0]

	testIndexer = vm.CreateTestIndexer(t, testContextInnerSource.ShardCoordinator, testContextInnerSource.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, txs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "2630", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	utils.ProcessSCRResult(t, testContextDst, scr, vmcommon.Ok, nil)

	// check receiver balance
	utils.TestAccount(t, testContextDst.Accounts, rcvAddr, 0, big.NewInt(100))
}
