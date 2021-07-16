// +build !race

package multiShard

import (
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/arwen"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/txsFee/utils"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	"github.com/stretchr/testify/require"
)

func TestRelayedSCDeployShouldWork(t *testing.T) {
	testContextRelayer, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(2, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextRelayer.Close()

	testContextInner, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContextInner.Close()

	relayerAddr := []byte("12345678901234567890123456789032")
	require.Equal(t, uint32(2), testContextRelayer.ShardCoordinator.ComputeId(relayerAddr))

	sndAddr := []byte("12345678901234567890123456789011")
	require.Equal(t, uint32(1), testContextRelayer.ShardCoordinator.ComputeId(sndAddr))

	gasPrice := uint64(10)
	gasLimit := uint64(1000)

	_, _ = vm.CreateAccount(testContextRelayer.Accounts, relayerAddr, 0, big.NewInt(50000))

	contractPath := "../../arwen/testdata/misc/fib_arwen/output/fib_arwen.wasm"
	scCode := arwen.GetSCCode(contractPath)
	userTx := vm.CreateTransaction(0, big.NewInt(0), sndAddr, vm.CreateEmptyAddress(), gasPrice, gasLimit, []byte(arwen.CreateDeployTxData(scCode)))

	rtxData := utils.PrepareRelayerTxData(userTx)
	rTxGasLimit := 1 + gasLimit + uint64(len(rtxData))
	rtx := vm.CreateTransaction(0, big.NewInt(0), relayerAddr, sndAddr, gasPrice, rTxGasLimit, rtxData)

	// execute on relayer shard
	retCode, err := testContextRelayer.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)

	_, err = testContextRelayer.Accounts.Commit()
	require.Nil(t, err)

	expectedBalanceRelayer := big.NewInt(26930)
	utils.TestAccount(t, testContextRelayer.Accounts, relayerAddr, 1, expectedBalanceRelayer)

	// check accumulated fees
	accumulatedFees := testContextRelayer.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(13070), accumulatedFees)

	intermediateTxs := testContextRelayer.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContextRelayer.ShardCoordinator, testContextRelayer.EconomicsData, false)
	testIndexer.SaveTransaction(rtx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(1307), indexerTx.GasUsed)
	require.Equal(t, "13070", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusPending.String(), indexerTx.Status)

	// execute on inner tx destination
	retCode, err = testContextInner.TxProcessor.ProcessTransaction(rtx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)

	_, err = testContextInner.Accounts.Commit()
	require.Nil(t, err)

	expectedBalanceRelayer = big.NewInt(0)
	utils.TestAccount(t, testContextInner.Accounts, sndAddr, 1, expectedBalanceRelayer)

	// check accumulated fees
	accumulatedFees = testContextInner.TxFeeHandler.GetAccumulatedFees()
	require.Equal(t, big.NewInt(8490), accumulatedFees)

	txs := testContextInner.GetIntermediateTransactions(t)

	testIndexer = vm.CreateTestIndexer(t, testContextInner.ShardCoordinator, testContextInner.EconomicsData, true)
	testIndexer.SaveTransaction(rtx, block.TxBlock, txs)

	indexerTx = testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, rtx.GasLimit, indexerTx.GasUsed)
	require.Equal(t, "23070", indexerTx.Fee)
	require.Equal(t, transaction.TxStatusSuccess.String(), indexerTx.Status)

	scr := txs[0]
	utils.ProcessSCRResult(t, testContextRelayer, scr, vmcommon.Ok, nil)

	expectedBalanceRelayer = big.NewInt(28440)
	utils.TestAccount(t, testContextRelayer.Accounts, relayerAddr, 1, expectedBalanceRelayer)
}
