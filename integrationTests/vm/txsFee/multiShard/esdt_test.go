package multiShard

import (
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/txsFee/utils"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	"github.com/stretchr/testify/require"
)

func TestESDTTransferShouldWork(t *testing.T) {
	testContext, err := vm.CreatePreparedTxProcessorWithVMsMultiShard(1, vm.ArgEnableEpoch{})
	require.Nil(t, err)
	defer testContext.Close()

	sndAddr := []byte("12345678901234567890123456789010")
	require.Equal(t, uint32(0), testContext.ShardCoordinator.ComputeId(sndAddr))

	rcvAddr := []byte("12345678901234567890123456789011")
	require.Equal(t, uint32(1), testContext.ShardCoordinator.ComputeId(rcvAddr))

	egldBalance := big.NewInt(100000000)
	esdtBalance := big.NewInt(100000000)
	token := []byte("miiutoken")
	utils.CreateAccountWithESDTBalance(t, testContext.Accounts, sndAddr, egldBalance, token, esdtBalance)

	gasPrice := uint64(10)
	gasLimit := uint64(40)
	tx := utils.CreateESDTTransferTx(0, sndAddr, rcvAddr, token, big.NewInt(100), gasPrice, gasLimit)
	retCode, err := testContext.TxProcessor.ProcessTransaction(tx)
	require.Equal(t, vmcommon.Ok, retCode)
	require.Nil(t, err)
	require.Nil(t, testContext.GetLatestError())

	_, err = testContext.Accounts.Commit()
	require.Nil(t, err)

	expectedReceiverBalance := big.NewInt(100)
	utils.CheckESDTBalance(t, testContext, rcvAddr, token, expectedReceiverBalance)

	intermediateTxs := testContext.GetIntermediateTransactions(t)
	testIndexer := vm.CreateTestIndexer(t, testContext.ShardCoordinator, testContext.EconomicsData, true)
	testIndexer.SaveTransaction(tx, block.TxBlock, intermediateTxs)

	indexerTx := testIndexer.GetIndexerPreparedTransaction(t)
	require.Equal(t, uint64(36), indexerTx.GasUsed)
	require.Equal(t, "360", indexerTx.Fee)
}
