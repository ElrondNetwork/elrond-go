package hardFork

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	arwenConfig "github.com/ElrondNetwork/arwen-wasm-vm/v1_4/config"
	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/genesis/process"
	"github.com/ElrondNetwork/elrond-go/integrationTests"
	"github.com/ElrondNetwork/elrond-go/integrationTests/mock"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/arwen"
	vmFactory "github.com/ElrondNetwork/elrond-go/process/factory"
	"github.com/ElrondNetwork/elrond-go/update/factory"
	"github.com/ElrondNetwork/elrond-go/vm/systemSmartContracts/defaults"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var log = logger.GetOrCreate("integrationTests/hardfork")

func TestHardForkWithoutTransactionInMultiShardedEnvironment(t *testing.T) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	exportBaseDirectory, err := ioutil.TempDir("", "export*")
	require.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(exportBaseDirectory)
	}()

	numOfShards := 1
	nodesPerShard := 1
	numMetachainNodes := 1

	genesisFile := "testdata/smartcontracts.json"
	nodes, hardforkTriggerNode := integrationTests.CreateNodesWithFullGenesis(
		numOfShards,
		nodesPerShard,
		numMetachainNodes,
		genesisFile,
	)

	roundsPerEpoch := uint64(10)
	for _, node := range nodes {
		node.EpochStartTrigger.SetRoundsPerEpoch(roundsPerEpoch)
		node.WaitTime = 100 * time.Millisecond
	}

	idxProposers := make([]int, numOfShards+1)
	for i := 0; i < numOfShards; i++ {
		idxProposers[i] = i * nodesPerShard
	}
	idxProposers[numOfShards] = numOfShards * nodesPerShard

	integrationTests.DisplayAndStartNodes(nodes)

	defer func() {
		for _, n := range nodes {
			_ = n.Messenger.Close()
		}

		_ = hardforkTriggerNode.Messenger.Close()
	}()

	round := uint64(0)
	nonce := uint64(0)
	round = integrationTests.IncrementAndPrintRound(round)
	nonce++

	time.Sleep(time.Second)

	nrRoundsToPropagateMultiShard := 5
	/////////----- wait for epoch end period
	nonce, round = integrationTests.WaitOperationToBeDone(t, nodes, int(roundsPerEpoch), nonce, round, idxProposers)

	time.Sleep(time.Second)

	nonce, _ = integrationTests.WaitOperationToBeDone(t, nodes, nrRoundsToPropagateMultiShard, nonce, round, idxProposers)

	time.Sleep(time.Second)

	epoch := uint32(1)
	verifyIfNodesHaveCorrectEpoch(t, epoch, nodes)
	verifyIfNodesHaveCorrectNonce(t, nonce-1, nodes)
	verifyIfAddedShardHeadersAreWithNewEpoch(t, nodes)

	log.Info("doing hardfork...")
	exportStorageConfigs := hardForkExport(t, nodes, epoch, exportBaseDirectory)
	for id, node := range nodes {
		node.ExportFolder = path.Join(exportBaseDirectory, fmt.Sprintf("%d", id))
	}
	hardForkImport(t, nodes, exportStorageConfigs)
	checkGenesisBlocksStateIsEqual(t, nodes)
}

func TestHardForkWithContinuousTransactionsInMultiShardedEnvironment(t *testing.T) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	exportBaseDirectory, err := ioutil.TempDir("", "export*")
	require.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(exportBaseDirectory)
	}()

	numOfShards := 1
	nodesPerShard := 1
	numMetachainNodes := 1

	genesisFile := "testdata/smartcontracts.json"
	nodes, hardforkTriggerNode := integrationTests.CreateNodesWithFullGenesis(
		numOfShards,
		nodesPerShard,
		numMetachainNodes,
		genesisFile,
	)

	roundsPerEpoch := uint64(10)
	for _, node := range nodes {
		node.EpochStartTrigger.SetRoundsPerEpoch(roundsPerEpoch)
	}

	idxProposers := make([]int, numOfShards+1)
	for i := 0; i < numOfShards; i++ {
		idxProposers[i] = i * nodesPerShard
	}
	idxProposers[numOfShards] = numOfShards * nodesPerShard

	integrationTests.DisplayAndStartNodes(nodes)

	defer func() {
		for _, n := range nodes {
			_ = n.Messenger.Close()
		}

		_ = hardforkTriggerNode.Messenger.Close()
	}()

	initialVal := big.NewInt(1000000000)
	sendValue := big.NewInt(5)
	integrationTests.MintAllNodes(nodes, initialVal)
	receiverAddress1 := []byte("12345678901234567890123456789012")
	receiverAddress2 := []byte("12345678901234567890123456789011")

	numPlayers := 10
	players := make([]*integrationTests.TestWalletAccount, numPlayers)
	for i := 0; i < numPlayers; i++ {
		players[i] = integrationTests.CreateTestWalletAccount(nodes[0].ShardCoordinator, 0)
	}
	integrationTests.MintAllPlayers(nodes, players, initialVal)

	round := uint64(0)
	nonce := uint64(0)
	round = integrationTests.IncrementAndPrintRound(round)
	nonce++

	time.Sleep(time.Second)
	transferToken := big.NewInt(10)
	ownerNode := nodes[0]
	initialSupply := "00" + hex.EncodeToString(big.NewInt(100000000000).Bytes())
	scCode := arwen.GetSCCode("../../vm/arwen/testdata/erc20-c-03/wrc20_arwen.wasm")
	scAddress, _ := ownerNode.BlockchainHook.NewAddress(ownerNode.OwnAccount.Address, ownerNode.OwnAccount.Nonce, vmFactory.ArwenVirtualMachine)
	integrationTests.CreateAndSendTransactionWithGasLimit(
		nodes[0],
		big.NewInt(0),
		integrationTests.MaxGasLimitPerBlock-1,
		make([]byte, 32),
		[]byte(arwen.CreateDeployTxData(scCode)+"@"+initialSupply),
		integrationTests.ChainID,
		integrationTests.MinTransactionVersion,
	)
	time.Sleep(time.Second)
	/////////----- wait for epoch end period
	epoch := uint32(2)
	nrRoundsToPropagateMultiShard := uint64(6)
	for i := uint64(0); i <= (uint64(epoch)*roundsPerEpoch)+nrRoundsToPropagateMultiShard; i++ {
		round, nonce = integrationTests.ProposeAndSyncOneBlock(t, nodes, idxProposers, round, nonce)
		integrationTests.AddSelfNotarizedHeaderByMetachain(nodes)
		for _, node := range nodes {
			integrationTests.CreateAndSendTransaction(node, nodes, sendValue, receiverAddress1, "", integrationTests.AdditionalGasLimit)
			integrationTests.CreateAndSendTransaction(node, nodes, sendValue, receiverAddress2, "", integrationTests.AdditionalGasLimit)
		}

		for _, player := range players {
			integrationTests.CreateAndSendTransactionWithGasLimit(
				ownerNode,
				big.NewInt(0),
				1000000,
				scAddress,
				[]byte("transferToken@"+hex.EncodeToString(player.Address)+"@00"+hex.EncodeToString(transferToken.Bytes())),
				integrationTests.ChainID,
				integrationTests.MinTransactionVersion,
			)
		}

		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)

	verifyIfNodesHaveCorrectEpoch(t, epoch, nodes)
	verifyIfNodesHaveCorrectNonce(t, nonce-1, nodes)
	verifyIfAddedShardHeadersAreWithNewEpoch(t, nodes)

	exportStorageConfigs := hardForkExport(t, nodes, epoch, exportBaseDirectory)
	for id, node := range nodes {
		node.ExportFolder = path.Join(exportBaseDirectory, fmt.Sprintf("%d", id))
	}

	hardForkImport(t, nodes, exportStorageConfigs)
	checkGenesisBlocksStateIsEqual(t, nodes)
}

func TestHardForkEarlyEndOfEpochWithContinuousTransactionsInMultiShardedEnvironment(t *testing.T) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	exportBaseDirectory, err := ioutil.TempDir("", "export*")
	require.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(exportBaseDirectory)
	}()

	numOfShards := 1
	nodesPerShard := 1
	numMetachainNodes := 1

	genesisFile := "testdata/smartcontracts.json"
	consensusNodes, hardforkTriggerNode := integrationTests.CreateNodesWithFullGenesis(
		numOfShards,
		nodesPerShard,
		numMetachainNodes,
		genesisFile,
	)
	allNodes := append(consensusNodes, hardforkTriggerNode)

	roundsPerEpoch := uint64(100)
	minRoundsPerEpoch := uint64(10)
	for _, node := range allNodes {
		node.EpochStartTrigger.SetRoundsPerEpoch(roundsPerEpoch)
		node.EpochStartTrigger.SetMinRoundsBetweenEpochs(minRoundsPerEpoch)
	}

	idxProposers := make([]int, numOfShards+1)
	for i := 0; i < numOfShards; i++ {
		idxProposers[i] = i * nodesPerShard
	}
	idxProposers[numOfShards] = numOfShards * nodesPerShard

	integrationTests.DisplayAndStartNodes(allNodes)

	defer func() {
		for _, n := range allNodes {
			_ = n.Messenger.Close()
		}
	}()

	initialVal := big.NewInt(1000000000)
	sendValue := big.NewInt(5)
	integrationTests.MintAllNodes(consensusNodes, initialVal)
	receiverAddress1 := []byte("12345678901234567890123456789012")
	receiverAddress2 := []byte("12345678901234567890123456789011")

	numPlayers := 10
	players := make([]*integrationTests.TestWalletAccount, numPlayers)
	for i := 0; i < numPlayers; i++ {
		players[i] = integrationTests.CreateTestWalletAccount(consensusNodes[0].ShardCoordinator, 0)
	}
	integrationTests.MintAllPlayers(consensusNodes, players, initialVal)

	round := uint64(0)
	nonce := uint64(0)
	round = integrationTests.IncrementAndPrintRound(round)
	nonce++

	time.Sleep(time.Second)
	transferToken := big.NewInt(10)
	ownerNode := consensusNodes[0]
	initialSupply := "00" + hex.EncodeToString(big.NewInt(100000000000).Bytes())
	scCode := arwen.GetSCCode("../../vm/arwen/testdata/erc20-c-03/wrc20_arwen.wasm")
	scAddress, _ := ownerNode.BlockchainHook.NewAddress(ownerNode.OwnAccount.Address, ownerNode.OwnAccount.Nonce, vmFactory.ArwenVirtualMachine)
	integrationTests.CreateAndSendTransactionWithGasLimit(
		consensusNodes[0],
		big.NewInt(0),
		integrationTests.MaxGasLimitPerBlock-1,
		make([]byte, 32),
		[]byte(arwen.CreateDeployTxData(scCode)+"@"+initialSupply),
		integrationTests.ChainID,
		integrationTests.MinTransactionVersion,
	)
	time.Sleep(time.Second)
	numRoundsBeforeHardfork := uint64(12)
	roundsForEarlyStartOfEpoch := uint64(3)
	for i := uint64(0); i < numRoundsBeforeHardfork+roundsForEarlyStartOfEpoch; i++ {
		if i == numRoundsBeforeHardfork {
			log.Info("triggering hardfork (with early end of epoch)")
			err = hardforkTriggerNode.Node.DirectTrigger(1, true)
			log.LogIfError(err)
		}

		round, nonce = integrationTests.ProposeAndSyncOneBlock(t, consensusNodes, idxProposers, round, nonce)
		integrationTests.AddSelfNotarizedHeaderByMetachain(consensusNodes)
		for _, node := range consensusNodes {
			integrationTests.CreateAndSendTransaction(node, allNodes, sendValue, receiverAddress1, "", integrationTests.AdditionalGasLimit)
			integrationTests.CreateAndSendTransaction(node, allNodes, sendValue, receiverAddress2, "", integrationTests.AdditionalGasLimit)
		}

		for _, player := range players {
			integrationTests.CreateAndSendTransactionWithGasLimit(
				ownerNode,
				big.NewInt(0),
				1000000,
				scAddress,
				[]byte("transferToken@"+hex.EncodeToString(player.Address)+"@00"+hex.EncodeToString(transferToken.Bytes())),
				integrationTests.ChainID,
				integrationTests.MinTransactionVersion,
			)
		}

		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	currentEpoch := uint32(1)

	exportStorageConfigs := hardForkExport(t, consensusNodes, currentEpoch, exportBaseDirectory)
	for id, node := range consensusNodes {
		node.ExportFolder = filepath.Join(exportBaseDirectory, fmt.Sprintf("%d", id))
	}

	hardForkImport(t, consensusNodes, exportStorageConfigs)
	checkGenesisBlocksStateIsEqual(t, consensusNodes)
}

func hardForkExport(t *testing.T, nodes []*integrationTests.TestProcessorNode, epoch uint32, exportBaseDirectory string) map[uint32][]config.StorageConfig {
	exportStorageConfigs := createHardForkExporter(t, nodes, exportBaseDirectory)
	for _, node := range nodes {
		log.Warn("***********************************************************************************")
		log.Warn("starting to export for node with shard",
			"id", node.ShardCoordinator.SelfId(),
			"base directory", exportBaseDirectory)
		err := node.ExportHandler.ExportAll(epoch)
		require.Nil(t, err)
		log.Warn("***********************************************************************************")
	}
	return exportStorageConfigs
}

func checkGenesisBlocksStateIsEqual(t *testing.T, nodes []*integrationTests.TestProcessorNode) {
	for _, nodeA := range nodes {
		for _, nodeB := range nodes {
			for _, genesisBlockA := range nodeA.GenesisBlocks {
				genesisBlockB := nodeB.GenesisBlocks[genesisBlockA.GetShardID()]
				assert.True(t, bytes.Equal(genesisBlockA.GetRootHash(), genesisBlockB.GetRootHash()))
			}
		}
	}

	for _, node := range nodes {
		for _, genesisBlock := range node.GenesisBlocks {
			if genesisBlock.GetShardID() == node.ShardCoordinator.SelfId() {
				rootHash, _ := node.AccntState.RootHash()
				assert.True(t, bytes.Equal(genesisBlock.GetRootHash(), rootHash))
			}
		}
	}
}

func hardForkImport(
	t *testing.T,
	nodes []*integrationTests.TestProcessorNode,
	importStorageConfigs map[uint32][]config.StorageConfig,
) {
	for _, node := range nodes {
		gasSchedule := arwenConfig.MakeGasMapForTests()
		defaults.FillGasMapInternal(gasSchedule, 1)
		log.Warn("started import process")

		coreComponents := integrationTests.GetDefaultCoreComponents()
		coreComponents.InternalMarshalizerField = integrationTests.TestMarshalizer
		coreComponents.TxMarshalizerField = integrationTests.TestMarshalizer
		coreComponents.HasherField = integrationTests.TestHasher
		coreComponents.Uint64ByteSliceConverterField = integrationTests.TestUint64Converter
		coreComponents.AddressPubKeyConverterField = integrationTests.TestAddressPubkeyConverter
		coreComponents.ValidatorPubKeyConverterField = integrationTests.TestValidatorPubkeyConverter
		coreComponents.ChainIdCalled = func() string {
			return string(node.ChainID)
		}
		coreComponents.MinTransactionVersionCalled = func() uint32 {
			return integrationTests.MinTransactionVersion
		}

		dataComponents := integrationTests.GetDefaultDataComponents()
		dataComponents.Store = node.Storage
		dataComponents.DataPool = node.DataPool
		dataComponents.BlockChain = node.BlockChain

		argsGenesis := process.ArgsGenesisBlockCreator{
			GenesisTime:       0,
			StartEpochNum:     100,
			Core:              coreComponents,
			Data:              dataComponents,
			Accounts:          node.AccntState,
			InitialNodesSetup: node.NodesSetup,
			Economics:         node.EconomicsData,
			ShardCoordinator:  node.ShardCoordinator,
			ValidatorAccounts: node.PeerState,
			GasSchedule:       mock.NewGasScheduleNotifierMock(gasSchedule),
			TxLogsProcessor:   &mock.TxLogsProcessorStub{},
			VirtualMachineConfig: config.VirtualMachineConfig{
				ArwenVersions: []config.ArwenVersionByEpoch{
					{StartEpoch: 0, Version: "*"},
				},
			},
			HardForkConfig: config.HardforkConfig{
				ImportFolder:             node.ExportFolder,
				StartEpoch:               100,
				StartNonce:               100,
				StartRound:               100,
				ImportStateStorageConfig: importStorageConfigs[node.ShardCoordinator.SelfId()][0],
				ImportKeysStorageConfig:  importStorageConfigs[node.ShardCoordinator.SelfId()][1],
				AfterHardFork:            true,
			},
			TrieStorageManagers: node.TrieStorageManagers,
			SystemSCConfig: config.SystemSmartContractsConfig{
				ESDTSystemSCConfig: config.ESDTSystemSCConfig{
					BaseIssuingCost: "1000",
					OwnerAddress:    "aaaaaa",
				},
				GovernanceSystemSCConfig: config.GovernanceSystemSCConfig{
					Active: config.GovernanceSystemSCConfigActive{
						ProposalCost:     "500",
						MinQuorum:        "50",
						MinPassThreshold: "50",
						MinVetoThreshold: "50",
					},
					FirstWhitelistedAddress: integrationTests.DelegationManagerConfigChangeAddress,
				},
				StakingSystemSCConfig: config.StakingSystemSCConfig{
					GenesisNodePrice:                     "1000",
					UnJailValue:                          "10",
					MinStepValue:                         "10",
					MinStakeValue:                        "1",
					UnBondPeriod:                         1,
					NumRoundsWithoutBleed:                1,
					MaximumPercentageToBleed:             1,
					BleedPercentagePerRound:              1,
					MaxNumberOfNodesForStake:             100,
					ActivateBLSPubKeyMessageVerification: false,
					MinUnstakeTokensValue:                "1",
				},
				DelegationManagerSystemSCConfig: config.DelegationManagerSystemSCConfig{
					MinCreationDeposit:  "100",
					MinStakeAmount:      "100",
					ConfigChangeAddress: integrationTests.DelegationManagerConfigChangeAddress,
				},
				DelegationSystemSCConfig: config.DelegationSystemSCConfig{
					MinServiceFee: 0,
					MaxServiceFee: 100,
				},
			},
			AccountsParser:      &mock.AccountsParserStub{},
			SmartContractParser: &mock.SmartContractParserStub{},
			BlockSignKeyGen:     &mock.KeyGenMock{},
			ImportStartHandler: &mock.ImportStartHandlerStub{
				ShouldStartImportCalled: func() bool {
					return true
				},
			},
			EpochConfig: &config.EpochConfig{
				EnableEpochs: config.EnableEpochs{
					BuiltInFunctionsEnableEpoch:        0,
					SCDeployEnableEpoch:                0,
					RelayedTransactionsEnableEpoch:     0,
					PenalizedTooMuchGasEnableEpoch:     0,
					StakingV2EnableEpoch:               1000000,
					StakeEnableEpoch:                   0,
					DelegationManagerEnableEpoch:       0,
					DelegationSmartContractEnableEpoch: 0,
				},
			},
		}

		genesisProcessor, err := process.NewGenesisBlockCreator(argsGenesis)
		require.Nil(t, err)
		genesisBlocks, err := genesisProcessor.CreateGenesisBlocks()
		require.Nil(t, err)
		require.NotNil(t, genesisBlocks)

		node.GenesisBlocks = genesisBlocks
		for _, genesisBlock := range genesisBlocks {
			log.Info("hardfork genesisblock roothash", "shardID", genesisBlock.GetShardID(), "rootHash", genesisBlock.GetRootHash())
			if node.ShardCoordinator.SelfId() == genesisBlock.GetShardID() {
				_ = node.BlockChain.SetGenesisHeader(genesisBlock)
				hash, _ := core.CalculateHash(integrationTests.TestMarshalizer, integrationTests.TestHasher, genesisBlock)
				node.BlockChain.SetGenesisHeaderHash(hash)
			}
		}
	}
}

func createHardForkExporter(
	t *testing.T,
	nodes []*integrationTests.TestProcessorNode,
	exportBaseDirectory string,
) map[uint32][]config.StorageConfig {
	returnedConfigs := make(map[uint32][]config.StorageConfig)

	for id, node := range nodes {
		accountsDBs := make(map[state.AccountsDbIdentifier]state.AccountsAdapter)
		accountsDBs[state.UserAccountsState] = node.AccntState
		accountsDBs[state.PeerAccountsState] = node.PeerState

		node.ExportFolder = path.Join(exportBaseDirectory, fmt.Sprintf("%d", id))
		exportConfig := config.StorageConfig{
			Cache: config.CacheConfig{
				Capacity: 100000,
				Type:     "LRU",
				Shards:   1,
			},
			DB: config.DBConfig{
				FilePath:          "ExportState",
				Type:              "LvlDBSerial",
				BatchDelaySeconds: 30,
				MaxBatchSize:      6,
				MaxOpenFiles:      10,
			},
		}
		keysConfig := config.StorageConfig{
			Cache: config.CacheConfig{
				Capacity: 100000,
				Type:     "LRU",
				Shards:   1,
			},
			DB: config.DBConfig{
				FilePath:          "ExportKeys",
				Type:              "LvlDBSerial",
				BatchDelaySeconds: 30,
				MaxBatchSize:      6,
				MaxOpenFiles:      10,
			},
		}

		returnedConfigs[node.ShardCoordinator.SelfId()] = append(returnedConfigs[node.ShardCoordinator.SelfId()], exportConfig)
		returnedConfigs[node.ShardCoordinator.SelfId()] = append(returnedConfigs[node.ShardCoordinator.SelfId()], keysConfig)

		coreComponents := integrationTests.GetDefaultCoreComponents()
		coreComponents.InternalMarshalizerField = integrationTests.TestMarshalizer
		coreComponents.TxMarshalizerField = integrationTests.TestTxSignMarshalizer
		coreComponents.HasherField = integrationTests.TestHasher
		coreComponents.TxSignHasherField = integrationTests.TestTxSignHasher
		coreComponents.Uint64ByteSliceConverterField = integrationTests.TestUint64Converter
		coreComponents.AddressPubKeyConverterField = integrationTests.TestAddressPubkeyConverter
		coreComponents.ValidatorPubKeyConverterField = integrationTests.TestValidatorPubkeyConverter
		coreComponents.ChainIdCalled = func() string {
			return string(node.ChainID)
		}

		cryptoComponents := integrationTests.GetDefaultCryptoComponents()
		cryptoComponents.BlockSig = node.OwnAccount.BlockSingleSigner
		cryptoComponents.TxSig = node.OwnAccount.SingleSigner
		cryptoComponents.MultiSig = node.MultiSigner
		cryptoComponents.BlKeyGen = node.OwnAccount.KeygenBlockSign
		cryptoComponents.TxKeyGen = node.OwnAccount.KeygenTxSign

		argsExportHandler := factory.ArgsExporter{
			CoreComponents:    coreComponents,
			CryptoComponents:  cryptoComponents,
			HeaderValidator:   node.HeaderValidator,
			DataPool:          node.DataPool,
			StorageService:    node.Storage,
			RequestHandler:    node.RequestHandler,
			ShardCoordinator:  node.ShardCoordinator,
			Messenger:         node.Messenger,
			ActiveAccountsDBs: accountsDBs,
			ExportFolder:      node.ExportFolder,
			ExportTriesStorageConfig: config.StorageConfig{
				Cache: config.CacheConfig{
					Capacity: 10000,
					Type:     "LRU",
					Shards:   1,
				},
				DB: config.DBConfig{
					FilePath:          "ExportTrie",
					Type:              "MemoryDB",
					BatchDelaySeconds: 30,
					MaxBatchSize:      6,
					MaxOpenFiles:      10,
				},
			},
			ExportStateStorageConfig: exportConfig,
			ExportStateKeysConfig:    keysConfig,
			MaxTrieLevelInMemory:     uint(5),
			WhiteListHandler:         node.WhiteListHandler,
			WhiteListerVerifiedTxs:   node.WhiteListerVerifiedTxs,
			InterceptorsContainer:    node.InterceptorsContainer,
			ExistingResolvers:        node.ResolversContainer,
			NodesCoordinator:         node.NodesCoordinator,
			HeaderSigVerifier:        node.HeaderSigVerifier,
			HeaderIntegrityVerifier:  node.HeaderIntegrityVerifier,
			ValidityAttester:         node.BlockTracker,
			OutputAntifloodHandler:   &mock.NilAntifloodHandler{},
			InputAntifloodHandler:    &mock.NilAntifloodHandler{},
			RoundHandler:             &mock.RoundHandlerMock{},
			InterceptorDebugConfig: config.InterceptorResolverDebugConfig{
				Enabled:                    true,
				EnablePrint:                true,
				CacheSize:                  10000,
				IntervalAutoPrintInSeconds: 20,
				NumRequestsThreshold:       3,
				NumResolveFailureThreshold: 3,
				DebugLineExpiration:        3,
			},
			MaxHardCapForMissingNodes: 500,
			NumConcurrentTrieSyncers:  50,
			TrieSyncerVersion:         2,
		}

		exportHandler, err := factory.NewExportHandlerFactory(argsExportHandler)
		assert.Nil(t, err)
		require.NotNil(t, exportHandler)

		node.ExportHandler, err = exportHandler.Create()
		assert.Nil(t, err)
		require.NotNil(t, node.ExportHandler)
	}

	return returnedConfigs
}

func verifyIfNodesHaveCorrectEpoch(
	t *testing.T,
	epoch uint32,
	nodes []*integrationTests.TestProcessorNode,
) {
	for _, node := range nodes {
		currentHeader := node.BlockChain.GetCurrentBlockHeader()
		assert.Equal(t, epoch, currentHeader.GetEpoch())
	}
}

func verifyIfNodesHaveCorrectNonce(
	t *testing.T,
	nonce uint64,
	nodes []*integrationTests.TestProcessorNode,
) {
	for _, node := range nodes {
		currentHeader := node.BlockChain.GetCurrentBlockHeader()
		assert.Equal(t, nonce, currentHeader.GetNonce())
	}
}

func verifyIfAddedShardHeadersAreWithNewEpoch(
	t *testing.T,
	nodes []*integrationTests.TestProcessorNode,
) {
	for _, node := range nodes {
		if node.ShardCoordinator.SelfId() != core.MetachainShardId {
			continue
		}

		currentMetaHdr, ok := node.BlockChain.GetCurrentBlockHeader().(*block.MetaBlock)
		if !ok {
			assert.Fail(t, "metablock should have been in current block header")
		}

		shardHDrStorage := node.Storage.GetStorer(dataRetriever.BlockHeaderUnit)
		for _, shardInfo := range currentMetaHdr.ShardInfo {
			header, err := node.DataPool.Headers().GetHeaderByHash(shardInfo.HeaderHash)
			if err == nil {
				assert.Equal(t, currentMetaHdr.GetEpoch(), header.GetEpoch())
				continue
			}

			buff, err := shardHDrStorage.Get(shardInfo.HeaderHash)
			assert.Nil(t, err)

			shardHeader := block.Header{}
			err = integrationTests.TestMarshalizer.Unmarshal(&shardHeader, buff)
			assert.Nil(t, err)
			assert.Equal(t, shardHeader.GetEpoch(), currentMetaHdr.GetEpoch())
		}
	}
}
