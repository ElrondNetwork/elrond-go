package process

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/forking"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	dataBlock "github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/genesis"
	"github.com/ElrondNetwork/elrond-go/genesis/process/disabled"
	"github.com/ElrondNetwork/elrond-go/genesis/process/intermediate"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/block/preprocess"
	"github.com/ElrondNetwork/elrond-go/process/coordinator"
	"github.com/ElrondNetwork/elrond-go/process/factory/shard"
	"github.com/ElrondNetwork/elrond-go/process/rewardTransaction"
	"github.com/ElrondNetwork/elrond-go/process/smartContract"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/builtInFunctions"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/hooks"
	"github.com/ElrondNetwork/elrond-go/process/transaction"
	"github.com/ElrondNetwork/elrond-go/storage/txcache"
	"github.com/ElrondNetwork/elrond-go/update"
	hardForkProcess "github.com/ElrondNetwork/elrond-go/update/process"
	vmcommonBuiltInFunctions "github.com/ElrondNetwork/elrond-vm-common/builtInFunctions"
	"github.com/ElrondNetwork/elrond-vm-common/parsers"
)

var log = logger.GetOrCreate("genesis/process")

var zero = big.NewInt(0)

type deployedScMetrics struct {
	numDelegation int
	numOtherTypes int
}

func createGenesisConfig() config.EnableEpochs {
	return config.EnableEpochs{
		BuiltInFunctionsEnableEpoch:            0,
		SCDeployEnableEpoch:                    unreachableEpoch,
		RelayedTransactionsEnableEpoch:         0,
		PenalizedTooMuchGasEnableEpoch:         0,
		AheadOfTimeGasUsageEnableEpoch:         unreachableEpoch,
		BelowSignedThresholdEnableEpoch:        unreachableEpoch,
		GasPriceModifierEnableEpoch:            unreachableEpoch,
		MetaProtectionEnableEpoch:              unreachableEpoch,
		TransactionSignedWithTxHashEnableEpoch: unreachableEpoch,
		SwitchHysteresisForMinNodesEnableEpoch: unreachableEpoch,
		SwitchJailWaitingEnableEpoch:           unreachableEpoch,
		BlockGasAndFeesReCheckEnableEpoch:      unreachableEpoch,
		RelayedTransactionsV2EnableEpoch:       unreachableEpoch,

		IncrementSCRNonceInMultiTransferEnableEpoch: unreachableEpoch,
	}
}

// CreateShardGenesisBlock will create a shard genesis block
func CreateShardGenesisBlock(
	arg ArgsGenesisBlockCreator,
	body *block.Body,
	nodesListSplitter genesis.NodesListSplitter,
	hardForkBlockProcessor update.HardForkBlockProcessor,
) (data.HeaderHandler, [][]byte, error) {
	if mustDoHardForkImportProcess(arg) {
		return createShardGenesisBlockAfterHardFork(arg, body, hardForkBlockProcessor)
	}

	processors, err := createProcessorsForShardGenesisBlock(arg, createGenesisConfig())
	if err != nil {
		return nil, nil, err
	}

	deployMetrics := &deployedScMetrics{}

	scAddresses, err := deployInitialSmartContracts(processors, arg, deployMetrics)
	if err != nil {
		return nil, nil, err
	}

	numSetBalances, err := setBalancesToTrie(arg)
	if err != nil {
		return nil, nil, fmt.Errorf("%w encountered when creating genesis block for shard %d while setting the balances to trie",
			err, arg.ShardCoordinator.SelfId())
	}

	numStaked, err := increaseStakersNonces(processors, arg)
	if err != nil {
		return nil, nil, fmt.Errorf("%w encountered when creating genesis block for shard %d while incrementing nonces",
			err, arg.ShardCoordinator.SelfId())
	}

	delegationResult, err := executeDelegation(processors, arg, nodesListSplitter)
	if err != nil {
		return nil, nil, fmt.Errorf("%w encountered when creating genesis block for shard %d while execution delegation",
			err, arg.ShardCoordinator.SelfId())
	}

	numCrossShardDelegations, err := incrementNoncesForCrossShardDelegations(processors, arg)
	if err != nil {
		return nil, nil, fmt.Errorf("%w encountered when creating genesis block for shard %d while incrementing crossshard nonce",
			err, arg.ShardCoordinator.SelfId())
	}

	rootHash, err := arg.Accounts.Commit()
	if err != nil {
		return nil, nil, fmt.Errorf("%w encountered when creating genesis block for shard %d while commiting",
			err, arg.ShardCoordinator.SelfId())
	}

	log.Debug("shard block genesis",
		"shard ID", arg.ShardCoordinator.SelfId(),
		"num delegation SC deployed", deployMetrics.numDelegation,
		"num other SC deployed", deployMetrics.numOtherTypes,
		"num set balances", numSetBalances,
		"num staked directly", numStaked,
		"total staked on a delegation SC", delegationResult.NumTotalStaked,
		"total delegation nodes", delegationResult.NumTotalDelegated,
		"cross shard delegation calls", numCrossShardDelegations,
	)

	round, nonce, epoch := getGenesisBlocksRoundNonceEpoch(arg)
	header := &block.Header{
		Epoch:           epoch,
		Round:           round,
		Nonce:           nonce,
		ShardID:         arg.ShardCoordinator.SelfId(),
		BlockBodyType:   block.StateBlock,
		PubKeysBitmap:   []byte{1},
		Signature:       rootHash,
		RootHash:        rootHash,
		PrevRandSeed:    rootHash,
		RandSeed:        rootHash,
		TimeStamp:       arg.GenesisTime,
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
		ChainID:         []byte(arg.Core.ChainID()),
		SoftwareVersion: []byte(""),
	}

	err = processors.vmContainer.Close()
	if err != nil {
		return nil, nil, err
	}

	err = processors.vmContainersFactory.Close()
	if err != nil {
		return nil, nil, err
	}

	return header, scAddresses, nil
}

func createShardGenesisBlockAfterHardFork(
	arg ArgsGenesisBlockCreator,
	body *block.Body,
	hardForkBlockProcessor update.HardForkBlockProcessor,
) (data.HeaderHandler, [][]byte, error) {
	if check.IfNil(hardForkBlockProcessor) {
		return nil, nil, update.ErrNilHardForkBlockProcessor
	}

	hdrHandler, err := hardForkBlockProcessor.CreateBlock(
		body,
		arg.Core.ChainID(),
		arg.HardForkConfig.StartRound,
		arg.HardForkConfig.StartNonce,
		arg.HardForkConfig.StartEpoch,
	)
	if err != nil {
		return nil, nil, err
	}
	hdrHandler.SetTimeStamp(arg.GenesisTime)

	err = arg.Accounts.RecreateTrie(hdrHandler.GetRootHash())
	if err != nil {
		return nil, nil, err
	}

	return hdrHandler, make([][]byte, 0), nil
}

func createArgsShardBlockCreatorAfterHardFork(
	arg ArgsGenesisBlockCreator,
	selfShardID uint32,
) (hardForkProcess.ArgsNewShardBlockCreatorAfterHardFork, error) {
	tmpArg := arg
	tmpArg.Accounts = arg.importHandler.GetAccountsDBForShard(arg.ShardCoordinator.SelfId())
	processors, err := createProcessorsForShardGenesisBlock(tmpArg, arg.EpochConfig.EnableEpochs)
	if err != nil {
		return hardForkProcess.ArgsNewShardBlockCreatorAfterHardFork{}, err
	}

	argsPendingTxProcessor := hardForkProcess.ArgsPendingTransactionProcessor{
		Accounts:         tmpArg.Accounts,
		TxProcessor:      processors.txProcessor,
		RwdTxProcessor:   processors.rwdProcessor,
		ScrTxProcessor:   processors.scrProcessor,
		PubKeyConv:       arg.Core.AddressPubKeyConverter(),
		ShardCoordinator: arg.ShardCoordinator,
	}
	pendingTxProcessor, err := hardForkProcess.NewPendingTransactionProcessor(argsPendingTxProcessor)
	if err != nil {
		return hardForkProcess.ArgsNewShardBlockCreatorAfterHardFork{}, err
	}

	argsShardBlockCreatorAfterHardFork := hardForkProcess.ArgsNewShardBlockCreatorAfterHardFork{
		Hasher:             arg.Core.Hasher(),
		ImportHandler:      arg.importHandler,
		Marshalizer:        arg.Core.InternalMarshalizer(),
		PendingTxProcessor: pendingTxProcessor,
		ShardCoordinator:   arg.ShardCoordinator,
		Storage:            arg.Data.StorageService(),
		TxCoordinator:      processors.txCoordinator,
		SelfShardID:        selfShardID,
	}

	return argsShardBlockCreatorAfterHardFork, nil
}

// setBalancesToTrie adds balances to trie
func setBalancesToTrie(arg ArgsGenesisBlockCreator) (int, error) {
	initialAccounts, err := arg.AccountsParser.InitialAccountsSplitOnAddressesShards(arg.ShardCoordinator)
	if err != nil {
		return 0, err
	}

	initialAccountsForShard := initialAccounts[arg.ShardCoordinator.SelfId()]

	for _, accnt := range initialAccountsForShard {
		err = setBalanceToTrie(arg, accnt)
		if err != nil {
			return 0, err
		}
	}

	return len(initialAccountsForShard), nil
}

func setBalanceToTrie(arg ArgsGenesisBlockCreator, accnt genesis.InitialAccountHandler) error {
	accWrp, err := arg.Accounts.LoadAccount(accnt.AddressBytes())
	if err != nil {
		return err
	}

	account, ok := accWrp.(state.UserAccountHandler)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	err = account.AddToBalance(accnt.GetBalanceValue())
	if err != nil {
		return err
	}

	return arg.Accounts.SaveAccount(account)
}

func createProcessorsForShardGenesisBlock(arg ArgsGenesisBlockCreator, enableEpochs config.EnableEpochs) (*genesisProcessors, error) {
	genesisArwenLocker := &sync.RWMutex{} // use a local instance as to not run in concurrent issues when doing bootstrap
	epochNotifier := forking.NewGenericEpochNotifier()

	argsBuiltIn := builtInFunctions.ArgsCreateBuiltInFunctionContainer{
		GasSchedule:                  arg.GasSchedule,
		MapDNSAddresses:              make(map[string]struct{}),
		EnableUserNameChange:         false,
		Marshalizer:                  arg.Core.InternalMarshalizer(),
		Accounts:                     arg.Accounts,
		ShardCoordinator:             arg.ShardCoordinator,
		EpochNotifier:                epochNotifier,
		ESDTMultiTransferEnableEpoch: unreachableEpoch,
	}
	builtInFuncs, err := builtInFunctions.CreateBuiltInFunctionContainer(argsBuiltIn)
	if err != nil {
		return nil, err
	}

	argsHook := hooks.ArgBlockChainHook{
		Accounts:           arg.Accounts,
		PubkeyConv:         arg.Core.AddressPubKeyConverter(),
		StorageService:     arg.Data.StorageService(),
		BlockChain:         arg.Data.Blockchain(),
		ShardCoordinator:   arg.ShardCoordinator,
		Marshalizer:        arg.Core.InternalMarshalizer(),
		Uint64Converter:    arg.Core.Uint64ByteSliceConverter(),
		BuiltInFunctions:   builtInFuncs,
		DataPool:           arg.Data.Datapool(),
		CompiledSCPool:     arg.Data.Datapool().SmartContracts(),
		NilCompiledSCStore: true,
	}
	esdtTransferParser, err := parsers.NewESDTTransferParser(arg.Core.InternalMarshalizer())
	if err != nil {
		return nil, err
	}

	argsNewVMFactory := shard.ArgVMContainerFactory{
		Config:                         arg.VirtualMachineConfig,
		BlockGasLimit:                  math.MaxUint64,
		GasSchedule:                    arg.GasSchedule,
		ArgBlockChainHook:              argsHook,
		EpochNotifier:                  epochNotifier,
		DeployEnableEpoch:              arg.EpochConfig.EnableEpochs.SCDeployEnableEpoch,
		AheadOfTimeGasUsageEnableEpoch: arg.EpochConfig.EnableEpochs.AheadOfTimeGasUsageEnableEpoch,
		ArwenV3EnableEpoch:             arg.EpochConfig.EnableEpochs.RepairCallbackEnableEpoch,
		ArwenChangeLocker:              genesisArwenLocker,
		ESDTTransferParser:             esdtTransferParser,
	}
	log.Debug("shardGenesisCreator: enable epoch for sc deploy", "epoch", argsNewVMFactory.DeployEnableEpoch)
	log.Debug("shardGenesisCreator: enable epoch for ahead of time gas usage", "epoch", argsNewVMFactory.AheadOfTimeGasUsageEnableEpoch)
	log.Debug("shardGenesisCreator: enable epoch for repair callback", "epoch", argsNewVMFactory.ArwenV3EnableEpoch)

	vmFactoryImpl, err := shard.NewVMContainerFactory(argsNewVMFactory)
	if err != nil {
		return nil, err
	}

	vmContainer, err := vmFactoryImpl.Create()
	if err != nil {
		return nil, err
	}

	err = vmcommonBuiltInFunctions.SetPayableHandler(builtInFuncs, vmFactoryImpl.BlockChainHookImpl())
	if err != nil {
		return nil, err
	}

	genesisFeeHandler := &disabled.FeeHandler{}
	interimProcFactory, err := shard.NewIntermediateProcessorsContainerFactory(
		arg.ShardCoordinator,
		arg.Core.InternalMarshalizer(),
		arg.Core.Hasher(),
		arg.Core.AddressPubKeyConverter(),
		arg.Data.StorageService(),
		arg.Data.Datapool(),
		genesisFeeHandler,
	)
	if err != nil {
		return nil, err
	}

	interimProcContainer, err := interimProcFactory.Create()
	if err != nil {
		return nil, err
	}

	scForwarder, err := interimProcContainer.Get(dataBlock.SmartContractResultBlock)
	if err != nil {
		return nil, err
	}

	receiptTxInterim, err := interimProcContainer.Get(dataBlock.ReceiptBlock)
	if err != nil {
		return nil, err
	}

	badTxInterim, err := interimProcContainer.Get(dataBlock.InvalidBlock)
	if err != nil {
		return nil, err
	}

	temporaryBlock := &dataBlock.Header{
		Epoch:     arg.StartEpochNum,
		TimeStamp: arg.GenesisTime,
	}
	epochNotifier.CheckEpoch(temporaryBlock)

	argsTxTypeHandler := coordinator.ArgNewTxTypeHandler{
		PubkeyConverter:        arg.Core.AddressPubKeyConverter(),
		ShardCoordinator:       arg.ShardCoordinator,
		BuiltInFunctions:       builtInFuncs,
		ArgumentParser:         parsers.NewCallArgsParser(),
		EpochNotifier:          epochNotifier,
		RelayedTxV2EnableEpoch: arg.EpochConfig.EnableEpochs.RelayedTransactionsV2EnableEpoch,
		ESDTTransferParser:     esdtTransferParser,
	}
	txTypeHandler, err := coordinator.NewTxTypeHandler(argsTxTypeHandler)
	if err != nil {
		return nil, err
	}

	gasHandler, err := preprocess.NewGasComputation(arg.Economics, txTypeHandler, epochNotifier, enableEpochs.SCDeployEnableEpoch)
	if err != nil {
		return nil, err
	}

	argsNewScProcessor := smartContract.ArgsNewSmartContractProcessor{
		VmContainer:                         vmContainer,
		ArgsParser:                          smartContract.NewArgumentParser(),
		Hasher:                              arg.Core.Hasher(),
		Marshalizer:                         arg.Core.InternalMarshalizer(),
		AccountsDB:                          arg.Accounts,
		BlockChainHook:                      vmFactoryImpl.BlockChainHookImpl(),
		PubkeyConv:                          arg.Core.AddressPubKeyConverter(),
		ShardCoordinator:                    arg.ShardCoordinator,
		ScrForwarder:                        scForwarder,
		TxFeeHandler:                        genesisFeeHandler,
		EconomicsFee:                        genesisFeeHandler,
		TxTypeHandler:                       txTypeHandler,
		GasHandler:                          gasHandler,
		GasSchedule:                         arg.GasSchedule,
		TxLogsProcessor:                     arg.TxLogsProcessor,
		BadTxForwarder:                      badTxInterim,
		EpochNotifier:                       epochNotifier,
		BuiltinEnableEpoch:                  enableEpochs.BuiltInFunctionsEnableEpoch,
		DeployEnableEpoch:                   enableEpochs.SCDeployEnableEpoch,
		PenalizedTooMuchGasEnableEpoch:      enableEpochs.PenalizedTooMuchGasEnableEpoch,
		RepairCallbackEnableEpoch:           enableEpochs.RepairCallbackEnableEpoch,
		ReturnDataToLastTransferEnableEpoch: enableEpochs.ReturnDataToLastTransferEnableEpoch,
		SenderInOutTransferEnableEpoch:      enableEpochs.SenderInOutTransferEnableEpoch,
		IsGenesisProcessing:                 true,
		StakingV2EnableEpoch:                arg.EpochConfig.EnableEpochs.StakingV2EnableEpoch,
		VMOutputCacher:                      txcache.NewDisabledCache(),
		ArwenChangeLocker:                   genesisArwenLocker,

		IncrementSCRNonceInMultiTransferEnableEpoch: enableEpochs.IncrementSCRNonceInMultiTransferEnableEpoch,
	}
	scProcessor, err := smartContract.NewSmartContractProcessor(argsNewScProcessor)
	if err != nil {
		return nil, err
	}

	rewardsTxProcessor, err := rewardTransaction.NewRewardTxProcessor(
		arg.Accounts,
		arg.Core.AddressPubKeyConverter(),
		arg.ShardCoordinator,
	)
	if err != nil {
		return nil, err
	}

	argsNewTxProcessor := transaction.ArgsNewTxProcessor{
		Accounts:                       arg.Accounts,
		Hasher:                         arg.Core.Hasher(),
		PubkeyConv:                     arg.Core.AddressPubKeyConverter(),
		Marshalizer:                    arg.Core.InternalMarshalizer(),
		SignMarshalizer:                arg.Core.TxMarshalizer(),
		ShardCoordinator:               arg.ShardCoordinator,
		ScProcessor:                    scProcessor,
		TxFeeHandler:                   genesisFeeHandler,
		TxTypeHandler:                  txTypeHandler,
		EconomicsFee:                   genesisFeeHandler,
		ReceiptForwarder:               receiptTxInterim,
		BadTxForwarder:                 badTxInterim,
		ArgsParser:                     smartContract.NewArgumentParser(),
		ScrForwarder:                   scForwarder,
		EpochNotifier:                  epochNotifier,
		RelayedTxEnableEpoch:           enableEpochs.RelayedTransactionsEnableEpoch,
		PenalizedTooMuchGasEnableEpoch: enableEpochs.PenalizedTooMuchGasEnableEpoch,
		MetaProtectionEnableEpoch:      enableEpochs.MetaProtectionEnableEpoch,
		RelayedTxV2EnableEpoch:         enableEpochs.RelayedTransactionsV2EnableEpoch,
	}
	transactionProcessor, err := transaction.NewTxProcessor(argsNewTxProcessor)
	if err != nil {
		return nil, errors.New("could not create transaction statisticsProcessor: " + err.Error())
	}

	disabledRequestHandler := &disabled.RequestHandler{}
	disabledBlockTracker := &disabled.BlockTracker{}
	disabledBlockSizeComputationHandler := &disabled.BlockSizeComputationHandler{}
	disabledBalanceComputationHandler := &disabled.BalanceComputationHandler{}

	preProcFactory, err := shard.NewPreProcessorsContainerFactory(
		arg.ShardCoordinator,
		arg.Data.StorageService(),
		arg.Core.InternalMarshalizer(),
		arg.Core.Hasher(),
		arg.Data.Datapool(),
		arg.Core.AddressPubKeyConverter(),
		arg.Accounts,
		disabledRequestHandler,
		transactionProcessor,
		scProcessor,
		scProcessor,
		rewardsTxProcessor,
		arg.Economics,
		gasHandler,
		disabledBlockTracker,
		disabledBlockSizeComputationHandler,
		disabledBalanceComputationHandler,
	)
	if err != nil {
		return nil, err
	}

	preProcContainer, err := preProcFactory.Create()
	if err != nil {
		return nil, err
	}

	argsTransactionCoordinator := coordinator.ArgTransactionCoordinator{
		Hasher:                            arg.Core.Hasher(),
		Marshalizer:                       arg.Core.InternalMarshalizer(),
		ShardCoordinator:                  arg.ShardCoordinator,
		Accounts:                          arg.Accounts,
		MiniBlockPool:                     arg.Data.Datapool().MiniBlocks(),
		RequestHandler:                    disabledRequestHandler,
		PreProcessors:                     preProcContainer,
		InterProcessors:                   interimProcContainer,
		GasHandler:                        gasHandler,
		FeeHandler:                        genesisFeeHandler,
		BlockSizeComputation:              disabledBlockSizeComputationHandler,
		BalanceComputation:                disabledBalanceComputationHandler,
		EconomicsFee:                      genesisFeeHandler,
		TxTypeHandler:                     txTypeHandler,
		BlockGasAndFeesReCheckEnableEpoch: enableEpochs.BlockGasAndFeesReCheckEnableEpoch,
		TransactionsLogProcessor:          arg.TxLogsProcessor,
	}
	txCoordinator, err := coordinator.NewTransactionCoordinator(argsTransactionCoordinator)
	if err != nil {
		return nil, err
	}

	argsNewSCQueryService := smartContract.ArgsNewSCQueryService{
		VmContainer:       vmContainer,
		EconomicsFee:      arg.Economics,
		BlockChainHook:    vmFactoryImpl.BlockChainHookImpl(),
		BlockChain:        arg.Data.Blockchain(),
		ArwenChangeLocker: genesisArwenLocker,
	}
	queryService, err := smartContract.NewSCQueryService(argsNewSCQueryService)
	if err != nil {
		return nil, err
	}

	return &genesisProcessors{
		txCoordinator:       txCoordinator,
		systemSCs:           nil,
		txProcessor:         transactionProcessor,
		scProcessor:         scProcessor,
		scrProcessor:        scProcessor,
		rwdProcessor:        rewardsTxProcessor,
		blockchainHook:      vmFactoryImpl.BlockChainHookImpl(),
		queryService:        queryService,
		vmContainersFactory: vmFactoryImpl,
		vmContainer:         vmContainer,
	}, nil
}

func deployInitialSmartContracts(
	processors *genesisProcessors,
	arg ArgsGenesisBlockCreator,
	deployMetrics *deployedScMetrics,
) ([][]byte, error) {
	smartContracts, err := arg.SmartContractParser.InitialSmartContractsSplitOnOwnersShards(arg.ShardCoordinator)
	if err != nil {
		return nil, err
	}

	var scAddresses = make([][]byte, 0)
	currentShardSmartContracts := smartContracts[arg.ShardCoordinator.SelfId()]
	for _, sc := range currentShardSmartContracts {
		var scResulted [][]byte
		scResulted, err = deployInitialSmartContract(processors, sc, arg, deployMetrics)
		if err != nil {
			return nil, fmt.Errorf("%w for owner %s and filename %s",
				err, sc.GetOwner(), sc.GetFilename())
		}

		scAddresses = append(scAddresses, scResulted...)
	}

	return scAddresses, nil
}

func deployInitialSmartContract(
	processors *genesisProcessors,
	sc genesis.InitialSmartContractHandler,
	arg ArgsGenesisBlockCreator,
	deployMetrics *deployedScMetrics,
) ([][]byte, error) {

	txExecutor, err := intermediate.NewTxExecutionProcessor(processors.txProcessor, arg.Accounts)
	if err != nil {
		return nil, err
	}

	var deployProc genesis.DeployProcessor

	switch sc.GetType() {
	case genesis.DNSType:
		deployMetrics.numOtherTypes++
		argDeployLibrary := intermediate.ArgDeployLibrarySC{
			Executor:         txExecutor,
			PubkeyConv:       arg.Core.AddressPubKeyConverter(),
			BlockchainHook:   processors.blockchainHook,
			ShardCoordinator: arg.ShardCoordinator,
		}
		deployProc, err = intermediate.NewDeployLibrarySC(argDeployLibrary)
		if err != nil {
			return nil, err
		}
	case genesis.DelegationType:
		deployMetrics.numDelegation++
		fallthrough
	default:
		argDeploy := intermediate.ArgDeployProcessor{
			Executor:       txExecutor,
			PubkeyConv:     arg.Core.AddressPubKeyConverter(),
			BlockchainHook: processors.blockchainHook,
			QueryService:   processors.queryService,
		}
		deployProc, err = intermediate.NewDeployProcessor(argDeploy)
		if err != nil {
			return nil, err
		}
	}

	return deployProc.Deploy(sc)
}

func increaseStakersNonces(processors *genesisProcessors, arg ArgsGenesisBlockCreator) (int, error) {
	txExecutor, err := intermediate.NewTxExecutionProcessor(processors.txProcessor, arg.Accounts)
	if err != nil {
		return 0, err
	}

	initialAddresses, err := arg.AccountsParser.InitialAccountsSplitOnAddressesShards(arg.ShardCoordinator)
	if err != nil {
		return 0, err
	}

	stakersCounter := 0
	initalAddressesInCurrentShard := initialAddresses[arg.ShardCoordinator.SelfId()]
	for _, ia := range initalAddressesInCurrentShard {
		if ia.GetStakingValue().Cmp(zero) < 1 {
			continue
		}

		numNodesStaked := big.NewInt(0).Set(ia.GetStakingValue())
		numNodesStaked.Div(numNodesStaked, arg.GenesisNodePrice)

		stakersCounter++
		err = txExecutor.AddNonce(ia.AddressBytes(), numNodesStaked.Uint64())
		if err != nil {
			return 0, fmt.Errorf("%w when adding nonce for address %s", err, ia.GetAddress())
		}
	}

	return stakersCounter, nil
}

func executeDelegation(
	processors *genesisProcessors,
	arg ArgsGenesisBlockCreator,
	nodesListSplitter genesis.NodesListSplitter,
) (genesis.DelegationResult, error) {
	txExecutor, err := intermediate.NewTxExecutionProcessor(processors.txProcessor, arg.Accounts)
	if err != nil {
		return genesis.DelegationResult{}, err
	}

	argDP := intermediate.ArgStandardDelegationProcessor{
		Executor:            txExecutor,
		ShardCoordinator:    arg.ShardCoordinator,
		AccountsParser:      arg.AccountsParser,
		SmartContractParser: arg.SmartContractParser,
		NodesListSplitter:   nodesListSplitter,
		QueryService:        processors.queryService,
		NodePrice:           arg.GenesisNodePrice,
	}

	delegationProcessor, err := intermediate.NewStandardDelegationProcessor(argDP)
	if err != nil {
		return genesis.DelegationResult{}, err
	}

	return delegationProcessor.ExecuteDelegation()
}

func incrementNoncesForCrossShardDelegations(processors *genesisProcessors, arg ArgsGenesisBlockCreator) (int, error) {
	txExecutor, err := intermediate.NewTxExecutionProcessor(processors.txProcessor, arg.Accounts)
	if err != nil {
		return 0, err
	}

	initialAddresses, err := arg.AccountsParser.InitialAccountsSplitOnAddressesShards(arg.ShardCoordinator)
	if err != nil {
		return 0, err
	}

	counter := 0
	initalAddressesInCurrentShard := initialAddresses[arg.ShardCoordinator.SelfId()]
	for _, ia := range initalAddressesInCurrentShard {
		dh := ia.GetDelegationHandler()
		if check.IfNil(dh) {
			continue
		}
		if arg.ShardCoordinator.SameShard(ia.AddressBytes(), dh.AddressBytes()) {
			continue
		}

		counter++
		err = txExecutor.AddNonce(ia.AddressBytes(), 1)
		if err != nil {
			return 0, fmt.Errorf("%w when adding nonce for address %s", err, ia.GetAddress())
		}
	}

	return counter, nil
}
