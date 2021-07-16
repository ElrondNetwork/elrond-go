package factory

import (
	"errors"
	"fmt"

	"github.com/ElrondNetwork/elrond-go/core"
	dataBlock "github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/epochStart"
	"github.com/ElrondNetwork/elrond-go/epochStart/bootstrap/disabled"
	metachainEpochStart "github.com/ElrondNetwork/elrond-go/epochStart/metachain"
	"github.com/ElrondNetwork/elrond-go/genesis"
	processDisabled "github.com/ElrondNetwork/elrond-go/genesis/process/disabled"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/block"
	"github.com/ElrondNetwork/elrond-go/process/block/postprocess"
	"github.com/ElrondNetwork/elrond-go/process/block/preprocess"
	"github.com/ElrondNetwork/elrond-go/process/coordinator"
	"github.com/ElrondNetwork/elrond-go/process/factory"
	"github.com/ElrondNetwork/elrond-go/process/factory/metachain"
	"github.com/ElrondNetwork/elrond-go/process/factory/shard"
	"github.com/ElrondNetwork/elrond-go/process/rewardTransaction"
	"github.com/ElrondNetwork/elrond-go/process/scToProtocol"
	"github.com/ElrondNetwork/elrond-go/process/smartContract"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/builtInFunctions"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/hooks"
	"github.com/ElrondNetwork/elrond-go/process/throttle"
	"github.com/ElrondNetwork/elrond-go/process/transaction"
	"github.com/ElrondNetwork/elrond-go/process/txsimulator"
	"github.com/ElrondNetwork/elrond-go/storage/txcache"
	"github.com/ElrondNetwork/elrond-go/vm"
	vmcommonBuiltInFunctions "github.com/ElrondNetwork/elrond-vm-common/builtInFunctions"
	"github.com/ElrondNetwork/elrond-vm-common/parsers"
)

func (pcf *processComponentsFactory) newBlockProcessor(
	requestHandler process.RequestHandler,
	forkDetector process.ForkDetector,
	epochStartTrigger epochStart.TriggerHandler,
	bootStorer process.BootStorer,
	validatorStatisticsProcessor process.ValidatorStatisticsProcessor,
	headerValidator process.HeaderConstructionValidator,
	blockTracker process.BlockTracker,
	pendingMiniBlocksHandler process.PendingMiniBlocksHandler,
	txSimulatorProcessorArgs *txsimulator.ArgsTxSimulator,
	arwenChangeLocker process.Locker,
) (process.BlockProcessor, error) {
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() < pcf.bootstrapComponents.ShardCoordinator().NumberOfShards() {
		return pcf.newShardBlockProcessor(
			requestHandler,
			forkDetector,
			epochStartTrigger,
			bootStorer,
			headerValidator,
			blockTracker,
			pcf.smartContractParser,
			txSimulatorProcessorArgs,
			arwenChangeLocker,
		)
	}
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		return pcf.newMetaBlockProcessor(
			requestHandler,
			forkDetector,
			validatorStatisticsProcessor,
			epochStartTrigger,
			bootStorer,
			headerValidator,
			blockTracker,
			pendingMiniBlocksHandler,
			txSimulatorProcessorArgs,
			arwenChangeLocker,
		)
	}

	return nil, errors.New("could not create block processor")
}

func (pcf *processComponentsFactory) newShardBlockProcessor(
	requestHandler process.RequestHandler,
	forkDetector process.ForkDetector,
	epochStartTrigger epochStart.TriggerHandler,
	bootStorer process.BootStorer,
	headerValidator process.HeaderConstructionValidator,
	blockTracker process.BlockTracker,
	smartContractParser genesis.InitialSmartContractParser,
	txSimulatorProcessorArgs *txsimulator.ArgsTxSimulator,
	arwenChangeLocker process.Locker,
) (process.BlockProcessor, error) {
	argsParser := smartContract.NewArgumentParser()

	mapDNSAddresses, err := smartContractParser.GetDeployedSCAddresses(genesis.DNSType)
	if err != nil {
		return nil, err
	}

	argsBuiltIn := builtInFunctions.ArgsCreateBuiltInFunctionContainer{
		GasSchedule:                  pcf.gasSchedule,
		MapDNSAddresses:              mapDNSAddresses,
		Marshalizer:                  pcf.coreData.InternalMarshalizer(),
		Accounts:                     pcf.state.AccountsAdapter(),
		ShardCoordinator:             pcf.bootstrapComponents.ShardCoordinator(),
		EpochNotifier:                pcf.epochNotifier,
		ESDTMultiTransferEnableEpoch: pcf.epochConfig.EnableEpochs.ESDTMultiTransferEnableEpoch,
	}

	builtInFuncs, err := builtInFunctions.CreateBuiltInFunctionContainer(argsBuiltIn)
	if err != nil {
		return nil, err
	}

	argsHook := hooks.ArgBlockChainHook{
		Accounts:           pcf.state.AccountsAdapter(),
		PubkeyConv:         pcf.coreData.AddressPubKeyConverter(),
		StorageService:     pcf.data.StorageService(),
		BlockChain:         pcf.data.Blockchain(),
		ShardCoordinator:   pcf.bootstrapComponents.ShardCoordinator(),
		Marshalizer:        pcf.coreData.InternalMarshalizer(),
		Uint64Converter:    pcf.coreData.Uint64ByteSliceConverter(),
		BuiltInFunctions:   builtInFuncs,
		DataPool:           pcf.data.Datapool(),
		CompiledSCPool:     pcf.data.Datapool().SmartContracts(),
		WorkingDir:         pcf.workingDir,
		NilCompiledSCStore: false,
		ConfigSCStorage:    pcf.config.SmartContractsStorage,
	}

	esdtTransferParser, err := parsers.NewESDTTransferParser(pcf.coreData.InternalMarshalizer())
	if err != nil {
		return nil, err
	}

	argsNewVMFactory := shard.ArgVMContainerFactory{
		Config:                         pcf.config.VirtualMachine.Execution,
		BlockGasLimit:                  pcf.coreData.EconomicsData().MaxGasLimitPerBlock(pcf.bootstrapComponents.ShardCoordinator().SelfId()),
		GasSchedule:                    pcf.gasSchedule,
		ArgBlockChainHook:              argsHook,
		EpochNotifier:                  pcf.coreData.EpochNotifier(),
		DeployEnableEpoch:              pcf.epochConfig.EnableEpochs.SCDeployEnableEpoch,
		AheadOfTimeGasUsageEnableEpoch: pcf.epochConfig.EnableEpochs.AheadOfTimeGasUsageEnableEpoch,
		ArwenV3EnableEpoch:             pcf.epochConfig.EnableEpochs.RepairCallbackEnableEpoch,
		ArwenChangeLocker:              arwenChangeLocker,
		ESDTTransferParser:             esdtTransferParser,
	}
	log.Debug("blockProcessorCreator: enable epoch for sc deploy", "epoch", argsNewVMFactory.DeployEnableEpoch)
	log.Debug("blockProcessorCreator: enable epoch for ahead of time gas usage", "epoch", argsNewVMFactory.AheadOfTimeGasUsageEnableEpoch)
	log.Debug("blockProcessorCreator: enable epoch for repair callback", "epoch", argsNewVMFactory.ArwenV3EnableEpoch)

	vmFactory, err := shard.NewVMContainerFactory(argsNewVMFactory)
	if err != nil {
		return nil, err
	}

	vmContainer, err := vmFactory.Create()
	if err != nil {
		return nil, err
	}

	err = vmcommonBuiltInFunctions.SetPayableHandler(builtInFuncs, vmFactory.BlockChainHookImpl())
	if err != nil {
		return nil, err
	}

	interimProcFactory, err := shard.NewIntermediateProcessorsContainerFactory(
		pcf.bootstrapComponents.ShardCoordinator(),
		pcf.coreData.InternalMarshalizer(),
		pcf.coreData.Hasher(),
		pcf.coreData.AddressPubKeyConverter(),
		pcf.data.StorageService(),
		pcf.data.Datapool(),
		pcf.coreData.EconomicsData(),
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

	argsTxTypeHandler := coordinator.ArgNewTxTypeHandler{
		PubkeyConverter:        pcf.coreData.AddressPubKeyConverter(),
		ShardCoordinator:       pcf.bootstrapComponents.ShardCoordinator(),
		BuiltInFunctions:       builtInFuncs,
		ArgumentParser:         parsers.NewCallArgsParser(),
		EpochNotifier:          pcf.coreData.EpochNotifier(),
		RelayedTxV2EnableEpoch: pcf.epochConfig.EnableEpochs.RelayedTransactionsV2EnableEpoch,
		ESDTTransferParser:     esdtTransferParser,
	}
	txTypeHandler, err := coordinator.NewTxTypeHandler(argsTxTypeHandler)
	if err != nil {
		return nil, err
	}

	gasHandler, err := preprocess.NewGasComputation(
		pcf.coreData.EconomicsData(),
		txTypeHandler,
		pcf.coreData.EpochNotifier(),
		pcf.epochConfig.EnableEpochs.SCDeployEnableEpoch,
	)
	if err != nil {
		return nil, err
	}

	txFeeHandler, err := postprocess.NewFeeAccumulator()
	if err != nil {
		return nil, err
	}

	enableEpochs := pcf.epochConfig.EnableEpochs

	argsNewScProcessor := smartContract.ArgsNewSmartContractProcessor{
		VmContainer:                         vmContainer,
		ArgsParser:                          argsParser,
		Hasher:                              pcf.coreData.Hasher(),
		Marshalizer:                         pcf.coreData.InternalMarshalizer(),
		AccountsDB:                          pcf.state.AccountsAdapter(),
		BlockChainHook:                      vmFactory.BlockChainHookImpl(),
		PubkeyConv:                          pcf.coreData.AddressPubKeyConverter(),
		ShardCoordinator:                    pcf.bootstrapComponents.ShardCoordinator(),
		ScrForwarder:                        scForwarder,
		TxFeeHandler:                        txFeeHandler,
		EconomicsFee:                        pcf.coreData.EconomicsData(),
		GasHandler:                          gasHandler,
		GasSchedule:                         pcf.gasSchedule,
		TxLogsProcessor:                     pcf.txLogsProcessor,
		TxTypeHandler:                       txTypeHandler,
		DeployEnableEpoch:                   enableEpochs.SCDeployEnableEpoch,
		BuiltinEnableEpoch:                  enableEpochs.BuiltInFunctionsEnableEpoch,
		PenalizedTooMuchGasEnableEpoch:      enableEpochs.PenalizedTooMuchGasEnableEpoch,
		RepairCallbackEnableEpoch:           pcf.epochConfig.EnableEpochs.RepairCallbackEnableEpoch,
		IsGenesisProcessing:                 false,
		ReturnDataToLastTransferEnableEpoch: pcf.epochConfig.EnableEpochs.ReturnDataToLastTransferEnableEpoch,
		SenderInOutTransferEnableEpoch:      pcf.epochConfig.EnableEpochs.SenderInOutTransferEnableEpoch,
		BadTxForwarder:                      badTxInterim,
		EpochNotifier:                       pcf.epochNotifier,
		StakingV2EnableEpoch:                pcf.epochConfig.EnableEpochs.StakingV2EnableEpoch,
		VMOutputCacher:                      txcache.NewDisabledCache(),
		ArwenChangeLocker:                   arwenChangeLocker,

		IncrementSCRNonceInMultiTransferEnableEpoch: enableEpochs.IncrementSCRNonceInMultiTransferEnableEpoch,
	}
	scProcessor, err := smartContract.NewSmartContractProcessor(argsNewScProcessor)
	if err != nil {
		return nil, err
	}

	rewardsTxProcessor, err := rewardTransaction.NewRewardTxProcessor(
		pcf.state.AccountsAdapter(),
		pcf.coreData.AddressPubKeyConverter(),
		pcf.bootstrapComponents.ShardCoordinator(),
	)
	if err != nil {
		return nil, err
	}

	argsNewTxProcessor := transaction.ArgsNewTxProcessor{
		Accounts:                       pcf.state.AccountsAdapter(),
		Hasher:                         pcf.coreData.Hasher(),
		PubkeyConv:                     pcf.coreData.AddressPubKeyConverter(),
		Marshalizer:                    pcf.coreData.InternalMarshalizer(),
		SignMarshalizer:                pcf.coreData.TxMarshalizer(),
		ShardCoordinator:               pcf.bootstrapComponents.ShardCoordinator(),
		ScProcessor:                    scProcessor,
		TxFeeHandler:                   txFeeHandler,
		TxTypeHandler:                  txTypeHandler,
		EconomicsFee:                   pcf.coreData.EconomicsData(),
		ReceiptForwarder:               receiptTxInterim,
		BadTxForwarder:                 badTxInterim,
		ArgsParser:                     argsParser,
		ScrForwarder:                   scForwarder,
		RelayedTxEnableEpoch:           enableEpochs.RelayedTransactionsEnableEpoch,
		PenalizedTooMuchGasEnableEpoch: enableEpochs.PenalizedTooMuchGasEnableEpoch,
		MetaProtectionEnableEpoch:      enableEpochs.MetaProtectionEnableEpoch,
		EpochNotifier:                  pcf.epochNotifier,
		RelayedTxV2EnableEpoch:         enableEpochs.RelayedTransactionsV2EnableEpoch,
	}
	transactionProcessor, err := transaction.NewTxProcessor(argsNewTxProcessor)
	if err != nil {
		return nil, errors.New("could not create transaction statisticsProcessor: " + err.Error())
	}

	err = pcf.createShardTxSimulatorProcessor(txSimulatorProcessorArgs, argsNewScProcessor, argsNewTxProcessor)
	if err != nil {
		return nil, err
	}

	blockSizeThrottler, err := throttle.NewBlockSizeThrottle(
		pcf.config.BlockSizeThrottleConfig.MinSizeInBytes,
		pcf.config.BlockSizeThrottleConfig.MaxSizeInBytes,
	)
	if err != nil {
		return nil, err
	}

	blockSizeComputationHandler, err := preprocess.NewBlockSizeComputation(
		pcf.coreData.InternalMarshalizer(),
		blockSizeThrottler,
		pcf.config.BlockSizeThrottleConfig.MaxSizeInBytes,
	)
	if err != nil {
		return nil, err
	}

	balanceComputationHandler, err := preprocess.NewBalanceComputation()
	if err != nil {
		return nil, err
	}

	preProcFactory, err := shard.NewPreProcessorsContainerFactory(
		pcf.bootstrapComponents.ShardCoordinator(),
		pcf.data.StorageService(),
		pcf.coreData.InternalMarshalizer(),
		pcf.coreData.Hasher(),
		pcf.data.Datapool(),
		pcf.coreData.AddressPubKeyConverter(),
		pcf.state.AccountsAdapter(),
		requestHandler,
		transactionProcessor,
		scProcessor,
		scProcessor,
		rewardsTxProcessor,
		pcf.coreData.EconomicsData(),
		gasHandler,
		blockTracker,
		blockSizeComputationHandler,
		balanceComputationHandler,
	)
	if err != nil {
		return nil, err
	}

	preProcContainer, err := preProcFactory.Create()
	if err != nil {
		return nil, err
	}

	argsTransactionCoordinator := coordinator.ArgTransactionCoordinator{
		Hasher:                            pcf.coreData.Hasher(),
		Marshalizer:                       pcf.coreData.InternalMarshalizer(),
		ShardCoordinator:                  pcf.bootstrapComponents.ShardCoordinator(),
		Accounts:                          pcf.state.AccountsAdapter(),
		MiniBlockPool:                     pcf.data.Datapool().MiniBlocks(),
		RequestHandler:                    requestHandler,
		PreProcessors:                     preProcContainer,
		InterProcessors:                   interimProcContainer,
		GasHandler:                        gasHandler,
		FeeHandler:                        txFeeHandler,
		BlockSizeComputation:              blockSizeComputationHandler,
		BalanceComputation:                balanceComputationHandler,
		EconomicsFee:                      pcf.coreData.EconomicsData(),
		TxTypeHandler:                     txTypeHandler,
		BlockGasAndFeesReCheckEnableEpoch: pcf.epochConfig.EnableEpochs.BlockGasAndFeesReCheckEnableEpoch,
		TransactionsLogProcessor:          pcf.txLogsProcessor,
	}
	txCoordinator, err := coordinator.NewTransactionCoordinator(argsTransactionCoordinator)
	if err != nil {
		return nil, err
	}

	accountsDb := make(map[state.AccountsDbIdentifier]state.AccountsAdapter)
	accountsDb[state.UserAccountsState] = pcf.state.AccountsAdapter()

	argumentsBaseProcessor := block.ArgBaseProcessor{
		CoreComponents:      pcf.coreData,
		DataComponents:      pcf.data,
		BootstrapComponents: pcf.bootstrapComponents,
		StatusComponents:    pcf.statusComponents,
		Config:              pcf.config,
		Version:             pcf.version,
		AccountsDB:          accountsDb,
		ForkDetector:        forkDetector,
		NodesCoordinator:    pcf.nodesCoordinator,
		RequestHandler:      requestHandler,
		BlockChainHook:      vmFactory.BlockChainHookImpl(),
		TxCoordinator:       txCoordinator,
		EpochStartTrigger:   epochStartTrigger,
		HeaderValidator:     headerValidator,
		BootStorer:          bootStorer,
		BlockTracker:        blockTracker,
		FeeHandler:          txFeeHandler,
		BlockSizeThrottler:  blockSizeThrottler,
		HistoryRepository:   pcf.historyRepo,
		EpochNotifier:       pcf.epochNotifier,
		VMContainersFactory: vmFactory,
		VmContainer:         vmContainer,
	}
	arguments := block.ArgShardProcessor{
		ArgBaseProcessor: argumentsBaseProcessor,
	}

	blockProcessor, err := block.NewShardProcessor(arguments)
	if err != nil {
		return nil, errors.New("could not create block statisticsProcessor: " + err.Error())
	}

	return blockProcessor, nil
}

func (pcf *processComponentsFactory) newMetaBlockProcessor(
	requestHandler process.RequestHandler,
	forkDetector process.ForkDetector,
	validatorStatisticsProcessor process.ValidatorStatisticsProcessor,
	epochStartTrigger epochStart.TriggerHandler,
	bootStorer process.BootStorer,
	headerValidator process.HeaderConstructionValidator,
	blockTracker process.BlockTracker,
	pendingMiniBlocksHandler process.PendingMiniBlocksHandler,
	txSimulatorProcessorArgs *txsimulator.ArgsTxSimulator,
	arwenChangeLocker process.Locker,
) (process.BlockProcessor, error) {

	argsBuiltIn := builtInFunctions.ArgsCreateBuiltInFunctionContainer{
		GasSchedule:                  pcf.gasSchedule,
		MapDNSAddresses:              make(map[string]struct{}), // no dns for meta
		Marshalizer:                  pcf.coreData.InternalMarshalizer(),
		Accounts:                     pcf.state.AccountsAdapter(),
		ShardCoordinator:             pcf.bootstrapComponents.ShardCoordinator(),
		EpochNotifier:                pcf.epochNotifier,
		ESDTMultiTransferEnableEpoch: pcf.epochConfig.EnableEpochs.ESDTMultiTransferEnableEpoch,
	}
	builtInFuncs, err := builtInFunctions.CreateBuiltInFunctionContainer(argsBuiltIn)
	if err != nil {
		return nil, err
	}

	argsHook := hooks.ArgBlockChainHook{
		Accounts:           pcf.state.AccountsAdapter(),
		PubkeyConv:         pcf.coreData.AddressPubKeyConverter(),
		StorageService:     pcf.data.StorageService(),
		BlockChain:         pcf.data.Blockchain(),
		ShardCoordinator:   pcf.bootstrapComponents.ShardCoordinator(),
		Marshalizer:        pcf.coreData.InternalMarshalizer(),
		Uint64Converter:    pcf.coreData.Uint64ByteSliceConverter(),
		BuiltInFunctions:   builtInFuncs,
		DataPool:           pcf.data.Datapool(),
		CompiledSCPool:     pcf.data.Datapool().SmartContracts(),
		ConfigSCStorage:    pcf.config.SmartContractsStorage,
		WorkingDir:         pcf.workingDir,
		NilCompiledSCStore: false,
	}

	argsNewVMContainer := metachain.ArgsNewVMContainerFactory{
		ArgBlockChainHook:   argsHook,
		Economics:           pcf.coreData.EconomicsData(),
		MessageSignVerifier: pcf.crypto.MessageSignVerifier(),
		GasSchedule:         pcf.gasSchedule,
		NodesConfigProvider: pcf.coreData.GenesisNodesSetup(),
		Hasher:              pcf.coreData.Hasher(),
		Marshalizer:         pcf.coreData.InternalMarshalizer(),
		SystemSCConfig:      pcf.systemSCConfig,
		ValidatorAccountsDB: pcf.state.PeerAccounts(),
		ChanceComputer:      pcf.coreData.Rater(),
		EpochNotifier:       pcf.coreData.EpochNotifier(),
		EpochConfig:         &pcf.epochConfig,
		ShardCoordinator:    pcf.bootstrapComponents.ShardCoordinator(),
	}
	vmFactory, err := metachain.NewVMContainerFactory(argsNewVMContainer)
	if err != nil {
		return nil, err
	}

	argsParser := smartContract.NewArgumentParser()

	vmContainer, err := vmFactory.Create()
	if err != nil {
		return nil, err
	}

	interimProcFactory, err := metachain.NewIntermediateProcessorsContainerFactory(
		pcf.bootstrapComponents.ShardCoordinator(),
		pcf.coreData.InternalMarshalizer(),
		pcf.coreData.Hasher(),
		pcf.coreData.AddressPubKeyConverter(),
		pcf.data.StorageService(),
		pcf.data.Datapool(),
		pcf.coreData.EconomicsData(),
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

	badTxForwarder, err := interimProcContainer.Get(dataBlock.InvalidBlock)
	if err != nil {
		return nil, err
	}

	esdtTransferParser, err := parsers.NewESDTTransferParser(pcf.coreData.InternalMarshalizer())
	if err != nil {
		return nil, err
	}

	argsTxTypeHandler := coordinator.ArgNewTxTypeHandler{
		PubkeyConverter:        pcf.coreData.AddressPubKeyConverter(),
		ShardCoordinator:       pcf.bootstrapComponents.ShardCoordinator(),
		BuiltInFunctions:       builtInFuncs,
		ArgumentParser:         parsers.NewCallArgsParser(),
		EpochNotifier:          pcf.coreData.EpochNotifier(),
		RelayedTxV2EnableEpoch: pcf.epochConfig.EnableEpochs.RelayedTransactionsV2EnableEpoch,
		ESDTTransferParser:     esdtTransferParser,
	}
	txTypeHandler, err := coordinator.NewTxTypeHandler(argsTxTypeHandler)
	if err != nil {
		return nil, err
	}

	gasHandler, err := preprocess.NewGasComputation(
		pcf.coreData.EconomicsData(),
		txTypeHandler,
		pcf.epochNotifier,
		pcf.epochConfig.EnableEpochs.SCDeployEnableEpoch,
	)
	if err != nil {
		return nil, err
	}

	txFeeHandler, err := postprocess.NewFeeAccumulator()
	if err != nil {
		return nil, err
	}

	enableEpochs := pcf.epochConfig.EnableEpochs
	argsNewScProcessor := smartContract.ArgsNewSmartContractProcessor{
		VmContainer:                         vmContainer,
		ArgsParser:                          argsParser,
		Hasher:                              pcf.coreData.Hasher(),
		Marshalizer:                         pcf.coreData.InternalMarshalizer(),
		AccountsDB:                          pcf.state.AccountsAdapter(),
		BlockChainHook:                      vmFactory.BlockChainHookImpl(),
		PubkeyConv:                          pcf.coreData.AddressPubKeyConverter(),
		ShardCoordinator:                    pcf.bootstrapComponents.ShardCoordinator(),
		ScrForwarder:                        scForwarder,
		TxFeeHandler:                        txFeeHandler,
		EconomicsFee:                        pcf.coreData.EconomicsData(),
		TxTypeHandler:                       txTypeHandler,
		GasHandler:                          gasHandler,
		GasSchedule:                         pcf.gasSchedule,
		TxLogsProcessor:                     pcf.txLogsProcessor,
		DeployEnableEpoch:                   enableEpochs.SCDeployEnableEpoch,
		BuiltinEnableEpoch:                  enableEpochs.BuiltInFunctionsEnableEpoch,
		PenalizedTooMuchGasEnableEpoch:      enableEpochs.PenalizedTooMuchGasEnableEpoch,
		RepairCallbackEnableEpoch:           enableEpochs.RepairCallbackEnableEpoch,
		IsGenesisProcessing:                 false,
		ReturnDataToLastTransferEnableEpoch: enableEpochs.ReturnDataToLastTransferEnableEpoch,
		SenderInOutTransferEnableEpoch:      enableEpochs.SenderInOutTransferEnableEpoch,
		BadTxForwarder:                      badTxForwarder,
		EpochNotifier:                       pcf.epochNotifier,
		StakingV2EnableEpoch:                pcf.epochConfig.EnableEpochs.StakingV2EnableEpoch,
		VMOutputCacher:                      txcache.NewDisabledCache(),
		ArwenChangeLocker:                   arwenChangeLocker,

		IncrementSCRNonceInMultiTransferEnableEpoch: enableEpochs.IncrementSCRNonceInMultiTransferEnableEpoch,
	}
	scProcessor, err := smartContract.NewSmartContractProcessor(argsNewScProcessor)
	if err != nil {
		return nil, err
	}

	argsNewMetaTxProcessor := transaction.ArgsNewMetaTxProcessor{
		Hasher:           pcf.coreData.Hasher(),
		Marshalizer:      pcf.coreData.InternalMarshalizer(),
		Accounts:         pcf.state.AccountsAdapter(),
		PubkeyConv:       pcf.coreData.AddressPubKeyConverter(),
		ShardCoordinator: pcf.bootstrapComponents.ShardCoordinator(),
		ScProcessor:      scProcessor,
		TxTypeHandler:    txTypeHandler,
		EconomicsFee:     pcf.coreData.EconomicsData(),
		ESDTEnableEpoch:  pcf.epochConfig.EnableEpochs.ESDTEnableEpoch,
		EpochNotifier:    pcf.epochNotifier,
	}

	transactionProcessor, err := transaction.NewMetaTxProcessor(argsNewMetaTxProcessor)
	if err != nil {
		return nil, errors.New("could not create transaction processor: " + err.Error())
	}

	err = pcf.createMetaTxSimulatorProcessor(txSimulatorProcessorArgs, argsNewScProcessor, txTypeHandler)
	if err != nil {
		return nil, err
	}

	blockSizeThrottler, err := throttle.NewBlockSizeThrottle(pcf.config.BlockSizeThrottleConfig.MinSizeInBytes, pcf.config.BlockSizeThrottleConfig.MaxSizeInBytes)
	if err != nil {
		return nil, err
	}

	blockSizeComputationHandler, err := preprocess.NewBlockSizeComputation(
		pcf.coreData.InternalMarshalizer(),
		blockSizeThrottler,
		pcf.config.BlockSizeThrottleConfig.MaxSizeInBytes,
	)
	if err != nil {
		return nil, err
	}

	balanceComputationHandler, err := preprocess.NewBalanceComputation()
	if err != nil {
		return nil, err
	}

	preProcFactory, err := metachain.NewPreProcessorsContainerFactory(
		pcf.bootstrapComponents.ShardCoordinator(),
		pcf.data.StorageService(),
		pcf.coreData.InternalMarshalizer(),
		pcf.coreData.Hasher(),
		pcf.data.Datapool(),
		pcf.state.AccountsAdapter(),
		requestHandler,
		transactionProcessor,
		scProcessor,
		pcf.coreData.EconomicsData(),
		gasHandler,
		blockTracker,
		pcf.coreData.AddressPubKeyConverter(),
		blockSizeComputationHandler,
		balanceComputationHandler,
	)
	if err != nil {
		return nil, err
	}

	preProcContainer, err := preProcFactory.Create()
	if err != nil {
		return nil, err
	}

	argsTransactionCoordinator := coordinator.ArgTransactionCoordinator{
		Hasher:                            pcf.coreData.Hasher(),
		Marshalizer:                       pcf.coreData.InternalMarshalizer(),
		ShardCoordinator:                  pcf.bootstrapComponents.ShardCoordinator(),
		Accounts:                          pcf.state.AccountsAdapter(),
		MiniBlockPool:                     pcf.data.Datapool().MiniBlocks(),
		RequestHandler:                    requestHandler,
		PreProcessors:                     preProcContainer,
		InterProcessors:                   interimProcContainer,
		GasHandler:                        gasHandler,
		FeeHandler:                        txFeeHandler,
		BlockSizeComputation:              blockSizeComputationHandler,
		BalanceComputation:                balanceComputationHandler,
		EconomicsFee:                      pcf.coreData.EconomicsData(),
		TxTypeHandler:                     txTypeHandler,
		BlockGasAndFeesReCheckEnableEpoch: enableEpochs.BlockGasAndFeesReCheckEnableEpoch,
		TransactionsLogProcessor:          pcf.txLogsProcessor,
	}
	txCoordinator, err := coordinator.NewTransactionCoordinator(argsTransactionCoordinator)
	if err != nil {
		return nil, err
	}

	argsStaking := scToProtocol.ArgStakingToPeer{
		PubkeyConv:       pcf.coreData.ValidatorPubKeyConverter(),
		Hasher:           pcf.coreData.Hasher(),
		Marshalizer:      pcf.coreData.InternalMarshalizer(),
		PeerState:        pcf.state.PeerAccounts(),
		BaseState:        pcf.state.AccountsAdapter(),
		ArgParser:        argsParser,
		CurrTxs:          pcf.data.Datapool().CurrentBlockTxs(),
		RatingsData:      pcf.coreData.RatingsData(),
		EpochNotifier:    pcf.coreData.EpochNotifier(),
		StakeEnableEpoch: pcf.epochConfig.EnableEpochs.StakeEnableEpoch,
	}
	smartContractToProtocol, err := scToProtocol.NewStakingToPeer(argsStaking)
	if err != nil {
		return nil, err
	}

	genesisHdr := pcf.data.Blockchain().GetGenesisHeader()
	argsEpochStartData := metachainEpochStart.ArgsNewEpochStartData{
		Marshalizer:       pcf.coreData.InternalMarshalizer(),
		Hasher:            pcf.coreData.Hasher(),
		Store:             pcf.data.StorageService(),
		DataPool:          pcf.data.Datapool(),
		BlockTracker:      blockTracker,
		ShardCoordinator:  pcf.bootstrapComponents.ShardCoordinator(),
		EpochStartTrigger: epochStartTrigger,
		RequestHandler:    requestHandler,
		GenesisEpoch:      genesisHdr.GetEpoch(),
	}
	epochStartDataCreator, err := metachainEpochStart.NewEpochStartData(argsEpochStartData)
	if err != nil {
		return nil, err
	}

	economicsDataProvider := metachainEpochStart.NewEpochEconomicsStatistics()
	argsEpochEconomics := metachainEpochStart.ArgsNewEpochEconomics{
		Marshalizer:           pcf.coreData.InternalMarshalizer(),
		Hasher:                pcf.coreData.Hasher(),
		Store:                 pcf.data.StorageService(),
		ShardCoordinator:      pcf.bootstrapComponents.ShardCoordinator(),
		RewardsHandler:        pcf.coreData.EconomicsData(),
		RoundTime:             pcf.coreData.RoundHandler(),
		GenesisNonce:          genesisHdr.GetNonce(),
		GenesisEpoch:          genesisHdr.GetEpoch(),
		GenesisTotalSupply:    pcf.coreData.EconomicsData().GenesisTotalSupply(),
		EconomicsDataNotified: economicsDataProvider,
		StakingV2EnableEpoch:  pcf.epochConfig.EnableEpochs.StakingV2EnableEpoch,
	}
	epochEconomics, err := metachainEpochStart.NewEndOfEpochEconomicsDataCreator(argsEpochEconomics)
	if err != nil {
		return nil, err
	}

	systemVM, err := vmContainer.Get(factory.SystemVirtualMachine)
	if err != nil {
		return nil, err
	}

	// TODO: in case of changing the minimum node price, make sure to update the staking data provider
	stakingDataProvider, err := metachainEpochStart.NewStakingDataProvider(systemVM, pcf.systemSCConfig.StakingSystemSCConfig.GenesisNodePrice)
	if err != nil {
		return nil, err
	}

	rewardsStorage := pcf.data.StorageService().GetStorer(dataRetriever.RewardTransactionUnit)
	miniBlockStorage := pcf.data.StorageService().GetStorer(dataRetriever.MiniBlockUnit)
	argsEpochRewards := metachainEpochStart.RewardsCreatorProxyArgs{
		BaseRewardsCreatorArgs: metachainEpochStart.BaseRewardsCreatorArgs{
			ShardCoordinator:              pcf.bootstrapComponents.ShardCoordinator(),
			PubkeyConverter:               pcf.coreData.AddressPubKeyConverter(),
			RewardsStorage:                rewardsStorage,
			MiniBlockStorage:              miniBlockStorage,
			Hasher:                        pcf.coreData.Hasher(),
			Marshalizer:                   pcf.coreData.InternalMarshalizer(),
			DataPool:                      pcf.data.Datapool(),
			ProtocolSustainabilityAddress: pcf.coreData.EconomicsData().ProtocolSustainabilityAddress(),
			NodesConfigProvider:           pcf.nodesCoordinator,
			UserAccountsDB:                pcf.state.AccountsAdapter(),
			RewardsFix1EpochEnable:        enableEpochs.SwitchJailWaitingEnableEpoch,
			DelegationSystemSCEnableEpoch: pcf.epochConfig.EnableEpochs.StakingV2EnableEpoch,
		},
		StakingDataProvider:   stakingDataProvider,
		RewardsHandler:        pcf.coreData.EconomicsData(),
		EconomicsDataProvider: economicsDataProvider,
		EpochEnableV2:         pcf.epochConfig.EnableEpochs.StakingV2EnableEpoch,
	}
	epochRewards, err := metachainEpochStart.NewRewardsCreatorProxy(argsEpochRewards)
	if err != nil {
		return nil, err
	}

	argsEpochValidatorInfo := metachainEpochStart.ArgsNewValidatorInfoCreator{
		ShardCoordinator: pcf.bootstrapComponents.ShardCoordinator(),
		MiniBlockStorage: miniBlockStorage,
		Hasher:           pcf.coreData.Hasher(),
		Marshalizer:      pcf.coreData.InternalMarshalizer(),
		DataPool:         pcf.data.Datapool(),
	}
	validatorInfoCreator, err := metachainEpochStart.NewValidatorInfoCreator(argsEpochValidatorInfo)
	if err != nil {
		return nil, err
	}

	accountsDb := make(map[state.AccountsDbIdentifier]state.AccountsAdapter)
	accountsDb[state.UserAccountsState] = pcf.state.AccountsAdapter()
	accountsDb[state.PeerAccountsState] = pcf.state.PeerAccounts()

	argumentsBaseProcessor := block.ArgBaseProcessor{
		CoreComponents:      pcf.coreData,
		DataComponents:      pcf.data,
		BootstrapComponents: pcf.bootstrapComponents,
		StatusComponents:    pcf.statusComponents,
		Config:              pcf.config,
		Version:             pcf.version,
		AccountsDB:          accountsDb,
		ForkDetector:        forkDetector,
		NodesCoordinator:    pcf.nodesCoordinator,
		RequestHandler:      requestHandler,
		BlockChainHook:      vmFactory.BlockChainHookImpl(),
		TxCoordinator:       txCoordinator,
		EpochStartTrigger:   epochStartTrigger,
		HeaderValidator:     headerValidator,
		BootStorer:          bootStorer,
		BlockTracker:        blockTracker,
		FeeHandler:          txFeeHandler,
		BlockSizeThrottler:  blockSizeThrottler,
		HistoryRepository:   pcf.historyRepo,
		EpochNotifier:       pcf.epochNotifier,
		VMContainersFactory: vmFactory,
		VmContainer:         vmContainer,
	}

	esdtOwnerAddress, err := pcf.coreData.AddressPubKeyConverter().Decode(pcf.systemSCConfig.ESDTSystemSCConfig.OwnerAddress)
	if err != nil {
		return nil, fmt.Errorf("%w while decoding systemSCConfig.ESDTSystemSCConfig.OwnerAddress "+
			"in processComponentsFactory.newMetaBlockProcessor", err)
	}

	argsEpochSystemSC := metachainEpochStart.ArgsNewEpochStartSystemSCProcessing{
		SystemVM:                systemVM,
		UserAccountsDB:          pcf.state.AccountsAdapter(),
		PeerAccountsDB:          pcf.state.PeerAccounts(),
		Marshalizer:             pcf.coreData.InternalMarshalizer(),
		StartRating:             pcf.coreData.RatingsData().StartRating(),
		ValidatorInfoCreator:    validatorStatisticsProcessor,
		EndOfEpochCallerAddress: vm.EndOfEpochAddress,
		StakingSCAddress:        vm.StakingSCAddress,
		ChanceComputer:          pcf.coreData.Rater(),
		EpochNotifier:           pcf.coreData.EpochNotifier(),
		GenesisNodesConfig:      pcf.coreData.GenesisNodesSetup(),
		MaxNodesEnableConfig:    enableEpochs.MaxNodesChangeEnableEpoch,
		StakingDataProvider:     stakingDataProvider,
		NodesConfigProvider:     pcf.nodesCoordinator,
		ShardCoordinator:        pcf.bootstrapComponents.ShardCoordinator(),
		ESDTOwnerAddressBytes:   esdtOwnerAddress,
		EpochConfig:             pcf.epochConfig,
	}
	epochStartSystemSCProcessor, err := metachainEpochStart.NewSystemSCProcessor(argsEpochSystemSC)
	if err != nil {
		return nil, err
	}

	arguments := block.ArgMetaProcessor{
		ArgBaseProcessor:             argumentsBaseProcessor,
		SCToProtocol:                 smartContractToProtocol,
		PendingMiniBlocksHandler:     pendingMiniBlocksHandler,
		EpochStartDataCreator:        epochStartDataCreator,
		EpochEconomics:               epochEconomics,
		EpochRewardsCreator:          epochRewards,
		EpochValidatorInfoCreator:    validatorInfoCreator,
		ValidatorStatisticsProcessor: validatorStatisticsProcessor,
		EpochSystemSCProcessor:       epochStartSystemSCProcessor,
		RewardsV2EnableEpoch:         pcf.epochConfig.EnableEpochs.StakingV2EnableEpoch,
	}

	metaProcessor, err := block.NewMetaProcessor(arguments)
	if err != nil {
		return nil, errors.New("could not create block processor: " + err.Error())
	}

	return metaProcessor, nil
}

func (pcf *processComponentsFactory) createShardTxSimulatorProcessor(
	txSimulatorProcessorArgs *txsimulator.ArgsTxSimulator,
	scProcArgs smartContract.ArgsNewSmartContractProcessor,
	txProcArgs transaction.ArgsNewTxProcessor,
) error {
	readOnlyAccountsDB, err := txsimulator.NewReadOnlyAccountsDB(pcf.state.AccountsAdapter())
	if err != nil {
		return err
	}

	interimProcFactory, err := shard.NewIntermediateProcessorsContainerFactory(
		pcf.bootstrapComponents.ShardCoordinator(),
		pcf.coreData.InternalMarshalizer(),
		pcf.coreData.Hasher(),
		pcf.coreData.AddressPubKeyConverter(),
		disabled.NewChainStorer(),
		pcf.data.Datapool(),
		&processDisabled.FeeHandler{},
	)
	if err != nil {
		return err
	}

	interimProcContainer, err := interimProcFactory.Create()
	if err != nil {
		return err
	}

	scForwarder, err := interimProcContainer.Get(dataBlock.SmartContractResultBlock)
	if err != nil {
		return err
	}
	scProcArgs.ScrForwarder = scForwarder

	receiptTxInterim, err := interimProcContainer.Get(dataBlock.ReceiptBlock)
	if err != nil {
		return err
	}
	txProcArgs.ReceiptForwarder = receiptTxInterim

	badTxInterim, err := interimProcContainer.Get(dataBlock.InvalidBlock)
	if err != nil {
		return err
	}
	scProcArgs.BadTxForwarder = badTxInterim
	txProcArgs.BadTxForwarder = badTxInterim

	scProcArgs.TxFeeHandler = &processDisabled.FeeHandler{}
	txProcArgs.TxFeeHandler = &processDisabled.FeeHandler{}

	scProcArgs.AccountsDB = readOnlyAccountsDB
	scProcArgs.VMOutputCacher = txSimulatorProcessorArgs.VMOutputCacher
	scProcessor, err := smartContract.NewSmartContractProcessor(scProcArgs)
	if err != nil {
		return err
	}
	txProcArgs.ScProcessor = scProcessor

	txProcArgs.Accounts = readOnlyAccountsDB

	txSimulatorProcessorArgs.TransactionProcessor, err = transaction.NewTxProcessor(txProcArgs)
	if err != nil {
		return err
	}

	txSimulatorProcessorArgs.IntermediateProcContainer = interimProcContainer

	return nil
}

func (pcf *processComponentsFactory) createMetaTxSimulatorProcessor(
	txSimulatorProcessorArgs *txsimulator.ArgsTxSimulator,
	scProcArgs smartContract.ArgsNewSmartContractProcessor,
	txTypeHandler process.TxTypeHandler,
) error {
	interimProcFactory, err := shard.NewIntermediateProcessorsContainerFactory(
		pcf.bootstrapComponents.ShardCoordinator(),
		pcf.coreData.InternalMarshalizer(),
		pcf.coreData.Hasher(),
		pcf.coreData.AddressPubKeyConverter(),
		disabled.NewChainStorer(),
		pcf.data.Datapool(),
		&processDisabled.FeeHandler{},
	)
	if err != nil {
		return err
	}

	interimProcContainer, err := interimProcFactory.Create()
	if err != nil {
		return err
	}

	scForwarder, err := interimProcContainer.Get(dataBlock.SmartContractResultBlock)
	if err != nil {
		return err
	}
	scProcArgs.ScrForwarder = scForwarder

	badTxInterim, err := interimProcContainer.Get(dataBlock.InvalidBlock)
	if err != nil {
		return err
	}
	scProcArgs.BadTxForwarder = badTxInterim
	scProcArgs.VMOutputCacher = txSimulatorProcessorArgs.VMOutputCacher

	scProcArgs.TxFeeHandler = &processDisabled.FeeHandler{}

	scProcArgs.VMOutputCacher = txSimulatorProcessorArgs.VMOutputCacher
	scProcessor, err := smartContract.NewSmartContractProcessor(scProcArgs)
	if err != nil {
		return err
	}

	accountsWrapper, err := txsimulator.NewReadOnlyAccountsDB(pcf.state.AccountsAdapter())
	if err != nil {
		return err
	}

	argsNewMetaTx := transaction.ArgsNewMetaTxProcessor{
		Hasher:           pcf.coreData.Hasher(),
		Marshalizer:      pcf.coreData.InternalMarshalizer(),
		Accounts:         accountsWrapper,
		PubkeyConv:       pcf.coreData.AddressPubKeyConverter(),
		ShardCoordinator: pcf.bootstrapComponents.ShardCoordinator(),
		ScProcessor:      scProcessor,
		TxTypeHandler:    txTypeHandler,
		EconomicsFee:     &processDisabled.FeeHandler{},
		ESDTEnableEpoch:  pcf.epochConfig.EnableEpochs.ESDTEnableEpoch,
		EpochNotifier:    pcf.epochNotifier,
	}

	txSimulatorProcessorArgs.TransactionProcessor, err = transaction.NewMetaTxProcessor(argsNewMetaTx)
	if err != nil {
		return err
	}

	txSimulatorProcessorArgs.IntermediateProcContainer = interimProcContainer

	return nil
}
