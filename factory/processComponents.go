package factory

import (
	"errors"
	"fmt"
	"math/big"
	syncGo "sync"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/cmd/node/factory"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/dblookupext"
	"github.com/ElrondNetwork/elrond-go/core/partitioning"
	"github.com/ElrondNetwork/elrond-go/data"
	dataBlock "github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/indexer"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/factory/containers"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/factory/epochProviders"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/factory/resolverscontainer"
	storageResolversContainers "github.com/ElrondNetwork/elrond-go/dataRetriever/factory/storageResolversContainer"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/requestHandlers"
	"github.com/ElrondNetwork/elrond-go/epochStart"
	"github.com/ElrondNetwork/elrond-go/epochStart/metachain"
	"github.com/ElrondNetwork/elrond-go/epochStart/notifier"
	"github.com/ElrondNetwork/elrond-go/epochStart/shardchain"
	errErd "github.com/ElrondNetwork/elrond-go/errors"
	"github.com/ElrondNetwork/elrond-go/fallback"
	"github.com/ElrondNetwork/elrond-go/genesis"
	"github.com/ElrondNetwork/elrond-go/genesis/checking"
	processGenesis "github.com/ElrondNetwork/elrond-go/genesis/process"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/block"
	"github.com/ElrondNetwork/elrond-go/process/block/bootstrapStorage"
	"github.com/ElrondNetwork/elrond-go/process/block/pendingMb"
	"github.com/ElrondNetwork/elrond-go/process/block/poolsCleaner"
	"github.com/ElrondNetwork/elrond-go/process/factory/interceptorscontainer"
	"github.com/ElrondNetwork/elrond-go/process/headerCheck"
	"github.com/ElrondNetwork/elrond-go/process/peer"
	"github.com/ElrondNetwork/elrond-go/process/smartContract"
	"github.com/ElrondNetwork/elrond-go/process/sync"
	"github.com/ElrondNetwork/elrond-go/process/track"
	"github.com/ElrondNetwork/elrond-go/process/transactionLog"
	"github.com/ElrondNetwork/elrond-go/process/txsimulator"
	"github.com/ElrondNetwork/elrond-go/redundancy"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/sharding/networksharding"
	"github.com/ElrondNetwork/elrond-go/storage"
	storageFactory "github.com/ElrondNetwork/elrond-go/storage/factory"
	"github.com/ElrondNetwork/elrond-go/storage/storageUnit"
	"github.com/ElrondNetwork/elrond-go/storage/timecache"
	"github.com/ElrondNetwork/elrond-go/update"
)

var log = logger.GetOrCreate("factory")

// timeSpanForBadHeaders is the expiry time for an added block header hash
var timeSpanForBadHeaders = time.Minute * 2

// processComponents struct holds the process components
type processComponents struct {
	nodesCoordinator            sharding.NodesCoordinator
	shardCoordinator            sharding.Coordinator
	interceptorsContainer       process.InterceptorsContainer
	resolversFinder             dataRetriever.ResolversFinder
	roundHandler                consensus.RoundHandler
	epochStartTrigger           epochStart.TriggerHandler
	epochStartNotifier          EpochStartNotifier
	forkDetector                process.ForkDetector
	blockProcessor              process.BlockProcessor
	blackListHandler            process.TimeCacher
	bootStorer                  process.BootStorer
	headerSigVerifier           process.InterceptedHeaderSigVerifier
	headerIntegrityVerifier     factory.HeaderIntegrityVerifierHandler
	validatorsStatistics        process.ValidatorStatisticsProcessor
	validatorsProvider          process.ValidatorsProvider
	blockTracker                process.BlockTracker
	pendingMiniBlocksHandler    process.PendingMiniBlocksHandler
	requestHandler              process.RequestHandler
	txLogsProcessor             process.TransactionLogProcessorDatabase
	headerConstructionValidator process.HeaderConstructionValidator
	peerShardMapper             process.NetworkShardingCollector
	txSimulatorProcessor        TransactionSimulatorProcessor
	miniBlocksPoolCleaner       process.PoolsCleaner
	txsPoolCleaner              process.PoolsCleaner
	fallbackHeaderValidator     process.FallbackHeaderValidator
	whiteListHandler            process.WhiteListHandler
	whiteListerVerifiedTxs      process.WhiteListHandler
	historyRepository           dblookupext.HistoryRepository
	importStartHandler          update.ImportStartHandler
	requestedItemsHandler       dataRetriever.RequestedItemsHandler
	importHandler               update.ImportHandler
	nodeRedundancyHandler       consensus.NodeRedundancyHandler
	currentEpochProvider        dataRetriever.CurrentNetworkEpochProviderHandler
	arwenChangeLocker           process.Locker
}

// ProcessComponentsFactoryArgs holds the arguments needed to create a process components factory
type ProcessComponentsFactoryArgs struct {
	Config                 config.Config
	EpochConfig            config.EpochConfig
	PrefConfigs            config.PreferencesConfig
	ImportDBConfig         config.ImportDbConfig
	AccountsParser         genesis.AccountsParser
	SmartContractParser    genesis.InitialSmartContractParser
	GasSchedule            core.GasScheduleNotifier
	NodesCoordinator       sharding.NodesCoordinator
	RequestedItemsHandler  dataRetriever.RequestedItemsHandler
	WhiteListHandler       process.WhiteListHandler
	WhiteListerVerifiedTxs process.WhiteListHandler
	MaxRating              uint32
	SystemSCConfig         *config.SystemSmartContractsConfig
	Version                string
	ImportStartHandler     update.ImportStartHandler
	WorkingDir             string
	HistoryRepo            dblookupext.HistoryRepository

	Data                DataComponentsHolder
	CoreData            CoreComponentsHolder
	Crypto              CryptoComponentsHolder
	State               StateComponentsHolder
	Network             NetworkComponentsHolder
	BootstrapComponents BootstrapComponentsHolder
	StatusComponents    StatusComponentsHolder
}

type processComponentsFactory struct {
	config                 config.Config
	epochConfig            config.EpochConfig
	prefConfigs            config.PreferencesConfig
	importDBConfig         config.ImportDbConfig
	accountsParser         genesis.AccountsParser
	smartContractParser    genesis.InitialSmartContractParser
	gasSchedule            core.GasScheduleNotifier
	nodesCoordinator       sharding.NodesCoordinator
	requestedItemsHandler  dataRetriever.RequestedItemsHandler
	whiteListHandler       process.WhiteListHandler
	whiteListerVerifiedTxs process.WhiteListHandler
	maxRating              uint32
	systemSCConfig         *config.SystemSmartContractsConfig
	txLogsProcessor        process.TransactionLogProcessor
	version                string
	importStartHandler     update.ImportStartHandler
	workingDir             string
	historyRepo            dblookupext.HistoryRepository
	epochNotifier          process.EpochNotifier
	importHandler          update.ImportHandler

	data                DataComponentsHolder
	coreData            CoreComponentsHolder
	crypto              CryptoComponentsHolder
	state               StateComponentsHolder
	network             NetworkComponentsHolder
	bootstrapComponents BootstrapComponentsHolder
	statusComponents    StatusComponentsHolder
}

// NewProcessComponentsFactory will return a new instance of processComponentsFactory
func NewProcessComponentsFactory(args ProcessComponentsFactoryArgs) (*processComponentsFactory, error) {
	err := checkProcessComponentsArgs(args)
	if err != nil {
		return nil, err
	}

	return &processComponentsFactory{
		config:                 args.Config,
		epochConfig:            args.EpochConfig,
		prefConfigs:            args.PrefConfigs,
		importDBConfig:         args.ImportDBConfig,
		accountsParser:         args.AccountsParser,
		smartContractParser:    args.SmartContractParser,
		gasSchedule:            args.GasSchedule,
		nodesCoordinator:       args.NodesCoordinator,
		data:                   args.Data,
		coreData:               args.CoreData,
		crypto:                 args.Crypto,
		state:                  args.State,
		network:                args.Network,
		bootstrapComponents:    args.BootstrapComponents,
		statusComponents:       args.StatusComponents,
		requestedItemsHandler:  args.RequestedItemsHandler,
		whiteListHandler:       args.WhiteListHandler,
		whiteListerVerifiedTxs: args.WhiteListerVerifiedTxs,
		maxRating:              args.MaxRating,
		systemSCConfig:         args.SystemSCConfig,
		version:                args.Version,
		importStartHandler:     args.ImportStartHandler,
		workingDir:             args.WorkingDir,
		historyRepo:            args.HistoryRepo,
		epochNotifier:          args.CoreData.EpochNotifier(),
	}, nil
}

// Create will create and return a struct containing process components
func (pcf *processComponentsFactory) Create() (*processComponents, error) {
	arwenChangeLocker := &syncGo.RWMutex{}

	currentEpochProvider, err := epochProviders.CreateCurrentEpochProvider(
		pcf.config,
		pcf.coreData.GenesisNodesSetup().GetRoundDuration(),
		pcf.coreData.GenesisTime().Unix(),
		pcf.prefConfigs.FullArchive,
	)
	if err != nil {
		return nil, err
	}

	pcf.epochNotifier.RegisterNotifyHandler(currentEpochProvider)

	fallbackHeaderValidator, err := fallback.NewFallbackHeaderValidator(
		pcf.data.Datapool().Headers(),
		pcf.coreData.InternalMarshalizer(),
		pcf.data.StorageService(),
	)
	if err != nil {
		return nil, err
	}

	argsHeaderSig := &headerCheck.ArgsHeaderSigVerifier{
		Marshalizer:             pcf.coreData.InternalMarshalizer(),
		Hasher:                  pcf.coreData.Hasher(),
		NodesCoordinator:        pcf.nodesCoordinator,
		MultiSigVerifier:        pcf.crypto.MultiSigner(),
		SingleSigVerifier:       pcf.crypto.BlockSigner(),
		KeyGen:                  pcf.crypto.BlockSignKeyGen(),
		FallbackHeaderValidator: fallbackHeaderValidator,
	}
	headerSigVerifier, err := headerCheck.NewHeaderSigVerifier(argsHeaderSig)
	if err != nil {
		return nil, err
	}

	resolversContainerFactory, err := pcf.newResolverContainerFactory(currentEpochProvider)
	if err != nil {
		return nil, err
	}

	resolversContainer, err := resolversContainerFactory.Create()
	if err != nil {
		return nil, err
	}

	resolversFinder, err := containers.NewResolversFinder(resolversContainer, pcf.bootstrapComponents.ShardCoordinator())
	if err != nil {
		return nil, err
	}

	requestHandler, err := requestHandlers.NewResolverRequestHandler(
		resolversFinder,
		pcf.requestedItemsHandler,
		pcf.whiteListHandler,
		core.MaxTxsToRequest,
		pcf.bootstrapComponents.ShardCoordinator().SelfId(),
		time.Second,
	)
	if err != nil {
		return nil, err
	}

	txLogsStorage := pcf.data.StorageService().GetStorer(dataRetriever.TxLogsUnit)

	if !pcf.config.LogsAndEvents.SaveInStorageEnabled && pcf.config.DbLookupExtensions.Enabled {
		log.Warn("processComponentsFactory.Create() node will save logs in storage because DbLookupExtensions is enabled")
	}

	saveLogsInStorage := pcf.config.LogsAndEvents.SaveInStorageEnabled || pcf.config.DbLookupExtensions.Enabled
	txLogsProcessor, err := transactionLog.NewTxLogProcessor(transactionLog.ArgTxLogProcessor{
		Storer:               txLogsStorage,
		Marshalizer:          pcf.coreData.InternalMarshalizer(),
		SaveInStorageEnabled: saveLogsInStorage,
	})
	if err != nil {
		return nil, err
	}

	pcf.txLogsProcessor = txLogsProcessor
	genesisBlocks, err := pcf.generateGenesisHeadersAndApplyInitialBalances()
	if err != nil {
		return nil, err
	}

	err = pcf.indexGenesisAccounts()
	if err != nil {
		log.Warn("cannot index genesis accounts", "error", err)
	}

	startEpochNum := pcf.bootstrapComponents.EpochBootstrapParams().Epoch()
	if startEpochNum == 0 {
		err = pcf.indexGenesisBlocks(genesisBlocks)
		if err != nil {
			return nil, err
		}
	}

	err = pcf.setGenesisHeader(genesisBlocks)
	if err != nil {
		return nil, err
	}

	validatorStatisticsProcessor, err := pcf.newValidatorStatisticsProcessor()
	if err != nil {
		return nil, err
	}

	cacheRefreshDuration := time.Duration(pcf.config.ValidatorStatistics.CacheRefreshIntervalInSec) * time.Second
	argVSP := peer.ArgValidatorsProvider{
		NodesCoordinator:                  pcf.nodesCoordinator,
		StartEpoch:                        startEpochNum,
		EpochStartEventNotifier:           pcf.coreData.EpochStartNotifierWithConfirm(),
		CacheRefreshIntervalDurationInSec: cacheRefreshDuration,
		ValidatorStatistics:               validatorStatisticsProcessor,
		MaxRating:                         pcf.maxRating,
		PubKeyConverter:                   pcf.coreData.ValidatorPubKeyConverter(),
	}

	validatorsProvider, err := peer.NewValidatorsProvider(argVSP)
	if err != nil {
		return nil, err
	}

	epochStartTrigger, err := pcf.newEpochStartTrigger(requestHandler)
	if err != nil {
		return nil, err
	}

	requestHandler.SetEpoch(epochStartTrigger.Epoch())

	err = dataRetriever.SetEpochHandlerToHdrResolver(resolversContainer, epochStartTrigger)
	if err != nil {
		return nil, err
	}

	validatorStatsRootHash, err := validatorStatisticsProcessor.RootHash()
	if err != nil {
		return nil, err
	}

	log.Debug("Validator stats created", "validatorStatsRootHash", validatorStatsRootHash)

	genesisMetaBlock, ok := genesisBlocks[core.MetachainShardId]
	if !ok {
		return nil, errors.New("genesis meta block does not exist")
	}

	genesisMetaBlock.SetValidatorStatsRootHash(validatorStatsRootHash)
	err = pcf.prepareGenesisBlock(genesisBlocks)
	if err != nil {
		return nil, err
	}

	bootStr := pcf.data.StorageService().GetStorer(dataRetriever.BootstrapUnit)
	bootStorer, err := bootstrapStorage.NewBootstrapStorer(pcf.coreData.InternalMarshalizer(), bootStr)
	if err != nil {
		return nil, err
	}

	argsHeaderValidator := block.ArgsHeaderValidator{
		Hasher:      pcf.coreData.Hasher(),
		Marshalizer: pcf.coreData.InternalMarshalizer(),
	}
	headerValidator, err := block.NewHeaderValidator(argsHeaderValidator)
	if err != nil {
		return nil, err
	}

	blockTracker, err := pcf.newBlockTracker(
		headerValidator,
		requestHandler,
		genesisBlocks,
	)
	if err != nil {
		return nil, err
	}

	mbsPoolsCleaner, err := poolsCleaner.NewMiniBlocksPoolsCleaner(
		pcf.data.Datapool().MiniBlocks(),
		pcf.coreData.RoundHandler(),
		pcf.bootstrapComponents.ShardCoordinator(),
	)
	if err != nil {
		return nil, err
	}

	mbsPoolsCleaner.StartCleaning()

	txsPoolsCleaner, err := poolsCleaner.NewTxsPoolsCleaner(
		pcf.coreData.AddressPubKeyConverter(),
		pcf.data.Datapool(),
		pcf.coreData.RoundHandler(),
		pcf.bootstrapComponents.ShardCoordinator(),
	)
	if err != nil {
		return nil, err
	}

	txsPoolsCleaner.StartCleaning()

	_, err = track.NewMiniBlockTrack(
		pcf.data.Datapool(),
		pcf.bootstrapComponents.ShardCoordinator(),
		pcf.whiteListHandler,
	)
	if err != nil {
		return nil, err
	}

	interceptorContainerFactory, blackListHandler, err := pcf.newInterceptorContainerFactory(
		headerSigVerifier,
		pcf.bootstrapComponents.HeaderIntegrityVerifier(),
		blockTracker,
		epochStartTrigger,
		requestHandler,
	)
	if err != nil {
		return nil, err
	}

	// TODO refactor all these factory calls
	interceptorsContainer, err := interceptorContainerFactory.Create()
	if err != nil {
		return nil, err
	}

	var pendingMiniBlocksHandler process.PendingMiniBlocksHandler
	pendingMiniBlocksHandler, err = pendingMb.NewNilPendingMiniBlocks()
	if err != nil {
		return nil, err
	}
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		pendingMiniBlocksHandler, err = pendingMb.NewPendingMiniBlocks()
		if err != nil {
			return nil, err
		}
	}

	forkDetector, err := pcf.newForkDetector(blackListHandler, blockTracker)
	if err != nil {
		return nil, err
	}

	vmOutputCacherConfig := storageFactory.GetCacherFromConfig(pcf.config.VMOutputCacher)
	vmOutputCacher, err := storageUnit.NewCache(vmOutputCacherConfig)
	if err != nil {
		return nil, err
	}

	txSimulatorProcessorArgs := &txsimulator.ArgsTxSimulator{
		AddressPubKeyConverter: pcf.coreData.AddressPubKeyConverter(),
		ShardCoordinator:       pcf.bootstrapComponents.ShardCoordinator(),
		VMOutputCacher:         vmOutputCacher,
		Hasher:                 pcf.coreData.Hasher(),
		Marshalizer:            pcf.coreData.InternalMarshalizer(),
	}

	blockProcessor, err := pcf.newBlockProcessor(
		requestHandler,
		forkDetector,
		epochStartTrigger,
		bootStorer,
		validatorStatisticsProcessor,
		headerValidator,
		blockTracker,
		pendingMiniBlocksHandler,
		txSimulatorProcessorArgs,
		arwenChangeLocker,
	)
	if err != nil {
		return nil, err
	}

	conversionBase := 10
	genesisNodePrice, ok := big.NewInt(0).SetString(pcf.systemSCConfig.StakingSystemSCConfig.GenesisNodePrice, conversionBase)
	if !ok {
		return nil, errors.New("invalid genesis node price")
	}

	nodesSetupChecker, err := checking.NewNodesSetupChecker(
		pcf.accountsParser,
		genesisNodePrice,
		pcf.coreData.ValidatorPubKeyConverter(),
		pcf.crypto.BlockSignKeyGen(),
	)
	if err != nil {
		return nil, err
	}

	err = nodesSetupChecker.Check(pcf.coreData.GenesisNodesSetup().AllInitialNodes())
	if err != nil {
		return nil, err
	}

	// TODO: maybe move PeerShardMapper to network components
	peerShardMapper, err := pcf.prepareNetworkShardingCollector()
	if err != nil {
		return nil, err
	}

	txSimulator, err := txsimulator.NewTransactionSimulator(*txSimulatorProcessorArgs)
	if err != nil {
		return nil, err
	}

	nodeRedundancyArg := redundancy.ArgNodeRedundancy{
		RedundancyLevel:    pcf.prefConfigs.RedundancyLevel,
		Messenger:          pcf.network.NetworkMessenger(),
		ObserverPrivateKey: pcf.crypto.PrivateKey(),
	}
	nodeRedundancyHandler, err := redundancy.NewNodeRedundancy(nodeRedundancyArg)
	if err != nil {
		return nil, err
	}

	return &processComponents{
		nodesCoordinator:            pcf.nodesCoordinator,
		shardCoordinator:            pcf.bootstrapComponents.ShardCoordinator(),
		interceptorsContainer:       interceptorsContainer,
		resolversFinder:             resolversFinder,
		roundHandler:                pcf.coreData.RoundHandler(),
		forkDetector:                forkDetector,
		blockProcessor:              blockProcessor,
		epochStartTrigger:           epochStartTrigger,
		epochStartNotifier:          pcf.coreData.EpochStartNotifierWithConfirm(),
		blackListHandler:            blackListHandler,
		bootStorer:                  bootStorer,
		headerSigVerifier:           headerSigVerifier,
		validatorsStatistics:        validatorStatisticsProcessor,
		validatorsProvider:          validatorsProvider,
		blockTracker:                blockTracker,
		pendingMiniBlocksHandler:    pendingMiniBlocksHandler,
		requestHandler:              requestHandler,
		txLogsProcessor:             txLogsProcessor,
		headerConstructionValidator: headerValidator,
		headerIntegrityVerifier:     pcf.bootstrapComponents.HeaderIntegrityVerifier(),
		peerShardMapper:             peerShardMapper,
		txSimulatorProcessor:        txSimulator,
		miniBlocksPoolCleaner:       mbsPoolsCleaner,
		txsPoolCleaner:              txsPoolsCleaner,
		fallbackHeaderValidator:     fallbackHeaderValidator,
		whiteListHandler:            pcf.whiteListHandler,
		whiteListerVerifiedTxs:      pcf.whiteListerVerifiedTxs,
		historyRepository:           pcf.historyRepo,
		importStartHandler:          pcf.importStartHandler,
		requestedItemsHandler:       pcf.requestedItemsHandler,
		importHandler:               pcf.importHandler,
		nodeRedundancyHandler:       nodeRedundancyHandler,
		currentEpochProvider:        currentEpochProvider,
		arwenChangeLocker:           arwenChangeLocker,
	}, nil
}

func (pcf *processComponentsFactory) newValidatorStatisticsProcessor() (process.ValidatorStatisticsProcessor, error) {

	storageService := pcf.data.StorageService()

	var peerDataPool peer.DataPool = pcf.data.Datapool()
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() < pcf.bootstrapComponents.ShardCoordinator().NumberOfShards() {
		peerDataPool = pcf.data.Datapool()
	}

	hardForkConfig := pcf.config.Hardfork
	ratingEnabledEpoch := uint32(0)
	if hardForkConfig.AfterHardFork {
		ratingEnabledEpoch = hardForkConfig.StartEpoch + hardForkConfig.ValidatorGracePeriodInEpochs
	}
	arguments := peer.ArgValidatorStatisticsProcessor{
		PeerAdapter:                     pcf.state.PeerAccounts(),
		PubkeyConv:                      pcf.coreData.ValidatorPubKeyConverter(),
		NodesCoordinator:                pcf.nodesCoordinator,
		ShardCoordinator:                pcf.bootstrapComponents.ShardCoordinator(),
		DataPool:                        peerDataPool,
		StorageService:                  storageService,
		Marshalizer:                     pcf.coreData.InternalMarshalizer(),
		Rater:                           pcf.coreData.Rater(),
		MaxComputableRounds:             pcf.config.GeneralSettings.MaxComputableRounds,
		RewardsHandler:                  pcf.coreData.EconomicsData(),
		NodesSetup:                      pcf.coreData.GenesisNodesSetup(),
		RatingEnableEpoch:               ratingEnabledEpoch,
		GenesisNonce:                    pcf.data.Blockchain().GetGenesisHeader().GetNonce(),
		EpochNotifier:                   pcf.coreData.EpochNotifier(),
		SwitchJailWaitingEnableEpoch:    pcf.epochConfig.EnableEpochs.SwitchJailWaitingEnableEpoch,
		BelowSignedThresholdEnableEpoch: pcf.epochConfig.EnableEpochs.BelowSignedThresholdEnableEpoch,
		StakingV2EnableEpoch:            pcf.epochConfig.EnableEpochs.StakingV2EnableEpoch,
	}

	validatorStatisticsProcessor, err := peer.NewValidatorStatisticsProcessor(arguments)
	if err != nil {
		return nil, err
	}

	return validatorStatisticsProcessor, nil
}

func (pcf *processComponentsFactory) newEpochStartTrigger(requestHandler process.RequestHandler) (epochStart.TriggerHandler, error) {
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() < pcf.bootstrapComponents.ShardCoordinator().NumberOfShards() {
		argsHeaderValidator := block.ArgsHeaderValidator{
			Hasher:      pcf.coreData.Hasher(),
			Marshalizer: pcf.coreData.InternalMarshalizer(),
		}
		headerValidator, err := block.NewHeaderValidator(argsHeaderValidator)
		if err != nil {
			return nil, err
		}

		argsPeerMiniBlockSyncer := shardchain.ArgPeerMiniBlockSyncer{
			MiniBlocksPool: pcf.data.Datapool().MiniBlocks(),
			Requesthandler: requestHandler,
		}

		peerMiniBlockSyncer, err := shardchain.NewPeerMiniBlockSyncer(argsPeerMiniBlockSyncer)
		if err != nil {
			return nil, err
		}

		argEpochStart := &shardchain.ArgsShardEpochStartTrigger{
			Marshalizer:          pcf.coreData.InternalMarshalizer(),
			Hasher:               pcf.coreData.Hasher(),
			HeaderValidator:      headerValidator,
			Uint64Converter:      pcf.coreData.Uint64ByteSliceConverter(),
			DataPool:             pcf.data.Datapool(),
			Storage:              pcf.data.StorageService(),
			RequestHandler:       requestHandler,
			Epoch:                pcf.bootstrapComponents.EpochBootstrapParams().Epoch(),
			EpochStartNotifier:   pcf.coreData.EpochStartNotifierWithConfirm(),
			Validity:             process.MetaBlockValidity,
			Finality:             process.BlockFinality,
			PeerMiniBlocksSyncer: peerMiniBlockSyncer,
			RoundHandler:         pcf.coreData.RoundHandler(),
			AppStatusHandler:     pcf.coreData.StatusHandler(),
		}
		epochStartTrigger, err := shardchain.NewEpochStartTrigger(argEpochStart)
		if err != nil {
			return nil, errors.New("error creating new start of epoch trigger" + err.Error())
		}

		return epochStartTrigger, nil
	}

	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		argEpochStart := &metachain.ArgsNewMetaEpochStartTrigger{
			GenesisTime:        time.Unix(pcf.coreData.GenesisNodesSetup().GetStartTime(), 0),
			Settings:           &pcf.config.EpochStartConfig,
			Epoch:              pcf.bootstrapComponents.EpochBootstrapParams().Epoch(),
			EpochStartRound:    pcf.data.Blockchain().GetGenesisHeader().GetRound(),
			EpochStartNotifier: pcf.coreData.EpochStartNotifierWithConfirm(),
			Storage:            pcf.data.StorageService(),
			Marshalizer:        pcf.coreData.InternalMarshalizer(),
			Hasher:             pcf.coreData.Hasher(),
			AppStatusHandler:   pcf.coreData.StatusHandler(),
		}
		epochStartTrigger, err := metachain.NewEpochStartTrigger(argEpochStart)
		if err != nil {
			return nil, errors.New("error creating new start of epoch trigger" + err.Error())
		}

		return epochStartTrigger, nil
	}

	return nil, errors.New("error creating new start of epoch trigger because of invalid shard id")
}

func (pcf *processComponentsFactory) generateGenesisHeadersAndApplyInitialBalances() (map[uint32]data.HeaderHandler, error) {
	genesisVmConfig := pcf.config.VirtualMachine.Execution
	conversionBase := 10
	genesisNodePrice, ok := big.NewInt(0).SetString(pcf.systemSCConfig.StakingSystemSCConfig.GenesisNodePrice, conversionBase)
	if !ok {
		return nil, errors.New("invalid genesis node price")
	}

	arg := processGenesis.ArgsGenesisBlockCreator{
		Core:                 pcf.coreData,
		Data:                 pcf.data,
		GenesisTime:          uint64(pcf.coreData.GenesisNodesSetup().GetStartTime()),
		StartEpochNum:        pcf.bootstrapComponents.EpochBootstrapParams().Epoch(),
		Accounts:             pcf.state.AccountsAdapter(),
		InitialNodesSetup:    pcf.coreData.GenesisNodesSetup(),
		Economics:            pcf.coreData.EconomicsData(),
		ShardCoordinator:     pcf.bootstrapComponents.ShardCoordinator(),
		AccountsParser:       pcf.accountsParser,
		SmartContractParser:  pcf.smartContractParser,
		ValidatorAccounts:    pcf.state.PeerAccounts(),
		GasSchedule:          pcf.gasSchedule,
		VirtualMachineConfig: genesisVmConfig,
		TxLogsProcessor:      pcf.txLogsProcessor,
		HardForkConfig:       pcf.config.Hardfork,
		TrieStorageManagers:  pcf.state.TrieStorageManagers(),
		SystemSCConfig:       *pcf.systemSCConfig,
		ImportStartHandler:   pcf.importStartHandler,
		WorkingDir:           pcf.workingDir,
		BlockSignKeyGen:      pcf.crypto.BlockSignKeyGen(),
		GenesisString:        pcf.config.GeneralSettings.GenesisString,
		GenesisNodePrice:     genesisNodePrice,
		EpochConfig:          &pcf.epochConfig,
	}

	gbc, err := processGenesis.NewGenesisBlockCreator(arg)
	if err != nil {
		return nil, err
	}
	pcf.importHandler = gbc.ImportHandler()

	return gbc.CreateGenesisBlocks()
}

func (pcf *processComponentsFactory) indexGenesisAccounts() error {
	if !pcf.statusComponents.OutportHandler().HasDrivers() {
		return nil
	}

	rootHash, err := pcf.state.AccountsAdapter().RootHash()
	if err != nil {
		return err
	}

	leavesChannel, err := pcf.state.AccountsAdapter().GetAllLeaves(rootHash)
	if err != nil {
		return err
	}

	genesisAccounts := make([]state.UserAccountHandler, 0)
	for leaf := range leavesChannel {
		userAccount, errUnmarshal := pcf.unmarshalUserAccount(leaf.Key(), leaf.Value())
		if errUnmarshal != nil {
			log.Debug("cannot unmarshal genesis user account. it may be a code leaf", "error", errUnmarshal)
			continue
		}

		genesisAccounts = append(genesisAccounts, userAccount)
	}

	pcf.statusComponents.OutportHandler().SaveAccounts(uint64(pcf.coreData.GenesisNodesSetup().GetStartTime()), genesisAccounts)
	return nil
}

func (pcf *processComponentsFactory) unmarshalUserAccount(address []byte, userAccountsBytes []byte) (state.UserAccountHandler, error) {
	userAccount, err := state.NewUserAccount(address)
	if err != nil {
		return nil, err
	}
	err = pcf.coreData.InternalMarshalizer().Unmarshal(userAccount, userAccountsBytes)
	if err != nil {
		return nil, err
	}

	return userAccount, nil
}

func (pcf *processComponentsFactory) setGenesisHeader(genesisBlocks map[uint32]data.HeaderHandler) error {
	genesisBlock, ok := genesisBlocks[pcf.bootstrapComponents.ShardCoordinator().SelfId()]
	if !ok {
		return errors.New("genesis block does not exist")
	}

	err := pcf.data.Blockchain().SetGenesisHeader(genesisBlock)
	if err != nil {
		return err
	}

	return nil
}

func (pcf *processComponentsFactory) prepareGenesisBlock(genesisBlocks map[uint32]data.HeaderHandler) error {
	genesisBlock, ok := genesisBlocks[pcf.bootstrapComponents.ShardCoordinator().SelfId()]
	if !ok {
		return errors.New("genesis block does not exist")
	}

	genesisBlockHash, err := core.CalculateHash(pcf.coreData.InternalMarshalizer(), pcf.coreData.Hasher(), genesisBlock)
	if err != nil {
		return err
	}

	err = pcf.data.Blockchain().SetGenesisHeader(genesisBlock)
	if err != nil {
		return err
	}

	pcf.data.Blockchain().SetGenesisHeaderHash(genesisBlockHash)

	marshalizedBlock, err := pcf.coreData.InternalMarshalizer().Marshal(genesisBlock)
	if err != nil {
		return err
	}

	nonceToByteSlice := pcf.coreData.Uint64ByteSliceConverter().ToByteSlice(genesisBlock.GetNonce())
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		errNotCritical := pcf.data.StorageService().Put(dataRetriever.MetaBlockUnit, genesisBlockHash, marshalizedBlock)
		if errNotCritical != nil {
			log.Error("error storing genesis metablock", "error", errNotCritical.Error())
		}
		errNotCritical = pcf.data.StorageService().Put(dataRetriever.MetaHdrNonceHashDataUnit, nonceToByteSlice, genesisBlockHash)
		if errNotCritical != nil {
			log.Error("error storing genesis metablock (nonce-hash)", "error", errNotCritical.Error())
		}
	} else {
		errNotCritical := pcf.data.StorageService().Put(dataRetriever.BlockHeaderUnit, genesisBlockHash, marshalizedBlock)
		if errNotCritical != nil {
			log.Error("error storing genesis shardblock", "error", errNotCritical.Error())
		}
		hdrNonceHashDataUnit := dataRetriever.ShardHdrNonceHashDataUnit + dataRetriever.UnitType(genesisBlock.GetShardID())
		errNotCritical = pcf.data.StorageService().Put(hdrNonceHashDataUnit, nonceToByteSlice, genesisBlockHash)
		if errNotCritical != nil {
			log.Error("error storing genesis shard header (nonce-hash)", "error", errNotCritical.Error())
		}
	}

	return nil
}

func (pcf *processComponentsFactory) indexGenesisBlocks(genesisBlocks map[uint32]data.HeaderHandler) error {
	// In Elastic Indexer, only index the metachain block
	genesisBlockHeader := genesisBlocks[core.MetachainShardId]
	genesisBlockHash, err := core.CalculateHash(pcf.coreData.InternalMarshalizer(), pcf.coreData.Hasher(), genesisBlockHeader)
	if err != nil {
		return err
	}

	if pcf.statusComponents.OutportHandler().HasDrivers() {
		log.Info("indexGenesisBlocks(): indexer.SaveBlock", "hash", genesisBlockHash)

		arg := &indexer.ArgsSaveBlockData{
			HeaderHash: genesisBlockHash,
			Body:       &dataBlock.Body{},
			Header:     genesisBlockHeader,
		}
		pcf.statusComponents.OutportHandler().SaveBlock(arg)
	}

	// In "dblookupext" index, record both the metachain and the shardID blocks
	var shardID uint32
	for shardID, genesisBlockHeader = range genesisBlocks {
		if pcf.bootstrapComponents.ShardCoordinator().SelfId() != shardID {
			continue
		}

		genesisBlockHash, err = core.CalculateHash(pcf.coreData.InternalMarshalizer(), pcf.coreData.Hasher(), genesisBlockHeader)
		if err != nil {
			return err
		}

		log.Info("indexGenesisBlocks(): historyRepo.RecordBlock", "shardID", shardID, "hash", genesisBlockHash)
		err = pcf.historyRepo.RecordBlock(genesisBlockHash, genesisBlockHeader, &dataBlock.Body{}, nil, nil)
		if err != nil {
			return err
		}

		nonceByHashDataUnit := dataRetriever.GetHdrNonceHashDataUnit(shardID)
		nonceAsBytes := pcf.coreData.Uint64ByteSliceConverter().ToByteSlice(genesisBlockHeader.GetNonce())
		err = pcf.data.StorageService().Put(nonceByHashDataUnit, nonceAsBytes, genesisBlockHash)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pcf *processComponentsFactory) newBlockTracker(
	headerValidator process.HeaderConstructionValidator,
	requestHandler process.RequestHandler,
	genesisBlocks map[uint32]data.HeaderHandler,
) (process.BlockTracker, error) {
	argBaseTracker := track.ArgBaseTracker{
		Hasher:           pcf.coreData.Hasher(),
		HeaderValidator:  headerValidator,
		Marshalizer:      pcf.coreData.InternalMarshalizer(),
		RequestHandler:   requestHandler,
		RoundHandler:     pcf.coreData.RoundHandler(),
		ShardCoordinator: pcf.bootstrapComponents.ShardCoordinator(),
		Store:            pcf.data.StorageService(),
		StartHeaders:     genesisBlocks,
		PoolsHolder:      pcf.data.Datapool(),
		WhitelistHandler: pcf.whiteListHandler,
	}

	if pcf.bootstrapComponents.ShardCoordinator().SelfId() < pcf.bootstrapComponents.ShardCoordinator().NumberOfShards() {
		arguments := track.ArgShardTracker{
			ArgBaseTracker: argBaseTracker,
		}

		return track.NewShardBlockTrack(arguments)
	}

	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		arguments := track.ArgMetaTracker{
			ArgBaseTracker: argBaseTracker,
		}

		return track.NewMetaBlockTrack(arguments)
	}

	return nil, errors.New("could not create block tracker")
}

// -- Resolvers container Factory begin
func (pcf *processComponentsFactory) newResolverContainerFactory(
	currentEpochProvider dataRetriever.CurrentNetworkEpochProviderHandler,
) (dataRetriever.ResolversContainerFactory, error) {

	if pcf.importDBConfig.IsImportDBMode {
		log.Debug("starting with storage resolvers", "path", pcf.importDBConfig.ImportDBWorkingDir)
		return pcf.newStorageResolver()
	}
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() < pcf.bootstrapComponents.ShardCoordinator().NumberOfShards() {
		return pcf.newShardResolverContainerFactory(currentEpochProvider)
	}
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		return pcf.newMetaResolverContainerFactory(currentEpochProvider)
	}

	return nil, errors.New("could not create interceptor and resolver container factory")
}

func (pcf *processComponentsFactory) newShardResolverContainerFactory(
	currentEpochProvider dataRetriever.CurrentNetworkEpochProviderHandler,
) (dataRetriever.ResolversContainerFactory, error) {

	dataPacker, err := partitioning.NewSimpleDataPacker(pcf.coreData.InternalMarshalizer())
	if err != nil {
		return nil, err
	}

	resolversContainerFactoryArgs := resolverscontainer.FactoryArgs{
		ShardCoordinator:            pcf.bootstrapComponents.ShardCoordinator(),
		Messenger:                   pcf.network.NetworkMessenger(),
		Store:                       pcf.data.StorageService(),
		Marshalizer:                 pcf.coreData.InternalMarshalizer(),
		DataPools:                   pcf.data.Datapool(),
		Uint64ByteSliceConverter:    pcf.coreData.Uint64ByteSliceConverter(),
		DataPacker:                  dataPacker,
		TriesContainer:              pcf.state.TriesContainer(),
		SizeCheckDelta:              pcf.config.Marshalizer.SizeCheckDelta,
		InputAntifloodHandler:       pcf.network.InputAntiFloodHandler(),
		OutputAntifloodHandler:      pcf.network.OutputAntiFloodHandler(),
		NumConcurrentResolvingJobs:  pcf.config.Antiflood.NumConcurrentResolverJobs,
		IsFullHistoryNode:           pcf.prefConfigs.FullArchive,
		CurrentNetworkEpochProvider: currentEpochProvider,
		ResolverConfig:              pcf.config.Resolvers,
		PreferredPeersHolder:        pcf.network.PreferredPeersHolderHandler(),
	}
	resolversContainerFactory, err := resolverscontainer.NewShardResolversContainerFactory(resolversContainerFactoryArgs)
	if err != nil {
		return nil, err
	}

	return resolversContainerFactory, nil
}

func (pcf *processComponentsFactory) newMetaResolverContainerFactory(
	currentEpochProvider dataRetriever.CurrentNetworkEpochProviderHandler,
) (dataRetriever.ResolversContainerFactory, error) {

	dataPacker, err := partitioning.NewSimpleDataPacker(pcf.coreData.InternalMarshalizer())
	if err != nil {
		return nil, err
	}

	resolversContainerFactoryArgs := resolverscontainer.FactoryArgs{
		ShardCoordinator:            pcf.bootstrapComponents.ShardCoordinator(),
		Messenger:                   pcf.network.NetworkMessenger(),
		Store:                       pcf.data.StorageService(),
		Marshalizer:                 pcf.coreData.InternalMarshalizer(),
		DataPools:                   pcf.data.Datapool(),
		Uint64ByteSliceConverter:    pcf.coreData.Uint64ByteSliceConverter(),
		DataPacker:                  dataPacker,
		TriesContainer:              pcf.state.TriesContainer(),
		SizeCheckDelta:              pcf.config.Marshalizer.SizeCheckDelta,
		InputAntifloodHandler:       pcf.network.InputAntiFloodHandler(),
		OutputAntifloodHandler:      pcf.network.OutputAntiFloodHandler(),
		NumConcurrentResolvingJobs:  pcf.config.Antiflood.NumConcurrentResolverJobs,
		IsFullHistoryNode:           pcf.prefConfigs.FullArchive,
		CurrentNetworkEpochProvider: currentEpochProvider,
		ResolverConfig:              pcf.config.Resolvers,
		PreferredPeersHolder:        pcf.network.PreferredPeersHolderHandler(),
	}
	resolversContainerFactory, err := resolverscontainer.NewMetaResolversContainerFactory(resolversContainerFactoryArgs)
	if err != nil {
		return nil, err
	}
	return resolversContainerFactory, nil
}

func (pcf *processComponentsFactory) newInterceptorContainerFactory(
	headerSigVerifier process.InterceptedHeaderSigVerifier,
	headerIntegrityVerifier factory.HeaderIntegrityVerifierHandler,
	validityAttester process.ValidityAttester,
	epochStartTrigger process.EpochStartTriggerHandler,
	requestHandler process.RequestHandler,
) (process.InterceptorsContainerFactory, process.TimeCacher, error) {
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() < pcf.bootstrapComponents.ShardCoordinator().NumberOfShards() {
		return pcf.newShardInterceptorContainerFactory(
			headerSigVerifier,
			headerIntegrityVerifier,
			validityAttester,
			epochStartTrigger,
			requestHandler,
		)
	}
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		return pcf.newMetaInterceptorContainerFactory(
			headerSigVerifier,
			headerIntegrityVerifier,
			validityAttester,
			epochStartTrigger,
			requestHandler,
		)
	}

	return nil, nil, errors.New("could not create interceptor container factory")
}

func (pcf *processComponentsFactory) newStorageResolver() (dataRetriever.ResolversContainerFactory, error) {
	pathManager, err := storageFactory.CreatePathManager(
		storageFactory.ArgCreatePathManager{
			WorkingDir: pcf.importDBConfig.ImportDBWorkingDir,
			ChainID:    pcf.coreData.ChainID(),
		},
	)
	if err != nil {
		return nil, err
	}

	manualEpochStartNotifier := notifier.NewManualEpochStartNotifier()
	defer func() {
		// we need to call this after we wired all the notified components
		if pcf.importDBConfig.IsImportDBMode {
			manualEpochStartNotifier.NewEpoch(pcf.bootstrapComponents.EpochBootstrapParams().Epoch() + 1)
		}
	}()

	storageServiceCreator, err := storageFactory.NewStorageServiceFactory(
		&pcf.config,
		&pcf.prefConfigs,
		pcf.bootstrapComponents.ShardCoordinator(),
		pathManager,
		manualEpochStartNotifier,
		pcf.coreData.NodeTypeProvider(),
		pcf.bootstrapComponents.EpochBootstrapParams().Epoch(),
		false,
	)
	if err != nil {
		return nil, err
	}

	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		store, errStore := storageServiceCreator.CreateForMeta()
		if errStore != nil {
			return nil, errStore
		}

		return pcf.createStorageResolversForMeta(
			store,
			manualEpochStartNotifier,
		)
	}

	store, err := storageServiceCreator.CreateForShard()
	if err != nil {
		return nil, err
	}

	return pcf.createStorageResolversForShard(
		store,
		manualEpochStartNotifier,
	)
}

func (pcf *processComponentsFactory) createStorageResolversForMeta(
	store dataRetriever.StorageService,
	manualEpochStartNotifier dataRetriever.ManualEpochStartNotifier,
) (dataRetriever.ResolversContainerFactory, error) {
	dataPacker, err := partitioning.NewSimpleDataPacker(pcf.coreData.InternalMarshalizer())
	if err != nil {
		return nil, err
	}

	resolversContainerFactoryArgs := storageResolversContainers.FactoryArgs{
		ShardCoordinator:         pcf.bootstrapComponents.ShardCoordinator(),
		Messenger:                pcf.network.NetworkMessenger(),
		Store:                    store,
		Marshalizer:              pcf.coreData.InternalMarshalizer(),
		Uint64ByteSliceConverter: pcf.coreData.Uint64ByteSliceConverter(),
		DataPacker:               dataPacker,
		ManualEpochStartNotifier: manualEpochStartNotifier,
		ChanGracefullyClose:      pcf.coreData.ChanStopNodeProcess(),
		Hasher:                   pcf.coreData.Hasher(),
		GeneralConfig:            pcf.config,
		ShardIDForTries:          pcf.importDBConfig.ImportDBTargetShardID,
		WorkingDirectory:         pcf.importDBConfig.ImportDBWorkingDir,
		ChainID:                  pcf.coreData.ChainID(),
	}
	resolversContainerFactory, err := storageResolversContainers.NewMetaResolversContainerFactory(resolversContainerFactoryArgs)
	if err != nil {
		return nil, err
	}

	return resolversContainerFactory, nil
}

func (pcf *processComponentsFactory) createStorageResolversForShard(
	store dataRetriever.StorageService,
	manualEpochStartNotifier dataRetriever.ManualEpochStartNotifier,
) (dataRetriever.ResolversContainerFactory, error) {
	dataPacker, err := partitioning.NewSimpleDataPacker(pcf.coreData.InternalMarshalizer())
	if err != nil {
		return nil, err
	}

	resolversContainerFactoryArgs := storageResolversContainers.FactoryArgs{
		ShardCoordinator:         pcf.bootstrapComponents.ShardCoordinator(),
		Messenger:                pcf.network.NetworkMessenger(),
		Store:                    store,
		Marshalizer:              pcf.coreData.InternalMarshalizer(),
		Uint64ByteSliceConverter: pcf.coreData.Uint64ByteSliceConverter(),
		DataPacker:               dataPacker,
		ManualEpochStartNotifier: manualEpochStartNotifier,
		ChanGracefullyClose:      pcf.coreData.ChanStopNodeProcess(),
		Hasher:                   pcf.coreData.Hasher(),
		GeneralConfig:            pcf.config,
		ShardIDForTries:          pcf.importDBConfig.ImportDBTargetShardID,
		WorkingDirectory:         pcf.importDBConfig.ImportDBWorkingDir,
		ChainID:                  pcf.coreData.ChainID(),
	}
	resolversContainerFactory, err := storageResolversContainers.NewShardResolversContainerFactory(resolversContainerFactoryArgs)
	if err != nil {
		return nil, err
	}

	return resolversContainerFactory, nil
}

func (pcf *processComponentsFactory) newShardInterceptorContainerFactory(
	headerSigVerifier process.InterceptedHeaderSigVerifier,
	headerIntegrityVerifier factory.HeaderIntegrityVerifierHandler,
	validityAttester process.ValidityAttester,
	epochStartTrigger process.EpochStartTriggerHandler,
	requestHandler process.RequestHandler,
) (process.InterceptorsContainerFactory, process.TimeCacher, error) {
	headerBlackList := timecache.NewTimeCache(timeSpanForBadHeaders)
	shardInterceptorsContainerFactoryArgs := interceptorscontainer.CommonInterceptorsContainerFactoryArgs{
		CoreComponents:            pcf.coreData,
		CryptoComponents:          pcf.crypto,
		Accounts:                  pcf.state.AccountsAdapter(),
		ShardCoordinator:          pcf.bootstrapComponents.ShardCoordinator(),
		NodesCoordinator:          pcf.nodesCoordinator,
		Messenger:                 pcf.network.NetworkMessenger(),
		Store:                     pcf.data.StorageService(),
		DataPool:                  pcf.data.Datapool(),
		MaxTxNonceDeltaAllowed:    core.MaxTxNonceDeltaAllowed,
		TxFeeHandler:              pcf.coreData.EconomicsData(),
		BlockBlackList:            headerBlackList,
		HeaderSigVerifier:         headerSigVerifier,
		HeaderIntegrityVerifier:   headerIntegrityVerifier,
		ValidityAttester:          validityAttester,
		EpochStartTrigger:         epochStartTrigger,
		WhiteListHandler:          pcf.whiteListHandler,
		WhiteListerVerifiedTxs:    pcf.whiteListerVerifiedTxs,
		AntifloodHandler:          pcf.network.InputAntiFloodHandler(),
		ArgumentsParser:           smartContract.NewArgumentParser(),
		SizeCheckDelta:            pcf.config.Marshalizer.SizeCheckDelta,
		EnableSignTxWithHashEpoch: pcf.epochConfig.EnableEpochs.TransactionSignedWithTxHashEnableEpoch,
		PreferredPeersHolder:      pcf.network.PreferredPeersHolderHandler(),
		RequestHandler:            requestHandler,
	}
	log.Debug("shardInterceptor: enable epoch for transaction signed with tx hash", "epoch", shardInterceptorsContainerFactoryArgs.EnableSignTxWithHashEpoch)

	interceptorContainerFactory, err := interceptorscontainer.NewShardInterceptorsContainerFactory(shardInterceptorsContainerFactoryArgs)
	if err != nil {
		return nil, nil, err
	}

	return interceptorContainerFactory, headerBlackList, nil
}

func (pcf *processComponentsFactory) newMetaInterceptorContainerFactory(
	headerSigVerifier process.InterceptedHeaderSigVerifier,
	headerIntegrityVerifier factory.HeaderIntegrityVerifierHandler,
	validityAttester process.ValidityAttester,
	epochStartTrigger process.EpochStartTriggerHandler,
	requestHandler process.RequestHandler,
) (process.InterceptorsContainerFactory, process.TimeCacher, error) {
	headerBlackList := timecache.NewTimeCache(timeSpanForBadHeaders)
	metaInterceptorsContainerFactoryArgs := interceptorscontainer.CommonInterceptorsContainerFactoryArgs{
		CoreComponents:            pcf.coreData,
		CryptoComponents:          pcf.crypto,
		ShardCoordinator:          pcf.bootstrapComponents.ShardCoordinator(),
		NodesCoordinator:          pcf.nodesCoordinator,
		Messenger:                 pcf.network.NetworkMessenger(),
		Store:                     pcf.data.StorageService(),
		DataPool:                  pcf.data.Datapool(),
		Accounts:                  pcf.state.AccountsAdapter(),
		MaxTxNonceDeltaAllowed:    core.MaxTxNonceDeltaAllowed,
		TxFeeHandler:              pcf.coreData.EconomicsData(),
		BlockBlackList:            headerBlackList,
		HeaderSigVerifier:         headerSigVerifier,
		HeaderIntegrityVerifier:   headerIntegrityVerifier,
		ValidityAttester:          validityAttester,
		EpochStartTrigger:         epochStartTrigger,
		WhiteListHandler:          pcf.whiteListHandler,
		WhiteListerVerifiedTxs:    pcf.whiteListerVerifiedTxs,
		AntifloodHandler:          pcf.network.InputAntiFloodHandler(),
		ArgumentsParser:           smartContract.NewArgumentParser(),
		SizeCheckDelta:            pcf.config.Marshalizer.SizeCheckDelta,
		EnableSignTxWithHashEpoch: pcf.epochConfig.EnableEpochs.TransactionSignedWithTxHashEnableEpoch,
		PreferredPeersHolder:      pcf.network.PreferredPeersHolderHandler(),
		RequestHandler:            requestHandler,
	}
	log.Debug("metaInterceptor: enable epoch for transaction signed with tx hash", "epoch", metaInterceptorsContainerFactoryArgs.EnableSignTxWithHashEpoch)

	interceptorContainerFactory, err := interceptorscontainer.NewMetaInterceptorsContainerFactory(metaInterceptorsContainerFactoryArgs)
	if err != nil {
		return nil, nil, err
	}

	return interceptorContainerFactory, headerBlackList, nil
}

func (pcf *processComponentsFactory) newForkDetector(
	headerBlackList process.TimeCacher,
	blockTracker process.BlockTracker,
) (process.ForkDetector, error) {
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() < pcf.bootstrapComponents.ShardCoordinator().NumberOfShards() {
		return sync.NewShardForkDetector(pcf.coreData.RoundHandler(), headerBlackList, blockTracker, pcf.coreData.GenesisNodesSetup().GetStartTime())
	}
	if pcf.bootstrapComponents.ShardCoordinator().SelfId() == core.MetachainShardId {
		return sync.NewMetaForkDetector(pcf.coreData.RoundHandler(), headerBlackList, blockTracker, pcf.coreData.GenesisNodesSetup().GetStartTime())
	}

	return nil, errors.New("could not create fork detector")
}

// PrepareNetworkShardingCollector will create the network sharding collector and apply it to the network messenger
func (pcf *processComponentsFactory) prepareNetworkShardingCollector() (*networksharding.PeerShardMapper, error) {
	networkShardingCollector, err := createNetworkShardingCollector(
		&pcf.config,
		pcf.nodesCoordinator,
		pcf.coreData.EpochStartNotifierWithConfirm(),
		pcf.network.PreferredPeersHolderHandler(),
		pcf.bootstrapComponents.EpochBootstrapParams().Epoch(),
	)
	if err != nil {
		return nil, err
	}

	localID := pcf.network.NetworkMessenger().ID()
	networkShardingCollector.UpdatePeerIDInfo(localID, pcf.crypto.PublicKeyBytes(), pcf.bootstrapComponents.ShardCoordinator().SelfId())

	err = pcf.network.NetworkMessenger().SetPeerShardResolver(networkShardingCollector)
	if err != nil {
		return nil, err
	}

	err = pcf.network.InputAntiFloodHandler().SetPeerValidatorMapper(networkShardingCollector)
	if err != nil {
		return nil, err
	}

	return networkShardingCollector, nil
}

func createNetworkShardingCollector(
	config *config.Config,
	nodesCoordinator sharding.NodesCoordinator,
	epochStartRegistrationHandler epochStart.RegistrationHandler,
	preferredPeersHolder PreferredPeersHolderHandler,
	epochStart uint32,
) (*networksharding.PeerShardMapper, error) {

	cacheConfig := config.PublicKeyPeerId
	cachePkPid, err := createCache(cacheConfig)
	if err != nil {
		return nil, err
	}

	cacheConfig = config.PublicKeyShardId
	cachePkShardID, err := createCache(cacheConfig)
	if err != nil {
		return nil, err
	}

	cacheConfig = config.PeerIdShardId
	cachePidShardID, err := createCache(cacheConfig)
	if err != nil {
		return nil, err
	}

	arg := networksharding.ArgPeerShardMapper{
		PeerIdPkCache:         cachePkPid,
		FallbackPkShardCache:  cachePkShardID,
		FallbackPidShardCache: cachePidShardID,
		NodesCoordinator:      nodesCoordinator,
		PreferredPeersHolder:  preferredPeersHolder,
		StartEpoch:            epochStart,
	}
	psm, err := networksharding.NewPeerShardMapper(arg)
	if err != nil {
		return nil, err
	}

	epochStartRegistrationHandler.RegisterHandler(psm)

	return psm, nil
}

func createCache(cacheConfig config.CacheConfig) (storage.Cacher, error) {
	return storageUnit.NewCache(storageFactory.GetCacherFromConfig(cacheConfig))
}

func checkProcessComponentsArgs(args ProcessComponentsFactoryArgs) error {
	baseErrMessage := "error creating process components"
	if check.IfNil(args.AccountsParser) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilAccountsParser)
	}
	if check.IfNil(args.SmartContractParser) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilSmartContractParser)
	}
	if args.GasSchedule == nil {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilGasSchedule)
	}
	if check.IfNil(args.NodesCoordinator) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilNodesCoordinator)
	}
	if check.IfNil(args.Data) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilDataComponentsHolder)
	}
	if check.IfNil(args.CoreData) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilCoreComponentsHolder)
	}
	if args.CoreData.EconomicsData() == nil {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilEconomicsData)
	}
	if check.IfNil(args.CoreData.RoundHandler()) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilRoundHandler)
	}
	if check.IfNil(args.Crypto) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilCryptoComponentsHolder)
	}
	if check.IfNil(args.State) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilStateComponentsHolder)
	}
	if check.IfNil(args.Network) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilNetworkComponentsHolder)
	}
	if check.IfNil(args.RequestedItemsHandler) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilRequestedItemHandler)
	}
	if check.IfNil(args.WhiteListHandler) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilWhiteListHandler)
	}
	if check.IfNil(args.WhiteListerVerifiedTxs) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilWhiteListVerifiedTxs)
	}
	if check.IfNil(args.CoreData.EpochStartNotifierWithConfirm()) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilEpochStartNotifier)
	}
	if check.IfNil(args.CoreData.Rater()) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilRater)
	}
	if check.IfNil(args.CoreData.RatingsData()) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilRatingData)
	}
	if check.IfNil(args.CoreData.ValidatorPubKeyConverter()) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilPubKeyConverter)
	}
	if args.SystemSCConfig == nil {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilSystemSCConfig)
	}
	if check.IfNil(args.CoreData.EpochNotifier()) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilEpochNotifier)
	}
	if check.IfNil(args.BootstrapComponents) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilBootstrapComponentsHolder)
	}
	if check.IfNil(args.BootstrapComponents.ShardCoordinator()) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilShardCoordinator)
	}
	if check.IfNil(args.StatusComponents) {
		return fmt.Errorf("%s: %w", baseErrMessage, errErd.ErrNilStatusComponentsHolder)
	}

	return nil
}

// Close closes all underlying components that need closing
func (pc *processComponents) Close() error {
	if !check.IfNil(pc.blockProcessor) {
		log.LogIfError(pc.blockProcessor.Close())
	}
	if !check.IfNil(pc.validatorsProvider) {
		log.LogIfError(pc.validatorsProvider.Close())
	}
	if !check.IfNil(pc.miniBlocksPoolCleaner) {
		log.LogIfError(pc.miniBlocksPoolCleaner.Close())
	}
	if !check.IfNil(pc.txsPoolCleaner) {
		log.LogIfError(pc.txsPoolCleaner.Close())
	}
	if !check.IfNil(pc.epochStartTrigger) {
		log.LogIfError(pc.epochStartTrigger.Close())
	}
	if !check.IfNil(pc.importHandler) {
		log.LogIfError(pc.importHandler.Close())
	}
	if !check.IfNil(pc.interceptorsContainer) {
		log.LogIfError(pc.interceptorsContainer.Close())
	}
	return nil
}
