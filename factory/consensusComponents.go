package factory

import (
	"time"

	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/consensus/chronology"
	"github.com/ElrondNetwork/elrond-go/consensus/spos"
	"github.com/ElrondNetwork/elrond-go/consensus/spos/sposFactory"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/throttler"
	"github.com/ElrondNetwork/elrond-go/core/watchdog"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/syncer"
	"github.com/ElrondNetwork/elrond-go/data/trie/factory"
	"github.com/ElrondNetwork/elrond-go/errors"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/sync"
	"github.com/ElrondNetwork/elrond-go/process/sync/storageBootstrap"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/update"
)

const timeoutGettingTrieNode = time.Minute

// ConsensusComponentsFactoryArgs holds the arguments needed to create a consensus components factory
type ConsensusComponentsFactoryArgs struct {
	Config              config.Config
	BootstrapRoundIndex uint64
	HardforkTrigger     HardforkTrigger
	CoreComponents      CoreComponentsHolder
	NetworkComponents   NetworkComponentsHolder
	CryptoComponents    CryptoComponentsHolder
	DataComponents      DataComponentsHolder
	ProcessComponents   ProcessComponentsHolder
	StateComponents     StateComponentsHolder
	StatusComponents    StatusComponentsHolder
	IsInImportMode      bool
}

type consensusComponentsFactory struct {
	config              config.Config
	bootstrapRoundIndex uint64
	hardforkTrigger     HardforkTrigger
	coreComponents      CoreComponentsHolder
	networkComponents   NetworkComponentsHolder
	cryptoComponents    CryptoComponentsHolder
	dataComponents      DataComponentsHolder
	processComponents   ProcessComponentsHolder
	stateComponents     StateComponentsHolder
	statusComponents    StatusComponentsHolder
	isInImportMode      bool
}

type consensusComponents struct {
	chronology         consensus.ChronologyHandler
	bootstrapper       process.Bootstrapper
	broadcastMessenger consensus.BroadcastMessenger
	worker             ConsensusWorker
	hardforkTrigger    HardforkTrigger
	consensusTopic     string
	consensusGroupSize int
}

// NewConsensusComponentsFactory creates an instance of consensusComponentsFactory
func NewConsensusComponentsFactory(args ConsensusComponentsFactoryArgs) (*consensusComponentsFactory, error) {
	if check.IfNil(args.CoreComponents) {
		return nil, errors.ErrNilCoreComponentsHolder
	}
	if check.IfNil(args.DataComponents) {
		return nil, errors.ErrNilDataComponentsHolder
	}
	if check.IfNil(args.CryptoComponents) {
		return nil, errors.ErrNilCryptoComponentsHolder
	}
	if check.IfNil(args.NetworkComponents) {
		return nil, errors.ErrNilNetworkComponentsHolder
	}
	if check.IfNil(args.ProcessComponents) {
		return nil, errors.ErrNilProcessComponentsHolder
	}
	if check.IfNil(args.StateComponents) {
		return nil, errors.ErrNilStateComponentsHolder
	}
	if check.IfNil(args.StatusComponents) {
		return nil, errors.ErrNilStatusComponentsHolder
	}
	if check.IfNil(args.HardforkTrigger) {
		return nil, errors.ErrNilHardforkTrigger
	}

	return &consensusComponentsFactory{
		config:              args.Config,
		bootstrapRoundIndex: args.BootstrapRoundIndex,
		hardforkTrigger:     args.HardforkTrigger,
		coreComponents:      args.CoreComponents,
		networkComponents:   args.NetworkComponents,
		cryptoComponents:    args.CryptoComponents,
		dataComponents:      args.DataComponents,
		processComponents:   args.ProcessComponents,
		stateComponents:     args.StateComponents,
		statusComponents:    args.StatusComponents,
		isInImportMode:      args.IsInImportMode,
	}, nil
}

// Create will init all the components needed for a new instance of consensusComponents
func (ccf *consensusComponentsFactory) Create() (*consensusComponents, error) {
	var err error

	err = ccf.checkArgs()
	if err != nil {
		return nil, err
	}
	cc := &consensusComponents{}

	consensusGroupSize, err := getConsensusGroupSize(ccf.coreComponents.GenesisNodesSetup(), ccf.processComponents.ShardCoordinator())
	if err != nil {
		return nil, err
	}

	cc.consensusGroupSize = int(consensusGroupSize)

	cc.hardforkTrigger = ccf.hardforkTrigger
	blockchain := ccf.dataComponents.Blockchain()
	notInitializedGenesisBlock := len(blockchain.GetGenesisHeaderHash()) == 0 ||
		check.IfNil(blockchain.GetGenesisHeader())
	if notInitializedGenesisBlock {
		return nil, errors.ErrGenesisBlockNotInitialized
	}

	cc.chronology, err = ccf.createChronology()
	if err != nil {
		return nil, err
	}

	cc.bootstrapper, err = ccf.createBootstrapper()
	if err != nil {
		return nil, err
	}

	cc.bootstrapper.StartSyncingBlocks()

	epoch := ccf.getEpoch()
	consensusState, err := ccf.createConsensusState(epoch, cc.consensusGroupSize)
	if err != nil {
		return nil, err
	}

	consensusService, err := sposFactory.GetConsensusCoreFactory(ccf.config.Consensus.Type)
	if err != nil {
		return nil, err
	}

	cc.broadcastMessenger, err = sposFactory.GetBroadcastMessenger(
		ccf.coreComponents.InternalMarshalizer(),
		ccf.coreComponents.Hasher(),
		ccf.networkComponents.NetworkMessenger(),
		ccf.processComponents.ShardCoordinator(),
		ccf.cryptoComponents.PrivateKey(),
		ccf.cryptoComponents.PeerSignatureHandler(),
		ccf.dataComponents.Datapool().Headers(),
		ccf.processComponents.InterceptorsContainer(),
		ccf.coreComponents.AlarmScheduler(),
	)
	if err != nil {
		return nil, err
	}

	marshalizer := ccf.coreComponents.InternalMarshalizer()
	sizeCheckDelta := ccf.config.Marshalizer.SizeCheckDelta
	if sizeCheckDelta > 0 {
		marshalizer = marshal.NewSizeCheckUnmarshalizer(marshalizer, sizeCheckDelta)
	}

	workerArgs := &spos.WorkerArgs{
		ConsensusService:         consensusService,
		BlockChain:               ccf.dataComponents.Blockchain(),
		BlockProcessor:           ccf.processComponents.BlockProcessor(),
		Bootstrapper:             cc.bootstrapper,
		BroadcastMessenger:       cc.broadcastMessenger,
		ConsensusState:           consensusState,
		ForkDetector:             ccf.processComponents.ForkDetector(),
		PeerSignatureHandler:     ccf.cryptoComponents.PeerSignatureHandler(),
		Marshalizer:              marshalizer,
		Hasher:                   ccf.coreComponents.Hasher(),
		RoundHandler:             ccf.processComponents.RoundHandler(),
		ShardCoordinator:         ccf.processComponents.ShardCoordinator(),
		SyncTimer:                ccf.coreComponents.SyncTimer(),
		HeaderSigVerifier:        ccf.processComponents.HeaderSigVerifier(),
		HeaderIntegrityVerifier:  ccf.processComponents.HeaderIntegrityVerifier(),
		ChainID:                  []byte(ccf.coreComponents.ChainID()),
		NetworkShardingCollector: ccf.processComponents.PeerShardMapper(),
		AntifloodHandler:         ccf.networkComponents.InputAntiFloodHandler(),
		PoolAdder:                ccf.dataComponents.Datapool().MiniBlocks(),
		SignatureSize:            ccf.config.ValidatorPubkeyConverter.SignatureLength,
		PublicKeySize:            ccf.config.ValidatorPubkeyConverter.Length,
		AppStatusHandler:         ccf.coreComponents.StatusHandler(),
		NodeRedundancyHandler:    ccf.processComponents.NodeRedundancyHandler(),
	}

	cc.worker, err = spos.NewWorker(workerArgs)
	if err != nil {
		return nil, err
	}

	cc.worker.StartWorking()
	ccf.dataComponents.Datapool().Headers().RegisterHandler(cc.worker.ReceivedHeader)

	// apply consensus group size on the input antiflooder just before consensus creation topic
	ccf.networkComponents.InputAntiFloodHandler().ApplyConsensusSize(
		ccf.processComponents.NodesCoordinator().ConsensusGroupSize(
			ccf.processComponents.ShardCoordinator().SelfId()),
	)
	err = ccf.createConsensusTopic(cc)
	if err != nil {
		return nil, err
	}

	consensusArgs := &spos.ConsensusCoreArgs{
		BlockChain:                    ccf.dataComponents.Blockchain(),
		BlockProcessor:                ccf.processComponents.BlockProcessor(),
		Bootstrapper:                  cc.bootstrapper,
		BroadcastMessenger:            cc.broadcastMessenger,
		ChronologyHandler:             cc.chronology,
		Hasher:                        ccf.coreComponents.Hasher(),
		Marshalizer:                   ccf.coreComponents.InternalMarshalizer(),
		BlsPrivateKey:                 ccf.cryptoComponents.PrivateKey(),
		BlsSingleSigner:               ccf.cryptoComponents.BlockSigner(),
		MultiSigner:                   ccf.cryptoComponents.MultiSigner(),
		RoundHandler:                  ccf.processComponents.RoundHandler(),
		ShardCoordinator:              ccf.processComponents.ShardCoordinator(),
		NodesCoordinator:              ccf.processComponents.NodesCoordinator(),
		SyncTimer:                     ccf.coreComponents.SyncTimer(),
		EpochStartRegistrationHandler: ccf.processComponents.EpochStartNotifier(),
		AntifloodHandler:              ccf.networkComponents.InputAntiFloodHandler(),
		PeerHonestyHandler:            ccf.networkComponents.PeerHonestyHandler(),
		HeaderSigVerifier:             ccf.processComponents.HeaderSigVerifier(),
		FallbackHeaderValidator:       ccf.processComponents.FallbackHeaderValidator(),
		NodeRedundancyHandler:         ccf.processComponents.NodeRedundancyHandler(),
	}

	consensusDataContainer, err := spos.NewConsensusCore(
		consensusArgs,
	)
	if err != nil {
		return nil, err
	}

	fct, err := sposFactory.GetSubroundsFactory(
		consensusDataContainer,
		consensusState,
		cc.worker,
		ccf.config.Consensus.Type,
		ccf.coreComponents.StatusHandler(),
		ccf.statusComponents.OutportHandler(),
		[]byte(ccf.coreComponents.ChainID()),
		ccf.networkComponents.NetworkMessenger().ID(),
	)
	if err != nil {
		return nil, err
	}

	err = fct.GenerateSubrounds()
	if err != nil {
		return nil, err
	}

	cc.chronology.StartRounds()

	err = ccf.addCloserInstances(cc.chronology, cc.bootstrapper, cc.worker, ccf.coreComponents.SyncTimer())
	if err != nil {
		return nil, err
	}

	return cc, nil
}

// Close will close all the inner components
func (cc *consensusComponents) Close() error {
	err := cc.chronology.Close()
	if err != nil {
		// todo: maybe just log error and try to close as much as possible
		return err
	}
	err = cc.worker.Close()
	if err != nil {
		return err
	}
	err = cc.bootstrapper.Close()
	if err != nil {
		return err
	}

	return nil
}

func (ccf *consensusComponentsFactory) createChronology() (consensus.ChronologyHandler, error) {
	wd := ccf.coreComponents.Watchdog()
	if ccf.statusComponents.OutportHandler().HasDrivers() {
		log.Warn("node is running with an outport with attached drivers. Chronology watchdog will be turned off as " +
			"it is incompatible with the indexing process.")
		wd = &watchdog.DisabledWatchdog{}
	}
	if ccf.isInImportMode {
		log.Warn("node is running in import mode. Chronology watchdog will be turned off as " +
			"it is incompatible with the import-db process.")
		wd = &watchdog.DisabledWatchdog{}
	}

	chronologyArg := chronology.ArgChronology{
		GenesisTime:      ccf.coreComponents.GenesisTime(),
		RoundHandler:     ccf.processComponents.RoundHandler(),
		SyncTimer:        ccf.coreComponents.SyncTimer(),
		Watchdog:         wd,
		AppStatusHandler: ccf.coreComponents.StatusHandler(),
	}
	chronologyHandler, err := chronology.NewChronology(chronologyArg)
	if err != nil {
		return nil, err
	}

	return chronologyHandler, nil
}

func (ccf *consensusComponentsFactory) getEpoch() uint32 {
	blockchain := ccf.dataComponents.Blockchain()
	epoch := blockchain.GetGenesisHeader().GetEpoch()
	crtBlockHeader := blockchain.GetCurrentBlockHeader()
	if !check.IfNil(crtBlockHeader) {
		epoch = crtBlockHeader.GetEpoch()
	}
	log.Info("starting consensus", "epoch", epoch)

	return epoch
}

// createConsensusState method creates a consensusState object
func (ccf *consensusComponentsFactory) createConsensusState(epoch uint32, consensusGroupSize int) (*spos.ConsensusState, error) {
	if ccf.cryptoComponents.PublicKey() == nil {
		return nil, errors.ErrNilPublicKey
	}
	selfId, err := ccf.cryptoComponents.PublicKey().ToByteArray()
	if err != nil {
		return nil, err
	}

	if check.IfNil(ccf.processComponents.NodesCoordinator()) {
		return nil, errors.ErrNilNodesCoordinator
	}
	eligibleNodesPubKeys, err := ccf.processComponents.NodesCoordinator().GetConsensusWhitelistedNodes(epoch)
	if err != nil {
		return nil, err
	}

	roundConsensus := spos.NewRoundConsensus(
		eligibleNodesPubKeys,
		// TODO: move the consensus data from nodesSetup json to config
		consensusGroupSize,
		string(selfId))

	roundConsensus.ResetRoundState()

	roundThreshold := spos.NewRoundThreshold()

	roundStatus := spos.NewRoundStatus()
	roundStatus.ResetRoundStatus()

	consensusState := spos.NewConsensusState(
		roundConsensus,
		roundThreshold,
		roundStatus)

	return consensusState, nil
}

func (ccf *consensusComponentsFactory) createBootstrapper() (process.Bootstrapper, error) {
	shardCoordinator := ccf.processComponents.ShardCoordinator()
	if check.IfNil(shardCoordinator) {
		return nil, errors.ErrNilShardCoordinator
	}

	if shardCoordinator.SelfId() < shardCoordinator.NumberOfShards() {
		return ccf.createShardBootstrapper()
	}

	if shardCoordinator.SelfId() == core.MetachainShardId {
		return ccf.createMetaChainBootstrapper()
	}

	return nil, sharding.ErrShardIdOutOfRange
}

func (ccf *consensusComponentsFactory) createShardBootstrapper() (process.Bootstrapper, error) {
	argsBaseStorageBootstrapper := storageBootstrap.ArgsBaseStorageBootstrapper{
		BootStorer:          ccf.processComponents.BootStorer(),
		ForkDetector:        ccf.processComponents.ForkDetector(),
		BlockProcessor:      ccf.processComponents.BlockProcessor(),
		ChainHandler:        ccf.dataComponents.Blockchain(),
		Marshalizer:         ccf.coreComponents.InternalMarshalizer(),
		Store:               ccf.dataComponents.StorageService(),
		Uint64Converter:     ccf.coreComponents.Uint64ByteSliceConverter(),
		BootstrapRoundIndex: ccf.bootstrapRoundIndex,
		ShardCoordinator:    ccf.processComponents.ShardCoordinator(),
		NodesCoordinator:    ccf.processComponents.NodesCoordinator(),
		EpochStartTrigger:   ccf.processComponents.EpochStartTrigger(),
		BlockTracker:        ccf.processComponents.BlockTracker(),
		ChainID:             ccf.coreComponents.ChainID(),
	}

	argsShardStorageBootstrapper := storageBootstrap.ArgsShardStorageBootstrapper{
		ArgsBaseStorageBootstrapper: argsBaseStorageBootstrapper,
	}

	shardStorageBootstrapper, err := storageBootstrap.NewShardStorageBootstrapper(argsShardStorageBootstrapper)
	if err != nil {
		return nil, err
	}

	accountsDBSyncer, err := ccf.createUserAccountsSyncer()
	if err != nil {
		return nil, err
	}

	argsBaseBootstrapper := sync.ArgBaseBootstrapper{
		PoolsHolder:          ccf.dataComponents.Datapool(),
		Store:                ccf.dataComponents.StorageService(),
		ChainHandler:         ccf.dataComponents.Blockchain(),
		RoundHandler:         ccf.processComponents.RoundHandler(),
		BlockProcessor:       ccf.processComponents.BlockProcessor(),
		WaitTime:             ccf.processComponents.RoundHandler().TimeDuration(),
		Hasher:               ccf.coreComponents.Hasher(),
		Marshalizer:          ccf.coreComponents.InternalMarshalizer(),
		ForkDetector:         ccf.processComponents.ForkDetector(),
		RequestHandler:       ccf.processComponents.RequestHandler(),
		ShardCoordinator:     ccf.processComponents.ShardCoordinator(),
		Accounts:             ccf.stateComponents.AccountsAdapter(),
		BlackListHandler:     ccf.processComponents.BlackListHandler(),
		NetworkWatcher:       ccf.networkComponents.NetworkMessenger(),
		BootStorer:           ccf.processComponents.BootStorer(),
		StorageBootstrapper:  shardStorageBootstrapper,
		EpochHandler:         ccf.processComponents.EpochStartTrigger(),
		MiniblocksProvider:   ccf.dataComponents.MiniBlocksProvider(),
		Uint64Converter:      ccf.coreComponents.Uint64ByteSliceConverter(),
		AppStatusHandler:     ccf.coreComponents.StatusHandler(),
		OutportHandler:      ccf.statusComponents.OutportHandler(),
		AccountsDBSyncer:     accountsDBSyncer,
		CurrentEpochProvider: ccf.processComponents.CurrentEpochProvider(),
		IsInImportMode:       ccf.isInImportMode,
	}

	argsShardBootstrapper := sync.ArgShardBootstrapper{
		ArgBaseBootstrapper: argsBaseBootstrapper,
	}

	bootstrap, err := sync.NewShardBootstrap(argsShardBootstrapper)
	if err != nil {
		return nil, err
	}

	return bootstrap, nil
}

func (ccf *consensusComponentsFactory) createArgsBaseAccountsSyncer(trieStorageManager data.StorageManager) syncer.ArgsNewBaseAccountsSyncer {
	return syncer.ArgsNewBaseAccountsSyncer{
		Hasher:                    ccf.coreComponents.Hasher(),
		Marshalizer:               ccf.coreComponents.InternalMarshalizer(),
		TrieStorageManager:        trieStorageManager,
		RequestHandler:            ccf.processComponents.RequestHandler(),
		Timeout:                   timeoutGettingTrieNode,
		Cacher:                    ccf.dataComponents.Datapool().TrieNodes(),
		MaxTrieLevelInMemory:      ccf.config.StateTriesConfig.MaxStateTrieLevelInMemory,
		MaxHardCapForMissingNodes: ccf.config.TrieSync.MaxHardCapForMissingNodes,
		TrieSyncerVersion:         ccf.config.TrieSync.TrieSyncerVersion,
	}
}

func (ccf *consensusComponentsFactory) createValidatorAccountsSyncer() (process.AccountsDBSyncer, error) {
	trieStorageManager, ok := ccf.stateComponents.TrieStorageManagers()[factory.PeerAccountTrie]
	if !ok {
		return nil, errors.ErrNilTrieStorageManager
	}

	args := syncer.ArgsNewValidatorAccountsSyncer{
		ArgsNewBaseAccountsSyncer: ccf.createArgsBaseAccountsSyncer(trieStorageManager),
	}
	return syncer.NewValidatorAccountsSyncer(args)
}

func (ccf *consensusComponentsFactory) createUserAccountsSyncer() (process.AccountsDBSyncer, error) {
	trieStorageManager, ok := ccf.stateComponents.TrieStorageManagers()[factory.UserAccountTrie]
	if !ok {
		return nil, errors.ErrNilTrieStorageManager
	}

	thr, err := throttler.NewNumGoRoutinesThrottler(int32(ccf.config.TrieSync.NumConcurrentTrieSyncers))
	if err != nil {
		return nil, err
	}

	argsUserAccountsSyncer := syncer.ArgsNewUserAccountsSyncer{
		ArgsNewBaseAccountsSyncer: ccf.createArgsBaseAccountsSyncer(trieStorageManager),
		ShardId:                   ccf.processComponents.ShardCoordinator().SelfId(),
		Throttler:                 thr,
	}
	return syncer.NewUserAccountsSyncer(argsUserAccountsSyncer)
}

func (ccf *consensusComponentsFactory) createMetaChainBootstrapper() (process.Bootstrapper, error) {
	argsBaseStorageBootstrapper := storageBootstrap.ArgsBaseStorageBootstrapper{
		BootStorer:          ccf.processComponents.BootStorer(),
		ForkDetector:        ccf.processComponents.ForkDetector(),
		BlockProcessor:      ccf.processComponents.BlockProcessor(),
		ChainHandler:        ccf.dataComponents.Blockchain(),
		Marshalizer:         ccf.coreComponents.InternalMarshalizer(),
		Store:               ccf.dataComponents.StorageService(),
		Uint64Converter:     ccf.coreComponents.Uint64ByteSliceConverter(),
		BootstrapRoundIndex: ccf.bootstrapRoundIndex,
		ShardCoordinator:    ccf.processComponents.ShardCoordinator(),
		NodesCoordinator:    ccf.processComponents.NodesCoordinator(),
		EpochStartTrigger:   ccf.processComponents.EpochStartTrigger(),
		BlockTracker:        ccf.processComponents.BlockTracker(),
		ChainID:             ccf.coreComponents.ChainID(),
	}

	argsMetaStorageBootstrapper := storageBootstrap.ArgsMetaStorageBootstrapper{
		ArgsBaseStorageBootstrapper: argsBaseStorageBootstrapper,
		PendingMiniBlocksHandler:    ccf.processComponents.PendingMiniBlocksHandler(),
	}

	metaStorageBootstrapper, err := storageBootstrap.NewMetaStorageBootstrapper(argsMetaStorageBootstrapper)
	if err != nil {
		return nil, err
	}

	accountsDBSyncer, err := ccf.createUserAccountsSyncer()
	if err != nil {
		return nil, err
	}

	validatorAccountsDBSyncer, err := ccf.createValidatorAccountsSyncer()
	if err != nil {
		return nil, err
	}

	argsBaseBootstrapper := sync.ArgBaseBootstrapper{
		PoolsHolder:          ccf.dataComponents.Datapool(),
		Store:                ccf.dataComponents.StorageService(),
		ChainHandler:         ccf.dataComponents.Blockchain(),
		RoundHandler:         ccf.processComponents.RoundHandler(),
		BlockProcessor:       ccf.processComponents.BlockProcessor(),
		WaitTime:             ccf.processComponents.RoundHandler().TimeDuration(),
		Hasher:               ccf.coreComponents.Hasher(),
		Marshalizer:          ccf.coreComponents.InternalMarshalizer(),
		ForkDetector:         ccf.processComponents.ForkDetector(),
		RequestHandler:       ccf.processComponents.RequestHandler(),
		ShardCoordinator:     ccf.processComponents.ShardCoordinator(),
		Accounts:             ccf.stateComponents.AccountsAdapter(),
		BlackListHandler:     ccf.processComponents.BlackListHandler(),
		NetworkWatcher:       ccf.networkComponents.NetworkMessenger(),
		BootStorer:           ccf.processComponents.BootStorer(),
		StorageBootstrapper:  metaStorageBootstrapper,
		EpochHandler:         ccf.processComponents.EpochStartTrigger(),
		MiniblocksProvider:   ccf.dataComponents.MiniBlocksProvider(),
		Uint64Converter:      ccf.coreComponents.Uint64ByteSliceConverter(),
		AppStatusHandler:     ccf.coreComponents.StatusHandler(),
		OutportHandler:       ccf.statusComponents.OutportHandler(),
		AccountsDBSyncer:     accountsDBSyncer,
		CurrentEpochProvider: ccf.processComponents.CurrentEpochProvider(),
		IsInImportMode:       ccf.isInImportMode,
	}

	argsMetaBootstrapper := sync.ArgMetaBootstrapper{
		ArgBaseBootstrapper:         argsBaseBootstrapper,
		EpochBootstrapper:           ccf.processComponents.EpochStartTrigger(),
		ValidatorAccountsDB:         ccf.stateComponents.PeerAccounts(),
		ValidatorStatisticsDBSyncer: validatorAccountsDBSyncer,
	}

	bootstrap, err := sync.NewMetaBootstrap(argsMetaBootstrapper)
	if err != nil {
		return nil, err
	}

	return bootstrap, nil
}

func (ccf *consensusComponentsFactory) createConsensusTopic(cc *consensusComponents) error {
	shardCoordinator := ccf.processComponents.ShardCoordinator()
	messenger := ccf.networkComponents.NetworkMessenger()

	if check.IfNil(shardCoordinator) {
		return errors.ErrNilShardCoordinator
	}
	if check.IfNil(messenger) {
		return errors.ErrNilMessenger
	}

	cc.consensusTopic = core.ConsensusTopic + shardCoordinator.CommunicationIdentifier(shardCoordinator.SelfId())
	if !ccf.networkComponents.NetworkMessenger().HasTopic(cc.consensusTopic) {
		err := ccf.networkComponents.NetworkMessenger().CreateTopic(cc.consensusTopic, true)
		if err != nil {
			return err
		}
	}

	return ccf.networkComponents.NetworkMessenger().RegisterMessageProcessor(cc.consensusTopic, core.DefaultInterceptorsIdentifier, cc.worker)
}

func (ccf *consensusComponentsFactory) addCloserInstances(closers ...update.Closer) error {
	for _, c := range closers {
		err := ccf.hardforkTrigger.AddCloser(c)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ccf *consensusComponentsFactory) checkArgs() error {
	blockchain := ccf.dataComponents.Blockchain()
	if check.IfNil(blockchain) {
		return errors.ErrNilBlockChainHandler
	}
	marshalizer := ccf.coreComponents.InternalMarshalizer()
	if check.IfNil(marshalizer) {
		return errors.ErrNilMarshalizer
	}
	dataPool := ccf.dataComponents.Datapool()
	if check.IfNil(dataPool) {
		return errors.ErrNilDataPoolsHolder
	}
	shardCoordinator := ccf.processComponents.ShardCoordinator()
	if check.IfNil(shardCoordinator) {
		return errors.ErrNilShardCoordinator
	}
	netMessenger := ccf.networkComponents.NetworkMessenger()
	if check.IfNil(netMessenger) {
		return errors.ErrNilMessenger
	}

	return nil
}

func getConsensusGroupSize(nodesConfig sharding.GenesisNodesSetupHandler, shardCoordinator sharding.Coordinator) (uint32, error) {
	if shardCoordinator.SelfId() == core.MetachainShardId {
		return nodesConfig.GetMetaConsensusGroupSize(), nil
	}
	if shardCoordinator.SelfId() < shardCoordinator.NumberOfShards() {
		return nodesConfig.GetShardConsensusGroupSize(), nil
	}

	return 0, sharding.ErrShardIdOutOfRange
}
