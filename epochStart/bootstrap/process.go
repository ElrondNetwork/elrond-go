package bootstrap

import (
	"bytes"
	"context"
	"fmt"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/partitioning"
	"github.com/ElrondNetwork/elrond-go/core/throttler"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/syncer"
	"github.com/ElrondNetwork/elrond-go/data/trie/factory"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters/uint64ByteSlice"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	factoryDataPool "github.com/ElrondNetwork/elrond-go/dataRetriever/factory"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/factory/containers"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/factory/resolverscontainer"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/requestHandlers"
	"github.com/ElrondNetwork/elrond-go/epochStart"
	"github.com/ElrondNetwork/elrond-go/epochStart/bootstrap/disabled"
	factoryInterceptors "github.com/ElrondNetwork/elrond-go/epochStart/bootstrap/factory"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/interceptors"
	disabledInterceptors "github.com/ElrondNetwork/elrond-go/process/interceptors/disabled"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
	storageFactory "github.com/ElrondNetwork/elrond-go/storage/factory"
	"github.com/ElrondNetwork/elrond-go/storage/storageUnit"
	"github.com/ElrondNetwork/elrond-go/storage/timecache"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/genesis/trieExport"
	"github.com/ElrondNetwork/elrond-go/update/sync"
)

var log = logger.GetOrCreate("epochStart/bootstrap")

const timeToWait = time.Minute
const timeoutGettingTrieNode = time.Minute
const timeBetweenRequests = 100 * time.Millisecond
const maxToRequest = 100
const gracePeriodInPercentage = float64(0.25)
const roundGracePeriod = 25

// Parameters defines the DTO for the result produced by the bootstrap component
type Parameters struct {
	Epoch       uint32
	SelfShardId uint32
	NumOfShards uint32
	NodesConfig *sharding.NodesCoordinatorRegistry
}

// ComponentsNeededForBootstrap holds the components which need to be initialized from network
type ComponentsNeededForBootstrap struct {
	EpochStartMetaBlock *block.MetaBlock
	PreviousEpochStart  *block.MetaBlock
	ShardHeader         *block.Header
	NodesConfig         *sharding.NodesCoordinatorRegistry
	Headers             map[string]data.HeaderHandler
	ShardCoordinator    sharding.Coordinator
	PendingMiniBlocks   map[string]*block.MiniBlock
}

// epochStartBootstrap will handle requesting the needed data to start when joining late the network
type epochStartBootstrap struct {
	// should come via arguments
	destinationShardAsObserver uint32
	publicKey                  crypto.PublicKey
	marshalizer                marshal.Marshalizer
	txSignMarshalizer          marshal.Marshalizer
	hasher                     hashing.Hasher
	messenger                  Messenger
	generalConfig              config.Config
	economicsData              process.EconomicsDataHandler
	singleSigner               crypto.SingleSigner
	blockSingleSigner          crypto.SingleSigner
	keyGen                     crypto.KeyGenerator
	blockKeyGen                crypto.KeyGenerator
	shardCoordinator           sharding.Coordinator
	genesisNodesConfig         sharding.GenesisNodesSetupHandler
	genesisShardCoordinator    sharding.Coordinator
	pathManager                storage.PathManagerHandler
	workingDir                 string
	defaultDBPath              string
	defaultEpochString         string
	defaultShardString         string
	rater                      sharding.ChanceComputer
	trieContainer              state.TriesHolder
	trieStorageManagers        map[string]data.StorageManager
	uint64Converter            typeConverters.Uint64ByteSliceConverter
	nodeShuffler               sharding.NodesShuffler
	rounder                    epochStart.Rounder
	addressPubkeyConverter     core.PubkeyConverter
	statusHandler              core.AppStatusHandler
	headerIntegrityVerifier    process.HeaderIntegrityVerifier
	enableSignTxWithHashEpoch  uint32
	txSignHasher               hashing.Hasher
	epochNotifier              process.EpochNotifier
	numConcurrentTrieSyncers   int
	maxHardCapForMissingNodes  int
	trieSyncerVersion          int

	// created components
	requestHandler            process.RequestHandler
	interceptorContainer      process.InterceptorsContainer
	dataPool                  dataRetriever.PoolsHolder
	miniBlocksSyncer          epochStart.PendingMiniBlocksSyncHandler
	headersSyncer             epochStart.HeadersByHashSyncer
	epochStartMetaBlockSyncer epochStart.StartOfEpochMetaSyncer
	nodesConfigHandler        StartOfEpochNodesConfigHandler
	whiteListHandler          update.WhiteListHandler
	whiteListerVerifiedTxs    update.WhiteListHandler
	storageOpenerHandler      storage.UnitOpenerHandler
	latestStorageDataProvider storage.LatestStorageDataProviderHandler
	argumentsParser           process.ArgumentsParser

	// gathered data
	epochStartMeta     *block.MetaBlock
	prevEpochStartMeta *block.MetaBlock
	syncedHeaders      map[string]data.HeaderHandler
	nodesConfig        *sharding.NodesCoordinatorRegistry
	baseData           baseDataInStorage
	startRound         int64
	nodeType           core.NodeType
	startEpoch         uint32
	shuffledOut        bool
}

type baseDataInStorage struct {
	shardId         uint32
	numberOfShards  uint32
	lastRound       int64
	epochStartRound uint64
	lastEpoch       uint32
	storageExists   bool
}

// ArgsEpochStartBootstrap holds the arguments needed for creating an epoch start data provider component
type ArgsEpochStartBootstrap struct {
	DestinationShardAsObserver uint32
	WorkingDir                 string
	DefaultDBPath              string
	DefaultEpochString         string
	DefaultShardString         string
	PublicKey                  crypto.PublicKey
	Marshalizer                marshal.Marshalizer
	TxSignMarshalizer          marshal.Marshalizer
	Hasher                     hashing.Hasher
	Messenger                  Messenger
	GeneralConfig              config.Config
	EconomicsData              process.EconomicsDataHandler
	SingleSigner               crypto.SingleSigner
	BlockSingleSigner          crypto.SingleSigner
	KeyGen                     crypto.KeyGenerator
	BlockKeyGen                crypto.KeyGenerator
	GenesisNodesConfig         sharding.GenesisNodesSetupHandler
	GenesisShardCoordinator    sharding.Coordinator
	PathManager                storage.PathManagerHandler
	StorageUnitOpener          storage.UnitOpenerHandler
	LatestStorageDataProvider  storage.LatestStorageDataProviderHandler
	Rater                      sharding.ChanceComputer
	Uint64Converter            typeConverters.Uint64ByteSliceConverter
	NodeShuffler               sharding.NodesShuffler
	Rounder                    epochStart.Rounder
	AddressPubkeyConverter     core.PubkeyConverter
	ArgumentsParser            process.ArgumentsParser
	StatusHandler              core.AppStatusHandler
	HeaderIntegrityVerifier    process.HeaderIntegrityVerifier
	TxSignHasher               hashing.Hasher
	EpochNotifier              process.EpochNotifier
}

// NewEpochStartBootstrap will return a new instance of epochStartBootstrap
func NewEpochStartBootstrap(args ArgsEpochStartBootstrap) (*epochStartBootstrap, error) {
	err := checkArguments(args)
	if err != nil {
		return nil, err
	}

	epochStartProvider := &epochStartBootstrap{
		publicKey:                  args.PublicKey,
		marshalizer:                args.Marshalizer,
		txSignMarshalizer:          args.TxSignMarshalizer,
		hasher:                     args.Hasher,
		messenger:                  args.Messenger,
		generalConfig:              args.GeneralConfig,
		economicsData:              args.EconomicsData,
		genesisNodesConfig:         args.GenesisNodesConfig,
		genesisShardCoordinator:    args.GenesisShardCoordinator,
		workingDir:                 args.WorkingDir,
		pathManager:                args.PathManager,
		defaultEpochString:         args.DefaultEpochString,
		defaultDBPath:              args.DefaultDBPath,
		defaultShardString:         args.DefaultShardString,
		keyGen:                     args.KeyGen,
		blockKeyGen:                args.BlockKeyGen,
		singleSigner:               args.SingleSigner,
		blockSingleSigner:          args.BlockSingleSigner,
		rater:                      args.Rater,
		destinationShardAsObserver: args.DestinationShardAsObserver,
		uint64Converter:            args.Uint64Converter,
		nodeShuffler:               args.NodeShuffler,
		rounder:                    args.Rounder,
		storageOpenerHandler:       args.StorageUnitOpener,
		latestStorageDataProvider:  args.LatestStorageDataProvider,
		addressPubkeyConverter:     args.AddressPubkeyConverter,
		statusHandler:              args.StatusHandler,
		shuffledOut:                false,
		nodeType:                   core.NodeTypeObserver,
		argumentsParser:            args.ArgumentsParser,
		headerIntegrityVerifier:    args.HeaderIntegrityVerifier,
		txSignHasher:               args.TxSignHasher,
		enableSignTxWithHashEpoch:  args.GeneralConfig.GeneralSettings.TransactionSignedWithTxHashEnableEpoch,
		epochNotifier:              args.EpochNotifier,
		numConcurrentTrieSyncers:   args.GeneralConfig.TrieSync.NumConcurrentTrieSyncers,
		maxHardCapForMissingNodes:  args.GeneralConfig.TrieSync.MaxHardCapForMissingNodes,
		trieSyncerVersion:          args.GeneralConfig.TrieSync.TrieSyncerVersion,
	}

	whiteListCache, err := storageUnit.NewCache(storageFactory.GetCacherFromConfig(epochStartProvider.generalConfig.WhiteListPool))
	if err != nil {
		return nil, err
	}

	epochStartProvider.whiteListHandler, err = interceptors.NewWhiteListDataVerifier(whiteListCache)
	if err != nil {
		return nil, err
	}

	epochStartProvider.whiteListerVerifiedTxs, err = disabledInterceptors.NewDisabledWhiteListDataVerifier()
	if err != nil {
		return nil, err
	}

	epochStartProvider.trieContainer = state.NewDataTriesHolder()
	epochStartProvider.trieStorageManagers = make(map[string]data.StorageManager)

	if epochStartProvider.generalConfig.Hardfork.AfterHardFork {
		epochStartProvider.startEpoch = epochStartProvider.generalConfig.Hardfork.StartEpoch
		epochStartProvider.baseData.lastEpoch = epochStartProvider.startEpoch
		epochStartProvider.startRound = int64(epochStartProvider.generalConfig.Hardfork.StartRound)
		epochStartProvider.baseData.lastRound = epochStartProvider.startRound
		epochStartProvider.baseData.epochStartRound = uint64(epochStartProvider.startRound)
	}

	return epochStartProvider, nil
}

func (e *epochStartBootstrap) isStartInEpochZero() bool {
	startTime := time.Unix(e.genesisNodesConfig.GetStartTime(), 0)
	isCurrentTimeBeforeGenesis := time.Since(startTime) < 0
	if isCurrentTimeBeforeGenesis {
		return true
	}

	currentRound := e.rounder.Index() - e.startRound
	epochEndPlusGracePeriod := float64(e.generalConfig.EpochStartConfig.RoundsPerEpoch) * (gracePeriodInPercentage + 1.0)
	log.Debug("IsStartInEpochZero", "currentRound", currentRound, "epochEndRound", epochEndPlusGracePeriod)
	return float64(currentRound) < epochEndPlusGracePeriod
}

func (e *epochStartBootstrap) prepareEpochZero() (Parameters, error) {
	err := e.createTriesComponentsForShardId(e.genesisShardCoordinator.SelfId())
	if err != nil {
		return Parameters{}, err
	}

	shardIDToReturn := e.genesisShardCoordinator.SelfId()
	if !e.isNodeInGenesisNodesConfig() {
		shardIDToReturn = e.applyShardIDAsObserverIfNeeded(e.genesisShardCoordinator.SelfId())
	}
	parameters := Parameters{
		Epoch:       e.startEpoch,
		SelfShardId: shardIDToReturn,
		NumOfShards: e.genesisShardCoordinator.NumberOfShards(),
	}
	return parameters, nil
}

func (e *epochStartBootstrap) isNodeInGenesisNodesConfig() bool {
	ownPubKey, err := e.publicKey.ToByteArray()
	if err != nil {
		return false
	}

	eligibleList, waitingList := e.genesisNodesConfig.InitialNodesInfo()

	for _, nodesInShard := range eligibleList {
		for _, eligibleNode := range nodesInShard {
			if bytes.Equal(eligibleNode.PubKeyBytes(), ownPubKey) {
				return true
			}
		}
	}

	for _, nodesInShard := range waitingList {
		for _, waitingNode := range nodesInShard {
			if bytes.Equal(waitingNode.PubKeyBytes(), ownPubKey) {
				return true
			}
		}
	}

	return false
}

// GetTriesComponents returns the created tries components according to the shardID for the current epoch
func (e *epochStartBootstrap) GetTriesComponents() (state.TriesHolder, map[string]data.StorageManager) {
	return e.trieContainer, e.trieStorageManagers
}

// Bootstrap runs the fast bootstrap method from the network or local storage
func (e *epochStartBootstrap) Bootstrap() (Parameters, error) {
	if !e.generalConfig.GeneralSettings.StartInEpochEnabled {
		log.Warn("fast bootstrap is disabled")

		e.initializeFromLocalStorage()
		if !e.baseData.storageExists {
			err := e.createTriesComponentsForShardId(e.genesisShardCoordinator.SelfId())
			if err != nil {
				return Parameters{}, err
			}

			return Parameters{
				Epoch:       e.startEpoch,
				SelfShardId: e.genesisShardCoordinator.SelfId(),
				NumOfShards: e.genesisShardCoordinator.NumberOfShards(),
			}, nil
		}

		newShardId, shuffledOut, err := e.getShardIDForLatestEpoch()
		if err != nil {
			return Parameters{}, err
		}

		err = e.createTriesComponentsForShardId(newShardId)
		if err != nil {
			return Parameters{}, err
		}

		epochToStart := e.baseData.lastEpoch
		if shuffledOut {
			epochToStart = e.startEpoch
		}

		newShardId = e.applyShardIDAsObserverIfNeeded(newShardId)
		return Parameters{
			Epoch:       epochToStart,
			SelfShardId: newShardId,
			NumOfShards: e.baseData.numberOfShards,
			NodesConfig: e.nodesConfig,
		}, nil
	}

	defer func() {
		log.Debug("unregistering all message processor and un-joining all topics")
		errMessenger := e.messenger.UnregisterAllMessageProcessors()
		log.LogIfError(errMessenger)

		errMessenger = e.messenger.UnjoinAllTopics()
		log.LogIfError(errMessenger)
	}()

	var err error
	e.shardCoordinator, err = sharding.NewMultiShardCoordinator(e.genesisShardCoordinator.NumberOfShards(), core.MetachainShardId)
	if err != nil {
		return Parameters{}, err
	}

	e.dataPool, err = factoryDataPool.NewDataPoolFromConfig(
		factoryDataPool.ArgsDataPool{
			Config:           &e.generalConfig,
			EconomicsData:    e.economicsData,
			ShardCoordinator: e.shardCoordinator,
		},
	)
	if err != nil {
		return Parameters{}, err
	}

	isStartInEpochZero := e.isStartInEpochZero()
	isCurrentEpochSaved := e.computeIfCurrentEpochIsSaved()

	if isStartInEpochZero || isCurrentEpochSaved {
		if e.baseData.lastEpoch <= e.startEpoch {
			return e.prepareEpochZero()
		}

		parameters, errPrepare := e.prepareEpochFromStorage()
		if errPrepare == nil {
			return parameters, nil
		}

		if e.shuffledOut {
			// sync was already tried - not need to continue from here
			return Parameters{}, errPrepare
		}

		log.Debug("could not start from storage - will try sync for start in epoch", "error", errPrepare)
	}

	err = e.prepareComponentsToSyncFromNetwork()
	if err != nil {
		return Parameters{}, err
	}

	e.epochStartMeta, err = e.epochStartMetaBlockSyncer.SyncEpochStartMeta(timeToWait)
	if err != nil {
		return Parameters{}, err
	}
	log.Debug("start in epoch bootstrap: got epoch start meta header", "epoch", e.epochStartMeta.Epoch, "nonce", e.epochStartMeta.Nonce)
	e.setEpochStartMetrics()

	err = e.createSyncers()
	if err != nil {
		return Parameters{}, err
	}

	params, err := e.requestAndProcessing()
	if err != nil {
		return Parameters{}, err
	}

	return params, nil
}

func (e *epochStartBootstrap) computeIfCurrentEpochIsSaved() bool {
	e.initializeFromLocalStorage()
	if !e.baseData.storageExists {
		return false
	}

	computedRound := e.rounder.Index()
	log.Debug("computed round", "round", computedRound, "lastRound", e.baseData.lastRound)
	if computedRound-e.baseData.lastRound < roundGracePeriod {
		return true
	}

	roundsSinceEpochStart := computedRound - int64(e.baseData.epochStartRound)
	log.Debug("epoch start round", "round", e.baseData.epochStartRound, "roundsSinceEpochStart", roundsSinceEpochStart)
	epochEndPlusGracePeriod := float64(e.generalConfig.EpochStartConfig.RoundsPerEpoch) * (gracePeriodInPercentage + 1.0)
	return float64(roundsSinceEpochStart) < epochEndPlusGracePeriod
}

func (e *epochStartBootstrap) prepareComponentsToSyncFromNetwork() error {
	err := e.createTriesComponentsForShardId(core.MetachainShardId)
	if err != nil {
		return err
	}

	err = e.createRequestHandler()
	if err != nil {
		return err
	}

	argsEpochStartSyncer := ArgsNewEpochStartMetaSyncer{
		RequestHandler:          e.requestHandler,
		Messenger:               e.messenger,
		Marshalizer:             e.marshalizer,
		TxSignMarshalizer:       e.txSignMarshalizer,
		ShardCoordinator:        e.shardCoordinator,
		Hasher:                  e.hasher,
		ChainID:                 []byte(e.genesisNodesConfig.GetChainId()),
		EconomicsData:           e.economicsData,
		KeyGen:                  e.keyGen,
		BlockKeyGen:             e.blockKeyGen,
		Signer:                  e.singleSigner,
		BlockSigner:             e.blockSingleSigner,
		WhitelistHandler:        e.whiteListHandler,
		AddressPubkeyConv:       e.addressPubkeyConverter,
		NonceConverter:          e.uint64Converter,
		StartInEpochConfig:      e.generalConfig.EpochStartConfig,
		HeaderIntegrityVerifier: e.headerIntegrityVerifier,
	}
	e.epochStartMetaBlockSyncer, err = NewEpochStartMetaSyncer(argsEpochStartSyncer)
	if err != nil {
		return err
	}

	return nil
}

func (e *epochStartBootstrap) createSyncers() error {
	var err error

	args := factoryInterceptors.ArgsEpochStartInterceptorContainer{
		Config:                    e.generalConfig,
		ShardCoordinator:          e.shardCoordinator,
		ProtoMarshalizer:          e.marshalizer,
		TxSignMarshalizer:         e.txSignMarshalizer,
		Hasher:                    e.hasher,
		Messenger:                 e.messenger,
		DataPool:                  e.dataPool,
		SingleSigner:              e.singleSigner,
		BlockSingleSigner:         e.blockSingleSigner,
		KeyGen:                    e.keyGen,
		BlockKeyGen:               e.blockKeyGen,
		WhiteListHandler:          e.whiteListHandler,
		WhiteListerVerifiedTxs:    e.whiteListerVerifiedTxs,
		AddressPubkeyConv:         e.addressPubkeyConverter,
		NonceConverter:            e.uint64Converter,
		ChainID:                   []byte(e.genesisNodesConfig.GetChainId()),
		ArgumentsParser:           e.argumentsParser,
		MinTransactionVersion:     e.genesisNodesConfig.GetMinTransactionVersion(),
		HeaderIntegrityVerifier:   e.headerIntegrityVerifier,
		EnableSignTxWithHashEpoch: e.enableSignTxWithHashEpoch,
		TxSignHasher:              e.txSignHasher,
		EpochNotifier:             e.epochNotifier,
	}

	e.interceptorContainer, err = factoryInterceptors.NewEpochStartInterceptorsContainer(args)
	if err != nil {
		return err
	}

	syncMiniBlocksArgs := sync.ArgsNewPendingMiniBlocksSyncer{
		Storage:        disabled.CreateMemUnit(),
		Cache:          e.dataPool.MiniBlocks(),
		Marshalizer:    e.marshalizer,
		RequestHandler: e.requestHandler,
	}
	e.miniBlocksSyncer, err = sync.NewPendingMiniBlocksSyncer(syncMiniBlocksArgs)
	if err != nil {
		return err
	}

	syncMissingHeadersArgs := sync.ArgsNewMissingHeadersByHashSyncer{
		Storage:        disabled.CreateMemUnit(),
		Cache:          e.dataPool.Headers(),
		Marshalizer:    e.marshalizer,
		RequestHandler: e.requestHandler,
	}
	e.headersSyncer, err = sync.NewMissingheadersByHashSyncer(syncMissingHeadersArgs)
	if err != nil {
		return err
	}

	return nil
}

func (e *epochStartBootstrap) syncHeadersFrom(meta *block.MetaBlock) (map[string]data.HeaderHandler, error) {
	hashesToRequest := make([][]byte, 0, len(meta.EpochStart.LastFinalizedHeaders)+1)
	shardIds := make([]uint32, 0, len(meta.EpochStart.LastFinalizedHeaders)+1)

	for _, epochStartData := range meta.EpochStart.LastFinalizedHeaders {
		hashesToRequest = append(hashesToRequest, epochStartData.HeaderHash)
		shardIds = append(shardIds, epochStartData.ShardID)
	}

	if meta.Epoch > e.startEpoch+1 { // no need to request genesis block
		hashesToRequest = append(hashesToRequest, meta.EpochStart.Economics.PrevEpochStartHash)
		shardIds = append(shardIds, core.MetachainShardId)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeToWait)
	err := e.headersSyncer.SyncMissingHeadersByHash(shardIds, hashesToRequest, ctx)
	cancel()
	if err != nil {
		return nil, err
	}

	syncedHeaders, err := e.headersSyncer.GetHeaders()
	if err != nil {
		return nil, err
	}

	if meta.Epoch == e.startEpoch+1 {
		syncedHeaders[string(meta.EpochStart.Economics.PrevEpochStartHash)] = &block.MetaBlock{}
	}

	return syncedHeaders, nil
}

// Bootstrap will handle requesting and receiving the needed information the node will bootstrap from
func (e *epochStartBootstrap) requestAndProcessing() (Parameters, error) {
	var err error
	e.baseData.numberOfShards = uint32(len(e.epochStartMeta.EpochStart.LastFinalizedHeaders))
	e.baseData.lastEpoch = e.epochStartMeta.Epoch

	e.syncedHeaders, err = e.syncHeadersFrom(e.epochStartMeta)
	if err != nil {
		return Parameters{}, err
	}
	log.Debug("start in epoch bootstrap: got shard headers and previous epoch start meta block")

	prevEpochStartMetaHash := e.epochStartMeta.EpochStart.Economics.PrevEpochStartHash
	prevEpochStartMeta, ok := e.syncedHeaders[string(prevEpochStartMetaHash)].(*block.MetaBlock)
	if !ok {
		return Parameters{}, epochStart.ErrWrongTypeAssertion
	}
	e.prevEpochStartMeta = prevEpochStartMeta

	pubKeyBytes, err := e.publicKey.ToByteArray()
	if err != nil {
		return Parameters{}, err
	}

	err = e.processNodesConfig(pubKeyBytes)
	if err != nil {
		return Parameters{}, err
	}
	log.Debug("start in epoch bootstrap: processNodesConfig")

	e.saveSelfShardId()
	e.shardCoordinator, err = sharding.NewMultiShardCoordinator(e.baseData.numberOfShards, e.baseData.shardId)
	if err != nil {
		return Parameters{}, fmt.Errorf("%w numberOfShards=%v shardId=%v", err, e.baseData.numberOfShards, e.baseData.shardId)
	}
	log.Debug("start in epoch bootstrap: shardCoordinator", "numOfShards", e.baseData.numberOfShards, "shardId", e.baseData.shardId)

	err = e.messenger.CreateTopic(core.ConsensusTopic+e.shardCoordinator.CommunicationIdentifier(e.shardCoordinator.SelfId()), true)
	if err != nil {
		return Parameters{}, err
	}

	if e.shardCoordinator.SelfId() == core.MetachainShardId {
		err = e.requestAndProcessForMeta()
		if err != nil {
			return Parameters{}, err
		}
	} else {
		err = e.createTriesComponentsForShardId(e.shardCoordinator.SelfId())
		if err != nil {
			return Parameters{}, err
		}

		err = e.requestAndProcessForShard()
		if err != nil {
			return Parameters{}, err
		}
	}

	log.Debug("removing cached received trie nodes")
	e.dataPool.TrieNodes().Clear()

	parameters := Parameters{
		Epoch:       e.baseData.lastEpoch,
		SelfShardId: e.baseData.shardId,
		NumOfShards: e.baseData.numberOfShards,
		NodesConfig: e.nodesConfig,
	}

	return parameters, nil
}

func (e *epochStartBootstrap) saveSelfShardId() {
	if e.baseData.shardId != core.AllShardId {
		return
	}

	e.baseData.shardId = e.destinationShardAsObserver

	if e.baseData.shardId > e.baseData.numberOfShards &&
		e.baseData.shardId != core.MetachainShardId {
		e.baseData.shardId = e.genesisShardCoordinator.SelfId()
	}
}

func (e *epochStartBootstrap) processNodesConfig(pubKey []byte) error {
	var err error
	shardId := e.destinationShardAsObserver
	if shardId > e.baseData.numberOfShards && shardId != core.MetachainShardId {
		shardId = e.genesisShardCoordinator.SelfId()
	}
	argsNewValidatorStatusSyncers := ArgsNewSyncValidatorStatus{
		DataPool:                  e.dataPool,
		Marshalizer:               e.marshalizer,
		RequestHandler:            e.requestHandler,
		ChanceComputer:            e.rater,
		GenesisNodesConfig:        e.genesisNodesConfig,
		NodeShuffler:              e.nodeShuffler,
		Hasher:                    e.hasher,
		PubKey:                    pubKey,
		ShardIdAsObserver:         shardId,
		WaitingListFixEnableEpoch: e.generalConfig.GeneralSettings.WaitingListFixEnableEpoch,
	}

	e.nodesConfigHandler, err = NewSyncValidatorStatus(argsNewValidatorStatusSyncers)
	if err != nil {
		return err
	}

	e.nodesConfig, e.baseData.shardId, err = e.nodesConfigHandler.NodesConfigFromMetaBlock(e.epochStartMeta, e.prevEpochStartMeta)
	e.baseData.shardId = e.applyShardIDAsObserverIfNeeded(e.baseData.shardId)

	return err
}

func (e *epochStartBootstrap) requestAndProcessForMeta() error {
	var err error

	log.Debug("start in epoch bootstrap: started syncPeerAccountsState")
	err = e.syncPeerAccountsState(e.epochStartMeta.ValidatorStatsRootHash)
	if err != nil {
		return err
	}
	log.Debug("start in epoch bootstrap: syncUserAccountsState")

	err = e.syncUserAccountsState(e.epochStartMeta.RootHash)
	if err != nil {
		return err
	}

	components := &ComponentsNeededForBootstrap{
		EpochStartMetaBlock: e.epochStartMeta,
		PreviousEpochStart:  e.prevEpochStartMeta,
		NodesConfig:         e.nodesConfig,
		Headers:             e.syncedHeaders,
		ShardCoordinator:    e.shardCoordinator,
	}

	storageHandlerComponent, err := NewMetaStorageHandler(
		e.generalConfig,
		e.shardCoordinator,
		e.pathManager,
		e.marshalizer,
		e.hasher,
		e.epochStartMeta.Epoch,
		e.uint64Converter,
	)
	if err != nil {
		return err
	}

	errSavingToStorage := storageHandlerComponent.SaveDataToStorage(components)
	if errSavingToStorage != nil {
		return errSavingToStorage
	}

	return nil
}

func (e *epochStartBootstrap) findSelfShardEpochStartData() (block.EpochStartShardData, error) {
	var epochStartData block.EpochStartShardData
	for _, shardData := range e.epochStartMeta.EpochStart.LastFinalizedHeaders {
		if shardData.ShardID == e.shardCoordinator.SelfId() {
			return shardData, nil
		}
	}
	return epochStartData, epochStart.ErrEpochStartDataForShardNotFound
}

func (e *epochStartBootstrap) requestAndProcessForShard() error {
	epochStartData, err := e.findSelfShardEpochStartData()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeToWait)
	err = e.miniBlocksSyncer.SyncPendingMiniBlocks(epochStartData.PendingMiniBlockHeaders, ctx)
	cancel()
	if err != nil {
		return err
	}

	pendingMiniBlocks, err := e.miniBlocksSyncer.GetMiniBlocks()
	if err != nil {
		return err
	}
	log.Debug("start in epoch bootstrap: GetMiniBlocks")

	shardIds := []uint32{
		core.MetachainShardId,
		core.MetachainShardId,
	}
	hashesToRequest := [][]byte{
		epochStartData.LastFinishedMetaBlock,
		epochStartData.FirstPendingMetaBlock,
	}

	e.headersSyncer.ClearFields()
	ctx, cancel = context.WithTimeout(context.Background(), timeToWait)
	err = e.headersSyncer.SyncMissingHeadersByHash(shardIds, hashesToRequest, ctx)
	cancel()
	if err != nil {
		return err
	}

	neededHeaders, err := e.headersSyncer.GetHeaders()
	if err != nil {
		return err
	}
	log.Debug("start in epoch bootstrap: SyncMissingHeadersByHash")

	for hash, hdr := range neededHeaders {
		e.syncedHeaders[hash] = hdr
	}

	ownShardHdr, ok := e.syncedHeaders[string(epochStartData.HeaderHash)].(*block.Header)
	if !ok {
		return epochStart.ErrWrongTypeAssertion
	}

	log.Debug("start in epoch bootstrap: started syncUserAccountsState")
	err = e.syncUserAccountsState(ownShardHdr.RootHash)
	if err != nil {
		return err
	}
	log.Debug("start in epoch bootstrap: syncUserAccountsState")

	components := &ComponentsNeededForBootstrap{
		EpochStartMetaBlock: e.epochStartMeta,
		PreviousEpochStart:  e.prevEpochStartMeta,
		ShardHeader:         ownShardHdr,
		NodesConfig:         e.nodesConfig,
		Headers:             e.syncedHeaders,
		ShardCoordinator:    e.shardCoordinator,
		PendingMiniBlocks:   pendingMiniBlocks,
	}

	storageHandlerComponent, err := NewShardStorageHandler(
		e.generalConfig,
		e.shardCoordinator,
		e.pathManager,
		e.marshalizer,
		e.hasher,
		e.baseData.lastEpoch,
		e.uint64Converter,
	)
	if err != nil {
		return err
	}

	errSavingToStorage := storageHandlerComponent.SaveDataToStorage(components)
	if errSavingToStorage != nil {
		return errSavingToStorage
	}

	return nil
}

func (e *epochStartBootstrap) syncUserAccountsState(rootHash []byte) error {
	thr, err := throttler.NewNumGoRoutinesThrottler(int32(e.numConcurrentTrieSyncers))
	if err != nil {
		return err
	}

	inactiveTrieExporter, err := trieExport.NewInactiveTrieExporter(e.marshalizer)
	if err != nil {
		return err
	}

	argsUserAccountsSyncer := syncer.ArgsNewUserAccountsSyncer{
		ArgsNewBaseAccountsSyncer: syncer.ArgsNewBaseAccountsSyncer{
			Hasher:                    e.hasher,
			Marshalizer:               e.marshalizer,
			TrieStorageManager:        e.trieStorageManagers[factory.UserAccountTrie],
			RequestHandler:            e.requestHandler,
			Timeout:                   timeoutGettingTrieNode,
			Cacher:                    e.dataPool.TrieNodes(),
			MaxTrieLevelInMemory:      e.generalConfig.StateTriesConfig.MaxStateTrieLevelInMemory,
			MaxHardCapForMissingNodes: e.maxHardCapForMissingNodes,
			TrieSyncerVersion:         e.trieSyncerVersion,
			TrieExporter:              inactiveTrieExporter,
		},
		ShardId:   e.shardCoordinator.SelfId(),
		Throttler: thr,
	}
	accountsDBSyncer, err := syncer.NewUserAccountsSyncer(argsUserAccountsSyncer)
	if err != nil {
		return err
	}

	err = accountsDBSyncer.SyncAccounts(rootHash, e.shardCoordinator.SelfId())
	if err != nil {
		return err
	}

	return nil
}

func (e *epochStartBootstrap) createTriesComponentsForShardId(shardId uint32) error {

	trieFactoryArgs := factory.TrieFactoryArgs{
		EvictionWaitingListCfg:   e.generalConfig.EvictionWaitingList,
		SnapshotDbCfg:            e.generalConfig.TrieSnapshotDB,
		Marshalizer:              e.marshalizer,
		Hasher:                   e.hasher,
		PathManager:              e.pathManager,
		TrieStorageManagerConfig: e.generalConfig.TrieStorageManagerConfig,
	}
	trieFactory, err := factory.NewTrieFactory(trieFactoryArgs)
	if err != nil {
		return err
	}

	userStorageManager, userAccountTrie, err := trieFactory.Create(
		e.generalConfig.AccountsTrieStorage,
		core.GetShardIDString(shardId),
		e.generalConfig.StateTriesConfig.AccountsStatePruningEnabled,
		e.generalConfig.StateTriesConfig.MaxStateTrieLevelInMemory,
	)
	if err != nil {
		return err
	}

	e.trieContainer.Put([]byte(factory.UserAccountTrie), userAccountTrie)
	e.trieStorageManagers[factory.UserAccountTrie] = userStorageManager

	peerStorageManager, peerAccountsTrie, err := trieFactory.Create(
		e.generalConfig.PeerAccountsTrieStorage,
		core.GetShardIDString(shardId),
		e.generalConfig.StateTriesConfig.PeerStatePruningEnabled,
		e.generalConfig.StateTriesConfig.MaxPeerTrieLevelInMemory,
	)
	if err != nil {
		return err
	}

	e.trieContainer.Put([]byte(factory.PeerAccountTrie), peerAccountsTrie)
	e.trieStorageManagers[factory.PeerAccountTrie] = peerStorageManager

	return nil
}

func (e *epochStartBootstrap) syncPeerAccountsState(rootHash []byte) error {
	inactiveTrieExporter, err := trieExport.NewInactiveTrieExporter(e.marshalizer)
	if err != nil {
		return err
	}

	argsValidatorAccountsSyncer := syncer.ArgsNewValidatorAccountsSyncer{
		ArgsNewBaseAccountsSyncer: syncer.ArgsNewBaseAccountsSyncer{
			Hasher:                    e.hasher,
			Marshalizer:               e.marshalizer,
			TrieStorageManager:        e.trieStorageManagers[factory.PeerAccountTrie],
			RequestHandler:            e.requestHandler,
			Timeout:                   timeoutGettingTrieNode,
			Cacher:                    e.dataPool.TrieNodes(),
			MaxTrieLevelInMemory:      e.generalConfig.StateTriesConfig.MaxPeerTrieLevelInMemory,
			MaxHardCapForMissingNodes: e.maxHardCapForMissingNodes,
			TrieSyncerVersion:         e.trieSyncerVersion,
			TrieExporter:              inactiveTrieExporter,
		},
	}
	accountsDBSyncer, err := syncer.NewValidatorAccountsSyncer(argsValidatorAccountsSyncer)
	if err != nil {
		return err
	}

	err = accountsDBSyncer.SyncAccounts(rootHash, e.shardCoordinator.SelfId())
	if err != nil {
		return err
	}

	return nil
}

func (e *epochStartBootstrap) createRequestHandler() error {
	dataPacker, err := partitioning.NewSimpleDataPacker(e.marshalizer)
	if err != nil {
		return err
	}

	storageService := disabled.NewChainStorer()

	resolversContainerArgs := resolverscontainer.FactoryArgs{
		ShardCoordinator:           e.shardCoordinator,
		Messenger:                  e.messenger,
		Store:                      storageService,
		Marshalizer:                e.marshalizer,
		DataPools:                  e.dataPool,
		Uint64ByteSliceConverter:   uint64ByteSlice.NewBigEndianConverter(),
		NumConcurrentResolvingJobs: 10,
		DataPacker:                 dataPacker,
		TriesContainer:             e.trieContainer,
		SizeCheckDelta:             0,
		InputAntifloodHandler:      disabled.NewAntiFloodHandler(),
		OutputAntifloodHandler:     disabled.NewAntiFloodHandler(),
	}
	resolverFactory, err := resolverscontainer.NewMetaResolversContainerFactory(resolversContainerArgs)
	if err != nil {
		return err
	}

	container, err := resolverFactory.Create()
	if err != nil {
		return err
	}

	err = resolverFactory.AddShardTrieNodeResolvers(container)
	if err != nil {
		return err
	}

	finder, err := containers.NewResolversFinder(container, e.shardCoordinator)
	if err != nil {
		return err
	}

	requestedItemsHandler := timecache.NewTimeCache(timeBetweenRequests)
	e.requestHandler, err = requestHandlers.NewResolverRequestHandler(
		finder,
		requestedItemsHandler,
		e.whiteListHandler,
		maxToRequest,
		core.MetachainShardId,
		timeBetweenRequests,
	)
	return err
}

func (e *epochStartBootstrap) setEpochStartMetrics() {
	if e.epochStartMeta != nil {
		e.statusHandler.SetUInt64Value(core.MetricNonceAtEpochStart, e.epochStartMeta.Nonce)
		e.statusHandler.SetUInt64Value(core.MetricRoundAtEpochStart, e.epochStartMeta.Round)
	}
}

func (e *epochStartBootstrap) applyShardIDAsObserverIfNeeded(receivedShardID uint32) uint32 {
	if e.nodeType == core.NodeTypeObserver &&
		e.destinationShardAsObserver != core.DisabledShardIDAsObserver &&
		e.destinationShardAsObserver != receivedShardID {
		log.Debug("shard id as observer applied", "destination shard ID", e.destinationShardAsObserver, "computed", receivedShardID)
		receivedShardID = e.destinationShardAsObserver
	}

	return receivedShardID
}

// IsInterfaceNil returns true if there is no value under the interface
func (e *epochStartBootstrap) IsInterfaceNil() bool {
	return e == nil
}
