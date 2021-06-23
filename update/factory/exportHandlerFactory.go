package factory

import (
	"fmt"
	"math"
	"os"
	"path"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/trie"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/debug/factory"
	"github.com/ElrondNetwork/elrond-go/epochStart"
	"github.com/ElrondNetwork/elrond-go/epochStart/notifier"
	"github.com/ElrondNetwork/elrond-go/epochStart/shardchain"
	"github.com/ElrondNetwork/elrond-go/genesis/process/disabled"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
	storageFactory "github.com/ElrondNetwork/elrond-go/storage/factory"
	"github.com/ElrondNetwork/elrond-go/storage/storageUnit"
	"github.com/ElrondNetwork/elrond-go/storage/timecache"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/genesis"
	"github.com/ElrondNetwork/elrond-go/update/genesis/trieExport"
	"github.com/ElrondNetwork/elrond-go/update/storing"
	"github.com/ElrondNetwork/elrond-go/update/sync"
)

var log = logger.GetOrCreate("update/factory")

// ArgsExporter is the argument structure to create a new exporter
type ArgsExporter struct {
	TxSignMarshalizer         marshal.Marshalizer
	Marshalizer               marshal.Marshalizer
	Hasher                    hashing.Hasher
	HeaderValidator           epochStart.HeaderValidator
	Uint64Converter           typeConverters.Uint64ByteSliceConverter
	DataPool                  dataRetriever.PoolsHolder
	StorageService            dataRetriever.StorageService
	RequestHandler            process.RequestHandler
	ShardCoordinator          sharding.Coordinator
	Messenger                 p2p.Messenger
	ActiveAccountsDBs         map[state.AccountsDbIdentifier]state.AccountsAdapter
	ExistingResolvers         dataRetriever.ResolversContainer
	ExportFolder              string
	ExportTriesStorageConfig  config.StorageConfig
	ExportStateStorageConfig  config.StorageConfig
	ExportStateKeysConfig     config.StorageConfig
	MaxTrieLevelInMemory      uint
	WhiteListHandler          process.WhiteListHandler
	WhiteListerVerifiedTxs    process.WhiteListHandler
	InterceptorsContainer     process.InterceptorsContainer
	MultiSigner               crypto.MultiSigner
	NodesCoordinator          sharding.NodesCoordinator
	SingleSigner              crypto.SingleSigner
	AddressPubKeyConverter    core.PubkeyConverter
	ValidatorPubKeyConverter  core.PubkeyConverter
	BlockKeyGen               crypto.KeyGenerator
	KeyGen                    crypto.KeyGenerator
	BlockSigner               crypto.SingleSigner
	HeaderSigVerifier         process.InterceptedHeaderSigVerifier
	HeaderIntegrityVerifier   process.HeaderIntegrityVerifier
	ValidityAttester          process.ValidityAttester
	InputAntifloodHandler     process.P2PAntifloodHandler
	OutputAntifloodHandler    process.P2PAntifloodHandler
	ChainID                   []byte
	RoundHandler              update.RoundHandler
	GenesisNodesSetupHandler  update.GenesisNodesSetupHandler
	InterceptorDebugConfig    config.InterceptorResolverDebugConfig
	MinTxVersion              uint32
	EnableSignTxWithHashEpoch uint32
	TxSignHasher              hashing.Hasher
	EpochNotifier             process.EpochNotifier
	MaxHardCapForMissingNodes int
	NumConcurrentTrieSyncers  int
	TrieSyncerVersion         int
}

type exportHandlerFactory struct {
	txSignMarshalizer         marshal.Marshalizer
	marshalizer               marshal.Marshalizer
	hasher                    hashing.Hasher
	headerValidator           epochStart.HeaderValidator
	uint64Converter           typeConverters.Uint64ByteSliceConverter
	dataPool                  dataRetriever.PoolsHolder
	storageService            dataRetriever.StorageService
	requestHandler            process.RequestHandler
	shardCoordinator          sharding.Coordinator
	messenger                 p2p.Messenger
	activeAccountsDBs         map[state.AccountsDbIdentifier]state.AccountsAdapter
	exportFolder              string
	exportTriesStorageConfig  config.StorageConfig
	exportStateStorageConfig  config.StorageConfig
	exportStateKeysConfig     config.StorageConfig
	maxTrieLevelInMemory      uint
	whiteListHandler          process.WhiteListHandler
	whiteListerVerifiedTxs    process.WhiteListHandler
	interceptorsContainer     process.InterceptorsContainer
	existingResolvers         dataRetriever.ResolversContainer
	epochStartTrigger         epochStart.TriggerHandler
	accounts                  state.AccountsAdapter
	multiSigner               crypto.MultiSigner
	nodesCoordinator          sharding.NodesCoordinator
	singleSigner              crypto.SingleSigner
	blockKeyGen               crypto.KeyGenerator
	keyGen                    crypto.KeyGenerator
	blockSigner               crypto.SingleSigner
	addressPubKeyConverter    core.PubkeyConverter
	validatorPubKeyConverter  core.PubkeyConverter
	headerSigVerifier         process.InterceptedHeaderSigVerifier
	headerIntegrityVerifier   process.HeaderIntegrityVerifier
	validityAttester          process.ValidityAttester
	resolverContainer         dataRetriever.ResolversContainer
	inputAntifloodHandler     process.P2PAntifloodHandler
	outputAntifloodHandler    process.P2PAntifloodHandler
	chainID                   []byte
	roundHandler              process.Rounder
	genesisNodesSetupHandler  update.GenesisNodesSetupHandler
	interceptorDebugConfig    config.InterceptorResolverDebugConfig
	minTxVersion              uint32
	enableSignTxWithHashEpoch uint32
	txSignHasher              hashing.Hasher
	epochNotifier             process.EpochNotifier
	maxHardCapForMissingNodes int
	numConcurrentTrieSyncers  int
	trieSyncerVersion         int
}

// NewExportHandlerFactory creates an exporter factory
func NewExportHandlerFactory(args ArgsExporter) (*exportHandlerFactory, error) {
	if check.IfNil(args.ShardCoordinator) {
		return nil, update.ErrNilShardCoordinator
	}
	if check.IfNil(args.Hasher) {
		return nil, update.ErrNilHasher
	}
	if check.IfNil(args.Marshalizer) {
		return nil, update.ErrNilMarshalizer
	}
	if check.IfNil(args.HeaderValidator) {
		return nil, update.ErrNilHeaderValidator
	}
	if check.IfNil(args.Uint64Converter) {
		return nil, update.ErrNilUint64Converter
	}
	if check.IfNil(args.DataPool) {
		return nil, update.ErrNilDataPoolHolder
	}
	if check.IfNil(args.StorageService) {
		return nil, update.ErrNilStorage
	}
	if check.IfNil(args.RequestHandler) {
		return nil, update.ErrNilRequestHandler
	}
	if check.IfNil(args.Messenger) {
		return nil, update.ErrNilMessenger
	}
	if args.ActiveAccountsDBs == nil {
		return nil, update.ErrNilAccounts
	}
	if check.IfNil(args.WhiteListHandler) {
		return nil, update.ErrNilWhiteListHandler
	}
	if check.IfNil(args.WhiteListerVerifiedTxs) {
		return nil, update.ErrNilWhiteListHandler
	}
	if check.IfNil(args.InterceptorsContainer) {
		return nil, update.ErrNilInterceptorsContainer
	}
	if check.IfNil(args.ExistingResolvers) {
		return nil, update.ErrNilResolverContainer
	}
	if check.IfNil(args.MultiSigner) {
		return nil, update.ErrNilMultiSigner
	}
	if check.IfNil(args.NodesCoordinator) {
		return nil, update.ErrNilNodesCoordinator
	}
	if check.IfNil(args.SingleSigner) {
		return nil, update.ErrNilSingleSigner
	}
	if check.IfNil(args.AddressPubKeyConverter) {
		return nil, fmt.Errorf("%w for addresses", update.ErrNilPubKeyConverter)
	}
	if check.IfNil(args.ValidatorPubKeyConverter) {
		return nil, fmt.Errorf("%w for validators", update.ErrNilPubKeyConverter)
	}
	if check.IfNil(args.BlockKeyGen) {
		return nil, update.ErrNilBlockKeyGen
	}
	if check.IfNil(args.KeyGen) {
		return nil, update.ErrNilKeyGenerator
	}
	if check.IfNil(args.BlockSigner) {
		return nil, update.ErrNilBlockSigner
	}
	if check.IfNil(args.HeaderSigVerifier) {
		return nil, update.ErrNilHeaderSigVerifier
	}
	if check.IfNil(args.HeaderIntegrityVerifier) {
		return nil, update.ErrNilHeaderIntegrityVerifier
	}
	if check.IfNil(args.ValidityAttester) {
		return nil, update.ErrNilValidityAttester
	}
	if check.IfNil(args.TxSignMarshalizer) {
		return nil, update.ErrNilMarshalizer
	}
	if check.IfNil(args.InputAntifloodHandler) {
		return nil, update.ErrNilAntiFloodHandler
	}
	if check.IfNil(args.OutputAntifloodHandler) {
		return nil, update.ErrNilAntiFloodHandler
	}
	if check.IfNil(args.RoundHandler) {
		return nil, update.ErrNilRoundHandler
	}
	if check.IfNil(args.GenesisNodesSetupHandler) {
		return nil, update.ErrNilGenesisNodesSetupHandler
	}
	if check.IfNil(args.TxSignHasher) {
		return nil, update.ErrNilHasher
	}
	if check.IfNil(args.EpochNotifier) {
		return nil, update.ErrNilEpochNotifier
	}
	if args.MaxHardCapForMissingNodes < 1 {
		return nil, update.ErrInvalidMaxHardCapForMissingNodes
	}
	if args.NumConcurrentTrieSyncers < 1 {
		return nil, update.ErrInvalidNumConcurrentTrieSyncers
	}
	err := trie.CheckTrieSyncerVersion(args.TrieSyncerVersion)
	if err != nil {
		return nil, err
	}

	e := &exportHandlerFactory{
		txSignMarshalizer:         args.TxSignMarshalizer,
		marshalizer:               args.Marshalizer,
		hasher:                    args.Hasher,
		headerValidator:           args.HeaderValidator,
		uint64Converter:           args.Uint64Converter,
		dataPool:                  args.DataPool,
		storageService:            args.StorageService,
		requestHandler:            args.RequestHandler,
		shardCoordinator:          args.ShardCoordinator,
		messenger:                 args.Messenger,
		activeAccountsDBs:         args.ActiveAccountsDBs,
		exportFolder:              args.ExportFolder,
		exportTriesStorageConfig:  args.ExportTriesStorageConfig,
		exportStateStorageConfig:  args.ExportStateStorageConfig,
		exportStateKeysConfig:     args.ExportStateKeysConfig,
		interceptorsContainer:     args.InterceptorsContainer,
		whiteListHandler:          args.WhiteListHandler,
		whiteListerVerifiedTxs:    args.WhiteListerVerifiedTxs,
		existingResolvers:         args.ExistingResolvers,
		accounts:                  args.ActiveAccountsDBs[state.UserAccountsState],
		multiSigner:               args.MultiSigner,
		nodesCoordinator:          args.NodesCoordinator,
		singleSigner:              args.SingleSigner,
		addressPubKeyConverter:    args.AddressPubKeyConverter,
		validatorPubKeyConverter:  args.ValidatorPubKeyConverter,
		blockKeyGen:               args.BlockKeyGen,
		keyGen:                    args.KeyGen,
		blockSigner:               args.BlockSigner,
		headerSigVerifier:         args.HeaderSigVerifier,
		headerIntegrityVerifier:   args.HeaderIntegrityVerifier,
		validityAttester:          args.ValidityAttester,
		inputAntifloodHandler:     args.InputAntifloodHandler,
		outputAntifloodHandler:    args.OutputAntifloodHandler,
		maxTrieLevelInMemory:      args.MaxTrieLevelInMemory,
		chainID:                   args.ChainID,
		roundHandler:              args.RoundHandler,
		genesisNodesSetupHandler:  args.GenesisNodesSetupHandler,
		interceptorDebugConfig:    args.InterceptorDebugConfig,
		minTxVersion:              args.MinTxVersion,
		enableSignTxWithHashEpoch: args.EnableSignTxWithHashEpoch,
		txSignHasher:              args.TxSignHasher,
		epochNotifier:             args.EpochNotifier,
		maxHardCapForMissingNodes: args.MaxHardCapForMissingNodes,
		numConcurrentTrieSyncers:  args.NumConcurrentTrieSyncers,
		trieSyncerVersion:         args.TrieSyncerVersion,
	}

	return e, nil
}

// Create makes a new export handler
func (e *exportHandlerFactory) Create() (update.ExportHandler, error) {
	err := e.prepareFolders(e.exportFolder)
	if err != nil {
		return nil, err
	}

	// TODO reuse the debugger when the one used for regular resolvers & interceptors will be moved inside the status components
	debugger, errNotCritical := factory.NewInterceptorResolverDebuggerFactory(e.interceptorDebugConfig)
	if errNotCritical != nil {
		log.Warn("error creating hardfork debugger", "error", errNotCritical)
	}

	argsPeerMiniBlocksSyncer := shardchain.ArgPeerMiniBlockSyncer{
		MiniBlocksPool: e.dataPool.MiniBlocks(),
		Requesthandler: e.requestHandler,
	}
	peerMiniBlocksSyncer, err := shardchain.NewPeerMiniBlockSyncer(argsPeerMiniBlocksSyncer)
	if err != nil {
		return nil, err
	}
	argsEpochTrigger := shardchain.ArgsShardEpochStartTrigger{
		Marshalizer:          e.marshalizer,
		Hasher:               e.hasher,
		HeaderValidator:      e.headerValidator,
		Uint64Converter:      e.uint64Converter,
		DataPool:             e.dataPool,
		Storage:              e.storageService,
		RequestHandler:       e.requestHandler,
		EpochStartNotifier:   notifier.NewEpochStartSubscriptionHandler(),
		Epoch:                0,
		Validity:             process.MetaBlockValidity,
		Finality:             process.BlockFinality,
		PeerMiniBlocksSyncer: peerMiniBlocksSyncer,
		Rounder:              e.roundHandler,
	}
	epochHandler, err := shardchain.NewEpochStartTrigger(&argsEpochTrigger)
	if err != nil {
		return nil, err
	}

	argsDataTrieFactory := ArgsNewDataTrieFactory{
		StorageConfig:        e.exportTriesStorageConfig,
		SyncFolder:           e.exportFolder,
		Marshalizer:          e.marshalizer,
		Hasher:               e.hasher,
		ShardCoordinator:     e.shardCoordinator,
		MaxTrieLevelInMemory: e.maxTrieLevelInMemory,
	}
	dataTriesContainerFactory, err := NewDataTrieFactory(argsDataTrieFactory)
	if err != nil {
		return nil, err
	}
	dataTries, err := dataTriesContainerFactory.Create()
	if err != nil {
		return nil, err
	}

	argsResolvers := ArgsNewResolversContainerFactory{
		ShardCoordinator:           e.shardCoordinator,
		Messenger:                  e.messenger,
		Marshalizer:                e.marshalizer,
		DataTrieContainer:          dataTries,
		ExistingResolvers:          e.existingResolvers,
		NumConcurrentResolvingJobs: 100,
		InputAntifloodHandler:      e.inputAntifloodHandler,
		OutputAntifloodHandler:     e.outputAntifloodHandler,
	}
	resolversFactory, err := NewResolversContainerFactory(argsResolvers)
	if err != nil {
		return nil, err
	}
	e.resolverContainer, err = resolversFactory.Create()
	if err != nil {
		return nil, err
	}

	e.resolverContainer.Iterate(func(key string, resolver dataRetriever.Resolver) bool {
		errNotCritical = resolver.SetResolverDebugHandler(debugger)
		if errNotCritical != nil {
			log.Warn("error setting debugger", "resolver", key, "error", errNotCritical)
		}

		return true
	})

	keysStorer, err := createStorer(e.exportStateKeysConfig, e.exportFolder)
	if err != nil {
		return nil, fmt.Errorf("%w while creating keys storer", err)
	}
	keysVals, err := createStorer(e.exportStateStorageConfig, e.exportFolder)
	if err != nil {
		return nil, fmt.Errorf("%w while creating keys-values storer", err)
	}

	arg := storing.ArgHardforkStorer{
		KeysStore:   keysStorer,
		KeyValue:    keysVals,
		Marshalizer: e.marshalizer,
	}
	hs, err := storing.NewHardforkStorer(arg)
	if err != nil {
		return nil, err
	}

	trieExporter, err := trieExport.NewTrieExport(
		e.exportFolder,
		e.shardCoordinator,
		e.marshalizer,
		hs,
		e.validatorPubKeyConverter,
		e.addressPubKeyConverter,
		e.genesisNodesSetupHandler,
	)
	if err != nil {
		return nil, err
	}

	argsAccountsSyncers := ArgsNewAccountsDBSyncersContainerFactory{
		TrieCacher:                e.dataPool.TrieNodes(),
		RequestHandler:            e.requestHandler,
		ShardCoordinator:          e.shardCoordinator,
		Hasher:                    e.hasher,
		Marshalizer:               e.marshalizer,
		TrieStorageManager:        dataTriesContainerFactory.TrieStorageManager(),
		TimoutGettingTrieNode:     update.TimeoutGettingTrieNodes,
		MaxTrieLevelInMemory:      e.maxTrieLevelInMemory,
		MaxHardCapForMissingNodes: e.maxHardCapForMissingNodes,
		NumConcurrentTrieSyncers:  e.numConcurrentTrieSyncers,
		TrieSyncerVersion:         e.trieSyncerVersion,
		TrieExporter:              trieExporter,
	}
	accountsDBSyncerFactory, err := NewAccountsDBSContainerFactory(argsAccountsSyncers)
	if err != nil {
		return nil, err
	}
	accountsDBSyncerContainer, err := accountsDBSyncerFactory.Create()
	if err != nil {
		return nil, err
	}

	argsNewHeadersSync := sync.ArgsNewHeadersSyncHandler{
		StorageService:   e.storageService,
		Cache:            e.dataPool.Headers(),
		Marshalizer:      e.marshalizer,
		Hasher:           e.hasher,
		EpochHandler:     epochHandler,
		RequestHandler:   e.requestHandler,
		Uint64Converter:  e.uint64Converter,
		ShardCoordinator: e.shardCoordinator,
	}
	epochStartHeadersSyncer, err := sync.NewHeadersSyncHandler(argsNewHeadersSync)
	if err != nil {
		return nil, err
	}

	argsNewSyncAccountsDBsHandler := sync.ArgsNewSyncAccountsDBsHandler{
		AccountsDBsSyncers: accountsDBSyncerContainer,
		ActiveAccountsDBs:  e.activeAccountsDBs,
	}
	epochStartTrieSyncer, err := sync.NewSyncAccountsDBsHandler(argsNewSyncAccountsDBsHandler)
	if err != nil {
		return nil, err
	}

	argsMiniBlockSyncer := sync.ArgsNewPendingMiniBlocksSyncer{
		Storage:        e.storageService.GetStorer(dataRetriever.MiniBlockUnit),
		Cache:          e.dataPool.MiniBlocks(),
		Marshalizer:    e.marshalizer,
		RequestHandler: e.requestHandler,
	}
	epochStartMiniBlocksSyncer, err := sync.NewPendingMiniBlocksSyncer(argsMiniBlockSyncer)
	if err != nil {
		return nil, err
	}

	argsPendingTransactions := sync.ArgsNewPendingTransactionsSyncer{
		DataPools:      e.dataPool,
		Storages:       e.storageService,
		Marshalizer:    e.marshalizer,
		RequestHandler: e.requestHandler,
	}
	epochStartTransactionsSyncer, err := sync.NewPendingTransactionsSyncer(argsPendingTransactions)
	if err != nil {
		return nil, err
	}

	argsSyncState := sync.ArgsNewSyncState{
		Headers:      epochStartHeadersSyncer,
		Tries:        epochStartTrieSyncer,
		MiniBlocks:   epochStartMiniBlocksSyncer,
		Transactions: epochStartTransactionsSyncer,
	}
	stateSyncer, err := sync.NewSyncState(argsSyncState)
	if err != nil {
		return nil, err
	}

	argsExporter := genesis.ArgsNewStateExporter{
		ShardCoordinator:         e.shardCoordinator,
		StateSyncer:              stateSyncer,
		Marshalizer:              e.marshalizer,
		HardforkStorer:           hs,
		Hasher:                   e.hasher,
		ExportFolder:             e.exportFolder,
		ValidatorPubKeyConverter: e.validatorPubKeyConverter,
		AddressPubKeyConverter:   e.addressPubKeyConverter,
		GenesisNodesSetupHandler: e.genesisNodesSetupHandler,
	}
	exportHandler, err := genesis.NewStateExporter(argsExporter)
	if err != nil {
		return nil, err
	}

	e.epochStartTrigger = epochHandler
	err = e.createInterceptors()
	if err != nil {
		return nil, err
	}

	e.interceptorsContainer.Iterate(func(key string, interceptor process.Interceptor) bool {
		errNotCritical = interceptor.SetInterceptedDebugHandler(debugger)
		if errNotCritical != nil {
			log.Warn("error setting debugger", "interceptor", key, "error", errNotCritical)
		}

		return true
	})

	return exportHandler, nil
}

func (e *exportHandlerFactory) prepareFolders(folder string) error {
	err := os.RemoveAll(folder)
	if err != nil {
		return err
	}

	return os.MkdirAll(folder, os.ModePerm)
}

func (e *exportHandlerFactory) createInterceptors() error {
	argsInterceptors := ArgsNewFullSyncInterceptorsContainerFactory{
		Accounts:                  e.accounts,
		ShardCoordinator:          e.shardCoordinator,
		NodesCoordinator:          e.nodesCoordinator,
		Messenger:                 e.messenger,
		Store:                     e.storageService,
		Marshalizer:               e.marshalizer,
		TxSignMarshalizer:         e.txSignMarshalizer,
		Hasher:                    e.hasher,
		KeyGen:                    e.keyGen,
		BlockSignKeyGen:           e.blockKeyGen,
		SingleSigner:              e.singleSigner,
		BlockSingleSigner:         e.blockSigner,
		MultiSigner:               e.multiSigner,
		DataPool:                  e.dataPool,
		AddressPubkeyConverter:    e.addressPubKeyConverter,
		MaxTxNonceDeltaAllowed:    math.MaxInt32,
		TxFeeHandler:              &disabled.FeeHandler{},
		BlockBlackList:            timecache.NewTimeCache(time.Second),
		HeaderSigVerifier:         e.headerSigVerifier,
		HeaderIntegrityVerifier:   e.headerIntegrityVerifier,
		SizeCheckDelta:            math.MaxUint32,
		ValidityAttester:          e.validityAttester,
		EpochStartTrigger:         e.epochStartTrigger,
		WhiteListHandler:          e.whiteListHandler,
		WhiteListerVerifiedTxs:    e.whiteListerVerifiedTxs,
		InterceptorsContainer:     e.interceptorsContainer,
		AntifloodHandler:          e.inputAntifloodHandler,
		NonceConverter:            e.uint64Converter,
		ChainID:                   e.chainID,
		MinTxVersion:              e.minTxVersion,
		EnableSignTxWithHashEpoch: e.enableSignTxWithHashEpoch,
		TxSignHasher:              e.txSignHasher,
		EpochNotifier:             e.epochNotifier,
	}
	fullSyncInterceptors, err := NewFullSyncInterceptorsContainerFactory(argsInterceptors)
	if err != nil {
		return err
	}

	interceptorsContainer, err := fullSyncInterceptors.Create()
	if err != nil {
		return err
	}

	e.interceptorsContainer = interceptorsContainer
	return nil
}

func createStorer(storageConfig config.StorageConfig, folder string) (storage.Storer, error) {
	dbConfig := storageFactory.GetDBFromConfig(storageConfig.DB)
	dbConfig.FilePath = path.Join(folder, storageConfig.DB.FilePath)
	accountsTrieStorage, err := storageUnit.NewStorageUnitFromConf(
		storageFactory.GetCacherFromConfig(storageConfig.Cache),
		dbConfig,
		storageFactory.GetBloomFromConfig(storageConfig.Bloom),
	)
	if err != nil {
		return nil, err
	}

	return accountsTrieStorage, nil
}

// IsInterfaceNil returns true if underlying object is nil
func (e *exportHandlerFactory) IsInterfaceNil() bool {
	return e == nil
}
