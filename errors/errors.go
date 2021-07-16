package errors

import "errors"

// ErrAccountsAdapterCreation signals that the accounts adapter cannot be created based on provided data
var ErrAccountsAdapterCreation = errors.New("error creating accounts adapter")

// ErrBlockchainCreation signals that the blockchain cannot be created
var ErrBlockchainCreation = errors.New("can not create blockchain")

// ErrDataPoolCreation signals that the data pool cannot be created
var ErrDataPoolCreation = errors.New("can not create data pool")

// ErrDataStoreCreation signals that the data store cannot be created
var ErrDataStoreCreation = errors.New("can not create data store")

// ErrGenesisBlockNotInitialized signals that genesis block is not initialized
var ErrGenesisBlockNotInitialized = errors.New("genesis block is not initialized")

// ErrHasherCreation signals that the hasher cannot be created based on provided data
var ErrHasherCreation = errors.New("error creating hasher")

// ErrInvalidChainID signals that an invalid chain ID has been provided
var ErrInvalidChainID = errors.New("invalid chain ID in consensus")

// ErrInvalidConsensusConfig signals that an invalid consensus type is specified in the configuration file
var ErrInvalidConsensusConfig = errors.New("invalid consensus type provided in config file")

// ErrInvalidRoundDuration signals that an invalid round duration has been provided
var ErrInvalidRoundDuration = errors.New("invalid round duration provided")

// ErrInvalidTransactionVersion signals  that an invalid transaction version has been provided
var ErrInvalidTransactionVersion = errors.New("invalid transaction version")

// ErrInvalidWorkingDir signals that an invalid working directory has been provided
var ErrInvalidWorkingDir = errors.New("invalid working directory")

// ErrMarshalizerCreation signals that the marshalizer cannot be created based on provided data
var ErrMarshalizerCreation = errors.New("error creating marshalizer")

// ErrMissingMultiHasherConfig signals that the multihasher type isn't specified in the configuration file
var ErrMissingMultiHasherConfig = errors.New("no multisig hasher provided in config file")

// ErrMultiSigHasherMissmatch signals that an invalid multisig hasher was provided
var ErrMultiSigHasherMissmatch = errors.New("wrong multisig hasher provided for bls consensus type")

// ErrNilAccountsAdapter signals that a nil accounts adapter has been provided
var ErrNilAccountsAdapter = errors.New("nil accounts adapter")

// ErrNilAccountsParser signals that a nil accounts parser has been provided
var ErrNilAccountsParser = errors.New("nil accounts parser")

//ErrNilAddressPublicKeyConverter signals that an operation has been attempted to or with a nil public key converter implementation
var ErrNilAddressPublicKeyConverter = errors.New("nil address pubkey converter")

// ErrNilAlarmScheduler is raised when a valid alarm scheduler is expected but nil is used
var ErrNilAlarmScheduler = errors.New("nil alarm scheduler")

// ErrNilBlackListHandler signals that a nil black list handler was provided
var ErrNilBlackListHandler = errors.New("nil black list handler")

// ErrNilBlockChainHandler is raised when a valid blockchain handler is expected but nil used
var ErrNilBlockChainHandler = errors.New("blockchain handler is nil")

// ErrNilBlockProcessor is raised when a valid block processor is expected but nil used
var ErrNilBlockProcessor = errors.New("block processor is nil")

// ErrNilBlockSigner signals the nil block signer was provided
var ErrNilBlockSigner = errors.New("nil block signer")

// ErrNilBlockSignKeyGen is raised when a valid block sign key generator is expected but nil used
var ErrNilBlockSignKeyGen = errors.New("block sign key generator is nil")

// ErrNilBlockTracker signals that a nil block tracker has been provided
var ErrNilBlockTracker = errors.New("trying to set nil block tracker")

// ErrNilBootStorer signals that the provided boot storer is nil
var ErrNilBootStorer = errors.New("nil boot storer")

// ErrNilBootstrapComponentsHolder signals that the provided bootstrap components holder is nil
var ErrNilBootstrapComponentsHolder = errors.New("nil bootstrap components holder")

// ErrNilBootstrapComponentsFactory signals that the provided bootstrap components factory is nil
var ErrNilBootstrapComponentsFactory = errors.New("nil bootstrap components factory")

// ErrNilConsensusComponentsFactory signals that the provided consensus components factory is nil
var ErrNilConsensusComponentsFactory = errors.New("nil consensus components factory")

// ErrNilCryptoComponentsFactory signals that the provided crypto components factory is nil
var ErrNilCryptoComponentsFactory = errors.New("nil crypto components factory")

// ErrNilCoreComponentsFactory signals that the provided core components factory is nil
var ErrNilCoreComponentsFactory = errors.New("nil core components factory")

// ErrNilDataComponentsFactory signals that the provided data components factory is nil
var ErrNilDataComponentsFactory = errors.New("nil data components factory")

// ErrNilHeartbeatComponentsFactory signals that the provided heartbeat components factory is nil
var ErrNilHeartbeatComponentsFactory = errors.New("nil heartbeat components factory")

// ErrNilNetworkComponentsFactory signals that the provided network components factory is nil
var ErrNilNetworkComponentsFactory = errors.New("nil network components factory")

// ErrNilProcessComponentsFactory signals that the provided process components factory is nil
var ErrNilProcessComponentsFactory = errors.New("nil process components factory")

// ErrNilStateComponentsFactory signals that the provided state components factory is nil
var ErrNilStateComponentsFactory = errors.New("nil state components factory")

// ErrNilStatusComponentsFactory signals that the provided status components factory is nil
var ErrNilStatusComponentsFactory = errors.New("nil status components factory")

// ErrNilBootstrapParamsHandler signals that the provided bootstrap parameters handler is nil
var ErrNilBootstrapParamsHandler = errors.New("nil bootstrap parameters handler")

// ErrNilBroadcastMessenger is raised when a valid broadcast messenger is expected but nil used
var ErrNilBroadcastMessenger = errors.New("broadcast messenger is nil")

// ErrNilChronologyHandler is raised when a valid chronology handler is expected but nil used
var ErrNilChronologyHandler = errors.New("chronology handler is nil")

// ErrNilConsensusComponentsHolder signals that a nil consensus components holder was provided
var ErrNilConsensusComponentsHolder = errors.New("nil consensus components holder")

// ErrNilConsensusWorker signals that a nil consensus worker was provided
var ErrNilConsensusWorker = errors.New("nil consensus worker")

// ErrNilCoreComponents signals that an operation has been attempted with nil core components
var ErrNilCoreComponents = errors.New("nil core components provided")

// ErrNilCoreComponentsHolder signals that a nil core components holder was provided
var ErrNilCoreComponentsHolder = errors.New("nil core components holder")

// ErrNilCryptoComponents signals that a nil crypto components has been provided
var ErrNilCryptoComponents = errors.New("nil crypto components provided")

// ErrNilCryptoComponentsHolder signals that a nil crypto components holder was provided
var ErrNilCryptoComponentsHolder = errors.New("nil crypto components holder")

// ErrNilDataComponents signals that a nil data components instance was provided
var ErrNilDataComponents = errors.New("nil data components provided")

// ErrNilDataComponentsHolder signals that a nil data components holder has been provided
var ErrNilDataComponentsHolder = errors.New("nil data components holder")

// ErrNilEconomicsData signals that a nil economics data handler has been provided
var ErrNilEconomicsData = errors.New("nil economics data provided")

// ErrNilEconomicsHandler signals that a nil economics handler has been provided
var ErrNilEconomicsHandler = errors.New("nil economics handler")

// ErrNilOutportHandler signals that a nil outport handler has been provided
var ErrNilOutportHandler = errors.New("nil outport handler")

// ErrNilEpochNotifier signals that a nil epoch notifier has been provided
var ErrNilEpochNotifier = errors.New("nil epoch notifier")

// ErrNilEpochStartBootstrapper signals that a nil epoch start bootstrapper was provided
var ErrNilEpochStartBootstrapper = errors.New("nil epoch start bootstrapper")

// ErrNilEpochStartNotifier signals that a nil epoch start notifier was provided
var ErrNilEpochStartNotifier = errors.New("nil epoch start notifier provided")

// ErrNilEpochStartTrigger signals that a nil start of epoch trigger has been provided
var ErrNilEpochStartTrigger = errors.New("nil start of epoch trigger")

// ErrNilFallbackHeaderValidator signals that a nil fallback header validator has been provided
var ErrNilFallbackHeaderValidator = errors.New("nil fallback header validator")

// ErrNilForkDetector is raised when a valid fork detector is expected but nil used
var ErrNilForkDetector = errors.New("fork detector is nil")

// ErrNilGasSchedule signals that an operation has been attempted with a nil gas schedule
var ErrNilGasSchedule = errors.New("nil gas schedule")

// ErrNilHasher is raised when a valid hasher is expected but nil used
var ErrNilHasher = errors.New("nil hasher provided")

// ErrNilTxSignHasher is raised when a nil tx sign hasher is provided
var ErrNilTxSignHasher = errors.New("nil tx signing hasher")

// ErrNilHeaderConstructionValidator signals that a nil header construction validator was provided
var ErrNilHeaderConstructionValidator = errors.New("nil header construction validator")

// ErrNilHeaderIntegrityVerifier signals that a nil header integrity verifier has been provided
var ErrNilHeaderIntegrityVerifier = errors.New("nil header integrity verifier")

// ErrNilHeaderSigVerifier signals that a nil header sig verifier has been provided
var ErrNilHeaderSigVerifier = errors.New("")

// ErrNilHeartbeatComponents signals that a nil heartbeat components instance was provided
var ErrNilHeartbeatComponents = errors.New("nil heartbeat component")

// ErrNilHeartbeatMessageHandler signals that a nil heartbeat message handler was provided
var ErrNilHeartbeatMessageHandler = errors.New("nil heartbeat message handler")

// ErrNilHeartbeatMonitor signals that a nil heartbeat monitor was provided
var ErrNilHeartbeatMonitor = errors.New("nil heartbeat monitor")

// ErrNilHeartbeatSender signals that a nil heartbeat sender was provided
var ErrNilHeartbeatSender = errors.New("nil heartbeat sender")

// ErrNilHeartbeatStorer signals that a nil heartbeat storer was provided
var ErrNilHeartbeatStorer = errors.New("nil heartbeat storer")

// ErrNilInputAntiFloodHandler signals that a nil input antiflood handler was provided
var ErrNilInputAntiFloodHandler = errors.New("nil input antiflood handler")

// ErrNilInterceptorsContainer signals that a nil interceptors container was provided
var ErrNilInterceptorsContainer = errors.New("nil interceptors container")

// ErrNilInternalMarshalizer signals that a nil internal marshalizer was provided
var ErrNilInternalMarshalizer = errors.New("nil internal marshalizer")

// ErrNilKeyLoader signals that a nil key loader was provided
var ErrNilKeyLoader = errors.New("nil key loader")

// ErrNilMarshalizer signals that a nil marshalizer was provided
var ErrNilMarshalizer = errors.New("nil marshalizer provided")

// ErrNilMessageSignVerifier signals that a nil message signiature verifier was provided
var ErrNilMessageSignVerifier = errors.New("nil message sign verifier")

// ErrNilMessenger signals that a nil messenger was provided
var ErrNilMessenger = errors.New("nil messenger")

// ErrNilMiniBlocksProvider signals a nil miniBlocks provider
var ErrNilMiniBlocksProvider = errors.New("nil miniBlocks provider")

// ErrNilMultiSigner signals that a nil multi-signer was provided
var ErrNilMultiSigner = errors.New("nil multi signer")

// ErrNilNetworkComponents signals that a nil network components instance was provided
var ErrNilNetworkComponents = errors.New("nil network components")

// ErrNilNetworkComponentsHolder signals that a nil network components holder was provided
var ErrNilNetworkComponentsHolder = errors.New("nil network components holder")

// ErrNilNodesConfig signals that a nil nodes configuration was provided
var ErrNilNodesConfig = errors.New("nil nodes config")

// ErrNilNodesCoordinator signals that a nil nodes coordinator was provided
var ErrNilNodesCoordinator = errors.New("nil nodes coordinator")

// ErrNilOutputAntiFloodHandler signals that a nil output antiflood handler was provided
var ErrNilOutputAntiFloodHandler = errors.New("nil output antiflood handler")

// ErrNilPath signals that a nil path was provided
var ErrNilPath = errors.New("nil path provided")

// ErrNilPathHandler signals that a nil path handler was provided
var ErrNilPathHandler = errors.New("nil path handler")

// ErrNilPeerAccounts signals that a nil peer accounts instance was provided
var ErrNilPeerAccounts = errors.New("nil peer accounts")

// ErrNilPeerBlackListHandler signals that a nil peer black list handler was provided
var ErrNilPeerBlackListHandler = errors.New("nil peer black list handler")

// ErrNilPeerHonestyHandler signals that a nil peer honesty handler was provided
var ErrNilPeerHonestyHandler = errors.New("nil peer honesty handler")

// ErrNilPeerShardMapper signals that a nil peer shard mapper was provided
var ErrNilPeerShardMapper = errors.New("nil peer shard mapper")

// ErrNilPeerSignHandler signals that a nil peer sign handler was provided
var ErrNilPeerSignHandler = errors.New("nil peer signature handler")

// ErrNilPendingMiniBlocksHandler signals that a nil pending miniBlocks handler
var ErrNilPendingMiniBlocksHandler = errors.New("nil pending miniBlocks handler")

// ErrNilPoolsHolder signals that a nil pools holder was provided
var ErrNilPoolsHolder = errors.New("nil pools holder")

// ErrNilPrivateKey signals that a nil provate key was provided
var ErrNilPrivateKey = errors.New("nil private key")

// ErrNilProcessComponents signals that a nil process components instance was provided
var ErrNilProcessComponents = errors.New("nil process components")

// ErrNilProcessComponentsHolder signals that a nil procss components holder was provided
var ErrNilProcessComponentsHolder = errors.New("nil process components holder")

// ErrNilPubKeyConverter signals that a nil public key converter was provided
var ErrNilPubKeyConverter = errors.New("nil public key converter")

// ErrNilPublicKey signals that a nil public key was provided
var ErrNilPublicKey = errors.New("nil public key")

// ErrNilRater signals that a nil rater was provided
var ErrNilRater = errors.New("nil rater")

// ErrNilRatingData signals that nil rating data were provided
var ErrNilRatingData = errors.New("nil rating data")

// ErrNilRatingsInfoHandler signals that nil ratings data information was provided
var ErrNilRatingsInfoHandler = errors.New("nil ratings info handler")

// ErrNilRequestedItemHandler signals that a nil requested items handler was provided
var ErrNilRequestedItemHandler = errors.New("nil requested item handler")

// ErrNilRequestHandler signals that a nil request handler was provided
var ErrNilRequestHandler = errors.New("nil request handler")

// ErrNilResolversFinder signals that a nil resolver finder was provided
var ErrNilResolversFinder = errors.New("nil resolvers finder")

// ErrNilRoundHandler signals that a nil roundHandler was provided
var ErrNilRoundHandler = errors.New("nil roundHandler")

// ErrNilShardCoordinator signals that a nil shard coordinator was provided
var ErrNilShardCoordinator = errors.New("nil shard coordinator provided")

// ErrNilSmartContractParser signals that a nil smart contract parser was provided
var ErrNilSmartContractParser = errors.New("nil smart contract parser")

// ErrNilSoftwareVersion signals that a nil software version was provided
var ErrNilSoftwareVersion = errors.New("nil software version")

// ErrNilStateComponents signals that a nil state components was provided
var ErrNilStateComponents = errors.New("nil state components")

// ErrNilStateComponentsHolder signals that a nil state components holder was provided
var ErrNilStateComponentsHolder = errors.New("nil state components holder")

// ErrNilStatusComponents signals that a nil status components instance was provided
var ErrNilStatusComponents = errors.New("nil status components")

// ErrNilStatusComponentsHolder signals that a nil status components holder was provided
var ErrNilStatusComponentsHolder = errors.New("nil status components holder")

// ErrNilStatusHandler signals that a nil status handler was provided
var ErrNilStatusHandler = errors.New("nil status handler provided")

// ErrNilHardforkTrigger signals that a nil hardfork trigger was provided
var ErrNilHardforkTrigger = errors.New("nil hardfork trigger")

// ErrNilStorageManagers signals that a nil storage managers instance was provided
var ErrNilStorageManagers = errors.New("nil storage managers")

// ErrNilStorageService signals that a nil storage service was provided
var ErrNilStorageService = errors.New("nil storage service")

// ErrNilSyncTimer signals that a nil ntp synchronized timer was provided
var ErrNilSyncTimer = errors.New("nil sync timer provided")

// ErrNilSystemSCConfig signals that a nil system smart contracts cofiguration was provided
var ErrNilSystemSCConfig = errors.New("nil system smart contract configuration")

// ErrNilTpsBenchmark signals that a nil tps benchmark handler was provided
var ErrNilTpsBenchmark = errors.New("nil tps benchmark")

// ErrNilTriesContainer signals that a nil tries container was provided
var ErrNilTriesContainer = errors.New("nil tries container provided")

// ErrNilTriesStorageManagers signals that nil tries storage managers were provided
var ErrNilTriesStorageManagers = errors.New("nil tries storage managers provided")

// ErrNilTrieStorageManager signals that a nil trie storage manager was provided
var ErrNilTrieStorageManager = errors.New("nil trie storage manager")

// ErrNilTxLogsProcessor signals that a nil transaction logs processor was provided
var ErrNilTxLogsProcessor = errors.New("nil transaction logs processor")

// ErrNilTxSigner signals that a nil transaction signer was provided
var ErrNilTxSigner = errors.New("nil transaction signer")

// ErrNilTxSignKeyGen signals that a nil transaction signer key generator was provided
var ErrNilTxSignKeyGen = errors.New("nil transaction signing key generator")

// ErrNilTxSignMarshalizer signals that a nil transaction sign marshalizer was provided
var ErrNilTxSignMarshalizer = errors.New("nil transaction marshalizer")

// ErrNilUint64ByteSliceConverter signals that a nil byte slice converter was provided
var ErrNilUint64ByteSliceConverter = errors.New("nil byte slice converter")

// ErrNilValidatorPublicKeyConverter signals that a nil validator public key converter was provided
var ErrNilValidatorPublicKeyConverter = errors.New("validator public key converter")

// ErrNilValidatorsProvider signals a nil validators provider
var ErrNilValidatorsProvider = errors.New("nil validator provider")

// ErrNilValidatorsStatistics signals a that nil validators statistics was handler was provided
var ErrNilValidatorsStatistics = errors.New("nil validator statistics")

// ErrNilVmMarshalizer signals that a nil vm marshalizer was provided
var ErrNilVmMarshalizer = errors.New("nil vm marshalizer")

// ErrNilWatchdog signals that a nil watchdog was provided
var ErrNilWatchdog = errors.New("nil watchdog")

// ErrNilWhiteListHandler signals that a nil whitelist handler was provided
var ErrNilWhiteListHandler = errors.New("nil white list handler")

// ErrNilWhiteListVerifiedTxs signals that a nil whitelist for verified transactions was prvovided
var ErrNilWhiteListVerifiedTxs = errors.New("nil white list verified txs")

// ErrPollingFunctionRegistration signals an error while registering the polling function registration
var ErrPollingFunctionRegistration = errors.New("cannot register handler func for num of connected peers")

// ErrPublicKeyMismatch signals a mismatch between two public keys that should have matched
var ErrPublicKeyMismatch = errors.New("public key mismatch between the computed and the one read from the file")

// ErrStatusPollingInit signals an error while initializing the application status polling
var ErrStatusPollingInit = errors.New("cannot init AppStatusPolling")

// ErrWrongTypeAssertion signals a wrong type assertion
var ErrWrongTypeAssertion = errors.New("wrong type assertion")

// ErrNewBootstrapDataProvider signals a new bootstrapDataProvider creation has failed
var ErrNewBootstrapDataProvider = errors.New("bootstrapDataProvider creation has failed")

// ErrBootstrapDataComponentsFactoryCreate signals that an error occured on bootstrapDataComponentsFactory create
var ErrBootstrapDataComponentsFactoryCreate = errors.New("bootstrapDataComponentsFactory create() failed")

// ErrConsensusComponentsFactoryCreate signals that an error occured on consensusComponentsFactory create
var ErrConsensusComponentsFactoryCreate = errors.New("consensusComponentsFactory create failed")

// ErrCoreComponentsFactoryCreate signals that an error occured on coreComponentsFactory create
var ErrCoreComponentsFactoryCreate = errors.New("coreComponentsFactory create failed")

// ErrCryptoComponentsFactoryCreate signals that an error occured on cryptoComponentsFactory create
var ErrCryptoComponentsFactoryCreate = errors.New("cryptoComponentsFactory create failed")

// ErrDataComponentsFactoryCreate signals that an error occured on dataComponentsFactory create
var ErrDataComponentsFactoryCreate = errors.New("dataComponentsFactory create failed")

// ErrHeartbeatComponentsFactoryCreate signals that an error occured on heartbeatComponentsFactory create
var ErrHeartbeatComponentsFactoryCreate = errors.New("heartbeatComponentsFactory create failed")

// ErrNetworkComponentsFactoryCreate signals that an error occured on networkComponentsFactory create
var ErrNetworkComponentsFactoryCreate = errors.New("networkComponentsFactory create failed")

// ErrStateComponentsFactoryCreate signals that an error occured on stateComponentsFactory create
var ErrStateComponentsFactoryCreate = errors.New("stateComponentsFactory create failed")

// ErrStatusComponentsFactoryCreate signals that an error occured on statusComponentsFactory create
var ErrStatusComponentsFactoryCreate = errors.New("statusComponentsFactory create failed")

// ErrNewEpochStartBootstrap signals a new epochStartBootstrap creation has failed
var ErrNewEpochStartBootstrap = errors.New("epochStartBootstrap creation has failed")

// ErrNewStorageEpochStartBootstrap signals that a new storageEpochStartBootstrap creation has failed
var ErrNewStorageEpochStartBootstrap = errors.New("storageEpochStartBootstrap creation has failed")

// ErrBootstrap signals the bootstrapping process has failed
var ErrBootstrap = errors.New("bootstrap process has failed")

// ErrNilDataPoolsHolder signals that a nil data pools holder was provided
var ErrNilDataPoolsHolder = errors.New("nil data pools holder")

// ErrNilNodeRedundancyHandler signals that a nil node redundancy handler was provided
var ErrNilNodeRedundancyHandler = errors.New("nil node redundancy handler")

// ErrNilLocker signals that a nil locker was provided
var ErrNilLocker = errors.New("nil locker")

// ErrNilCurrentEpochProvider signals that a nil current epoch provider was provided
var ErrNilCurrentEpochProvider = errors.New("nil current epoch provider")
