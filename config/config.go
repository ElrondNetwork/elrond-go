package config

// CacheConfig will map the cache configuration
type CacheConfig struct {
	Name                 string
	Type                 string
	Capacity             uint32
	SizePerSender        uint32
	SizeInBytes          uint64
	SizeInBytesPerSender uint32
	Shards               uint32
}

// HeadersPoolConfig will map the headers cache configuration
type HeadersPoolConfig struct {
	MaxHeadersPerShard            int
	NumElementsToRemoveOnEviction int
}

// DBConfig will map the database configuration
type DBConfig struct {
	FilePath          string
	Type              string
	BatchDelaySeconds int
	MaxBatchSize      int
	MaxOpenFiles      int
	UseTmpAsFilePath  bool
}

// BloomFilterConfig will map the bloom filter configuration
type BloomFilterConfig struct {
	Size     uint
	HashFunc []string
}

// StorageConfig will map the storage unit configuration
type StorageConfig struct {
	Cache CacheConfig
	DB    DBConfig
	Bloom BloomFilterConfig
}

// TrieSyncStorageConfig will map trie sync storage configuration
type TrieSyncStorageConfig struct {
	DB          DBConfig
	Capacity    uint32
	SizeInBytes uint64
}

// PubkeyConfig will map the public key configuration
type PubkeyConfig struct {
	Length          int
	Type            string
	SignatureLength int
}

// TypeConfig will map the string type configuration
type TypeConfig struct {
	Type string
}

// MarshalizerConfig holds the marshalizer related configuration
type MarshalizerConfig struct {
	Type string
	// TODO check if we still need this
	SizeCheckDelta uint32
}

// ConsensusConfig holds the consensus configuration parameters
type ConsensusConfig struct {
	Type string
}

// NTPConfig will hold the configuration for NTP queries
type NTPConfig struct {
	Hosts               []string
	Port                int
	TimeoutMilliseconds int
	SyncPeriodSeconds   int
	Version             int
}

// EvictionWaitingListConfig will hold the configuration for the EvictionWaitingList
type EvictionWaitingListConfig struct {
	Size uint
	DB   DBConfig
}

// EpochStartConfig will hold the configuration of EpochStart settings
type EpochStartConfig struct {
	MinRoundsBetweenEpochs            int64
	RoundsPerEpoch                    int64
	MinShuffledOutRestartThreshold    float64
	MaxShuffledOutRestartThreshold    float64
	MinNumConnectedPeersToStart       int
	MinNumOfPeersToConsiderBlockValid int
}

// BlockSizeThrottleConfig will hold the configuration for adaptive block size throttle
type BlockSizeThrottleConfig struct {
	MinSizeInBytes uint32
	MaxSizeInBytes uint32
}

// SoftwareVersionConfig will hold the configuration for software version checker
type SoftwareVersionConfig struct {
	StableTagLocation        string
	PollingIntervalInMinutes int
}

// Config will hold the entire application configuration parameters
type Config struct {
	MiniBlocksStorage               StorageConfig
	PeerBlockBodyStorage            StorageConfig
	BlockHeaderStorage              StorageConfig
	TxStorage                       StorageConfig
	UnsignedTransactionStorage      StorageConfig
	RewardTxStorage                 StorageConfig
	ShardHdrNonceHashStorage        StorageConfig
	MetaHdrNonceHashStorage         StorageConfig
	StatusMetricsStorage            StorageConfig
	ReceiptsStorage                 StorageConfig
	SmartContractsStorage           StorageConfig
	SmartContractsStorageForSCQuery StorageConfig
	TrieEpochRootHashStorage        StorageConfig

	BootstrapStorage StorageConfig
	MetaBlockStorage StorageConfig

	AccountsTrieStorage      StorageConfig
	PeerAccountsTrieStorage  StorageConfig
	TrieSnapshotDB           DBConfig
	EvictionWaitingList      EvictionWaitingListConfig
	StateTriesConfig         StateTriesConfig
	TrieStorageManagerConfig TrieStorageManagerConfig
	BadBlocksCache           CacheConfig

	TxBlockBodyDataPool         CacheConfig
	PeerBlockBodyDataPool       CacheConfig
	TxDataPool                  CacheConfig
	UnsignedTransactionDataPool CacheConfig
	RewardTransactionDataPool   CacheConfig
	TrieNodesChunksDataPool     CacheConfig
	WhiteListPool               CacheConfig
	WhiteListerVerifiedTxs      CacheConfig
	SmartContractDataPool       CacheConfig
	TrieSyncStorage             TrieSyncStorageConfig
	EpochStartConfig            EpochStartConfig
	AddressPubkeyConverter      PubkeyConfig
	ValidatorPubkeyConverter    PubkeyConfig
	Hasher                      TypeConfig
	MultisigHasher              TypeConfig
	Marshalizer                 MarshalizerConfig
	VmMarshalizer               TypeConfig
	TxSignMarshalizer           TypeConfig
	TxSignHasher                TypeConfig

	PublicKeyShardId      CacheConfig
	PublicKeyPeerId       CacheConfig
	PeerIdShardId         CacheConfig
	PublicKeyPIDSignature CacheConfig
	PeerHonesty           CacheConfig

	Antiflood           AntifloodConfig
	ResourceStats       ResourceStatsConfig
	Heartbeat           HeartbeatConfig
	ValidatorStatistics ValidatorStatisticsConfig
	GeneralSettings     GeneralSettingsConfig
	Consensus           ConsensusConfig
	StoragePruning      StoragePruningConfig
	LogsAndEvents       LogsAndEventsConfig

	NTPConfig               NTPConfig
	HeadersPoolConfig       HeadersPoolConfig
	BlockSizeThrottleConfig BlockSizeThrottleConfig
	VirtualMachine          VirtualMachineServicesConfig

	Hardfork HardforkConfig
	Debug    DebugConfig
	Health   HealthServiceConfig

	SoftwareVersionConfig SoftwareVersionConfig
	DbLookupExtensions    DbLookupExtensionsConfig
	Versions              VersionsConfig
	Logs                  LogsConfig
	TrieSync              TrieSyncConfig
	Resolvers             ResolverConfig
	VMOutputCacher        CacheConfig
}

// LogsConfig will hold settings related to the logging sub-system
type LogsConfig struct {
	LogFileLifeSpanInSec int
}

// StoragePruningConfig will hold settings related to storage pruning
type StoragePruningConfig struct {
	Enabled                        bool
	ValidatorCleanOldEpochsData    bool
	ObserverCleanOldEpochsData     bool
	NumEpochsToKeep                uint64
	NumActivePersisters            uint64
	FullArchiveNumActivePersisters uint32
}

// ResourceStatsConfig will hold all resource stats settings
type ResourceStatsConfig struct {
	Enabled              bool
	RefreshIntervalInSec int
}

// HeartbeatConfig will hold all heartbeat settings
type HeartbeatConfig struct {
	MinTimeToWaitBetweenBroadcastsInSec int
	MaxTimeToWaitBetweenBroadcastsInSec int
	DurationToConsiderUnresponsiveInSec int
	HeartbeatRefreshIntervalInSec       uint32
	HideInactiveValidatorIntervalInSec  uint32
	HeartbeatStorage                    StorageConfig
}

// ValidatorStatisticsConfig will hold validator statistics specific settings
type ValidatorStatisticsConfig struct {
	CacheRefreshIntervalInSec uint32
}

// MaxNodesChangeConfig defines a config change tuple, with a maximum number enabled in a certain epoch number
type MaxNodesChangeConfig struct {
	EpochEnable            uint32
	MaxNumNodes            uint32
	NodesToShufflePerShard uint32
}

// GeneralSettingsConfig will hold the general settings for a node
type GeneralSettingsConfig struct {
	StatusPollingIntervalSec int
	MaxComputableRounds      uint64
	StartInEpochEnabled      bool
	ChainID                  string
	MinTransactionVersion    uint32
	GenesisString            string
	GenesisMaxNumberOfShards uint32
}

// FacadeConfig will hold different configuration option that will be passed to the main ElrondFacade
type FacadeConfig struct {
	RestApiInterface string
	PprofEnabled     bool
}

// StateTriesConfig will hold information about state tries
type StateTriesConfig struct {
	CheckpointRoundsModulus     uint
	CheckpointsEnabled          bool
	AccountsStatePruningEnabled bool
	PeerStatePruningEnabled     bool
	MaxStateTrieLevelInMemory   uint
	MaxPeerTrieLevelInMemory    uint
	UserStatePruningQueueSize   uint
	PeerStatePruningQueueSize   uint
}

// TrieStorageManagerConfig will hold config information about trie storage manager
type TrieStorageManagerConfig struct {
	PruningBufferLen              uint32
	SnapshotsBufferLen            uint32
	MaxSnapshots                  uint32
	KeepSnapshots                 bool
	CheckpointHashesHolderMaxSize uint64
}

// EndpointsThrottlersConfig holds a pair of an endpoint and its maximum number of simultaneous go routines
type EndpointsThrottlersConfig struct {
	Endpoint         string
	MaxNumGoRoutines int32
}

// WebServerAntifloodConfig will hold the anti-flooding parameters for the web server
type WebServerAntifloodConfig struct {
	SimultaneousRequests         uint32
	SameSourceRequests           uint32
	SameSourceResetIntervalInSec uint32
	EndpointsThrottlers          []EndpointsThrottlersConfig
}

// BlackListConfig will hold the p2p peer black list threshold values
type BlackListConfig struct {
	ThresholdNumMessagesPerInterval uint32
	ThresholdSizePerInterval        uint64
	NumFloodingRounds               uint32
	PeerBanDurationInSeconds        uint32
}

// TopicMaxMessagesConfig will hold the maximum number of messages/sec per topic value
type TopicMaxMessagesConfig struct {
	Topic             string
	NumMessagesPerSec uint32
}

// TopicAntifloodConfig will hold the maximum values per second to be used in certain topics
type TopicAntifloodConfig struct {
	DefaultMaxMessagesPerSec uint32
	MaxMessages              []TopicMaxMessagesConfig
}

// TxAccumulatorConfig will hold the tx accumulator config values
type TxAccumulatorConfig struct {
	MaxAllowedTimeInMilliseconds   uint32
	MaxDeviationTimeInMilliseconds uint32
}

// AntifloodConfig will hold all p2p antiflood parameters
type AntifloodConfig struct {
	Enabled                   bool
	NumConcurrentResolverJobs int32
	OutOfSpecs                FloodPreventerConfig
	FastReacting              FloodPreventerConfig
	SlowReacting              FloodPreventerConfig
	PeerMaxOutput             AntifloodLimitsConfig
	Cache                     CacheConfig
	WebServer                 WebServerAntifloodConfig
	Topic                     TopicAntifloodConfig
	TxAccumulator             TxAccumulatorConfig
}

// FloodPreventerConfig will hold all flood preventer parameters
type FloodPreventerConfig struct {
	IntervalInSeconds uint32
	ReservedPercent   float32
	PeerMaxInput      AntifloodLimitsConfig
	BlackList         BlackListConfig
}

// AntifloodLimitsConfig will hold the maximum antiflood limits in both number of messages and total
// size of the messages
type AntifloodLimitsConfig struct {
	BaseMessagesPerInterval uint32
	TotalSizePerInterval    uint64
	IncreaseFactor          IncreaseFactorConfig
}

// IncreaseFactorConfig defines the configurations used to increase the set values of a flood preventer
type IncreaseFactorConfig struct {
	Threshold uint32
	Factor    float32
}

// VirtualMachineServicesConfig holds configuration for the Virtual Machine(s): both querying and execution services.
type VirtualMachineServicesConfig struct {
	Execution VirtualMachineConfig
	Querying  QueryVirtualMachineConfig
}

// VirtualMachineConfig holds configuration for a Virtual Machine service
type VirtualMachineConfig struct {
	ArwenVersions []ArwenVersionByEpoch
}

// ArwenVersionByEpoch represents the Arwen version to be used starting with an epoch
type ArwenVersionByEpoch struct {
	StartEpoch uint32
	Version    string
}

// QueryVirtualMachineConfig holds the configuration for the virtual machine(s) used in query process
type QueryVirtualMachineConfig struct {
	VirtualMachineConfig
	NumConcurrentVMs int
}

// HardforkConfig holds the configuration for the hardfork trigger
type HardforkConfig struct {
	ExportStateStorageConfig     StorageConfig
	ExportKeysStorageConfig      StorageConfig
	ExportTriesStorageConfig     StorageConfig
	ImportStateStorageConfig     StorageConfig
	ImportKeysStorageConfig      StorageConfig
	PublicKeyToListenFrom        string
	ImportFolder                 string
	GenesisTime                  int64
	StartRound                   uint64
	StartNonce                   uint64
	CloseAfterExportInMinutes    uint32
	StartEpoch                   uint32
	ValidatorGracePeriodInEpochs uint32
	EnableTrigger                bool
	EnableTriggerFromP2P         bool
	MustImport                   bool
	AfterHardFork                bool
}

// LogsAndEventsConfig hold the configuration for the logs and events
type LogsAndEventsConfig struct {
	SaveInStorageEnabled bool
	TxLogsStorage        StorageConfig
}

// DbLookupExtensionsConfig holds the configuration for the db lookup extensions
type DbLookupExtensionsConfig struct {
	Enabled                            bool
	MiniblocksMetadataStorageConfig    StorageConfig
	MiniblockHashByTxHashStorageConfig StorageConfig
	EpochByHashStorageConfig           StorageConfig
	ResultsHashesByTxHashStorageConfig StorageConfig
}

// DebugConfig will hold debugging configuration
type DebugConfig struct {
	InterceptorResolver InterceptorResolverDebugConfig
	Antiflood           AntifloodDebugConfig
	ShuffleOut          ShuffleOutDebugConfig
	EpochStart          EpochStartDebugConfig
}

// HealthServiceConfig will hold health service (monitoring) configuration
type HealthServiceConfig struct {
	IntervalVerifyMemoryInSeconds             int
	IntervalDiagnoseComponentsInSeconds       int
	IntervalDiagnoseComponentsDeeplyInSeconds int
	MemoryUsageToCreateProfiles               int
	NumMemoryUsageRecordsToKeep               int
	FolderPath                                string
}

// InterceptorResolverDebugConfig will hold the interceptor-resolver debug configuration
type InterceptorResolverDebugConfig struct {
	Enabled                    bool
	EnablePrint                bool
	CacheSize                  int
	IntervalAutoPrintInSeconds int
	NumRequestsThreshold       int
	NumResolveFailureThreshold int
	DebugLineExpiration        int
}

// AntifloodDebugConfig will hold the antiflood debug configuration
type AntifloodDebugConfig struct {
	Enabled                    bool
	CacheSize                  int
	IntervalAutoPrintInSeconds int
}

// ShuffleOutDebugConfig will hold the shuffle out debug configuration
type ShuffleOutDebugConfig struct {
	CallGCWhenShuffleOut    bool
	ExtraPrintsOnShuffleOut bool
	DoProfileOnShuffleOut   bool
}

// EpochStartDebugConfig will hold the epoch debug configuration
type EpochStartDebugConfig struct {
	GoRoutineAnalyserEnabled     bool
	ProcessDataTrieOnCommitEpoch bool
}

// ApiRoutesConfig holds the configuration related to Rest API routes
type ApiRoutesConfig struct {
	Logging     ApiLoggingConfig
	APIPackages map[string]APIPackageConfig
}

// ApiLoggingConfig holds the configuration related to API requests logging
type ApiLoggingConfig struct {
	LoggingEnabled          bool
	ThresholdInMicroSeconds int
}

// APIPackageConfig holds the configuration for the routes of each package
type APIPackageConfig struct {
	Routes []RouteConfig
}

// RouteConfig holds the configuration for a single route
type RouteConfig struct {
	Name string
	Open bool
}

// VersionByEpochs represents a version entry that will be applied between the provided epochs
type VersionByEpochs struct {
	StartEpoch uint32
	Version    string
}

// VersionsConfig represents the versioning config area
type VersionsConfig struct {
	DefaultVersion   string
	VersionsByEpochs []VersionByEpochs
	Cache            CacheConfig
}

// Configs is a holder for the node configuration parameters
type Configs struct {
	GeneralConfig            *Config
	ApiRoutesConfig          *ApiRoutesConfig
	EconomicsConfig          *EconomicsConfig
	SystemSCConfig           *SystemSmartContractsConfig
	RatingsConfig            *RatingsConfig
	PreferencesConfig        *Preferences
	ExternalConfig           *ExternalConfig
	P2pConfig                *P2PConfig
	FlagsConfig              *ContextFlagsConfig
	ImportDbConfig           *ImportDbConfig
	ConfigurationPathsHolder *ConfigurationPathsHolder
	EpochConfig              *EpochConfig
}

// ConfigurationPathsHolder holds all configuration filenames and configuration paths used to start the node
type ConfigurationPathsHolder struct {
	MainConfig               string
	ApiRoutes                string
	Economics                string
	SystemSC                 string
	Ratings                  string
	Preferences              string
	External                 string
	P2p                      string
	GasScheduleDirectoryName string
	Nodes                    string
	Genesis                  string
	SmartContracts           string
	ValidatorKey             string
	Epoch                    string
}

// TrieSyncConfig represents the trie synchronization configuration area
type TrieSyncConfig struct {
	NumConcurrentTrieSyncers  int
	MaxHardCapForMissingNodes int
	TrieSyncerVersion         int
}

// ResolverConfig represents the config options to be used when setting up the resolver instances
type ResolverConfig struct {
	NumCrossShardPeers  uint32
	NumIntraShardPeers  uint32
	NumFullHistoryPeers uint32
}
