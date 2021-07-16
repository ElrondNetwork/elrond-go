package block

import (
	"github.com/ElrondNetwork/elrond-go/cmd/node/factory"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/dblookupext"
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/outport"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
)

type coreComponentsHolder interface {
	Hasher() hashing.Hasher
	InternalMarshalizer() marshal.Marshalizer
	Uint64ByteSliceConverter() typeConverters.Uint64ByteSliceConverter
	RoundHandler() consensus.RoundHandler
	StatusHandler() core.AppStatusHandler
	IsInterfaceNil() bool
}

type dataComponentsHolder interface {
	StorageService() dataRetriever.StorageService
	Datapool() dataRetriever.PoolsHolder
	Blockchain() data.ChainHandler
	IsInterfaceNil() bool
}

type bootstrapComponentsHolder interface {
	ShardCoordinator() sharding.Coordinator
	HeaderIntegrityVerifier() factory.HeaderIntegrityVerifierHandler
	IsInterfaceNil() bool
}

type statusComponentsHolder interface {
	OutportHandler() outport.OutportHandler
	TpsBenchmark() statistics.TPSBenchmark
	IsInterfaceNil() bool
}

// ArgBaseProcessor holds all dependencies required by the process data factory in order to create
// new instances
type ArgBaseProcessor struct {
	CoreComponents      coreComponentsHolder
	DataComponents      dataComponentsHolder
	BootstrapComponents bootstrapComponentsHolder
	StatusComponents    statusComponentsHolder

	Config              config.Config
	AccountsDB          map[state.AccountsDbIdentifier]state.AccountsAdapter
	ForkDetector        process.ForkDetector
	NodesCoordinator    sharding.NodesCoordinator
	FeeHandler          process.TransactionFeeHandler
	RequestHandler      process.RequestHandler
	BlockChainHook      process.BlockChainHookHandler
	TxCoordinator       process.TransactionCoordinator
	EpochStartTrigger   process.EpochStartTriggerHandler
	HeaderValidator     process.HeaderConstructionValidator
	BootStorer          process.BootStorer
	BlockTracker        process.BlockTracker
	BlockSizeThrottler  process.BlockSizeThrottler
	Version             string
	HistoryRepository   dblookupext.HistoryRepository
	EpochNotifier       process.EpochNotifier
	VMContainersFactory process.VirtualMachinesContainerFactory
	VmContainer         process.VirtualMachinesContainer
}

// ArgShardProcessor holds all dependencies required by the process data factory in order to create
// new instances of shard processor
type ArgShardProcessor struct {
	ArgBaseProcessor
}

// ArgMetaProcessor holds all dependencies required by the process data factory in order to create
// new instances of meta processor
type ArgMetaProcessor struct {
	ArgBaseProcessor
	PendingMiniBlocksHandler     process.PendingMiniBlocksHandler
	SCToProtocol                 process.SmartContractToProtocolHandler
	EpochStartDataCreator        process.EpochStartDataCreator
	EpochEconomics               process.EndOfEpochEconomics
	EpochRewardsCreator          process.RewardsCreator
	EpochValidatorInfoCreator    process.EpochStartValidatorInfoCreator
	EpochSystemSCProcessor       process.EpochStartSystemSCProcessor
	ValidatorStatisticsProcessor process.ValidatorStatisticsProcessor
	RewardsV2EnableEpoch         uint32
}
