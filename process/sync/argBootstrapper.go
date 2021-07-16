package sync

import (
	"time"

	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/core"
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

// ArgBaseBootstrapper holds all dependencies required by the bootstrap data factory in order to create
// new instances
type ArgBaseBootstrapper struct {
	PoolsHolder          dataRetriever.PoolsHolder
	Store                dataRetriever.StorageService
	ChainHandler         data.ChainHandler
	RoundHandler         consensus.RoundHandler
	BlockProcessor       process.BlockProcessor
	WaitTime             time.Duration
	Hasher               hashing.Hasher
	Marshalizer          marshal.Marshalizer
	ForkDetector         process.ForkDetector
	RequestHandler       process.RequestHandler
	ShardCoordinator     sharding.Coordinator
	Accounts             state.AccountsAdapter
	BlackListHandler     process.TimeCacher
	NetworkWatcher       process.NetworkConnectionWatcher
	BootStorer           process.BootStorer
	StorageBootstrapper  process.BootstrapperFromStorage
	EpochHandler         dataRetriever.EpochHandler
	MiniblocksProvider   process.MiniBlockProvider
	Uint64Converter      typeConverters.Uint64ByteSliceConverter
	AppStatusHandler     core.AppStatusHandler
	OutportHandler       outport.OutportHandler
	AccountsDBSyncer     process.AccountsDBSyncer
	CurrentEpochProvider process.CurrentNetworkEpochProviderHandler
	IsInImportMode       bool
}

// ArgShardBootstrapper holds all dependencies required by the bootstrap data factory in order to create
// new instances of shard bootstrapper
type ArgShardBootstrapper struct {
	ArgBaseBootstrapper
}

// ArgMetaBootstrapper holds all dependencies required by the bootstrap data factory in order to create
// new instances of meta bootstrapper
type ArgMetaBootstrapper struct {
	ArgBaseBootstrapper
	EpochBootstrapper           process.EpochBootstrapper
	ValidatorStatisticsDBSyncer process.AccountsDBSyncer
	ValidatorAccountsDB         state.AccountsAdapter
}
