package factory

import (
	"context"
	"fmt"

	indexerFactory "github.com/ElrondNetwork/elastic-indexer-go/factory"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/core/statistics/softwareVersion/factory"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/errors"
	"github.com/ElrondNetwork/elrond-go/outport"
	outportDriverFactory "github.com/ElrondNetwork/elrond-go/outport/factory"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
)

// TODO: move app status handler initialization here

type statusComponents struct {
	statusHandler   core.AppStatusHandler
	tpsBenchmark    statistics.TPSBenchmark
	outportHandler  outport.OutportHandler
	softwareVersion statistics.SoftwareVersionChecker
	resourceMonitor statistics.ResourceMonitorHandler
	cancelFunc      func()
}

// StatusComponentsFactoryArgs redefines the arguments structure needed for the status components factory
type StatusComponentsFactoryArgs struct {
	Config             config.Config
	ExternalConfig     config.ExternalConfig
	EconomicsConfig    config.EconomicsConfig
	ShardCoordinator   sharding.Coordinator
	NodesCoordinator   sharding.NodesCoordinator
	EpochStartNotifier EpochStartNotifier
	CoreComponents     CoreComponentsHolder
	DataComponents     DataComponentsHolder
	NetworkComponents  NetworkComponentsHolder
	StateComponents    StateComponentsHolder
	IsInImportMode     bool
}

type statusComponentsFactory struct {
	config             config.Config
	externalConfig     config.ExternalConfig
	economicsConfig    config.EconomicsConfig
	shardCoordinator   sharding.Coordinator
	nodesCoordinator   sharding.NodesCoordinator
	epochStartNotifier EpochStartNotifier
	forkDetector       process.ForkDetector
	coreComponents     CoreComponentsHolder
	dataComponents     DataComponentsHolder
	networkComponents  NetworkComponentsHolder
	stateComponents    StateComponentsHolder
	isInImportMode     bool
}

// NewStatusComponentsFactory will return a status components factory
func NewStatusComponentsFactory(args StatusComponentsFactoryArgs) (*statusComponentsFactory, error) {
	if check.IfNil(args.CoreComponents) {
		return nil, errors.ErrNilCoreComponentsHolder
	}
	if check.IfNil(args.DataComponents) {
		return nil, errors.ErrNilDataComponentsHolder
	}
	if check.IfNil(args.NetworkComponents) {
		return nil, errors.ErrNilNetworkComponentsHolder
	}
	if check.IfNil(args.CoreComponents.AddressPubKeyConverter()) {
		return nil, fmt.Errorf("%w for address", errors.ErrNilPubKeyConverter)
	}
	if check.IfNil(args.CoreComponents.ValidatorPubKeyConverter()) {
		return nil, fmt.Errorf("%w for validator", errors.ErrNilPubKeyConverter)
	}
	if check.IfNil(args.ShardCoordinator) {
		return nil, errors.ErrNilShardCoordinator
	}
	if check.IfNil(args.NodesCoordinator) {
		return nil, errors.ErrNilNodesCoordinator
	}
	if check.IfNil(args.EpochStartNotifier) {
		return nil, errors.ErrNilEpochStartNotifier
	}

	return &statusComponentsFactory{
		config:             args.Config,
		externalConfig:     args.ExternalConfig,
		economicsConfig:    args.EconomicsConfig,
		shardCoordinator:   args.ShardCoordinator,
		nodesCoordinator:   args.NodesCoordinator,
		epochStartNotifier: args.EpochStartNotifier,
		coreComponents:     args.CoreComponents,
		dataComponents:     args.DataComponents,
		networkComponents:  args.NetworkComponents,
		stateComponents:    args.StateComponents,
		isInImportMode:     args.IsInImportMode,
	}, nil
}

// Create will create and return the status components
func (scf *statusComponentsFactory) Create() (*statusComponents, error) {
	var err error
	var resMon *statistics.ResourceMonitor
	log.Trace("initializing stats file")
	if scf.config.ResourceStats.Enabled {
		resMon, err = startStatisticsMonitor(
			&scf.config,
			scf.coreComponents.PathHandler(),
			core.GetShardIDString(scf.shardCoordinator.SelfId()))
		if err != nil {
			return nil, err
		}
	}

	log.Trace("creating software checker structure")
	softwareVersionCheckerFactory, err := factory.NewSoftwareVersionFactory(
		scf.coreComponents.StatusHandler(),
		scf.config.SoftwareVersionConfig,
	)
	if err != nil {
		return nil, err
	}

	softwareVersionChecker, err := softwareVersionCheckerFactory.Create()
	if err != nil {
		return nil, err
	}

	softwareVersionChecker.StartCheckSoftwareVersion()

	initialTpsBenchmark := scf.coreComponents.StatusHandlerUtils().LoadTpsBenchmarkFromStorage(
		scf.dataComponents.StorageService().GetStorer(dataRetriever.StatusMetricsUnit),
		scf.coreComponents.InternalMarshalizer(),
	)

	roundDurationSec := scf.coreComponents.GenesisNodesSetup().GetRoundDuration() / 1000
	if roundDurationSec < 1 {
		return nil, errors.ErrInvalidRoundDuration
	}
	tpsBenchmark, err := statistics.NewTPSBenchmarkWithInitialData(
		scf.coreComponents.StatusHandler(),
		initialTpsBenchmark,
		scf.shardCoordinator.NumberOfShards(),
		roundDurationSec,
	)
	if err != nil {
		return nil, err
	}

	outportHandler, err := scf.createOutportDriver()
	if err != nil {
		return nil, err
	}

	_, cancelFunc := context.WithCancel(context.Background())
	return &statusComponents{
		softwareVersion: softwareVersionChecker,
		tpsBenchmark:    tpsBenchmark,
		outportHandler:  outportHandler,
		statusHandler:   scf.coreComponents.StatusHandler(),
		resourceMonitor: resMon,
		cancelFunc:      cancelFunc,
	}, nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (scf *statusComponentsFactory) IsInterfaceNil() bool {
	return scf == nil
}

// Close closes all underlying components that need closing
func (pc *statusComponents) Close() error {
	pc.cancelFunc()

	if !check.IfNil(pc.softwareVersion) {
		log.LogIfError(pc.softwareVersion.Close())
	}

	if !check.IfNil(pc.resourceMonitor) {
		log.LogIfError(pc.resourceMonitor.Close())
	}

	return nil
}

// createElasticIndexer creates a new elasticIndexer where the server listens on the url,
// authentication for the server is using the username and password
func (scf *statusComponentsFactory) createOutportDriver() (outport.OutportHandler, error) {
	elasticSearchConfig := scf.externalConfig.ElasticSearchConnector
	indexerFactoryArgs := &indexerFactory.ArgsIndexerFactory{
		Enabled:                  elasticSearchConfig.Enabled,
		IndexerCacheSize:         elasticSearchConfig.IndexerCacheSize,
		ShardCoordinator:         scf.shardCoordinator,
		Url:                      elasticSearchConfig.URL,
		UserName:                 elasticSearchConfig.Username,
		Password:                 elasticSearchConfig.Password,
		Marshalizer:              scf.coreComponents.InternalMarshalizer(),
		Hasher:                   scf.coreComponents.Hasher(),
		EpochStartNotifier:       scf.epochStartNotifier,
		NodesCoordinator:         scf.nodesCoordinator,
		AddressPubkeyConverter:   scf.coreComponents.AddressPubKeyConverter(),
		ValidatorPubkeyConverter: scf.coreComponents.ValidatorPubKeyConverter(),
		EnabledIndexes:           elasticSearchConfig.EnabledIndexes,
		AccountsDB:               scf.stateComponents.AccountsAdapter(),
		Denomination:             scf.economicsConfig.GlobalSettings.Denomination,
		TransactionFeeCalculator: scf.coreComponents.EconomicsData(),
		UseKibana:                elasticSearchConfig.UseKibana,
		IsInImportDBMode:         scf.isInImportMode,
	}

	return outportDriverFactory.CreateOutport(indexerFactoryArgs)
}

func startStatisticsMonitor(
	generalConfig *config.Config,
	pathManager storage.PathManagerHandler,
	shardId string,
) (*statistics.ResourceMonitor, error) {
	if generalConfig.ResourceStats.RefreshIntervalInSec < 1 {
		return nil, fmt.Errorf("invalid RefreshIntervalInSec in section [ResourceStats]. Should be an integer higher than 1")
	}
	resMon, err := statistics.NewResourceMonitor(generalConfig, pathManager, shardId)
	if err != nil {
		return nil, err
	}

	resMon.StartMonitoring()

	return resMon, nil
}
