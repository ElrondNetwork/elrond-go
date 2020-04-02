package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go-logger/redirects"
	"github.com/ElrondNetwork/elrond-go/cmd/node/factory"
	"github.com/ElrondNetwork/elrond-go/cmd/node/metrics"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/accumulator"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/indexer"
	"github.com/ElrondNetwork/elrond-go/core/serviceContainer"
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/ElrondNetwork/elrond-go/crypto/signing/mcl"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/epochStart"
	"github.com/ElrondNetwork/elrond-go/epochStart/notifier"
	"github.com/ElrondNetwork/elrond-go/facade"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/node"
	"github.com/ElrondNetwork/elrond-go/node/external"
	"github.com/ElrondNetwork/elrond-go/ntp"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/coordinator"
	"github.com/ElrondNetwork/elrond-go/process/economics"
	"github.com/ElrondNetwork/elrond-go/process/factory/metachain"
	"github.com/ElrondNetwork/elrond-go/process/factory/shard"
	"github.com/ElrondNetwork/elrond-go/process/rating"
	"github.com/ElrondNetwork/elrond-go/process/smartContract"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/hooks"
	"github.com/ElrondNetwork/elrond-go/process/transaction"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
	storageFactory "github.com/ElrondNetwork/elrond-go/storage/factory"
	"github.com/ElrondNetwork/elrond-go/storage/lrucache"
	"github.com/ElrondNetwork/elrond-go/storage/pathmanager"
	"github.com/ElrondNetwork/elrond-go/storage/timecache"
	"github.com/ElrondNetwork/elrond-go/vm"
	"github.com/google/gops/agent"
	"github.com/urfave/cli"
)

const (
	defaultStatsPath        = "stats"
	defaultLogsPath         = "logs"
	defaultDBPath           = "db"
	defaultEpochString      = "Epoch"
	defaultStaticDbString   = "Static"
	defaultShardString      = "Shard"
	metachainShardName      = "metachain"
	defaultChancesSelection = 1
)

var (
	nodeHelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}
USAGE:
   {{.HelpName}} {{if .VisibleFlags}}[global options]{{end}}
   {{if len .Authors}}
AUTHOR:
   {{range .Authors}}{{ . }}{{end}}
   {{end}}{{if .Commands}}
GLOBAL OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}
VERSION:
   {{.Version}}
   {{end}}
`
	filePathPlaceholder = "[path]"
	// genesisFile defines a flag for the path of the bootstrapping file.
	genesisFile = cli.StringFlag{
		Name: "genesis-file",
		Usage: "The `" + filePathPlaceholder + "` for the genesis file. This JSON file contains initial data to " +
			"bootstrap from, such as initial balances for accounts.",
		Value: "./config/genesis.json",
	}
	// nodesFile defines a flag for the path of the initial nodes file.
	nodesFile = cli.StringFlag{
		Name: "nodes-setup-file",
		Usage: "The `" + filePathPlaceholder + "` for the nodes setup. This JSON file contains initial nodes info, " +
			"such as consensus group size, round duration, validators public keys and so on.",
		Value: "./config/nodesSetup.json",
	}
	// sk defines a flag for the path of the multi sign private key used when starting the node
	sk = cli.StringFlag{
		Name: "sk",
		Usage: "The `" + filePathPlaceholder + "` for the PEM file which contains the private key the node will " +
			"use for block signing.",
		Value: "",
	}
	// configurationFile defines a flag for the path to the main toml configuration file
	configurationFile = cli.StringFlag{
		Name: "config",
		Usage: "The `" + filePathPlaceholder + "` for the main configuration file. This TOML file contain the main " +
			"configurations such as storage setups, epoch duration and so on.",
		Value: "./config/config.toml",
	}
	// configurationEconomicsFile defines a flag for the path to the economics toml configuration file
	configurationEconomicsFile = cli.StringFlag{
		Name: "config-economics",
		Usage: "The `" + filePathPlaceholder + "` for the economics configuration file. This TOML file contains " +
			"economics configurations such as minimum gas price for a transactions and so on.",
		Value: "./config/economics.toml",
	}
	// configurationEconomicsFile defines a flag for the path to the economics toml configuration file
	configurationRatingsFile = cli.StringFlag{
		Name:  "config-ratings",
		Usage: "The ratings configuration file to load",
		Value: "./config/ratings.toml",
	}
	// configurationPreferencesFile defines a flag for the path to the preferences toml configuration file
	configurationPreferencesFile = cli.StringFlag{
		Name: "config-preferences",
		Usage: "The `" + filePathPlaceholder + "` for the preferences configuration file. This TOML file contains " +
			"preferences configurations, such as the node display name or the shard to start in when starting as observer",
		Value: "./config/prefs.toml",
	}
	// externalConfigFile defines a flag for the path to the external toml configuration file
	externalConfigFile = cli.StringFlag{
		Name: "config-external",
		Usage: "The `" + filePathPlaceholder + "` for the external configuration file. This TOML file contains" +
			" external configurations such as ElasticSearch's URL and login information",
		Value: "./config/external.toml",
	}
	// p2pConfigurationFile defines a flag for the path to the toml file containing P2P configuration
	p2pConfigurationFile = cli.StringFlag{
		Name: "p2p-config",
		Usage: "The `" + filePathPlaceholder + "` for the p2p configuration file. This TOML file contains peer-to-peer " +
			"configurations such as port, target peer count or KadDHT settings",
		Value: "./config/p2p.toml",
	}
	// gasScheduleConfigurationFile defines a flag for the path to the toml file containing the gas costs used in SmartContract execution
	gasScheduleConfigurationFile = cli.StringFlag{
		Name: "gas-costs-config",
		Usage: "The `" + filePathPlaceholder + "` for the gas costs configuration file. This TOML file contains " +
			"gas costs used in SmartContract execution",
		Value: "./config/gasSchedule.toml",
	}
	// port defines a flag for setting the port on which the node will listen for connections
	port = cli.IntFlag{
		Name:  "port",
		Usage: "The `[p2p port]` number on which the application will start",
		Value: 0,
	}
	// profileMode defines a flag for profiling the binary
	// If enabled, it will open the pprof routes over the default gin rest webserver.
	// There are several routes that will be available for profiling (profiling can be analyzed with: go tool pprof):
	//  /debug/pprof/ (can be accessed in the browser, will list the available options)
	//  /debug/pprof/goroutine
	//  /debug/pprof/heap
	//  /debug/pprof/threadcreate
	//  /debug/pprof/block
	//  /debug/pprof/mutex
	//  /debug/pprof/profile (CPU profile)
	//  /debug/pprof/trace?seconds=5 (CPU trace) -> being a trace, can be analyzed with: go tool trace
	// Usage: go tool pprof http(s)://ip.of.the.server/debug/pprof/xxxxx
	profileMode = cli.BoolFlag{
		Name: "profile-mode",
		Usage: "Boolean option for enabling the profiling mode. If set, the /debug/pprof routes will be available " +
			"on the node for profiling the application.",
	}
	// skIndex defines a flag that specifies the 0-th based index of the private key to be used from initialNodesSk.pem file
	skIndex = cli.IntFlag{
		Name:  "sk-index",
		Usage: "The index in the PEM file of the private key to be used by the node.",
		Value: 0,
	}
	// gopsEn used to enable diagnosis of running go processes
	gopsEn = cli.BoolFlag{
		Name:  "gops-enable",
		Usage: "Boolean option for enabling gops over the process. If set, stack can be viewed by calling 'gops stack <pid>'.",
	}
	// storageCleanup defines a flag for choosing the option of starting the node from scratch. If it is not set (false)
	// it starts from the last state stored on disk
	storageCleanup = cli.BoolFlag{
		Name: "storage-cleanup",
		Usage: "Boolean option for starting the node with clean storage. If set, the Node will empty its storage " +
			"before starting, otherwise it will start from the last state stored on disk..",
	}

	// restApiInterface defines a flag for the interface on which the rest API will try to bind with
	restApiInterface = cli.StringFlag{
		Name: "rest-api-interface",
		Usage: "The interface `address and port` to which the REST API will attempt to bind. " +
			"To bind to all available interfaces, set this flag to :8080",
		Value: facade.DefaultRestInterface,
	}

	// restApiDebug defines a flag for starting the rest API engine in debug mode
	restApiDebug = cli.BoolFlag{
		Name:  "rest-api-debug",
		Usage: "Boolean option for starting the Rest API in debug mode.",
	}

	// nodeDisplayName defines the friendly name used by a node in the public monitoring tools. If set, will override
	// the NodeDisplayName from prefs.toml
	nodeDisplayName = cli.StringFlag{
		Name: "display-name",
		Usage: "The user-friendly name for the node, appearing in the public monitoring tools. Will override the " +
			"name set in the preferences TOML file.",
		Value: "",
	}

	//useLogView is used when termui interface is not needed.
	useLogView = cli.BoolFlag{
		Name: "use-log-view",
		Usage: "Boolean option for enabling the simple node's interface. If set, the node will not enable the " +
			"user-friendly terminal view of the node.",
	}

	// initialNodesSkPemFile defines a flag for the path to the ...
	initialNodesSkPemFile = cli.StringFlag{
		Name:  "initial-nodes-sk-pem-file",
		Usage: "The `filepath` for the PEM file which contains the secret keys for initial nodes.",
		Value: "./config/initialNodesSk.pem",
	}
	// logLevel defines the logger level
	logLevel = cli.StringFlag{
		Name: "log-level",
		Usage: "This flag specifies the logger `level(s)`. It can contain multiple comma-separated value. For example" +
			", if set to *:INFO the logs for all packages will have the INFO level. However, if set to *:INFO,api:DEBUG" +
			" the logs for all packages will have the INFO level, excepting the api package which will receive a DEBUG" +
			" log level.",
		Value: "*:" + logger.LogInfo.String(),
	}
	//logFile is used when the log output needs to be logged in a file
	logSaveFile = cli.BoolFlag{
		Name:  "log-save",
		Usage: "Boolean option for enabling log saving. If set, it will automatically save all the logs into a file.",
	}
	//logWithCorrelation is used to enable log correlation elements
	logWithCorrelation = cli.BoolFlag{
		Name:  "log-correlation",
		Usage: "Boolean option for enabling log correlation elements.",
	}
	//logWithLoggerName is used to enable log correlation elements
	logWithLoggerName = cli.BoolFlag{
		Name:  "log-logger-name",
		Usage: "Boolean option for logger name in the logs.",
	}
	// disableAnsiColor defines if the logger subsystem should prevent displaying ANSI colors
	disableAnsiColor = cli.BoolFlag{
		Name:  "disable-ansi-color",
		Usage: "Boolean option for disabling ANSI colors in the logging system.",
	}
	// bootstrapRoundIndex defines a flag that specifies the round index from which node should bootstrap from storage
	bootstrapRoundIndex = cli.Uint64Flag{
		Name:  "bootstrap-round-index",
		Usage: "This flag specifies the round `index` from which node should bootstrap from storage.",
		Value: math.MaxUint64,
	}
	// enableTxIndexing enables transaction indexing. There can be cases when it's too expensive to index all transactions
	//  so we provide the command line option to disable this behaviour
	enableTxIndexing = cli.BoolTFlag{
		Name: "tx-indexing",
		Usage: "Boolean option for enabling transactions indexing. There can be cases when it's too expensive to " +
			"index all transactions so this flag will disable this.",
	}

	// workingDirectory defines a flag for the path for the working directory.
	workingDirectory = cli.StringFlag{
		Name:  "working-directory",
		Usage: "This flag specifies the `directory` where the node will store databases, logs and statistics.",
		Value: "",
	}

	// destinationShardAsObserver defines a flag for the prefered shard to be assigned to as an observer.
	destinationShardAsObserver = cli.StringFlag{
		Name: "destination-shard-as-observer",
		Usage: "This flag specifies the shard to start in when running as an observer. It will override the configuration " +
			"set in the preferences TOML config file.",
		Value: "",
	}

	isNodefullArchive = cli.BoolFlag{
		Name: "full-archive",
		Usage: "Boolean option for enabling a node to have full archive. If set, the node won't remove any database " +
			"and will have a full history over epochs.",
	}

	numEpochsToSave = cli.Uint64Flag{
		Name: "num-epochs-to-keep",
		Usage: "This flag represents the number of epochs which will kept in the databases. It is relevant only if " +
			"the full archive flag is not set.",
		Value: uint64(2),
	}

	numActivePersisters = cli.Uint64Flag{
		Name: "num-active-persisters",
		Usage: "This flag represents the number of databases (1 database = 1 epoch) which are kept open at a moment. " +
			"It is relevant even if the node is full archive or not.",
		Value: uint64(2),
	}

	rm *statistics.ResourceMonitor
)

// dbIndexer will hold the database indexer. Defined globally so it can be initialised only in
//  certain conditions. If those conditions will not be met, it will stay as nil
var dbIndexer indexer.Indexer

// coreServiceContainer is defined globally so it can be injected with appropriate
//  params depending on the type of node we are starting
var coreServiceContainer serviceContainer.Core

// appVersion should be populated at build time using ldflags
// Usage examples:
// linux/mac:
//            go build -i -v -ldflags="-X main.appVersion=$(git describe --tags --long --dirty)"
// windows:
//            for /f %i in ('git describe --tags --long --dirty') do set VERS=%i
//            go build -i -v -ldflags="-X main.appVersion=%VERS%"
var appVersion = core.UnVersionedAppString

func main() {
	_ = logger.SetDisplayByteSlice(logger.ToHexShort)
	log := logger.GetOrCreate("main")

	app := cli.NewApp()
	cli.AppHelpTemplate = nodeHelpTemplate
	app.Name = "Elrond Node CLI App"
	app.Version = fmt.Sprintf("%s/%s/%s-%s", appVersion, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	app.Usage = "This is the entry point for starting a new Elrond node - the app will start after the genesis timestamp"
	app.Flags = []cli.Flag{
		genesisFile,
		nodesFile,
		configurationFile,
		configurationEconomicsFile,
		configurationRatingsFile,
		configurationPreferencesFile,
		externalConfigFile,
		p2pConfigurationFile,
		gasScheduleConfigurationFile,
		sk,
		initialNodesSkPemFile,
		skIndex,
		port,
		profileMode,
		storageCleanup,
		gopsEn,
		nodeDisplayName,
		restApiInterface,
		restApiDebug,
		disableAnsiColor,
		logLevel,
		logSaveFile,
		logWithCorrelation,
		logWithLoggerName,
		useLogView,
		bootstrapRoundIndex,
		enableTxIndexing,
		workingDirectory,
		destinationShardAsObserver,
		isNodefullArchive,
		numEpochsToSave,
		numActivePersisters,
	}
	app.Authors = []cli.Author{
		{
			Name:  "The Elrond Team",
			Email: "contact@elrond.com",
		},
	}

	app.Action = func(c *cli.Context) error {
		return startNode(c, log, app.Version)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}

func getSuite(config *config.Config) (crypto.Suite, error) {
	switch config.Consensus.Type {
	case factory.BlsConsensusType:
		return mcl.NewSuiteBLS12(), nil
	default:
		return nil, errors.New("no consensus provided in config file")
	}
}

func startNode(ctx *cli.Context, log logger.Logger, version string) error {
	log.Trace("startNode called")
	workingDir := getWorkingDir(ctx, log)

	var err error
	withLogFile := ctx.GlobalBool(logSaveFile.Name)
	if withLogFile {
		var fileForLogs *os.File
		fileForLogs, err = prepareLogFile(workingDir)
		if err != nil {
			return fmt.Errorf("%w creating a log file", err)
		}

		defer func() {
			_ = fileForLogs.Close()
		}()
	}

	logger.ToggleCorrelation(ctx.GlobalBool(logWithCorrelation.Name))
	logger.ToggleLoggerName(ctx.GlobalBool(logWithLoggerName.Name))
	logLevelFlagValue := ctx.GlobalString(logLevel.Name)
	err = logger.SetLogLevel(logLevelFlagValue)
	if err != nil {
		return err
	}
	noAnsiColor := ctx.GlobalBool(disableAnsiColor.Name)
	if noAnsiColor {
		err = logger.RemoveLogObserver(os.Stdout)
		if err != nil {
			//we need to print this manually as we do not have console log observer
			fmt.Println("error removing log observer: " + err.Error())
			return err
		}

		err = logger.AddLogObserver(os.Stdout, &logger.PlainFormatter{})
		if err != nil {
			//we need to print this manually as we do not have console log observer
			fmt.Println("error setting log observer: " + err.Error())
			return err
		}
	}
	log.Trace("logger updated", "level", logLevelFlagValue, "disable ANSI color", noAnsiColor)

	enableGopsIfNeeded(ctx, log)

	log.Info("starting node", "version", version, "pid", os.Getpid())
	log.Trace("reading configs")

	configurationFileName := ctx.GlobalString(configurationFile.Name)
	generalConfig, err := loadMainConfig(configurationFileName)
	if err != nil {
		return err
	}
	log.Debug("config", "file", configurationFileName)

	configurationEconomicsFileName := ctx.GlobalString(configurationEconomicsFile.Name)
	economicsConfig, err := loadEconomicsConfig(configurationEconomicsFileName)
	if err != nil {
		return err
	}
	log.Debug("config", "file", configurationEconomicsFileName)

	configurationRatingsFileName := ctx.GlobalString(configurationRatingsFile.Name)
	ratingsConfig, err := loadRatingsConfig(configurationRatingsFileName)
	if err != nil {
		return err
	}
	log.Debug("config", "file", configurationRatingsFileName)

	configurationPreferencesFileName := ctx.GlobalString(configurationPreferencesFile.Name)
	preferencesConfig, err := loadPreferencesConfig(configurationPreferencesFileName)
	if err != nil {
		return err
	}
	log.Debug("config", "file", configurationPreferencesFileName)

	externalConfigurationFileName := ctx.GlobalString(externalConfigFile.Name)
	externalConfig, err := loadExternalConfig(externalConfigurationFileName)
	if err != nil {
		return err
	}
	log.Debug("config", "file", externalConfigurationFileName)

	p2pConfigurationFileName := ctx.GlobalString(p2pConfigurationFile.Name)
	p2pConfig, err := core.LoadP2PConfig(p2pConfigurationFileName)
	if err != nil {
		return err
	}

	log.Debug("config", "file", p2pConfigurationFileName)
	if ctx.IsSet(port.Name) {
		p2pConfig.Node.Port = uint32(ctx.GlobalUint(port.Name))
	}

	genesisConfig, err := sharding.NewGenesisConfig(ctx.GlobalString(genesisFile.Name))
	if err != nil {
		return err
	}
	log.Debug("config", "file", ctx.GlobalString(genesisFile.Name))

	nodesConfig, err := sharding.NewNodesSetup(ctx.GlobalString(nodesFile.Name))
	if err != nil {
		return err
	}
	log.Debug("config", "file", ctx.GlobalString(nodesFile.Name))

	syncer := ntp.NewSyncTime(generalConfig.NTPConfig, nil)
	go syncer.StartSync()

	log.Debug("NTP average clock offset", "value", syncer.ClockOffset())

	//TODO: The next 5 lines should be deleted when we are done testing from a precalculated (not hard coded) timestamp
	if nodesConfig.StartTime == 0 {
		time.Sleep(1000 * time.Millisecond)
		ntpTime := syncer.CurrentTime()
		nodesConfig.StartTime = (ntpTime.Unix()/60 + 1) * 60
	}

	startTime := time.Unix(nodesConfig.StartTime, 0)

	log.Info("start time",
		"formatted", startTime.Format("Mon Jan 2 15:04:05 MST 2006"),
		"seconds", startTime.Unix())

	log.Trace("getting suite")
	suite, err := getSuite(generalConfig)
	if err != nil {
		return err
	}

	initialNodesSkPemFileName := ctx.GlobalString(initialNodesSkPemFile.Name)
	keyGen, privKey, pubKey, err := factory.GetSigningParams(
		ctx,
		sk.Name,
		skIndex.Name,
		initialNodesSkPemFileName,
		suite)
	if err != nil {
		return fmt.Errorf("%w: consider regenerating your keys", err)
	}
	log.Debug("block sign pubkey", "hex", factory.GetPkEncoded(pubKey))

	if ctx.IsSet(destinationShardAsObserver.Name) {
		preferencesConfig.Preferences.DestinationShardAsObserver = ctx.GlobalString(destinationShardAsObserver.Name)
	}

	if ctx.IsSet(nodeDisplayName.Name) {
		preferencesConfig.Preferences.NodeDisplayName = ctx.GlobalString(nodeDisplayName.Name)
	}

	shardCoordinator, nodeType, err := createShardCoordinator(nodesConfig, pubKey, preferencesConfig.Preferences, log)
	if err != nil {
		return err
	}

	var shardId = core.GetShardIdString(shardCoordinator.SelfId())
	logger.SetCorrelationShard(shardId)

	pathTemplateForPruningStorer := filepath.Join(
		workingDir,
		defaultDBPath,
		nodesConfig.ChainID,
		fmt.Sprintf("%s_%s", defaultEpochString, core.PathEpochPlaceholder),
		fmt.Sprintf("%s_%s", defaultShardString, core.PathShardPlaceholder),
		core.PathIdentifierPlaceholder)

	pathTemplateForStaticStorer := filepath.Join(
		workingDir,
		defaultDBPath,
		nodesConfig.ChainID,
		defaultStaticDbString,
		fmt.Sprintf("%s_%s", defaultShardString, core.PathShardPlaceholder),
		core.PathIdentifierPlaceholder)

	var pathManager *pathmanager.PathManager
	pathManager, err = pathmanager.NewPathManager(pathTemplateForPruningStorer, pathTemplateForStaticStorer)
	if err != nil {
		return err
	}

	var currentEpoch uint32
	var errNotCritical error
	currentEpoch, errNotCritical = storageFactory.FindLastEpochFromStorage(
		workingDir,
		nodesConfig.ChainID,
		defaultDBPath,
		defaultEpochString,
	)
	if errNotCritical != nil {
		currentEpoch = 0
		log.Debug("no epoch db found in storage", "error", errNotCritical.Error())
	}

	storageCleanupFlagValue := ctx.GlobalBool(storageCleanup.Name)
	if storageCleanupFlagValue {
		dbPath := filepath.Join(
			workingDir,
			defaultDBPath)
		log.Trace("cleaning storage", "path", dbPath)
		err = os.RemoveAll(dbPath)
		if err != nil {
			return err
		}
	}

	log.Trace("creating core components")
	coreArgs := factory.NewCoreComponentsFactoryArgs(generalConfig, pathManager, shardId, []byte(nodesConfig.ChainID))
	coreComponents, err := factory.CoreComponentsFactory(coreArgs)
	if err != nil {
		return err
	}

	log.Trace("creating economics data components")
	economicsData, err := economics.NewEconomicsData(economicsConfig)
	if err != nil {
		return err
	}

	log.Trace("creating ratings data components")
	ratingsData, err := rating.NewRatingsData(ratingsConfig)
	if err != nil {
		return err
	}

	rater, err := rating.NewBlockSigningRater(ratingsData)
	if err != nil {
		return err
	}

	log.Trace("initializing stats file")
	err = initStatsFileMonitor(generalConfig, pubKey, log, workingDir, pathManager, shardId)
	if err != nil {
		return err
	}

	handlersArgs := factory.NewStatusHandlersFactoryArgs(useLogView.Name, ctx, coreComponents.InternalMarshalizer, coreComponents.Uint64ByteSliceConverter)
	statusHandlersInfo, err := factory.CreateStatusHandlers(handlersArgs)
	if err != nil {
		return err
	}

	coreComponents.StatusHandler = statusHandlersInfo.StatusHandler

	log.Trace("creating data components")
	epochStartNotifier := notifier.NewEpochStartSubscriptionHandler()
	dataArgs := factory.NewDataComponentsFactoryArgs(generalConfig, economicsData, shardCoordinator, coreComponents, pathManager, epochStartNotifier, currentEpoch)
	dataComponents, err := factory.DataComponentsFactory(dataArgs)
	if err != nil {
		return err
	}

	log.Trace("initializing metrics")
	metrics.InitMetrics(coreComponents.StatusHandler, pubKey, nodeType, shardCoordinator, nodesConfig, version, economicsConfig, generalConfig.EpochStartConfig.RoundsPerEpoch)

	err = statusHandlersInfo.UpdateStorerAndMetricsForPersistentHandler(dataComponents.Store.GetStorer(dataRetriever.StatusMetricsUnit))
	if err != nil {
		return err
	}

	log.Trace("creating nodes coordinator")
	if ctx.IsSet(isNodefullArchive.Name) {
		generalConfig.StoragePruning.FullArchive = ctx.GlobalBool(isNodefullArchive.Name)
	}
	if ctx.IsSet(numEpochsToSave.Name) {
		generalConfig.StoragePruning.NumEpochsToKeep = ctx.GlobalUint64(numEpochsToSave.Name)
	}
	if ctx.IsSet(numActivePersisters.Name) {
		generalConfig.StoragePruning.NumActivePersisters = ctx.GlobalUint64(numActivePersisters.Name)
	}

	nodesCoordinator, err := createNodesCoordinator(
		nodesConfig,
		preferencesConfig.Preferences,
		epochStartNotifier,
		pubKey,
		coreComponents.Hasher,
		rater,
		dataComponents.Store.GetStorer(dataRetriever.BootstrapUnit),
	)
	if err != nil {
		return err
	}

	log.Trace("creating state components")
	stateArgs := factory.NewStateComponentsFactoryArgs(
		generalConfig,
		genesisConfig,
		shardCoordinator,
		coreComponents,
		pathManager,
	)
	stateComponents, err := factory.StateComponentsFactory(stateArgs)
	if err != nil {
		return err
	}

	err = statusHandlersInfo.UpdateStorerAndMetricsForPersistentHandler(dataComponents.Store.GetStorer(dataRetriever.StatusMetricsUnit))
	if err != nil {
		return err
	}

	log.Trace("creating crypto components")
	cryptoArgs := factory.NewCryptoComponentsFactoryArgs(
		ctx,
		generalConfig,
		nodesConfig,
		shardCoordinator,
		keyGen,
		privKey,
		log,
	)
	cryptoComponents, err := factory.CryptoComponentsFactory(cryptoArgs)
	if err != nil {
		return err
	}

	metrics.SaveStringMetric(coreComponents.StatusHandler, core.MetricNodeDisplayName, preferencesConfig.Preferences.NodeDisplayName)
	metrics.SaveStringMetric(coreComponents.StatusHandler, core.MetricChainId, nodesConfig.ChainID)
	metrics.SaveUint64Metric(coreComponents.StatusHandler, core.MetricMinGasPrice, economicsData.MinGasPrice())

	sessionInfoFileOutput := fmt.Sprintf("%s:%s\n%s:%s\n%s:%v\n%s:%s\n%s:%v\n",
		"PkBlockSign", factory.GetPkEncoded(pubKey),
		"ShardId", shardId,
		"TotalShards", shardCoordinator.NumberOfShards(),
		"AppVersion", version,
		"GenesisTimeStamp", startTime.Unix(),
	)

	sessionInfoFileOutput += fmt.Sprintf("\nStarted with parameters:\n")
	for _, flag := range ctx.App.Flags {
		flagValue := fmt.Sprintf("%v", ctx.GlobalGeneric(flag.GetName()))
		if flagValue != "" {
			sessionInfoFileOutput += fmt.Sprintf("%s = %v\n", flag.GetName(), flagValue)
		}
	}

	statsFolder := filepath.Join(workingDir, defaultStatsPath)
	copyConfigToStatsFolder(
		statsFolder,
		[]string{
			configurationFileName,
			configurationEconomicsFileName,
			configurationRatingsFileName,
			configurationPreferencesFileName,
			p2pConfigurationFileName,
			configurationFileName,
			ctx.GlobalString(genesisFile.Name),
			ctx.GlobalString(nodesFile.Name),
		})

	statsFile := filepath.Join(statsFolder, "session.info")
	err = ioutil.WriteFile(statsFile, []byte(sessionInfoFileOutput), os.ModePerm)
	log.LogIfError(err)

	log.Trace("creating network components")
	networkComponents, err := factory.NetworkComponentsFactory(*p2pConfig, *generalConfig, coreComponents.StatusHandler)
	if err != nil {
		return err
	}

	log.Trace("creating tps benchmark components")
	tpsBenchmark, err := statistics.NewTPSBenchmark(shardCoordinator.NumberOfShards(), nodesConfig.RoundDuration/1000)
	if err != nil {
		return err
	}

	if externalConfig.ElasticSearchConnector.Enabled {
		log.Trace("creating elastic search components")
		dbIndexer, err = createElasticIndexer(
			ctx,
			externalConfig.ElasticSearchConnector,
			externalConfig.ElasticSearchConnector.URL,
			shardCoordinator,
			coreComponents.InternalMarshalizer,
			coreComponents.Hasher,
		)
		if err != nil {
			return err
		}

		err = setServiceContainer(shardCoordinator, tpsBenchmark)
		if err != nil {
			return err
		}
	}

	gasScheduleConfigurationFileName := ctx.GlobalString(gasScheduleConfigurationFile.Name)
	gasSchedule, err := core.LoadGasScheduleConfig(gasScheduleConfigurationFileName)
	if err != nil {
		return err
	}

	log.Trace("creating time cache for requested items components")
	requestedItemsHandler := timecache.NewTimeCache(time.Duration(uint64(time.Millisecond) * nodesConfig.RoundDuration))

	log.Trace("creating process components")
	processArgs := factory.NewProcessComponentsFactoryArgs(
		coreArgs,
		genesisConfig,
		economicsData,
		nodesConfig,
		gasSchedule,
		syncer,
		shardCoordinator,
		nodesCoordinator,
		dataComponents,
		coreComponents,
		cryptoComponents,
		stateComponents,
		networkComponents,
		coreServiceContainer,
		requestedItemsHandler,
		epochStartNotifier,
		&generalConfig.EpochStartConfig,
		0,
		rater,
		generalConfig.Marshalizer.SizeCheckDelta,
		generalConfig.StateTriesConfig.CheckpointRoundsModulus,
		generalConfig.GeneralSettings.MaxComputableRounds,
		generalConfig.Antiflood.NumConcurrentResolverJobs,
		generalConfig.BlockSizeThrottleConfig.MinSizeInBytes,
		generalConfig.BlockSizeThrottleConfig.MaxSizeInBytes,
		ratingsConfig.General.MaxRating,
	)
	processComponents, err := factory.ProcessComponentsFactory(processArgs)
	if err != nil {
		return err
	}

	var elasticIndexer indexer.Indexer
	if check.IfNil(coreServiceContainer) {
		elasticIndexer = nil
	} else {
		elasticIndexer = coreServiceContainer.Indexer()
	}
	log.Trace("creating node structure")
	currentNode, err := createNode(
		generalConfig,
		preferencesConfig,
		nodesConfig,
		economicsData,
		syncer,
		keyGen,
		privKey,
		pubKey,
		shardCoordinator,
		nodesCoordinator,
		coreComponents,
		stateComponents,
		dataComponents,
		cryptoComponents,
		processComponents,
		networkComponents,
		ctx.GlobalUint64(bootstrapRoundIndex.Name),
		version,
		elasticIndexer,
		requestedItemsHandler,
		epochStartNotifier,
	)
	if err != nil {
		return err
	}

	log.Trace("creating software checker structure")
	softwareVersionChecker, err := factory.CreateSoftwareVersionChecker(coreComponents.StatusHandler)
	if err != nil {
		log.Debug("nil software version checker", "error", err.Error())
	} else {
		softwareVersionChecker.StartCheckSoftwareVersion()
	}

	if shardCoordinator.SelfId() == core.MetachainShardId {
		log.Trace("activating nodesCoordinator's validators indexing")
		indexValidatorsListIfNeeded(elasticIndexer, nodesCoordinator, log)
	}

	log.Trace("creating api resolver structure")
	apiResolver, err := createApiResolver(
		stateComponents.AccountsAdapter,
		stateComponents.AddressConverter,
		dataComponents.Store,
		dataComponents.Blkc,
		coreComponents.InternalMarshalizer,
		coreComponents.Uint64ByteSliceConverter,
		shardCoordinator,
		statusHandlersInfo.StatusMetrics,
		gasSchedule,
		economicsData,
		cryptoComponents.MessageSignVerifier,
	)
	if err != nil {
		return err
	}

	log.Trace("starting status pooling components")
	statusPollingInterval := time.Duration(generalConfig.GeneralSettings.StatusPollingIntervalSec) * time.Second
	err = metrics.StartStatusPolling(
		currentNode.GetAppStatusHandler(),
		statusPollingInterval,
		networkComponents,
		processComponents,
	)
	if err != nil {
		return err
	}

	updateMachineStatisticsDuration := time.Second
	err = metrics.StartMachineStatisticsPolling(coreComponents.StatusHandler, updateMachineStatisticsDuration)
	if err != nil {
		return err
	}

	log.Trace("creating elrond node facade")
	restAPIServerDebugMode := ctx.GlobalBool(restApiDebug.Name)
	ef, err := facade.NewElrondNodeFacade(
		currentNode,
		apiResolver,
		restAPIServerDebugMode,
		generalConfig.Antiflood.WebServer,
	)
	if err != nil {
		return fmt.Errorf("%w while creating NodeFacade", err)
	}

	efConfig := &config.FacadeConfig{
		RestApiInterface: ctx.GlobalString(restApiInterface.Name),
		PprofEnabled:     ctx.GlobalBool(profileMode.Name),
	}

	ef.SetSyncer(syncer)
	ef.SetTpsBenchmark(tpsBenchmark)
	ef.SetConfig(efConfig)

	log.Trace("starting background services")
	ef.StartBackgroundServices()

	log.Debug("bootstrapping node...")
	err = ef.StartNode()
	if err != nil {
		log.Error("starting node failed", "epoch", currentEpoch, "error", err.Error())
		return err
	}

	log.Info("application is now running")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Info("terminating at user's signal...")

	log.Debug("closing all store units....")
	err = dataComponents.Store.CloseAll()
	log.LogIfError(err)

	dataTries := coreComponents.TriesContainer.GetAll()
	for _, trie := range dataTries {
		err = trie.ClosePersister()
		log.LogIfError(err)
	}

	if rm != nil {
		err = rm.Close()
		log.LogIfError(err)
	}

	log.Info("closing network connections...")
	err = networkComponents.NetMessenger.Close()
	log.LogIfError(err)

	return nil
}

func copyConfigToStatsFolder(statsFolder string, configs []string) {
	for _, configFile := range configs {
		copySingleFile(statsFolder, configFile)
	}
}

func copySingleFile(folder string, configFile string) {
	fileName := filepath.Base(configFile)

	source, err := core.OpenFile(configFile)
	if err != nil {
		return
	}
	defer func() {
		err = source.Close()
		if err != nil {
			fmt.Println(fmt.Sprintf("Could not close %s", source.Name()))
		}
	}()

	destPath := filepath.Join(folder, fileName)
	destination, err := os.Create(destPath)
	if err != nil {
		return
	}
	defer func() {
		err = destination.Close()
		if err != nil {
			fmt.Println(fmt.Sprintf("Could not close %s", source.Name()))
		}
	}()

	_, err = io.Copy(destination, source)
	if err != nil {
		fmt.Println(fmt.Sprintf("Could not copy %s", source.Name()))
	}
}

func getWorkingDir(ctx *cli.Context, log logger.Logger) string {
	var workingDir string
	var err error
	if ctx.IsSet(workingDirectory.Name) {
		workingDir = ctx.GlobalString(workingDirectory.Name)
	} else {
		workingDir, err = os.Getwd()
		if err != nil {
			log.LogIfError(err)
			workingDir = ""
		}
	}
	log.Trace("working directory", "path", workingDir)

	return workingDir
}

func prepareLogFile(workingDir string) (*os.File, error) {
	logDirectory := filepath.Join(workingDir, defaultLogsPath)
	fileForLog, err := core.CreateFile("elrond-go", logDirectory, "log")
	if err != nil {
		return nil, err
	}

	//we need this function as to close file.Close() when the code panics and the defer func associated
	//with the file pointer in the main func will never be reached
	runtime.SetFinalizer(fileForLog, func(f *os.File) {
		_ = f.Close()
	})

	err = redirects.RedirectStderr(fileForLog)
	if err != nil {
		return nil, err
	}

	err = logger.AddLogObserver(fileForLog, &logger.PlainFormatter{})
	if err != nil {
		return nil, fmt.Errorf("%w adding file log observer", err)
	}

	return fileForLog, nil
}

func indexValidatorsListIfNeeded(elasticIndexer indexer.Indexer, coordinator sharding.NodesCoordinator, log logger.Logger) {
	if check.IfNil(elasticIndexer) {
		return
	}

	validatorsPubKeys, err := coordinator.GetAllEligibleValidatorsPublicKeys(0)
	if err != nil {
		log.Warn("GetAllEligibleValidatorPublicKeys for epoch 0 failed", "error", err)
	}

	if len(validatorsPubKeys) > 0 {
		go elasticIndexer.SaveValidatorsPubKeys(validatorsPubKeys)
	}
}

func enableGopsIfNeeded(ctx *cli.Context, log logger.Logger) {
	var gopsEnabled bool
	if ctx.IsSet(gopsEn.Name) {
		gopsEnabled = ctx.GlobalBool(gopsEn.Name)
	}

	if gopsEnabled {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Error("failure to init gops", "error", err.Error())
		}
	}

	log.Trace("gops", "enabled", gopsEnabled)
}

func loadMainConfig(filepath string) (*config.Config, error) {
	cfg := &config.Config{}
	err := core.LoadTomlFile(cfg, filepath)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func loadEconomicsConfig(filepath string) (*config.EconomicsConfig, error) {
	cfg := &config.EconomicsConfig{}
	err := core.LoadTomlFile(cfg, filepath)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func loadRatingsConfig(filepath string) (config.RatingsConfig, error) {
	cfg := &config.RatingsConfig{}
	err := core.LoadTomlFile(cfg, filepath)
	if err != nil {
		return config.RatingsConfig{}, err
	}

	return *cfg, nil
}

func loadPreferencesConfig(filepath string) (*config.Preferences, error) {
	cfg := &config.Preferences{}
	err := core.LoadTomlFile(cfg, filepath)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func loadExternalConfig(filepath string) (*config.ExternalConfig, error) {
	cfg := &config.ExternalConfig{}
	err := core.LoadTomlFile(cfg, filepath)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func getShardIdFromNodePubKey(pubKey crypto.PublicKey, nodesConfig *sharding.NodesSetup) (uint32, error) {
	if pubKey == nil {
		return 0, errors.New("nil public key")
	}

	publicKey, err := pubKey.ToByteArray()
	if err != nil {
		return 0, err
	}

	selfShardId, err := nodesConfig.GetShardIDForPubKey(publicKey)
	if err != nil {
		return 0, err
	}

	return selfShardId, err
}

func createShardCoordinator(
	nodesConfig *sharding.NodesSetup,
	pubKey crypto.PublicKey,
	prefsConfig config.PreferencesConfig,
	log logger.Logger,
) (sharding.Coordinator, core.NodeType, error) {
	// TODO: after start in epoch is merged, this needs to be refactored as the shardID cannot always be taken
	// from initial configuration but needs to be determined by nodes coordinator
	selfShardId, err := getShardIdFromNodePubKey(pubKey, nodesConfig)
	nodeType := core.NodeTypeValidator
	if err == sharding.ErrPublicKeyNotFoundInGenesis {
		nodeType = core.NodeTypeObserver
		log.Info("starting as observer node")

		selfShardId, err = processDestinationShardAsObserver(prefsConfig)
	}
	if err != nil {
		return nil, "", err
	}

	var shardName string
	if selfShardId == core.MetachainShardId {
		shardName = metachainShardName
	} else {
		shardName = fmt.Sprintf("%d", selfShardId)
	}
	log.Info("shard info", "started in shard", shardName)

	shardCoordinator, err := sharding.NewMultiShardCoordinator(nodesConfig.NumberOfShards(), selfShardId)
	if err != nil {
		return nil, "", err
	}

	return shardCoordinator, nodeType, nil
}

func createNodesCoordinator(
	nodesConfig *sharding.NodesSetup,
	prefsConfig config.PreferencesConfig,
	epochStartSubscriber epochStart.EpochStartSubscriber,
	pubKey crypto.PublicKey,
	hasher hashing.Hasher,
	rater sharding.RaterHandler,
	bootStorer storage.Storer,
) (sharding.NodesCoordinator, error) {
	shardIDAsObserver, err := processDestinationShardAsObserver(prefsConfig)
	if err != nil {
		return nil, err
	}

	nbShards := nodesConfig.NumberOfShards()
	shardConsensusGroupSize := int(nodesConfig.ConsensusGroupSize)
	metaConsensusGroupSize := int(nodesConfig.MetaChainConsensusGroupSize)
	eligibleNodesInfo, waitingNodesInfo := nodesConfig.InitialNodesInfo()

	eligibleValidators, errEligibleValidators := nodesInfoToValidators(eligibleNodesInfo)
	if errEligibleValidators != nil {
		return nil, errEligibleValidators
	}

	waitingValidators, errWaitingValidators := nodesInfoToValidators(waitingNodesInfo)
	if errWaitingValidators != nil {
		return nil, errWaitingValidators
	}

	pubKeyBytes, err := pubKey.ToByteArray()
	if err != nil {
		return nil, err
	}

	nodeShuffler := sharding.NewXorValidatorsShuffler(
		nodesConfig.MinNodesPerShard,
		nodesConfig.MetaChainMinNodes,
		nodesConfig.Hysteresis,
		nodesConfig.Adaptivity,
	)

	consensusGroupCache, err := lrucache.NewCache(25000)
	if err != nil {
		return nil, err
	}

	argumentsNodesCoordinator := sharding.ArgNodesCoordinator{
		ShardConsensusGroupSize: shardConsensusGroupSize,
		MetaConsensusGroupSize:  metaConsensusGroupSize,
		Hasher:                  hasher,
		Shuffler:                nodeShuffler,
		EpochStartSubscriber:    epochStartSubscriber,
		BootStorer:              bootStorer,
		ShardIDAsObserver:       shardIDAsObserver,
		NbShards:                nbShards,
		EligibleNodes:           eligibleValidators,
		WaitingNodes:            waitingValidators,
		SelfPublicKey:           pubKeyBytes,
		ConsensusGroupCache:     consensusGroupCache,
	}

	baseNodesCoordinator, err := sharding.NewIndexHashedNodesCoordinator(argumentsNodesCoordinator)
	if err != nil {
		return nil, err
	}

	nodesCoordinator, err := sharding.NewIndexHashedNodesCoordinatorWithRater(baseNodesCoordinator, rater)
	if err != nil {
		return nil, err
	}

	return nodesCoordinator, nil
}

func nodesInfoToValidators(nodesInfo map[uint32][]*sharding.NodeInfo) (map[uint32][]sharding.Validator, error) {
	validatorsMap := make(map[uint32][]sharding.Validator)

	for shId, nodeInfoList := range nodesInfo {
		validators := make([]sharding.Validator, 0)
		for _, nodeInfo := range nodeInfoList {
			validator, err := sharding.NewValidator(nodeInfo.PubKey(), nodeInfo.Address(), defaultChancesSelection)
			if err != nil {
				return nil, err
			}

			validators = append(validators, validator)
		}
		validatorsMap[shId] = validators
	}

	return validatorsMap, nil
}

func processDestinationShardAsObserver(prefsConfig config.PreferencesConfig) (uint32, error) {
	destShard := strings.ToLower(prefsConfig.DestinationShardAsObserver)
	if len(destShard) == 0 {
		return 0, errors.New("option DestinationShardAsObserver is not set in prefs.toml")
	}
	if destShard == metachainShardName {
		return core.MetachainShardId, nil
	}

	val, err := strconv.ParseUint(destShard, 10, 32)
	if err != nil {
		return 0, errors.New("error parsing DestinationShardAsObserver option: " + err.Error())
	}

	return uint32(val), err
}

// createElasticIndexer creates a new elasticIndexer where the server listens on the url,
// authentication for the server is using the username and password
func createElasticIndexer(
	ctx *cli.Context,
	elasticSearchConfig config.ElasticSearchConfig,
	url string,
	coordinator sharding.Coordinator,
	marshalizer marshal.Marshalizer,
	hasher hashing.Hasher,
) (indexer.Indexer, error) {
	arguments := indexer.ElasticIndexerArgs{
		Url:              url,
		UserName:         elasticSearchConfig.Username,
		Password:         elasticSearchConfig.Password,
		ShardCoordinator: coordinator,
		Marshalizer:      marshalizer,
		Hasher:           hasher,
		Options:          &indexer.Options{TxIndexingEnabled: ctx.GlobalBoolT(enableTxIndexing.Name)},
	}

	var err error
	dbIndexer, err = indexer.NewElasticIndexer(arguments)
	if err != nil {
		return nil, err
	}

	return dbIndexer, nil
}

func getConsensusGroupSize(nodesConfig *sharding.NodesSetup, shardCoordinator sharding.Coordinator) (uint32, error) {
	if shardCoordinator.SelfId() == core.MetachainShardId {
		return nodesConfig.MetaChainConsensusGroupSize, nil
	}
	if shardCoordinator.SelfId() < shardCoordinator.NumberOfShards() {
		return nodesConfig.ConsensusGroupSize, nil
	}

	return 0, state.ErrUnknownShardId
}

func createNode(
	config *config.Config,
	preferencesConfig *config.Preferences,
	nodesConfig *sharding.NodesSetup,
	economicsData process.FeeHandler,
	syncer ntp.SyncTimer,
	keyGen crypto.KeyGenerator,
	privKey crypto.PrivateKey,
	pubKey crypto.PublicKey,
	shardCoordinator sharding.Coordinator,
	nodesCoordinator sharding.NodesCoordinator,
	coreData *factory.Core,
	state *factory.State,
	data *factory.Data,
	crypto *factory.Crypto,
	process *factory.Process,
	network *factory.Network,
	bootstrapRoundIndex uint64,
	version string,
	indexer indexer.Indexer,
	requestedItemsHandler dataRetriever.RequestedItemsHandler,
	epochStartSubscriber epochStart.EpochStartSubscriber,
) (*node.Node, error) {
	var err error
	var consensusGroupSize uint32
	consensusGroupSize, err = getConsensusGroupSize(nodesConfig, shardCoordinator)
	if err != nil {
		return nil, err
	}

	var txAccumulator node.Accumulator
	txAccumulatorConfig := config.Antiflood.TxAccumulator
	txAccumulator, err = accumulator.NewTimeAccumulator(
		time.Duration(txAccumulatorConfig.MaxAllowedTimeInMilliseconds)*time.Millisecond,
		time.Duration(txAccumulatorConfig.MaxDeviationTimeInMilliseconds)*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}

	networkShardingCollector, err := factory.PrepareNetworkShardingCollector(
		network,
		config,
		nodesCoordinator,
		shardCoordinator,
		process.EpochStartTrigger,
	)
	if err != nil {
		return nil, err
	}

	var nd *node.Node
	nd, err = node.NewNode(
		node.WithMessenger(network.NetMessenger),
		node.WithHasher(coreData.Hasher),
		node.WithInternalMarshalizer(coreData.InternalMarshalizer, config.Marshalizer.SizeCheckDelta),
		node.WithVmMarshalizer(coreData.VmMarshalizer),
		node.WithTxSignMarshalizer(coreData.TxSignMarshalizer),
		node.WithTxFeeHandler(economicsData),
		node.WithInitialNodesPubKeys(crypto.InitialPubKeys),
		node.WithAddressConverter(state.AddressConverter),
		node.WithAccountsAdapter(state.AccountsAdapter),
		node.WithBlockChain(data.Blkc),
		node.WithDataStore(data.Store),
		node.WithRoundDuration(nodesConfig.RoundDuration),
		node.WithConsensusGroupSize(int(consensusGroupSize)),
		node.WithSyncer(syncer),
		node.WithBlockProcessor(process.BlockProcessor),
		node.WithGenesisTime(time.Unix(nodesConfig.StartTime, 0)),
		node.WithRounder(process.Rounder),
		node.WithShardCoordinator(shardCoordinator),
		node.WithNodesCoordinator(nodesCoordinator),
		node.WithUint64ByteSliceConverter(coreData.Uint64ByteSliceConverter),
		node.WithSingleSigner(crypto.SingleSigner),
		node.WithMultiSigner(crypto.MultiSigner),
		node.WithKeyGen(keyGen),
		node.WithKeyGenForAccounts(crypto.TxSignKeyGen),
		node.WithPubKey(pubKey),
		node.WithPrivKey(privKey),
		node.WithForkDetector(process.ForkDetector),
		node.WithInterceptorsContainer(process.InterceptorsContainer),
		node.WithResolversFinder(process.ResolversFinder),
		node.WithConsensusType(config.Consensus.Type),
		node.WithTxSingleSigner(crypto.TxSingleSigner),
		node.WithTxStorageSize(config.TxStorage.Cache.Size),
		node.WithBootstrapRoundIndex(bootstrapRoundIndex),
		node.WithAppStatusHandler(coreData.StatusHandler),
		node.WithIndexer(indexer),
		node.WithEpochStartTrigger(process.EpochStartTrigger),
		node.WithEpochStartSubscriber(epochStartSubscriber),
		node.WithBlackListHandler(process.BlackListHandler),
		node.WithNetworkShardingCollector(networkShardingCollector),
		node.WithBootStorer(process.BootStorer),
		node.WithRequestedItemsHandler(requestedItemsHandler),
		node.WithHeaderSigVerifier(process.HeaderSigVerifier),
		node.WithValidatorStatistics(process.ValidatorsStatistics),
		node.WithValidatorsProvider(process.ValidatorsProvider),
		node.WithChainID(coreData.ChainID),
		node.WithBlockTracker(process.BlockTracker),
		node.WithRequestHandler(process.RequestHandler),
		node.WithInputAntifloodHandler(network.InputAntifloodHandler),
		node.WithTxAccumulator(txAccumulator),
	)
	if err != nil {
		return nil, errors.New("error creating node: " + err.Error())
	}

	err = nd.AddQueryHandler("interceptor", coreData.InterceptorResolverDebugHandler)
	if err != nil {
		return nil, err
	}

	err = nd.StartHeartbeat(config.Heartbeat, version, preferencesConfig.Preferences.NodeDisplayName)
	if err != nil {
		return nil, err
	}

	err = nd.ApplyOptions(node.WithDataPool(data.Datapool))
	if err != nil {
		return nil, errors.New("error creating node: " + err.Error())
	}

	if shardCoordinator.SelfId() < shardCoordinator.NumberOfShards() {
		err = nd.CreateShardedStores()
		if err != nil {
			return nil, err
		}
	}
	if shardCoordinator.SelfId() == core.MetachainShardId {
		err = nd.ApplyOptions(node.WithPendingMiniBlocksHandler(process.PendingMiniBlocksHandler))
		if err != nil {
			return nil, errors.New("error creating meta-node: " + err.Error())
		}
	}
	return nd, nil
}

func initStatsFileMonitor(
	config *config.Config,
	pubKey crypto.PublicKey,
	log logger.Logger,
	workingDir string,
	pathManager storage.PathManagerHandler,
	shardId string,
) error {

	publicKey, err := pubKey.ToByteArray()
	if err != nil {
		return err
	}

	hexPublicKey := core.GetTrimmedPk(hex.EncodeToString(publicKey))

	statsFile, err := core.CreateFile(hexPublicKey, filepath.Join(workingDir, defaultStatsPath), "txt")
	if err != nil {
		return err
	}
	err = startStatisticsMonitor(statsFile, config, log, pathManager, shardId)
	if err != nil {
		return err
	}
	return nil
}

func setServiceContainer(shardCoordinator sharding.Coordinator, tpsBenchmark *statistics.TpsBenchmark) error {
	var err error
	if shardCoordinator.SelfId() < shardCoordinator.NumberOfShards() {
		coreServiceContainer, err = serviceContainer.NewServiceContainer(serviceContainer.WithIndexer(dbIndexer))
		if err != nil {
			return err
		}
		return nil
	}
	if shardCoordinator.SelfId() == core.MetachainShardId {
		coreServiceContainer, err = serviceContainer.NewServiceContainer(
			serviceContainer.WithIndexer(dbIndexer),
			serviceContainer.WithTPSBenchmark(tpsBenchmark))
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("could not init core service container")
}

func startStatisticsMonitor(
	file *os.File,
	generalConfig *config.Config,
	log logger.Logger,
	pathManager storage.PathManagerHandler,
	shardId string,
) error {
	if !generalConfig.ResourceStats.Enabled {
		return nil
	}

	if generalConfig.ResourceStats.RefreshIntervalInSec < 1 {
		return errors.New("invalid RefreshIntervalInSec in section [ResourceStats]. Should be an integer higher than 1")
	}

	resMon, err := statistics.NewResourceMonitor(file)
	if err != nil {
		return err
	}

	go func() {
		for {
			err = resMon.SaveStatistics(generalConfig, pathManager, shardId)
			log.LogIfError(err)
			time.Sleep(time.Second * time.Duration(generalConfig.ResourceStats.RefreshIntervalInSec))
		}
	}()

	return nil
}

func createApiResolver(
	accnts state.AccountsAdapter,
	addrConv state.AddressConverter,
	storageService dataRetriever.StorageService,
	blockChain data.ChainHandler,
	marshalizer marshal.Marshalizer,
	uint64Converter typeConverters.Uint64ByteSliceConverter,
	shardCoordinator sharding.Coordinator,
	statusMetrics external.StatusMetricsHandler,
	gasSchedule map[string]map[string]uint64,
	economics *economics.EconomicsData,
	messageSigVerifier vm.MessageSignVerifier,
) (facade.ApiResolver, error) {
	var vmFactory process.VirtualMachinesContainerFactory
	var err error

	argsHook := hooks.ArgBlockChainHook{
		Accounts:         accnts,
		AddrConv:         addrConv,
		StorageService:   storageService,
		BlockChain:       blockChain,
		ShardCoordinator: shardCoordinator,
		Marshalizer:      marshalizer,
		Uint64Converter:  uint64Converter,
	}

	if shardCoordinator.SelfId() == core.MetachainShardId {
		vmFactory, err = metachain.NewVMContainerFactory(argsHook, economics, messageSigVerifier, gasSchedule)
		if err != nil {
			return nil, err
		}
	} else {
		vmFactory, err = shard.NewVMContainerFactory(economics.MaxGasLimitPerBlock(), gasSchedule, argsHook)
		if err != nil {
			return nil, err
		}
	}

	vmContainer, err := vmFactory.Create()
	if err != nil {
		return nil, err
	}

	scQueryService, err := smartContract.NewSCQueryService(vmContainer, economics)
	if err != nil {
		return nil, err
	}

	txTypeHandler, err := coordinator.NewTxTypeHandler(addrConv, shardCoordinator, accnts)
	if err != nil {
		return nil, err
	}

	txCostHandler, err := transaction.NewTransactionCostEstimator(txTypeHandler, economics, scQueryService, gasSchedule)
	if err != nil {
		return nil, err
	}

	return external.NewNodeApiResolver(scQueryService, statusMetrics, txCostHandler)
}
