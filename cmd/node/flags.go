package main

import (
	"fmt"
	"math"
	"os"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/facade"
	"github.com/urfave/cli"
)

var (
	filePathPlaceholder = "[path]"
	// genesisFile defines a flag for the path of the bootstrapping file.
	genesisFile = cli.StringFlag{
		Name: "genesis-file",
		Usage: "The `" + filePathPlaceholder + "` for the genesis file. This JSON file contains initial data to " +
			"bootstrap from, such as initial balances for accounts.",
		Value: "./config/genesis.json",
	}
	// smartContractsFile defines a flag for the path of the file containing initial smart contracts.
	smartContractsFile = cli.StringFlag{
		Name: "smart-contracts-file",
		Usage: "The `" + filePathPlaceholder + "` for the initial smart contracts file. This JSON file contains data used " +
			"to deploy initial smart contracts such as delegation smart contracts",
		Value: "./config/genesisSmartContracts.json",
	}
	// nodesFile defines a flag for the path of the initial nodes file.
	nodesFile = cli.StringFlag{
		Name: "nodes-setup-file",
		Usage: "The `" + filePathPlaceholder + "` for the nodes setup. This JSON file contains initial nodes info, " +
			"such as consensus group size, round duration, validators public keys and so on.",
		Value: "./config/nodesSetup.json",
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
	// configurationApiFile defines a flag for the path to the api routes toml configuration file
	configurationApiFile = cli.StringFlag{
		Name: "config-api",
		Usage: "The `" + filePathPlaceholder + "` for the api configuration file. This TOML file contains " +
			"all available routes for Rest API and options to enable or disable them.",
		Value: "./config/api.toml",
	}
	// configurationSystemSCFile defines a flag for the path to the system sc toml configuration file
	configurationSystemSCFile = cli.StringFlag{
		Name:  "config-systemSmartContracts",
		Usage: "The `" + filePathPlaceholder + "` for the system smart contracts configuration file.",
		Value: "./config/systemSmartContractsConfig.toml",
	}
	// configurationRatingsFile defines a flag for the path to the ratings toml configuration file
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
	// epochConfigurationFile defines a flag for the path to the toml file containing the epoch configuration
	epochConfigurationFile = cli.StringFlag{
		Name: "epoch-config",
		Usage: "The `" + filePathPlaceholder + "` for the epoch configuration file. This TOML file contains" +
			" activation epochs configurations",
		Value: "./config/enableEpochs.toml",
	}
	// gasScheduleConfigurationDirectory defines a flag for the path to the directory containing the gas costs used in execution
	gasScheduleConfigurationDirectory = cli.StringFlag{
		Name:  "gas-costs-config",
		Usage: "The `" + filePathPlaceholder + "` for the gas costs configuration directory.",
		Value: "./config/gasSchedules",
	}
	// port defines a flag for setting the port on which the node will listen for connections
	port = cli.StringFlag{
		Name: "port",
		Usage: "The `[p2p port]` number on which the application will start. Can use single values such as " +
			"`0, 10230, 15670` or range of ports such as `5000-10000`",
		Value: "0",
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
	// useHealthService is used to enable the health service
	useHealthService = cli.BoolFlag{
		Name:  "use-health-service",
		Usage: "Boolean option for enabling the health service.",
	}
	// validatorKeyIndex defines a flag that specifies the 0-th based index of the private key to be used from validatorKey.pem file
	validatorKeyIndex = cli.IntFlag{
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

	// identityFlagName defines the keybase's identity. If set, will override the identity from prefs.toml
	identityFlagName = cli.StringFlag{
		Name:  "keybase-identity",
		Usage: "The keybase's identity. If set, will override the one set in the preferences TOML file.",
		Value: "",
	}

	//useLogView is a deprecated flag, but kept for backwards compatibility
	useLogView = cli.BoolFlag{
		Name: "use-log-view",
		Usage: "Deprecated flag. This flag's value is not used anymore as the only way the node starts now is within " +
			"log view, but because the majority of the nodes starting scripts have this flag, it was not removed.",
	}

	// validatorKeyPemFile defines a flag for the path to the validator key used in block signing
	validatorKeyPemFile = cli.StringFlag{
		Name:  "validator-key-pem-file",
		Usage: "The `filepath` for the PEM file which contains the secret keys for the validator key.",
		Value: "./config/validatorKey.pem",
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

	startInEpoch = cli.BoolFlag{
		Name: "start-in-epoch",
		Usage: "Boolean option for enabling a node the fast bootstrap mechanism from the network." +
			"Should be enabled if data is not available in local disk.",
	}

	// importDbDirectory defines a flag for the optional import DB directory on which the node will re-check the blockchain against
	importDbDirectory = cli.StringFlag{
		Name: "import-db",
		Usage: "This flag, if set, will make the node start the import process using the provided data path. Will re-check" +
			"and re-process everything",
		Value: "",
	}
	// importDbNoSigCheck defines a flag for the optional import DB no signature check option
	importDbNoSigCheck = cli.BoolFlag{
		Name:  "import-db-no-sig-check",
		Usage: "This flag, if set, will cause the signature checks on headers to be skipped. Can be used only if the import-db was previously set",
	}
	// importDbSaveEpochRootHash defines a flag for optional import DB trie exporting
	importDbSaveEpochRootHash = cli.BoolFlag{
		Name:  "import-db-save-epoch-root-hash",
		Usage: "This flag, if set, will export the trie snapshots at every new epoch",
	}
	// importDbStartInEpoch defines a flag for an optional flag that can specify the start in epoch value when executing the import-db process
	importDbStartInEpoch = cli.Uint64Flag{
		Name:  "import-db-start-epoch",
		Value: 0,
		Usage: "This flag will specify the start in epoch value in import-db process",
	}
	// redundancyLevel defines a flag that specifies the level of redundancy used by the current instance for the node (-1 = disabled, 0 = main instance (default), 1 = first backup, 2 = second backup, etc.)
	redundancyLevel = cli.Int64Flag{
		Name:  "redundancy-level",
		Usage: "This flag specifies the level of redundancy used by the current instance for the node (-1 = disabled, 0 = main instance (default), 1 = first backup, 2 = second backup, etc.)",
		Value: 0,
	}
	// fullArchive defines a flag that, if set, will make the node act like a full history node
	fullArchive = cli.BoolFlag{
		Name:  "full-archive",
		Usage: "Boolean option for settings an observer as full archive, which will sync the entire database of its shard",
	}
)

func getFlags() []cli.Flag {
	return []cli.Flag{
		genesisFile,
		smartContractsFile,
		nodesFile,
		configurationFile,
		configurationApiFile,
		configurationEconomicsFile,
		configurationSystemSCFile,
		configurationRatingsFile,
		configurationPreferencesFile,
		externalConfigFile,
		p2pConfigurationFile,
		epochConfigurationFile,
		gasScheduleConfigurationDirectory,
		validatorKeyIndex,
		validatorKeyPemFile,
		port,
		profileMode,
		useHealthService,
		storageCleanup,
		gopsEn,
		nodeDisplayName,
		identityFlagName,
		restApiInterface,
		restApiDebug,
		disableAnsiColor,
		logLevel,
		logSaveFile,
		logWithCorrelation,
		logWithLoggerName,
		useLogView,
		bootstrapRoundIndex,
		workingDirectory,
		destinationShardAsObserver,
		numEpochsToSave,
		numActivePersisters,
		startInEpoch,
		importDbDirectory,
		importDbNoSigCheck,
		importDbSaveEpochRootHash,
		importDbStartInEpoch,
		redundancyLevel,
		fullArchive,
	}
}

func getFlagsConfig(ctx *cli.Context, log logger.Logger) *config.ContextFlagsConfig {
	flagsConfig := &config.ContextFlagsConfig{}

	workingDir := ctx.GlobalString(workingDirectory.Name)
	flagsConfig.WorkingDir = getWorkingDir(workingDir, log)
	flagsConfig.EnableGops = ctx.GlobalBool(gopsEn.Name)
	flagsConfig.SaveLogFile = ctx.GlobalBool(logSaveFile.Name)
	flagsConfig.EnableLogCorrelation = ctx.GlobalBool(logWithCorrelation.Name)
	flagsConfig.EnableLogName = ctx.GlobalBool(logWithLoggerName.Name)
	flagsConfig.LogLevel = ctx.GlobalString(logLevel.Name)
	flagsConfig.DisableAnsiColor = ctx.GlobalBool(disableAnsiColor.Name)
	flagsConfig.CleanupStorage = ctx.GlobalBool(storageCleanup.Name)
	flagsConfig.UseHealthService = ctx.GlobalBool(useHealthService.Name)
	flagsConfig.BootstrapRoundIndex = ctx.GlobalUint64(bootstrapRoundIndex.Name)
	flagsConfig.EnableRestAPIServerDebugMode = ctx.GlobalBool(restApiDebug.Name)
	flagsConfig.RestApiInterface = ctx.GlobalString(restApiInterface.Name)
	flagsConfig.EnablePprof = ctx.GlobalBool(profileMode.Name)
	flagsConfig.UseLogView = ctx.GlobalBool(useLogView.Name)
	flagsConfig.ValidatorKeyIndex = ctx.GlobalInt(validatorKeyIndex.Name)
	return flagsConfig
}

func applyFlags(ctx *cli.Context, cfgs *config.Configs, flagsConfig *config.ContextFlagsConfig, log logger.Logger) error {

	cfgs.ConfigurationPathsHolder.Nodes = ctx.GlobalString(nodesFile.Name)
	cfgs.ConfigurationPathsHolder.Genesis = ctx.GlobalString(genesisFile.Name)
	cfgs.ConfigurationPathsHolder.GasScheduleDirectoryName = ctx.GlobalString(gasScheduleConfigurationDirectory.Name)
	cfgs.ConfigurationPathsHolder.SmartContracts = ctx.GlobalString(smartContractsFile.Name)
	cfgs.ConfigurationPathsHolder.ValidatorKey = ctx.GlobalString(validatorKeyPemFile.Name)

	if ctx.IsSet(startInEpoch.Name) {
		log.Debug("start in epoch is enabled")
		cfgs.GeneralConfig.GeneralSettings.StartInEpochEnabled = ctx.GlobalBool(startInEpoch.Name)
	}

	if ctx.IsSet(numEpochsToSave.Name) {
		cfgs.GeneralConfig.StoragePruning.NumEpochsToKeep = ctx.GlobalUint64(numEpochsToSave.Name)
	}
	if ctx.IsSet(numActivePersisters.Name) {
		cfgs.GeneralConfig.StoragePruning.NumActivePersisters = ctx.GlobalUint64(numActivePersisters.Name)
	}
	if ctx.IsSet(redundancyLevel.Name) {
		cfgs.PreferencesConfig.Preferences.RedundancyLevel = ctx.GlobalInt64(redundancyLevel.Name)
	}
	if ctx.IsSet(fullArchive.Name) {
		cfgs.PreferencesConfig.Preferences.FullArchive = ctx.GlobalBool(fullArchive.Name)
	}

	importDbDirectoryValue := ctx.GlobalString(importDbDirectory.Name)
	importDBConfigs := &config.ImportDbConfig{
		IsImportDBMode:                len(importDbDirectoryValue) > 0,
		ImportDBWorkingDir:            importDbDirectoryValue,
		ImportDbNoSigCheckFlag:        ctx.GlobalBool(importDbNoSigCheck.Name),
		ImportDbSaveTrieEpochRootHash: ctx.GlobalBool(importDbSaveEpochRootHash.Name),
		ImportDBStartInEpoch:          uint32(ctx.GlobalUint64(importDbStartInEpoch.Name)),
	}
	cfgs.FlagsConfig = flagsConfig
	cfgs.ImportDbConfig = importDBConfigs
	err := applyCompatibleConfigs(log, cfgs)
	if err != nil {
		return err
	}

	for _, flag := range ctx.App.Flags {
		flagValue := fmt.Sprintf("%v", ctx.GlobalGeneric(flag.GetName()))
		if flagValue != "" {
			flagsConfig.SessionInfoFileOutput += fmt.Sprintf("%s = %v\n", flag.GetName(), flagValue)
		}
	}

	return nil
}

func getWorkingDir(workingDir string, log logger.Logger) string {
	var err error
	if len(workingDir) == 0 {
		workingDir, err = os.Getwd()
		if err != nil {
			log.LogIfError(err)
			workingDir = ""
		}
	}
	log.Trace("working directory", "path", workingDir)

	return workingDir
}

func applyCompatibleConfigs(log logger.Logger, configs *config.Configs) error {
	importDbFlags := configs.ImportDbConfig
	importDbFlags.ImportDbNoSigCheckFlag = importDbFlags.ImportDbNoSigCheckFlag && importDbFlags.IsImportDBMode
	importDbFlags.ImportDbSaveTrieEpochRootHash = importDbFlags.ImportDbSaveTrieEpochRootHash && importDbFlags.IsImportDBMode

	if importDbFlags.IsImportDBMode {
		return processConfigImportDBMode(log, configs)
	}

	// if FullArchive is enabled, we override the conflicting StoragePruning settings and StartInEpoch as well
	if configs.PreferencesConfig.Preferences.FullArchive {
		return processConfigFullArchiveMode(log, configs)
	}

	return nil
}

func processConfigImportDBMode(log logger.Logger, configs *config.Configs) error {
	importDbFlags := configs.ImportDbConfig
	generalConfigs := configs.GeneralConfig
	p2pConfigs := configs.P2pConfig
	prefsConfig := configs.PreferencesConfig

	var err error

	importDbFlags.ImportDBTargetShardID, err = core.ProcessDestinationShardAsObserver(prefsConfig.Preferences.DestinationShardAsObserver)
	if err != nil {
		return err
	}

	if importDbFlags.ImportDBStartInEpoch == 0 {
		generalConfigs.GeneralSettings.StartInEpochEnabled = false
	}

	generalConfigs.StoragePruning.NumActivePersisters = generalConfigs.StoragePruning.NumEpochsToKeep
	generalConfigs.TrieStorageManagerConfig.KeepSnapshots = true
	generalConfigs.StateTriesConfig.CheckpointsEnabled = false
	p2pConfigs.Node.ThresholdMinConnectedPeers = 0
	p2pConfigs.KadDhtPeerDiscovery.Enabled = false

	defaultSecondsToCheckHealth := 300                   // 5minutes
	defaultDiagnoseMemoryLimit := 6 * 1024 * 1024 * 1024 // 6GB

	generalConfigs.Health.IntervalDiagnoseComponentsDeeplyInSeconds = defaultSecondsToCheckHealth
	generalConfigs.Health.IntervalDiagnoseComponentsInSeconds = defaultSecondsToCheckHealth
	generalConfigs.Health.IntervalVerifyMemoryInSeconds = defaultSecondsToCheckHealth
	generalConfigs.Health.MemoryUsageToCreateProfiles = defaultDiagnoseMemoryLimit

	alterStorageConfigsForDBImport(generalConfigs)

	log.Warn("the node is in import mode! Will auto-set some config values, including storage config values",
		"GeneralSettings.StartInEpochEnabled", generalConfigs.GeneralSettings.StartInEpochEnabled,
		"StateTriesConfig.CheckpointsEnabled", generalConfigs.StateTriesConfig.CheckpointsEnabled,
		"StoragePruning.NumActivePersisters", generalConfigs.StoragePruning.NumEpochsToKeep,
		"TrieStorageManagerConfig.KeepSnapshots", generalConfigs.TrieStorageManagerConfig.KeepSnapshots,
		"p2p.ThresholdMinConnectedPeers", p2pConfigs.Node.ThresholdMinConnectedPeers,
		"no sig check", importDbFlags.ImportDbNoSigCheckFlag,
		"import save trie epoch root hash", importDbFlags.ImportDbSaveTrieEpochRootHash,
		"import DB start in epoch", importDbFlags.ImportDBStartInEpoch,
		"import DB shard ID", importDbFlags.ImportDBTargetShardID,
		"kad dht discoverer", "off",
		"health interval diagnose components deeply in seconds", generalConfigs.Health.IntervalDiagnoseComponentsDeeplyInSeconds,
		"health interval diagnose components in seconds", generalConfigs.Health.IntervalDiagnoseComponentsInSeconds,
		"health interval verify memory in seconds", generalConfigs.Health.IntervalVerifyMemoryInSeconds,
		"health memory usage threshold", core.ConvertBytes(uint64(generalConfigs.Health.MemoryUsageToCreateProfiles)),
	)
	return nil
}

func processConfigFullArchiveMode(log logger.Logger, configs *config.Configs) error {
	generalConfigs := configs.GeneralConfig

	configs.GeneralConfig.GeneralSettings.StartInEpochEnabled = false
	configs.GeneralConfig.StoragePruning.ValidatorCleanOldEpochsData = false
	configs.GeneralConfig.StoragePruning.ObserverCleanOldEpochsData = false
	configs.GeneralConfig.StoragePruning.Enabled = true
	configs.GeneralConfig.StoragePruning.NumEpochsToKeep = math.MaxUint64

	log.Warn("the node is in full archive mode! Will auto-set some config values",
		"GeneralSettings.StartInEpochEnabled", generalConfigs.GeneralSettings.StartInEpochEnabled,
		"StoragePruning.ValidatorCleanOldEpochsData", generalConfigs.StoragePruning.ValidatorCleanOldEpochsData,
		"StoragePruning.ObserverCleanOldEpochsData", generalConfigs.StoragePruning.ObserverCleanOldEpochsData,
		"StoragePruning.Enabled", generalConfigs.StoragePruning.Enabled,
		"StoragePruning.NumEpochsToKeep", configs.GeneralConfig.StoragePruning.NumEpochsToKeep,
	)

	return nil
}

func alterStorageConfigsForDBImport(config *config.Config) {
	changeStorageConfigForDBImport(&config.MiniBlocksStorage)
	changeStorageConfigForDBImport(&config.BlockHeaderStorage)
	changeStorageConfigForDBImport(&config.MetaBlockStorage)
	changeStorageConfigForDBImport(&config.ShardHdrNonceHashStorage)
	changeStorageConfigForDBImport(&config.MetaHdrNonceHashStorage)
	changeStorageConfigForDBImport(&config.PeerAccountsTrieStorage)
}

func changeStorageConfigForDBImport(storageConfig *config.StorageConfig) {
	alterCoefficient := uint32(10)

	storageConfig.Cache.Capacity = storageConfig.Cache.Capacity * alterCoefficient
	storageConfig.DB.MaxBatchSize = storageConfig.DB.MaxBatchSize * int(alterCoefficient)
}
