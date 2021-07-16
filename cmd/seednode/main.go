package main

import (
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/cmd/node/factory"
	"github.com/ElrondNetwork/elrond-go/cmd/seednode/api"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/logging"
	"github.com/ElrondNetwork/elrond-go/display"
	"github.com/ElrondNetwork/elrond-go/epochStart/bootstrap/disabled"
	"github.com/ElrondNetwork/elrond-go/facade"
	"github.com/ElrondNetwork/elrond-go/marshal"
	factoryMarshalizer "github.com/ElrondNetwork/elrond-go/marshal/factory"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/libp2p"
	"github.com/urfave/cli"
)

const (
	defaultLogsPath     = "logs"
	logFilePrefix       = "elrond-seed"
	filePathPlaceholder = "[path]"
)

var (
	seedNodeHelpTemplate = `NAME:
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
	// port defines a flag for setting the port on which the node will listen for connections
	port = cli.StringFlag{
		Name: "port",
		Usage: "The `[p2p port]` number on which the application will start. Can use single values such as " +
			"`0, 10230, 15670` or range of ports such as `5000-10000`",
		Value: "10000",
	}
	// restApiInterfaceFlag defines a flag for the interface on which the rest API will try to bind with
	restApiInterfaceFlag = cli.StringFlag{
		Name: "rest-api-interface",
		Usage: "The interface `address and port` to which the REST API will attempt to bind. " +
			"To bind to all available interfaces, set this flag to :8080. If set to `off` then the API won't be available",
		Value: facade.DefaultRestInterface,
	}
	// p2pSeed defines a flag to be used as a seed when generating P2P credentials. Useful for seed nodes.
	p2pSeed = cli.StringFlag{
		Name:  "p2p-seed",
		Usage: "P2P seed will be used when generating credentials for p2p component. Can be any string.",
		Value: "seed",
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
	// configurationFile defines a flag for the path to the main toml configuration file
	configurationFile = cli.StringFlag{
		Name: "config",
		Usage: "The `" + filePathPlaceholder + "` for the main configuration file. This TOML file contain the main " +
			"configurations such as the marshalizer type",
		Value: "./config/config.toml",
	}
	p2pConfigurationFile = "./config/p2p.toml"
)

var log = logger.GetOrCreate("main")

func main() {
	app := cli.NewApp()
	cli.AppHelpTemplate = seedNodeHelpTemplate
	app.Name = "SeedNode CLI App"
	app.Usage = "This is the entry point for starting a new seed node - the app will help bootnodes connect to the network"
	app.Flags = []cli.Flag{
		port,
		restApiInterfaceFlag,
		p2pSeed,
		logLevel,
		logSaveFile,
		configurationFile,
	}
	app.Version = "v0.0.1"
	app.Authors = []cli.Author{
		{
			Name:  "The Elrond Team",
			Email: "contact@elrond.com",
		},
	}

	app.Action = func(c *cli.Context) error {
		return startNode(c)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}

func startNode(ctx *cli.Context) error {
	var err error

	logLevelFlagValue := ctx.GlobalString(logLevel.Name)
	err = logger.SetLogLevel(logLevelFlagValue)
	if err != nil {
		return err
	}

	configurationFileName := ctx.GlobalString(configurationFile.Name)
	generalConfig, err := loadMainConfig(configurationFileName)
	if err != nil {
		return err
	}

	internalMarshalizer, err := factoryMarshalizer.NewMarshalizer(generalConfig.Marshalizer.Type)
	if err != nil {
		return fmt.Errorf("error creating marshalizer (internal): %s", err.Error())
	}

	withLogFile := ctx.GlobalBool(logSaveFile.Name)
	var fileLogging factory.FileLoggingHandler
	if withLogFile {
		workingDir := getWorkingDir(log)
		fileLogging, err = logging.NewFileLogging(workingDir, defaultLogsPath, logFilePrefix)
		if err != nil {
			return fmt.Errorf("%w creating a log file", err)
		}

		err = fileLogging.ChangeFileLifeSpan(time.Second * time.Duration(generalConfig.Logs.LogFileLifeSpanInSec))
		if err != nil {
			return err
		}
	}

	startRestServices(ctx, internalMarshalizer)

	log.Info("starting seednode...")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	p2pConfig, err := core.LoadP2PConfig(p2pConfigurationFile)
	if err != nil {
		return err
	}
	log.Info("initialized with p2p config",
		"filename", p2pConfigurationFile,
	)
	if ctx.IsSet(port.Name) {
		p2pConfig.Node.Port = ctx.GlobalString(port.Name)
	}
	if ctx.IsSet(p2pSeed.Name) {
		p2pConfig.Node.Seed = ctx.GlobalString(p2pSeed.Name)
	}

	err = checkExpectedPeerCount(*p2pConfig)
	if err != nil {
		return err
	}

	messenger, err := createNode(*p2pConfig, internalMarshalizer)
	if err != nil {
		return err
	}

	err = messenger.Bootstrap()
	if err != nil {
		return err
	}

	log.Info("application is now running...")
	mainLoop(messenger, sigs)

	log.Debug("closing seednode")
	if !check.IfNil(fileLogging) {
		err = fileLogging.Close()
		log.LogIfError(err)
	}

	return nil
}

func mainLoop(messenger p2p.Messenger, stop chan os.Signal) {
	displayMessengerInfo(messenger)
	for {
		select {
		case <-stop:
			log.Info("terminating at user's signal...")
			return
		case <-time.After(time.Second * 5):
			displayMessengerInfo(messenger)
		}
	}
}

func loadMainConfig(filepath string) (*config.Config, error) {
	cfg := &config.Config{}
	err := core.LoadTomlFile(cfg, filepath)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func createNode(p2pConfig config.P2PConfig, marshalizer marshal.Marshalizer) (p2p.Messenger, error) {
	arg := libp2p.ArgsNetworkMessenger{
		Marshalizer:          marshalizer,
		ListenAddress:        libp2p.ListenAddrWithIp4AndTcp,
		P2pConfig:            p2pConfig,
		SyncTimer:            &libp2p.LocalSyncTimer{},
		PreferredPeersHolder: disabled.NewPreferredPeersHolder(),
		NodeOperationMode:    p2p.NormalOperation,
	}

	return libp2p.NewNetworkMessenger(arg)
}

func displayMessengerInfo(messenger p2p.Messenger) {
	headerSeedAddresses := []string{"Seednode addresses:"}
	addresses := make([]*display.LineData, 0)

	for _, address := range messenger.Addresses() {
		addresses = append(addresses, display.NewLineData(false, []string{address}))
	}

	tbl, _ := display.CreateTableString(headerSeedAddresses, addresses)
	log.Info("\n" + tbl)

	mesConnectedAddrs := messenger.ConnectedAddresses()
	sort.Slice(mesConnectedAddrs, func(i, j int) bool {
		return strings.Compare(mesConnectedAddrs[i], mesConnectedAddrs[j]) < 0
	})

	log.Info("known peers", "num peers", len(messenger.Peers()))
	headerConnectedAddresses := []string{fmt.Sprintf("Seednode is connected to %d peers:", len(mesConnectedAddrs))}
	connAddresses := make([]*display.LineData, len(mesConnectedAddrs))

	for idx, address := range mesConnectedAddrs {
		connAddresses[idx] = display.NewLineData(false, []string{address})
	}

	tbl2, _ := display.CreateTableString(headerConnectedAddresses, connAddresses)
	log.Info("\n" + tbl2)
}

func getWorkingDir(log logger.Logger) string {
	workingDir, err := os.Getwd()
	if err != nil {
		log.LogIfError(err)
		workingDir = ""
	}

	log.Trace("working directory", "path", workingDir)

	return workingDir
}

func checkExpectedPeerCount(p2pConfig config.P2PConfig) error {
	maxExpectedPeerCount := p2pConfig.Node.MaximumExpectedPeerCount

	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("%w while getting RLimits", err)
	}

	log.Info("file limits",
		"current", rLimit.Cur,
		"max", rLimit.Max,
		"expected", maxExpectedPeerCount,
	)

	if maxExpectedPeerCount > rLimit.Cur {
		return fmt.Errorf("provided maxExpectedPeerCount is less than the current OS configured value")
	}

	return nil
}

func startRestServices(ctx *cli.Context, marshalizer marshal.Marshalizer) {
	restApiInterface := ctx.GlobalString(restApiInterfaceFlag.Name)
	if restApiInterface != facade.DefaultRestPortOff {
		go startGinServer(restApiInterface, marshalizer)
	} else {
		log.Info("rest api is disabled")
	}
}

func startGinServer(restApiInterface string, marshalizer marshal.Marshalizer) {
	err := api.Start(restApiInterface, marshalizer)
	if err != nil {
		log.LogIfError(err)
	}
}
