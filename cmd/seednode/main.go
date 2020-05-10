package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/display"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/libp2p"
	"github.com/urfave/cli"
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
	port = cli.IntFlag{
		Name:  "port",
		Usage: "Port number on which the application will start",
		Value: 10000,
	}
	// p2pSeed defines a flag to be used as a seed when generating P2P credentials. Useful for seed nodes.
	p2pSeed = cli.StringFlag{
		Name:  "p2p-seed",
		Usage: "P2P seed will be used when generating credentials for p2p component. Can be any string.",
		Value: "seed",
	}

	p2pConfigurationFile = "./config/p2p.toml"
)

func main() {
	app := cli.NewApp()
	cli.AppHelpTemplate = seedNodeHelpTemplate
	app.Name = "SeedNode CLI App"
	app.Usage = "This is the entry point for starting a new seed node - the app will help bootnodes connect to the network"
	app.Flags = []cli.Flag{port, p2pSeed}
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
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func startNode(ctx *cli.Context) error {
	fmt.Println("Starting node...")

	stop := make(chan bool, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	p2pConfig, err := core.LoadP2PConfig(p2pConfigurationFile)
	if err != nil {
		return err
	}
	fmt.Printf("Initialized with p2p config from: %s\n", p2pConfigurationFile)
	if ctx.IsSet(port.Name) {
		p2pConfig.Node.Port = uint32(ctx.GlobalUint(port.Name))
	}
	if ctx.IsSet(p2pSeed.Name) {
		p2pConfig.Node.Seed = ctx.GlobalString(p2pSeed.Name)
	}

	fmt.Println("Seed node....")
	messenger, err := createNode(*p2pConfig)
	if err != nil {
		return err
	}

	err = messenger.Bootstrap()
	if err != nil {
		return err
	}

	go func() {
		<-sigs
		fmt.Println("terminating at user's signal...")
		stop <- true
	}()

	fmt.Println("Application is now running...")
	displayMessengerInfo(messenger)
	for {
		select {
		case <-stop:
			return nil
		case <-time.After(time.Second * 5):
			displayMessengerInfo(messenger)
		}
	}
}

func createNode(p2pConfig config.P2PConfig) (p2p.Messenger, error) {
	arg := libp2p.ArgsNetworkMessenger{
		Context:       context.Background(),
		ListenAddress: libp2p.ListenAddrWithIp4AndTcp,
		P2pConfig:     p2pConfig,
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
	fmt.Println(tbl)

	pids := messenger.ConnectedPeers()
	fmt.Printf("Seednode is connected to %d peers:\r\n", len(pids))

	headerConnectedAddresses := []string{"Peer", "Addresses", "Connected?"}

	sort.Slice(pids, func(i, j int) bool {
		return pids[i].Pretty() < pids[j].Pretty()
	})

	connAddresses := make([]*display.LineData, 0)
	for _, pid := range pids {
		addrs := messenger.PeerAddresses(pid)

		for i := 0; i < len(addrs); i++ {
			connected := ""
			if i == 0 {
				connected = "YES"
			}

			ld := display.NewLineData(i == len(addrs)-1, []string{pid.Pretty(), addrs[i], connected})

			connAddresses = append(connAddresses, ld)
		}

	}

	tbl2, _ := display.CreateTableString(headerConnectedAddresses, connAddresses)
	fmt.Println(tbl2)
}
