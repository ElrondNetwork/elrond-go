package flags

import "github.com/urfave/cli"

var (
	// GenesisFile defines a flag for the path of the bootstrapping file.
	GenesisFile = cli.StringFlag{
		Name:  "genesis-file",
		Usage: "The node will extract bootstrapping info from the genesis.json",
		Value: "genesis.json",
	}
	// PrivateKey defines a flag for the path of the private key used when starting the node
	PrivateKey = cli.StringFlag{
		Name:  "private-key",
		Usage: "Private key that the node will load on startup and will sign transactions - temporary until we have a wallet that can do that",
	}
	// WithUI defines a flag for choosing the option of starting with/without UI. If false, the node will start automatically
	WithUI = cli.BoolTFlag{
		Name:  "with-ui",
		Usage: "If true, the application will be accompanied by a UI. The node will have to be manually started from the UI",
	}
	// Port defines a flag for setting the port on which the node will listen for connections
	Port = cli.IntFlag{
		Name:  "port",
		Usage: "Port number on which the application will start",
		Value: 4001,
	}
	// MaxAllowedPeers defines a flag for setting the maximum number of connections allowed at once
	MaxAllowedPeers = cli.IntFlag{
		Name:  "max-allowed-peers",
		Usage: "Maximum connections the user is willing to accept",
		Value: 4,
	}
	PublicKey = cli.StringFlag{
		Name:  "public-key",
		Usage: "Public key that the node will load on startup and will be its Id",
		Value: "16Uiu2HAmERYu2qxDQkUnWsxS6pVwjQBbv74HLq8e3SbFsTtMkdqz",
	}
)
