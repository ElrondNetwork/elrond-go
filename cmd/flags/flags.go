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
		Value: "b5671723b8c64b16b3d4f5a2db9a2e3b61426e87c945b5453279f0701a10c70f",
	}
	// WithUI defines a flag for choosing the option of starting with/without UI. If false, the node will start automatically
	WithUI = cli.BoolTFlag{
		Name:  "with-ui",
		Usage: "If true, the application will be accompanied by a UI. The node will have to be manually started from the UI",
	}
)
