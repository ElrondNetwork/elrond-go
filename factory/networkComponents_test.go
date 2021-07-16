package factory_test

import (
	"errors"
	"testing"

	"github.com/ElrondNetwork/elrond-go/config"
	errErd "github.com/ElrondNetwork/elrond-go/errors"
	"github.com/ElrondNetwork/elrond-go/factory"
	"github.com/ElrondNetwork/elrond-go/factory/mock"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/libp2p"
	"github.com/stretchr/testify/require"
)

func TestNewNetworkComponentsFactory_NilStatusHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := getNetworkArgs()
	args.StatusHandler = nil
	ncf, err := factory.NewNetworkComponentsFactory(args)
	require.Nil(t, ncf)
	require.Equal(t, errErd.ErrNilStatusHandler, err)
}

func TestNewNetworkComponentsFactory_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	args := getNetworkArgs()
	args.Marshalizer = nil
	ncf, err := factory.NewNetworkComponentsFactory(args)
	require.Nil(t, ncf)
	require.True(t, errors.Is(err, errErd.ErrNilMarshalizer))
}

func TestNewNetworkComponentsFactory_OkValsShouldWork(t *testing.T) {
	t.Parallel()
	args := getNetworkArgs()
	ncf, err := factory.NewNetworkComponentsFactory(args)
	require.NoError(t, err)
	require.NotNil(t, ncf)
}

func TestNetworkComponentsFactory_Create_ShouldErrDueToBadConfig(t *testing.T) {
	args := getNetworkArgs()
	args.MainConfig = config.Config{}
	args.P2pConfig = config.P2PConfig{}

	ncf, _ := factory.NewNetworkComponentsFactory(args)

	nc, err := ncf.Create()
	require.Error(t, err)
	require.Nil(t, nc)
}

func TestNetworkComponentsFactory_Create_ShouldWork(t *testing.T) {
	args := getNetworkArgs()
	ncf, _ := factory.NewNetworkComponentsFactory(args)
	ncf.SetListenAddress(libp2p.ListenLocalhostAddrWithIp4AndTcp)

	nc, err := ncf.Create()
	require.NoError(t, err)
	require.NotNil(t, nc)
}

// ------------ Test NetworkComponents --------------------
func TestNetworkComponents_Close_ShouldWork(t *testing.T) {
	args := getNetworkArgs()
	ncf, _ := factory.NewNetworkComponentsFactory(args)

	nc, _ := ncf.Create()

	err := nc.Close()
	require.NoError(t, err)
}

func getNetworkArgs() factory.NetworkComponentsFactoryArgs {
	p2pConfig := config.P2PConfig{
		Node: config.NodeConfig{
			Port: "0",
			Seed: "seed",
		},
		KadDhtPeerDiscovery: config.KadDhtPeerDiscoveryConfig{
			Enabled:                          false,
			Type:                             "optimized",
			RefreshIntervalInSec:             10,
			ProtocolID:                       "erd/kad/1.0.0",
			InitialPeerList:                  []string{"peer0", "peer1"},
			BucketSize:                       10,
			RoutingTableRefreshIntervalInSec: 5,
		},
		Sharding: config.ShardingConfig{
			TargetPeerCount:         10,
			MaxIntraShardValidators: 10,
			MaxCrossShardValidators: 10,
			MaxIntraShardObservers:  10,
			MaxCrossShardObservers:  10,
			MaxSeeders:              2,
			Type:                    "NilListSharder",
			AdditionalConnections: config.AdditionalConnectionsConfig{
				MaxFullHistoryObservers: 10,
			},
		},
	}

	mainConfig := config.Config{
		PeerHonesty: config.CacheConfig{
			Type:     "LRU",
			Capacity: 5000,
			Shards:   16,
		},
		Debug: config.DebugConfig{
			Antiflood: config.AntifloodDebugConfig{
				Enabled:                    true,
				CacheSize:                  100,
				IntervalAutoPrintInSeconds: 1,
			},
		},
	}

	appStatusHandler := &mock.AppStatusHandlerMock{}

	return factory.NetworkComponentsFactoryArgs{
		P2pConfig:     p2pConfig,
		MainConfig:    mainConfig,
		StatusHandler: appStatusHandler,
		Marshalizer:   &mock.MarshalizerMock{},
		RatingsConfig: config.RatingsConfig{
			General:    config.General{},
			ShardChain: config.ShardChain{},
			MetaChain:  config.MetaChain{},
			PeerHonesty: config.PeerHonestyConfig{
				DecayCoefficient:             0.9779,
				DecayUpdateIntervalInSeconds: 10,
				MaxScore:                     100,
				MinScore:                     -100,
				BadPeerThreshold:             -80,
				UnitValue:                    1.0,
			},
		},
		Syncer:            &libp2p.LocalSyncTimer{},
		NodeOperationMode: p2p.NormalOperation,
	}
}
