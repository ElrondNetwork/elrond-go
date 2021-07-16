package peerDisconnecting

import (
	"context"
	"testing"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/integrationTests"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/libp2p"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/p2pmocks"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var log = logger.GetOrCreate("integrationtests/p2p/peerdisconnecting")

func TestSeedersDisconnectionWith2AdvertiserAnd3Peers(t *testing.T) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	netw := mocknet.New(context.Background())
	p2pConfig := createDefaultConfig()
	p2pConfig.KadDhtPeerDiscovery.RefreshIntervalInSec = 1

	p2pConfig.Sharding = config.ShardingConfig{
		TargetPeerCount:         100,
		MaxIntraShardValidators: 40,
		MaxCrossShardValidators: 40,
		MaxIntraShardObservers:  1,
		MaxCrossShardObservers:  1,
		MaxSeeders:              3,
		Type:                    p2p.ListsSharder,
		AdditionalConnections: config.AdditionalConnectionsConfig{
			MaxFullHistoryObservers: 0,
		},
	}
	p2pConfig.Node.ThresholdMinConnectedPeers = 3

	numOfPeers := 3
	seeders, seedersList := createBootstrappedSeeders(p2pConfig, 2, netw)

	integrationTests.WaitForBootstrapAndShowConnected(seeders, integrationTests.P2pBootstrapDelay)

	//Step 2. Create noOfPeers instances of messenger type and call bootstrap
	p2pConfig.KadDhtPeerDiscovery.InitialPeerList = seedersList
	peers := make([]p2p.Messenger, numOfPeers)
	for i := 0; i < numOfPeers; i++ {
		arg := libp2p.ArgsNetworkMessenger{
			ListenAddress:        libp2p.ListenLocalhostAddrWithIp4AndTcp,
			P2pConfig:            p2pConfig,
			PreferredPeersHolder: &p2pmocks.PeersHolderStub{},
			NodeOperationMode:    p2p.NormalOperation,
			Marshalizer:          &testscommon.MarshalizerMock{},
			SyncTimer:            &testscommon.SyncTimerStub{},
		}
		node, err := libp2p.NewMockMessenger(arg, netw)
		require.Nil(t, err)
		peers[i] = node
	}

	//cleanup function that closes all messengers
	defer func() {
		for i := 0; i < numOfPeers; i++ {
			if peers[i] != nil {
				_ = peers[i].Close()
			}
		}

		for i := 0; i < len(seeders); i++ {
			if seeders[i] != nil {
				_ = seeders[i].Close()
			}
		}
	}()

	//link all peers so they can connect to each other
	_ = netw.LinkAll()

	//Step 3. Call bootstrap on all peers
	for _, p := range peers {
		_ = p.Bootstrap()
	}
	integrationTests.WaitForBootstrapAndShowConnected(append(seeders, peers...), integrationTests.P2pBootstrapDelay)

	//Step 4. Disconnect the seeders
	log.Info("--- Disconnecting seeders: %v ---\n", seeders)
	disconnectSeedersFromPeers(seeders, peers, netw)

	for i := 0; i < 2; i++ {
		integrationTests.WaitForBootstrapAndShowConnected(append(seeders, peers...), integrationTests.P2pBootstrapDelay)
	}

	//Step 4.1. Test that the peers are disconnected
	for _, p := range peers {
		assert.Equal(t, numOfPeers-1, len(p.ConnectedPeers()))
	}

	for _, s := range seeders {
		assert.Equal(t, len(seeders)-1, len(s.ConnectedPeers()))
	}

	//Step 5. Re-link and test connections
	log.Info("--- Re-linking ---")
	_ = netw.LinkAll()
	for i := 0; i < 2; i++ {
		integrationTests.WaitForBootstrapAndShowConnected(append(seeders, peers...), integrationTests.P2pBootstrapDelay)
	}

	//Step 5.1. Test that the peers got reconnected
	for _, p := range append(peers, seeders...) {
		assert.Equal(t, numOfPeers+len(seeders)-1, len(p.ConnectedPeers()))
	}
}

func createBootstrappedSeeders(baseP2PConfig config.P2PConfig, numSeeders int, netw mocknet.Mocknet) ([]p2p.Messenger, []string) {
	seeders := make([]p2p.Messenger, numSeeders)
	seedersAddresses := make([]string, numSeeders)

	p2pConfigSeeder := baseP2PConfig
	argSeeder := libp2p.ArgsNetworkMessenger{
		ListenAddress:        libp2p.ListenLocalhostAddrWithIp4AndTcp,
		P2pConfig:            p2pConfigSeeder,
		PreferredPeersHolder: &p2pmocks.PeersHolderStub{},
		NodeOperationMode:    p2p.NormalOperation,
		Marshalizer:          &testscommon.MarshalizerMock{},
		SyncTimer:            &testscommon.SyncTimerStub{},
	}
	seeders[0], _ = libp2p.NewMockMessenger(argSeeder, netw)
	_ = seeders[0].Bootstrap()
	seedersAddresses[0] = integrationTests.GetConnectableAddress(seeders[0])

	for i := 1; i < numSeeders; i++ {
		p2pConfigSeeder = baseP2PConfig
		p2pConfigSeeder.KadDhtPeerDiscovery.InitialPeerList = []string{integrationTests.GetConnectableAddress(seeders[0])}
		argSeeder = libp2p.ArgsNetworkMessenger{
			ListenAddress:        libp2p.ListenLocalhostAddrWithIp4AndTcp,
			P2pConfig:            p2pConfigSeeder,
			PreferredPeersHolder: &p2pmocks.PeersHolderStub{},
			NodeOperationMode:    p2p.NormalOperation,
			Marshalizer:          &testscommon.MarshalizerMock{},
			SyncTimer:            &testscommon.SyncTimerStub{},
		}
		seeders[i], _ = libp2p.NewMockMessenger(argSeeder, netw)
		_ = netw.LinkAll()
		_ = seeders[i].Bootstrap()
		seedersAddresses[i] = integrationTests.GetConnectableAddress(seeders[i])
	}

	return seeders, seedersAddresses
}

func disconnectSeedersFromPeers(seeders []p2p.Messenger, peers []p2p.Messenger, netw mocknet.Mocknet) {
	for _, p := range peers {
		for _, s := range seeders {
			disconnectPeers(p, s, netw)
		}
	}
}

func disconnectPeers(peer1 p2p.Messenger, peer2 p2p.Messenger, netw mocknet.Mocknet) {
	_ = netw.UnlinkPeers(getPeerId(peer1), getPeerId(peer2))
	_ = netw.DisconnectPeers(getPeerId(peer1), getPeerId(peer2))
	_ = netw.DisconnectPeers(getPeerId(peer2), getPeerId(peer1))
}
