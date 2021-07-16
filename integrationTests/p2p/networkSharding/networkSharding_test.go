package networkSharding

import (
	"fmt"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/integrationTests"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/stretchr/testify/assert"
)

var p2pBootstrapStepDelay = 2 * time.Second

func createDefaultConfig() config.P2PConfig {
	return config.P2PConfig{
		Node: config.NodeConfig{
			Port: "0",
		},
		KadDhtPeerDiscovery: config.KadDhtPeerDiscoveryConfig{
			Enabled:                          true,
			Type:                             "optimized",
			RefreshIntervalInSec:             1,
			RoutingTableRefreshIntervalInSec: 1,
			ProtocolID:                       "/erd/kad/1.0.0",
			InitialPeerList:                  nil,
			BucketSize:                       100,
		},
	}
}

func TestConnectionsInNetworkShardingWithShardingWithLists(t *testing.T) {
	p2pConfig := createDefaultConfig()
	p2pConfig.Sharding = config.ShardingConfig{
		TargetPeerCount:         12,
		MaxIntraShardValidators: 6,
		MaxCrossShardValidators: 1,
		MaxIntraShardObservers:  1,
		MaxCrossShardObservers:  1,
		MaxSeeders:              1,
		Type:                    p2p.ListsSharder,
		AdditionalConnections: config.AdditionalConnectionsConfig{
			MaxFullHistoryObservers: 1,
		},
	}

	testConnectionsInNetworkSharding(t, p2pConfig)
}

func testConnectionsInNetworkSharding(t *testing.T, p2pConfig config.P2PConfig) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	nodesPerShard := 8
	numMetaNodes := 8
	numObserversOnShard := 2
	numShards := 2
	consensusGroupSize := 2

	advertiser := integrationTests.CreateMessengerWithKadDht("")
	_ = advertiser.Bootstrap()
	seedAddress := integrationTests.GetConnectableAddress(advertiser)

	p2pConfig.KadDhtPeerDiscovery.InitialPeerList = []string{seedAddress}

	// create map of shard - testNodeProcessors for metachain and shard chain
	nodesMap := integrationTests.CreateNodesWithTestP2PNodes(
		nodesPerShard,
		numMetaNodes,
		numShards,
		consensusGroupSize,
		numMetaNodes,
		numObserversOnShard,
		p2pConfig,
	)

	defer func() {
		stopNodes(advertiser, nodesMap)
	}()

	createTestInterceptorForEachNode(nodesMap)

	time.Sleep(time.Second * 2)

	startNodes(nodesMap)

	fmt.Println("Delaying for node bootstrap and topic announcement...")
	time.Sleep(p2pBootstrapStepDelay)

	for i := 0; i < 15; i++ {
		fmt.Println("\n" + integrationTests.MakeDisplayTableForP2PNodes(nodesMap))

		time.Sleep(time.Second)
	}

	sendMessageOnGlobalTopic(nodesMap)
	sendMessagesOnIntraShardTopic(nodesMap)
	sendMessagesOnCrossShardTopic(nodesMap)

	for i := 0; i < 10; i++ {
		fmt.Println("\n" + integrationTests.MakeDisplayTableForP2PNodes(nodesMap))

		time.Sleep(time.Second)
	}

	testCounters(t, nodesMap, 1, 1, numShards*2)
	testUnknownSeederPeers(t, nodesMap)
}

func stopNodes(advertiser p2p.Messenger, nodesMap map[uint32][]*integrationTests.TestP2PNode) {
	_ = advertiser.Close()
	for _, nodes := range nodesMap {
		for _, n := range nodes {
			_ = n.Messenger.Close()
		}
	}
}

func startNodes(nodesMap map[uint32][]*integrationTests.TestP2PNode) {
	for _, nodes := range nodesMap {
		for _, n := range nodes {
			_ = n.Messenger.Bootstrap()
		}
	}
}

func createTestInterceptorForEachNode(nodesMap map[uint32][]*integrationTests.TestP2PNode) {
	for _, nodes := range nodesMap {
		for _, n := range nodes {
			n.CreateTestInterceptors()
		}
	}
}

func sendMessageOnGlobalTopic(nodesMap map[uint32][]*integrationTests.TestP2PNode) {
	fmt.Println("sending a message on global topic")
	nodesMap[0][0].Messenger.Broadcast(integrationTests.GlobalTopic, []byte("global message"))
	time.Sleep(time.Second)
}

func sendMessagesOnIntraShardTopic(nodesMap map[uint32][]*integrationTests.TestP2PNode) {
	fmt.Println("sending a message on intra shard topic")
	for _, nodes := range nodesMap {
		n := nodes[0]

		identifier := integrationTests.ShardTopic +
			n.ShardCoordinator.CommunicationIdentifier(n.ShardCoordinator.SelfId())
		nodes[0].Messenger.Broadcast(identifier, []byte("intra shard message"))
	}
	time.Sleep(time.Second)
}

func sendMessagesOnCrossShardTopic(nodesMap map[uint32][]*integrationTests.TestP2PNode) {
	fmt.Println("sending messages on cross shard topics")

	for shardIdSrc, nodes := range nodesMap {
		n := nodes[0]

		for shardIdDest := range nodesMap {
			if shardIdDest == shardIdSrc {
				continue
			}

			identifier := integrationTests.ShardTopic +
				n.ShardCoordinator.CommunicationIdentifier(shardIdDest)
			nodes[0].Messenger.Broadcast(identifier, []byte("cross shard message"))
		}
	}
	time.Sleep(time.Second)
}

func testCounters(
	t *testing.T,
	nodesMap map[uint32][]*integrationTests.TestP2PNode,
	globalTopicMessagesCount int,
	intraTopicMessagesCount int,
	crossTopicMessagesCount int,
) {

	for _, nodes := range nodesMap {
		for _, n := range nodes {
			assert.Equal(t, globalTopicMessagesCount, n.CountGlobalMessages())
			assert.Equal(t, intraTopicMessagesCount, n.CountIntraShardMessages())
			assert.Equal(t, crossTopicMessagesCount, n.CountCrossShardMessages())
		}
	}
}

func testUnknownSeederPeers(
	t *testing.T,
	nodesMap map[uint32][]*integrationTests.TestP2PNode,
) {

	for _, nodes := range nodesMap {
		for _, n := range nodes {
			assert.Equal(t, 0, len(n.Messenger.GetConnectedPeersInfo().UnknownPeers))
			assert.Equal(t, 1, len(n.Messenger.GetConnectedPeersInfo().Seeders))
		}
	}
}
