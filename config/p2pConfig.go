package config

// P2PConfig will hold all the P2P settings
type P2PConfig struct {
	Node                NodeConfig
	KadDhtPeerDiscovery KadDhtPeerDiscoveryConfig
	Sharding            ShardingConfig
}

// NodeConfig will hold basic p2p settings
type NodeConfig struct {
	Port                       string
	Seed                       string
	MaximumExpectedPeerCount   uint64
	ThresholdMinConnectedPeers uint32
}

// KadDhtPeerDiscoveryConfig will hold the kad-dht discovery config settings
type KadDhtPeerDiscoveryConfig struct {
	Enabled                          bool
	Type                             string
	RefreshIntervalInSec             uint32
	ProtocolID                       string
	InitialPeerList                  []string
	BucketSize                       uint32
	RoutingTableRefreshIntervalInSec uint32
}

// ShardingConfig will hold the network sharding config settings
type ShardingConfig struct {
	TargetPeerCount         uint32
	MaxIntraShardValidators uint32
	MaxCrossShardValidators uint32
	MaxIntraShardObservers  uint32
	MaxCrossShardObservers  uint32
	MaxSeeders              uint32
	Type                    string
	AdditionalConnections   AdditionalConnectionsConfig
}

// AdditionalConnectionsConfig will hold the additional connections that will be open when certain conditions are met
// All these values should be added to the maximum target peer count value
type AdditionalConnectionsConfig struct {
	MaxFullHistoryObservers uint32
}
