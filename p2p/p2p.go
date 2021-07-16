package p2p

import (
	"context"
	"encoding/hex"
	"io"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
)

const displayLastPidChars = 12

const (
	// ListsSharder is the variant that uses lists
	ListsSharder = "ListsSharder"
	// OneListSharder is the variant that is shard agnostic and uses one list
	OneListSharder = "OneListSharder"
	// NilListSharder is the variant that will not do connection trimming
	NilListSharder = "NilListSharder"
)

// NodeOperation defines the p2p node operation
type NodeOperation string

// NormalOperation defines the normal mode operation: either seeder, observer or validator
const NormalOperation NodeOperation = "normal operation"

// FullArchiveMode defines the node operation as a full archive mode
const FullArchiveMode NodeOperation = "full archive mode"

// MessageProcessor is the interface used to describe what a receive message processor should do
// All implementations that will be called from Messenger implementation will need to satisfy this interface
// If the function returns a non nil value, the received message will not be propagated to its connected peers
type MessageProcessor interface {
	ProcessReceivedMessage(message MessageP2P, fromConnectedPeer core.PeerID) error
	IsInterfaceNil() bool
}

// SendableData represents the struct used in data throttler implementation
type SendableData struct {
	Buff  []byte
	Topic string
}

// PeerDiscoverer defines the behaviour of a peer discovery mechanism
type PeerDiscoverer interface {
	Bootstrap() error
	Name() string
	IsInterfaceNil() bool
}

// Reconnecter defines the behaviour of a network reconnection mechanism
type Reconnecter interface {
	ReconnectToNetwork(ctx context.Context)
	IsInterfaceNil() bool
}

// Messenger is the main struct used for communication with other peers
type Messenger interface {
	io.Closer

	// ID is the Messenger's unique peer identifier across the network (a
	// string). It is derived from the public key of the P2P credentials.
	ID() core.PeerID

	// Peers is the list of IDs of peers known to the Messenger.
	Peers() []core.PeerID

	// Addresses is the list of addresses that the Messenger is currently bound
	// to and listening to.
	Addresses() []string

	// ConnectToPeer explicitly connect to a specific peer with a known address (note that the
	// address contains the peer ID). This function is usually not called
	// manually, because any underlying implementation of the Messenger interface
	// should be keeping connections to peers open.
	ConnectToPeer(address string) error

	// IsConnected returns true if the Messenger are connected to a specific peer.
	IsConnected(peerID core.PeerID) bool

	// ConnectedPeers returns the list of IDs of the peers the Messenger is
	// currently connected to.
	ConnectedPeers() []core.PeerID

	// ConnectedAddresses returns the list of addresses of the peers to which the
	// Messenger is currently connected.
	ConnectedAddresses() []string

	// PeerAddresses returns the known addresses for the provided peer ID
	PeerAddresses(pid core.PeerID) []string

	// ConnectedPeersOnTopic returns the IDs of the peers to which the Messenger
	// is currently connected, but filtered by a topic they are registered to.
	ConnectedPeersOnTopic(topic string) []core.PeerID

	// ConnectedFullHistoryPeersOnTopic returns the IDs of the full history peers to which the Messenger
	// is currently connected, but filtered by a topic they are registered to.
	ConnectedFullHistoryPeersOnTopic(topic string) []core.PeerID

	// Bootstrap runs the initialization phase which includes peer discovery,
	// setting up initial connections and self-announcement in the network.
	Bootstrap() error

	// CreateTopic defines a new topic for sending messages, and optionally
	// creates a channel in the LoadBalancer for this topic (otherwise, the topic
	// will use a default channel).
	CreateTopic(name string, createChannelForTopic bool) error

	// HasTopic returns true if the Messenger has declared interest in a topic
	// and it is listening to messages referencing it.
	HasTopic(name string) bool

	// RegisterMessageProcessor adds the provided MessageProcessor to the list
	// of handlers that are invoked whenever a message is received on the
	// specified topic.
	RegisterMessageProcessor(topic string, identifier string, handler MessageProcessor) error

	// UnregisterAllMessageProcessors removes all the MessageProcessor set by the
	// Messenger from the list of registered handlers for the messages on the
	// given topic.
	UnregisterAllMessageProcessors() error

	// UnregisterMessageProcessor removes the MessageProcessor set by the
	// Messenger from the list of registered handlers for the messages on the
	// given topic.
	UnregisterMessageProcessor(topic string, identifier string) error

	// BroadcastOnChannelBlocking asynchronously waits until it can send a
	// message on the channel, but once it is able to, it synchronously sends the
	// message, blocking until sending is completed.
	BroadcastOnChannelBlocking(channel string, topic string, buff []byte) error

	// BroadcastOnChannel asynchronously sends a message on a given topic
	// through a specified channel.
	BroadcastOnChannel(channel string, topic string, buff []byte)

	// Broadcast is a convenience function that calls BroadcastOnChannelBlocking,
	// but implicitly sets the channel to be identical to the specified topic.
	Broadcast(topic string, buff []byte)

	// SendToConnectedPeer asynchronously sends a message to a peer directly,
	// bypassing pubsub and topics. It opens a new connection with the given
	// peer, but reuses a connection and a stream if possible.
	SendToConnectedPeer(topic string, buff []byte, peerID core.PeerID) error

	IsConnectedToTheNetwork() bool
	ThresholdMinConnectedPeers() int
	SetThresholdMinConnectedPeers(minConnectedPeers int) error
	SetPeerShardResolver(peerShardResolver PeerShardResolver) error
	SetPeerDenialEvaluator(handler PeerDenialEvaluator) error
	GetConnectedPeersInfo() *ConnectedPeersInfo
	UnjoinAllTopics() error
	Port() int

	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// MessageP2P defines what a p2p message can do (should return)
type MessageP2P interface {
	From() []byte
	Data() []byte
	Payload() []byte
	SeqNo() []byte
	Topic() string
	Signature() []byte
	Key() []byte
	Peer() core.PeerID
	Timestamp() int64
	IsInterfaceNil() bool
}

// ChannelLoadBalancer defines what a load balancer that uses chans should do
type ChannelLoadBalancer interface {
	AddChannel(channel string) error
	RemoveChannel(channel string) error
	GetChannelOrDefault(channel string) chan *SendableData
	CollectOneElementFromChannels() *SendableData
	Close() error
	IsInterfaceNil() bool
}

// DirectSender defines a component that can send direct messages to connected peers
type DirectSender interface {
	NextSeqno() []byte
	Send(topic string, buff []byte, peer core.PeerID) error
	IsInterfaceNil() bool
}

// PeerDiscoveryFactory defines the factory for peer discoverer implementation
type PeerDiscoveryFactory interface {
	CreatePeerDiscoverer() (PeerDiscoverer, error)
	IsInterfaceNil() bool
}

// MessageOriginatorPid will output the message peer id in a pretty format
// If it can, it will display the last displayLastPidChars (12) characters from the pid
func MessageOriginatorPid(msg MessageP2P) string {
	return PeerIdToShortString(msg.Peer())
}

// PeerIdToShortString trims the first displayLastPidChars characters of the provided peer ID after
// converting the peer ID to string using the Pretty functionality
func PeerIdToShortString(pid core.PeerID) string {
	prettyPid := pid.Pretty()
	lenPrettyPid := len(prettyPid)

	if lenPrettyPid > displayLastPidChars {
		return "..." + prettyPid[lenPrettyPid-displayLastPidChars:]
	}

	return prettyPid
}

// MessageOriginatorSeq will output the sequence number as hex
func MessageOriginatorSeq(msg MessageP2P) string {
	return hex.EncodeToString(msg.SeqNo())
}

// PeerShardResolver is able to resolve the link between the provided PeerID and the shardID
type PeerShardResolver interface {
	GetPeerInfo(pid core.PeerID) core.P2PPeerInfo
	IsInterfaceNil() bool
}

// ConnectedPeersInfo represents the DTO structure used to output the metrics for connected peers
type ConnectedPeersInfo struct {
	SelfShardID              uint32
	UnknownPeers             []string
	Seeders                  []string
	IntraShardValidators     map[uint32][]string
	IntraShardObservers      map[uint32][]string
	CrossShardValidators     map[uint32][]string
	CrossShardObservers      map[uint32][]string
	FullHistoryObservers     map[uint32][]string
	NumValidatorsOnShard     map[uint32]int
	NumObserversOnShard      map[uint32]int
	NumPreferredPeersOnShard map[uint32]int
	NumIntraShardValidators  int
	NumIntraShardObservers   int
	NumCrossShardValidators  int
	NumCrossShardObservers   int
	NumFullHistoryObservers  int
}

// NetworkShardingCollector defines the updating methods used by the network sharding component
// The interface assures that the collected data will be used by the p2p network sharding components
type NetworkShardingCollector interface {
	UpdatePeerIDInfo(pid core.PeerID, pk []byte, shardID uint32)
	IsInterfaceNil() bool
}

// SignerVerifier is used in higher level protocol authentication of 2 peers after the basic p2p connection has been made
type SignerVerifier interface {
	Sign(message []byte) ([]byte, error)
	Verify(message []byte, sig []byte, pk []byte) error
	PublicKey() []byte
	IsInterfaceNil() bool
}

// Marshalizer defines the 2 basic operations: serialize (marshal) and deserialize (unmarshal)
type Marshalizer interface {
	Marshal(obj interface{}) ([]byte, error)
	Unmarshal(obj interface{}, buff []byte) error
	IsInterfaceNil() bool
}

// PreferredPeersHolderHandler defines the behavior of a component able to handle preferred peers operations
type PreferredPeersHolderHandler interface {
	Put(publicKey []byte, peerID core.PeerID, shardID uint32)
	Get() map[uint32][]core.PeerID
	Contains(peerID core.PeerID) bool
	Remove(peerID core.PeerID)
	Clear()
	IsInterfaceNil() bool
}

// PeerCounts represents the DTO structure used to output the count metrics for connected peers
type PeerCounts struct {
	UnknownPeers    int
	IntraShardPeers int
	CrossShardPeers int
}

// Sharder defines the eviction computing process of unwanted peers
type Sharder interface {
	SetSeeders(addresses []string)
	IsSeeder(pid core.PeerID) bool
	SetPeerShardResolver(psp PeerShardResolver) error
	IsInterfaceNil() bool
}

// PeerDenialEvaluator defines the behavior of a component that is able to decide if a peer ID is black listed or not
//TODO merge this interface with the PeerShardResolver => P2PProtocolHandler ?
//TODO move antiflooding inside network messenger
type PeerDenialEvaluator interface {
	IsDenied(pid core.PeerID) bool
	UpsertPeerID(pid core.PeerID, duration time.Duration) error
	IsInterfaceNil() bool
}

// ConnectionMonitorWrapper uses a connection monitor but checks if the peer is blacklisted or not
//TODO this should be removed after merging of the PeerShardResolver and BlacklistHandler
type ConnectionMonitorWrapper interface {
	CheckConnectionsBlocking()
	SetPeerDenialEvaluator(handler PeerDenialEvaluator) error
	PeerDenialEvaluator() PeerDenialEvaluator
	IsInterfaceNil() bool
}

// Cacher defines the interface for a cacher used in p2p to better prevent the reprocessing of an old message
type Cacher interface {
	HasOrAdd(key []byte, value interface{}, sizeInBytes int) (has, added bool)
	IsInterfaceNil() bool
}

// Debugger represent a p2p debugger able to print p2p statistics (messages received/sent per topic)
type Debugger interface {
	AddIncomingMessage(topic string, size uint64, isRejected bool)
	AddOutgoingMessage(topic string, size uint64, isRejected bool)
	Close() error
	IsInterfaceNil() bool
}

// SyncTimer represent an entity able to tell the current time
type SyncTimer interface {
	CurrentTime() time.Time
	IsInterfaceNil() bool
}
