package networksharding

import (
	"fmt"
	"math/big"
	"math/bits"
	"sort"
	"strings"
	"sync"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/libp2p/networksharding/sorting"
	"github.com/libp2p/go-libp2p-core/peer"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

var _ p2p.Sharder = (*listsSharder)(nil)

const minAllowedConnectedPeersListSharder = 5
const minAllowedValidators = 1
const minAllowedObservers = 1
const minUnknownPeers = 1

const intraShardValidators = 0
const intraShardObservers = 10
const crossShardValidators = 20
const crossShardObservers = 30
const seeders = 40
const unknown = 50
const fullHistoryObservers = 60

var log = logger.GetOrCreate("p2p/libp2p/networksharding")

var leadingZerosCount = []int{
	8, 7, 6, 6, 5, 5, 5, 5,
	4, 4, 4, 4, 4, 4, 4, 4,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
}

// this will fail if we have less than 256 values in the slice
var _ = leadingZerosCount[255]

// ArgListsSharder represents the argument structure used in the initialization of a listsSharder implementation
type ArgListsSharder struct {
	PeerResolver         p2p.PeerShardResolver
	SelfPeerId           peer.ID
	P2pConfig            config.P2PConfig
	PreferredPeersHolder p2p.PreferredPeersHolderHandler
	NodeOperationMode    p2p.NodeOperation
}

// listsSharder is the struct able to compute an eviction list of connected peers id according to the
// provided parameters. It basically splits all connected peers into 3 lists: intra shard peers, cross shard peers
// and unknown peers by the following rule: both intra shard and cross shard lists are upper bounded to provided
// maximum levels, unknown list is able to fill the gap until maximum peer count value is fulfilled.
type listsSharder struct {
	mutResolver             sync.RWMutex
	peerShardResolver       p2p.PeerShardResolver
	selfPeerId              peer.ID
	maxPeerCount            int
	maxIntraShardValidators int
	maxCrossShardValidators int
	maxIntraShardObservers  int
	maxCrossShardObservers  int
	maxSeeders              int
	maxFullHistoryObservers int
	maxUnknown              int
	mutSeeders              sync.RWMutex
	seeders                 []string
	computeDistance         func(src peer.ID, dest peer.ID) *big.Int
	preferredPeersHolder    p2p.PreferredPeersHolderHandler
}

type peersConnections struct {
	maxPeerCount         int
	intraShardValidators int
	crossShardValidators int
	intraShardObservers  int
	crossShardObservers  int
	seeders              int
	fullHistoryObservers int
	unknown              int
}

// NewListsSharder creates a new kad list based kad sharder instance
func NewListsSharder(arg ArgListsSharder) (*listsSharder, error) {
	if check.IfNil(arg.PeerResolver) {
		return nil, p2p.ErrNilPeerShardResolver
	}
	if arg.P2pConfig.Sharding.TargetPeerCount < minAllowedConnectedPeersListSharder {
		return nil, fmt.Errorf("%w, maxPeerCount should be at least %d", p2p.ErrInvalidValue, minAllowedConnectedPeersListSharder)
	}
	if arg.P2pConfig.Sharding.MaxIntraShardValidators < minAllowedValidators {
		return nil, fmt.Errorf("%w, maxIntraShardValidators should be at least %d", p2p.ErrInvalidValue, minAllowedValidators)
	}
	if arg.P2pConfig.Sharding.MaxCrossShardValidators < minAllowedValidators {
		return nil, fmt.Errorf("%w, maxCrossShardValidators should be at least %d", p2p.ErrInvalidValue, minAllowedValidators)
	}
	if arg.P2pConfig.Sharding.MaxIntraShardObservers < minAllowedObservers {
		return nil, fmt.Errorf("%w, maxIntraShardObservers should be at least %d", p2p.ErrInvalidValue, minAllowedObservers)
	}
	if arg.P2pConfig.Sharding.MaxCrossShardObservers < minAllowedObservers {
		return nil, fmt.Errorf("%w, maxCrossShardObservers should be at least %d", p2p.ErrInvalidValue, minAllowedObservers)
	}
	if check.IfNil(arg.PreferredPeersHolder) {
		return nil, fmt.Errorf("%w while creating a new listsShared", p2p.ErrNilPreferredPeersHolder)
	}
	peersConn, err := processNumConnections(arg)
	if err != nil {
		return nil, err
	}

	ls := &listsSharder{
		peerShardResolver:       arg.PeerResolver,
		selfPeerId:              arg.SelfPeerId,
		maxPeerCount:            peersConn.maxPeerCount,
		computeDistance:         computeDistanceByCountingBits,
		maxIntraShardValidators: peersConn.intraShardValidators,
		maxCrossShardValidators: peersConn.crossShardValidators,
		maxIntraShardObservers:  peersConn.intraShardObservers,
		maxCrossShardObservers:  peersConn.crossShardObservers,
		maxSeeders:              peersConn.seeders,
		maxFullHistoryObservers: peersConn.fullHistoryObservers,
		maxUnknown:              peersConn.unknown,
		preferredPeersHolder:    arg.PreferredPeersHolder,
	}

	return ls, nil
}

func processNumConnections(arg ArgListsSharder) (peersConnections, error) {
	peersConn := peersConnections{
		maxPeerCount:         int(arg.P2pConfig.Sharding.TargetPeerCount),
		intraShardValidators: int(arg.P2pConfig.Sharding.MaxIntraShardValidators),
		crossShardValidators: int(arg.P2pConfig.Sharding.MaxCrossShardValidators),
		intraShardObservers:  int(arg.P2pConfig.Sharding.MaxIntraShardObservers),
		crossShardObservers:  int(arg.P2pConfig.Sharding.MaxCrossShardObservers),
		seeders:              int(arg.P2pConfig.Sharding.MaxSeeders),
		fullHistoryObservers: 0,
	}
	if arg.NodeOperationMode == p2p.FullArchiveMode {
		peersConn.fullHistoryObservers = int(arg.P2pConfig.Sharding.AdditionalConnections.MaxFullHistoryObservers)
		peersConn.maxPeerCount += peersConn.fullHistoryObservers
	}

	if peersConn.crossShardObservers+peersConn.intraShardObservers+peersConn.fullHistoryObservers == 0 {
		log.Warn("No connections to observers are possible. This is NOT a recommended setting!")
	}

	providedPeers := peersConn.intraShardValidators + peersConn.crossShardValidators +
		peersConn.intraShardObservers + peersConn.crossShardObservers +
		peersConn.seeders + peersConn.fullHistoryObservers
	if providedPeers+minUnknownPeers > peersConn.maxPeerCount {
		return peersConnections{}, fmt.Errorf("%w, maxValidators + maxObservers + seeders + full archive nodes should be less than %d", p2p.ErrInvalidValue, peersConn.maxPeerCount)
	}

	peersConn.unknown = peersConn.maxPeerCount - providedPeers

	return peersConn, nil
}

// ComputeEvictionList returns the eviction list
func (ls *listsSharder) ComputeEvictionList(pidList []peer.ID) []peer.ID {
	peerDistances := ls.splitPeerIds(pidList)

	existingNumIntraShardValidators := len(peerDistances[intraShardValidators])
	existingNumIntraShardObservers := len(peerDistances[intraShardObservers])
	existingNumCrossShardValidators := len(peerDistances[crossShardValidators])
	existingNumCrossShardObservers := len(peerDistances[crossShardObservers])
	existingNumSeeders := len(peerDistances[seeders])
	existingNumFullHistoryObservers := len(peerDistances[fullHistoryObservers])
	existingNumUnknown := len(peerDistances[unknown])

	var numIntraShardValidators, numCrossShardValidators int
	var numIntraShardObservers, numCrossShardObservers int
	var numFullHistoryObservers int
	var numSeeders, numUnknown, remaining int

	numIntraShardValidators, remaining = computeUsedAndSpare(existingNumIntraShardValidators, ls.maxIntraShardValidators)
	numCrossShardValidators, remaining = computeUsedAndSpare(existingNumCrossShardValidators, ls.maxCrossShardValidators+remaining)
	numIntraShardObservers, remaining = computeUsedAndSpare(existingNumIntraShardObservers, ls.maxIntraShardObservers+remaining)
	numCrossShardObservers, remaining = computeUsedAndSpare(existingNumCrossShardObservers, ls.maxCrossShardObservers+remaining)
	numSeeders, _ = computeUsedAndSpare(existingNumSeeders, ls.maxSeeders) //we are not mixing remaining value. We are strict with the number of seeders
	numFullHistoryObservers, _ = computeUsedAndSpare(existingNumFullHistoryObservers, ls.maxFullHistoryObservers)
	numUnknown, _ = computeUsedAndSpare(existingNumUnknown, ls.maxUnknown+remaining)

	evictionProposed := evict(peerDistances[intraShardValidators], numIntraShardValidators)
	e := evict(peerDistances[crossShardValidators], numCrossShardValidators)
	evictionProposed = append(evictionProposed, e...)
	e = evict(peerDistances[intraShardObservers], numIntraShardObservers)
	evictionProposed = append(evictionProposed, e...)
	e = evict(peerDistances[crossShardObservers], numCrossShardObservers)
	evictionProposed = append(evictionProposed, e...)
	e = evict(peerDistances[seeders], numSeeders)
	evictionProposed = append(evictionProposed, e...)
	e = evict(peerDistances[fullHistoryObservers], numFullHistoryObservers)
	evictionProposed = append(evictionProposed, e...)
	e = evict(peerDistances[unknown], numUnknown)
	evictionProposed = append(evictionProposed, e...)

	return evictionProposed
}

// computeUsedAndSpare returns the used and the remaining of the two provided (capacity) values
// if used > maximum, used will equal to maximum and remaining will be 0
func computeUsedAndSpare(existing int, maximum int) (int, int) {
	if existing < maximum {
		return existing, maximum - existing
	}

	return maximum, 0
}

// Has returns true if provided pid is among the provided list
func (ls *listsSharder) Has(pid peer.ID, list []peer.ID) bool {
	return has(pid, list)
}

func has(pid peer.ID, list []peer.ID) bool {
	for _, p := range list {
		if p == pid {
			return true
		}
	}

	return false
}

func (ls *listsSharder) splitPeerIds(peers []peer.ID) map[int]sorting.PeerDistances {
	peerDistances := map[int]sorting.PeerDistances{
		intraShardValidators: {},
		intraShardObservers:  {},
		crossShardValidators: {},
		crossShardObservers:  {},
		fullHistoryObservers: {},
		seeders:              {},
		unknown:              {},
	}

	ls.mutResolver.RLock()
	selfPeerInfo := ls.peerShardResolver.GetPeerInfo(core.PeerID(ls.selfPeerId))
	ls.mutResolver.RUnlock()

	for _, p := range peers {
		pd := &sorting.PeerDistance{
			ID:       p,
			Distance: ls.computeDistance(p, ls.selfPeerId),
		}
		pid := core.PeerID(p)
		isSeeder := ls.IsSeeder(pid)
		if isSeeder {
			peerDistances[seeders] = append(peerDistances[seeders], pd)
			continue
		}

		ls.mutResolver.RLock()
		peerInfo := ls.peerShardResolver.GetPeerInfo(pid)
		ls.mutResolver.RUnlock()

		if ls.preferredPeersHolder.Contains(pid) {
			continue
		}

		if peerInfo.PeerType == core.UnknownPeer {
			peerDistances[unknown] = append(peerDistances[unknown], pd)
			continue
		}

		isCrossShard := peerInfo.ShardID != selfPeerInfo.ShardID
		if isCrossShard {
			switch peerInfo.PeerType {
			case core.ValidatorPeer:
				peerDistances[crossShardValidators] = append(peerDistances[crossShardValidators], pd)
			case core.ObserverPeer:
				peerDistances[crossShardObservers] = append(peerDistances[crossShardObservers], pd)
			}

			continue
		}

		switch peerInfo.PeerType {
		case core.ValidatorPeer:
			peerDistances[intraShardValidators] = append(peerDistances[intraShardValidators], pd)
		case core.ObserverPeer:
			shouldAppendToFullHistory := peerInfo.PeerSubType == core.FullHistoryObserver && ls.maxFullHistoryObservers > 0
			if shouldAppendToFullHistory {
				peerDistances[fullHistoryObservers] = append(peerDistances[fullHistoryObservers], pd)
			} else {
				peerDistances[intraShardObservers] = append(peerDistances[intraShardObservers], pd)
			}
		}
	}

	return peerDistances
}

func evict(distances sorting.PeerDistances, numKeep int) []peer.ID {
	if numKeep < 0 {
		numKeep = 0
	}
	if numKeep >= len(distances) {
		return make([]peer.ID, 0)
	}

	sort.Sort(distances)
	evictedPD := distances[numKeep:]
	evictedPids := make([]peer.ID, len(evictedPD))
	for i, pd := range evictedPD {
		evictedPids[i] = pd.ID
	}

	return evictedPids
}

// computes the kademlia distance between 2 provided peers by doing byte xor operations and counting the resulting bits
func computeDistanceByCountingBits(src peer.ID, dest peer.ID) *big.Int {
	srcBuff := kbucket.ConvertPeerID(src)
	destBuff := kbucket.ConvertPeerID(dest)

	cumulatedBits := 0
	for i := 0; i < len(srcBuff); i++ {
		result := srcBuff[i] ^ destBuff[i]
		cumulatedBits += bits.OnesCount8(result)
	}

	return big.NewInt(0).SetInt64(int64(cumulatedBits))
}

// computes the kademlia distance between 2 provided peers by doing byte xor operations and applying log2 on the result
func computeDistanceLog2Based(src peer.ID, dest peer.ID) *big.Int {
	srcBuff := kbucket.ConvertPeerID(src)
	destBuff := kbucket.ConvertPeerID(dest)

	val := 0
	for i := 0; i < len(srcBuff); i++ {
		result := srcBuff[i] ^ destBuff[i]
		val += leadingZerosCount[result]
		if result != 0 {
			break
		}
	}

	val = len(srcBuff)*8 - val

	return big.NewInt(0).SetInt64(int64(val))
}

// IsSeeder returns true if the provided peer is a seeder
func (ls *listsSharder) IsSeeder(pid core.PeerID) bool {
	ls.mutSeeders.RLock()
	defer ls.mutSeeders.RUnlock()

	strPretty := pid.Pretty()
	for _, seeder := range ls.seeders {
		if strings.Contains(seeder, strPretty) {
			return true
		}
	}

	return false
}

// SetSeeders will set the seeders
func (ls *listsSharder) SetSeeders(addresses []string) {
	ls.mutSeeders.Lock()
	ls.seeders = addresses
	ls.mutSeeders.Unlock()
}

// SetPeerShardResolver sets the peer shard resolver for this sharder
func (ls *listsSharder) SetPeerShardResolver(psp p2p.PeerShardResolver) error {
	if check.IfNil(psp) {
		return p2p.ErrNilPeerShardResolver
	}

	ls.mutResolver.Lock()
	ls.peerShardResolver = psp
	ls.mutResolver.Unlock()

	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (ls *listsSharder) IsInterfaceNil() bool {
	return ls == nil
}
