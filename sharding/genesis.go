package sharding

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ElrondNetwork/elrond-go-sandbox/core"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/logger"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
)

var log = logger.DefaultLogger()

// InitialNode holds data from json and decoded data from genesis process
type InitialNode struct {
	PubKey        string `json:"pubkey"`
	Balance       string `json:"balance"`
	assignedShard uint32
	pubKey        []byte
	balance       *big.Int
}

// Genesis hold data for decoded data from json file
type Genesis struct {
	StartTime          int64  `json:"startTime"`
	RoundDuration      uint64 `json:"roundDuration"`
	ConsensusGroupSize uint32 `json:"consensusGroupSize"`
	MinNodesPerShard   uint32 `json:"minNodesPerShard"`

	MetaChainActive             uint32 `json:"metaChainActive"`
	MetaChainConsensusGroupSize uint32 `json:"metaChainConsensusGroupSize"`
	MetaChainMinNodes           uint32 `json:"metaChainMinNodes"`

	InitialNodes []*InitialNode `json:"initialNodes"`

	nrOfShards         uint32
	nrOfNodes          uint32
	nrOfMetaChainNodes uint32
	allNodesPubKeys    map[uint32][]string
}

// NewGenesisConfig creates a new decoded genesis structure from json config file
func NewGenesisConfig(genesisFilePath string) (*Genesis, error) {
	genesis := &Genesis{}

	err := core.LoadJsonFile(genesis, genesisFilePath, log)
	if err != nil {
		return nil, err
	}

	err = genesis.processConfig()
	if err != nil {
		return nil, err
	}

	if genesis.MetaChainActive == 1 {
		genesis.processMetaChainAssigment()
	}

	genesis.processShardAssignment()
	genesis.createInitialNodesPubKeys()

	return genesis, nil
}

func (g *Genesis) processConfig() error {
	var err error
	var ok bool

	g.nrOfNodes = 0
	g.nrOfMetaChainNodes = 0
	for i := 0; i < len(g.InitialNodes); i++ {
		g.InitialNodes[i].pubKey, err = hex.DecodeString(g.InitialNodes[i].PubKey)

		// decoder treats empty string as correct, it is not allowed to have empty string as public key
		if g.InitialNodes[i].PubKey == "" || err != nil {
			g.InitialNodes[i].pubKey = nil
			return ErrCouldNotParsePubKey
		}

		g.InitialNodes[i].balance, ok = new(big.Int).SetString(g.InitialNodes[i].Balance, 10)
		if !ok {
			log.Warn(fmt.Sprintf("error decoding balance %s for public key %s - setting to 0",
				g.InitialNodes[i].Balance, g.InitialNodes[i].PubKey))
			g.InitialNodes[i].balance = big.NewInt(0)
		}

		g.nrOfNodes++
	}

	if g.ConsensusGroupSize < 1 {
		return ErrNegativeOrZeroConsensusGroupSize
	}
	if g.nrOfNodes < g.ConsensusGroupSize {
		return ErrNotEnoughValidators
	}
	if g.MinNodesPerShard < g.ConsensusGroupSize {
		return ErrMinNodesPerShardSmallerThanConsensusSize
	}
	if g.nrOfNodes < g.MinNodesPerShard {
		return ErrNodesSizeSmallerThanMinNoOfNodes
	}

	if g.MetaChainActive == 1 {
		if g.MetaChainConsensusGroupSize < 1 {
			return ErrNegativeOrZeroConsensusGroupSize
		}
		if g.MetaChainMinNodes < g.MetaChainConsensusGroupSize {
			return ErrMinNodesPerShardSmallerThanConsensusSize
		}

		totalMinConsenus := g.MetaChainConsensusGroupSize + g.ConsensusGroupSize
		totalMinNodes := g.MetaChainMinNodes + g.MinNodesPerShard
		if g.nrOfNodes < totalMinConsenus {
			return ErrNotEnoughValidators
		}
		if g.nrOfNodes < totalMinNodes {
			return ErrNodesSizeSmallerThanMinNoOfNodes
		}
	}

	return nil
}

func (g *Genesis) processMetaChainAssigment() {
	g.nrOfMetaChainNodes = 0
	for id := uint32(0); id < g.MetaChainMinNodes; id++ {
		if g.InitialNodes[id].pubKey != nil {
			g.InitialNodes[id].assignedShard = MetachainShardId
			g.nrOfMetaChainNodes++
		}
	}
}

func (g *Genesis) processShardAssignment() {
	// initial implementation - as there is no other info than public key, we allocate first nodes in FIFO order to shards
	g.nrOfShards = (g.nrOfNodes - g.nrOfMetaChainNodes) / g.MinNodesPerShard

	currentShard := uint32(0)
	countSetNodes := g.nrOfMetaChainNodes
	for ; currentShard < g.nrOfShards; currentShard++ {
		for id := countSetNodes; id < g.nrOfMetaChainNodes+(currentShard+1)*g.MinNodesPerShard; id++ {
			// consider only nodes with valid public key
			if g.InitialNodes[id].pubKey != nil {
				g.InitialNodes[id].assignedShard = currentShard
				countSetNodes++
			}
		}
	}

	// allocate the rest
	currentShard = 0
	for i := countSetNodes; i < g.nrOfNodes; i++ {
		g.InitialNodes[i].assignedShard = currentShard
		currentShard = (currentShard + 1) % g.nrOfShards
	}
}

func (g *Genesis) createInitialNodesPubKeys() {
	nrOfShardAndMeta := g.nrOfShards
	if g.MetaChainActive == 1 {
		nrOfShardAndMeta += 1
	}

	g.allNodesPubKeys = make(map[uint32][]string, nrOfShardAndMeta)
	for _, in := range g.InitialNodes {
		if in.pubKey != nil {
			g.allNodesPubKeys[in.assignedShard] = append(g.allNodesPubKeys[in.assignedShard], string(in.pubKey))
		}
	}
}

// InitialNodesPubKeys - gets initial public keys
func (g *Genesis) InitialNodesPubKeys() map[uint32][]string {
	return g.allNodesPubKeys
}

// InitialNodesPubKeysForShard - gets initial public keys
func (g *Genesis) InitialNodesPubKeysForShard(shardId uint32) ([]string, error) {
	if g.allNodesPubKeys[shardId] == nil {
		return nil, ErrShardIdOutOfRange
	}

	if len(g.allNodesPubKeys[shardId]) == 0 {
		return nil, ErrNoPubKeys
	}

	return g.allNodesPubKeys[shardId], nil
}

// InitialNodesBalances - gets the initial balances of the nodes
func (g *Genesis) InitialNodesBalances(shardCoordinator Coordinator, adrConv state.AddressConverter) (map[string]*big.Int, error) {
	if shardCoordinator == nil {
		return nil, ErrNilShardCoordinator
	}
	if adrConv == nil {
		return nil, ErrNilAddressConverter
	}

	var balances = make(map[string]*big.Int)
	for _, in := range g.InitialNodes {
		address, err := adrConv.CreateAddressFromPublicKeyBytes(in.pubKey)
		if err != nil {
			return nil, err
		}
		addressShard := shardCoordinator.ComputeId(address)
		if addressShard == shardCoordinator.SelfId() {
			balances[string(in.pubKey)] = in.balance
		}
	}

	return balances, nil
}

// NumberOfShards returns the calculated number of shards
func (g *Genesis) NumberOfShards() uint32 {
	return g.nrOfShards
}

// IsMetaChainActive returns if MetaChain is active
func (g *Genesis) IsMetaChainActive() bool {
	return g.MetaChainActive == 1
}

// GetShardIDForPubKey returns the allocated shard ID from publick key
func (g *Genesis) GetShardIDForPubKey(pubKey []byte) (uint32, error) {
	for _, in := range g.InitialNodes {
		if in.pubKey != nil && bytes.Equal(pubKey, in.pubKey) {
			return in.assignedShard, nil
		}
	}
	return 0, ErrNoValidPublicKey
}
