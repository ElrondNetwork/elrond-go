//go:generate protoc -I=proto -I=$GOPATH/src -I=$GOPATH/src/github.com/ElrondNetwork/protobuf/protobuf  --gogoslick_out=. metaBlock.proto
package block

import (
	"math/big"
	"sort"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/headerVersionData"
)

// don't break the interface
var _ = data.HeaderHandler(&MetaBlock{})
var _ = data.MetaHeaderHandler(&MetaBlock{})

// GetShardID returns the metachain shard id
func (m *MetaBlock) GetShardID() uint32 {
	return core.MetachainShardId
}

// SetNonce sets header nonce
func (m *MetaBlock) SetNonce(n uint64) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.Nonce = n

	return nil
}

// SetEpoch sets header epoch
func (m *MetaBlock) SetEpoch(e uint32) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.Epoch = e

	return nil
}

// SetRound sets header round
func (m *MetaBlock) SetRound(r uint64) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.Round = r

	return nil
}

// SetRootHash sets root hash
func (m *MetaBlock) SetRootHash(rHash []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.RootHash = rHash

	return nil
}

// SetValidatorStatsRootHash set's the root hash for the validator statistics trie
func (m *MetaBlock) SetValidatorStatsRootHash(rHash []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.ValidatorStatsRootHash = rHash

	return nil
}

// SetPrevHash sets prev hash
func (m *MetaBlock) SetPrevHash(pvHash []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.PrevHash = pvHash

	return nil
}

// SetPrevRandSeed sets the previous randomness seed
func (m *MetaBlock) SetPrevRandSeed(pvRandSeed []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.PrevRandSeed = pvRandSeed

	return nil
}

// SetRandSeed sets the current random seed
func (m *MetaBlock) SetRandSeed(randSeed []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.RandSeed = randSeed

	return nil
}

// SetPubKeysBitmap sets public key bitmap
func (m *MetaBlock) SetPubKeysBitmap(pkbm []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.PubKeysBitmap = pkbm

	return nil
}

// SetSignature set header signature
func (m *MetaBlock) SetSignature(sg []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.Signature = sg

	return nil
}

// SetLeaderSignature will set the leader's signature
func (m *MetaBlock) SetLeaderSignature(sg []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.LeaderSignature = sg

	return nil
}

// SetChainID sets the chain ID on which this block is valid on
func (m *MetaBlock) SetChainID(chainID []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.ChainID = chainID

	return nil
}

// SetSoftwareVersion sets the software version of the block
func (m *MetaBlock) SetSoftwareVersion(version []byte) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.SoftwareVersion = version

	return nil
}

// SetAccumulatedFees sets the accumulated fees in the header
func (m *MetaBlock) SetAccumulatedFees(value *big.Int) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}
	if value == nil {
		return data.ErrInvalidValue
	}
	if m.AccumulatedFees == nil {
		m.AccumulatedFees = big.NewInt(0)
	}

	m.AccumulatedFees.Set(value)

	return nil
}

// SetAccumulatedFeesInEpoch sets the epoch accumulated fees in the header
func (m *MetaBlock) SetAccumulatedFeesInEpoch(value *big.Int) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}
	if value == nil {
		return data.ErrInvalidValue
	}
	if m.AccumulatedFeesInEpoch == nil {
		m.AccumulatedFeesInEpoch = big.NewInt(0)
	}

	m.AccumulatedFeesInEpoch.Set(value)

	return nil
}

// SetDeveloperFees sets the developer fees in the header
func (m *MetaBlock) SetDeveloperFees(value *big.Int) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}
	if value == nil {
		return data.ErrInvalidValue
	}
	if m.DeveloperFees == nil {
		m.DeveloperFees = big.NewInt(0)
	}

	m.DeveloperFees.Set(value)

	return nil
}

// SetDeveloperFees sets the developer fees in the header
func (m *MetaBlock) SetDevFeesInEpoch(value *big.Int) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}
	if value == nil {
		return data.ErrInvalidValue
	}
	if m.DevFeesInEpoch == nil {
		m.DevFeesInEpoch = big.NewInt(0)
	}

	m.DevFeesInEpoch.Set(value)

	return nil
}

// SetTimeStamp sets header timestamp
func (m *MetaBlock) SetTimeStamp(ts uint64) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.TimeStamp = ts

	return nil
}

// SetTxCount sets the transaction count of the current meta block
func (m *MetaBlock) SetTxCount(txCount uint32) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	m.TxCount = txCount

	return nil
}

// SetShardID sets header shard ID
func (m *MetaBlock) SetShardID(_ uint32) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}

	return nil
}

// GetMiniBlockHeadersWithDst as a map of hashes and sender IDs
func (m *MetaBlock) GetMiniBlockHeadersWithDst(destId uint32) map[string]uint32 {
	if m == nil {
		return nil
	}

	hashDst := make(map[string]uint32)
	for i := 0; i < len(m.ShardInfo); i++ {
		if m.ShardInfo[i].ShardID == destId {
			continue
		}

		for _, val := range m.ShardInfo[i].ShardMiniBlockHeaders {
			if val.ReceiverShardID == destId && val.SenderShardID != destId {
				hashDst[string(val.Hash)] = val.SenderShardID
			}
		}
	}

	for _, val := range m.MiniBlockHeaders {
		isDestinationShard := (val.ReceiverShardID == destId ||
			val.ReceiverShardID == core.AllShardId) &&
			val.SenderShardID != destId
		if isDestinationShard {
			hashDst[string(val.Hash)] = val.SenderShardID
		}
	}

	return hashDst
}

// GetOrderedCrossMiniblocksWithDst gets all cross miniblocks with the given destination shard ID, ordered in a
// chronological way, taking into consideration the round in which they were created/executed in the sender shard
func (m *MetaBlock) GetOrderedCrossMiniblocksWithDst(destId uint32) []*data.MiniBlockInfo {
	if m == nil {
		return nil
	}

	miniBlocks := make([]*data.MiniBlockInfo, 0)

	for i := 0; i < len(m.ShardInfo); i++ {
		if m.ShardInfo[i].ShardID == destId {
			continue
		}

		for _, mb := range m.ShardInfo[i].ShardMiniBlockHeaders {
			if mb.ReceiverShardID == destId && mb.SenderShardID != destId {
				miniBlocks = append(miniBlocks, &data.MiniBlockInfo{
					Hash:          mb.Hash,
					SenderShardID: mb.SenderShardID,
					Round:         m.ShardInfo[i].Round,
				})
			}
		}
	}

	for _, mb := range m.MiniBlockHeaders {
		isDestinationShard := (mb.ReceiverShardID == destId ||
			mb.ReceiverShardID == core.AllShardId) &&
			mb.SenderShardID != destId
		if isDestinationShard {
			miniBlocks = append(miniBlocks, &data.MiniBlockInfo{
				Hash:          mb.Hash,
				SenderShardID: mb.SenderShardID,
				Round:         m.Round,
			})
		}
	}

	sort.Slice(miniBlocks, func(i, j int) bool {
		return miniBlocks[i].Round < miniBlocks[j].Round
	})

	return miniBlocks
}

// GetMiniBlockHeadersHashes gets the miniblock hashes
func (m *MetaBlock) GetMiniBlockHeadersHashes() [][]byte {
	if m == nil {
		return nil
	}

	result := make([][]byte, 0, len(m.MiniBlockHeaders))
	for _, miniblock := range m.MiniBlockHeaders {
		result = append(result, miniblock.Hash)
	}

	return result
}

// IsInterfaceNil returns true if there is no value under the interface
func (m *MetaBlock) IsInterfaceNil() bool {
	return m == nil
}

// IsStartOfEpochBlock verifies if the block is of type start of epoch
func (m *MetaBlock) IsStartOfEpochBlock() bool {
	if m == nil {
		return false
	}

	return len(m.EpochStart.LastFinalizedHeaders) > 0
}

// ShallowClone will return a clone of the object
func (m *MetaBlock) ShallowClone() data.HeaderHandler {
	if m == nil {
		return nil
	}

	metaBlockCopy := *m

	return &metaBlockCopy
}

// GetMiniBlockHeaderHandlers returns the miniBlock headers as an array of miniBlock header handlers
func (m *MetaBlock) GetMiniBlockHeaderHandlers() []data.MiniBlockHeaderHandler {
	if m == nil {
		return nil
	}

	mbHeaders := m.GetMiniBlockHeaders()
	mbHeaderHandlers := make([]data.MiniBlockHeaderHandler, len(mbHeaders))

	for i := range mbHeaders {
		mbHeaderHandlers[i] = &mbHeaders[i]
	}

	return mbHeaderHandlers
}

// HasScheduledSupport returns false as the initial metaBlock version does not support scheduled data
func (m *MetaBlock) HasScheduledSupport() bool {
	return false
}

// SetMiniBlockHeaderHandlers sets the miniBlock headers from the given miniBlock header handlers
func (m *MetaBlock) SetMiniBlockHeaderHandlers(mbHeaderHandlers []data.MiniBlockHeaderHandler) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}
	if mbHeaderHandlers == nil {
		m.MiniBlockHeaders = nil
		return nil
	}

	mbHeaders := make([]MiniBlockHeader, len(mbHeaderHandlers))
	for i := range mbHeaderHandlers {
		mbHeader, ok := mbHeaderHandlers[i].(*MiniBlockHeader)
		if !ok {
			return data.ErrInvalidTypeAssertion
		}
		if mbHeader == nil {
			return data.ErrNilPointerDereference
		}
		mbHeaders[i] = *mbHeader
	}

	m.MiniBlockHeaders = mbHeaders

	return nil
}

// SetReceiptsHash sets the receipts hash
func (m *MetaBlock) SetReceiptsHash(hash []byte) error {
	if m == nil {
		return nil
	}

	m.ReceiptsHash = hash

	return nil
}

// GetShardInfoHandlers - gets the shardInfo as an array of ShardDataHandler
func (m *MetaBlock) GetShardInfoHandlers() []data.ShardDataHandler {
	if m == nil || m.ShardInfo == nil {
		return nil
	}

	shardInfoHandlers := make([]data.ShardDataHandler, len(m.ShardInfo))
	for i := range m.ShardInfo {
		shardInfoHandlers[i] = &m.ShardInfo[i]
	}

	return shardInfoHandlers
}

// GetEpochStartHandler -
func (m *MetaBlock) GetEpochStartHandler() data.EpochStartHandler {
	if m == nil {
		return nil
	}

	return &m.EpochStart
}

// SetShardInfoHandlers -
func (m *MetaBlock) SetShardInfoHandlers(shardInfo []data.ShardDataHandler) error {
	if m == nil {
		return data.ErrNilPointerReceiver
	}
	if shardInfo == nil {
		m.ShardInfo = nil
		return nil
	}

	sInfo := make([]ShardData, len(shardInfo))
	for i := range shardInfo {
		shData, ok := shardInfo[i].(*ShardData)
		if !ok {
			return data.ErrInvalidTypeAssertion
		}
		if shData == nil {
			return data.ErrNilPointerDereference
		}
		sInfo[i] = *shData
	}

	m.ShardInfo = sInfo

	return nil
}

// SetScheduledRootHash not supported on the first version of metablock
func (m *MetaBlock) SetScheduledRootHash(_ []byte) error {
	return data.ErrScheduledRootHashNotSupported
}

// ValidateHeaderVersion - always valid for initial version
func (m *MetaBlock) ValidateHeaderVersion() error {
	return nil
}

// SetAdditionalData sets the additional version-related data
func (m *MetaBlock) SetAdditionalData(_ headerVersionData.HeaderAdditionalData) error {
	// no extra data for the initial version metaBlock header
	return nil
}

// GetAdditionalData gets the additional version-related data for the header
func (m *MetaBlock) GetAdditionalData() headerVersionData.HeaderAdditionalData {
	// no extra data for the initial version of meta block header
	return nil
}
