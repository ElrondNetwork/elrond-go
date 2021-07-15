//go:generate protoc -I=proto -I=$GOPATH/src -I=$GOPATH/src/github.com/ElrondNetwork/protobuf/protobuf  --gogoslick_out=. blockV2.proto
package block

import (
	"math/big"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/headerVersionData"
)

// GetShardID returns the header shardID
func (hv2 *HeaderV2) GetShardID() uint32 {
	if hv2 == nil {
		return 0
	}

	return hv2.Header.GetShardID()
}

// GetNonce returns the header nonce
func (hv2 *HeaderV2) GetNonce() uint64 {
	if hv2 == nil {
		return 0
	}

	return hv2.Header.GetNonce()
}

// GetEpoch returns the header epoch
func (hv2 *HeaderV2) GetEpoch() uint32 {
	if hv2 == nil {
		return 0
	}

	return hv2.Header.GetEpoch()
}

// GetRound returns the header round
func (hv2 *HeaderV2) GetRound() uint64 {
	if hv2 == nil {
		return 0
	}

	return hv2.Header.GetRound()
}

// GetRootHash returns the header root hash
func (hv2 *HeaderV2) GetRootHash() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetRootHash()
}

// GetPrevHash returns the header previous header hash
func (hv2 *HeaderV2) GetPrevHash() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetPrevHash()
}

// GetPrevRandSeed returns the header previous random seed
func (hv2 *HeaderV2) GetPrevRandSeed() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetPrevRandSeed()
}

// GetRandSeed returns the header random seed
func (hv2 *HeaderV2) GetRandSeed() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetRandSeed()
}

// GetPubKeysBitmap returns the header public key bitmap for the aggregated signatures
func (hv2 *HeaderV2) GetPubKeysBitmap() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetPubKeysBitmap()
}

// GetSignature returns the header aggregated signature
func (hv2 *HeaderV2) GetSignature() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetSignature()
}

// GetLeaderSignature returns the leader signature on top of the finalized (signed) header
func (hv2 *HeaderV2) GetLeaderSignature() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetLeaderSignature()
}

// GetChainID returns the chain ID
func (hv2 *HeaderV2) GetChainID() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetChainID()
}

// GetSoftwareVersion returns the header software version
func (hv2 *HeaderV2) GetSoftwareVersion() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetSoftwareVersion()
}

// GetTimeStamp returns the header timestamp
func (hv2 *HeaderV2) GetTimeStamp() uint64 {
	if hv2 == nil {
		return 0
	}

	return hv2.Header.GetTimeStamp()
}

// GetTxCount returns the number of txs included in the block
func (hv2 *HeaderV2) GetTxCount() uint32 {
	if hv2 == nil {
		return 0
	}

	return hv2.Header.GetTxCount()
}

// GetReceiptsHash returns the header receipt hash
func (hv2 *HeaderV2) GetReceiptsHash() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetReceiptsHash()
}

// GetAccumulatedFees returns the block accumulated fees
func (hv2 *HeaderV2) GetAccumulatedFees() *big.Int {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetAccumulatedFees()
}

// GetDeveloperFees returns the block developer fees
func (hv2 *HeaderV2) GetDeveloperFees() *big.Int {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetDeveloperFees()
}

// GetReserved returns the reserved field
func (hv2 *HeaderV2) GetReserved() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetReserved()
}

// GetMetaBlockHashes returns the metaBlock hashes
func (hv2 *HeaderV2) GetMetaBlockHashes() [][]byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetMetaBlockHashes()
}

// GetEpochStartMetaHash returns the epoch start metaBlock hash
func (hv2 *HeaderV2) GetEpochStartMetaHash() []byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetEpochStartMetaHash()
}

// SetNonce sets header nonce
func (hv2 *HeaderV2) SetNonce(n uint64) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}
	return hv2.Header.SetNonce(n)
}

// SetEpoch sets header epoch
func (hv2 *HeaderV2) SetEpoch(e uint32) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetEpoch(e)
}

// SetRound sets header round
func (hv2 *HeaderV2) SetRound(r uint64) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetRound(r)
}

// SetRootHash sets root hash
func (hv2 *HeaderV2) SetRootHash(rHash []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetRootHash(rHash)
}

// SetPrevHash sets prev hash
func (hv2 *HeaderV2) SetPrevHash(pvHash []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetPrevHash(pvHash)
}

// SetPrevRandSeed sets previous random seed
func (hv2 *HeaderV2) SetPrevRandSeed(pvRandSeed []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetPrevRandSeed(pvRandSeed)
}

// SetRandSeed sets previous random seed
func (hv2 *HeaderV2) SetRandSeed(randSeed []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetRandSeed(randSeed)
}

// SetPubKeysBitmap sets public key bitmap
func (hv2 *HeaderV2) SetPubKeysBitmap(pkbm []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetPubKeysBitmap(pkbm)
}

// SetSignature sets header signature
func (hv2 *HeaderV2) SetSignature(sg []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetSignature(sg)
}

// SetLeaderSignature will set the leader's signature
func (hv2 *HeaderV2) SetLeaderSignature(sg []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetLeaderSignature(sg)
}

// SetChainID sets the chain ID on which this block is valid on
func (hv2 *HeaderV2) SetChainID(chainID []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetChainID(chainID)
}

// SetSoftwareVersion sets the software version of the header
func (hv2 *HeaderV2) SetSoftwareVersion(version []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetSoftwareVersion(version)
}

// SetTimeStamp sets header timestamp
func (hv2 *HeaderV2) SetTimeStamp(ts uint64) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetTimeStamp(ts)
}

// SetAccumulatedFees sets the accumulated fees in the header
func (hv2 *HeaderV2) SetAccumulatedFees(value *big.Int) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetAccumulatedFees(value)
}

// SetDeveloperFees sets the developer fees in the header
func (hv2 *HeaderV2) SetDeveloperFees(value *big.Int) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetDeveloperFees(value)
}

// SetTxCount sets the transaction count of the block associated with this header
func (hv2 *HeaderV2) SetTxCount(txCount uint32) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetTxCount(txCount)
}

// SetShardID sets header shard ID
func (hv2 *HeaderV2) SetShardID(shId uint32) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetShardID(shId)
}

// GetMiniBlockHeadersWithDst as a map of hashes and sender IDs
func (hv2 *HeaderV2) GetMiniBlockHeadersWithDst(destId uint32) map[string]uint32 {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetMiniBlockHeadersWithDst(destId)
}

// GetOrderedCrossMiniblocksWithDst gets all cross miniblocks with the given destination shard ID, ordered in a
// chronological way, taking into consideration the round in which they were created/executed in the sender shard
func (hv2 *HeaderV2) GetOrderedCrossMiniblocksWithDst(destId uint32) []*data.MiniBlockInfo {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetOrderedCrossMiniblocksWithDst(destId)
}

// GetMiniBlockHeadersHashes gets the miniblock hashes
func (hv2 *HeaderV2) GetMiniBlockHeadersHashes() [][]byte {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetMiniBlockHeadersHashes()
}

// MapMiniBlockHashesToShards is a map of mini block hashes and sender IDs
func (hv2 *HeaderV2) MapMiniBlockHashesToShards() map[string]uint32 {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.MapMiniBlockHashesToShards()
}

// ShallowClone returns a clone of the object
func (hv2 *HeaderV2) ShallowClone() data.HeaderHandler {
	if hv2 == nil || hv2.Header == nil {
		return nil
	}

	internalHeaderCopy := *hv2.Header
	headerCopy := *hv2
	headerCopy.Header = &internalHeaderCopy

	return &headerCopy
}

// IsInterfaceNil returns true if there is no value under the interface
func (hv2 *HeaderV2) IsInterfaceNil() bool {
	return hv2 == nil
}

// IsStartOfEpochBlock verifies if the block is of type start of epoch
func (hv2 *HeaderV2) IsStartOfEpochBlock() bool {
	if hv2 == nil {
		return false
	}

	return hv2.Header.IsStartOfEpochBlock()
}

// GetBlockBodyTypeInt32 returns the block body type as int32
func (hv2 *HeaderV2) GetBlockBodyTypeInt32() int32 {
	if hv2 == nil {
		return -1
	}

	return hv2.Header.GetBlockBodyTypeInt32()
}

// GetMiniBlockHeaderHandlers returns the miniBlock headers as an array of miniBlock header handlers
func (hv2 *HeaderV2) GetMiniBlockHeaderHandlers() []data.MiniBlockHeaderHandler {
	if hv2 == nil {
		return nil
	}

	return hv2.Header.GetMiniBlockHeaderHandlers()
}

// SetMiniBlockHeaderHandlers sets the miniBlock headers from the given miniBlock header handlers
func (hv2 *HeaderV2) SetMiniBlockHeaderHandlers(mbHeaderHandlers []data.MiniBlockHeaderHandler) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetMiniBlockHeaderHandlers(mbHeaderHandlers)
}

// SetReceiptsHash sets the receipts hash
func (hv2 *HeaderV2) SetReceiptsHash(hash []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetReceiptsHash(hash)
}

// SetMetaBlockHashes sets the metaBlock hashes
func (hv2 *HeaderV2) SetMetaBlockHashes(hashes [][]byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetMetaBlockHashes(hashes)
}

// SetEpochStartMetaHash sets the epoch start metaBlock hash
func (hv2 *HeaderV2) SetEpochStartMetaHash(hash []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	return hv2.Header.SetEpochStartMetaHash(hash)
}

// HasScheduledSupport returns true as the second block version does support scheduled data
func (hv2 *HeaderV2) HasScheduledSupport() bool {
	return true
}

// SetScheduledRootHash sets the scheduled root hash
func (hv2 *HeaderV2) SetScheduledRootHash(rootHash []byte) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	hv2.ScheduledRootHash = rootHash

	return nil
}

// ValidateHeaderVersion does extra validation for header version
func (hv2 *HeaderV2) ValidateHeaderVersion() error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	// the header needs to have a not nil & not empty scheduled root hash
	if len(hv2.ScheduledRootHash) == 0 {
		return data.ErrNilScheduledRootHash
	}

	return hv2.Header.ValidateHeaderVersion()
}

// SetAdditionalData sets the additional version related data for the header
func (hv2 *HeaderV2) SetAdditionalData(headerVersionData headerVersionData.HeaderAdditionalData) error {
	if hv2 == nil {
		return data.ErrNilPointerReceiver
	}

	if check.IfNil(headerVersionData) {
		return data.ErrNilPointerDereference
	}
	return hv2.SetScheduledRootHash(headerVersionData.GetScheduledRootHash())
}

// GetAdditionalData gets the additional version related data for the header
func (hv2 *HeaderV2) GetAdditionalData() headerVersionData.HeaderAdditionalData {
	if hv2 == nil {
		return nil
	}

	additionalVersionData := &headerVersionData.AdditionalData{
		ScheduledRootHash: hv2.GetScheduledRootHash(),
	}
	return additionalVersionData
}
