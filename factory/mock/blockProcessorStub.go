package mock

import (
	"math/big"
	"time"

	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/process/block/processedMb"
)

// BlockProcessorStub mocks the implementation for a blockProcessor
type BlockProcessorStub struct {
	ProcessBlockCalled               func(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error
	ProcessScheduledBlockCalled      func(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error
	CommitBlockCalled                func(header data.HeaderHandler, body data.BodyHandler) error
	RevertAccountStateCalled         func()
	CreateGenesisBlockCalled         func(balances map[string]*big.Int) (data.HeaderHandler, error)
	CreateBlockCalled                func(initialHdrData data.HeaderHandler, haveTime func() bool) (data.HeaderHandler, data.BodyHandler, error)
	RestoreBlockIntoPoolsCalled      func(header data.HeaderHandler, body data.BodyHandler) error
	SetOnRequestTransactionCalled    func(f func(destShardID uint32, txHash []byte))
	MarshalizedDataToBroadcastCalled func(header data.HeaderHandler, body data.BodyHandler) (map[uint32][]byte, map[string][][]byte, error)
	DecodeBlockBodyCalled            func(dta []byte) data.BodyHandler
	DecodeBlockHeaderCalled          func(dta []byte) data.HeaderHandler
	AddLastNotarizedHdrCalled        func(shardId uint32, processedHdr data.HeaderHandler)
	CreateNewHeaderCalled            func(round uint64, nonce uint64) (data.HeaderHandler, error)
	PruneStateOnRollbackCalled       func(currHeader data.HeaderHandler, currHeaderHash []byte, prevHeader data.HeaderHandler, prevHeaderHash []byte)
	RevertStateToBlockCalled         func(header data.HeaderHandler, rootHash []byte) error
}

// RestoreLastNotarizedHrdsToGenesis -
func (bps *BlockProcessorStub) RestoreLastNotarizedHrdsToGenesis() {
}

// SetNumProcessedObj -
func (bps *BlockProcessorStub) SetNumProcessedObj(_ uint64) {
}

// ProcessBlock mocks processing a block
func (bps *BlockProcessorStub) ProcessBlock(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
	return bps.ProcessBlockCalled(header, body, haveTime)
}

// ProcessScheduledBlock mocks processing a scheduled block
func (bps *BlockProcessorStub) ProcessScheduledBlock(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
	return bps.ProcessScheduledBlockCalled(header, body, haveTime)
}

// CommitBlock mocks the commit of a block
func (bps *BlockProcessorStub) CommitBlock(header data.HeaderHandler, body data.BodyHandler) error {
	return bps.CommitBlockCalled(header, body)
}

// RevertAccountState mocks revert of the accounts state
func (bps *BlockProcessorStub) RevertAccountState() {
	bps.RevertAccountStateCalled()
}

// CreateGenesisBlock mocks the creation of a genesis block body
func (bps *BlockProcessorStub) CreateGenesisBlock(balances map[string]*big.Int) (data.HeaderHandler, error) {
	return bps.CreateGenesisBlockCalled(balances)
}

// PruneStateOnRollback recreates thee state tries to the root hashes indicated by the provided header
func (bps *BlockProcessorStub) PruneStateOnRollback(currHeader data.HeaderHandler, currHeaderHash []byte, prevHeader data.HeaderHandler, prevHeaderHash []byte) {
	if bps.PruneStateOnRollbackCalled != nil {
		bps.PruneStateOnRollbackCalled(currHeader, currHeaderHash, prevHeader, prevHeaderHash)
	}
}

// CreateBlock mocks the creation of a new block with header and body
func (bps *BlockProcessorStub) CreateBlock(initialHdrData data.HeaderHandler, haveTime func() bool) (data.HeaderHandler, data.BodyHandler, error) {
	return bps.CreateBlockCalled(initialHdrData, haveTime)
}

// RestoreBlockIntoPools -
func (bps *BlockProcessorStub) RestoreBlockIntoPools(header data.HeaderHandler, body data.BodyHandler) error {
	return bps.RestoreBlockIntoPoolsCalled(header, body)
}

// MarshalizedDataToBroadcast -
func (bps *BlockProcessorStub) MarshalizedDataToBroadcast(header data.HeaderHandler, body data.BodyHandler) (map[uint32][]byte, map[string][][]byte, error) {
	return bps.MarshalizedDataToBroadcastCalled(header, body)
}

// DecodeBlockBody -
func (bps *BlockProcessorStub) DecodeBlockBody(dta []byte) data.BodyHandler {
	return bps.DecodeBlockBodyCalled(dta)
}

// DecodeBlockHeader -
func (bps *BlockProcessorStub) DecodeBlockHeader(dta []byte) data.HeaderHandler {
	return bps.DecodeBlockHeaderCalled(dta)
}

// AddLastNotarizedHdr -
func (bps *BlockProcessorStub) AddLastNotarizedHdr(shardId uint32, processedHdr data.HeaderHandler) {
	bps.AddLastNotarizedHdrCalled(shardId, processedHdr)
}

// CreateNewHeader creates a new header
func (bps *BlockProcessorStub) CreateNewHeader(round uint64, nonce uint64) (data.HeaderHandler, error) {
	return bps.CreateNewHeaderCalled(round, nonce)
}

// ApplyProcessedMiniBlocks -
func (bps *BlockProcessorStub) ApplyProcessedMiniBlocks(_ *processedMb.ProcessedMiniBlockTracker) {
}

// IsInterfaceNil returns true if there is no value under the interface
func (bps *BlockProcessorStub) IsInterfaceNil() bool {
	return bps == nil
}

// Close -
func (bps *BlockProcessorStub) Close() error {
	return nil
}

// RevertStateToBlock recreates the state tries to the root hashes indicated by the provided header
func (bps *BlockProcessorStub) RevertStateToBlock(header data.HeaderHandler, rootHash []byte) error {
	if bps.RevertStateToBlockCalled != nil {
		return bps.RevertStateToBlockCalled(header, rootHash)
	}

	return nil
}
