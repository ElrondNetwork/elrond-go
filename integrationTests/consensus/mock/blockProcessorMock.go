package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/ElrondNetwork/elrond-go-sandbox/marshal"
	"math/big"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/data"
)

// BlockProcessorMock mocks the implementation for a blockProcessor
type BlockProcessorMock struct {
	NrCommitBlockCalled              uint32
	Marshalizer                      marshal.Marshalizer
	ProcessBlockCalled               func(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error
	CommitBlockCalled                func(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler) error
	RevertAccountStateCalled         func()
	CreateGenesisBlockCalled         func(balances map[string]*big.Int) (data.HeaderHandler, error)
	CreateBlockCalled                func(round int32, haveTime func() bool) (data.BodyHandler, error)
	RestoreBlockIntoPoolsCalled      func(header data.HeaderHandler, body data.BodyHandler) error
	SetOnRequestTransactionCalled    func(f func(destShardID uint32, txHash []byte))
	CreateBlockHeaderCalled          func(body data.BodyHandler, round int32, haveTime func() bool) (data.HeaderHandler, error)
	MarshalizedDataToBroadcastCalled func(header data.HeaderHandler, body data.BodyHandler) (map[uint32][]byte, map[uint32][][]byte, error)
	DecodeBlockBodyCalled            func(dta []byte) data.BodyHandler
	DecodeBlockHeaderCalled          func(dta []byte) data.HeaderHandler
}

// SetOnRequestTransaction mocks setting request transaction call back function
func (blProcMock *BlockProcessorMock) SetOnRequestTransaction(f func(destShardID uint32, txHash []byte)) {
	blProcMock.SetOnRequestTransactionCalled(f)
}

// ProcessBlock mocks pocessing a block
func (blProcMock *BlockProcessorMock) ProcessBlock(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
	return blProcMock.ProcessBlockCalled(blockChain, header, body, haveTime)
}

// CommitBlock mocks the commit of a block
func (blProcMock *BlockProcessorMock) CommitBlock(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler) error {
	return blProcMock.CommitBlockCalled(blockChain, header, body)
}

// RevertAccountState mocks revert of the accounts state
func (blProcMock *BlockProcessorMock) RevertAccountState() {
	blProcMock.RevertAccountStateCalled()
}

// CreateGenesisBlock mocks the creation of a genesis block body
func (blProcMock *BlockProcessorMock) CreateGenesisBlock(balances map[string]*big.Int) (data.HeaderHandler, error) {
	return blProcMock.CreateGenesisBlockCalled(balances)
}

// CreateTxBlockBody mocks the creation of a transaction block body
func (blProcMock *BlockProcessorMock) CreateBlockBody(round int32, haveTime func() bool) (data.BodyHandler, error) {
	return blProcMock.CreateBlockCalled(round, haveTime)
}

func (blProcMock *BlockProcessorMock) RestoreBlockIntoPools(header data.HeaderHandler, body data.BodyHandler) error {
	return blProcMock.RestoreBlockIntoPoolsCalled(header, body)
}

func (blProcMock BlockProcessorMock) CreateBlockHeader(body data.BodyHandler, round int32, haveTime func() bool) (data.HeaderHandler, error) {
	return blProcMock.CreateBlockHeaderCalled(body, round, haveTime)
}

func (blProcMock BlockProcessorMock) MarshalizedDataToBroadcast(header data.HeaderHandler, body data.BodyHandler) (map[uint32][]byte, map[uint32][][]byte, error) {
	return blProcMock.MarshalizedDataToBroadcastCalled(header, body)
}

// DecodeBlockBody method decodes block body from a given byte array
func (blProcMock BlockProcessorMock) DecodeBlockBody(dta []byte) data.BodyHandler {
	if dta == nil {
		return nil
	}

	var body block.Body

	err := blProcMock.Marshalizer.Unmarshal(&body, dta)
	if err != nil {
		return nil
	}

	return body
}

// DecodeBlockHeader method decodes block header from a given byte array
func (blProcMock BlockProcessorMock) DecodeBlockHeader(dta []byte) data.HeaderHandler {
	if dta == nil {
		return nil
	}

	var header block.Header

	err := blProcMock.Marshalizer.Unmarshal(&header, dta)
	if err != nil {
		return nil
	}

	return &header
}
