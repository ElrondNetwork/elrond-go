package bn_test

import (
	"testing"

	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos/bn"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos/mock"
	"github.com/ElrondNetwork/elrond-go-sandbox/crypto"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/blockchain"
	"github.com/ElrondNetwork/elrond-go-sandbox/process"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func initSubroundEndRound() bn.SubroundEndRound {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, _ := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	return srEndRound
}

func TestSubroundEndRound_NewSubroundEndRoundNilSubroundShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	srEndRound, err := bn.NewSubroundEndRound(
		nil,
		&blockChain,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilSubround)
}

func TestSubroundEndRound_NewSubroundEndRoundNilBlockChainShouldFail(t *testing.T) {
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		nil,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilBlockChain)
}

func TestSubroundEndRound_NewSubroundEndRoundNilBlockProcessorShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		nil,
		consensusState,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilBlockProcessor)
}

func TestSubroundEndRound_NewSubroundEndRoundNilConsensusStateShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		nil,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilConsensusState)
}

func TestSubroundEndRound_NewSubroundEndRoundNilMultisignerShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		consensusState,
		nil,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilMultiSigner)
}

func TestSubroundEndRound_NewSubroundEndRoundNilRounderShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		nil,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilRounder)
}

func TestSubroundEndRound_NewSubroundEndRoundNilSyncTimerShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		rounderMock,
		nil,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilSyncTimer)
}

func TestSubroundEndRound_NewSubroundEndRoundNilBroadcastTxBlockBodyFunctionShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		nil,
		broadcastHeader,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilBroadcastTxBlockBodyFunction)
}

func TestSubroundEndRound_NewSubroundEndRoundNilBroadcastHeaderFunctionShouldFail(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		nil,
		extend,
	)

	assert.Nil(t, srEndRound)
	assert.Equal(t, err, spos.ErrNilBroadcastHeaderFunction)
}

func TestSubroundEndRound_NewSubroundEndRoundShouldWork(t *testing.T) {
	blockChain := blockchain.BlockChain{}
	blockProcessorMock := initBlockProcessorMock()
	consensusState := initConsensusState()
	multiSignerMock := initMultiSignerMock()
	rounderMock := initRounderMock()
	syncTimerMock := mock.SyncTimerMock{}

	ch := make(chan bool, 1)

	sr, _ := bn.NewSubround(
		int(bn.SrSignature),
		int(bn.SrEndRound),
		-1,
		int64(85*roundTimeDuration/100),
		int64(95*roundTimeDuration/100),
		"(END_ROUND)",
		ch,
	)

	srEndRound, err := bn.NewSubroundEndRound(
		sr,
		&blockChain,
		blockProcessorMock,
		consensusState,
		multiSignerMock,
		rounderMock,
		syncTimerMock,
		broadcastTxBlockBody,
		broadcastHeader,
		extend,
	)

	assert.NotNil(t, srEndRound)
	assert.Nil(t, err)
}

func TestSubroundEndRound_DoEndRoundJobErrAggregatingSigShouldFail(t *testing.T) {
	sr := *initSubroundEndRound()

	multiSignerMock := initMultiSignerMock()

	multiSignerMock.AggregateSigsMock = func(bitmap []byte) ([]byte, error) {
		return nil, crypto.ErrNilHasher
	}

	sr.SetMultiSigner(multiSignerMock)
	sr.ConsensusState().Header = &block.Header{}

	r := sr.DoEndRoundJob()
	assert.False(t, r)
}

func TestSubroundEndRound_DoEndRoundJobErrCommitBlockShouldFail(t *testing.T) {
	sr := *initSubroundEndRound()

	blProcMock := initBlockProcessorMock()

	blProcMock.CommitBlockCalled = func(
		blockChain *blockchain.BlockChain,
		header *block.Header,
		block *block.TxBlockBody,
	) error {
		return blockchain.ErrHeaderUnitNil
	}

	sr.SetBlockProcessor(blProcMock)
	sr.ConsensusState().Header = &block.Header{}

	r := sr.DoEndRoundJob()
	assert.False(t, r)
}

func TestSubroundEndRound_DoEndRoundJobErrRemBlockTxOK(t *testing.T) {
	sr := *initSubroundEndRound()

	blProcMock := initBlockProcessorMock()

	blProcMock.RemoveBlockTxsFromPoolCalled = func(body *block.TxBlockBody) error {
		return process.ErrNilBlockBodyPool
	}

	sr.SetBlockProcessor(blProcMock)
	sr.ConsensusState().Header = &block.Header{}

	r := sr.DoEndRoundJob()
	assert.True(t, r)
}

func TestSubroundEndRound_DoEndRoundJobErrBroadcastTxBlockBodyOK(t *testing.T) {
	sr := *initSubroundEndRound()

	sr.SetBroadcastTxBlockBody(func(txBlockBody *block.TxBlockBody) error {
		return spos.ErrNilBroadcastTxBlockBodyFunction
	})

	sr.ConsensusState().Header = &block.Header{}

	r := sr.DoEndRoundJob()
	assert.True(t, r)
}

func TestSubroundEndRound_DoEndRoundJobErrBroadcastHeaderOK(t *testing.T) {
	sr := *initSubroundEndRound()

	sr.SetBroadcastHeader(func(header *block.Header) error {
		return spos.ErrNilBroadcastHeaderFunction
	})

	sr.ConsensusState().Header = &block.Header{}

	r := sr.DoEndRoundJob()
	assert.True(t, r)
}

func TestSubroundEndRound_DoEndRoundJobAllOK(t *testing.T) {
	sr := *initSubroundEndRound()

	sr.ConsensusState().Header = &block.Header{}

	r := sr.DoEndRoundJob()
	assert.True(t, r)
}

func TestSubroundEndRound_DoEndRoundConsensusCheckShouldReturnFalseWhenRoundIsCanceled(t *testing.T) {
	sr := *initSubroundEndRound()

	sr.ConsensusState().RoundCanceled = true

	ok := sr.DoEndRoundConsensusCheck()
	assert.False(t, ok)
}

func TestSubroundEndRound_DoEndRoundConsensusCheckShouldReturnTrueWhenRoundIsFinished(t *testing.T) {
	sr := *initSubroundEndRound()

	sr.ConsensusState().SetStatus(bn.SrEndRound, spos.SsFinished)

	ok := sr.DoEndRoundConsensusCheck()
	assert.True(t, ok)
}

func TestSubroundEndRound_DoEndRoundConsensusCheckShouldReturnFalseWhenRoundIsNotFinished(t *testing.T) {
	sr := *initSubroundEndRound()

	ok := sr.DoEndRoundConsensusCheck()
	assert.False(t, ok)
}

func TestSubroundEndRound_CheckSignaturesValidityShouldErrNilSignature(t *testing.T) {
	sr := *initSubroundEndRound()

	err := sr.CheckSignaturesValidity([]byte(string(2)))
	assert.Equal(t, spos.ErrNilSignature, err)
}

func TestSubroundEndRound_CheckSignaturesValidityShouldErrInvalidIndex(t *testing.T) {
	sr := *initSubroundEndRound()

	sr.MultiSigner().Reset(nil, 0)

	sr.ConsensusState().SetJobDone(sr.ConsensusState().ConsensusGroup()[0], bn.SrSignature, true)

	err := sr.CheckSignaturesValidity([]byte(string(1)))
	assert.Equal(t, crypto.ErrInvalidIndex, err)
}

func TestSubroundEndRound_CheckSignaturesValidityShouldErrInvalidSignatureShare(t *testing.T) {
	sr := *initSubroundEndRound()

	multiSignerMock := initMultiSignerMock()

	err := errors.New("invalid signature share")
	multiSignerMock.VerifySignatureShareMock = func(index uint16, sig []byte, bitmap []byte) error {
		return err
	}

	sr.SetMultiSigner(multiSignerMock)

	sr.ConsensusState().SetJobDone(sr.ConsensusState().ConsensusGroup()[0], bn.SrSignature, true)

	err2 := sr.CheckSignaturesValidity([]byte(string(1)))
	assert.Equal(t, err, err2)
}

func TestSubroundEndRound_CheckSignaturesValidityShouldRetunNil(t *testing.T) {
	sr := *initSubroundEndRound()

	sr.ConsensusState().SetJobDone(sr.ConsensusState().ConsensusGroup()[0], bn.SrSignature, true)

	err := sr.CheckSignaturesValidity([]byte(string(1)))
	assert.Equal(t, nil, err)
}
