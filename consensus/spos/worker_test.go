package spos_test

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/consensus/mock"
	"github.com/ElrondNetwork/elrond-go/consensus/spos"
	"github.com/ElrondNetwork/elrond-go/consensus/spos/bls"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/p2pmocks"
	"github.com/stretchr/testify/assert"
)

const roundTimeDuration = 100 * time.Millisecond

var fromConnectedPeerId = core.PeerID("connected peer id")

const HashSize = 32
const SignatureSize = 48
const PublicKeySize = 96

var blockHeaderHash = make([]byte, HashSize)
var invalidBlockHeaderHash = make([]byte, HashSize+1)
var signature = make([]byte, SignatureSize)
var invalidSignature = make([]byte, SignatureSize+1)
var publicKey = make([]byte, PublicKeySize)

func createDefaultWorkerArgs(appStatusHandler core.AppStatusHandler) *spos.WorkerArgs {
	blockchainMock := &mock.BlockChainMock{}
	blockProcessor := &mock.BlockProcessorMock{
		DecodeBlockHeaderCalled: func(dta []byte) data.HeaderHandler {
			return nil
		},
		RevertAccountStateCalled: func() {
		},
		DecodeBlockBodyCalled: func(dta []byte) data.BodyHandler {
			return nil
		},
	}
	bootstrapperMock := &mock.BootstrapperMock{}
	broadcastMessengerMock := &mock.BroadcastMessengerMock{}
	consensusState := initConsensusState()
	forkDetectorMock := &mock.ForkDetectorMock{}
	forkDetectorMock.AddHeaderCalled = func(header data.HeaderHandler, hash []byte, state process.BlockHeaderState, selfNotarizedHeaders []data.HeaderHandler, selfNotarizedHeadersHashes [][]byte) error {
		return nil
	}
	keyGeneratorMock, _, _ := mock.InitKeys()
	marshalizerMock := mock.MarshalizerMock{}
	roundHandlerMock := initRoundHandlerMock()
	shardCoordinatorMock := mock.ShardCoordinatorMock{}
	singleSignerMock := &mock.SingleSignerMock{
		SignStub: func(private crypto.PrivateKey, msg []byte) ([]byte, error) {
			return []byte("signed"), nil
		},
		VerifyStub: func(public crypto.PublicKey, msg []byte, sig []byte) error {
			return nil
		},
	}
	syncTimerMock := &mock.SyncTimerMock{}
	hasher := &mock.HasherMock{}
	blsService, _ := bls.NewConsensusService()
	poolAdder := testscommon.NewCacherMock()

	scheduledProcessorArgs := spos.ScheduledProcessorWrapperArgs{
		SyncTimer:                  syncTimerMock,
		Processor:                  blockProcessor,
		ProcessingTimeMilliSeconds: 10,
	}
	scheduledProcessor, _ := spos.NewScheduledProcessorWrapper(scheduledProcessorArgs)

	peerSigHandler := &mock.PeerSignatureHandler{Signer: singleSignerMock, KeyGen: keyGeneratorMock}
	workerArgs := &spos.WorkerArgs{
		ConsensusService:         blsService,
		BlockChain:               blockchainMock,
		BlockProcessor:           blockProcessor,
		ScheduledProcessor:       scheduledProcessor,
		Bootstrapper:             bootstrapperMock,
		BroadcastMessenger:       broadcastMessengerMock,
		ConsensusState:           consensusState,
		ForkDetector:             forkDetectorMock,
		Marshalizer:              marshalizerMock,
		Hasher:                   hasher,
		RoundHandler:             roundHandlerMock,
		ShardCoordinator:         shardCoordinatorMock,
		PeerSignatureHandler:     peerSigHandler,
		SyncTimer:                syncTimerMock,
		HeaderSigVerifier:        &mock.HeaderSigVerifierStub{},
		HeaderIntegrityVerifier:  &mock.HeaderIntegrityVerifierStub{},
		ChainID:                  chainID,
		NetworkShardingCollector: &p2pmocks.NetworkShardingCollectorStub{},
		AntifloodHandler:         createMockP2PAntifloodHandler(),
		PoolAdder:                poolAdder,
		SignatureSize:            SignatureSize,
		PublicKeySize:            PublicKeySize,
		AppStatusHandler:         appStatusHandler,
		NodeRedundancyHandler:    &mock.NodeRedundancyHandlerStub{},
	}

	return workerArgs
}

func createMockP2PAntifloodHandler() *mock.P2PAntifloodHandlerStub {
	return &mock.P2PAntifloodHandlerStub{
		CanProcessMessageCalled: func(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
			return nil
		},
		CanProcessMessagesOnTopicCalled: func(peer core.PeerID, topic string, numMessages uint32, totalSize uint64, sequence []byte) error {
			return nil
		},
	}
}

func initWorker(appStatusHandler core.AppStatusHandler) *spos.Worker {
	workerArgs := createDefaultWorkerArgs(appStatusHandler)
	sposWorker, _ := spos.NewWorker(workerArgs)

	return sposWorker
}

func initRoundHandlerMock() *mock.RoundHandlerMock {
	return &mock.RoundHandlerMock{
		RoundIndex: 0,
		TimeStampCalled: func() time.Time {
			return time.Unix(0, 0)
		},
		TimeDurationCalled: func() time.Duration {
			return roundTimeDuration
		},
	}
}

func TestWorker_NewWorkerConsensusServiceNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.ConsensusService = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilConsensusService, err)
}

func TestWorker_NewWorkerBlockChainNilShouldFail(t *testing.T) {
	t.Parallel()
	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.BlockChain = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilBlockChain, err)
}

func TestWorker_NewWorkerBlockProcessorNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.BlockProcessor = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilBlockProcessor, err)
}

func TestWorker_NewWorkerBootstrapperNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.Bootstrapper = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilBootstrapper, err)
}

func TestWorker_NewWorkerBroadcastMessengerNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.BroadcastMessenger = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilBroadcastMessenger, err)
}

func TestWorker_NewWorkerConsensusStateNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.ConsensusState = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilConsensusState, err)
}

func TestWorker_NewWorkerForkDetectorNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.ForkDetector = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilForkDetector, err)
}

func TestWorker_NewWorkerMarshalizerNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.Marshalizer = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilMarshalizer, err)
}

func TestWorker_NewWorkerHasherNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.Hasher = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilHasher, err)
}

func TestWorker_NewWorkerRoundHandlerNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.RoundHandler = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilRoundHandler, err)
}

func TestWorker_NewWorkerShardCoordinatorNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.ShardCoordinator = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilShardCoordinator, err)
}

func TestWorker_NewWorkerPeerSignatureHandlerNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.PeerSignatureHandler = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilPeerSignatureHandler, err)
}

func TestWorker_NewWorkerSyncTimerNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.SyncTimer = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestWorker_NewWorkerHeaderSigVerifierNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.HeaderSigVerifier = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilHeaderSigVerifier, err)
}

func TestWorker_NewWorkerHeaderIntegrityVerifierShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.HeaderIntegrityVerifier = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilHeaderIntegrityVerifier, err)
}

func TestWorker_NewWorkerEmptyChainIDShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.ChainID = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrInvalidChainID, err)
}

func TestWorker_NewWorkerNilNetworkShardingCollectorShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.NetworkShardingCollector = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilNetworkShardingCollector, err)
}

func TestWorker_NewWorkerNilAntifloodHandlerShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.AntifloodHandler = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilAntifloodHandler, err)
}

func TestWorker_NewWorkerPoolAdderNilShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	workerArgs.PoolAdder = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilPoolAdder, err)
}

func TestWorker_NewWorkerNodeRedundancyHandlerShouldFail(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerMock{})
	workerArgs.NodeRedundancyHandler = nil
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, wrk)
	assert.Equal(t, spos.ErrNilNodeRedundancyHandler, err)
}

func TestWorker_NewWorkerShouldWork(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	wrk, err := spos.NewWorker(workerArgs)

	assert.Nil(t, err)
	assert.False(t, check.IfNil(wrk))
}

func TestWorker_ProcessReceivedMessageShouldErrIfFloodIsDetectedOnTopic(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("flood detected")
	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	antifloodHandler := &mock.P2PAntifloodHandlerStub{
		CanProcessMessageCalled: func(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
			return nil
		},
		CanProcessMessagesOnTopicCalled: func(peer core.PeerID, topic string, numMessages uint32, totalSize uint64, sequence []byte) error {
			return expectedErr
		},
	}

	workerArgs.AntifloodHandler = antifloodHandler
	wrk, _ := spos.NewWorker(workerArgs)

	msg := &mock.P2PMessageMock{DataField: []byte("aaa"), TopicField: "topic1"}
	err := wrk.ProcessReceivedMessage(msg, "peer")
	assert.Equal(t, expectedErr, err)
}

func TestWorker_ReceivedSyncStateShouldNotSendOnChannelWhenInputIsFalse(t *testing.T) {
	t.Parallel()
	wrk := initWorker(&mock.AppStatusHandlerStub{})
	wrk.ReceivedSyncState(false)
	rcv := false
	select {
	case rcv = <-wrk.ConsensusStateChangedChannel():
	case <-time.After(100 * time.Millisecond):
	}

	assert.False(t, rcv)
}

func TestWorker_ReceivedSyncStateShouldNotSendOnChannelWhenChannelIsBusy(t *testing.T) {
	t.Parallel()
	wrk := initWorker(&mock.AppStatusHandlerStub{})
	wrk.ConsensusStateChangedChannel() <- false
	wrk.ReceivedSyncState(true)
	rcv := false
	select {
	case rcv = <-wrk.ConsensusStateChangedChannel():
	case <-time.After(100 * time.Millisecond):
	}

	assert.False(t, rcv)
}

func TestWorker_ReceivedSyncStateShouldSendOnChannel(t *testing.T) {
	t.Parallel()
	wrk := initWorker(&mock.AppStatusHandlerStub{})
	wrk.ReceivedSyncState(true)
	rcv := false
	select {
	case rcv = <-wrk.ConsensusStateChangedChannel():
	case <-time.After(100 * time.Millisecond):
	}

	assert.True(t, rcv)
}

func TestWorker_InitReceivedMessagesShouldInitMap(t *testing.T) {
	t.Parallel()
	wrk := initWorker(&mock.AppStatusHandlerStub{})
	wrk.NilReceivedMessages()
	wrk.InitReceivedMessages()

	assert.NotNil(t, wrk.ReceivedMessages()[bls.MtBlockBody])
}

func TestWorker_AddReceivedMessageCallShouldWork(t *testing.T) {
	t.Parallel()
	wrk := initWorker(&mock.AppStatusHandlerStub{})
	receivedMessageCall := func(*consensus.Message) bool {
		return true
	}
	wrk.AddReceivedMessageCall(bls.MtBlockBody, receivedMessageCall)
	receivedMessageCalls := wrk.ReceivedMessagesCalls()

	assert.Equal(t, 1, len(receivedMessageCalls))
	assert.NotNil(t, receivedMessageCalls[bls.MtBlockBody])
	assert.True(t, receivedMessageCalls[bls.MtBlockBody](nil))
}

func TestWorker_RemoveAllReceivedMessageCallsShouldWork(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	receivedMessageCall := func(*consensus.Message) bool {
		return true
	}
	wrk.AddReceivedMessageCall(bls.MtBlockBody, receivedMessageCall)
	receivedMessageCalls := wrk.ReceivedMessagesCalls()

	assert.Equal(t, 1, len(receivedMessageCalls))
	assert.NotNil(t, receivedMessageCalls[bls.MtBlockBody])
	assert.True(t, receivedMessageCalls[bls.MtBlockBody](nil))

	wrk.RemoveAllReceivedMessagesCalls()
	receivedMessageCalls = wrk.ReceivedMessagesCalls()

	assert.Equal(t, 0, len(receivedMessageCalls))
	assert.Nil(t, receivedMessageCalls[bls.MtBlockBody])
}

func TestWorker_ProcessReceivedMessageTxBlockBodyShouldRetNil(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	time.Sleep(time.Second)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: currentPid,
	}
	err := wrk.ProcessReceivedMessage(msg, fromConnectedPeerId)

	assert.Nil(t, err)
}

func TestWorker_ProcessReceivedMessageNilMessageShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	err := wrk.ProcessReceivedMessage(nil, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.Equal(t, spos.ErrNilMessage, err)
}

func TestWorker_ProcessReceivedMessageNilMessageDataFieldShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.Equal(t, spos.ErrNilDataToProcess, err)
}

func TestWorker_ProcessReceivedMessageRedundancyNodeShouldResetInactivityIfNeeded(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	var wasCalled bool
	nodeRedundancyMock := &mock.NodeRedundancyHandlerStub{
		IsRedundancyNodeCalled: func() bool {
			return true
		},
		ResetInactivityIfNeededCalled: func(selfPubKey string, consensusMsgPubKey string, consensusMsgPeerID core.PeerID) {
			wasCalled = true
		},
	}
	wrk.SetNodeRedundancyHandler(nodeRedundancyMock)
	buff, _ := wrk.Marshalizer().Marshal(&consensus.Message{})
	_ = wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)

	assert.True(t, wasCalled)
}

func TestWorker_ProcessReceivedMessageNodeNotInEligibleListShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		publicKey,
		signature,
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.True(t, errors.Is(err, spos.ErrNodeIsNotInEligibleList))
}

func TestWorker_ProcessReceivedMessageComputeReceivedProposedBlockMetric(t *testing.T) {
	t.Parallel()

	receivedValue := uint64(0)
	wrk := *initWorker(&mock.AppStatusHandlerStub{
		SetUInt64ValueHandler: func(key string, value uint64) {
			receivedValue = value
		},
	})
	wrk.SetBlockProcessor(&mock.BlockProcessorMock{
		DecodeBlockHeaderCalled: func(dta []byte) data.HeaderHandler {
			return &block.Header{
				ChainID:         chainID,
				SoftwareVersion: []byte("version"),
			}
		},
		RevertAccountStateCalled: func() {
		},
		DecodeBlockBodyCalled: func(dta []byte) data.BodyHandler {
			return nil
		},
	})
	roundDuration := time.Millisecond * 1000
	delay := time.Millisecond * 430
	roundStartTimeStamp := time.Now()
	wrk.SetRoundHandler(&mock.RoundHandlerMock{
		RoundIndex: 0,
		TimeDurationCalled: func() time.Duration {
			return roundDuration
		},
		TimeStampCalled: func() time.Time {
			return roundStartTimeStamp
		},
	})
	hdr := &block.Header{ChainID: chainID}
	hdrHash, _ := core.CalculateHash(mock.MarshalizerMock{}, mock.HasherMock{}, hdr)
	hdrStr, _ := mock.MarshalizerMock{}.Marshal(hdr)
	cnsMsg := consensus.NewConsensusMessage(
		hdrHash,
		nil,
		nil,
		hdrStr,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockHeader),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)

	time.Sleep(delay)

	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: currentPid,
	}
	_ = wrk.ProcessReceivedMessage(msg, "")

	minimumExpectedValue := uint64(delay * 100 / roundDuration)
	assert.True(t,
		receivedValue >= minimumExpectedValue,
		fmt.Sprintf("minimum expected was %d, got %d", minimumExpectedValue, receivedValue),
	)
}

func TestWorker_ProcessReceivedMessageInconsistentChainIDInConsensusMessageShouldErr(t *testing.T) {
	t.Parallel()

	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		blockHeaderHash,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		1,
		[]byte("inconsistent chain ID"),
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)

	assert.True(t, errors.Is(err, spos.ErrInvalidChainID))
}

func TestWorker_ProcessReceivedMessageTypeInvalidShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		blockHeaderHash,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		666,
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[666]))
	assert.True(t, errors.Is(err, spos.ErrInvalidMessageType), err)
}

func TestWorker_ProcessReceivedHeaderHashSizeInvalidShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		invalidBlockHeaderHash,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.True(t, errors.Is(err, spos.ErrInvalidHeaderHashSize), err)
}

func TestWorker_ProcessReceivedMessageForFutureRoundShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockBody),
		2,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.True(t, errors.Is(err, spos.ErrMessageForFutureRound))
}

func TestWorker_ProcessReceivedMessageForPastRoundShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockBody),
		-1,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.True(t, errors.Is(err, spos.ErrMessageForPastRound))
}

func TestWorker_ProcessReceivedMessageTypeLimitReachedShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: currentPid,
	}

	err := wrk.ProcessReceivedMessage(msg, fromConnectedPeerId)
	time.Sleep(time.Second)
	assert.Equal(t, 1, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.Nil(t, err)

	err = wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)
	assert.Equal(t, 1, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.True(t, errors.Is(err, spos.ErrMessageTypeLimitReached))
}

func TestWorker_ProcessReceivedMessageInvalidSignatureShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		invalidSignature,
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.True(t, errors.Is(err, spos.ErrInvalidSignatureSize))
}

func TestWorker_ProcessReceivedMessageReceivedMessageIsFromSelfShouldRetNilAndNotProcess(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().SelfPubKey()),
		signature,
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: currentPid,
	}
	err := wrk.ProcessReceivedMessage(msg, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.Nil(t, err)
}

func TestWorker_ProcessReceivedMessageWhenRoundIsCanceledShouldRetNilAndNotProcess(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.ConsensusState().RoundCanceled = true
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: currentPid,
	}
	err := wrk.ProcessReceivedMessage(msg, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockBody]))
	assert.Nil(t, err)
}

func TestWorker_ProcessReceivedMessageWrongChainIDInProposedBlockShouldError(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.SetBlockProcessor(
		&mock.BlockProcessorMock{
			DecodeBlockHeaderCalled: func(dta []byte) data.HeaderHandler {
				return &testscommon.HeaderHandlerStub{
					CheckChainIDCalled: func(reference []byte) error {
						return spos.ErrInvalidChainID
					},
					GetPrevHashCalled: func() []byte {
						return make([]byte, 0)
					},
				}
			},
			RevertAccountStateCalled: func() {
			},
		},
	)

	hdr := &block.Header{ChainID: wrongChainID}
	hdrHash, _ := core.CalculateHash(mock.MarshalizerMock{}, mock.HasherMock{}, hdr)
	cnsMsg := consensus.NewConsensusMessage(
		hdrHash,
		nil,
		nil,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockHeader),
		0,
		wrongChainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	err := wrk.ProcessReceivedMessage(&mock.P2PMessageMock{DataField: buff}, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.True(t, errors.Is(err, spos.ErrInvalidChainID))
}

func TestWorker_ProcessReceivedMessageWithABadOriginatorShouldErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.SetBlockProcessor(
		&mock.BlockProcessorMock{
			DecodeBlockHeaderCalled: func(dta []byte) data.HeaderHandler {
				return &testscommon.HeaderHandlerStub{
					CheckChainIDCalled: func(reference []byte) error {
						return nil
					},
					GetPrevHashCalled: func() []byte {
						return make([]byte, 0)
					},
				}
			},
			RevertAccountStateCalled: func() {
			},
			DecodeBlockBodyCalled: func(dta []byte) data.BodyHandler {
				return nil
			},
		},
	)

	hdr := &block.Header{ChainID: chainID}
	hdrHash, _ := core.CalculateHash(mock.MarshalizerMock{}, mock.HasherMock{}, hdr)
	hdrStr, _ := mock.MarshalizerMock{}.Marshal(hdr)
	cnsMsg := consensus.NewConsensusMessage(
		hdrHash,
		nil,
		nil,
		hdrStr,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockHeader),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: "other originator",
	}
	err := wrk.ProcessReceivedMessage(msg, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(wrk.ReceivedMessages()[bls.MtBlockHeader]))
	assert.True(t, errors.Is(err, spos.ErrOriginatorMismatch))
}

func TestWorker_ProcessReceivedMessageOkValsShouldWork(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.SetBlockProcessor(
		&mock.BlockProcessorMock{
			DecodeBlockHeaderCalled: func(dta []byte) data.HeaderHandler {
				return &testscommon.HeaderHandlerStub{
					CheckChainIDCalled: func(reference []byte) error {
						return nil
					},
					GetPrevHashCalled: func() []byte {
						return make([]byte, 0)
					},
				}
			},
			RevertAccountStateCalled: func() {
			},
			DecodeBlockBodyCalled: func(dta []byte) data.BodyHandler {
				return nil
			},
		},
	)

	hdr := &block.Header{ChainID: chainID}
	hdrHash, _ := core.CalculateHash(mock.MarshalizerMock{}, mock.HasherMock{}, hdr)
	hdrStr, _ := mock.MarshalizerMock{}.Marshal(hdr)
	cnsMsg := consensus.NewConsensusMessage(
		hdrHash,
		nil,
		nil,
		hdrStr,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockHeader),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: currentPid,
	}
	err := wrk.ProcessReceivedMessage(msg, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, 1, len(wrk.ReceivedMessages()[bls.MtBlockHeader]))
	assert.Nil(t, err)
}

func TestWorker_CheckSelfStateShouldErrMessageFromItself(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		nil,
		nil,
		[]byte(wrk.ConsensusState().SelfPubKey()),
		nil,
		0,
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	err := wrk.CheckSelfState(cnsMsg)
	assert.Equal(t, spos.ErrMessageFromItself, err)
}

func TestWorker_CheckSelfStateShouldErrRoundCanceled(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.ConsensusState().RoundCanceled = true
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		nil,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		nil,
		0,
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	err := wrk.CheckSelfState(cnsMsg)
	assert.Equal(t, spos.ErrRoundCanceled, err)
}

func TestWorker_CheckSelfStateShouldNotErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		nil,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		nil,
		0,
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	err := wrk.CheckSelfState(cnsMsg)
	assert.Nil(t, err)
}

func TestWorker_CheckSignatureShouldReturnNilErr(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	err := wrk.CheckSignature(cnsMsg)

	assert.Nil(t, err)
}

func TestWorker_ExecuteMessagesShouldNotExecuteWhenConsensusDataIsNil(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	wrk.InitReceivedMessages()
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	msgType := consensus.MessageType(cnsMsg.MsgType)
	cnsDataList := wrk.ReceivedMessages()[msgType]
	cnsDataList = append(cnsDataList, nil)
	wrk.SetReceivedMessages(msgType, cnsDataList)
	wrk.ExecuteMessage(cnsDataList)

	assert.Nil(t, wrk.ReceivedMessages()[msgType][0])
}

func TestWorker_ExecuteMessagesShouldNotExecuteWhenMessageIsForOtherRound(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	wrk.InitReceivedMessages()
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		-1,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	msgType := consensus.MessageType(cnsMsg.MsgType)
	cnsDataList := wrk.ReceivedMessages()[msgType]
	cnsDataList = append(cnsDataList, cnsMsg)
	wrk.SetReceivedMessages(msgType, cnsDataList)
	wrk.ExecuteMessage(cnsDataList)

	assert.NotNil(t, wrk.ReceivedMessages()[msgType][0])
}

func TestWorker_ExecuteBlockBodyMessagesShouldNotExecuteWhenStartRoundIsNotFinished(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	wrk.InitReceivedMessages()
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	msgType := consensus.MessageType(cnsMsg.MsgType)
	cnsDataList := wrk.ReceivedMessages()[msgType]
	cnsDataList = append(cnsDataList, cnsMsg)
	wrk.SetReceivedMessages(msgType, cnsDataList)
	wrk.ExecuteMessage(cnsDataList)

	assert.NotNil(t, wrk.ReceivedMessages()[msgType][0])
}

func TestWorker_ExecuteBlockHeaderMessagesShouldNotExecuteWhenStartRoundIsNotFinished(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	wrk.InitReceivedMessages()
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockHeader),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	msgType := consensus.MessageType(cnsMsg.MsgType)
	cnsDataList := wrk.ReceivedMessages()[msgType]
	cnsDataList = append(cnsDataList, cnsMsg)
	wrk.SetReceivedMessages(msgType, cnsDataList)
	wrk.ExecuteMessage(cnsDataList)

	assert.NotNil(t, wrk.ReceivedMessages()[msgType][0])
}

func TestWorker_ExecuteSignatureMessagesShouldNotExecuteWhenBlockIsNotFinished(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	wrk.InitReceivedMessages()
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtSignature),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	msgType := consensus.MessageType(cnsMsg.MsgType)
	cnsDataList := wrk.ReceivedMessages()[msgType]
	cnsDataList = append(cnsDataList, cnsMsg)
	wrk.SetReceivedMessages(msgType, cnsDataList)
	wrk.ExecuteMessage(cnsDataList)

	assert.NotNil(t, wrk.ReceivedMessages()[msgType][0])
}

func TestWorker_ExecuteMessagesShouldExecute(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.StartWorking()
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	wrk.InitReceivedMessages()
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	msgType := consensus.MessageType(cnsMsg.MsgType)
	cnsDataList := wrk.ReceivedMessages()[msgType]
	cnsDataList = append(cnsDataList, cnsMsg)
	wrk.SetReceivedMessages(msgType, cnsDataList)
	wrk.ConsensusState().SetStatus(bls.SrStartRound, spos.SsFinished)
	wrk.ExecuteMessage(cnsDataList)

	assert.Nil(t, wrk.ReceivedMessages()[msgType][0])

	_ = wrk.Close()
}

func TestWorker_CheckChannelsShouldWork(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.StartWorking()
	wrk.SetReceivedMessagesCalls(bls.MtBlockHeader, func(cnsMsg *consensus.Message) bool {
		_ = wrk.ConsensusState().SetJobDone(wrk.ConsensusState().ConsensusGroup()[0], bls.SrBlock, true)
		return true
	})
	rnd := wrk.RoundHandler()
	roundDuration := rnd.TimeDuration()
	rnd.UpdateRound(time.Now(), time.Now().Add(roundDuration))
	cnsGroup := wrk.ConsensusState().ConsensusGroup()
	hdr := &block.Header{}
	hdr.Nonce = 1
	hdr.TimeStamp = uint64(wrk.RoundHandler().TimeStamp().Unix())
	hdrStr, _ := mock.MarshalizerMock{}.Marshal(hdr)
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		nil,
		hdrStr,
		[]byte(cnsGroup[0]),
		[]byte("sig"),
		int(bls.MtBlockHeader),
		1,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	wrk.ExecuteMessageChannel() <- cnsMsg
	time.Sleep(1000 * time.Millisecond)
	isBlockJobDone, err := wrk.ConsensusState().JobDone(cnsGroup[0], bls.SrBlock)

	assert.Nil(t, err)
	assert.True(t, isBlockJobDone)

	_ = wrk.Close()
}

func TestWorker_ExtendShouldReturnWhenRoundIsCanceled(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	executed := false
	bootstrapperMock := &mock.BootstrapperMock{
		GetNodeStateCalled: func() core.NodeState {
			return core.NsNotSynchronized
		},
		CreateAndCommitEmptyBlockCalled: func(shardForCurrentNode uint32) (data.BodyHandler, data.HeaderHandler, error) {
			executed = true
			return nil, nil, errors.New("error")
		},
	}
	wrk.SetBootstrapper(bootstrapperMock)
	wrk.ConsensusState().RoundCanceled = true
	wrk.Extend(0)

	assert.False(t, executed)
}

func TestWorker_ExtendShouldReturnWhenGetNodeStateNotReturnSynchronized(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	executed := false
	bootstrapperMock := &mock.BootstrapperMock{
		GetNodeStateCalled: func() core.NodeState {
			return core.NsNotSynchronized
		},
		CreateAndCommitEmptyBlockCalled: func(shardForCurrentNode uint32) (data.BodyHandler, data.HeaderHandler, error) {
			executed = true
			return nil, nil, errors.New("error")
		},
	}
	wrk.SetBootstrapper(bootstrapperMock)
	wrk.Extend(0)

	assert.False(t, executed)
}

func TestWorker_ExtendShouldReturnWhenCreateEmptyBlockFail(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	executed := false
	bmm := &mock.BroadcastMessengerMock{
		BroadcastBlockCalled: func(handler data.BodyHandler, handler2 data.HeaderHandler) error {
			executed = true
			return nil
		},
	}
	wrk.SetBroadcastMessenger(bmm)
	bootstrapperMock := &mock.BootstrapperMock{
		CreateAndCommitEmptyBlockCalled: func(shardForCurrentNode uint32) (data.BodyHandler, data.HeaderHandler, error) {
			return nil, nil, errors.New("error")
		}}
	wrk.SetBootstrapper(bootstrapperMock)
	wrk.Extend(0)

	assert.False(t, executed)
}

func TestWorker_ExtendShouldWorkAfterAWhile(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	executed := int32(0)
	blockProcessor := &mock.BlockProcessorMock{
		RevertAccountStateCalled: func() {
			atomic.AddInt32(&executed, 1)
		},
	}
	wrk.SetBlockProcessor(blockProcessor)
	wrk.ConsensusState().SetProcessingBlock(true)
	n := 10
	go func() {
		for n > 0 {
			time.Sleep(100 * time.Millisecond)
			n--
		}
		wrk.ConsensusState().SetProcessingBlock(false)
	}()
	wrk.Extend(1)

	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
	assert.Equal(t, 0, n)
}

func TestWorker_ExtendShouldWork(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	executed := int32(0)
	blockProcessor := &mock.BlockProcessorMock{
		RevertAccountStateCalled: func() {
			atomic.AddInt32(&executed, 1)
		},
	}
	wrk.SetBlockProcessor(blockProcessor)
	wrk.Extend(1)
	time.Sleep(1000 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&executed))
}

func TestWorker_ExecuteStoredMessagesShouldWork(t *testing.T) {
	t.Parallel()
	wrk := *initWorker(&mock.AppStatusHandlerStub{})
	wrk.StartWorking()
	blk := &block.Body{}
	blkStr, _ := mock.MarshalizerMock{}.Marshal(blk)
	wrk.InitReceivedMessages()
	cnsMsg := consensus.NewConsensusMessage(
		nil,
		nil,
		blkStr,
		nil,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		[]byte("sig"),
		int(bls.MtBlockBody),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	msgType := consensus.MessageType(cnsMsg.MsgType)
	cnsDataList := wrk.ReceivedMessages()[msgType]
	cnsDataList = append(cnsDataList, cnsMsg)
	wrk.SetReceivedMessages(msgType, cnsDataList)
	wrk.ConsensusState().SetStatus(bls.SrStartRound, spos.SsFinished)

	rcvMsg := wrk.ReceivedMessages()
	assert.Equal(t, 1, len(rcvMsg[msgType]))

	wrk.ExecuteStoredMessages()

	rcvMsg = wrk.ReceivedMessages()
	assert.Equal(t, 0, len(rcvMsg[msgType]))

	_ = wrk.Close()
}

func TestWorker_AppStatusHandlerNilShouldErr(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(nil)
	_, err := spos.NewWorker(workerArgs)

	assert.Equal(t, spos.ErrNilAppStatusHandler, err)
}

func TestWorker_ProcessReceivedMessageWrongHeaderShouldErr(t *testing.T) {
	t.Parallel()

	workerArgs := createDefaultWorkerArgs(&mock.AppStatusHandlerStub{})
	headerSigVerifier := &mock.HeaderSigVerifierStub{}
	headerSigVerifier.VerifyRandSeedCalled = func(header data.HeaderHandler) error {
		return process.ErrRandSeedDoesNotMatch
	}

	workerArgs.HeaderSigVerifier = headerSigVerifier
	wrk, _ := spos.NewWorker(workerArgs)

	hdr := &block.Header{}
	hdr.Nonce = 1
	hdr.TimeStamp = uint64(wrk.RoundHandler().TimeStamp().Unix())
	hdrStr, _ := mock.MarshalizerMock{}.Marshal(hdr)
	hdrHash := mock.HasherMock{}.Compute(string(hdrStr))
	cnsMsg := consensus.NewConsensusMessage(
		hdrHash,
		nil,
		nil,
		hdrStr,
		[]byte(wrk.ConsensusState().ConsensusGroup()[0]),
		signature,
		int(bls.MtBlockHeader),
		0,
		chainID,
		nil,
		nil,
		nil,
		currentPid,
	)
	buff, _ := wrk.Marshalizer().Marshal(cnsMsg)
	time.Sleep(time.Second)
	msg := &mock.P2PMessageMock{
		DataField: buff,
		PeerField: currentPid,
	}
	err := wrk.ProcessReceivedMessage(msg, "")
	assert.True(t, errors.Is(err, spos.ErrInvalidHeader))
}
