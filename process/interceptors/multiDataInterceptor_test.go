package interceptors_test

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/batch"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/interceptors"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/p2pmocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fromConnectedPeerId = core.PeerID("from connected peer Id")

func createMockArgMultiDataInterceptor() interceptors.ArgMultiDataInterceptor {
	argSingle := interceptors.ArgSingleDataInterceptor{
		Topic:                "test topic",
		DataFactory:          &mock.InterceptedDataFactoryStub{},
		Processor:            &mock.InterceptorProcessorStub{},
		Throttler:            createMockThrottler(),
		AntifloodHandler:     &mock.P2PAntifloodHandlerStub{},
		WhiteListRequest:     &testscommon.WhiteListHandlerStub{},
		PreferredPeersHolder: &p2pmocks.PeersHolderStub{},
		CurrentPeerId:        "pid",
	}

	return interceptors.ArgMultiDataInterceptor{
		ArgSingleDataInterceptor: argSingle,
		Marshalizer:              &mock.MarshalizerMock{},
	}
}

func TestNewMultiDataInterceptor_EmptyTopicShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.Topic = ""
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrEmptyTopic, err)
}

func TestNewMultiDataInterceptor_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.Marshalizer = nil
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrNilMarshalizer, err)
}

func TestNewMultiDataInterceptor_NilInterceptedDataFactoryShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = nil
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrNilInterceptedDataFactory, err)
}

func TestNewMultiDataInterceptor_NilInterceptedDataProcessorShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.Processor = nil
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrNilInterceptedDataProcessor, err)
}

func TestNewMultiDataInterceptor_NilInterceptorThrottlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.Throttler = nil
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrNilInterceptorThrottler, err)
}

func TestNewMultiDataInterceptor_NilAntifloodHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.AntifloodHandler = nil
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrNilAntifloodHandler, err)
}

func TestNewMultiDataInterceptor_NilPreferredPeersHolderShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.PreferredPeersHolder = nil
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.Nil(t, mdi)
	assert.Equal(t, process.ErrNilPreferredPeersHolder, err)
}

func TestNewMultiDataInterceptor_NilWhiteListHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.WhiteListRequest = nil
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrNilWhiteListHandler, err)
}

func TestNewMultiDataInterceptor_EmptyPeerIDShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.CurrentPeerId = ""
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	assert.True(t, check.IfNil(mdi))
	assert.Equal(t, process.ErrEmptyPeerID, err)
}

func TestNewMultiDataInterceptor(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	mdi, err := interceptors.NewMultiDataInterceptor(arg)

	require.False(t, check.IfNil(mdi))
	require.Nil(t, err)
	assert.Equal(t, arg.Topic, mdi.Topic())
}

//------- ProcessReceivedMessage

func TestMultiDataInterceptor_ProcessReceivedMessageNilMessageShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	err := mdi.ProcessReceivedMessage(nil, fromConnectedPeerId)

	assert.Equal(t, process.ErrNilMessage, err)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 0)
}

func TestMultiDataInterceptor_ProcessReceivedMessageUnmarshalFailsShouldErr(t *testing.T) {
	t.Parallel()

	errExpeced := errors.New("expected error")
	originatorPid := core.PeerID("originator")
	originatorBlackListed := false
	fromConnectedPeerBlackListed := false
	arg := createMockArgMultiDataInterceptor()
	arg.Marshalizer = &mock.MarshalizerStub{
		UnmarshalCalled: func(obj interface{}, buff []byte) error {
			return errExpeced
		},
	}
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		BlacklistPeerCalled: func(peer core.PeerID, reason string, duration time.Duration) {
			if peer == originatorPid {
				originatorBlackListed = true
			}
			if peer == fromConnectedPeerId {
				fromConnectedPeerBlackListed = true
			}
		},
	}

	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	msg := &mock.P2PMessageMock{
		DataField: []byte("data to be processed"),
		PeerField: originatorPid,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	assert.Equal(t, errExpeced, err)
	assert.True(t, originatorBlackListed)
	assert.True(t, fromConnectedPeerBlackListed)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestMultiDataInterceptor_ProcessReceivedMessageUnmarshalReturnsEmptySliceShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	arg.Marshalizer = &mock.MarshalizerStub{
		UnmarshalCalled: func(obj interface{}, buff []byte) error {
			return nil
		},
	}
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	msg := &mock.P2PMessageMock{
		DataField: []byte("data to be processed"),
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	assert.Equal(t, process.ErrNoDataInMessage, err)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestMultiDataInterceptor_ProcessReceivedCreateFailsShouldErr(t *testing.T) {
	t.Parallel()

	buffData := [][]byte{[]byte("buff1"), []byte("buff2")}

	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	errExpected := errors.New("expected err")
	originatorPid := core.PeerID("originator")
	originatorBlackListed := false
	fromConnectedPeerBlackListed := false
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return nil, errExpected
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		BlacklistPeerCalled: func(peer core.PeerID, reason string, duration time.Duration) {
			if peer == originatorPid {
				originatorBlackListed = true
			}
			if peer == fromConnectedPeerId {
				fromConnectedPeerBlackListed = true
			}
		},
	}
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	dataField, _ := arg.Marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
		PeerField: originatorPid,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Equal(t, errExpected, err)
	assert.Equal(t, int32(0), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(0), atomic.LoadInt32(&processCalledNum))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	assert.True(t, originatorBlackListed)
	assert.True(t, fromConnectedPeerBlackListed)
}

func TestMultiDataInterceptor_ProcessReceivedPartiallyCorrectDataShouldErr(t *testing.T) {
	t.Parallel()

	correctData := []byte("buff1")
	incorrectData := []byte("buff2")
	buffData := [][]byte{incorrectData, correctData}

	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	errExpected := errors.New("expected err")
	interceptedData := &testscommon.InterceptedDataStub{
		CheckValidityCalled: func() error {
			return nil
		},
		IsForCurrentShardCalled: func() bool {
			return true
		},
	}
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			if bytes.Equal(buff, incorrectData) {
				return nil, errExpected
			}

			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	dataField, _ := arg.Marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Equal(t, errExpected, err)
	assert.Equal(t, int32(0), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(0), atomic.LoadInt32(&processCalledNum))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestMultiDataInterceptor_ProcessReceivedMessageNotValidShouldErrAndNotProcess(t *testing.T) {
	t.Parallel()

	errExpected := errors.New("expected err")
	testProcessReceiveMessageMultiData(t, true, errExpected, 0)
}

func TestMultiDataInterceptor_ProcessReceivedMessageIsAddressedToOtherShardShouldRetNilAndNotProcess(t *testing.T) {
	t.Parallel()

	testProcessReceiveMessageMultiData(t, false, process.ErrInterceptedDataNotForCurrentShard, 0)
}

func TestMultiDataInterceptor_ProcessReceivedMessageOkMessageShouldRetNil(t *testing.T) {
	t.Parallel()

	testProcessReceiveMessageMultiData(t, true, nil, 2)
}

func testProcessReceiveMessageMultiData(t *testing.T, isForCurrentShard bool, expectedErr error, calledNum int) {
	buffData := [][]byte{[]byte("buff1"), []byte("buff2")}

	marshalizer := &mock.MarshalizerMock{}
	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	interceptedData := &testscommon.InterceptedDataStub{
		CheckValidityCalled: func() error {
			return expectedErr
		},
		IsForCurrentShardCalled: func() bool {
			return isForCurrentShard
		},
	}
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	dataField, _ := marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Equal(t, expectedErr, err)
	assert.Equal(t, int32(calledNum), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(calledNum), atomic.LoadInt32(&processCalledNum))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestMultiDataInterceptor_ProcessReceivedMessageCheckBatchErrors(t *testing.T) {
	buffData := [][]byte{[]byte("buff1"), []byte("buff2")}

	marshalizer := &mock.MarshalizerMock{}
	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	throttler := createMockThrottler()
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			assert.Fail(t, "should have not called create intercepted data")
			return nil, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)
	expectedErr := errors.New("expected error")
	_ = mdi.SetChunkProcessor(
		&mock.ChunkProcessorStub{
			CheckBatchCalled: func(b *batch.Batch, w process.WhiteListHandler) (process.CheckedChunkResult, error) {
				return process.CheckedChunkResult{}, expectedErr
			},
		},
	)

	dataField, _ := marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Equal(t, expectedErr, err)
	assert.Equal(t, int32(1), throttler.StartProcessingCount())
	assert.Equal(t, int32(1), throttler.EndProcessingCount())
}

func TestMultiDataInterceptor_ProcessReceivedMessageCheckBatchIsIncomplete(t *testing.T) {
	buffData := [][]byte{[]byte("buff1"), []byte("buff2")}

	marshalizer := &mock.MarshalizerMock{}
	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	throttler := createMockThrottler()
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			assert.Fail(t, "should have not called create intercepted data")
			return nil, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)
	_ = mdi.SetChunkProcessor(
		&mock.ChunkProcessorStub{
			CheckBatchCalled: func(b *batch.Batch, w process.WhiteListHandler) (process.CheckedChunkResult, error) {
				return process.CheckedChunkResult{
					IsChunk:        true,
					HaveAllChunks:  false,
					CompleteBuffer: nil,
				}, nil
			},
		},
	)

	dataField, _ := marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Nil(t, err)
	assert.Equal(t, int32(1), throttler.StartProcessingCount())
	assert.Equal(t, int32(1), throttler.EndProcessingCount())
}

func TestMultiDataInterceptor_ProcessReceivedMessageCheckBatchIsComplete(t *testing.T) {
	buffData := [][]byte{[]byte("buff1")}
	newBuffData := []byte("new buff")

	createCalled := false
	marshalizer := &mock.MarshalizerMock{}
	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	throttler := createMockThrottler()
	arg := createMockArgMultiDataInterceptor()
	interceptedData := &testscommon.InterceptedDataStub{
		CheckValidityCalled: func() error {
			return nil
		},
		IsForCurrentShardCalled: func() bool {
			return true
		},
	}
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			assert.Equal(t, newBuffData, buff) //chunk processor switched the buffer
			createCalled = true
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)
	_ = mdi.SetChunkProcessor(
		&mock.ChunkProcessorStub{
			CheckBatchCalled: func(b *batch.Batch, w process.WhiteListHandler) (process.CheckedChunkResult, error) {
				return process.CheckedChunkResult{
					IsChunk:        true,
					HaveAllChunks:  true,
					CompleteBuffer: newBuffData,
				}, nil
			},
		},
	)

	dataField, _ := marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Nil(t, err)
	assert.True(t, createCalled)
	assert.Equal(t, int32(1), throttler.StartProcessingCount())
	assert.Equal(t, int32(1), throttler.EndProcessingCount())
}

func TestMultiDataInterceptor_ProcessReceivedMessageWhitelistedShouldRetNil(t *testing.T) {
	t.Parallel()

	buffData := [][]byte{[]byte("buff1"), []byte("buff2")}

	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	throttler := createMockThrottler()
	interceptedData := &testscommon.InterceptedDataStub{
		CheckValidityCalled: func() error {
			return nil
		},
		IsForCurrentShardCalled: func() bool {
			return false
		},
	}
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
	arg.WhiteListRequest = &testscommon.WhiteListHandlerStub{
		IsWhiteListedCalled: func(interceptedData process.InterceptedData) bool {
			return true
		},
	}
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	dataField, _ := arg.Marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Nil(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(2), atomic.LoadInt32(&processCalledNum))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestMultiDataInterceptor_InvalidTxVersionShouldBackList(t *testing.T) {
	t.Parallel()

	processReceivedMessageMultiDataInvalidVersion(t, process.ErrInvalidTransactionVersion)
}

func TestMultiDataInterceptor_InvalidTxChainIDShouldBackList(t *testing.T) {
	t.Parallel()

	processReceivedMessageMultiDataInvalidVersion(t, process.ErrInvalidChainID)
}

func processReceivedMessageMultiDataInvalidVersion(t *testing.T, expectedErr error) {
	buffData := [][]byte{[]byte("buff1"), []byte("buff2")}
	marshalizer := &mock.MarshalizerMock{}
	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	interceptedData := &testscommon.InterceptedDataStub{
		CheckValidityCalled: func() error {
			return expectedErr
		},
		IsForCurrentShardCalled: func() bool {
			return false
		},
	}

	isOriginatorBlackListed := false
	isFromConnectedPeerBlackListed := false
	originator := core.PeerID("originator")
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		BlacklistPeerCalled: func(peer core.PeerID, reason string, duration time.Duration) {
			switch string(peer) {
			case string(originator):
				isOriginatorBlackListed = true
			case string(fromConnectedPeerId):
				isFromConnectedPeerBlackListed = true
			}
		},
	}
	arg.WhiteListRequest = &testscommon.WhiteListHandlerStub{
		IsWhiteListedCalled: func(interceptedData process.InterceptedData) bool {
			return true
		},
	}
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	dataField, _ := marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
		PeerField: originator,
	}

	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)
	assert.Equal(t, expectedErr, err)
	assert.True(t, isFromConnectedPeerBlackListed)
	assert.True(t, isOriginatorBlackListed)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

//------- debug

func TestMultiDataInterceptor_SetInterceptedDebugHandlerNilShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	err := mdi.SetInterceptedDebugHandler(nil)

	assert.Equal(t, process.ErrNilDebugger, err)
}

func TestMultiDataInterceptor_SetInterceptedDebugHandlerShouldWork(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	debugger := &mock.InterceptedDebugHandlerStub{}
	err := mdi.SetInterceptedDebugHandler(debugger)

	assert.Nil(t, err)
	assert.True(t, debugger == mdi.InterceptedDebugHandler()) //pointer testing
}

func TestMultiDataInterceptor_ProcessReceivedMessageIsOriginatorNotOkButWhiteListed(t *testing.T) {
	t.Parallel()

	buffData := [][]byte{[]byte("buff1"), []byte("buff2")}

	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	throttler := createMockThrottler()
	interceptedData := &testscommon.InterceptedDataStub{
		CheckValidityCalled: func() error {
			return nil
		},
		IsForCurrentShardCalled: func() bool {
			return false
		},
	}

	whiteListHandler := &testscommon.WhiteListHandlerStub{
		IsWhiteListedCalled: func(interceptedData process.InterceptedData) bool {
			return true
		},
	}
	errOriginator := process.ErrOnlyValidatorsCanUseThisTopic
	arg := createMockArgMultiDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		IsOriginatorEligibleForTopicCalled: func(pid core.PeerID, topic string) error {
			return errOriginator
		},
	}
	arg.WhiteListRequest = whiteListHandler
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)

	dataField, _ := arg.Marshalizer.Marshal(&batch.Batch{Data: buffData})
	msg := &mock.P2PMessageMock{
		DataField: dataField,
	}
	err := mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Nil(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(2), atomic.LoadInt32(&processCalledNum))
	assert.Equal(t, int32(1), throttler.StartProcessingCount())
	assert.Equal(t, int32(1), throttler.EndProcessingCount())

	whiteListHandler.IsWhiteListedCalled = func(interceptedData process.InterceptedData) bool {
		return false
	}
	err = mdi.ProcessReceivedMessage(msg, fromConnectedPeerId)
	time.Sleep(time.Second)

	assert.Equal(t, err, errOriginator)
	assert.Equal(t, int32(2), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(2), atomic.LoadInt32(&processCalledNum))
	assert.Equal(t, int32(2), throttler.StartProcessingCount())
	assert.Equal(t, int32(2), throttler.EndProcessingCount())
}

func TestMultiDataInterceptor_RegisterHandler(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	wasCalled := false
	arg.Processor = &mock.InterceptorProcessorStub{
		RegisterHandlerCalled: func(handler func(topic string, hash []byte, data interface{})) {
			wasCalled = true
		},
	}

	mdi, _ := interceptors.NewMultiDataInterceptor(arg)
	mdi.RegisterHandler(nil)

	assert.True(t, wasCalled)
}

func TestMultiDataInterceptor_SetChunkProcessor(t *testing.T) {
	t.Parallel()

	arg := createMockArgMultiDataInterceptor()
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)
	err := mdi.SetChunkProcessor(nil)
	assert.Equal(t, process.ErrNilChunksProcessor, err)

	cps := &mock.ChunkProcessorStub{}
	err = mdi.SetChunkProcessor(cps)
	assert.Nil(t, err)
	assert.Equal(t, cps, mdi.ChunksProcessor())
}

func TestMultiDataInterceptor_Close(t *testing.T) {
	t.Parallel()

	closeCalled := false
	arg := createMockArgMultiDataInterceptor()
	mdi, _ := interceptors.NewMultiDataInterceptor(arg)
	cps := &mock.ChunkProcessorStub{
		CloseCalled: func() error {
			closeCalled = true
			return nil
		},
	}
	_ = mdi.SetChunkProcessor(cps)

	err := mdi.Close()

	assert.Nil(t, err)
	assert.True(t, closeCalled)
}
