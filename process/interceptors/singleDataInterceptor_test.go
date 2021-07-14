package interceptors_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/interceptors"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/p2pmocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockArgSingleDataInterceptor() interceptors.ArgSingleDataInterceptor {
	return interceptors.ArgSingleDataInterceptor{
		Topic:                "test topic",
		DataFactory:          &mock.InterceptedDataFactoryStub{},
		Processor:            &mock.InterceptorProcessorStub{},
		Throttler:            createMockThrottler(),
		AntifloodHandler:     &mock.P2PAntifloodHandlerStub{},
		WhiteListRequest:     &testscommon.WhiteListHandlerStub{},
		PreferredPeersHolder: &p2pmocks.PeersHolderStub{},
		CurrentPeerId:        "pid",
	}
}

func createMockInterceptorStub(checkCalledNum *int32, processCalledNum *int32) process.InterceptorProcessor {
	return &mock.InterceptorProcessorStub{
		ValidateCalled: func(data process.InterceptedData) error {
			if checkCalledNum != nil {
				atomic.AddInt32(checkCalledNum, 1)
			}

			return nil
		},
		SaveCalled: func(data process.InterceptedData) error {
			if processCalledNum != nil {
				atomic.AddInt32(processCalledNum, 1)
			}

			return nil
		},
	}
}

func createMockThrottler() *mock.InterceptorThrottlerStub {
	return &mock.InterceptorThrottlerStub{
		CanProcessCalled: func() bool {
			return true
		},
	}
}

// checkThrottlerNumStartEndCalls will check if the correct number of start and end calls were done upon a throttler
// calling this method is encouraged to be done in tests involving interceptors as to minimize the risk of having bugs
// when considering throttling p2p messages
func checkThrottlerNumStartEndCalls(t *testing.T, throttler process.InterceptorThrottler, numCalls int32) {
	mockThrottler, isMockThrottler := throttler.(*mock.InterceptorThrottlerStub)
	require.True(t, isMockThrottler)
	require.Equal(t, numCalls, mockThrottler.EndProcessingCount())
	require.Equal(t, numCalls, mockThrottler.StartProcessingCount())
}

func TestNewSingleDataInterceptor_EmptyTopicShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.Topic = ""
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrEmptyTopic, err)
}

func TestNewSingleDataInterceptor_NilInterceptedDataFactoryShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.DataFactory = nil
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrNilInterceptedDataFactory, err)
}

func TestNewSingleDataInterceptor_NilInterceptedDataProcessorShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.Processor = nil
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrNilInterceptedDataProcessor, err)
}

func TestNewSingleDataInterceptor_NilInterceptorThrottlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.Throttler = nil
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrNilInterceptorThrottler, err)
}

func TestNewSingleDataInterceptor_NilP2PAntifloodHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.AntifloodHandler = nil
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrNilAntifloodHandler, err)
}

func TestNewSingleDataInterceptor_NilPreferredPeersHolderShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.PreferredPeersHolder = nil
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrNilPreferredPeersHolder, err)
}

func TestNewSingleDataInterceptor_NilWhiteListHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.WhiteListRequest = nil
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrNilWhiteListHandler, err)
}

func TestNewSingleDataInterceptor_EmptyPeerIDShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	arg.CurrentPeerId = ""
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	assert.Nil(t, sdi)
	assert.Equal(t, process.ErrEmptyPeerID, err)
}

func TestNewSingleDataInterceptor(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	sdi, err := interceptors.NewSingleDataInterceptor(arg)

	require.False(t, check.IfNil(sdi))
	require.Nil(t, err)
	assert.Equal(t, arg.Topic, sdi.Topic())
}

//------- ProcessReceivedMessage

func TestSingleDataInterceptor_ProcessReceivedMessageNilMessageShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	err := sdi.ProcessReceivedMessage(nil, fromConnectedPeerId)

	assert.Equal(t, process.ErrNilMessage, err)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 0)
}

func TestSingleDataInterceptor_ProcessReceivedMessageFactoryCreationErrorShouldErr(t *testing.T) {
	t.Parallel()

	errExpected := errors.New("expected error")
	originatorPid := core.PeerID("originator")
	originatorBlackListed := false
	fromConnectedPeerBlackListed := false
	arg := createMockArgSingleDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return nil, errExpected
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
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	msg := &mock.P2PMessageMock{
		DataField: []byte("data to be processed"),
		PeerField: originatorPid,
	}
	err := sdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	assert.Equal(t, errExpected, err)
	assert.True(t, originatorBlackListed)
	assert.True(t, fromConnectedPeerBlackListed)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestSingleDataInterceptor_ProcessReceivedMessageIsNotValidShouldNotCallProcess(t *testing.T) {
	t.Parallel()

	errExpected := errors.New("expected error")
	testProcessReceiveMessage(t, true, errExpected, 0)
}

func TestSingleDataInterceptor_ProcessReceivedMessageIsNotForCurrentShardShouldNotCallProcess(t *testing.T) {
	t.Parallel()

	testProcessReceiveMessage(t, false, nil, 0)
}

func TestSingleDataInterceptor_ProcessReceivedMessageShouldWork(t *testing.T) {
	t.Parallel()

	testProcessReceiveMessage(t, true, nil, 1)
}

func testProcessReceiveMessage(t *testing.T, isForCurrentShard bool, validityErr error, calledNum int) {
	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	throttler := createMockThrottler()
	interceptedData := &testscommon.InterceptedDataStub{
		CheckValidityCalled: func() error {
			return validityErr
		},
		IsForCurrentShardCalled: func() bool {
			return isForCurrentShard
		},
	}

	arg := createMockArgSingleDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	msg := &mock.P2PMessageMock{
		DataField: []byte("data to be processed"),
	}
	err := sdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Equal(t, validityErr, err)
	assert.Equal(t, int32(calledNum), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(calledNum), atomic.LoadInt32(&processCalledNum))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestSingleDataInterceptor_ProcessReceivedMessageWhitelistedShouldWork(t *testing.T) {
	t.Parallel()

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

	arg := createMockArgSingleDataInterceptor()
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
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	msg := &mock.P2PMessageMock{
		DataField: []byte("data to be processed"),
	}
	err := sdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Nil(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(1), atomic.LoadInt32(&processCalledNum))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestSingleDataInterceptor_InvalidTxVersionShouldBlackList(t *testing.T) {
	t.Parallel()

	processReceivedMessageSingleDataInvalidVersion(t, process.ErrInvalidTransactionVersion)
}

func TestSingleDataInterceptor_InvalidTxChainIDShouldBlackList(t *testing.T) {
	t.Parallel()

	processReceivedMessageSingleDataInvalidVersion(t, process.ErrInvalidTransactionVersion)
}

func processReceivedMessageSingleDataInvalidVersion(t *testing.T, expectedErr error) {
	checkCalledNum := int32(0)
	processCalledNum := int32(0)
	throttler := createMockThrottler()
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
	arg := createMockArgSingleDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
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
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	msg := &mock.P2PMessageMock{
		DataField: []byte("data to be processed"),
		PeerField: originator,
	}
	err := sdi.ProcessReceivedMessage(msg, fromConnectedPeerId)
	assert.Equal(t, expectedErr, err)
	assert.True(t, isFromConnectedPeerBlackListed)
	assert.True(t, isOriginatorBlackListed)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
}

func TestSingleDataInterceptor_ProcessReceivedMessageWithOriginator(t *testing.T) {
	t.Parallel()

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
	arg := createMockArgSingleDataInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (data process.InterceptedData, e error) {
			return interceptedData, nil
		},
	}
	arg.Processor = createMockInterceptorStub(&checkCalledNum, &processCalledNum)
	arg.Throttler = throttler
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		IsOriginatorEligibleForTopicCalled: func(pid core.PeerID, topic string) error {
			return process.ErrOnlyValidatorsCanUseThisTopic
		},
	}
	arg.WhiteListRequest = whiteListHandler
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	msg := &mock.P2PMessageMock{
		DataField: []byte("data to be processed"),
	}
	err := sdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Nil(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(1), atomic.LoadInt32(&processCalledNum))
	assert.Equal(t, int32(1), throttler.EndProcessingCount())
	assert.Equal(t, int32(1), throttler.EndProcessingCount())

	whiteListHandler.IsWhiteListedCalled = func(interceptedData process.InterceptedData) bool {
		return false
	}

	err = sdi.ProcessReceivedMessage(msg, fromConnectedPeerId)

	time.Sleep(time.Second)

	assert.Equal(t, err, process.ErrOnlyValidatorsCanUseThisTopic)
	assert.Equal(t, int32(1), atomic.LoadInt32(&checkCalledNum))
	assert.Equal(t, int32(1), atomic.LoadInt32(&processCalledNum))
	assert.Equal(t, int32(2), throttler.EndProcessingCount())
	assert.Equal(t, int32(2), throttler.EndProcessingCount())
}

//------- debug

func TestSingleDataInterceptor_SetInterceptedDebugHandlerNilShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	err := sdi.SetInterceptedDebugHandler(nil)

	assert.Equal(t, process.ErrNilDebugger, err)
}

func TestSingleDataInterceptor_SetInterceptedDebugHandlerShouldWork(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	debugger := &mock.InterceptedDebugHandlerStub{}
	err := sdi.SetInterceptedDebugHandler(debugger)

	assert.Nil(t, err)
	assert.True(t, debugger == sdi.InterceptedDebugHandler()) //pointer testing
}

func TestSingleDataInterceptor_Close(t *testing.T) {
	t.Parallel()

	arg := createMockArgSingleDataInterceptor()
	sdi, _ := interceptors.NewSingleDataInterceptor(arg)

	err := sdi.Close()
	assert.Nil(t, err)
}

//------- IsInterfaceNil

func TestSingleDataInterceptor_IsInterfaceNil(t *testing.T) {
	t.Parallel()

	var sdi *interceptors.SingleDataInterceptor

	assert.True(t, check.IfNil(sdi))
}
