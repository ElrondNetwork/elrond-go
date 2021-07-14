package interceptors_test

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/batch"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/interceptors"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/p2pmocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockArgPeerAuthenticationInterceptor() interceptors.ArgPeerAuthenticationInterceptor {
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

	return interceptors.ArgPeerAuthenticationInterceptor{
		ArgSingleDataInterceptor: argSingle,
		Marshalizer:              &mock.MarshalizerMock{},
		ValidatorChecker:         &mock.NodesCoordinatorMock{},
		AuthenticationProcessor:  &mock.PeerAuthenticationProcessorStub{},
		ObserversThrottler:       createMockThrottler(),
	}
}

func TestNewPeerAuthenticationInterceptor_NilFactoryShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationInterceptor()
	arg.DataFactory = nil
	interceptor, err := interceptors.NewPeerAuthenticationInterceptor(arg)

	assert.True(t, check.IfNil(interceptor))
	assert.Equal(t, process.ErrNilInterceptedDataFactory, err)
}

func TestNewPeerAuthenticationInterceptor_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationInterceptor()
	arg.Marshalizer = nil
	interceptor, err := interceptors.NewPeerAuthenticationInterceptor(arg)

	assert.True(t, check.IfNil(interceptor))
	assert.Equal(t, process.ErrNilMarshalizer, err)
}

func TestNewPeerAuthenticationInterceptor_NilValidatorCheckerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationInterceptor()
	arg.ValidatorChecker = nil
	interceptor, err := interceptors.NewPeerAuthenticationInterceptor(arg)

	assert.True(t, check.IfNil(interceptor))
	assert.Equal(t, process.ErrNilValidatorChecker, err)
}

func TestNewPeerAuthenticationInterceptor_NilHeartbeatProcessorShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationInterceptor()
	arg.AuthenticationProcessor = nil
	interceptor, err := interceptors.NewPeerAuthenticationInterceptor(arg)

	assert.True(t, check.IfNil(interceptor))
	assert.Equal(t, process.ErrNilAuthenticationProcessor, err)
}

func TestNewPeerAuthenticationInterceptor_NilObserversThrottlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationInterceptor()
	arg.ObserversThrottler = nil
	interceptor, err := interceptors.NewPeerAuthenticationInterceptor(arg)

	assert.True(t, check.IfNil(interceptor))
	assert.True(t, errors.Is(err, process.ErrNilInterceptorThrottler))
}

func TestNewPeerAuthenticationInterceptor_ShouldWork(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationInterceptor()
	interceptor, err := interceptors.NewPeerAuthenticationInterceptor(arg)

	assert.False(t, check.IfNil(interceptor))
	assert.Nil(t, err)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessagePreProcessFailShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationInterceptor()
	arg.Throttler = &mock.InterceptorThrottlerStub{
		CanProcessCalled: func() bool {
			return false
		},
	}
	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  []byte("data"),
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	assert.Equal(t, process.ErrSystemBusy, err)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 0)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 0)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessageNotAnInterceptedDataShouldErr(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("expected error")
	arg := createMockArgPeerAuthenticationInterceptor()
	numBlackListedCalled := 0
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (process.InterceptedData, error) {
			return nil, expectedErr
		},
	}
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		BlacklistPeerCalled: func(peer core.PeerID, reason string, duration time.Duration) {
			assert.True(t, strings.Contains(reason, "can not create object from received bytes"))
			numBlackListedCalled++
		},
	}
	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	b := &batch.Batch{
		Data: [][]byte{[]byte("buff1"), []byte("buff2")},
	}
	buffBatch, _ := arg.Marshalizer.Marshal(b)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  buffBatch,
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 2, numBlackListedCalled)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 0)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessageNotAValidInterceptedDataShouldErr(t *testing.T) {
	t.Parallel()

	cause := "intercepted data is not of type process.InterceptedPeerInfo"
	arg := createMockArgPeerAuthenticationInterceptor()
	numBlackListedCalled := 0
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (process.InterceptedData, error) {
			return &testscommon.InterceptedDataStub{}, nil
		},
	}
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		BlacklistPeerCalled: func(peer core.PeerID, reason string, duration time.Duration) {
			assert.True(t, strings.Contains(reason, cause))
			numBlackListedCalled++
		},
	}
	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	b := &batch.Batch{
		Data: [][]byte{[]byte("buff1"), []byte("buff2")},
	}
	buffBatch, _ := arg.Marshalizer.Marshal(b)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  buffBatch,
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	require.NotNil(t, err)
	assert.Equal(t, cause, err.Error())
	assert.Equal(t, 2, numBlackListedCalled)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 0)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessageObserversMoreObserversShouldNotSend(t *testing.T) {
	t.Parallel()

	processCalled := uint32(0)
	arg := createMockArgPeerAuthenticationInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (process.InterceptedData, error) {
			return &testscommon.InterceptedPeerHeartbeatStub{}, nil
		},
	}
	arg.AuthenticationProcessor = &mock.PeerAuthenticationProcessorStub{
		ProcessCalled: func(peerHeartbeat process.InterceptedPeerAuthentication) error {
			atomic.AddUint32(&processCalled, 1)

			return nil
		},
	}
	arg.ValidatorChecker = &mock.NodesCoordinatorMock{
		GetValidatorWithPublicKeyCalled: func(publicKey []byte) (validator sharding.Validator, shardId uint32, err error) {
			return nil, 0, fmt.Errorf("provided peer is an observer")
		},
	}
	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	b := &batch.Batch{
		Data: [][]byte{[]byte("buff1"), []byte("buff2")},
	}
	buffBatch, _ := arg.Marshalizer.Marshal(b)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  buffBatch,
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	require.Equal(t, process.ErrShouldNotBroadcastMessage, err)

	assert.Equal(t, uint32(2), atomic.LoadUint32(&processCalled))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 2)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessageObserversShouldProcessAndBroadcast(t *testing.T) {
	t.Parallel()

	processCalled := uint32(0)
	arg := createMockArgPeerAuthenticationInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (process.InterceptedData, error) {
			return &testscommon.InterceptedPeerHeartbeatStub{}, nil
		},
	}
	arg.AuthenticationProcessor = &mock.PeerAuthenticationProcessorStub{
		ProcessCalled: func(peerHeartbeat process.InterceptedPeerAuthentication) error {
			atomic.AddUint32(&processCalled, 1)

			return nil
		},
	}
	arg.ValidatorChecker = &mock.NodesCoordinatorMock{
		GetValidatorWithPublicKeyCalled: func(publicKey []byte) (validator sharding.Validator, shardId uint32, err error) {
			return nil, 0, fmt.Errorf("provided peer is an observer")
		},
	}
	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	b := &batch.Batch{
		Data: [][]byte{[]byte("buff1")},
	}
	buffBatch, _ := arg.Marshalizer.Marshal(b)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  buffBatch,
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	require.Nil(t, err)

	assert.Equal(t, uint32(1), atomic.LoadUint32(&processCalled))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 1)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessageObserversShouldNotProcess(t *testing.T) {
	t.Parallel()

	processCalled := uint32(0)
	arg := createMockArgPeerAuthenticationInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (process.InterceptedData, error) {
			return &testscommon.InterceptedPeerHeartbeatStub{}, nil
		},
	}
	arg.AuthenticationProcessor = &mock.PeerAuthenticationProcessorStub{
		ProcessCalled: func(peerHeartbeat process.InterceptedPeerAuthentication) error {
			atomic.AddUint32(&processCalled, 1)

			return nil
		},
	}
	arg.ValidatorChecker = &mock.NodesCoordinatorMock{
		GetValidatorWithPublicKeyCalled: func(publicKey []byte) (validator sharding.Validator, shardId uint32, err error) {
			return nil, 0, fmt.Errorf("provided peer is an observer")
		},
	}
	arg.ObserversThrottler = &mock.InterceptorThrottlerStub{
		CanProcessCalled: func() bool {
			return false
		},
	}
	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	b := &batch.Batch{
		Data: [][]byte{[]byte("buff1"), []byte("buff2")},
	}
	buffBatch, _ := arg.Marshalizer.Marshal(b)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  buffBatch,
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	require.Equal(t, process.ErrShouldNotBroadcastMessage, err)

	assert.Equal(t, uint32(0), atomic.LoadUint32(&processCalled))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 0)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessageProcessError(t *testing.T) {
	t.Parallel()

	numBlackListedCalled := 0
	expectedErr := errors.New("expected error")
	cause := "peer info processing error"
	arg := createMockArgPeerAuthenticationInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (process.InterceptedData, error) {
			return &testscommon.InterceptedPeerHeartbeatStub{}, nil
		},
	}
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		BlacklistPeerCalled: func(peer core.PeerID, reason string, duration time.Duration) {
			assert.True(t, strings.Contains(reason, cause))
			numBlackListedCalled++
		},
	}
	arg.AuthenticationProcessor = &mock.PeerAuthenticationProcessorStub{
		ProcessCalled: func(peerHeartbeat process.InterceptedPeerAuthentication) error {
			return expectedErr
		},
	}
	arg.ValidatorChecker = &mock.NodesCoordinatorMock{
		GetValidatorWithPublicKeyCalled: func(publicKey []byte) (validator sharding.Validator, shardId uint32, err error) {
			return nil, 0, nil
		},
	}
	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	b := &batch.Batch{
		Data: [][]byte{[]byte("buff1"), []byte("buff2")},
	}
	buffBatch, _ := arg.Marshalizer.Marshal(b)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  buffBatch,
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	require.Equal(t, expectedErr, err)
	assert.Equal(t, 2, numBlackListedCalled)
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 1)
}

func TestPeerAuthenticationInterceptor_ProcessReceivedMessageValidatorsShouldWork(t *testing.T) {
	t.Parallel()

	processCalled := uint32(0)
	arg := createMockArgPeerAuthenticationInterceptor()
	arg.DataFactory = &mock.InterceptedDataFactoryStub{
		CreateCalled: func(buff []byte) (process.InterceptedData, error) {
			return &testscommon.InterceptedPeerHeartbeatStub{}, nil
		},
	}
	arg.AuthenticationProcessor = &mock.PeerAuthenticationProcessorStub{
		ProcessCalled: func(peerHeartbeat process.InterceptedPeerAuthentication) error {
			atomic.AddUint32(&processCalled, 1)

			return nil
		},
	}
	arg.ValidatorChecker = &mock.NodesCoordinatorMock{
		GetValidatorWithPublicKeyCalled: func(publicKey []byte) (validator sharding.Validator, shardId uint32, err error) {
			return nil, 0, nil //is a validator
		},
	}

	interceptor, _ := interceptors.NewPeerAuthenticationInterceptor(arg)

	b := &batch.Batch{
		Data: [][]byte{[]byte("buff1"), []byte("buff2")},
	}
	buffBatch, _ := arg.Marshalizer.Marshal(b)

	msg := &mock.P2PMessageMock{
		FromField:  []byte("from"),
		DataField:  buffBatch,
		TopicField: arg.Topic,
		PeerField:  "",
	}

	err := interceptor.ProcessReceivedMessage(msg, "")
	require.Equal(t, process.ErrShouldNotBroadcastMessage, err)

	assert.Equal(t, uint32(2), atomic.LoadUint32(&processCalled))
	checkThrottlerNumStartEndCalls(t, arg.Throttler, 1)
	checkThrottlerNumStartEndCalls(t, arg.ObserversThrottler, 2)
}
