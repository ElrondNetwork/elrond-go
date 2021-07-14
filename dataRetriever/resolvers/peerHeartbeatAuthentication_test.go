package resolvers_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/batch"
	"github.com/ElrondNetwork/elrond-go/data/heartbeat"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/mock"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/resolvers"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockArgPeerAuthenticationResolver() resolvers.ArgPeerAuthenticationResolver {
	return resolvers.ArgPeerAuthenticationResolver{
		SenderResolver:         &mock.TopicResolverSenderStub{},
		PeerAuthenticationPool: testscommon.NewCacherStub(),
		Marshalizer:            &mock.MarshalizerMock{},
		AntifloodHandler:       &mock.P2PAntifloodHandlerStub{},
		Throttler:              &mock.ThrottlerStub{},
		DataPacker:             &mock.DataPackerStub{},
	}
}

func TestNewPeerAuthenticationResolverResolver_NilSenderResolverShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	arg.SenderResolver = nil
	res, err := resolvers.NewPeerAuthenticationResolver(arg)

	assert.Equal(t, dataRetriever.ErrNilResolverSender, err)
	assert.True(t, check.IfNil(res))
}

func TestNewPeerAuthenticationResolverResolver_NilPoolShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	arg.PeerAuthenticationPool = nil
	res, err := resolvers.NewPeerAuthenticationResolver(arg)

	assert.Equal(t, dataRetriever.ErrNilPeerAuthenticationPool, err)
	assert.True(t, check.IfNil(res))
}

func TestNewPeerAuthenticationResolverResolver_NilBlockMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	arg.Marshalizer = nil
	res, err := resolvers.NewPeerAuthenticationResolver(arg)

	assert.Equal(t, dataRetriever.ErrNilMarshalizer, err)
	assert.True(t, check.IfNil(res))
}

func TestNewPeerAuthenticationResolverResolver_NilAntifloodHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	arg.AntifloodHandler = nil
	res, err := resolvers.NewPeerAuthenticationResolver(arg)

	assert.Equal(t, dataRetriever.ErrNilAntifloodHandler, err)
	assert.True(t, check.IfNil(res))
}

func TestNewPeerAuthenticationResolverResolver_NilThrottlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	arg.Throttler = nil
	res, err := resolvers.NewPeerAuthenticationResolver(arg)

	assert.Equal(t, dataRetriever.ErrNilThrottler, err)
	assert.True(t, check.IfNil(res))
}

func TestNewPeerAuthenticationResolverResolver_NilDataPackerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	arg.DataPacker = nil
	res, err := resolvers.NewPeerAuthenticationResolver(arg)

	assert.Equal(t, dataRetriever.ErrNilDataPacker, err)
	assert.True(t, check.IfNil(res))
}

func TestNewPeerAuthenticationResolverResolver_OkValsShouldWork(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	res, err := resolvers.NewPeerAuthenticationResolver(arg)

	assert.Nil(t, err)
	assert.False(t, check.IfNil(res))
}

func TestPeerAuthenticationResolverResolver_RequestDataFromPubKeyArrayMarshalErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	arg.Marshalizer.(*mock.MarshalizerMock).Fail = true
	mbRes, err := resolvers.NewPeerAuthenticationResolver(arg)
	assert.Nil(t, err)

	err = mbRes.RequestDataFromPublicKeyArray([][]byte{[]byte("pk1")}, 0)
	require.NotNil(t, err)
}

func TestPeerAuthenticationResolverResolver_RequestDataFromPubKeyArray(t *testing.T) {
	t.Parallel()

	called := false
	arg := createMockArgPeerAuthenticationResolver()
	arg.SenderResolver = &mock.TopicResolverSenderStub{
		SendOnRequestTopicCalled: func(rd *dataRetriever.RequestData, hashes [][]byte) error {
			called = true
			return nil
		},
	}
	mbRes, err := resolvers.NewPeerAuthenticationResolver(arg)
	assert.Nil(t, err)

	err = mbRes.RequestDataFromPublicKeyArray([][]byte{[]byte("pk1")}, 0)
	require.Nil(t, err)
	require.True(t, called)
}

func TestPeerAuthenticationResolverResolver_ProcessReceivedAntifloodErrorsShouldErr(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("expected error")
	arg := createMockArgPeerAuthenticationResolver()
	arg.AntifloodHandler = &mock.P2PAntifloodHandlerStub{
		CanProcessMessageCalled: func(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
			return expectedErr
		},
	}
	mbRes, _ := resolvers.NewPeerAuthenticationResolver(arg)

	err := mbRes.ProcessReceivedMessage(createRequestMsg(dataRetriever.PubkeyArrayType, nil), fromConnectedPeerId)
	assert.True(t, errors.Is(err, expectedErr))
	assert.False(t, arg.Throttler.(*mock.ThrottlerStub).StartWasCalled)
	assert.False(t, arg.Throttler.(*mock.ThrottlerStub).EndWasCalled)
}

func TestPeerAuthenticationResolverResolver_ProcessReceivedMessageNilValueShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	mbRes, _ := resolvers.NewPeerAuthenticationResolver(arg)

	err := mbRes.ProcessReceivedMessage(createRequestMsg(dataRetriever.PubkeyArrayType, nil), fromConnectedPeerId)
	assert.Equal(t, dataRetriever.ErrNilValue, err)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).StartWasCalled)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).EndWasCalled)
}

func TestPeerAuthenticationResolverResolver_ProcessReceivedMessageWrongTypeShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockArgPeerAuthenticationResolver()
	mbRes, _ := resolvers.NewPeerAuthenticationResolver(arg)

	err := mbRes.ProcessReceivedMessage(createRequestMsg(dataRetriever.HashType, make([]byte, 0)), fromConnectedPeerId)

	assert.True(t, errors.Is(err, dataRetriever.ErrRequestTypeNotImplemented))
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).StartWasCalled)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).EndWasCalled)
}

func TestPeerAuthenticationResolverResolver_ProcessReceivedMessageFoundInPoolShouldRetValAndSend(t *testing.T) {
	t.Parallel()

	marshalizer := &mock.MarshalizerMock{}
	pk := []byte("pk1")
	pkList := [][]byte{pk}
	requestedBuff, merr := marshalizer.Marshal(&batch.Batch{Data: pkList})
	require.Nil(t, merr)

	wasResolved := false
	wasSent := false

	cache := testscommon.NewCacherStub()
	cache.PeekCalled = func(key []byte) (value interface{}, ok bool) {
		if bytes.Equal(key, pk) {
			wasResolved = true
			return &heartbeat.PeerAuthentication{}, true
		}

		return nil, false
	}

	arg := createMockArgPeerAuthenticationResolver()
	arg.SenderResolver = &mock.TopicResolverSenderStub{
		SendCalled: func(buff []byte, peer core.PeerID) error {
			wasSent = true
			return nil
		},
	}
	arg.PeerAuthenticationPool = cache
	mbRes, _ := resolvers.NewPeerAuthenticationResolver(arg)

	err := mbRes.ProcessReceivedMessage(
		createRequestMsg(dataRetriever.PubkeyArrayType, requestedBuff),
		fromConnectedPeerId,
	)

	assert.Nil(t, err)
	assert.True(t, wasResolved)
	assert.True(t, wasSent)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).StartWasCalled)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).EndWasCalled)
}

func TestPeerAuthenticationResolverResolver_ProcessReceivedMessageFoundInPoolMarshalizerFailShouldErr(t *testing.T) {
	t.Parallel()

	errExpected := errors.New("expected error")
	goodMarshalizer := &mock.MarshalizerMock{}
	marshalizer := &mock.MarshalizerStub{
		MarshalCalled: func(obj interface{}) (i []byte, e error) {
			return nil, errExpected
		},
		UnmarshalCalled: func(obj interface{}, buff []byte) error {

			return goodMarshalizer.Unmarshal(obj, buff)
		},
	}
	pk := []byte("pk1")
	pkList := [][]byte{pk}
	requestedBuff, merr := goodMarshalizer.Marshal(&batch.Batch{Data: pkList})
	require.Nil(t, merr)

	cache := testscommon.NewCacherStub()
	cache.PeekCalled = func(key []byte) (value interface{}, ok bool) {
		if bytes.Equal(key, pk) {
			return &heartbeat.PeerAuthentication{}, true
		}

		return nil, false
	}

	arg := createMockArgPeerAuthenticationResolver()
	arg.PeerAuthenticationPool = cache
	arg.Marshalizer = marshalizer
	mbRes, _ := resolvers.NewPeerAuthenticationResolver(arg)

	err := mbRes.ProcessReceivedMessage(
		createRequestMsg(dataRetriever.PubkeyArrayType, requestedBuff),
		fromConnectedPeerId,
	)

	assert.True(t, errors.Is(err, errExpected))
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).StartWasCalled)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).EndWasCalled)
}

func TestPeerAuthenticationResolverResolver_ProcessReceivedMessageMissingDataShouldNotSend(t *testing.T) {
	t.Parallel()

	marshalizer := &mock.MarshalizerMock{}
	pk := []byte("pk1")
	pkList := [][]byte{pk}
	requestedBuff, merr := marshalizer.Marshal(&batch.Batch{Data: pkList})
	require.Nil(t, merr)

	wasSent := false

	cache := testscommon.NewCacherStub()
	cache.PeekCalled = func(key []byte) (value interface{}, ok bool) {
		return nil, false
	}

	arg := createMockArgPeerAuthenticationResolver()
	arg.SenderResolver = &mock.TopicResolverSenderStub{
		SendCalled: func(buff []byte, peer core.PeerID) error {
			wasSent = true
			return nil
		},
	}
	arg.PeerAuthenticationPool = cache
	mbRes, _ := resolvers.NewPeerAuthenticationResolver(arg)

	_ = mbRes.ProcessReceivedMessage(
		createRequestMsg(dataRetriever.PubkeyArrayType, requestedBuff),
		fromConnectedPeerId,
	)

	assert.False(t, wasSent)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).StartWasCalled)
	assert.True(t, arg.Throttler.(*mock.ThrottlerStub).EndWasCalled)
}
