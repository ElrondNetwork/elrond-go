package antiflood_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/process/throttle/antiflood"
	"github.com/ElrondNetwork/elrond-go/process/throttle/antiflood/disabled"
	"github.com/stretchr/testify/assert"
)

const selfPid = core.PeerID("self")

func TestNewP2PAntiflood_NilBlacklistHandlerShouldErr(t *testing.T) {
	t.Parallel()

	afm, err := antiflood.NewP2PAntiflood(
		selfPid,
		nil,
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)
	assert.True(t, check.IfNil(afm))
	assert.True(t, errors.Is(err, process.ErrNilBlackListHandler))
}

func TestNewP2PAntiflood_EmptyFloodPreventerListShouldErr(t *testing.T) {
	t.Parallel()

	afm, err := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
	)
	assert.True(t, check.IfNil(afm))
	assert.True(t, errors.Is(err, process.ErrEmptyFloodPreventerList))
}

func TestNewP2PAntiflood_NilTopicFloodPreventerShouldErr(t *testing.T) {
	t.Parallel()

	afm, err := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		nil,
		&mock.FloodPreventerStub{},
	)
	assert.True(t, check.IfNil(afm))
	assert.True(t, errors.Is(err, process.ErrNilTopicFloodPreventer))
}

func TestNewP2PAntiflood_ShouldWork(t *testing.T) {
	t.Parallel()

	afm, err := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)

	assert.False(t, check.IfNil(afm))
	assert.Nil(t, err)
}

//------- CanProcessMessage

func TestP2PAntiflood_CanProcessMessageNilMessageShouldError(t *testing.T) {
	t.Parallel()

	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)

	err := afm.CanProcessMessage(nil, "connected peer")
	assert.Equal(t, p2p.ErrNilMessage, err)
}

func TestP2PAntiflood_CanNotIncrementFromConnectedPeerShouldError(t *testing.T) {
	t.Parallel()

	messageOriginator := []byte("originator")
	fromConnectedPeer := core.PeerID("from connected peer")
	message := &mock.P2PMessageMock{
		DataField: []byte("data"),
		FromField: messageOriginator,
	}
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{
			IncreaseLoadCalled: func(fromConnectedPid core.PeerID, originator core.PeerID, size uint64) error {
				if fromConnectedPeer != fromConnectedPeer {
					assert.Fail(t, "should have been the connected peer")
				}

				return process.ErrSystemBusy
			},
		},
	)

	err := afm.CanProcessMessage(message, fromConnectedPeer)
	assert.True(t, errors.Is(err, process.ErrSystemBusy))
}

func TestP2PAntiflood_CanNotIncrementMessageOriginatorShouldError(t *testing.T) {
	t.Parallel()

	messageOriginator := []byte("originator")
	fromConnectedPeer := core.PeerID("from connected peer")
	message := &mock.P2PMessageMock{
		DataField: []byte("data"),
		FromField: messageOriginator,
		PeerField: core.PeerID(messageOriginator),
	}
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{
			IncreaseLoadCalled: func(fromConnectedPid core.PeerID, originator core.PeerID, size uint64) error {
				if core.PeerID(messageOriginator) == message.PeerField {
					return process.ErrSystemBusy
				}
				if fromConnectedPeer != fromConnectedPeer {
					return process.ErrSystemBusy
				}

				return nil
			},
		},
	)

	err := afm.CanProcessMessage(message, fromConnectedPeer)
	assert.True(t, errors.Is(err, process.ErrSystemBusy))
}

func TestP2PAntiflood_ShouldWork(t *testing.T) {
	t.Parallel()

	messageOriginator := []byte("originator")
	fromConnectedPeer := core.PeerID("from connected peer")
	message := &mock.P2PMessageMock{
		DataField: []byte("data"),
		PeerField: core.PeerID(messageOriginator),
	}
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{
			IncreaseLoadCalled: func(fromConnectedPid core.PeerID, originator core.PeerID, size uint64) error {
				return nil
			},
		},
	)

	err := afm.CanProcessMessage(message, fromConnectedPeer)
	assert.Nil(t, err)
}

func TestP2PAntiflood_ShouldWorkWithMoreThanOneFlodPreventer(t *testing.T) {
	t.Parallel()

	messageOriginator := []byte("originator")
	fromConnectedPeer := core.PeerID("from connected peer")
	message := &mock.P2PMessageMock{
		DataField: []byte("data"),
		PeerField: core.PeerID(messageOriginator),
	}
	numIncreasedLoads := 0

	fp := &mock.FloodPreventerStub{
		IncreaseLoadCalled: func(fromConnectedPid core.PeerID, originator core.PeerID, size uint64) error {
			numIncreasedLoads++
			return nil
		},
	}

	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		fp,
		fp,
	)

	err := afm.CanProcessMessage(message, fromConnectedPeer)
	assert.Nil(t, err)
	assert.Equal(t, 2, numIncreasedLoads)
}

//------- CanProcessMessagesOnTopic

func TestP2pAntiflood_CanProcessMessagesOnTopicCanNotAccumulateShouldError(t *testing.T) {
	t.Parallel()

	numMessagesCall := uint32(78)
	topicCall := "topic"
	identifierCall := core.PeerID("id")
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{
			IncreaseLoadCalled: func(pid core.PeerID, topic string, numMessages uint32) error {
				if pid == identifierCall && topic == topicCall && numMessages == numMessagesCall {
					return process.ErrSystemBusy
				}

				return nil
			},
		},
		&mock.FloodPreventerStub{},
	)

	err := afm.CanProcessMessagesOnTopic(identifierCall, topicCall, numMessagesCall, 0, nil)

	assert.True(t, errors.Is(err, process.ErrSystemBusy))
}

func TestP2pAntiflood_CanProcessMessagesOnTopicCanAccumulateShouldWork(t *testing.T) {
	t.Parallel()

	numMessagesCall := uint32(78)
	topicCall := "topic"
	identifierCall := core.PeerID("id")
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{
			IncreaseLoadCalled: func(pid core.PeerID, topic string, numMessages uint32) error {
				if pid == identifierCall && topic == topicCall && numMessages == numMessagesCall {
					return nil
				}

				return process.ErrSystemBusy
			},
		},
		&mock.FloodPreventerStub{},
	)

	err := afm.CanProcessMessagesOnTopic(identifierCall, topicCall, numMessagesCall, 0, nil)

	assert.Nil(t, err)
}

func TestP2pAntiflood_CanProcessMessagesOriginatorIsBlacklistedShouldErr(t *testing.T) {
	t.Parallel()

	identifier := core.PeerID("id")
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{
			HasCalled: func(pid core.PeerID) bool {
				return true
			},
		},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{
			IncreaseLoadCalled: func(fromConnectedPid core.PeerID, originator core.PeerID, size uint64) error {
				return nil
			},
		},
	)
	message := &mock.P2PMessageMock{
		DataField: []byte("data"),
		PeerField: identifier,
	}

	err := afm.CanProcessMessage(message, identifier)

	assert.True(t, errors.Is(err, process.ErrOriginatorIsBlacklisted))
}

func TestP2pAntiflood_ResetForTopicSetMaxMessagesShouldWork(t *testing.T) {
	t.Parallel()

	resetTopicCalled := false
	resetTopicParameter := ""
	setMaxMessagesForTopicCalled := false
	setMaxMessagesForTopicParameter1 := ""
	setMaxMessagesForTopicParameter2 := uint32(0)
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{
			ResetForTopicCalled: func(topic string) {
				resetTopicCalled = true
				resetTopicParameter = topic
			},
			SetMaxMessagesForTopicCalled: func(topic string, num uint32) {
				setMaxMessagesForTopicCalled = true
				setMaxMessagesForTopicParameter1 = topic
				setMaxMessagesForTopicParameter2 = num
			},
		},
		&mock.FloodPreventerStub{},
	)

	resetTopic := "reset topic"
	afm.ResetForTopic(resetTopic)
	assert.True(t, resetTopicCalled)
	assert.Equal(t, resetTopic, resetTopicParameter)

	setMaxMessagesForTopic := "set max message for topic"
	setMaxMessagesForTopicNum := uint32(77463)
	afm.SetMaxMessagesForTopic(setMaxMessagesForTopic, setMaxMessagesForTopicNum)
	assert.True(t, setMaxMessagesForTopicCalled)
	assert.Equal(t, setMaxMessagesForTopic, setMaxMessagesForTopicParameter1)
	assert.Equal(t, setMaxMessagesForTopicNum, setMaxMessagesForTopicParameter2)
}

func TestP2pAntiflood_ApplyConsensusSize(t *testing.T) {
	t.Parallel()

	wasCalled := false
	expectedSize := 878264
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{
			ApplyConsensusSizeCalled: func(size int) {
				assert.Equal(t, expectedSize, size)
				wasCalled = true
			},
		},
	)

	afm.ApplyConsensusSize(expectedSize)
	assert.True(t, wasCalled)
}

func TestP2pAntiflood_SetDebuggerNilDebuggerShouldErr(t *testing.T) {
	t.Parallel()

	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)

	err := afm.SetDebugger(nil)
	assert.Equal(t, process.ErrNilDebugger, err)
}

func TestP2pAntiflood_SetDebuggerShouldWork(t *testing.T) {
	t.Parallel()

	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)

	debugger := &disabled.AntifloodDebugger{}
	err := afm.SetDebugger(debugger)

	assert.Nil(t, err)
	assert.True(t, afm.Debugger() == debugger)
}

func TestP2pAntiflood_Close(t *testing.T) {
	t.Parallel()

	numCalls := int32(0)
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)
	_ = afm.SetDebugger(&mock.AntifloodDebuggerStub{
		CloseCalled: func() error {
			atomic.AddInt32(&numCalls, 1)

			return nil
		},
	})

	err := afm.Close()

	assert.Nil(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&numCalls))
}

func TestP2pAntiflood_BlacklistPeerErrShouldDoNothing(t *testing.T) {
	t.Parallel()

	numCalls := int32(0)
	expectedErr := errors.New("expected error")
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{
			AddWithSpanCalled: func(pid core.PeerID, span time.Duration) error {
				atomic.AddInt32(&numCalls, 1)

				return expectedErr
			},
		},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)

	afm.BlacklistPeer("pid", "reason", time.Second)

	assert.Equal(t, int32(1), atomic.LoadInt32(&numCalls))
}

func TestP2pAntiflood_BlacklistPeerShouldWork(t *testing.T) {
	t.Parallel()

	numCalls := int32(0)
	afm, _ := antiflood.NewP2PAntiflood(
		selfPid,
		&mock.PeerBlackListHandlerStub{
			AddWithSpanCalled: func(pid core.PeerID, span time.Duration) error {
				atomic.AddInt32(&numCalls, 1)

				return nil
			},
		},
		&mock.TopicAntiFloodStub{},
		&mock.FloodPreventerStub{},
	)

	afm.BlacklistPeer("pid", "reason", time.Second)

	assert.Equal(t, int32(1), atomic.LoadInt32(&numCalls))
}
