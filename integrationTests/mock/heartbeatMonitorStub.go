package mock

import (
	"github.com/ElrondNetwork/elrond-go/core"
	heartbeatData "github.com/ElrondNetwork/elrond-go/heartbeat/data"
	"github.com/ElrondNetwork/elrond-go/p2p"
)

// HeartbeatMonitorStub -
type HeartbeatMonitorStub struct {
	SetAppStatusHandlerCalled    func(ash core.AppStatusHandler) error
	ProcessReceivedMessageCalled func(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error
	GetHeartbeatsCalled          func() []heartbeatData.PubKeyHeartbeat
}

// SetAppStatusHandler -
func (hbms *HeartbeatMonitorStub) SetAppStatusHandler(ash core.AppStatusHandler) error {
	if hbms.SetAppStatusHandlerCalled != nil {
		return hbms.SetAppStatusHandlerCalled(ash)
	}
	return nil
}

// ProcessReceivedMessage -
func (hbms *HeartbeatMonitorStub) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	if hbms.ProcessReceivedMessageCalled != nil {
		return hbms.ProcessReceivedMessageCalled(message, fromConnectedPeer)
	}

	return nil
}

// GetHeartbeats -
func (hbms *HeartbeatMonitorStub) GetHeartbeats() []heartbeatData.PubKeyHeartbeat {
	if hbms.GetHeartbeatsCalled != nil {
		return hbms.GetHeartbeatsCalled()
	}
	return nil
}

// IsInterfaceNil -
func (hbms *HeartbeatMonitorStub) IsInterfaceNil() bool {
	return hbms == nil
}
