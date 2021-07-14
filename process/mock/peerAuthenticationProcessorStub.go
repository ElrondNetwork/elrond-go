package mock

import "github.com/ElrondNetwork/elrond-go/process"

// PeerAuthenticationProcessorStub -
type PeerAuthenticationProcessorStub struct {
	ProcessCalled func(peerHeartbeat process.InterceptedPeerAuthentication) error
}

// Process -
func (proc *PeerAuthenticationProcessorStub) Process(peerHeartbeat process.InterceptedPeerAuthentication) error {
	if proc.ProcessCalled != nil {
		return proc.ProcessCalled(peerHeartbeat)
	}

	return nil
}

// IsInterfaceNil -
func (proc *PeerAuthenticationProcessorStub) IsInterfaceNil() bool {
	return proc == nil
}
