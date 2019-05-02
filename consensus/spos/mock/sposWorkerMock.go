package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus"
	"github.com/ElrondNetwork/elrond-go-sandbox/data"
	"github.com/ElrondNetwork/elrond-go-sandbox/p2p"
)

type SposWorkerMock struct {
	AddReceivedMessageCallCalled func(messageType consensus.MessageType, receivedMessageCall func(cnsDta *consensus.
					Message) bool)
	RemoveAllReceivedMessagesCallsCalled   func()
	ProcessReceivedMessageCalled           func(message p2p.MessageP2P) error
	SendConsensusMessageCalled             func(cnsDta *consensus.Message) bool
	ExtendCalled                           func(subroundId int)
	GetConsensusStateChangedChannelsCalled func() chan bool
	GetBroadcastBlockCalled                func(data.BodyHandler, data.HeaderHandler) error
}

func (sposWorkerMock *SposWorkerMock) AddReceivedMessageCall(messageType consensus.MessageType,
	receivedMessageCall func(cnsDta *consensus.Message) bool) {
	sposWorkerMock.AddReceivedMessageCallCalled(messageType, receivedMessageCall)
}

func (sposWorkerMock *SposWorkerMock) RemoveAllReceivedMessagesCalls() {
	sposWorkerMock.RemoveAllReceivedMessagesCallsCalled()
}

func (sposWorkerMock *SposWorkerMock) ProcessReceivedMessage(message p2p.MessageP2P) error {
	return sposWorkerMock.ProcessReceivedMessageCalled(message)
}

func (sposWorkerMock *SposWorkerMock) SendConsensusMessage(cnsDta *consensus.Message) bool {
	return sposWorkerMock.SendConsensusMessageCalled(cnsDta)
}

func (sposWorkerMock *SposWorkerMock) Extend(subroundId int) {
	sposWorkerMock.ExtendCalled(subroundId)
}

func (sposWorkerMock *SposWorkerMock) GetConsensusStateChangedChannels() chan bool {
	return sposWorkerMock.GetConsensusStateChangedChannelsCalled()
}

func (sposWorkerMock *SposWorkerMock) GetBroadcastBlock(body data.BodyHandler, header data.HeaderHandler) error {
	return sposWorkerMock.GetBroadcastBlockCalled(body, header)
}
