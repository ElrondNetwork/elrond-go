package bn

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/logger"
)

var log = logger.DefaultLogger()

// worker defines the data needed by spos to communicate between nodes which are in the validators group
type worker struct {
}

// NewConsensusService creates a new worker object
func NewConsensusService() (*worker, error) {
	wrk := worker{}

	return &wrk, nil
}

func (wrk *worker) InitReceivedMessages() map[consensus.MessageType][]*consensus.Message {

	receivedMessages := make(map[consensus.MessageType][]*consensus.Message)
	for i := MtBlockBody; i <= MtSignature; i++ {
		receivedMessages[i] = make([]*consensus.Message, 0)
	}

	return receivedMessages

}

func (wrk *worker) GetStringValue(messageType consensus.MessageType) string {
	return getStringValue(messageType)
}

func (wrk *worker) GetSubroundName(subroundId int) string {
	return getSubroundName(subroundId)
}
func (wrk *worker) GetMessageRange() []consensus.MessageType {
	v := []consensus.MessageType{}

	for i := MtBlockBody; i <= MtSignature; i++ {
		v = append(v, i)
	}

	return v
}

func (wrk *worker) CanProceed(consensusState *spos.ConsensusState, msgType consensus.MessageType) bool {
	finished := false
	switch msgType {
	case MtBlockBody:
		if consensusState.Status(SrStartRound) != spos.SsFinished {
			return finished
		}
	case MtBlockHeader:
		if consensusState.Status(SrStartRound) != spos.SsFinished {
			return finished
		}
	case MtCommitmentHash:
		if consensusState.Status(SrBlock) != spos.SsFinished {
			return finished
		}
	case MtBitmap:
		if consensusState.Status(SrBlock) != spos.SsFinished {
			return finished
		}
	case MtCommitment:
		if consensusState.Status(SrBitmap) != spos.SsFinished {
			return finished
		}
	case MtSignature:
		if consensusState.Status(SrBitmap) != spos.SsFinished {
			return finished
		}
	}
	return true
}
