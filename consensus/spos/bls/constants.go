package bls

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus"
)

const (
	// SrStartRound defines ID of Subround "Start round"
	SrStartRound = iota
	// SrBlock defines ID of Subround "block"
	SrBlock
	// SrSignature defines ID of Subround "signature"
	SrSignature
	// SrEndRound defines ID of Subround "End round"
	SrEndRound
)

const (
	// MtUnknown defines ID of a message that has unknown Data inside
	MtUnknown consensus.MessageType = iota
	// MtBlockBody defines ID of a message that has a block body inside
	MtBlockBody
	// MtBlockHeader defines ID of a message that has a block header inside
	MtBlockHeader
	// MtSignature defines ID of a message that has a Signature inside
	MtSignature
)

// syncThesholdPercent sepcifies the max allocated time to syncronize as a percentage of the total time of the round
const syncThresholdPercent = 50

// processingThresholdPercent specifies the max allocated time for processing the block as a percentage of the total time of the round
const processingThresholdPercent = 65

// srStartStartTime specifies the start time, from the total time of the round, of Subround Start
const srStartStartTime = 0.0

// srEndStartTime specifies the end time, from the total time of the round, of Subround Start
const srStartEndTime = 0.05

// srBlockStartTime specifies the start time, from the total time of the round, of Subround Block
const srBlockStartTime = 0.05

// srBlockEndTime specifies the end time, from the total time of the round, of Subround Block
const srBlockEndTime = 0.25

// srSignatureStartTime specifies the start time, from the total time of the round, of Subround Signature
const srSignatureStartTime = 0.25

// srSignatureEndTime specifies the end time, from the total time of the round, of Subround Signature
const srSignatureEndTime = 0.65

// srEndStartTime specifies the start time, from the total time of the round, of Subround End
const srEndStartTime = 0.65

// srEndEndTime specifies the end time, from the total time of the round, of Subround End
const srEndEndTime = 0.75

func getStringValue(msgType consensus.MessageType) string {
	switch msgType {
	case MtBlockBody:
		return "(BLOCK_BODY)"
	case MtBlockHeader:
		return "(BLOCK_HEADER)"
	case MtSignature:
		return "(SIGNATURE)"
	case MtUnknown:
		return "(UNKNOWN)"
	default:
		return "Undefined message type"
	}
}

// getSubroundName returns the name of each Subround from a given Subround ID
func getSubroundName(subroundId int) string {
	switch subroundId {
	case SrStartRound:
		return "(START_ROUND)"
	case SrBlock:
		return "(BLOCK)"
	case SrSignature:
		return "(SIGNATURE)"
	case SrEndRound:
		return "(END_ROUND)"
	default:
		return "Undefined subround"
	}
}
