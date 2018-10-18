package spos

import (
	"fmt"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/chronology"
)

type SRSignature struct {
	doLog               bool
	endTime             int64
	cns                 *Consensus
	OnReceivedSignature func(*[]byte, *chronology.Chronology) bool
	OnSendSignature     func(chronology.Subround) bool
}

func NewSRSignature(doLog bool, endTime int64, cns *Consensus, onReceivedSignature func(*[]byte, *chronology.Chronology) bool, onSendSignature func(chronology.Subround) bool) *SRSignature {
	sr := SRSignature{doLog: doLog, endTime: endTime, cns: cns, OnReceivedSignature: onReceivedSignature, OnSendSignature: onSendSignature}
	return &sr
}

func (sr *SRSignature) DoWork(chr *chronology.Chronology) bool {
	for chr.SelfSubround() != chronology.SrCanceled {
		time.Sleep(sleepTime * time.Millisecond)
		switch sr.doSignature(chr) {
		case rNone:
			continue
		case rFalse:
			return false
		case rTrue:
			return true
		default:
			return false
		}
	}

	sr.Log(fmt.Sprintf(chr.SyncTime().FormatedCurrentTime(chr.ClockOffset())+"Step 5: Canceled round %d in subround %s", chr.Round().Index(), sr.Name()))
	return false
}

func (sr *SRSignature) doSignature(chr *chronology.Chronology) Response {
	sr.cns.SetSentMessage(sr.OnSendSignature(chronology.Subround(SrSignature)))

	timeSubRound := chr.GetSubroundFromDateTime(chr.SyncTime().CurrentTime(chr.ClockOffset()))

	if timeSubRound > chronology.Subround(SrSignature) {
		sr.Log(fmt.Sprintf(chr.SyncTime().FormatedCurrentTime(chr.ClockOffset())+"Step 5: Extended the "+sr.Name()+" subround. Got only %d from %d sigantures which are not enough", sr.cns.GetSignaturesCount(), len(sr.cns.ConsensusGroup)))
		sr.cns.RoundStatus.Signature = SsExtended
		return rTrue // Try to give a chance to this round if the necesary signatures will arrive later
	}

	if sr.cns.SentMessage() || sr.cns.ReceivedMessage() {
		sr.cns.SetSentMessage(false)
		sr.cns.SetReceivedMessage(false)
		if ok, n := sr.cns.CheckConsensus(chronology.Subround(SrBlock), chronology.Subround(SrSignature)); ok {
			sr.Log(fmt.Sprintf(chr.SyncTime().FormatedCurrentTime(chr.ClockOffset())+"Step 5: Received %d from %d signatures, which are matching with bitmap and are enough", n, len(sr.cns.ConsensusGroup)))
			return rTrue
		}
	}

	return rNone
}

func (sr *SRSignature) Current() chronology.Subround {
	return chronology.Subround(SrSignature)
}

func (sr *SRSignature) Next() chronology.Subround {
	return chronology.Subround(SrEndRound)
}

func (sr *SRSignature) EndTime() int64 {
	return int64(sr.endTime)
}

func (sr *SRSignature) Name() string {
	return "<SIGNATURE>"
}

func (sr *SRSignature) Log(message string) {
	if sr.doLog {
		fmt.Printf(message + "\n")
	}
}
