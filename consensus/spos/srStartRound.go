package spos

import (
	"fmt"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/chronology"
)

type SRStartRound struct {
	doLog   bool
	endTime int64
	cns     *Consensus
}

func NewSRStartRound(doLog bool, endTime int64, cns *Consensus) SRStartRound {
	sr := SRStartRound{doLog: doLog, endTime: endTime, cns: cns}
	return sr
}

func (sr *SRStartRound) DoWork() bool {
	for sr.cns.chr.GetSelfSubRound() != chronology.SR_ABORDED {
		time.Sleep(SLEEP_TIME * time.Millisecond)
		switch sr.doStartRound() {
		case R_None:
			continue
		case R_False:
			return false
		case R_True:
			return true
		default:
			return false
		}
	}

	sr.Log(fmt.Sprintf(sr.cns.chr.GetFormatedCurrentTime()+"Step 0: Aborded round %d in subround %s", sr.cns.chr.GetRoundIndex(), sr.Name()))
	return false
}

func (sr *SRStartRound) doStartRound() Response {
	sr.Log(fmt.Sprintf(sr.cns.chr.GetFormatedCurrentTime() + "Step 0: Preparing for this round"))

	sr.cns.Block.ResetBlock()
	sr.cns.ResetRoundStatus()
	sr.cns.ResetValidationMap()

	return R_True
}

func (sr *SRStartRound) Current() chronology.Subround {
	return chronology.Subround(SR_START_ROUND)
}

func (sr *SRStartRound) Next() chronology.Subround {
	return chronology.Subround(SR_BLOCK)
}

func (sr *SRStartRound) EndTime() int64 {
	return int64(sr.endTime)
}

func (sr *SRStartRound) Name() string {
	return "<START_ROUND>"
}

func (sr *SRStartRound) Log(message string) {
	if sr.doLog {
		fmt.Printf(message + "\n")
	}
}
