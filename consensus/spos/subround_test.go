package spos_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/chronology"
	"github.com/ElrondNetwork/elrond-go-sandbox/chronology/ntp"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos"
	"github.com/stretchr/testify/assert"
)

func InitSubroundImpl() (*chronology.Chronology, *spos.Consensus) {

	genesisTime := time.Now()
	currentTime := genesisTime

	rnd := chronology.NewRound(genesisTime,
		currentTime,
		RoundTimeDuration)

	chr := chronology.NewChronology(true,
		true,
		rnd,
		genesisTime,
		&ntp.LocalTime{})

	vld := spos.NewValidators(nil,
		nil,
		[]string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		"1")

	for i := 0; i < len(vld.ConsensusGroup()); i++ {
		vld.SetAgreement(vld.ConsensusGroup()[i], spos.SrBlock, false)
		vld.SetAgreement(vld.ConsensusGroup()[i], spos.SrCommitmentHash, false)
		vld.SetAgreement(vld.ConsensusGroup()[i], spos.SrBitmap, false)
		vld.SetAgreement(vld.ConsensusGroup()[i], spos.SrCommitment, false)
		vld.SetAgreement(vld.ConsensusGroup()[i], spos.SrSignature, false)
	}

	pbft := 2*len(vld.ConsensusGroup())/3 + 1

	rt := spos.NewRoundThreshold()

	rt.SetThreshold(spos.SrBlock, 1)
	rt.SetThreshold(spos.SrCommitmentHash, pbft)
	rt.SetThreshold(spos.SrBitmap, pbft)
	rt.SetThreshold(spos.SrCommitment, pbft)
	rt.SetThreshold(spos.SrSignature, pbft)

	rs := spos.NewRoundStatus()

	rs.SetStatus(spos.SrBlock, spos.SsNotFinished)
	rs.SetStatus(spos.SrCommitmentHash, spos.SsNotFinished)
	rs.SetStatus(spos.SrBitmap, spos.SsNotFinished)
	rs.SetStatus(spos.SrCommitment, spos.SsNotFinished)
	rs.SetStatus(spos.SrSignature, spos.SsNotFinished)

	cns := spos.NewConsensus(true,
		nil,
		vld,
		rt,
		rs,
		chr)

	return chr, cns
}

func TestSubroundImpl_DoWork1(t *testing.T) {

	fmt.Printf("1: Test the case when consensus is done -> true\n")

	chr, _ := InitSubroundImpl()

	sr := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		DoSubroundJob,
		DoExtendSubround,
		DoCheckConsensusWithSuccess)

	chr.AddSubround(sr)
	r := sr.DoWork(chr)
	assert.Equal(t, true, r)
}

func TestSubroundImpl_DoWork2(t *testing.T) {

	fmt.Printf("2: Test the case when consensus is not done -> false\n")

	chr, _ := InitSubroundImpl()

	sr := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		DoSubroundJob,
		DoExtendSubround,
		DoCheckConsensusWithoutSuccess)

	chr.AddSubround(sr)
	chr.SetSelfSubround(-1)
	r := sr.DoWork(chr)
	assert.Equal(t, false, r)
}

func TestSubroundImpl_DoWork3(t *testing.T) {

	fmt.Printf("3: Test the case when time has expired -> true\n")

	chr, _ := InitSubroundImpl()

	sr1 := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		DoSubroundJob,
		DoExtendSubround,
		DoCheckConsensusWithoutSuccess)

	chr.AddSubround(sr1)

	sr2 := spos.NewSubroundImpl(chronology.Subround(spos.SrCommitmentHash),
		chronology.Subround(spos.SrBitmap),
		int64(40*RoundTimeDuration/100),
		"<COMMITMENT_HASH>",
		DoSubroundJob,
		DoExtendSubround,
		DoCheckConsensusWithoutSuccess)

	chr.AddSubround(sr2)

	chr.SetClockOffset(time.Duration(sr1.EndTime() + 1))
	r := sr1.DoWork(chr)
	assert.Equal(t, true, r)
}

func TestSubroundImpl_DoWork4(t *testing.T) {

	fmt.Printf("4: Test the case when job is done but consensus is not done and than the time will be " +
		"expired -> true\n")

	chr, _ := InitSubroundImpl()

	sr1 := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		DoSubroundJob,
		DoExtendSubround,
		DoCheckConsensusWithoutSuccess)

	chr.AddSubround(sr1)

	sr2 := spos.NewSubroundImpl(chronology.Subround(spos.SrCommitmentHash),
		chronology.Subround(spos.SrBitmap),
		int64(40*RoundTimeDuration/100),
		"<COMMITMENT_HASH>",
		DoSubroundJob,
		DoExtendSubround,
		DoCheckConsensusWithoutSuccess)

	chr.AddSubround(sr2)
	chr.SetClockOffset(time.Duration(sr1.EndTime() - int64(5*time.Millisecond)))
	r := sr1.DoWork(chr)
	assert.Equal(t, true, r)
}

func TestNewSubroundImpl(t *testing.T) {

	sr := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		nil,
		nil,
		nil)

	assert.NotNil(t, sr)
}

func TestSubroundImpl_Current(t *testing.T) {

	sr := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		nil,
		nil,
		nil)

	assert.Equal(t, chronology.Subround(spos.SrBlock), sr.Current())
}

func TestSubroundImpl_Next(t *testing.T) {

	sr := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		nil,
		nil,
		nil)

	assert.Equal(t, chronology.Subround(spos.SrCommitmentHash), sr.Next())
}

func TestSubroundImpl_EndTime(t *testing.T) {

	sr := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		nil,
		nil,
		nil)

	assert.Equal(t, int64(25*RoundTimeDuration/100), sr.EndTime())
}

func TestSubroundImpl_Name(t *testing.T) {

	sr := spos.NewSubroundImpl(chronology.Subround(spos.SrBlock),
		chronology.Subround(spos.SrCommitmentHash),
		int64(25*RoundTimeDuration/100),
		"<BLOCK>",
		nil,
		nil,
		nil)

	assert.Equal(t, "<BLOCK>", sr.Name())
}
