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

func InitSRSignature() (*chronology.Chronology, *spos.SRSignature) {
	genesisTime := time.Now()
	currentTime := genesisTime

	rnd := chronology.NewRound(genesisTime, currentTime, spos.RoundTimeDuration)

	chr := chronology.NewChronology(true, true, rnd, genesisTime, &ntp.LocalTime{})

	vld := spos.NewValidators(nil, nil, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}, "2")
	pbft := 2*len(vld.ConsensusGroup)/3 + 1
	th := spos.NewThreshold(1, pbft, pbft, pbft, pbft)
	rs := spos.NewRoundStatus(spos.SsNotFinished, spos.SsNotFinished, spos.SsNotFinished, spos.SsNotFinished, spos.SsNotFinished)

	cns := spos.NewConsensus(true, vld, th, rs, chr)

	sr := spos.NewSRSignature(true, int64(100*spos.RoundTimeDuration/100), cns, nil)
	chr.AddSubround(sr)

	return chr, sr
}

func TestNewSRSignature(t *testing.T) {
	sr := spos.NewSRSignature(true, int64(100*spos.RoundTimeDuration/100), nil, nil)
	assert.NotNil(t, sr)
}

func TestSRSignature_DoWork1(t *testing.T) {

	chr, sr := InitSRSignature()

	fmt.Printf("1: Test case when send message is done but consensus is not done -> RNone\n")

	sr.OnSendSignature = SndWithSuccess

	r := sr.DoSignature(chr)

	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Block)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.ComitmentHash)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Bitmap)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Comitment)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Signature)
	assert.Equal(t, spos.RNone, r)
}

func TestSRSignature_DoWork2(t *testing.T) {

	chr, sr := InitSRSignature()

	fmt.Printf("2: Test case when send message is done but consensus is semi-done (only subround BLOCK, COMITMENT_HASH, BITMAP and COMITMENT is done) -> RNone\n")

	sr.OnSendSignature = SndWithSuccess

	sr.Cns.ValidationMap[sr.Cns.Self].Block = true

	for i := 0; i < len(sr.Cns.ConsensusGroup); i++ {
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].ComitmentHash = true
	}

	for i := 0; i < sr.Cns.Threshold.Bitmap; i++ {
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Bitmap = true
	}

	for i := 0; i < sr.Cns.Threshold.Comitment; i++ {
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Comitment = true
	}

	r := sr.DoSignature(chr)

	assert.Equal(t, spos.SsFinished, sr.Cns.RoundStatus.Block)
	assert.Equal(t, spos.SsFinished, sr.Cns.RoundStatus.ComitmentHash)
	assert.Equal(t, spos.SsFinished, sr.Cns.RoundStatus.Bitmap)
	assert.Equal(t, spos.SsFinished, sr.Cns.RoundStatus.Comitment)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Signature)
	assert.Equal(t, spos.RNone, r)
}

func TestSRSignature_DoWork3(t *testing.T) {

	chr, sr := InitSRSignature()

	fmt.Printf("3: Test case when send message and consensus is done -> RTrue\n")

	sr.OnSendSignature = SndWithSuccess

	sr.Cns.RoundStatus.Block = spos.SsFinished
	sr.Cns.RoundStatus.ComitmentHash = spos.SsFinished
	sr.Cns.RoundStatus.Bitmap = spos.SsFinished
	sr.Cns.RoundStatus.Comitment = spos.SsFinished

	for i := 0; i < sr.Cns.Threshold.Signature; i++ {
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Bitmap = true
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Signature = true
	}

	r := sr.DoSignature(chr)

	assert.Equal(t, spos.SsFinished, sr.Cns.RoundStatus.Signature)
	assert.Equal(t, spos.RTrue, r)
}

func TestSRSignature_DoWork4(t *testing.T) {

	chr, sr := InitSRSignature()

	fmt.Printf("4: Test case when time has expired -> RTrue\n")

	sr.OnSendSignature = SndWithoutSuccess

	chr.SetClockOffset(time.Duration(sr.EndTime() + 1))

	r := sr.DoSignature(chr)

	assert.Equal(t, spos.SsExtended, sr.Cns.RoundStatus.Signature)
	assert.Equal(t, r, spos.RTrue)
}

func TestSRSignature_DoWork5(t *testing.T) {

	chr, sr := InitSRSignature()

	fmt.Printf("5: Test case when receive message is done but consensus is not done -> RNone\n")

	sr.OnSendSignature = SndWithoutSuccess
	sr.Cns.SetReceivedMessage(true)

	r := sr.DoSignature(chr)

	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Block)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.ComitmentHash)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Bitmap)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Comitment)
	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Signature)
	assert.Equal(t, spos.RNone, r)
}

func TestSRSignature_DoWork6(t *testing.T) {

	chr, sr := InitSRSignature()

	fmt.Printf("6: Test case when receive message and consensus is done -> true\n")

	sr.OnSendSignature = SndWithoutSuccess
	sr.Cns.SetReceivedMessage(true)

	sr.Cns.RoundStatus.Block = spos.SsFinished
	sr.Cns.RoundStatus.ComitmentHash = spos.SsFinished
	sr.Cns.RoundStatus.Bitmap = spos.SsFinished
	sr.Cns.RoundStatus.Comitment = spos.SsFinished

	for i := 0; i < sr.Cns.Threshold.Signature; i++ {
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Bitmap = true
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Signature = true
	}

	r := sr.DoWork(chr)

	assert.Equal(t, spos.SsFinished, sr.Cns.RoundStatus.Signature)
	assert.Equal(t, true, r)
}

func TestSRSignature_DoWork7(t *testing.T) {

	chr, sr := InitSRSignature()

	fmt.Printf("7: Test case when receive message is not done and round should be canceled -> false\n")

	sr.OnSendSignature = SndWithoutSuccess
	sr.Cns.SetReceivedMessage(false)
	sr.Cns.Chr.SetSelfSubround(chronology.SrCanceled)

	sr.Cns.RoundStatus.Block = spos.SsFinished
	sr.Cns.RoundStatus.ComitmentHash = spos.SsFinished
	sr.Cns.RoundStatus.Bitmap = spos.SsFinished
	sr.Cns.RoundStatus.Comitment = spos.SsFinished

	for i := 0; i < sr.Cns.Threshold.Signature; i++ {
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Bitmap = true
		sr.Cns.ValidationMap[sr.Cns.ConsensusGroup[i]].Signature = true
	}

	r := sr.DoWork(chr)

	assert.Equal(t, spos.SsNotFinished, sr.Cns.RoundStatus.Signature)
	assert.Equal(t, chronology.SrCanceled, chr.SelfSubround())
	assert.Equal(t, false, r)
}

func TestSRSignature_Current(t *testing.T) {
	sr := spos.NewSRSignature(true, int64(100*spos.RoundTimeDuration/100), nil, nil)
	assert.Equal(t, chronology.Subround(spos.SrSignature), sr.Current())
}

func TestSRSignature_Next(t *testing.T) {
	sr := spos.NewSRSignature(true, int64(100*spos.RoundTimeDuration/100), nil, nil)
	assert.Equal(t, chronology.Subround(spos.SrEndRound), sr.Next())
}

func TestSRSignature_EndTime(t *testing.T) {
	sr := spos.NewSRSignature(true, int64(100*spos.RoundTimeDuration/100), nil, nil)
	assert.Equal(t, int64(100*spos.RoundTimeDuration/100), sr.EndTime())
}

func TestSRSignature_Name(t *testing.T) {
	sr := spos.NewSRSignature(true, int64(100*spos.RoundTimeDuration/100), nil, nil)
	assert.Equal(t, "<SIGNATURE>", sr.Name())
}

func TestSRSignature_Log(t *testing.T) {
	sr := spos.NewSRSignature(true, int64(100*spos.RoundTimeDuration/100), nil, nil)
	sr.Log("Test SRSignature")
}
