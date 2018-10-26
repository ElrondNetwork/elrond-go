package chronology_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/chronology"
	"github.com/ElrondNetwork/elrond-go-sandbox/chronology/ntp"
	"github.com/stretchr/testify/assert"
)

const (
	srStartRound chronology.Subround = iota
	srBlock
	srCommitmentHash
	srBitmap
	srCommitment
	srSignature
	srEndRound
)

const roundTimeDuration = time.Duration(10 * time.Millisecond)

// #################### <START_ROUND> ####################

type SRStartRound struct {
	Hits int
}

func (sr *SRStartRound) DoWork(chr *chronology.Chronology) bool {
	sr.Hits++
	fmt.Printf("DoStartRound with %d hits\n", sr.Hits)
	return true
}

func (sr *SRStartRound) Current() chronology.Subround {
	return srStartRound
}

func (sr *SRStartRound) Next() chronology.Subround {
	return srBlock
}

func (sr *SRStartRound) EndTime() int64 {
	return int64(5 * roundTimeDuration / 100)
}

func (sr *SRStartRound) Name() string {
	return "<START_ROUND>"
}

// #################### <BLOCK> ####################

type SRBlock struct {
	Hits int
}

func (sr *SRBlock) DoWork(chr *chronology.Chronology) bool {
	sr.Hits++
	fmt.Printf("DoBlock with %d hits\n", sr.Hits)
	return true
}

func (sr *SRBlock) Current() chronology.Subround {
	return srBlock
}

func (sr *SRBlock) Next() chronology.Subround {
	return srCommitmentHash
}

func (sr *SRBlock) EndTime() int64 {
	return int64(25 * roundTimeDuration / 100)
}

func (sr *SRBlock) Name() string {
	return "<BLOCK>"
}

// #################### <COMMITMENT_HASH> ####################

type SRCommitmentHash struct {
	Hits int
}

func (sr *SRCommitmentHash) DoWork(chr *chronology.Chronology) bool {
	sr.Hits++
	fmt.Printf("DoCommitmentHash with %d hits\n", sr.Hits)
	return true
}

func (sr *SRCommitmentHash) Current() chronology.Subround {
	return srCommitmentHash
}

func (sr *SRCommitmentHash) Next() chronology.Subround {
	return srBitmap
}

func (sr *SRCommitmentHash) EndTime() int64 {
	return int64(40 * roundTimeDuration / 100)
}

func (sr *SRCommitmentHash) Name() string {
	return "<COMMITMENT_HASH>"
}

// #################### <BITMAP> ####################

type SRBitmap struct {
	Hits int
}

func (sr *SRBitmap) DoWork(chr *chronology.Chronology) bool {
	sr.Hits++
	fmt.Printf("DoBitmap with %d hits\n", sr.Hits)
	return true
}

func (sr *SRBitmap) Current() chronology.Subround {
	return srBitmap
}

func (sr *SRBitmap) Next() chronology.Subround {
	return srCommitment
}

func (sr *SRBitmap) EndTime() int64 {
	return int64(55 * roundTimeDuration / 100)
}

func (sr *SRBitmap) Name() string {
	return "<BITMAP>"
}

// #################### <COMMITMENT> ####################

type SRCommitment struct {
	Hits int
}

func (sr *SRCommitment) DoWork(chr *chronology.Chronology) bool {
	sr.Hits++
	fmt.Printf("DoCommitment with %d hits\n", sr.Hits)
	return true
}

func (sr *SRCommitment) Current() chronology.Subround {
	return srCommitment
}

func (sr *SRCommitment) Next() chronology.Subround {
	return srSignature
}

func (sr *SRCommitment) EndTime() int64 {
	return int64(70 * roundTimeDuration / 100)
}

func (sr *SRCommitment) Name() string {
	return "<COMMITMENT>"
}

// #################### <SIGNATURE> ####################

type SRSignature struct {
	Hits int
}

func (sr *SRSignature) DoWork(chr *chronology.Chronology) bool {
	sr.Hits++
	fmt.Printf("DoSignature with %d hits\n", sr.Hits)
	return true
}

func (sr *SRSignature) Current() chronology.Subround {
	return srSignature
}

func (sr *SRSignature) Next() chronology.Subround {
	return srEndRound
}

func (sr *SRSignature) EndTime() int64 {
	return int64(85 * roundTimeDuration / 100)
}

func (sr *SRSignature) Name() string {
	return "<SIGNATURE>"
}

// #################### <END_ROUND> ####################

type SREndRound struct {
	Hits int
}

func (sr *SREndRound) DoWork(chr *chronology.Chronology) bool {
	sr.Hits++
	fmt.Printf("DoEndRound with %d hits\n", sr.Hits)
	return true
}

func (sr *SREndRound) Current() chronology.Subround {
	return srEndRound
}

func (sr *SREndRound) Next() chronology.Subround {
	return srStartRound
}

func (sr *SREndRound) EndTime() int64 {
	return int64(100 * roundTimeDuration / 100)
}

func (sr *SREndRound) Name() string {
	return "<END_ROUND>"
}

func TestStartRound(t *testing.T) {

	genesisTime := time.Now()
	currentTime := genesisTime

	rnd := chronology.NewRound(genesisTime, currentTime, roundTimeDuration)
	syncTime := &ntp.LocalTime{}
	syncTime.SetClockOffset(roundTimeDuration + 1)

	chr := chronology.NewChronology(true, true, rnd, genesisTime, syncTime)

	chr.AddSubround(&SRStartRound{})
	chr.AddSubround(&SRBlock{})
	chr.AddSubround(&SRCommitmentHash{})
	chr.AddSubround(&SRBitmap{})
	chr.AddSubround(&SRCommitment{})
	chr.AddSubround(&SRSignature{})
	chr.AddSubround(&SREndRound{})

	for {
		chr.StartRound()
		if len(chr.SubroundHandlers()) > 0 {
			if chr.SelfSubround() == chr.SubroundHandlers()[len(chr.SubroundHandlers())-1].Next() {
				break
			}
		}
	}
}

func TestRoundState(t *testing.T) {
	currentTime := time.Now()

	rnd := chronology.NewRound(currentTime, currentTime, roundTimeDuration)
	chr := chronology.NewChronology(true, true, rnd, currentTime, &ntp.LocalTime{})

	state := chr.GetSubroundFromDateTime(currentTime.Add(-1 * time.Hour))
	assert.Equal(t, chronology.SrBeforeRound, state)

	state = chr.GetSubroundFromDateTime(currentTime.Add(1 * time.Hour))
	assert.Equal(t, chronology.SrAfterRound, state)

	state = chr.GetSubroundFromDateTime(currentTime)
	assert.Equal(t, chronology.SrUnknown, state)

	chr.AddSubround(&SRStartRound{})

	state = chr.GetSubroundFromDateTime(currentTime)
	assert.NotEqual(t, chronology.SrUnknown, state)
}

func TestLoadSubrounder(t *testing.T) {
	chr := chronology.Chronology{}

	sr := chr.LoadSubroundHandler(chronology.SrBeforeRound)
	assert.Nil(t, sr)

	sr = chr.LoadSubroundHandler(chronology.SrAfterRound)
	assert.Nil(t, sr)

	chr.AddSubround(&SRStartRound{})

	sr = chr.LoadSubroundHandler(srStartRound)
	assert.NotNil(t, sr)

	assert.Equal(t, sr.Name(), chr.SubroundHandlers()[0].Name())
}

func TestGettersAndSetters(t *testing.T) {
	genesisTime := time.Now()
	currentTime := genesisTime

	rnd := chronology.NewRound(genesisTime, currentTime, roundTimeDuration)
	syncTime := &ntp.LocalTime{}

	chr := chronology.NewChronology(true, true, rnd, genesisTime, syncTime)

	assert.Equal(t, 0, chr.Round().Index())
	assert.Equal(t, chronology.SrBeforeRound, chr.SelfSubround())

	chr.SetSelfSubround(srStartRound)
	assert.Equal(t, srStartRound, chr.SelfSubround())

	assert.Equal(t, chronology.SrBeforeRound, chr.TimeSubround())
	assert.Equal(t, time.Duration(0), chr.ClockOffset())
	assert.NotNil(t, chr.SyncTime())
	assert.Equal(t, time.Duration(0), chr.SyncTime().ClockOffset())

	chr.SetClockOffset(time.Duration(5))
	assert.Equal(t, time.Duration(5), chr.ClockOffset())

	fmt.Printf("%v\n%v\n%v", chr.SyncTime().CurrentTime(chr.ClockOffset()),
		chr.SyncTime().FormatedCurrentTime(chr.ClockOffset()), chr.SubroundHandlers())
}
