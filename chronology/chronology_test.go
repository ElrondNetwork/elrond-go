package chronology_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/chronology"
	"github.com/ElrondNetwork/elrond-go-sandbox/chronology/mock"
	"github.com/ElrondNetwork/elrond-go-sandbox/chronology/ntp"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
)

const (
	srStartRound chronology.SubroundId = iota
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

func (sr *SRStartRound) DoWork(func() chronology.SubroundId, func() bool) bool {
	sr.Hits++
	fmt.Printf("DoStartRound with %d hits\n", sr.Hits)
	return true
}

func (sr *SRStartRound) Current() chronology.SubroundId {
	return srStartRound
}

func (sr *SRStartRound) Next() chronology.SubroundId {
	return srBlock
}

func (sr *SRStartRound) EndTime() int64 {
	return int64(5 * roundTimeDuration / 100)
}

func (sr *SRStartRound) Name() string {
	return "<START_ROUND>"
}

func (sr *SRStartRound) Check() bool {
	return true
}

// #################### <BLOCK> ####################

type SRBlock struct {
	Hits int
}

func (sr *SRBlock) DoWork(func() chronology.SubroundId, func() bool) bool {
	sr.Hits++
	fmt.Printf("DoBlock with %d hits\n", sr.Hits)
	return true
}

func (sr *SRBlock) Current() chronology.SubroundId {
	return srBlock
}

func (sr *SRBlock) Next() chronology.SubroundId {
	return srCommitmentHash
}

func (sr *SRBlock) EndTime() int64 {
	return int64(25 * roundTimeDuration / 100)
}

func (sr *SRBlock) Name() string {
	return "<BLOCK>"
}

func (sr *SRBlock) Check() bool {
	return true
}

// #################### <COMMITMENT_HASH> ####################

type SRCommitmentHash struct {
	Hits int
}

func (sr *SRCommitmentHash) DoWork(func() chronology.SubroundId, func() bool) bool {
	sr.Hits++
	fmt.Printf("DoCommitmentHash with %d hits\n", sr.Hits)
	return true
}

func (sr *SRCommitmentHash) Current() chronology.SubroundId {
	return srCommitmentHash
}

func (sr *SRCommitmentHash) Next() chronology.SubroundId {
	return srBitmap
}

func (sr *SRCommitmentHash) EndTime() int64 {
	return int64(40 * roundTimeDuration / 100)
}

func (sr *SRCommitmentHash) Name() string {
	return "<COMMITMENT_HASH>"
}

func (sr *SRCommitmentHash) Check() bool {
	return true
}

// #################### <BITMAP> ####################

type SRBitmap struct {
	Hits int
}

func (sr *SRBitmap) DoWork(func() chronology.SubroundId, func() bool) bool {
	sr.Hits++
	fmt.Printf("DoBitmap with %d hits\n", sr.Hits)
	return true
}

func (sr *SRBitmap) Current() chronology.SubroundId {
	return srBitmap
}

func (sr *SRBitmap) Next() chronology.SubroundId {
	return srCommitment
}

func (sr *SRBitmap) EndTime() int64 {
	return int64(55 * roundTimeDuration / 100)
}

func (sr *SRBitmap) Name() string {
	return "<BITMAP>"
}

func (sr *SRBitmap) Check() bool {
	return true
}

// #################### <COMMITMENT> ####################

type SRCommitment struct {
	Hits int
}

func (sr *SRCommitment) DoWork(func() chronology.SubroundId, func() bool) bool {
	sr.Hits++
	fmt.Printf("DoCommitment with %d hits\n", sr.Hits)
	return true
}

func (sr *SRCommitment) Current() chronology.SubroundId {
	return srCommitment
}

func (sr *SRCommitment) Next() chronology.SubroundId {
	return srSignature
}

func (sr *SRCommitment) EndTime() int64 {
	return int64(70 * roundTimeDuration / 100)
}

func (sr *SRCommitment) Name() string {
	return "<COMMITMENT>"
}

func (sr *SRCommitment) Check() bool {
	return true
}

// #################### <SIGNATURE> ####################

type SRSignature struct {
	Hits int
}

func (sr *SRSignature) DoWork(func() chronology.SubroundId, func() bool) bool {
	sr.Hits++
	fmt.Printf("DoSignature with %d hits\n", sr.Hits)
	return true
}

func (sr *SRSignature) Current() chronology.SubroundId {
	return srSignature
}

func (sr *SRSignature) Next() chronology.SubroundId {
	return srEndRound
}

func (sr *SRSignature) EndTime() int64 {
	return int64(85 * roundTimeDuration / 100)
}

func (sr *SRSignature) Name() string {
	return "<SIGNATURE>"
}

func (sr *SRSignature) Check() bool {
	return true
}

// #################### <END_ROUND> ####################

type SREndRound struct {
	Hits int
}

func (sr *SREndRound) DoWork(func() chronology.SubroundId, func() bool) bool {
	sr.Hits++
	fmt.Printf("DoEndRound with %d hits\n", sr.Hits)
	return true
}

func (sr *SREndRound) Current() chronology.SubroundId {
	return srEndRound
}

func (sr *SREndRound) Next() chronology.SubroundId {
	return srStartRound
}

func (sr *SREndRound) EndTime() int64 {
	return int64(100 * roundTimeDuration / 100)
}

func (sr *SREndRound) Name() string {
	return "<END_ROUND>"
}

func (sr *SREndRound) Check() bool {
	return true
}

func TestStartRound(t *testing.T) {
	genesisTime := time.Now()
	currentTime := genesisTime

	rnd := chronology.NewRound(
		genesisTime,
		currentTime,
		roundTimeDuration)

	chr := chronology.NewChronology(
		true,
		rnd,
		genesisTime,
		ntp.NewSyncTime(roundTimeDuration, nil))

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
	chr := chronology.NewChronology(true, rnd, currentTime, ntp.NewSyncTime(roundTimeDuration, nil))

	state := chr.GetSubroundFromDateTime(currentTime)
	assert.Equal(t, chronology.SubroundId(-1), state)

	chr.AddSubround(&SRStartRound{})

	state = chr.GetSubroundFromDateTime(currentTime.Add(-1 * time.Hour))
	assert.Equal(t, chronology.SubroundId(-1), state)

	state = chr.GetSubroundFromDateTime(currentTime.Add(1 * time.Hour))
	assert.Equal(t, chronology.SubroundId(-1), state)

	state = chr.GetSubroundFromDateTime(currentTime)
	assert.Equal(t, chr.SubroundHandlers()[0].Current(), state)
}

func TestLoadSubrounder(t *testing.T) {
	chr := chronology.Chronology{}

	sr := chr.LoadSubroundHandler(-1)
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
	chr := chronology.NewChronology(true, rnd, genesisTime, ntp.NewSyncTime(roundTimeDuration, nil))

	assert.Equal(t, int32(0), chr.Round().Index())
	assert.Equal(t, chronology.SubroundId(-1), chr.SelfSubround())

	chr.SetSelfSubround(srStartRound)
	assert.Equal(t, srStartRound, chr.SelfSubround())

	assert.Equal(t, chronology.SubroundId(-1), chr.TimeSubround())
	assert.Equal(t, time.Duration(0), chr.ClockOffset())
	assert.NotNil(t, chr.SyncTime())
	assert.Equal(t, time.Duration(0), chr.SyncTime().ClockOffset())

	chr.SetClockOffset(time.Duration(5))
	assert.Equal(t, time.Duration(5), chr.ClockOffset())
	chr.AddSubround(&SRStartRound{})

	spew.Dump(chr.SubroundHandlers())
}

func TestRoundTimeStamp_ShouldReturnCorrectTimeStamp(t *testing.T) {
	genesisTime := time.Now()
	currentTime := genesisTime

	rnd := chronology.NewRound(genesisTime, currentTime, roundTimeDuration)
	chr := chronology.NewChronology(true, rnd, genesisTime, ntp.NewSyncTime(roundTimeDuration, nil))

	timeStamp := chr.RoundTimeStampFromIndex(2)

	assert.Equal(t, genesisTime.Add(time.Duration(2*rnd.TimeDuration())).Unix(), int64(timeStamp))
}

//------- UpdateSelfSubroundIfNeeded

func TestChronology_UpdateSelfSubroundIfNeededShouldNotChangeForDifferentSubroundId(t *testing.T) {
	round := chronology.NewRound(time.Unix(0, 0), time.Unix(0, 0), time.Duration(4))
	syncer := &mock.SyncTimeMock{
		CurrentTimeCalled: func(duration time.Duration) time.Time {
			return time.Unix(-1, 0)
		},
	}

	subRoundId := chronology.SubroundId(-5)

	chr := chronology.NewChronology(true, round, time.Unix(0, 0), syncer)
	chr.SetSelfSubround(subRoundId)
	chr.UpdateSelfSubroundIfNeeded(-4)

	assert.Equal(t, subRoundId, chr.SelfSubround())
}

func TestChronology_UpdateSelfSubroundIfNeededShouldNotChangeForNotFoundSubroundHandler(t *testing.T) {
	round := chronology.NewRound(time.Unix(0, 0), time.Unix(0, 0), time.Duration(4))
	syncer := &mock.SyncTimeMock{
		CurrentTimeCalled: func(duration time.Duration) time.Time {
			return time.Unix(-1, 0)
		},
	}

	subRoundId := chronology.SubroundId(-5)

	chr := chronology.NewChronology(true, round, time.Unix(0, 0), syncer)
	chr.SetSelfSubround(subRoundId)
	chr.UpdateSelfSubroundIfNeeded(subRoundId)

	assert.Equal(t, subRoundId, chr.SelfSubround())
}

func createStubSubroundHandler(subroundId int) *mock.SubroundHandlerStub {
	return &mock.SubroundHandlerStub{
		CurrentCalled: func() chronology.SubroundId {
			return chronology.SubroundId(subroundId)
		},
	}
}

func TestChronology_UpdateSelfSubroundIfNeededShouldNotChangeForNegativeRound(t *testing.T) {
	round := chronology.NewRound(time.Unix(0, 0), time.Unix(-1, 0), time.Duration(4))
	syncer := &mock.SyncTimeMock{
		CurrentTimeCalled: func(duration time.Duration) time.Time {
			return time.Unix(-1, 0)
		},
	}

	subRoundId := chronology.SubroundId(2)

	chr := chronology.NewChronology(true, round, time.Unix(0, 0), syncer)
	//add 3 stubs for subroundHandler
	chr.AddSubround(createStubSubroundHandler(0))
	chr.AddSubround(createStubSubroundHandler(1))
	chr.AddSubround(createStubSubroundHandler(2))

	chr.SetSelfSubround(subRoundId)
	chr.UpdateSelfSubroundIfNeeded(subRoundId)

	assert.Equal(t, subRoundId, chr.SelfSubround())
}

func TestChronology_UpdateSelfSubroundIfNeededShouldNotChangeForDoWorkReturningFalse(t *testing.T) {
	round := chronology.NewRound(time.Unix(0, 0), time.Unix(0, 0), time.Duration(4))
	syncer := &mock.SyncTimeMock{
		CurrentTimeCalled: func(duration time.Duration) time.Time {
			return time.Unix(-1, 0)
		},
	}

	subRoundId := chronology.SubroundId(2)

	chr := chronology.NewChronology(true, round, time.Unix(0, 0), syncer)
	//add 3 stubs for subroundHandler
	chr.AddSubround(createStubSubroundHandler(0))
	chr.AddSubround(createStubSubroundHandler(1))
	crtSubroundHandler := createStubSubroundHandler(2)
	crtSubroundHandler.DoWorkCalled = func(id func() chronology.SubroundId, i func() bool) bool {
		return false
	}
	chr.AddSubround(crtSubroundHandler)

	chr.SetSelfSubround(subRoundId)
	chr.UpdateSelfSubroundIfNeeded(subRoundId)

	assert.Equal(t, subRoundId, chr.SelfSubround())
}

func TestChronology_UpdateSelfSubroundIfNeededShouldNotChangeWhileWorkingRoundIsCancelled(t *testing.T) {
	round := chronology.NewRound(time.Unix(0, 0), time.Unix(0, 0), time.Duration(4))
	syncer := &mock.SyncTimeMock{
		CurrentTimeCalled: func(duration time.Duration) time.Time {
			return time.Unix(-1, 0)
		},
	}

	subRoundId := chronology.SubroundId(2)

	chr := chronology.NewChronology(true, round, time.Unix(0, 0), syncer)
	//add 3 stubs for subroundHandler
	chr.AddSubround(createStubSubroundHandler(0))
	chr.AddSubround(createStubSubroundHandler(1))
	crtSubroundHandler := createStubSubroundHandler(2)
	crtSubroundHandler.DoWorkCalled = func(id func() chronology.SubroundId, i func() bool) bool {
		chr.SetSelfSubround(-1)
		return true
	}
	chr.AddSubround(crtSubroundHandler)

	chr.SetSelfSubround(subRoundId)
	chr.UpdateSelfSubroundIfNeeded(subRoundId)

	assert.Equal(t, chronology.SubroundId(-1), chr.SelfSubround())
}

func TestChronology_UpdateSelfSubroundIfNeededShouldShouldChange(t *testing.T) {
	round := chronology.NewRound(time.Unix(0, 0), time.Unix(0, 0), time.Duration(4))
	syncer := &mock.SyncTimeMock{
		CurrentTimeCalled: func(duration time.Duration) time.Time {
			return time.Unix(-1, 0)
		},
	}

	subRoundId := chronology.SubroundId(2)
	nextSubRoundId := chronology.SubroundId(100)

	chr := chronology.NewChronology(true, round, time.Unix(0, 0), syncer)
	//add 3 stubs for subroundHandler
	chr.AddSubround(createStubSubroundHandler(0))
	chr.AddSubround(createStubSubroundHandler(1))
	crtSubroundHandler := createStubSubroundHandler(2)
	crtSubroundHandler.DoWorkCalled = func(id func() chronology.SubroundId, i func() bool) bool {
		return true
	}
	crtSubroundHandler.NextCalled = func() chronology.SubroundId {
		return nextSubRoundId
	}
	chr.AddSubround(crtSubroundHandler)

	chr.SetSelfSubround(subRoundId)
	chr.UpdateSelfSubroundIfNeeded(subRoundId)

	assert.Equal(t, nextSubRoundId, chr.SelfSubround())
}

func TestRemoveAllSubrounds_ShouldReturnEmptySubroundHandlersArray(t *testing.T) {
	genesisTime := time.Now()
	currentTime := genesisTime

	rnd := chronology.NewRound(
		genesisTime,
		currentTime,
		roundTimeDuration)

	chr := chronology.NewChronology(
		true,
		rnd,
		genesisTime,
		ntp.NewSyncTime(roundTimeDuration, nil))

	chr.AddSubround(&SRStartRound{})
	chr.AddSubround(&SRBlock{})
	chr.AddSubround(&SRCommitmentHash{})
	chr.AddSubround(&SRBitmap{})
	chr.AddSubround(&SRCommitment{})
	chr.AddSubround(&SRSignature{})
	chr.AddSubround(&SREndRound{})

	assert.Equal(t, 7, len(chr.SubroundHandlers()))

	chr.RemoveAllSubrounds()

	assert.Equal(t, 0, len(chr.SubroundHandlers()))
}
