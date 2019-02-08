package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/chronology"
)

type ChronologyHandlerMock struct {
	AddSubroundCalled        func(chronology.SubroundHandler)
	RemoveAllSubroundsCalled func()
	StartRoundCalled         func()
}

func (chrm *ChronologyHandlerMock) AddSubround(subroundHandler chronology.SubroundHandler) {
	if chrm.AddSubroundCalled != nil {
		chrm.AddSubroundCalled(subroundHandler)
	}
}

func (chrm *ChronologyHandlerMock) RemoveAllSubrounds() {
	if chrm.RemoveAllSubroundsCalled != nil {
		chrm.RemoveAllSubroundsCalled()
	}
}

func (chrm *ChronologyHandlerMock) StartRounds() {
	if chrm.StartRoundCalled != nil {
		chrm.StartRoundCalled()
	}
}
