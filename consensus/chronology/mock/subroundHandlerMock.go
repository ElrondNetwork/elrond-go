package mock

import (
	"time"
)

type SubroundHandlerMock struct {
	DoWorkCalled    func(func() time.Duration) bool
	PreviousCalled  func() int
	NextCalled      func() int
	CurrentCalled   func() int
	StartTimeCalled func() int64
	EndTimeCalled   func() int64
	NameCalled      func() string
	JobCalled       func() bool
	CheckCalled     func() bool
}

func (srm *SubroundHandlerMock) DoWork(handler func() time.Duration) bool {
	return srm.DoWorkCalled(handler)
}

func (srm *SubroundHandlerMock) Previous() int {
	return srm.PreviousCalled()
}

func (srm *SubroundHandlerMock) Next() int {
	return srm.NextCalled()
}

func (srm *SubroundHandlerMock) Current() int {
	return srm.CurrentCalled()
}

func (srm *SubroundHandlerMock) StartTime() int64 {
	return srm.StartTimeCalled()
}

func (srm *SubroundHandlerMock) EndTime() int64 {
	return srm.EndTimeCalled()
}

func (srm *SubroundHandlerMock) Name() string {
	return srm.NameCalled()
}

func (srm *SubroundHandlerMock) Job() bool {
	return srm.JobCalled()
}

func (srm *SubroundHandlerMock) Check() bool {
	return srm.CheckCalled()
}
