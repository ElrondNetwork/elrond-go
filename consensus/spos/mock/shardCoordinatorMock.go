package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
)

type ShardCoordinatorMock struct {
}

func (scm ShardCoordinatorMock) NumberOfShards() uint32 {
	panic("implement me")
}

func (scm ShardCoordinatorMock) ComputeId(address state.AddressContainer) uint32 {
	panic("implement me")
}

func (scm ShardCoordinatorMock) SetSelfId(shardId uint32) error {
	panic("implement me")
}

func (scm ShardCoordinatorMock) SelfId() uint32 {
	return 0
}

func (scm ShardCoordinatorMock) SameShard(firstAddress, secondAddress state.AddressContainer) bool {
	return true
}
