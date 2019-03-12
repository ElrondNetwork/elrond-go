package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
)

type oneShardCoordinatorMock struct {
	noShards uint32
}

func NewOneShardCoordinatorMock() *oneShardCoordinatorMock {
	return &oneShardCoordinatorMock{noShards: 1}
}

func (scm *oneShardCoordinatorMock) NumberOfShards() uint32 {
	return scm.noShards
}

func (scm *oneShardCoordinatorMock) ComputeId(address state.AddressContainer) uint32 {

	return uint32(0)
}

func (scm *oneShardCoordinatorMock) SelfId() uint32 {
	return 0
}

func (scm *oneShardCoordinatorMock) SetSelfId(shardId uint32) error {
	return nil
}

func (scm *oneShardCoordinatorMock) SameShard(firstAddress, secondAddress state.AddressContainer) bool {
	return true
}
