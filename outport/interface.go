package outport

import (
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/indexer"
	"github.com/ElrondNetwork/elrond-go/data/state"
)

// Driver is an interface for saving node specific data to other storage.
// This could be an elastic search index, a MySql database or any other external services.
type Driver interface {
	SaveBlock(args *indexer.ArgsSaveBlockData)
	RevertIndexedBlock(header data.HeaderHandler, body data.BodyHandler)
	SaveRoundsInfo(roundsInfos []*indexer.RoundInfo)
	UpdateTPS(tpsBenchmark statistics.TPSBenchmark)
	SaveValidatorsPubKeys(validatorsPubKeys map[uint32][][]byte, epoch uint32)
	SaveValidatorsRating(indexID string, infoRating []*indexer.ValidatorRatingInfo)
	SaveAccounts(blockTimestamp uint64, acc []state.UserAccountHandler)
	Close() error
	IsInterfaceNil() bool
}

// OutportHandler is interface that defines what a proxy implementation should be able to do
type OutportHandler interface {
	Driver
	SubscribeDriver(driver Driver) error
	HasDrivers() bool
}
