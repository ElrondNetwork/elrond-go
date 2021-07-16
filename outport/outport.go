package outport

import (
	"sync"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/indexer"
	"github.com/ElrondNetwork/elrond-go/data/state"
)

type outport struct {
	mutex   sync.RWMutex
	drivers []Driver
}

var log = logger.GetOrCreate("outport")

// NewOutport will create a new instance of proxy
func NewOutport() *outport {
	return &outport{
		drivers: make([]Driver, 0),
		mutex:   sync.RWMutex{},
	}
}

// SaveBlock will save block for every driver
func (o *outport) SaveBlock(args *indexer.ArgsSaveBlockData) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	for _, driver := range o.drivers {
		driver.SaveBlock(args)
	}
}

// RevertIndexedBlock will revert block for every driver
func (o *outport) RevertIndexedBlock(header data.HeaderHandler, body data.BodyHandler) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	for _, driver := range o.drivers {
		driver.RevertIndexedBlock(header, body)
	}
}

// SaveRoundsInfo will save rounds information for every driver
func (o *outport) SaveRoundsInfo(roundsInfos []*indexer.RoundInfo) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	for _, driver := range o.drivers {
		driver.SaveRoundsInfo(roundsInfos)
	}
}

// UpdateTPS will update tps for every driver
func (o *outport) UpdateTPS(tpsBenchmark statistics.TPSBenchmark) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	for _, driver := range o.drivers {
		driver.UpdateTPS(tpsBenchmark)
	}
}

// SaveValidatorsPubKeys will save validators public keys for every driver
func (o *outport) SaveValidatorsPubKeys(validatorsPubKeys map[uint32][][]byte, epoch uint32) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	for _, driver := range o.drivers {
		driver.SaveValidatorsPubKeys(validatorsPubKeys, epoch)
	}
}

// SaveValidatorsRating will save validators rating for every driver
func (o *outport) SaveValidatorsRating(indexID string, infoRating []*indexer.ValidatorRatingInfo) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	for _, driver := range o.drivers {
		driver.SaveValidatorsRating(indexID, infoRating)
	}
}

// SaveAccounts will save accounts  for every driver
func (o *outport) SaveAccounts(blockTimestamp uint64, acc []state.UserAccountHandler) {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	for _, driver := range o.drivers {
		driver.SaveAccounts(blockTimestamp, acc)
	}
}

// Close will close all the drivers that are in outport
func (o *outport) Close() error {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	var err error
	for _, driver := range o.drivers {
		errClose := driver.Close()
		if errClose != nil {
			log.Error("cannot close driver", "error", errClose.Error())
			err = errClose
		}

	}

	return err
}

// HasDrivers returns true if there is at least one driver in the outport
func (o *outport) HasDrivers() bool {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return len(o.drivers) != 0
}

// SubscribeDriver can subscribe a driver to the outport
func (o *outport) SubscribeDriver(driver Driver) error {
	if check.IfNil(driver) {
		return ErrNilDriver
	}

	o.mutex.Lock()
	o.drivers = append(o.drivers, driver)
	o.mutex.Unlock()

	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (o *outport) IsInterfaceNil() bool {
	return o == nil
}
