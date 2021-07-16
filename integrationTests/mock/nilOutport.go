package mock

import (
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/indexer"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/outport"
)

type nilOutport struct{}

// NewNilOutport --
func NewNilOutport() *nilOutport {
	return new(nilOutport)
}

// SaveBlock -
func (n *nilOutport) SaveBlock(_ *indexer.ArgsSaveBlockData) {
}

// RevertIndexedBlock -
func (n *nilOutport) RevertIndexedBlock(_ data.HeaderHandler, _ data.BodyHandler) {
}

// SaveRoundsInfo -
func (n *nilOutport) SaveRoundsInfo(_ []*indexer.RoundInfo) {
}

// UpdateTPS -
func (n *nilOutport) UpdateTPS(_ statistics.TPSBenchmark) {
}

// SaveValidatorsPubKeys -
func (n *nilOutport) SaveValidatorsPubKeys(_ map[uint32][][]byte, _ uint32) {
}

// SaveValidatorsRating -
func (n *nilOutport) SaveValidatorsRating(_ string, _ []*indexer.ValidatorRatingInfo) {
}

// SaveAccounts -
func (n *nilOutport) SaveAccounts(_ uint64, _ []state.UserAccountHandler) {
}

// Close -
func (n *nilOutport) Close() error {
	return nil
}

// IsInterfaceNil -
func (n *nilOutport) IsInterfaceNil() bool {
	return n == nil
}

// SubscribeDriver -
func (n *nilOutport) SubscribeDriver(_ outport.Driver) error {
	return nil
}

// HasDrivers -
func (n *nilOutport) HasDrivers() bool {
	return false
}
