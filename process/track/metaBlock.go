package track

import (
	"github.com/ElrondNetwork/elrond-go/data"
)

// metaBlockTracker implements NotarisedBlocksTracker interface which tracks notarised blocks
type metaBlockTracker struct {
}

// NewMetaBlockTracker creates a new metaBlockTracker object
func NewMetaBlockTracker() (*metaBlockTracker, error) {
	mbt := metaBlockTracker{}
	return &mbt, nil
}

// UnnotarisedBlocks gets all the blocks which are not notarised yet
func (mbt *metaBlockTracker) UnnotarisedBlocks() []data.HeaderHandler {
	return make([]data.HeaderHandler, 0)
}

// RemoveNotarisedBlocks removes all the blocks which already have been notarised
func (mbt *metaBlockTracker) RemoveNotarisedBlocks(headerHandler data.HeaderHandler) error {
	return nil
}

// AddBlock adds new block to be tracked
func (mbt *metaBlockTracker) AddBlock(headerHandler data.HeaderHandler) {
}

// SetBlockBroadcastRound sets the round in which the block with the given nonce has been broadcast
func (mbt *metaBlockTracker) SetBlockBroadcastRound(nonce uint64, round int64) {
}

// BlockBroadcastRound gets the round in which the block with given nonce has been broadcast
func (mbt *metaBlockTracker) BlockBroadcastRound(nonce uint64) int64 {
	return 0
}

// IsInterfaceNil return if there is no value under the interface
func (mbt *metaBlockTracker) IsInterfaceNil() bool {
	if mbt == nil {
		return true
	}
	return false
}
