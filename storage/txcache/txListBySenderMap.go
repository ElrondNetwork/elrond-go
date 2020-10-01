package txcache

import (
	"sync"

	"github.com/ElrondNetwork/elrond-go/core/atomic"
	"github.com/ElrondNetwork/elrond-go/storage/txcache/maps"
)

const numberOfScoreChunks = uint32(100)

// txListBySenderMap is a map-like structure for holding and accessing transactions by sender
type txListBySenderMap struct {
	backingMap        *maps.BucketSortedMap
	senderConstraints senderConstraints
	counter           atomic.Counter
	scoreComputer     scoreComputer
	mutex             sync.Mutex
}

// newTxListBySenderMap creates a new instance of TxListBySenderMap
func newTxListBySenderMap(nChunksHint uint32, senderConstraints senderConstraints, scoreComputer scoreComputer) *txListBySenderMap {
	backingMap := maps.NewBucketSortedMap(nChunksHint, numberOfScoreChunks)

	return &txListBySenderMap{
		backingMap:        backingMap,
		senderConstraints: senderConstraints,
		scoreComputer:     scoreComputer,
	}
}

// addTx adds a transaction in the map, in the corresponding list (selected by its sender)
func (txMap *txListBySenderMap) addTx(tx *WrappedTransaction) (bool, [][]byte) {
	sender := string(tx.Tx.GetSndAddr())
	listForSender := txMap.getOrAddListForSender(sender)
	return listForSender.AddTx(tx)
}

// getOrAddListForSender gets or lazily creates a list (using double-checked locking pattern)
func (txMap *txListBySenderMap) getOrAddListForSender(sender string) *txListForSender {
	listForSender, ok := txMap.getListForSender(sender)
	if ok {
		return listForSender
	}

	txMap.mutex.Lock()
	defer txMap.mutex.Unlock()

	listForSender, ok = txMap.getListForSender(sender)
	if ok {
		return listForSender
	}

	return txMap.addSender(sender)
}

func (txMap *txListBySenderMap) getListForSender(sender string) (*txListForSender, bool) {
	listForSenderUntyped, ok := txMap.backingMap.Get(sender)
	if !ok {
		return nil, false
	}

	listForSender := listForSenderUntyped.(*txListForSender)
	return listForSender, true
}

func (txMap *txListBySenderMap) addSender(sender string) *txListForSender {
	listForSender := newTxListForSender(sender, &txMap.senderConstraints, txMap.notifyScoreChange)

	txMap.backingMap.Set(listForSender)
	txMap.counter.Increment()

	return listForSender
}

// This function should only be called in a critical section managed by a "txListForSender"
func (txMap *txListBySenderMap) notifyScoreChange(txList *txListForSender, scoreParams senderScoreParams) {
	score := txMap.scoreComputer.computeScore(scoreParams)
	txList.setLastComputedScore(score)
	txMap.backingMap.NotifyScoreChange(txList, score)
}

// removeTx removes a transaction from the map
func (txMap *txListBySenderMap) removeTx(tx *WrappedTransaction) bool {
	sender := string(tx.Tx.GetSndAddr())

	listForSender, ok := txMap.getListForSender(sender)
	if !ok {
		// This happens when a sender whose transactions were selected for processing is removed from cache in the meantime.
		// When it comes to remove one if its transactions due to processing (commited / finalized block), they don't exist in cache anymore.
		log.Trace("txListBySenderMap.removeTx() detected slight inconsistency: sender of tx not in cache", "tx", tx.TxHash, "sender", []byte(sender))
		return false
	}

	isFound := listForSender.RemoveTx(tx)
	isEmpty := listForSender.IsEmpty()
	if isEmpty {
		txMap.removeSender(sender)
	}

	return isFound
}

func (txMap *txListBySenderMap) removeGroupOfTxs(group groupOfTxs) int {
	listForSender, ok := txMap.getListForSender(string(group.sender))
	if !ok {
		log.Trace("txListBySenderMap.removeGroupOfTxs() detected slight inconsistency: sender not in cache", "len(txs)", len(group.transactions), "sender", []byte(group.sender))
		return 0
	}

	return listForSender.RemoveSortedTransactions(group.transactions)
}

func (txMap *txListBySenderMap) removeSender(sender string) bool {
	_, removed := txMap.backingMap.Remove(sender)
	if removed {
		txMap.counter.Decrement()
	}

	return removed
}

// RemoveSendersBulk removes senders, in bulk
func (txMap *txListBySenderMap) RemoveSendersBulk(senders []string) uint32 {
	numRemoved := uint32(0)

	for _, senderKey := range senders {
		if txMap.removeSender(senderKey) {
			numRemoved++
		}
	}

	return numRemoved
}

func (txMap *txListBySenderMap) notifyAccountNonce(accountKey []byte, nonce uint64) {
	sender := string(accountKey)
	listForSender, ok := txMap.getListForSender(sender)
	if !ok {
		return
	}

	listForSender.notifyAccountNonce(nonce)
}

func (txMap *txListBySenderMap) getSnapshotAscending() []*txListForSender {
	itemsSnapshot := txMap.backingMap.GetSnapshotAscending()
	listsSnapshot := make([]*txListForSender, len(itemsSnapshot))

	for i, item := range itemsSnapshot {
		listsSnapshot[i] = item.(*txListForSender)
	}

	return listsSnapshot
}

func (txMap *txListBySenderMap) getSnapshotDescending() []*txListForSender {
	itemsSnapshot := txMap.backingMap.GetSnapshotDescending()
	listsSnapshot := make([]*txListForSender, len(itemsSnapshot))

	for i, item := range itemsSnapshot {
		listsSnapshot[i] = item.(*txListForSender)
	}

	return listsSnapshot
}

func (txMap *txListBySenderMap) clear() {
	txMap.backingMap.Clear()
	txMap.counter.Set(0)
}
