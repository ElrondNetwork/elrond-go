package txcache

import (
	"github.com/ElrondNetwork/elrond-go/core/atomic"
	"github.com/ElrondNetwork/elrond-go/storage/txcache/maps"
)

// txByHashMap is a new map-like structure for holding and accessing transactions by txHash
type txByHashMap struct {
	backingMap *maps.ConcurrentMap
	counter    atomic.Counter
	numBytes   atomic.Counter
}

// newTxByHashMap creates a new TxByHashMap instance
func newTxByHashMap(nChunksHint uint32) *txByHashMap {
	backingMap := maps.NewConcurrentMap(nChunksHint)

	return &txByHashMap{
		backingMap: backingMap,
	}
}

// addTx adds a transaction to the map
func (txMap *txByHashMap) addTx(tx *WrappedTransaction) bool {
	added := txMap.backingMap.SetIfAbsent(string(tx.TxHash), tx)
	if added {
		txMap.counter.Increment()
		txMap.numBytes.Add(tx.Size)
	}

	return added
}

// removeTx removes a transaction from the map
func (txMap *txByHashMap) removeTx(txHash string) (*WrappedTransaction, bool) {
	item, removed := txMap.backingMap.Remove(txHash)
	if !removed {
		return nil, false
	}

	tx, ok := item.(*WrappedTransaction)
	if !ok {
		return nil, false
	}

	if removed {
		txMap.counter.Decrement()
		txMap.numBytes.Subtract(tx.Size)
	}

	return tx, true
}

// getTx gets a transaction from the map
func (txMap *txByHashMap) getTx(txHash string) (*WrappedTransaction, bool) {
	txUntyped, ok := txMap.backingMap.Get(txHash)
	if !ok {
		return nil, false
	}

	tx := txUntyped.(*WrappedTransaction)
	return tx, true
}

func (txMap *txByHashMap) getTxs(keys [][]byte) []*WrappedTransaction {
	result := make([]*WrappedTransaction, 0, len(keys))

	for _, key := range keys {
		txUntyped, ok := txMap.backingMap.Get(string(key))
		if !ok {
			continue
		}

		result = append(result, txUntyped.(*WrappedTransaction))
	}

	return result
}

// RemoveTxsBulk removes transactions, in bulk
func (txMap *txByHashMap) RemoveTxsBulk(txHashes [][]byte) uint32 {
	numRemoved := uint32(0)

	for _, txHash := range txHashes {
		_, removed := txMap.removeTx(string(txHash))
		if removed {
			numRemoved++
		}
	}

	return numRemoved
}

// forEach iterates over the senders
func (txMap *txByHashMap) forEach(function ForEachTransaction) {
	txMap.backingMap.IterCb(func(key string, item interface{}) {
		tx := item.(*WrappedTransaction)
		function([]byte(key), tx)
	})
}

func (txMap *txByHashMap) clear() {
	txMap.backingMap.Clear()
	txMap.counter.Set(0)
}

func (txMap *txByHashMap) keys() [][]byte {
	keys := txMap.backingMap.Keys()
	keysAsBytes := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		keysAsBytes[i] = []byte(keys[i])
	}

	return keysAsBytes
}
