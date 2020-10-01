package txcache

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
)

const oneMilion = 1000000
const oneBillion = oneMilion * 1000
const delta = 0.00000001
const estimatedSizeOfBoundedTxFields = uint64(128)

func toNanoERD(erd float64) uint64 {
	return uint64(erd * float64(1000000000))
}

func kBToBytes(kB float32) uint64 {
	return uint64(kB * 1000)
}

func (cache *TxCache) areInternalMapsConsistent() bool {
	journal := cache.checkInternalConsistency()
	return journal.isFine()
}

func (cache *TxCache) getHashesForSender(sender string) []string {
	return cache.getListForSender(sender).getTxHashesAsStrings()
}

func (cache *TxCache) getListForSender(sender string) *txListForSender {
	return cache.txListBySender.testGetListForSender(sender)
}

func (txMap *txListBySenderMap) testGetListForSender(sender string) *txListForSender {
	list, ok := txMap.getListForSender(sender)
	if !ok {
		panic("sender not in cache")
	}

	return list
}

func (cache *TxCache) getScoreOfSender(sender string) uint32 {
	list := cache.getListForSender(sender)
	scoreParams := list.getScoreParams()
	computer := cache.txListBySender.scoreComputer
	return computer.computeScore(scoreParams)
}

func (cache *TxCache) getNumFailedSelectionsOfSender(sender string) int {
	return int(cache.getListForSender(sender).numFailedSelections.Get())
}

func (cache *TxCache) isSenderSweepable(sender string) bool {
	for _, item := range cache.sweepingListOfSenders {
		if item.sender == sender {
			return true
		}
	}

	return false
}

func (listForSender *txListForSender) getTxHashesAsStrings() []string {
	hashes := listForSender.getTxHashes()
	return hashesAsStrings(hashes)
}

func getTxHashesOfTxWrappersAsStrings(txs []*WrappedTransaction) []string {
	hashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		hashes = append(hashes, string(tx.TxHash))
	}

	return hashes
}

func hashesAsStrings(hashes [][]byte) []string {
	result := make([]string, len(hashes))
	for i := 0; i < len(hashes); i++ {
		result[i] = string(hashes[i])
	}

	return result
}

func hashesAsBytes(hashes []string) [][]byte {
	result := make([][]byte, len(hashes))
	for i := 0; i < len(hashes); i++ {
		result[i] = []byte(hashes[i])
	}

	return result
}

func addManyTransactionsWithUniformDistribution(cache *TxCache, nSenders int, nTransactionsPerSender int) {
	for senderTag := 0; senderTag < nSenders; senderTag++ {
		sender := createFakeSenderAddress(senderTag)

		for txNonce := nTransactionsPerSender; txNonce > 0; txNonce-- {
			txHash := createFakeTxHash(sender, txNonce)
			tx := createTx(txHash, string(sender), uint64(txNonce))
			cache.AddTx(tx)
		}
	}
}

func createDummiestTx(nonce uint64) *WrappedTransaction {
	return createTx([]byte(fmt.Sprintf("%d", nonce)), ".", nonce)
}

func createTx(hash []byte, sender string, nonce uint64) *WrappedTransaction {
	tx := &transaction.Transaction{
		SndAddr: []byte(sender),
		Nonce:   nonce,
	}

	return &WrappedTransaction{
		Tx:     tx,
		TxHash: hash,
		Size:   int64(estimatedSizeOfBoundedTxFields),
	}
}

func createTxWithParams(hash []byte, sender string, nonce uint64, size uint64, gasLimit uint64, gasPrice uint64) *WrappedTransaction {
	dataLength := int(size) - int(estimatedSizeOfBoundedTxFields)
	if dataLength < 0 {
		panic("createTxWithData(): invalid length for dummy tx")
	}

	tx := &transaction.Transaction{
		SndAddr:  []byte(sender),
		Nonce:    nonce,
		Data:     make([]byte, dataLength),
		GasLimit: gasLimit,
		GasPrice: gasPrice,
	}

	return &WrappedTransaction{
		Tx:     tx,
		TxHash: hash,
		Size:   int64(size),
	}
}

func createFakeSenderAddress(senderTag int) []byte {
	bytes := make([]byte, 32)
	binary.LittleEndian.PutUint64(bytes, uint64(senderTag))
	binary.LittleEndian.PutUint64(bytes[24:], uint64(senderTag))
	return bytes
}

func createFakeTxHash(fakeSenderAddress []byte, nonce int) []byte {
	bytes := make([]byte, 32)
	copy(bytes, fakeSenderAddress)
	binary.LittleEndian.PutUint64(bytes[8:], uint64(nonce))
	binary.LittleEndian.PutUint64(bytes[16:], uint64(nonce))
	return bytes
}

func measureWithStopWatch(b *testing.B, function func()) {
	sw := core.NewStopWatch()
	sw.Start("time")
	function()
	sw.Stop("time")

	duration := sw.GetMeasurementsMap()["time"]
	b.ReportMetric(duration, "time@stopWatch")
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
// Reference: https://stackoverflow.com/a/32843750/1475331
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

var _ scoreComputer = (*disabledScoreComputer)(nil)

type disabledScoreComputer struct {
}

func (computer *disabledScoreComputer) computeScore(_ senderScoreParams) uint32 {
	return 0
}
