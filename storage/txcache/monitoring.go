package txcache

import (
	"encoding/hex"
	"fmt"
	"strings"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
)

var log = logger.GetOrCreate("txcache")

func (cache *TxCache) monitorEvictionWrtSenderLimit(sender []byte, evicted [][]byte) {
	log.Trace("TxCache.AddTx() evict transactions wrt. limit by sender", "name", cache.name, "sender", sender, "num", len(evicted))

	for i := 0; i < core.MinInt(len(evicted), numEvictedTxsToDisplay); i++ {
		log.Trace("TxCache.AddTx() evict transactions wrt. limit by sender", "name", cache.name, "sender", sender, "tx", evicted[i])
	}
}

func (cache *TxCache) monitorEvictionStart() *core.StopWatch {
	log.Debug("TxCache: eviction started", "name", cache.name, "numBytes", cache.NumBytes(), "txs", cache.CountTx(), "senders", cache.CountSenders())
	cache.displaySendersHistogram()
	sw := core.NewStopWatch()
	sw.Start("eviction")
	return sw
}

func (cache *TxCache) monitorEvictionEnd(stopWatch *core.StopWatch) {
	stopWatch.Stop("eviction")
	duration := stopWatch.GetMeasurement("eviction")
	log.Debug("TxCache: eviction ended", "name", cache.name, "duration", duration, "numBytes", cache.NumBytes(), "txs", cache.CountTx(), "senders", cache.CountSenders())
	cache.evictionJournal.display()
	cache.displaySendersHistogram()
}

func (cache *TxCache) monitorSelectionStart() *core.StopWatch {
	log.Debug("TxCache: selection started", "name", cache.name, "numBytes", cache.NumBytes(), "txs", cache.CountTx(), "senders", cache.CountSenders())
	cache.displaySendersHistogram()
	sw := core.NewStopWatch()
	sw.Start("selection")
	return sw
}

func (cache *TxCache) monitorSelectionEnd(selection []*WrappedTransaction, stopWatch *core.StopWatch) {
	stopWatch.Stop("selection")
	duration := stopWatch.GetMeasurement("selection")
	numSendersSelected := cache.numSendersSelected.Reset()
	numSendersWithInitialGap := cache.numSendersWithInitialGap.Reset()
	numSendersWithMiddleGap := cache.numSendersWithMiddleGap.Reset()
	numSendersInGracePeriod := cache.numSendersInGracePeriod.Reset()

	log.Debug("TxCache: selection ended", "name", cache.name, "duration", duration,
		"numTxSelected", len(selection),
		"numSendersSelected", numSendersSelected,
		"numSendersWithInitialGap", numSendersWithInitialGap,
		"numSendersWithMiddleGap", numSendersWithMiddleGap,
		"numSendersInGracePeriod", numSendersInGracePeriod,
	)
}

type batchSelectionJournal struct {
	copied        int
	isFirstBatch  bool
	hasInitialGap bool
	hasMiddleGap  bool
	isGracePeriod bool
}

func (cache *TxCache) monitorBatchSelectionEnd(journal batchSelectionJournal) {
	if !journal.isFirstBatch {
		return
	}

	if journal.hasInitialGap {
		cache.numSendersWithInitialGap.Increment()
	} else if journal.hasMiddleGap {
		// Currently, we only count middle gaps on first batch (for simplicity)
		cache.numSendersWithMiddleGap.Increment()
	}

	if journal.isGracePeriod {
		cache.numSendersInGracePeriod.Increment()
	} else if journal.copied > 0 {
		cache.numSendersSelected.Increment()
	}
}

func (cache *TxCache) monitorSweepingStart() *core.StopWatch {
	sw := core.NewStopWatch()
	sw.Start("sweeping")
	return sw
}

func (cache *TxCache) monitorSweepingEnd(numTxs uint32, numSenders uint32, stopWatch *core.StopWatch) {
	stopWatch.Stop("sweeping")
	duration := stopWatch.GetMeasurement("sweeping")
	log.Debug("TxCache: swept senders:", "name", cache.name, "duration", duration, "txs", numTxs, "senders", numSenders)
	cache.displaySendersHistogram()
}

func (cache *TxCache) displaySendersHistogram() {
	txListBySenderMap := cache.txListBySender.backingMap
	log.Debug("TxCache.sendersHistogram:", "chunks", txListBySenderMap.ChunksCounts(), "scoreChunks", txListBySenderMap.ScoreChunksCounts())
}

// evictionJournal keeps a short journal about the eviction process
// This is useful for debugging and reasoning about the eviction
type evictionJournal struct {
	evictionPerformed bool
	passOneNumTxs     uint32
	passOneNumSenders uint32
	passOneNumSteps   uint32
}

func (journal *evictionJournal) display() {
	log.Debug("Eviction.pass1:", "txs", journal.passOneNumTxs, "senders", journal.passOneNumSenders, "steps", journal.passOneNumSteps)
}

// Diagnose checks the state of the cache for inconsistencies and displays a summary
func (cache *TxCache) Diagnose(deep bool) {
	cache.diagnoseShallowly()
	if deep {
		cache.diagnoseDeeply()
	}
}

func (cache *TxCache) diagnoseShallowly() {
	sw := core.NewStopWatch()
	sw.Start("diagnose")

	sizeInBytes := cache.NumBytes()
	numTxsEstimate := int(cache.CountTx())
	numTxsInChunks := cache.txByHash.backingMap.Count()
	txsKeys := cache.txByHash.backingMap.Keys()
	numSendersEstimate := uint32(cache.CountSenders())
	numSendersInChunks := cache.txListBySender.backingMap.Count()
	numSendersInScoreChunks := cache.txListBySender.backingMap.CountSorted()
	sendersKeys := cache.txListBySender.backingMap.Keys()
	sendersKeysSorted := cache.txListBySender.backingMap.KeysSorted()
	sendersSnapshot := cache.txListBySender.getSnapshotAscending()

	sw.Stop("diagnose")
	duration := sw.GetMeasurement("diagnose")

	fine := (numSendersEstimate == numSendersInChunks && numSendersEstimate == numSendersInScoreChunks)
	fine = fine && (len(sendersKeys) == len(sendersKeysSorted) && len(sendersKeys) == len(sendersSnapshot))
	fine = fine && (int(numSendersEstimate) == len(sendersKeys))
	fine = fine && (numTxsEstimate == numTxsInChunks && numTxsEstimate == len(txsKeys))

	log.Debug("TxCache.diagnoseShallowly()", "name", cache.name, "duration", duration, "fine", fine)
	log.Debug("TxCache.Size:", "current", sizeInBytes, "max", cache.config.NumBytesThreshold)
	log.Debug("TxCache.NumSenders:", "estimate", numSendersEstimate, "inChunks", numSendersInChunks, "inScoreChunks", numSendersInScoreChunks)
	log.Debug("TxCache.NumSenders (continued):", "keys", len(sendersKeys), "keysSorted", len(sendersKeysSorted), "snapshot", len(sendersSnapshot))
	log.Debug("TxCache.NumTxs:", "estimate", numTxsEstimate, "inChunks", numTxsInChunks, "keys", len(txsKeys))
}

func (cache *TxCache) diagnoseDeeply() {
	sw := core.NewStopWatch()
	sw.Start("diagnose")

	journal := cache.checkInternalConsistency()
	cache.displaySendersSummary()

	sw.Stop("diagnose")
	duration := sw.GetMeasurement("diagnose")

	log.Debug("TxCache.diagnoseDeeply()", "name", cache.name, "duration", duration)
	journal.display()
	cache.displaySendersHistogram()
}

type internalConsistencyJournal struct {
	numInMapByHash        int
	numInMapBySender      int
	numMissingInMapByHash int
}

func (journal *internalConsistencyJournal) isFine() bool {
	return (journal.numInMapByHash == journal.numInMapBySender) && (journal.numMissingInMapByHash == 0)
}

func (journal *internalConsistencyJournal) display() {
	log.Debug("TxCache.internalConsistencyJournal:", "fine", journal.isFine(), "numInMapByHash", journal.numInMapByHash, "numInMapBySender", journal.numInMapBySender, "numMissingInMapByHash", journal.numMissingInMapByHash)
}

func (cache *TxCache) checkInternalConsistency() internalConsistencyJournal {
	internalMapByHash := cache.txByHash
	internalMapBySender := cache.txListBySender

	senders := internalMapBySender.getSnapshotAscending()
	numInMapByHash := len(internalMapByHash.keys())
	numInMapBySender := 0
	numMissingInMapByHash := 0

	for _, sender := range senders {
		numInMapBySender += int(sender.countTx())

		for _, hash := range sender.getTxHashes() {
			_, ok := internalMapByHash.getTx(string(hash))
			if !ok {
				numMissingInMapByHash++
			}
		}
	}

	return internalConsistencyJournal{
		numInMapByHash:        numInMapByHash,
		numInMapBySender:      numInMapBySender,
		numMissingInMapByHash: numMissingInMapByHash,
	}
}

func (cache *TxCache) displaySendersSummary() {
	if log.GetLevel() != logger.LogTrace {
		return
	}

	senders := cache.txListBySender.getSnapshotAscending()
	if len(senders) == 0 {
		return
	}

	var builder strings.Builder
	builder.WriteString("\n[#index (score)] address [nonce known / nonce vs lowestTxNonce] txs = numTxs, !numFailedSelections\n")

	for i, sender := range senders {
		address := hex.EncodeToString([]byte(sender.sender))
		accountNonce := sender.accountNonce.Get()
		accountNonceKnown := sender.accountNonceKnown.IsSet()
		numFailedSelections := sender.numFailedSelections.Get()
		score := sender.getLastComputedScore()
		numTxs := sender.countTxWithLock()

		lowestTxNonce := -1
		lowestTx := sender.getLowestNonceTx()
		if lowestTx != nil {
			lowestTxNonce = int(lowestTx.Tx.GetNonce())
		}

		_, _ = fmt.Fprintf(&builder, "[#%d (%d)] %s [%t / %d vs %d] txs = %d, !%d\n", i, score, address, accountNonceKnown, accountNonce, lowestTxNonce, numTxs, numFailedSelections)
	}

	summary := builder.String()
	log.Info("TxCache.displaySendersSummary()", "name", cache.name, "summary\n", summary)
}

func (cache *txListForSender) monitorOnFoundInsertionPlace(iterations *int, incomingTx *WrappedTransaction, hint listForSenderHint, length int) {
	if *iterations > numIterationsSuboptimalListTraversal {
		log.Trace("txListForSender.monitorOnFoundInsertionPlace: suboptimal",
			"iterations", *iterations,
			"nonce", incomingTx.Tx.GetNonce(),
			"sender", incomingTx.Tx.GetSndAddr(),
			"hint", hint.nonce,
			"length", length,
		)
	}
}

func (cache *txListForSender) monitorOnFoundTransaction(iterations *int, txToFind *WrappedTransaction, length int) {
	if *iterations > numIterationsSuboptimalListTraversal {
		log.Trace("txListForSender.monitorOnFoundTransaction: suboptimal",
			"iterations", *iterations,
			"nonce", txToFind.Tx.GetNonce(),
			"sender", txToFind.Tx.GetSndAddr(),
			"length", length,
		)
	}
}
