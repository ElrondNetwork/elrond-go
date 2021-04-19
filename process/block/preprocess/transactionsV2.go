package preprocess

import (
	"bytes"
	"errors"
	"math/big"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/storage/txcache"
)

// TxType identifies the type of the tx
type TxType int32

const (
	nonScTx TxType = 0
	scTx    TxType = 1
)

const additionalTimeForCreatingScheduledMiniBlocks = 150 * time.Millisecond

type processedTxsInfo struct {
	numTxsAdded                        int
	numBadTxs                          int
	numTxsSkipped                      int
	numTxsFailed                       int
	numTxsWithInitialBalanceConsumed   int
	numCrossShardScCallsOrSpecialTxs   int
	totalTimeUsedForProcess            time.Duration
	totalTimeUsedForComputeGasConsumed time.Duration
}

type scheduledTxsInfo struct {
	numScheduledTxsAdded                        int
	numScheduledBadTxs                          int
	numScheduledTxsSkipped                      int
	numScheduledTxsWithInitialBalanceConsumed   int
	numScheduledCrossShardScCalls               int
	totalTimeUsedForScheduledVerify             time.Duration
	totalTimeUsedForScheduledComputeGasConsumed time.Duration
}

func (txs *transactions) createAndProcessMiniBlocksFromMeV2(
	haveTime func() bool,
	isShardStuck func(uint32) bool,
	isMaxBlockSizeReached func(int, int) bool,
	sortedTxs []*txcache.WrappedTransaction,
) (block.MiniBlockSlice, map[string]struct{}, error) {
	log.Debug("createAndProcessMiniBlocksFromMeV2 has been started")

	mapSCTxs := make(map[string]struct{})
	mapTxsForShard := make(map[uint32]int)
	mapScsForShard := make(map[uint32]int)
	mapCrossShardScCallsOrSpecialTxs := make(map[uint32]int)
	maxCrossShardScCallsOrSpecialTxsPerShard := 0

	processingInfo := processedTxsInfo{}

	firstInvalidTxFound := false
	firstCrossShardScCallOrSpecialTxFound := false

	mapGasConsumedByMiniBlockInReceiverShard := txs.initGasConsumed()

	gasInfo := gasConsumedInfo{
		gasConsumedByMiniBlockInReceiverShard: uint64(0),
		gasConsumedByMiniBlocksInSenderShard:  uint64(0),
		totalGasConsumedInSelfShard:           txs.gasHandler.TotalGasConsumed(),
	}

	log.Debug("createAndProcessMiniBlocksFromMeV2", "totalGasConsumedInSelfShard", gasInfo.totalGasConsumedInSelfShard)

	senderAddressToSkip := []byte("")

	defer func() {
		go txs.notifyTransactionProviderIfNeeded()
	}()

	mapMiniBlocks := txs.createEmptyMiniBlocks(block.TxBlock, nil)

	for index := range sortedTxs {
		if !haveTime() {
			log.Debug("time is out in createAndProcessMiniBlocksFromMeV2")
			break
		}

		txHash := sortedTxs[index].TxHash
		senderShardID := sortedTxs[index].SenderShardID
		receiverShardID := sortedTxs[index].ReceiverShardID

		if isShardStuck != nil && isShardStuck(receiverShardID) {
			log.Trace("shard is stuck", "shard", receiverShardID)
			continue
		}

		tx, ok := sortedTxs[index].Tx.(*transaction.Transaction)
		if !ok {
			log.Debug("wrong type assertion",
				"hash", txHash,
				"sender shard", senderShardID,
				"receiver shard", receiverShardID)
			continue
		}

		if len(senderAddressToSkip) > 0 {
			if bytes.Equal(senderAddressToSkip, tx.GetSndAddr()) {
				processingInfo.numTxsSkipped++
				continue
			}
		}

		_, txTypeDstShard := txs.txTypeHandler.ComputeTransactionType(tx)
		isReceiverSmartContractAddress := txTypeDstShard == process.SCDeployment || txTypeDstShard == process.SCInvoking
		firstMiniBlockSplitForReceiverShardFound := (isReceiverSmartContractAddress && mapScsForShard[receiverShardID] == 0 && mapTxsForShard[receiverShardID] > 0) ||
			(!isReceiverSmartContractAddress && mapTxsForShard[receiverShardID] == 0 && mapScsForShard[receiverShardID] > 0)

		miniBlock, ok := mapMiniBlocks[receiverShardID]
		if !ok {
			log.Debug("mini block is not created", "shard", receiverShardID)
			continue
		}

		numNewMiniBlocks := 0
		if len(miniBlock.TxHashes) == 0 || firstCrossShardScCallOrSpecialTxFound {
			numNewMiniBlocks = 1
		}
		numNewTxs := 1

		isCrossShardScCallOrSpecialTx := receiverShardID != txs.shardCoordinator.SelfId() &&
			(isReceiverSmartContractAddress || len(tx.RcvUserName) > 0)
		if isCrossShardScCallOrSpecialTx {
			if !firstCrossShardScCallOrSpecialTxFound {
				numNewMiniBlocks++
			}
			if mapCrossShardScCallsOrSpecialTxs[receiverShardID] >= maxCrossShardScCallsOrSpecialTxsPerShard {
				numNewTxs += core.AdditionalScrForEachScCallOrSpecialTx
			}
		}

		if isMaxBlockSizeReached(numNewMiniBlocks, numNewTxs) {
			log.Debug("max txs accepted in one block is reached",
				"num txs added", processingInfo.numTxsAdded,
				"total txs", len(sortedTxs))
			break
		}

		addressHasEnoughBalance, isAddressSet, txMaxTotalCost := txs.hasAddressEnoughInitialBalance(tx)
		if !addressHasEnoughBalance {
			processingInfo.numTxsWithInitialBalanceConsumed++
			continue
		}

		txType := nonScTx
		if isReceiverSmartContractAddress {
			txType = scTx
		}

		err := txs.processTransaction(
			tx,
			txType,
			txHash,
			senderShardID,
			receiverShardID,
			&gasInfo,
			mapGasConsumedByMiniBlockInReceiverShard,
			&processingInfo)
		if err != nil && !errors.Is(err, process.ErrFailedTransaction) {
			if errors.Is(err, process.ErrHigherNonceInTransaction) {
				senderAddressToSkip = tx.GetSndAddr()
			}
			continue
		}

		senderAddressToSkip = []byte("")

		gasRefunded := txs.gasHandler.GasRefunded(txHash)
		mapGasConsumedByMiniBlockInReceiverShard[receiverShardID][txType] -= gasRefunded
		if senderShardID == receiverShardID {
			gasInfo.gasConsumedByMiniBlocksInSenderShard -= gasRefunded
			gasInfo.totalGasConsumedInSelfShard -= gasRefunded
		}

		if errors.Is(err, process.ErrFailedTransaction) {
			if !firstInvalidTxFound {
				firstInvalidTxFound = true
				txs.blockSizeComputation.AddNumMiniBlocks(1)
			}

			txs.blockSizeComputation.AddNumTxs(1)
			processingInfo.numTxsFailed++
			continue
		}

		if isAddressSet {
			ok = txs.balanceComputation.SubBalanceFromAddress(tx.GetSndAddr(), txMaxTotalCost)
			if !ok {
				log.Error("createAndProcessMiniBlocksFromMeV2.SubBalanceFromAddress",
					"sender address", tx.GetSndAddr(),
					"tx max total cost", txMaxTotalCost,
					"err", process.ErrInsufficientFunds)
			}
		}

		if len(miniBlock.TxHashes) == 0 || firstMiniBlockSplitForReceiverShardFound {
			txs.blockSizeComputation.AddNumMiniBlocks(1)
		}

		miniBlock.TxHashes = append(miniBlock.TxHashes, txHash)
		txs.blockSizeComputation.AddNumTxs(1)
		if isCrossShardScCallOrSpecialTx {
			if !firstCrossShardScCallOrSpecialTxFound {
				firstCrossShardScCallOrSpecialTxFound = true
				txs.blockSizeComputation.AddNumMiniBlocks(1)
			}
			mapCrossShardScCallsOrSpecialTxs[receiverShardID]++
			if mapCrossShardScCallsOrSpecialTxs[receiverShardID] > maxCrossShardScCallsOrSpecialTxsPerShard {
				maxCrossShardScCallsOrSpecialTxsPerShard = mapCrossShardScCallsOrSpecialTxs[receiverShardID]
				//we need to increment this as to account for the corresponding SCR hash
				txs.blockSizeComputation.AddNumTxs(core.AdditionalScrForEachScCallOrSpecialTx)
			}
			processingInfo.numCrossShardScCallsOrSpecialTxs++
		}

		if isReceiverSmartContractAddress {
			mapSCTxs[string(txHash)] = struct{}{}
			mapScsForShard[receiverShardID]++
		} else {
			mapTxsForShard[receiverShardID]++
		}

		processingInfo.numTxsAdded++
	}

	miniBlocks := txs.getMiniBlockSliceFromMapV2(mapMiniBlocks, mapSCTxs)

	txs.displayProcessingResults(
		gasInfo.gasConsumedByMiniBlocksInSenderShard,
		gasInfo.totalGasConsumedInSelfShard,
		mapGasConsumedByMiniBlockInReceiverShard,
		mapTxsForShard,
		mapScsForShard,
		miniBlocks,
		&processingInfo,
		len(sortedTxs),
	)

	log.Debug("createAndProcessMiniBlocksFromMeV2 has been finished")

	return miniBlocks, mapSCTxs, nil
}

func (txs *transactions) initGasConsumed() map[uint32]map[TxType]uint64 {
	mapGasConsumedByMiniBlockInReceiverShard := make(map[uint32]map[TxType]uint64)
	for shardID := uint32(0); shardID < txs.shardCoordinator.NumberOfShards(); shardID++ {
		mapGasConsumedByMiniBlockInReceiverShard[shardID] = make(map[TxType]uint64)
	}

	mapGasConsumedByMiniBlockInReceiverShard[core.MetachainShardId] = make(map[TxType]uint64)
	return mapGasConsumedByMiniBlockInReceiverShard
}

func (txs *transactions) createEmptyMiniBlocks(blockType block.Type, reserved []byte) map[uint32]*block.MiniBlock {
	mapMiniBlocks := make(map[uint32]*block.MiniBlock)
	for shardID := uint32(0); shardID < txs.shardCoordinator.NumberOfShards(); shardID++ {
		mapMiniBlocks[shardID] = txs.createEmptyMiniBlock(txs.shardCoordinator.SelfId(), shardID, blockType, reserved)
	}

	mapMiniBlocks[core.MetachainShardId] = txs.createEmptyMiniBlock(txs.shardCoordinator.SelfId(), core.MetachainShardId, blockType, reserved)
	return mapMiniBlocks
}

func (txs *transactions) hasAddressEnoughInitialBalance(tx *transaction.Transaction) (bool, bool, *big.Int) {
	addressHasEnoughBalance := true
	txMaxTotalCost := big.NewInt(0)
	isAddressSet := txs.balanceComputation.IsAddressSet(tx.GetSndAddr())
	if isAddressSet {
		txMaxTotalCost = txs.getTxMaxTotalCost(tx)
		addressHasEnoughBalance = txs.balanceComputation.AddressHasEnoughBalance(tx.GetSndAddr(), txMaxTotalCost)
	}

	return addressHasEnoughBalance, isAddressSet, txMaxTotalCost
}

func (txs *transactions) processTransaction(
	tx *transaction.Transaction,
	txType TxType,
	txHash []byte,
	senderShardID uint32,
	receiverShardID uint32,
	gasInfo *gasConsumedInfo,
	mapGasConsumedByMiniBlockInReceiverShard map[uint32]map[TxType]uint64,
	processingInfo *processedTxsInfo,
) error {
	snapshot := txs.accounts.JournalLen()

	gasInfo.gasConsumedByMiniBlockInReceiverShard = mapGasConsumedByMiniBlockInReceiverShard[receiverShardID][txType]
	oldGasConsumedByMiniBlocksInSenderShard := gasInfo.gasConsumedByMiniBlocksInSenderShard
	oldGasConsumedByMiniBlockInReceiverShard := gasInfo.gasConsumedByMiniBlockInReceiverShard
	oldTotalGasConsumedInSelfShard := gasInfo.totalGasConsumedInSelfShard

	startTime := time.Now()
	err := txs.computeGasConsumed(
		senderShardID,
		receiverShardID,
		tx,
		txHash,
		gasInfo)
	elapsedTime := time.Since(startTime)
	processingInfo.totalTimeUsedForComputeGasConsumed += elapsedTime
	if err != nil {
		log.Trace("processTransaction.computeGasConsumed", "error", err)
		return err
	}

	mapGasConsumedByMiniBlockInReceiverShard[receiverShardID][txType] = gasInfo.gasConsumedByMiniBlockInReceiverShard

	// execute transaction to change the trie root hash
	startTime = time.Now()
	err = txs.processAndRemoveBadTransaction(
		txHash,
		tx,
		senderShardID,
		receiverShardID,
	)
	elapsedTime = time.Since(startTime)
	processingInfo.totalTimeUsedForProcess += elapsedTime

	txs.mutAccountsInfo.Lock()
	txs.accountsInfo[string(tx.GetSndAddr())] = &txShardInfo{senderShardID: senderShardID, receiverShardID: receiverShardID}
	txs.mutAccountsInfo.Unlock()

	if err != nil && !errors.Is(err, process.ErrFailedTransaction) {
		processingInfo.numBadTxs++
		log.Trace("bad tx", "error", err.Error(), "hash", txHash)

		errRevert := txs.accounts.RevertToSnapshot(snapshot)
		if errRevert != nil {
			log.Warn("revert to snapshot", "error", errRevert.Error())
		}

		txs.gasHandler.RemoveGasConsumed([][]byte{txHash})
		txs.gasHandler.RemoveGasRefunded([][]byte{txHash})

		gasInfo.gasConsumedByMiniBlocksInSenderShard = oldGasConsumedByMiniBlocksInSenderShard
		mapGasConsumedByMiniBlockInReceiverShard[receiverShardID][txType] = oldGasConsumedByMiniBlockInReceiverShard
		gasInfo.totalGasConsumedInSelfShard = oldTotalGasConsumedInSelfShard
	}

	return err
}

func (txs *transactions) getMiniBlockSliceFromMapV2(
	mapMiniBlocks map[uint32]*block.MiniBlock,
	mapSCTxs map[string]struct{},
) block.MiniBlockSlice {
	miniBlocks := make(block.MiniBlockSlice, 0)

	for shardID := uint32(0); shardID < txs.shardCoordinator.NumberOfShards(); shardID++ {
		if miniBlock, ok := mapMiniBlocks[shardID]; ok {
			if len(miniBlock.TxHashes) > 0 {
				miniBlocks = append(miniBlocks, splitMiniBlockIfNeeded(miniBlock, mapSCTxs)...)
			}
		}
	}

	if miniBlock, ok := mapMiniBlocks[core.MetachainShardId]; ok {
		if len(miniBlock.TxHashes) > 0 {
			miniBlocks = append(miniBlocks, splitMiniBlockIfNeeded(miniBlock, mapSCTxs)...)
		}
	}

	return miniBlocks
}

func splitMiniBlockIfNeeded(miniBlock *block.MiniBlock, mapSCTxs map[string]struct{}) block.MiniBlockSlice {
	splitMiniBlocks := make(block.MiniBlockSlice, 0)
	nonScTxHashes := make([][]byte, 0)
	scTxHashes := make([][]byte, 0)

	for _, txHash := range miniBlock.TxHashes {
		_, isSCTx := mapSCTxs[string(txHash)]
		if !isSCTx {
			nonScTxHashes = append(nonScTxHashes, txHash)
			continue
		}

		scTxHashes = append(scTxHashes, txHash)
	}

	if len(nonScTxHashes) > 0 {
		nonScMiniBlock := &block.MiniBlock{
			TxHashes:        nonScTxHashes,
			SenderShardID:   miniBlock.SenderShardID,
			ReceiverShardID: miniBlock.ReceiverShardID,
			Type:            miniBlock.Type,
			Reserved:        miniBlock.Reserved,
		}

		splitMiniBlocks = append(splitMiniBlocks, nonScMiniBlock)
	}

	if len(scTxHashes) > 0 {
		scMiniBlock := &block.MiniBlock{
			TxHashes:        scTxHashes,
			SenderShardID:   miniBlock.SenderShardID,
			ReceiverShardID: miniBlock.ReceiverShardID,
			Type:            miniBlock.Type,
			Reserved:        miniBlock.Reserved,
		}

		splitMiniBlocks = append(splitMiniBlocks, scMiniBlock)
	}

	return splitMiniBlocks
}

func (txs *transactions) createScheduledMiniBlocks(
	haveTime func() bool,
	haveAdditionalTime func() bool,
	isShardStuck func(uint32) bool,
	isMaxBlockSizeReached func(int, int) bool,
	sortedTxs []*txcache.WrappedTransaction,
	mapSCTxs map[string]struct{},
) block.MiniBlockSlice {
	log.Debug("createScheduledMiniBlocks has been started")

	mapMiniBlocks := txs.createEmptyMiniBlocks(block.TxBlock, []byte{byte(block.ScheduledBlock)})
	mapCrossShardScCallTxs := make(map[uint32]int)
	maxCrossShardScCallTxsPerShard := 0

	schedulingInfo := scheduledTxsInfo{}

	firstCrossShardScCallTxFound := false

	mapGasConsumedByMiniBlockInReceiverShard := txs.initGasConsumed()

	gasInfo := gasConsumedInfo{
		gasConsumedByMiniBlockInReceiverShard: uint64(0),
		gasConsumedByMiniBlocksInSenderShard:  uint64(0),
		totalGasConsumedInSelfShard:           uint64(0),
	}

	senderAddressToSkip := []byte("")

	for index := range sortedTxs {
		if !haveTime() && !haveAdditionalTime() {
			log.Debug("time is out in createScheduledMiniBlocks")
			break
		}

		txHash := sortedTxs[index].TxHash
		senderShardID := sortedTxs[index].SenderShardID
		receiverShardID := sortedTxs[index].ReceiverShardID

		_, alreadyAdded := mapSCTxs[string(txHash)]
		if alreadyAdded {
			continue
		}

		if isShardStuck != nil && isShardStuck(receiverShardID) {
			log.Trace("shard is stuck", "shard", receiverShardID)
			continue
		}

		tx, ok := sortedTxs[index].Tx.(*transaction.Transaction)
		if !ok {
			log.Debug("wrong type assertion",
				"hash", txHash,
				"sender shard", senderShardID,
				"receiver shard", receiverShardID)
			continue
		}

		if len(senderAddressToSkip) > 0 {
			if bytes.Equal(senderAddressToSkip, tx.GetSndAddr()) {
				schedulingInfo.numScheduledTxsSkipped++
				continue
			}
		}

		senderAddressToSkip = tx.GetSndAddr()

		_, txTypeDstShard := txs.txTypeHandler.ComputeTransactionType(tx)
		isReceiverSmartContractAddress := txTypeDstShard == process.SCDeployment || txTypeDstShard == process.SCInvoking
		if !isReceiverSmartContractAddress {
			continue
		}

		miniBlock, ok := mapMiniBlocks[receiverShardID]
		if !ok {
			log.Debug("scheduled mini block is not created", "shard", receiverShardID)
			continue
		}

		numNewMiniBlocks := 0
		if len(miniBlock.TxHashes) == 0 {
			numNewMiniBlocks = 1
		}
		numNewTxs := 1

		isCrossShardScCallTx := receiverShardID != txs.shardCoordinator.SelfId()
		if isCrossShardScCallTx {
			if !firstCrossShardScCallTxFound {
				numNewMiniBlocks++
			}
			if mapCrossShardScCallTxs[receiverShardID] >= maxCrossShardScCallTxsPerShard {
				numNewTxs += core.AdditionalScrForEachScCallOrSpecialTx
			}
		}

		if isMaxBlockSizeReached(numNewMiniBlocks, numNewTxs) {
			log.Debug("max txs accepted in one block is reached",
				"num scheduled txs added", schedulingInfo.numScheduledTxsAdded,
				"total txs", len(sortedTxs))
			break
		}

		addressHasEnoughBalance, isAddressSet, txMaxTotalCost := txs.hasAddressEnoughInitialBalance(tx)
		if !addressHasEnoughBalance {
			schedulingInfo.numScheduledTxsWithInitialBalanceConsumed++
			continue
		}

		err := txs.verifyTransaction(
			tx,
			scTx,
			txHash,
			senderShardID,
			receiverShardID,
			&gasInfo,
			mapGasConsumedByMiniBlockInReceiverShard,
			&schedulingInfo)
		if err != nil {
			continue
		}

		if isAddressSet {
			ok = txs.balanceComputation.SubBalanceFromAddress(tx.GetSndAddr(), txMaxTotalCost)
			if !ok {
				log.Error("createScheduledMiniBlocks.SubBalanceFromAddress",
					"sender address", tx.GetSndAddr(),
					"tx max total cost", txMaxTotalCost,
					"err", process.ErrInsufficientFunds)
			}
		}

		if len(miniBlock.TxHashes) == 0 {
			txs.blockSizeComputation.AddNumMiniBlocks(1)
		}

		miniBlock.TxHashes = append(miniBlock.TxHashes, txHash)
		txs.blockSizeComputation.AddNumTxs(1)
		if isCrossShardScCallTx {
			if !firstCrossShardScCallTxFound {
				firstCrossShardScCallTxFound = true
				txs.blockSizeComputation.AddNumMiniBlocks(1)
			}
			mapCrossShardScCallTxs[receiverShardID]++
			if mapCrossShardScCallTxs[receiverShardID] > maxCrossShardScCallTxsPerShard {
				maxCrossShardScCallTxsPerShard = mapCrossShardScCallTxs[receiverShardID]
				//we need to increment this as to account for the corresponding SCR hash
				txs.blockSizeComputation.AddNumTxs(core.AdditionalScrForEachScCallOrSpecialTx)
			}
			schedulingInfo.numScheduledCrossShardScCalls++
		}

		mapSCTxs[string(txHash)] = struct{}{}
		schedulingInfo.numScheduledTxsAdded++
		txs.scheduledTxsExecutionHandler.Add(txHash, tx)
	}

	miniBlocks := txs.getMiniBlockSliceFromMapV2(mapMiniBlocks, mapSCTxs)

	txs.displayProcessingResultsOfScheduledMiniBlocks(
		gasInfo.totalGasConsumedInSelfShard,
		mapGasConsumedByMiniBlockInReceiverShard,
		miniBlocks,
		&schedulingInfo,
		len(sortedTxs),
	)

	log.Debug("createScheduledMiniBlocks has been finished")

	return miniBlocks
}

func (txs *transactions) verifyTransaction(
	tx *transaction.Transaction,
	txType TxType,
	txHash []byte,
	senderShardID uint32,
	receiverShardID uint32,
	gasInfo *gasConsumedInfo,
	mapGasConsumedByMiniBlockInReceiverShard map[uint32]map[TxType]uint64,
	schedulingInfo *scheduledTxsInfo,
) error {
	gasInfo.gasConsumedByMiniBlockInReceiverShard = mapGasConsumedByMiniBlockInReceiverShard[receiverShardID][txType]
	oldGasConsumedByMiniBlocksInSenderShard := gasInfo.gasConsumedByMiniBlocksInSenderShard
	oldGasConsumedByMiniBlockInReceiverShard := gasInfo.gasConsumedByMiniBlockInReceiverShard
	oldTotalGasConsumedInSelfShard := gasInfo.totalGasConsumedInSelfShard

	startTime := time.Now()
	err := txs.computeGasConsumed(
		senderShardID,
		receiverShardID,
		tx,
		txHash,
		gasInfo)
	elapsedTime := time.Since(startTime)
	schedulingInfo.totalTimeUsedForScheduledComputeGasConsumed += elapsedTime
	if err != nil {
		log.Trace("verifyTransaction.computeGasConsumed", "error", err)
		return err
	}

	mapGasConsumedByMiniBlockInReceiverShard[receiverShardID][txType] = gasInfo.gasConsumedByMiniBlockInReceiverShard

	startTime = time.Now()
	err = txs.txProcessor.VerifyTransaction(tx)
	elapsedTime = time.Since(startTime)
	schedulingInfo.totalTimeUsedForScheduledVerify += elapsedTime

	txs.mutAccountsInfo.Lock()
	txs.accountsInfo[string(tx.GetSndAddr())] = &txShardInfo{senderShardID: senderShardID, receiverShardID: receiverShardID}
	txs.mutAccountsInfo.Unlock()

	if err != nil {
		isTxTargetedForDeletion := errors.Is(err, process.ErrLowerNonceInTransaction) || errors.Is(err, process.ErrInsufficientFee)
		if isTxTargetedForDeletion {
			strCache := process.ShardCacherIdentifier(senderShardID, receiverShardID)
			txs.txPool.RemoveData(txHash, strCache)
		}

		schedulingInfo.numScheduledBadTxs++
		log.Trace("bad tx", "error", err.Error(), "hash", txHash)

		txs.gasHandler.RemoveGasConsumed([][]byte{txHash})

		gasInfo.gasConsumedByMiniBlocksInSenderShard = oldGasConsumedByMiniBlocksInSenderShard
		mapGasConsumedByMiniBlockInReceiverShard[receiverShardID][txType] = oldGasConsumedByMiniBlockInReceiverShard
		gasInfo.totalGasConsumedInSelfShard = oldTotalGasConsumedInSelfShard

		return err
	}

	txShardInfoToSet := &txShardInfo{senderShardID: senderShardID, receiverShardID: receiverShardID}
	txs.txsForCurrBlock.mutTxsForBlock.Lock()
	txs.txsForCurrBlock.txHashAndInfo[string(txHash)] = &txInfo{tx: tx, txShardInfo: txShardInfoToSet}
	txs.txsForCurrBlock.mutTxsForBlock.Unlock()

	return nil
}

func (txs *transactions) displayProcessingResults(
	gasConsumedByMiniBlocksInSenderShard uint64,
	totalGasConsumedInSelfShard uint64,
	mapGasConsumedByMiniBlockInReceiverShard map[uint32]map[TxType]uint64,
	mapTxsForShard map[uint32]int,
	mapScsForShard map[uint32]int,
	miniBlocks block.MiniBlockSlice,
	processingInfo *processedTxsInfo,
	nbSortedTxs int,
) {
	if len(miniBlocks) > 0 {
		log.Debug("mini blocks from me created", "num", len(miniBlocks))
	}

	log.Debug("displayProcessingResults",
		"self shard", txs.shardCoordinator.SelfId(),
		"gas consumed in self shard for txs from me", gasConsumedByMiniBlocksInSenderShard,
		"total gas consumed in self shard", totalGasConsumedInSelfShard)

	for _, miniBlock := range miniBlocks {
		log.Debug("mini block info",
			"type", miniBlock.Type,
			"sender shard", miniBlock.SenderShardID,
			"receiver shard", miniBlock.ReceiverShardID,
			"gas consumed in receiver shard for non sc txs", mapGasConsumedByMiniBlockInReceiverShard[miniBlock.ReceiverShardID][nonScTx],
			"gas consumed in receiver shard for sc txs", mapGasConsumedByMiniBlockInReceiverShard[miniBlock.ReceiverShardID][scTx],
			"txs added", len(miniBlock.TxHashes),
			"non sc txs", mapTxsForShard[miniBlock.ReceiverShardID],
			"sc txs", mapScsForShard[miniBlock.ReceiverShardID])
	}

	log.Debug("transactions info", "total txs", nbSortedTxs,
		"num txs added", processingInfo.numTxsAdded,
		"num bad txs", processingInfo.numBadTxs,
		"num txs failed", processingInfo.numTxsFailed,
		"num txs skipped", processingInfo.numTxsSkipped,
		"num txs with initial balance consumed", processingInfo.numTxsWithInitialBalanceConsumed,
		"num cross shard sc calls or special txs", processingInfo.numCrossShardScCallsOrSpecialTxs,
		"used time for computeGasConsumed", processingInfo.totalTimeUsedForComputeGasConsumed,
		"used time for processAndRemoveBadTransaction", processingInfo.totalTimeUsedForProcess,
	)
}

func (txs *transactions) displayProcessingResultsOfScheduledMiniBlocks(
	totalGasConsumedInSelfShard uint64,
	mapGasConsumedByMiniBlockInReceiverShard map[uint32]map[TxType]uint64,
	miniBlocks block.MiniBlockSlice,
	schedulingInfo *scheduledTxsInfo,
	nbSortedTxs int,
) {
	if len(miniBlocks) > 0 {
		log.Debug("scheduled mini blocks from me created", "num", len(miniBlocks))
	}

	log.Debug("displayProcessingResultsOfScheduledMiniBlocks",
		"self shard", txs.shardCoordinator.SelfId(),
		"total gas consumed in self shard", totalGasConsumedInSelfShard)

	for _, miniBlock := range miniBlocks {
		log.Debug("scheduled mini block info",
			"type", "ScheduledBlock",
			"sender shard", miniBlock.SenderShardID,
			"receiver shard", miniBlock.ReceiverShardID,
			"gas consumed in receiver shard", mapGasConsumedByMiniBlockInReceiverShard[miniBlock.ReceiverShardID][scTx],
			"txs added", len(miniBlock.TxHashes))
	}

	log.Debug("scheduled transactions info", "total txs", nbSortedTxs,
		"num scheduled txs added", schedulingInfo.numScheduledTxsAdded,
		"num scheduled bad txs", schedulingInfo.numScheduledBadTxs,
		"num scheduled txs skipped", schedulingInfo.numScheduledTxsSkipped,
		"num scheduled txs with initial balance consumed", schedulingInfo.numScheduledTxsWithInitialBalanceConsumed,
		"num scheduled cross shard sc calls", schedulingInfo.numScheduledCrossShardScCalls,
		"used time for scheduled computeGasConsumed", schedulingInfo.totalTimeUsedForScheduledComputeGasConsumed,
		"used time for scheduled VerifyTransaction", schedulingInfo.totalTimeUsedForScheduledVerify,
	)
}

func getAdditionalTimeFunc() func() bool {
	startTime := time.Now()
	return func() bool {
		return additionalTimeForCreatingScheduledMiniBlocks > time.Since(startTime)
	}
}