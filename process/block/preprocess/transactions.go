package preprocess

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/sliceUtil"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/storage/txcache"
)

var _ process.DataMarshalizer = (*transactions)(nil)
var _ process.PreProcessor = (*transactions)(nil)

var log = logger.GetOrCreate("process/block/preprocess")

// TODO: increase code coverage with unit test

type transactions struct {
	*basePreProcess
	chRcvAllTxs          chan bool
	onRequestTransaction func(shardID uint32, txHashes [][]byte)
	txsForCurrBlock      txsForBlock
	txPool               dataRetriever.ShardedDataCacherNotifier
	storage              dataRetriever.StorageService
	txProcessor          process.TransactionProcessor
	orderedTxs           map[string][]data.TransactionHandler
	orderedTxHashes      map[string][][]byte
	mutOrderedTxs        sync.RWMutex
	blockTracker         BlockTracker
	blockType            block.Type
	accountsInfo         map[string]*txShardInfo
	mutAccountsInfo      sync.RWMutex
	emptyAddress         []byte
}

// NewTransactionPreprocessor creates a new transaction preprocessor object
func NewTransactionPreprocessor(
	txDataPool dataRetriever.ShardedDataCacherNotifier,
	store dataRetriever.StorageService,
	hasher hashing.Hasher,
	marshalizer marshal.Marshalizer,
	txProcessor process.TransactionProcessor,
	shardCoordinator sharding.Coordinator,
	accounts state.AccountsAdapter,
	onRequestTransaction func(shardID uint32, txHashes [][]byte),
	economicsFee process.FeeHandler,
	gasHandler process.GasHandler,
	blockTracker BlockTracker,
	blockType block.Type,
	pubkeyConverter core.PubkeyConverter,
	blockSizeComputation BlockSizeComputationHandler,
	balanceComputation BalanceComputationHandler,
) (*transactions, error) {

	if check.IfNil(hasher) {
		return nil, process.ErrNilHasher
	}
	if check.IfNil(marshalizer) {
		return nil, process.ErrNilMarshalizer
	}
	if check.IfNil(txDataPool) {
		return nil, process.ErrNilTransactionPool
	}
	if check.IfNil(store) {
		return nil, process.ErrNilTxStorage
	}
	if check.IfNil(txProcessor) {
		return nil, process.ErrNilTxProcessor
	}
	if check.IfNil(shardCoordinator) {
		return nil, process.ErrNilShardCoordinator
	}
	if check.IfNil(accounts) {
		return nil, process.ErrNilAccountsAdapter
	}
	if onRequestTransaction == nil {
		return nil, process.ErrNilRequestHandler
	}
	if check.IfNil(economicsFee) {
		return nil, process.ErrNilEconomicsFeeHandler
	}
	if check.IfNil(gasHandler) {
		return nil, process.ErrNilGasHandler
	}
	if check.IfNil(blockTracker) {
		return nil, process.ErrNilBlockTracker
	}
	if check.IfNil(pubkeyConverter) {
		return nil, process.ErrNilPubkeyConverter
	}
	if check.IfNil(blockSizeComputation) {
		return nil, process.ErrNilBlockSizeComputationHandler
	}
	if check.IfNil(balanceComputation) {
		return nil, process.ErrNilBalanceComputationHandler
	}

	bpp := basePreProcess{
		hasher:               hasher,
		marshalizer:          marshalizer,
		shardCoordinator:     shardCoordinator,
		gasHandler:           gasHandler,
		economicsFee:         economicsFee,
		blockSizeComputation: blockSizeComputation,
		balanceComputation:   balanceComputation,
		accounts:             accounts,
		pubkeyConverter:      pubkeyConverter,
	}

	txs := transactions{
		basePreProcess:       &bpp,
		storage:              store,
		txPool:               txDataPool,
		onRequestTransaction: onRequestTransaction,
		txProcessor:          txProcessor,
		blockTracker:         blockTracker,
		blockType:            blockType,
	}

	txs.chRcvAllTxs = make(chan bool)
	txs.txPool.RegisterOnAdded(txs.receivedTransaction)

	txs.txsForCurrBlock.txHashAndInfo = make(map[string]*txInfo)
	txs.orderedTxs = make(map[string][]data.TransactionHandler)
	txs.orderedTxHashes = make(map[string][][]byte)
	txs.accountsInfo = make(map[string]*txShardInfo)

	txs.emptyAddress = make([]byte, txs.pubkeyConverter.Len())

	return &txs, nil
}

// waitForTxHashes waits for a call whether all the requested transactions appeared
func (txs *transactions) waitForTxHashes(waitTime time.Duration) error {
	select {
	case <-txs.chRcvAllTxs:
		return nil
	case <-time.After(waitTime):
		return process.ErrTimeIsOut
	}
}

// IsDataPrepared returns non error if all the requested transactions arrived and were saved into the pool
func (txs *transactions) IsDataPrepared(requestedTxs int, haveTime func() time.Duration) error {
	if requestedTxs > 0 {
		log.Debug("requested missing txs", "num txs", requestedTxs)
		err := txs.waitForTxHashes(haveTime())
		txs.txsForCurrBlock.mutTxsForBlock.Lock()
		missingTxs := txs.txsForCurrBlock.missingTxs
		txs.txsForCurrBlock.missingTxs = 0
		txs.txsForCurrBlock.mutTxsForBlock.Unlock()
		log.Debug("received missing txs", "num txs", requestedTxs-missingTxs, "requested", requestedTxs, "missing", missingTxs)
		if err != nil {
			return err
		}
	}

	return nil
}

// RemoveTxBlockFromPools removes transactions and miniblocks from associated pools
func (txs *transactions) RemoveTxBlockFromPools(body *block.Body, miniBlockPool storage.Cacher) error {
	return txs.removeDataFromPools(body, miniBlockPool, txs.txPool, txs.isMiniBlockCorrect)
}

// RestoreTxBlockIntoPools restores the transactions and miniblocks to associated pools
func (txs *transactions) RestoreTxBlockIntoPools(
	body *block.Body,
	miniBlockPool storage.Cacher,
) (int, error) {
	if check.IfNil(body) {
		return 0, process.ErrNilBlockBody
	}
	if check.IfNil(miniBlockPool) {
		return 0, process.ErrNilMiniBlockPool
	}

	txsRestored := 0
	for i := 0; i < len(body.MiniBlocks); i++ {
		miniBlock := body.MiniBlocks[i]
		if !txs.isMiniBlockCorrect(miniBlock.Type) {
			continue
		}

		strCache := process.ShardCacherIdentifier(miniBlock.SenderShardID, miniBlock.ReceiverShardID)
		txsBuff, err := txs.storage.GetAll(dataRetriever.TransactionUnit, miniBlock.TxHashes)
		if err != nil {
			log.Debug("tx from mini block was not found in TransactionUnit",
				"sender shard ID", miniBlock.SenderShardID,
				"receiver shard ID", miniBlock.ReceiverShardID,
				"num txs", len(miniBlock.TxHashes),
			)

			return txsRestored, err
		}

		for txHash, txBuff := range txsBuff {
			tx := transaction.Transaction{}
			err = txs.marshalizer.Unmarshal(&tx, txBuff)
			if err != nil {
				return txsRestored, err
			}

			txs.txPool.AddData([]byte(txHash), &tx, tx.Size(), strCache)
		}

		//TODO: Should be analyzed if restoring into pool only cross-shard miniblocks with destination in self shard,
		//would create problems or not
		if miniBlock.SenderShardID != txs.shardCoordinator.SelfId() {
			miniBlockHash, errHash := core.CalculateHash(txs.marshalizer, txs.hasher, miniBlock)
			if errHash != nil {
				return txsRestored, errHash
			}

			miniBlockPool.Put(miniBlockHash, miniBlock, miniBlock.Size())
		}

		txsRestored += len(miniBlock.TxHashes)
	}

	return txsRestored, nil
}

// ProcessBlockTransactions processes all the transaction from the block.Body, updates the state
func (txs *transactions) ProcessBlockTransactions(
	body *block.Body,
	haveTime func() bool,
) error {

	if txs.isBodyToMe(body) {
		return txs.processTxsToMe(body, haveTime)
	}

	if txs.isBodyFromMe(body) {
		return txs.processTxsFromMe(body, haveTime)
	}

	return process.ErrInvalidBody
}

func (txs *transactions) computeTxsToMe(body *block.Body) ([]*txcache.WrappedTransaction, error) {
	if check.IfNil(body) {
		return nil, process.ErrNilBlockBody
	}

	allTxs := make([]*txcache.WrappedTransaction, 0)
	for _, miniBlock := range body.MiniBlocks {
		shouldSkipMiniblock := miniBlock.SenderShardID == txs.shardCoordinator.SelfId() || !txs.isMiniBlockCorrect(miniBlock.Type)
		if shouldSkipMiniblock {
			continue
		}
		if miniBlock.Type != txs.blockType {
			return nil, fmt.Errorf("%w: block type: %s, sender shard id: %d, receiver shard id: %d",
				process.ErrInvalidMiniBlockType,
				miniBlock.Type,
				miniBlock.SenderShardID,
				miniBlock.ReceiverShardID)
		}

		txsFromMiniBlock, err := txs.computeTxsFromMiniBlock(miniBlock)
		if err != nil {
			return nil, err
		}

		allTxs = append(allTxs, txsFromMiniBlock...)
	}

	return allTxs, nil
}

func (txs *transactions) computeTxsFromMe(body *block.Body) ([]*txcache.WrappedTransaction, error) {
	if check.IfNil(body) {
		return nil, process.ErrNilBlockBody
	}

	allTxs := make([]*txcache.WrappedTransaction, 0)
	for _, miniBlock := range body.MiniBlocks {
		shouldSkipMiniblock := miniBlock.SenderShardID != txs.shardCoordinator.SelfId() || !txs.isMiniBlockCorrect(miniBlock.Type)
		if shouldSkipMiniblock {
			continue
		}

		txsFromMiniBlock, err := txs.computeTxsFromMiniBlock(miniBlock)
		if err != nil {
			return nil, err
		}

		allTxs = append(allTxs, txsFromMiniBlock...)
	}

	return allTxs, nil
}

func (txs *transactions) computeTxsFromMiniBlock(miniBlock *block.MiniBlock) ([]*txcache.WrappedTransaction, error) {
	txsFromMiniBlock := make([]*txcache.WrappedTransaction, 0, len(miniBlock.TxHashes))

	for i := 0; i < len(miniBlock.TxHashes); i++ {
		txHash := miniBlock.TxHashes[i]
		txs.txsForCurrBlock.mutTxsForBlock.RLock()
		txInfoFromMap, ok := txs.txsForCurrBlock.txHashAndInfo[string(txHash)]
		txs.txsForCurrBlock.mutTxsForBlock.RUnlock()

		if !ok || check.IfNil(txInfoFromMap.tx) {
			log.Warn("missing transaction in computeTxsFromMiniBlock", "type", miniBlock.Type, "txHash", txHash)
			return nil, process.ErrMissingTransaction
		}

		tx, ok := txInfoFromMap.tx.(*transaction.Transaction)
		if !ok {
			return nil, process.ErrWrongTypeAssertion
		}

		calculatedSenderShardId, err := txs.getShardFromAddress(tx.GetSndAddr())
		if err != nil {
			return nil, err
		}

		calculatedReceiverShardId, err := txs.getShardFromAddress(tx.GetRcvAddr())
		if err != nil {
			return nil, err
		}

		wrappedTx := &txcache.WrappedTransaction{
			Tx:              tx,
			TxHash:          txHash,
			SenderShardID:   calculatedSenderShardId,
			ReceiverShardID: calculatedReceiverShardId,
		}

		txsFromMiniBlock = append(txsFromMiniBlock, wrappedTx)
	}

	return txsFromMiniBlock, nil
}

func (txs *transactions) getShardFromAddress(address []byte) (uint32, error) {
	isEmptyAddress := bytes.Equal(address, txs.emptyAddress)
	if isEmptyAddress {
		return txs.shardCoordinator.SelfId(), nil
	}

	return txs.shardCoordinator.ComputeId(address), nil
}

func (txs *transactions) processTxsToMe(
	body *block.Body,
	haveTime func() bool,
) error {
	if check.IfNil(body) {
		return process.ErrNilBlockBody
	}

	txsToMe, err := txs.computeTxsToMe(body)
	if err != nil {
		return err
	}

	gasConsumedByMiniBlockInSenderShard := uint64(0)
	gasConsumedByMiniBlockInReceiverShard := uint64(0)
	totalGasConsumedInSelfShard := txs.gasHandler.TotalGasConsumed()

	log.Trace("processTxsToMe", "totalGasConsumedInSelfShard", totalGasConsumedInSelfShard)

	for index := range txsToMe {
		if !haveTime() {
			return process.ErrTimeIsOut
		}

		tx, ok := txsToMe[index].Tx.(*transaction.Transaction)
		if !ok {
			return process.ErrWrongTypeAssertion
		}

		txHash := txsToMe[index].TxHash
		senderShardID := txsToMe[index].SenderShardID
		receiverShardID := txsToMe[index].ReceiverShardID

		txs.saveAccountBalanceForAddress(tx.GetRcvAddr())

		err = txs.processAndRemoveBadTransaction(
			txHash,
			tx,
			senderShardID,
			receiverShardID)
		if err != nil {
			return err
		}

		err = txs.computeGasConsumed(
			senderShardID,
			receiverShardID,
			tx,
			txHash,
			&gasConsumedByMiniBlockInSenderShard,
			&gasConsumedByMiniBlockInReceiverShard,
			&totalGasConsumedInSelfShard)
		if err != nil {
			return err
		}
	}

	return nil
}

func (txs *transactions) processTxsFromMe(
	body *block.Body,
	haveTime func() bool,
) error {
	if check.IfNil(body) {
		return process.ErrNilBlockBody
	}

	txsFromMe, err := txs.computeTxsFromMe(body)
	if err != nil {
		return err
	}

	txcache.SortTransactionsBySenderAndNonce(txsFromMe)

	isShardStuckFalse := func(uint32) bool {
		return false
	}
	isMaxBlockSizeReachedFalse := func(int, int) bool {
		return false
	}

	calculatedMiniBlocks, err := txs.createAndProcessMiniBlocksFromMe(
		haveTime,
		isShardStuckFalse,
		isMaxBlockSizeReachedFalse,
		txsFromMe,
	)
	if err != nil {
		return err
	}

	if !haveTime() {
		return process.ErrTimeIsOut
	}

	receivedMiniBlocks := make(block.MiniBlockSlice, 0)
	for _, miniBlock := range body.MiniBlocks {
		if miniBlock.Type == block.InvalidBlock {
			continue
		}

		receivedMiniBlocks = append(receivedMiniBlocks, miniBlock)
	}

	receivedBodyHash, err := core.CalculateHash(txs.marshalizer, txs.hasher, &block.Body{MiniBlocks: receivedMiniBlocks})
	if err != nil {
		return err
	}

	calculatedBodyHash, err := core.CalculateHash(txs.marshalizer, txs.hasher, &block.Body{MiniBlocks: calculatedMiniBlocks})
	if err != nil {
		return err
	}

	if !bytes.Equal(receivedBodyHash, calculatedBodyHash) {
		for _, mb := range receivedMiniBlocks {
			log.Debug("received miniblock", "type", mb.Type, "sender", mb.SenderShardID, "receiver", mb.ReceiverShardID, "numTxs", len(mb.TxHashes))
		}

		for _, mb := range calculatedMiniBlocks {
			log.Debug("calculated miniblock", "type", mb.Type, "sender", mb.SenderShardID, "receiver", mb.ReceiverShardID, "numTxs", len(mb.TxHashes))
		}

		log.Debug("block body missmatch",
			"received body hash", receivedBodyHash,
			"calculated body hash", calculatedBodyHash)
		return process.ErrBlockBodyHashMismatch
	}

	return nil
}

// SaveTxBlockToStorage saves transactions from body into storage
func (txs *transactions) SaveTxBlockToStorage(body *block.Body) error {
	if check.IfNil(body) {
		return process.ErrNilBlockBody
	}

	for i := 0; i < len(body.MiniBlocks); i++ {
		miniBlock := body.MiniBlocks[i]
		if !txs.isMiniBlockCorrect(miniBlock.Type) {
			continue
		}

		err := txs.saveTxsToStorage(miniBlock.TxHashes, &txs.txsForCurrBlock, txs.storage, dataRetriever.TransactionUnit)
		if err != nil {
			return err
		}
	}

	return nil
}

// receivedTransaction is a call back function which is called when a new transaction
// is added in the transaction pool
func (txs *transactions) receivedTransaction(key []byte, value interface{}) {
	wrappedTx, ok := value.(*txcache.WrappedTransaction)
	if !ok {
		log.Warn("transactions.receivedTransaction", "error", process.ErrWrongTypeAssertion)
		return
	}

	receivedAllMissing := txs.baseReceivedTransaction(key, wrappedTx.Tx, &txs.txsForCurrBlock)

	if receivedAllMissing {
		txs.chRcvAllTxs <- true
	}
}

// CreateBlockStarted cleans the local cache map for processed/created transactions at this round
func (txs *transactions) CreateBlockStarted() {
	_ = core.EmptyChannel(txs.chRcvAllTxs)

	txs.txsForCurrBlock.mutTxsForBlock.Lock()
	txs.txsForCurrBlock.missingTxs = 0
	txs.txsForCurrBlock.txHashAndInfo = make(map[string]*txInfo)
	txs.txsForCurrBlock.mutTxsForBlock.Unlock()

	txs.mutOrderedTxs.Lock()
	txs.orderedTxs = make(map[string][]data.TransactionHandler)
	txs.orderedTxHashes = make(map[string][][]byte)
	txs.mutOrderedTxs.Unlock()

	txs.mutAccountsInfo.Lock()
	txs.accountsInfo = make(map[string]*txShardInfo)
	txs.mutAccountsInfo.Unlock()
}

// RequestBlockTransactions request for transactions if missing from a block.Body
func (txs *transactions) RequestBlockTransactions(body *block.Body) int {
	if check.IfNil(body) {
		return 0
	}

	return txs.computeExistingAndRequestMissingTxsForShards(body)
}

// computeExistingAndRequestMissingTxsForShards calculates what transactions are available and requests
// what are missing from block.Body
func (txs *transactions) computeExistingAndRequestMissingTxsForShards(body *block.Body) int {
	numMissingTxsForShard := txs.computeExistingAndRequestMissing(
		body,
		&txs.txsForCurrBlock,
		txs.chRcvAllTxs,
		txs.isMiniBlockCorrect,
		txs.txPool,
		txs.onRequestTransaction,
	)

	return numMissingTxsForShard
}

// processAndRemoveBadTransactions processed transactions, if txs are with error it removes them from pool
func (txs *transactions) processAndRemoveBadTransaction(
	txHash []byte,
	tx *transaction.Transaction,
	sndShardId uint32,
	dstShardId uint32,
) error {

	_, err := txs.txProcessor.ProcessTransaction(tx)
	isTxTargetedForDeletion := errors.Is(err, process.ErrLowerNonceInTransaction) || errors.Is(err, process.ErrInsufficientFee)
	if isTxTargetedForDeletion {
		strCache := process.ShardCacherIdentifier(sndShardId, dstShardId)
		txs.txPool.RemoveData(txHash, strCache)
	}

	if err != nil && !errors.Is(err, process.ErrFailedTransaction) {
		return err
	}

	txShardInfoToSet := &txShardInfo{senderShardID: sndShardId, receiverShardID: dstShardId}
	txs.txsForCurrBlock.mutTxsForBlock.Lock()
	txs.txsForCurrBlock.txHashAndInfo[string(txHash)] = &txInfo{tx: tx, txShardInfo: txShardInfoToSet}
	txs.txsForCurrBlock.mutTxsForBlock.Unlock()

	return err
}

func (txs *transactions) notifyTransactionProviderIfNeeded() {
	txs.mutAccountsInfo.RLock()
	for senderAddress, txShardInfoValue := range txs.accountsInfo {
		if txShardInfoValue.senderShardID != txs.shardCoordinator.SelfId() {
			continue
		}

		account, err := txs.getAccountForAddress([]byte(senderAddress))
		if err != nil {
			log.Debug("notifyTransactionProviderIfNeeded.getAccountForAddress", "error", err)
			continue
		}

		strCache := process.ShardCacherIdentifier(txShardInfoValue.senderShardID, txShardInfoValue.receiverShardID)
		txShardPool := txs.txPool.ShardDataStore(strCache)
		if check.IfNil(txShardPool) {
			log.Trace("notifyTransactionProviderIfNeeded", "error", process.ErrNilTxDataPool)
			continue
		}

		sortedTransactionsProvider := createSortedTransactionsProvider(txShardPool)
		sortedTransactionsProvider.NotifyAccountNonce([]byte(senderAddress), account.GetNonce())
	}
	txs.mutAccountsInfo.RUnlock()
}

func (txs *transactions) getAccountForAddress(address []byte) (state.AccountHandler, error) {
	account, err := txs.accounts.GetExistingAccount(address)
	if err != nil {
		return nil, err
	}

	return account, nil
}

// RequestTransactionsForMiniBlock requests missing transactions for a certain miniblock
func (txs *transactions) RequestTransactionsForMiniBlock(miniBlock *block.MiniBlock) int {
	if miniBlock == nil {
		return 0
	}

	missingTxsForMiniBlock := txs.computeMissingTxsForMiniBlock(miniBlock)
	if len(missingTxsForMiniBlock) > 0 {
		txs.onRequestTransaction(miniBlock.SenderShardID, missingTxsForMiniBlock)
	}

	return len(missingTxsForMiniBlock)
}

// computeMissingTxsForMiniBlock computes missing transactions for a certain miniblock
func (txs *transactions) computeMissingTxsForMiniBlock(miniBlock *block.MiniBlock) [][]byte {
	if miniBlock.Type != txs.blockType {
		return nil
	}

	missingTransactions := make([][]byte, 0, len(miniBlock.TxHashes))
	searchFirst := txs.blockType == block.InvalidBlock

	for _, txHash := range miniBlock.TxHashes {
		tx, _ := process.GetTransactionHandlerFromPool(
			miniBlock.SenderShardID,
			miniBlock.ReceiverShardID,
			txHash,
			txs.txPool,
			searchFirst)

		if tx == nil || tx.IsInterfaceNil() {
			missingTransactions = append(missingTransactions, txHash)
		}
	}

	return sliceUtil.TrimSliceSliceByte(missingTransactions)
}

// getAllTxsFromMiniBlock gets all the transactions from a miniblock into a new structure
func (txs *transactions) getAllTxsFromMiniBlock(
	mb *block.MiniBlock,
	haveTime func() bool,
) ([]*transaction.Transaction, [][]byte, error) {

	strCache := process.ShardCacherIdentifier(mb.SenderShardID, mb.ReceiverShardID)
	txCache := txs.txPool.ShardDataStore(strCache)
	if txCache == nil {
		return nil, nil, process.ErrNilTransactionPool
	}

	// verify if all transaction exists
	txsSlice := make([]*transaction.Transaction, 0, len(mb.TxHashes))
	txHashes := make([][]byte, 0, len(mb.TxHashes))
	for _, txHash := range mb.TxHashes {
		if !haveTime() {
			return nil, nil, process.ErrTimeIsOut
		}

		tmp, _ := txCache.Peek(txHash)
		if tmp == nil {
			return nil, nil, process.ErrNilTransaction
		}

		tx, ok := tmp.(*transaction.Transaction)
		if !ok {
			return nil, nil, process.ErrWrongTypeAssertion
		}
		txHashes = append(txHashes, txHash)
		txsSlice = append(txsSlice, tx)
	}

	return txsSlice, txHashes, nil
}

// CreateAndProcessMiniBlocks creates miniblocks from storage and processes the transactions added into the miniblocks
// as long as it has time
func (txs *transactions) CreateAndProcessMiniBlocks(haveTime func() bool) (block.MiniBlockSlice, error) {
	startTime := time.Now()
	sortedTxs, err := txs.computeSortedTxs(txs.shardCoordinator.SelfId(), txs.shardCoordinator.SelfId())
	elapsedTime := time.Since(startTime)
	if err != nil {
		log.Debug("computeSortedTxs", "error", err.Error())
		return make(block.MiniBlockSlice, 0), nil
	}

	if len(sortedTxs) == 0 {
		log.Trace("no transaction found after computeSortedTxs",
			"time [s]", elapsedTime,
		)
		return make(block.MiniBlockSlice, 0), nil
	}

	if !haveTime() {
		log.Debug("time is up after computeSortedTxs",
			"num txs", len(sortedTxs),
			"time [s]", elapsedTime,
		)
		return make(block.MiniBlockSlice, 0), nil
	}

	log.Debug("elapsed time to computeSortedTxs",
		"num txs", len(sortedTxs),
		"time [s]", elapsedTime,
	)

	startTime = time.Now()
	miniBlocks, err := txs.createAndProcessMiniBlocksFromMe(
		haveTime,
		txs.blockTracker.IsShardStuck,
		txs.blockSizeComputation.IsMaxBlockSizeReached,
		sortedTxs,
	)
	elapsedTime = time.Since(startTime)
	log.Debug("elapsed time to createAndProcessMiniBlocksFromMe",
		"time [s]", elapsedTime,
	)

	if err != nil {
		log.Debug("createAndProcessMiniBlocksFromMe", "error", err.Error())
		return make(block.MiniBlockSlice, 0), nil
	}

	return miniBlocks, nil
}

func (txs *transactions) createAndProcessMiniBlocksFromMe(
	haveTime func() bool,
	isShardStuck func(uint32) bool,
	isMaxBlockSizeReached func(int, int) bool,
	sortedTxs []*txcache.WrappedTransaction,
) (block.MiniBlockSlice, error) {
	log.Debug("createAndProcessMiniBlocksFromMe has been started")

	mapMiniBlocks := make(map[uint32]*block.MiniBlock)

	numTxsAdded := 0
	numTxsBad := 0
	numTxsSkipped := 0
	numTxsFailed := 0
	numTxsWithInitialBalanceConsumed := 0
	numCrossShardScCalls := 0

	totalTimeUsedForProcesss := time.Duration(0)
	totalTimeUsedForComputeGasConsumed := time.Duration(0)

	firstInvalidTxFound := false
	firstCrossShardScCallFound := false

	gasConsumedByMiniBlocksInSenderShard := uint64(0)
	mapGasConsumedByMiniBlockInReceiverShard := make(map[uint32]uint64)
	totalGasConsumedInSelfShard := txs.gasHandler.TotalGasConsumed()

	log.Debug("createAndProcessMiniBlocksFromMe", "totalGasConsumedInSelfShard", totalGasConsumedInSelfShard)

	senderAddressToSkip := []byte("")

	defer func() {
		go txs.notifyTransactionProviderIfNeeded()
	}()

	for shardID := uint32(0); shardID < txs.shardCoordinator.NumberOfShards(); shardID++ {
		mapMiniBlocks[shardID] = txs.createEmptyMiniBlock(txs.shardCoordinator.SelfId(), shardID, block.TxBlock)
	}

	mapMiniBlocks[core.MetachainShardId] = txs.createEmptyMiniBlock(txs.shardCoordinator.SelfId(), core.MetachainShardId, block.TxBlock)

	for index := range sortedTxs {
		if !haveTime() {
			log.Debug("time is out in createAndProcessMiniBlocksFromMe")
			break
		}

		tx, ok := sortedTxs[index].Tx.(*transaction.Transaction)
		if !ok {
			log.Debug("wrong type assertion",
				"hash", sortedTxs[index].TxHash,
				"sender shard", sortedTxs[index].SenderShardID,
				"receiver shard", sortedTxs[index].ReceiverShardID)
			continue
		}

		txHash := sortedTxs[index].TxHash
		senderShardID := sortedTxs[index].SenderShardID
		receiverShardID := sortedTxs[index].ReceiverShardID

		miniBlock, ok := mapMiniBlocks[receiverShardID]
		if !ok {
			log.Debug("miniblock is not created", "shard", receiverShardID)
			continue
		}

		numNewMiniBlocks := 0
		if len(miniBlock.TxHashes) == 0 {
			numNewMiniBlocks = 1
		}
		numNewTxs := 1

		isCrossShardScCall := receiverShardID != txs.shardCoordinator.SelfId() && core.IsSmartContractAddress(tx.RcvAddr)
		if isCrossShardScCall {
			if !firstCrossShardScCallFound {
				numNewMiniBlocks++
			}
			numNewTxs += core.MultiplyFactorForScCall
		}

		if isMaxBlockSizeReached(numNewMiniBlocks, numNewTxs) {
			log.Debug("max txs accepted in one block is reached",
				"num txs added", numTxsAdded,
				"total txs", len(sortedTxs))
			break
		}

		if isShardStuck != nil && isShardStuck(receiverShardID) {
			log.Trace("shard is stuck", "shard", receiverShardID)
			continue
		}

		if len(senderAddressToSkip) > 0 {
			if bytes.Equal(senderAddressToSkip, tx.GetSndAddr()) {
				numTxsSkipped++
				continue
			}
		}

		txMaxTotalCost := big.NewInt(0)
		isAddressSet := txs.balanceComputation.IsAddressSet(tx.GetSndAddr())
		if isAddressSet {
			txMaxTotalCost = txs.getTxMaxTotalCost(tx)
		}

		if isAddressSet {
			addressHasEnoughBalance := txs.balanceComputation.AddressHasEnoughBalance(tx.GetSndAddr(), txMaxTotalCost)
			if !addressHasEnoughBalance {
				numTxsWithInitialBalanceConsumed++
				continue
			}
		}

		snapshot := txs.accounts.JournalLen()

		gasConsumedByMiniBlockInReceiverShard := mapGasConsumedByMiniBlockInReceiverShard[receiverShardID]
		oldGasConsumedByMiniBlocksInSenderShard := gasConsumedByMiniBlocksInSenderShard
		oldGasConsumedByMiniBlockInReceiverShard := gasConsumedByMiniBlockInReceiverShard
		oldTotalGasConsumedInSelfShard := totalGasConsumedInSelfShard
		startTime := time.Now()
		err := txs.computeGasConsumed(
			senderShardID,
			receiverShardID,
			tx,
			txHash,
			&gasConsumedByMiniBlocksInSenderShard,
			&gasConsumedByMiniBlockInReceiverShard,
			&totalGasConsumedInSelfShard)
		elapsedTime := time.Since(startTime)
		totalTimeUsedForComputeGasConsumed += elapsedTime
		if err != nil {
			log.Trace("createAndProcessMiniBlocksFromMe.computeGasConsumed", "error", err)
			continue
		}

		mapGasConsumedByMiniBlockInReceiverShard[receiverShardID] = gasConsumedByMiniBlockInReceiverShard

		// execute transaction to change the trie root hash
		startTime = time.Now()
		err = txs.processAndRemoveBadTransaction(
			txHash,
			tx,
			senderShardID,
			receiverShardID,
		)
		elapsedTime = time.Since(startTime)
		totalTimeUsedForProcesss += elapsedTime

		txs.mutAccountsInfo.Lock()
		txs.accountsInfo[string(tx.GetSndAddr())] = &txShardInfo{senderShardID: senderShardID, receiverShardID: receiverShardID}
		txs.mutAccountsInfo.Unlock()

		if err != nil && !errors.Is(err, process.ErrFailedTransaction) {
			if errors.Is(err, process.ErrHigherNonceInTransaction) {
				senderAddressToSkip = tx.GetSndAddr()
			}

			numTxsBad++
			log.Trace("bad tx",
				"error", err.Error(),
				"hash", txHash,
			)

			err = txs.accounts.RevertToSnapshot(snapshot)
			if err != nil {
				log.Warn("revert to snapshot", "error", err.Error())
			}

			txs.gasHandler.RemoveGasConsumed([][]byte{txHash})
			txs.gasHandler.RemoveGasRefunded([][]byte{txHash})

			gasConsumedByMiniBlocksInSenderShard = oldGasConsumedByMiniBlocksInSenderShard
			mapGasConsumedByMiniBlockInReceiverShard[receiverShardID] = oldGasConsumedByMiniBlockInReceiverShard
			totalGasConsumedInSelfShard = oldTotalGasConsumedInSelfShard
			continue
		}

		senderAddressToSkip = []byte("")

		gasRefunded := txs.gasHandler.GasRefunded(txHash)
		mapGasConsumedByMiniBlockInReceiverShard[receiverShardID] -= gasRefunded
		if senderShardID == receiverShardID {
			gasConsumedByMiniBlocksInSenderShard -= gasRefunded
			totalGasConsumedInSelfShard -= gasRefunded
		}

		if errors.Is(err, process.ErrFailedTransaction) {
			if !firstInvalidTxFound {
				firstInvalidTxFound = true
				txs.blockSizeComputation.AddNumMiniBlocks(1)
			}

			txs.blockSizeComputation.AddNumTxs(1)
			numTxsFailed++
			continue
		}

		if isAddressSet {
			ok = txs.balanceComputation.SubBalanceFromAddress(tx.GetSndAddr(), txMaxTotalCost)
			if !ok {
				log.Error("createAndProcessMiniBlocksFromMe.SubBalanceFromAddress",
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
		if isCrossShardScCall {
			if !firstCrossShardScCallFound {
				firstCrossShardScCallFound = true
				txs.blockSizeComputation.AddNumMiniBlocks(1)
			}
			//we need to increment this as to account for the corresponding SCR hash
			txs.blockSizeComputation.AddNumTxs(core.MultiplyFactorForScCall)
			numCrossShardScCalls++
		}
		numTxsAdded++
	}

	miniBlocks := txs.getMiniBlockSliceFromMap(mapMiniBlocks)

	log.Debug("createAndProcessMiniBlocksFromMe",
		"self shard", txs.shardCoordinator.SelfId(),
		"gas consumed in sender shard", gasConsumedByMiniBlocksInSenderShard,
		"total gas consumed in self shard", totalGasConsumedInSelfShard)

	for _, miniBlock := range miniBlocks {
		log.Debug("mini block info",
			"type", miniBlock.Type,
			"sender shard", miniBlock.SenderShardID,
			"receiver shard", miniBlock.ReceiverShardID,
			"gas consumed in receiver shard", mapGasConsumedByMiniBlockInReceiverShard[miniBlock.ReceiverShardID],
			"txs added", len(miniBlock.TxHashes))
	}

	log.Debug("createAndProcessMiniBlocksFromMe has been finished",
		"total txs", len(sortedTxs),
		"num txs added", numTxsAdded,
		"num txs bad", numTxsBad,
		"num txs failed", numTxsFailed,
		"num txs skipped", numTxsSkipped,
		"num txs with initial balance consumed", numTxsWithInitialBalanceConsumed,
		"num cross shard sc calls", numCrossShardScCalls,
		"used time for computeGasConsumed", totalTimeUsedForComputeGasConsumed,
		"used time for processAndRemoveBadTransaction", totalTimeUsedForProcesss)

	return miniBlocks, nil
}

func (txs *transactions) createEmptyMiniBlock(
	senderShardID uint32,
	receiverShardID uint32,
	blockType block.Type,
) *block.MiniBlock {

	miniBlock := &block.MiniBlock{
		Type:            blockType,
		SenderShardID:   senderShardID,
		ReceiverShardID: receiverShardID,
		TxHashes:        make([][]byte, 0),
	}

	return miniBlock
}

func (txs *transactions) getMiniBlockSliceFromMap(mapMiniBlocks map[uint32]*block.MiniBlock) block.MiniBlockSlice {
	miniBlocks := make(block.MiniBlockSlice, 0)

	for shardID := uint32(0); shardID < txs.shardCoordinator.NumberOfShards(); shardID++ {
		if miniBlock, ok := mapMiniBlocks[shardID]; ok {
			if len(miniBlock.TxHashes) > 0 {
				miniBlocks = append(miniBlocks, miniBlock)
			}
		}
	}

	if miniBlock, ok := mapMiniBlocks[core.MetachainShardId]; ok {
		if len(miniBlock.TxHashes) > 0 {
			miniBlocks = append(miniBlocks, miniBlock)
		}
	}

	return miniBlocks
}

func (txs *transactions) computeSortedTxs(
	sndShardId uint32,
	dstShardId uint32,
) ([]*txcache.WrappedTransaction, error) {
	strCache := process.ShardCacherIdentifier(sndShardId, dstShardId)
	txShardPool := txs.txPool.ShardDataStore(strCache)

	if check.IfNil(txShardPool) {
		return nil, process.ErrNilTxDataPool
	}

	sortedTransactionsProvider := createSortedTransactionsProvider(txShardPool)
	log.Debug("computeSortedTxs.GetSortedTransactions")
	sortedTxs := sortedTransactionsProvider.GetSortedTransactions()

	txcache.SortTransactionsBySenderAndNonce(sortedTxs)
	return sortedTxs, nil
}

// ProcessMiniBlock processes all the transactions from a and saves the processed transactions in local cache complete miniblock
func (txs *transactions) ProcessMiniBlock(
	miniBlock *block.MiniBlock,
	haveTime func() bool,
	getNumOfCrossInterMbsAndTxs func() (int, int),
) ([][]byte, error) {

	if miniBlock.Type != block.TxBlock {
		return nil, process.ErrWrongTypeInMiniBlock
	}

	var err error
	processedTxHashes := make([][]byte, 0)
	miniBlockTxs, miniBlockTxHashes, err := txs.getAllTxsFromMiniBlock(miniBlock, haveTime)
	if err != nil {
		return nil, err
	}

	if txs.blockSizeComputation.IsMaxBlockSizeWithoutThrottleReached(1, len(miniBlockTxs)) {
		return nil, process.ErrMaxBlockSizeReached
	}

	defer func() {
		if err != nil {
			txs.gasHandler.RemoveGasConsumed(processedTxHashes)
			txs.gasHandler.RemoveGasRefunded(processedTxHashes)
		}
	}()

	gasConsumedByMiniBlockInSenderShard := uint64(0)
	gasConsumedByMiniBlockInReceiverShard := uint64(0)
	totalGasConsumedInSelfShard := txs.gasHandler.TotalGasConsumed()

	log.Trace("transactions.ProcessMiniBlock", "totalGasConsumedInSelfShard", totalGasConsumedInSelfShard)

	for index := range miniBlockTxs {
		if !haveTime() {
			err = process.ErrTimeIsOut
			return processedTxHashes, err
		}

		err = txs.computeGasConsumed(
			miniBlock.SenderShardID,
			miniBlock.ReceiverShardID,
			miniBlockTxs[index],
			miniBlockTxHashes[index],
			&gasConsumedByMiniBlockInSenderShard,
			&gasConsumedByMiniBlockInReceiverShard,
			&totalGasConsumedInSelfShard)
		if err != nil {
			return processedTxHashes, err
		}

		processedTxHashes = append(processedTxHashes, miniBlockTxHashes[index])
	}

	numOfOldCrossInterMbs, numOfOldCrossInterTxs := getNumOfCrossInterMbsAndTxs()

	for index := range miniBlockTxs {
		if !haveTime() {
			err = process.ErrTimeIsOut
			return processedTxHashes, err
		}

		txs.saveAccountBalanceForAddress(miniBlockTxs[index].GetRcvAddr())

		_, err = txs.txProcessor.ProcessTransaction(miniBlockTxs[index])
		if err != nil {
			return processedTxHashes, err
		}
	}

	numOfCrtCrossInterMbs, numOfCrtCrossInterTxs := getNumOfCrossInterMbsAndTxs()
	numOfNewCrossInterMbs := numOfCrtCrossInterMbs - numOfOldCrossInterMbs
	numOfNewCrossInterTxs := numOfCrtCrossInterTxs - numOfOldCrossInterTxs

	log.Trace("transactions.ProcessMiniBlock",
		"numOfOldCrossInterMbs", numOfOldCrossInterMbs, "numOfOldCrossInterTxs", numOfOldCrossInterTxs,
		"numOfCrtCrossInterMbs", numOfCrtCrossInterMbs, "numOfCrtCrossInterTxs", numOfCrtCrossInterTxs,
		"numOfNewCrossInterMbs", numOfNewCrossInterMbs, "numOfNewCrossInterTxs", numOfNewCrossInterTxs,
	)

	numMiniBlocks := 1 + numOfNewCrossInterMbs
	numTxs := len(miniBlockTxs) + numOfNewCrossInterTxs*core.MultiplyFactorForScCall
	if txs.blockSizeComputation.IsMaxBlockSizeWithoutThrottleReached(numMiniBlocks, numTxs) {
		return processedTxHashes, process.ErrMaxBlockSizeReached
	}

	txShardInfoToSet := &txShardInfo{senderShardID: miniBlock.SenderShardID, receiverShardID: miniBlock.ReceiverShardID}

	txs.txsForCurrBlock.mutTxsForBlock.Lock()
	for index, txHash := range miniBlockTxHashes {
		txs.txsForCurrBlock.txHashAndInfo[string(txHash)] = &txInfo{tx: miniBlockTxs[index], txShardInfo: txShardInfoToSet}
	}
	txs.txsForCurrBlock.mutTxsForBlock.Unlock()

	txs.blockSizeComputation.AddNumMiniBlocks(numMiniBlocks)
	txs.blockSizeComputation.AddNumTxs(numTxs)

	return nil, nil
}

// CreateMarshalizedData marshalizes transactions and creates and saves them into a new structure
func (txs *transactions) CreateMarshalizedData(txHashes [][]byte) ([][]byte, error) {
	mrsScrs, err := txs.createMarshalizedData(txHashes, &txs.txsForCurrBlock)
	if err != nil {
		return nil, err
	}

	return mrsScrs, nil
}

// GetAllCurrentUsedTxs returns all the transactions used at current creation / processing
func (txs *transactions) GetAllCurrentUsedTxs() map[string]data.TransactionHandler {
	txPool := make(map[string]data.TransactionHandler, len(txs.txsForCurrBlock.txHashAndInfo))

	txs.txsForCurrBlock.mutTxsForBlock.RLock()
	for txHash, txInfoFromMap := range txs.txsForCurrBlock.txHashAndInfo {
		txPool[txHash] = txInfoFromMap.tx
	}
	txs.txsForCurrBlock.mutTxsForBlock.RUnlock()

	return txPool
}

// IsInterfaceNil returns true if there is no value under the interface
func (txs *transactions) IsInterfaceNil() bool {
	return txs == nil
}

func (txs *transactions) isBodyToMe(body *block.Body) bool {
	for _, miniBlock := range body.MiniBlocks {
		if miniBlock.SenderShardID == txs.shardCoordinator.SelfId() {
			return false
		}
	}
	return true
}

func (txs *transactions) isBodyFromMe(body *block.Body) bool {
	for _, miniBlock := range body.MiniBlocks {
		if miniBlock.SenderShardID != txs.shardCoordinator.SelfId() {
			return false
		}
	}
	return true
}

func (txs *transactions) isMiniBlockCorrect(mbType block.Type) bool {
	return mbType == block.TxBlock || mbType == block.InvalidBlock
}
