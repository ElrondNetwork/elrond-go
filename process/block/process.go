package block

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/data"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/blockchain"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/ElrondNetwork/elrond-go-sandbox/display"
	"github.com/ElrondNetwork/elrond-go-sandbox/hashing"
	"github.com/ElrondNetwork/elrond-go-sandbox/logger"
	"github.com/ElrondNetwork/elrond-go-sandbox/marshal"
	"github.com/ElrondNetwork/elrond-go-sandbox/process"
	"github.com/ElrondNetwork/elrond-go-sandbox/sharding"
	"github.com/ElrondNetwork/elrond-go-sandbox/storage"
)

var log = logger.NewDefaultLogger()

var txCounterMutex = sync.RWMutex{}
var txsCurrentBlockProcessed = 0
var txsTotalProcessed = 0

const maxTransactionsInBlock = 15000

// blockProcessor implements blockProcessor interface and actually it tries to execute block
type blockProcessor struct {
	dataPool             data.PoolsHolder
	hasher               hashing.Hasher
	marshalizer          marshal.Marshalizer
	txProcessor          process.TransactionProcessor
	ChRcvAllTxs          chan bool
	OnRequestTransaction func(destShardID uint32, txHash []byte)
	requestedTxHashes    map[string]bool
	mut                  sync.RWMutex
	mutCrossData         sync.RWMutex
	accounts             state.AccountsAdapter
	shardCoordinator     sharding.Coordinator
	forkDetector         process.ForkDetector
	crossTxsForBlock     map[string]*transaction.Transaction
	miniBlockMissing     map[string]uint32
	OnRequestMiniBlocks  func(hashes map[uint32][][]byte)
}

// NewBlockProcessor creates a new blockProcessor object
func NewBlockProcessor(
	dataPool data.PoolsHolder,
	hasher hashing.Hasher,
	marshalizer marshal.Marshalizer,
	txProcessor process.TransactionProcessor,
	accounts state.AccountsAdapter,
	shardCoordinator sharding.Coordinator,
	forkDetector process.ForkDetector,
	requestTransactionHandler func(destShardID uint32, txHash []byte),
	requestMiniBlockHandler func(hashes map[uint32][][]byte),
) (*blockProcessor, error) {

	if dataPool == nil {
		return nil, process.ErrNilDataPoolHolder
	}

	if hasher == nil {
		return nil, process.ErrNilHasher
	}

	if marshalizer == nil {
		return nil, process.ErrNilMarshalizer
	}

	if txProcessor == nil {
		return nil, process.ErrNilTxProcessor
	}

	if accounts == nil {
		return nil, process.ErrNilAccountsAdapter
	}

	if shardCoordinator == nil {
		return nil, process.ErrNilShardCoordinator
	}

	if forkDetector == nil {
		return nil, process.ErrNilForkDetector
	}

	if requestTransactionHandler == nil {
		return nil, process.ErrNilTransactionHandler
	}

	if requestMiniBlockHandler == nil {
		return nil, process.ErrNilMiniBlocksRequestHandler
	}

	bp := blockProcessor{
		dataPool:         dataPool,
		hasher:           hasher,
		marshalizer:      marshalizer,
		txProcessor:      txProcessor,
		accounts:         accounts,
		shardCoordinator: shardCoordinator,
		forkDetector:     forkDetector,
	}

	bp.ChRcvAllTxs = make(chan bool)
	bp.OnRequestTransaction = requestTransactionHandler

	transactionPool := bp.dataPool.Transactions()
	if transactionPool == nil {
		return nil, process.ErrNilTransactionPool
	}
	transactionPool.RegisterHandler(bp.receivedTransaction)

	bp.OnRequestMiniBlocks = requestMiniBlockHandler
	bp.crossTxsForBlock = make(map[string]*transaction.Transaction, 0)
	bp.miniBlockMissing = make(map[string]uint32, 0)

	metaBlockPool := bp.dataPool.MetaBlocks()
	if metaBlockPool == nil {
		return nil, process.ErrNilMetaBlockPool
	}
	metaBlockPool.RegisterHandler(bp.receivedMetaBlock)

	miniBlockPool := bp.dataPool.MiniBlocks()
	if miniBlockPool == nil {
		return nil, process.ErrNilMiniBlockPool
	}
	miniBlockPool.RegisterHandler(bp.receivedMiniBlock)

	return &bp, nil
}

func checkForNils(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler) error {
	if blockChain == nil {
		return process.ErrNilBlockChain
	}
	if header == nil {
		return process.ErrNilBlockHeader
	}
	if body == nil {
		return process.ErrNilMiniBlocks
	}
	return nil
}

// RevertAccountState reverts the account state for cleanup failed process
func (bp *blockProcessor) RevertAccountState() {
	err := bp.accounts.RevertToSnapshot(0)
	if err != nil {
		log.Error(err.Error())
	}
}

// ProcessBlock processes a block. It returns nil if all ok or the specific error
func (bp *blockProcessor) ProcessBlock(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
	err := checkForNils(blockChain, header, body)
	if err != nil {
		return err
	}

	blockBody, ok := body.(block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	blockHeader, ok := header.(*block.Header)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	if haveTime == nil {
		return process.ErrNilHaveTimeHandler
	}

	concreteBlockChain, ok := blockChain.(*blockchain.BlockChain)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	err = bp.validateHeader(concreteBlockChain, blockHeader)
	if err != nil {
		return err
	}

	requestedTxs := bp.requestBlockTransactions(blockBody)

	if haveTime() < 0 {
		return process.ErrTimeIsOut
	}

	if requestedTxs > 0 {
		log.Info(fmt.Sprintf("requested %d missing txs\n", requestedTxs))
		err = bp.waitForTxHashes(haveTime())
		log.Info(fmt.Sprintf("received %d missing txs\n", requestedTxs-len(bp.requestedTxHashes)))
		if err != nil {
			return err
		}
	}

	if bp.accounts.JournalLen() != 0 {
		return process.ErrAccountStateDirty
	}

	defer func() {
		if err != nil {
			bp.RevertAccountState()
		}
	}()

	err = bp.processBlockTransactions(blockBody, int32(blockHeader.Round), haveTime)
	if err != nil {
		return err
	}

	if !bp.verifyStateRoot(header.GetRootHash()) {
		err = process.ErrRootStateMissmatch
		return err
	}

	return nil
}

// RemoveBlockInfoFromPool removes the TxBlock transactions from associated tx pools
func (bp *blockProcessor) RemoveBlockInfoFromPool(body data.BodyHandler) error {
	if body == nil {
		return process.ErrNilTxBlockBody
	}

	blockBody, ok := body.(block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	transactionPool := bp.dataPool.Transactions()
	if transactionPool == nil {
		return process.ErrNilTransactionPool
	}

	for i := 0; i < len(blockBody); i++ {
		strCache := process.ShardCacherIdentifier(bp.shardCoordinator.SelfId(), blockBody[i].ShardID)
		transactionPool.RemoveSetOfDataFromPool((blockBody)[i].TxHashes, strCache)
	}

	return nil
}

// verifyStateRoot verifies the state root hash given as parameter against the
// Merkle trie root hash stored for accounts and returns if equal or not
func (bp *blockProcessor) verifyStateRoot(rootHash []byte) bool {
	return bytes.Equal(bp.accounts.RootHash(), rootHash)
}

// CreateBlockBody creates a a list of miniblocks by filling them with transactions out of the transactions pools
// as long as the transactions limit for the block has not been reached and there is still time to add transactions
func (bp *blockProcessor) CreateBlockBody(round int32, haveTime func() bool) (data.BodyHandler, error) {
	miniBlocks, err := bp.createMiniBlocks(bp.shardCoordinator.NumberOfShards(), maxTransactionsInBlock, round, haveTime)

	if err != nil {
		return nil, err
	}

	return miniBlocks, nil
}

// CreateGenesisBlockBody creates the genesis block body from map of account balances
func (bp *blockProcessor) CreateGenesisBlock(balances map[string]*big.Int) (rootHash []byte, err error) {
	// TODO: balances map should be validated
	return bp.txProcessor.SetBalancesToTrie(balances)
}

// getRootHash returns the accounts merkle tree root hash
func (bp *blockProcessor) getRootHash() []byte {
	return bp.accounts.RootHash()
}

func (bp *blockProcessor) validateHeader(blockChain *blockchain.BlockChain, header *block.Header) error {
	// basic validation was already done on interceptor
	if blockChain.GetCurrentBlockHeader() == nil {
		if !bp.isFirstBlockInEpoch(header) {
			return process.ErrWrongNonceInBlock
		}
	} else {
		if bp.isCorrectNonce(blockChain.GetCurrentBlockHeader().GetNonce(), header.Nonce) {
			return process.ErrWrongNonceInBlock
		}

		if !bytes.Equal(header.PrevHash, blockChain.GetCurrentBlockHeaderHash()) {

			log.Info(fmt.Sprintf(
				"header.Nonce = %d has header.PrevHash = %s and blockChain.CurrentBlockHeader.Nonce = %d has blockChain.CurrentBlockHeaderHash = %s\n",
				header.Nonce,
				toB64(header.PrevHash),
				blockChain.GetCurrentBlockHeader().GetNonce(),
				toB64(blockChain.GetCurrentBlockHeaderHash())))

			return process.ErrInvalidBlockHash
		}
	}

	return nil
}

func (bp *blockProcessor) isCorrectNonce(currentBlockNonce, receivedBlockNonce uint64) bool {
	return currentBlockNonce+1 != receivedBlockNonce
}

func (bp *blockProcessor) isFirstBlockInEpoch(header *block.Header) bool {
	return header.Round == 0
}

func (bp *blockProcessor) processBlockTransactions(body block.Body, round int32, haveTime func() time.Duration) error {
	// basic validation already done in interceptors
	txPool := bp.dataPool.Transactions()

	for i := 0; i < len(body); i++ {
		miniBlock := body[i]
		shardId := miniBlock.ShardID

		for j := 0; j < len(miniBlock.TxHashes); j++ {
			if haveTime() < 0 {
				return process.ErrTimeIsOut
			}

			txHash := miniBlock.TxHashes[j]
			tx := bp.getTransactionFromPool(shardId, txHash)
			err := bp.processAndRemoveBadTransaction(
				txHash,
				tx,
				txPool,
				round,
				bp.shardCoordinator.SelfId(),
				miniBlock.ShardID,
			)

			if err != nil {
				return err
			}
		}
	}
	return nil
}

// CommitBlock commits the block in the blockchain if everything was checked successfully
func (bp *blockProcessor) CommitBlock(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler) error {
	var err error
	defer func() {
		if err != nil {
			bp.RevertAccountState()
		}
	}()

	err = checkForNils(blockChain, header, body)
	if err != nil {
		return err
	}

	buff, err := bp.marshalizer.Marshal(header)
	if err != nil {
		return err
	}

	headerHash := bp.hasher.Compute(string(buff))
	err = blockChain.Put(data.BlockHeaderUnit, headerHash, buff)
	if err != nil {
		return err
	}

	blockBody, ok := body.(block.Body)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	blockHeader, ok := header.(*block.Header)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	for i := 0; i < len(blockBody); i++ {
		buff, err = bp.marshalizer.Marshal((blockBody)[i])
		if err != nil {
			return err
		}

		miniBlockHash := bp.hasher.Compute(string(buff))
		err = blockChain.Put(data.MiniBlockUnit, miniBlockHash, buff)
		if err != nil {
			return err
		}
	}

	headerNoncePool := bp.dataPool.HeadersNonces()
	if headerNoncePool == nil {
		err = process.ErrNilDataPoolHolder
		return err
	}

	_ = headerNoncePool.Put(header.GetNonce(), headerHash)

	for i := 0; i < len(blockBody); i++ {
		miniBlock := (blockBody)[i]
		for j := 0; j < len(miniBlock.TxHashes); j++ {
			txHash := miniBlock.TxHashes[j]
			tx := bp.getTransactionFromPool(miniBlock.ShardID, txHash)
			if tx == nil {
				err = process.ErrMissingTransaction
				return err
			}

			buff, err = bp.marshalizer.Marshal(tx)
			if err != nil {
				return err
			}

			err = blockChain.Put(data.TransactionUnit, txHash, buff)
			if err != nil {
				return err
			}
		}
	}

	_, err = bp.accounts.Commit()
	if err != nil {
		return err
	}

	errNotCritical := bp.RemoveBlockInfoFromPool(body)
	if errNotCritical != nil {
		log.Info(errNotCritical.Error())
	}

	errNotCritical = bp.forkDetector.AddHeader(blockHeader, headerHash, true)
	if errNotCritical != nil {
		log.Info(errNotCritical.Error())
	}

	err = blockChain.SetCurrentBlockBody(blockBody)
	if err != nil {
		return err
	}

	err = blockChain.SetCurrentBlockHeader(blockHeader)
	if err != nil {
		return err
	}

	blockChain.SetCurrentBlockHeaderHash(headerHash)

	// write data to log
	go bp.displayBlockchain(blockHeader, blockBody)

	return nil
}

// getTransactionFromPool gets the transaction from a given shard id and a given transaction hash
func (bp *blockProcessor) getTransactionFromPool(destShardID uint32, txHash []byte) *transaction.Transaction {
	txPool := bp.dataPool.Transactions()
	if txPool == nil {
		log.Error(process.ErrNilTransactionPool.Error())
		return nil
	}

	strCache := process.ShardCacherIdentifier(bp.shardCoordinator.SelfId(), destShardID)
	txStore := txPool.ShardDataStore(strCache)
	if txStore == nil {
		log.Error(process.ErrNilTxStorage.Error())
		return nil
	}

	val, ok := txStore.Peek(txHash)
	if !ok {
		return nil
	}

	v := val.(*transaction.Transaction)
	return v
}

// getCrossTransactionFromPool gets the transaction from a given shard id and a given transaction hash
func (bp *blockProcessor) getCrossTransactionFromPool(senderShardID, destShardID uint32, txHash []byte) *transaction.Transaction {
	txPool := bp.dataPool.Transactions()
	if txPool == nil {
		log.Error(process.ErrNilTransactionPool.Error())
		return nil
	}

	strCache := process.ShardCacherIdentifier(senderShardID, destShardID)
	txStore := txPool.ShardDataStore(strCache)
	if txStore == nil {
		log.Error(process.ErrNilTxStorage.Error())
		return nil
	}

	val, ok := txStore.Peek(txHash)
	if !ok {
		return nil
	}

	v := val.(*transaction.Transaction)
	return v
}

// receivedTransaction is a call back function which is called when a new transaction
// is added in the transaction pool
func (bp *blockProcessor) receivedTransaction(txHash []byte) {
	bp.mut.Lock()
	if len(bp.requestedTxHashes) > 0 {
		if bp.requestedTxHashes[string(txHash)] {
			delete(bp.requestedTxHashes, string(txHash))
		}
		lenReqTxHashes := len(bp.requestedTxHashes)
		bp.mut.Unlock()

		if lenReqTxHashes == 0 {
			bp.ChRcvAllTxs <- true
		}
		return
	}
	bp.mut.Unlock()
}

// receivedMetaBlock is a callback function when a new metablock was received
// upon receiving, it parses the new metablock and requests miniblocks and transactions
// which destination is the current shard
func (bp *blockProcessor) receivedMetaBlock(metaBlockHash []byte) {
	metaBlockCache := bp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return
	}

	miniBlockCache := bp.dataPool.MiniBlocks()
	if miniBlockCache == nil {
		return
	}

	metaBlock, ok := metaBlockCache.Peek(metaBlockHash)
	if !ok {
		return
	}

	hdr, ok := metaBlock.(data.HeaderHandler)
	if !ok {
		return
	}

	currentMissingMiniBlocks := make(map[uint32][][]byte, 0)
	hashSnd := hdr.GetMiniBlockHeadersWithDst(bp.shardCoordinator.SelfId())

	bp.mutCrossData.Lock()
	for k, senderShardId := range hashSnd {
		miniVal, _ := miniBlockCache.Peek([]byte(k))
		if miniVal == nil {
			bp.miniBlockMissing[k] = senderShardId
			currentMissingMiniBlocks[senderShardId] = append(currentMissingMiniBlocks[senderShardId], []byte(k))
			continue
		}
	}
	bp.mutCrossData.Unlock()
	go bp.OnRequestMiniBlocks(currentMissingMiniBlocks)
}

// receivedMiniBlock is a callback function when a new miniblock was received
// it will further ask for missing transactions
func (bp *blockProcessor) receivedMiniBlock(miniBlockHash []byte) {
	metaBlockCache := bp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return
	}

	miniBlockCache := bp.dataPool.MiniBlocks()
	if miniBlockCache == nil {
		return
	}

	bp.mutCrossData.Lock()
	defer bp.mutCrossData.Unlock()

	val, ok := miniBlockCache.Peek(miniBlockHash)
	if !ok {
		return
	}

	miniBlock, ok := val.(block.MiniBlock)
	if !ok {
		return
	}

	srId, ok := bp.miniBlockMissing[string(miniBlockHash)]
	if !ok {
		return
	}

	// request transactions
	bp.mut.Lock()
	for _, txHash := range miniBlock.TxHashes {
		tx := bp.getCrossTransactionFromPool(srId, miniBlock.ShardID, txHash)
		if tx == nil {
			go bp.OnRequestTransaction(srId, txHash)
		}
	}
	bp.mut.Unlock()
}

func (bp *blockProcessor) requestBlockTransactions(body block.Body) int {
	bp.mut.Lock()
	requestedTxs := 0
	missingTxsForShards := bp.computeMissingTxsForShards(body)
	bp.requestedTxHashes = make(map[string]bool)
	if bp.OnRequestTransaction != nil {
		for shardId, txHashes := range missingTxsForShards {
			for _, txHash := range txHashes {
				requestedTxs++
				bp.requestedTxHashes[string(txHash)] = true
				go bp.OnRequestTransaction(shardId, txHash)
			}
		}
	}
	bp.mut.Unlock()
	return requestedTxs
}

func (bp *blockProcessor) computeMissingTxsForShards(body block.Body) map[uint32][][]byte {
	missingTxsForShard := make(map[uint32][][]byte)

	for i := 0; i < len(body); i++ {
		miniBlock := body[i]
		shardId := miniBlock.ShardID
		currentShardMissingTransactions := make([][]byte, 0)

		for j := 0; j < len(miniBlock.TxHashes); j++ {
			txHash := miniBlock.TxHashes[j]
			tx := bp.getTransactionFromPool(shardId, txHash)

			if tx == nil {
				currentShardMissingTransactions = append(currentShardMissingTransactions, txHash)
			}
		}

		if len(currentShardMissingTransactions) > 0 {
			missingTxsForShard[shardId] = currentShardMissingTransactions
		}
	}

	return missingTxsForShard
}

func (bp *blockProcessor) processAndRemoveBadTransaction(
	transactionHash []byte,
	transaction *transaction.Transaction,
	txPool data.ShardedDataCacherNotifier,
	round int32,
	sndShardId uint32,
	dstShardId uint32,
) error {
	if txPool == nil {
		return process.ErrNilTransactionPool
	}

	err := bp.txProcessor.ProcessTransaction(transaction, round)
	if err == process.ErrLowerNonceInTransaction {
		strCache := process.ShardCacherIdentifier(sndShardId, dstShardId)
		txPool.RemoveData(transactionHash, strCache)
	}

	return err
}

func (bp *blockProcessor) getAllTxsFromMiniBlock(mb *block.MiniBlock, srShardId uint32, haveTime func() bool) ([]*transaction.Transaction, [][]byte, error) {
	txPool := bp.dataPool.Transactions()
	if txPool == nil {
		return nil, nil, process.ErrNilTransactionPool
	}

	strCache := process.ShardCacherIdentifier(srShardId, bp.shardCoordinator.SelfId())
	txCache := txPool.ShardDataStore(strCache)
	if txCache == nil {
		return nil, nil, process.ErrNilTransactionPool
	}

	// verify if all transaction exists
	transactions := make([]*transaction.Transaction, 0)
	txHashes := make([][]byte, 0)
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
		transactions = append(transactions, tx)
	}

	return transactions, txHashes, nil
}

// full verification through metachain header
func (bp *blockProcessor) createAndProcessCrossMiniBlocksDstMe(noShards uint32, maxTxInBlock int, round int32, haveTime func() bool) (block.MiniBlockSlice, uint32, error) {
	metaBlockCache := bp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return nil, 0, process.ErrNilMetaBlockPool
	}

	miniBlockCache := bp.dataPool.MiniBlocks()
	if miniBlockCache == nil {
		return nil, 0, process.ErrNilMiniBlockPool
	}

	txPool := bp.dataPool.Transactions()
	if txPool == nil {
		return nil, 0, process.ErrNilTransactionPool
	}

	miniBlocks := make(block.MiniBlockSlice, 0)
	nrTxAdded := uint32(0)
	// parse all the metablock headers
	for _, key := range metaBlockCache.Keys() {
		if !haveTime() {
			log.Info(fmt.Sprintf("time is up after putting %d cross txs with destination to current shard \n", nrTxAdded))
			return miniBlocks, nrTxAdded, nil
		}

		val, _ := metaBlockCache.Peek(key)
		if val == nil {
			continue
		}

		hdr, ok := val.(data.HeaderHandler)
		if !ok {
			continue
		}

		// get mini block hashes and senders id with destination to me
		hashSnd := hdr.GetMiniBlockHeadersWithDst(bp.shardCoordinator.SelfId())
		processedMbs := 0
		for k, senderShardId := range hashSnd {
			if !haveTime() {
				break
			}

			if hdr.WasMiniBlockProcessed([]byte(k)) {
				processedMbs++
				continue
			}

			miniVal, _ := miniBlockCache.Peek([]byte(k))
			if miniVal == nil {
				continue
			}

			miniBlock, ok := miniVal.(*block.MiniBlock)
			if !ok {
				continue
			}

			if miniBlock.ShardID != bp.shardCoordinator.SelfId() {
				hdr.SetProcessed([]byte(k))
				processedMbs++
				// miniblock is not for me removed
				miniBlockCache.Remove([]byte(k))
				delete(bp.miniBlockMissing, k)
				continue
			}

			// overflow would happen if processing would continue
			txOverFlow := nrTxAdded+uint32(len(miniBlock.TxHashes)) > uint32(maxTxInBlock)
			if txOverFlow {
				return miniBlocks, nrTxAdded, nil
			}

			miniBlockTxs, txHashes, err := bp.getAllTxsFromMiniBlock(miniBlock, senderShardId, haveTime)
			if err != nil {
				break
			}

			// process all transactions from miniblock
			snapshot := bp.accounts.JournalLen()
			for index, tx := range miniBlockTxs {
				if !haveTime() {
					err = process.ErrTimeIsOut
					break
				}
				if tx == nil {
					err = process.ErrNilTransaction
					break
				}

				err = bp.txProcessor.ProcessTransaction(miniBlockTxs[index], round)
				if err == process.ErrLowerNonceInTransaction {
					strCache := process.ShardCacherIdentifier(senderShardId, bp.shardCoordinator.SelfId())
					txPool.RemoveData(txHashes[index], strCache)
				}

				if err != nil {
					break
				}
			}
			// all txs from miniblock has to be processed together
			if err != nil {
				log.Error(err.Error())
				err = bp.accounts.RevertToSnapshot(snapshot)
				if err != nil {
					log.Error(err.Error())
				}
				continue
			}

			// all txs processed, add to processed miniblocks
			delete(bp.miniBlockMissing, k)
			miniBlocks = append(miniBlocks, miniBlock)
			nrTxAdded = nrTxAdded + uint32(len(miniBlock.TxHashes))
			hdr.SetProcessed([]byte(k))
			processedMbs++
		}

		if processedMbs >= len(hashSnd) {
			log.Info(fmt.Sprintf("All miniblocks processed with dest current shard from %s\n", string(hdr.GetRootHash())))
			// relevant information from
			metaBlockCache.Remove(key)
		}
	}

	return miniBlocks, nrTxAdded, nil
}

func (bp *blockProcessor) createMiniBlocks(noShards uint32, maxTxInBlock int, round int32, haveTime func() bool) (block.Body, error) {
	miniBlocks := make(block.Body, 0)
	bp.crossTxsForBlock = make(map[string]*transaction.Transaction)

	if bp.accounts.JournalLen() != 0 {
		return nil, process.ErrAccountStateDirty
	}

	if !haveTime() {
		log.Info(fmt.Sprintf("time is up after entered in createMiniBlocks method\n"))
		return miniBlocks, nil
	}

	txPool := bp.dataPool.Transactions()
	if txPool == nil {
		return nil, process.ErrNilTransactionPool
	}

	destMeMiniBlocks, txs, err := bp.createAndProcessCrossMiniBlocksDstMe(noShards, maxTxInBlock, round, haveTime)
	if err != nil {
		log.Info(err.Error())
	}

	if len(destMeMiniBlocks) > 0 {
		miniBlocks = append(miniBlocks, destMeMiniBlocks...)
	}

	if !haveTime() {
		log.Info(fmt.Sprintf("time is up added %d transactions\n", txs))
		return miniBlocks, nil
	}

	if txs > uint32(maxTxInBlock) {
		log.Info(fmt.Sprintf("block is full: added %d transactions\n", txs))
		return miniBlocks, nil
	}

	for i := 0; i < int(noShards); i++ {
		strCache := process.ShardCacherIdentifier(bp.shardCoordinator.SelfId(), uint32(i))
		txStore := txPool.ShardDataStore(strCache)

		timeBefore := time.Now()
		orderedTxes, orderedTxHashes, err := getTxs(txStore)
		timeAfter := time.Now()

		if !haveTime() {
			log.Info(fmt.Sprintf("time is up after ordered %d txs in %v sec\n", len(orderedTxes), timeAfter.Sub(timeBefore).Seconds()))
			return miniBlocks, nil
		}

		log.Info(fmt.Sprintf("time elapsed to ordered %d txs: %v sec\n", len(orderedTxes), timeAfter.Sub(timeBefore).Seconds()))

		if err != nil {
			log.Debug(fmt.Sprintf("when trying to order txs: %s", err.Error()))
			continue
		}

		miniBlock := block.MiniBlock{}
		miniBlock.ShardID = uint32(i)
		miniBlock.TxHashes = make([][]byte, 0)
		tXsForShard := make([]*transaction.Transaction, 0)
		log.Info(fmt.Sprintf("creating mini blocks has been started: have %d txs in pool for shard id %d\n", len(orderedTxes), miniBlock.ShardID))

		for index, tx := range orderedTxes {
			if !haveTime() {
				break
			}

			if tx == nil {
				log.Error("did not find transaction in pool")
				continue
			}

			snapshot := bp.accounts.JournalLen()

			// execute transaction to change the trie root hash
			err := bp.processAndRemoveBadTransaction(
				orderedTxHashes[index],
				orderedTxes[index],
				txPool,
				round,
				bp.shardCoordinator.SelfId(),
				miniBlock.ShardID,
			)

			if err != nil {
				log.Error(err.Error())
				err = bp.accounts.RevertToSnapshot(snapshot)
				if err != nil {
					log.Error(err.Error())
				}
				continue
			}

			bp.crossTxsForBlock[string(orderedTxHashes[index])] = orderedTxes[index]
			miniBlock.TxHashes = append(miniBlock.TxHashes, orderedTxHashes[index])
			tXsForShard = append(tXsForShard, orderedTxes[index])
			txs++

			if txs >= uint32(maxTxInBlock) { // max transactions count in one block was reached
				log.Info(fmt.Sprintf("max txs accepted in one block is reached: added %d txs from %d txs\n", len(miniBlock.TxHashes), len(orderedTxes)))

				if len(miniBlock.TxHashes) > 0 {
					miniBlocks = append(miniBlocks, &miniBlock)
				}

				log.Info(fmt.Sprintf("creating mini blocks has been finished: created %d mini blocks\n", len(miniBlocks)))
				return miniBlocks, nil
			}
		}

		if !haveTime() {
			log.Info(fmt.Sprintf("time is up: added %d txs from %d txs\n", len(miniBlock.TxHashes), len(orderedTxes)))

			if len(miniBlock.TxHashes) > 0 {
				miniBlocks = append(miniBlocks, &miniBlock)
			}

			log.Info(fmt.Sprintf("creating mini blocks has been finished: created %d mini blocks\n", len(miniBlocks)))
			return miniBlocks, nil
		}

		if len(miniBlock.TxHashes) > 0 {
			miniBlocks = append(miniBlocks, &miniBlock)
		}
	}

	log.Info(fmt.Sprintf("creating mini blocks has been finished: created %d mini blocks\n", len(miniBlocks)))
	return miniBlocks, nil
}

// CreateBlockHeader creates a miniblock header list given a block body
func (bp *blockProcessor) CreateBlockHeader(body data.BodyHandler) (data.HeaderHandler, error) {
	header := &block.Header{MiniBlockHeaders: make([]block.MiniBlockHeader, 0)}
	header.RootHash = bp.getRootHash()
	header.ShardId = bp.shardCoordinator.SelfId()

	if body == nil {
		return header, nil
	}

	blockBody, ok := body.(block.Body)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	mbLen := len(blockBody)
	miniBlockHeaders := make([]block.MiniBlockHeader, mbLen)
	for i := 0; i < mbLen; i++ {
		mbBytes, err := bp.marshalizer.Marshal(blockBody[i])
		if err != nil {
			return nil, err
		}
		mbHash := bp.hasher.Compute(string(mbBytes))

		miniBlockHeaders[i] = block.MiniBlockHeader{
			Hash:            mbHash,
			SenderShardID:   bp.shardCoordinator.SelfId(),
			ReceiverShardID: blockBody[i].ShardID,
		}
	}

	header.MiniBlockHeaders = miniBlockHeaders
	return header, nil
}

func (bp *blockProcessor) waitForTxHashes(waitTime time.Duration) error {
	select {
	case <-bp.ChRcvAllTxs:
		return nil
	case <-time.After(waitTime):
		return process.ErrTimeIsOut
	}
}

func (bp *blockProcessor) displayBlockchain(blockHeader *block.Header, txBlockBody block.Body) {
	if blockHeader == nil || txBlockBody == nil {
		return
	}

	headerHash, err := bp.computeHeaderHash(blockHeader)

	if err != nil {
		log.Error(err.Error())
		return
	}

	bp.displayLogInfo(blockHeader, txBlockBody, headerHash)
}

func (bp *blockProcessor) computeHeaderHash(hdr *block.Header) ([]byte, error) {
	headerMarsh, err := bp.marshalizer.Marshal(hdr)
	if err != nil {
		return nil, err
	}

	headerHash := bp.hasher.Compute(string(headerMarsh))

	return headerHash, nil
}

func (bp *blockProcessor) displayLogInfo(
	header *block.Header,
	body block.Body,
	headerHash []byte,
) {
	dispHeader, dispLines := createDisplayableHeaderAndBlockBody(header, body)

	tblString, err := display.CreateTableString(dispHeader, dispLines)
	if err != nil {
		log.Error(err.Error())
	}

	txCounterMutex.Lock()
	tblString = tblString + fmt.Sprintf("\nHeader hash: %s\n\nTotal txs "+
		"processed until now: %d. Total txs processed for this block: %d. Total txs remained in pool: %d\n",
		toB64(headerHash),
		txsTotalProcessed,
		txsCurrentBlockProcessed,
		bp.getNrTxsWithDst(header.ShardId))
	txCounterMutex.Unlock()

	log.Info(tblString)
}

func createDisplayableHeaderAndBlockBody(
	header *block.Header,
	body block.Body,
) ([]string, []*display.LineData) {

	tableHeader := []string{"Part", "Parameter", "Value"}

	lines := displayHeader(header)

	if header.BlockBodyType == block.TxBlock {
		lines = displayTxBlockBody(lines, body)

		return tableHeader, lines
	}

	//TODO: implement the other block bodies

	lines = append(lines, display.NewLineData(false, []string{"Unknown", "", ""}))
	return tableHeader, lines
}

func displayHeader(header *block.Header) []*display.LineData {
	lines := make([]*display.LineData, 0)

	lines = append(lines, display.NewLineData(false, []string{
		"Header",
		"Nonce",
		fmt.Sprintf("%d", header.Nonce)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Shard",
		fmt.Sprintf("%d", header.ShardId)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Epoch",
		fmt.Sprintf("%d", header.Epoch)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Round",
		fmt.Sprintf("%d", header.Round)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Timestamp",
		fmt.Sprintf("%d", header.TimeStamp)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Prev hash",
		toB64(header.PrevHash)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Prev rand seed",
		toB64(header.PrevRandSeed)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Rand seed",
		toB64(header.RandSeed)}))
	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Body type",
		header.BlockBodyType.String()}))

	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Pub keys bitmap",
		toHex(header.PubKeysBitmap)}))

	lines = append(lines, display.NewLineData(false, []string{
		"",
		"Signature",
		toB64(header.Signature)}))

	lines = append(lines, display.NewLineData(true, []string{
		"",
		"Root hash",
		toB64(header.RootHash)}))
	return lines
}

func displayTxBlockBody(lines []*display.LineData, body block.Body) []*display.LineData {

	txCounterMutex.RLock()
	txsCurrentBlockProcessed = 0
	txCounterMutex.RUnlock()

	for i := 0; i < len(body); i++ {
		miniBlock := body[i]

		part := fmt.Sprintf("TxBody_%d", miniBlock.ShardID)

		if miniBlock.TxHashes == nil || len(miniBlock.TxHashes) == 0 {
			lines = append(lines, display.NewLineData(false, []string{
				part, "", "<NIL> or <EMPTY>"}))
		}

		txCounterMutex.Lock()
		txsCurrentBlockProcessed += len(miniBlock.TxHashes)
		txsTotalProcessed += len(miniBlock.TxHashes)
		txCounterMutex.Unlock()

		for j := 0; j < len(miniBlock.TxHashes); j++ {
			if j == 0 || j >= len(miniBlock.TxHashes)-1 {
				lines = append(lines, display.NewLineData(false, []string{
					part,
					fmt.Sprintf("Tx blockBodyHash %d", j+1),
					toB64(miniBlock.TxHashes[j])}))

				part = ""
			} else if j == 1 {
				lines = append(lines, display.NewLineData(false, []string{
					part,
					fmt.Sprintf("..."),
					fmt.Sprintf("...")}))

				part = ""
			}
		}

		lines[len(lines)-1].HorizontalRuleAfter = true
	}

	return lines
}

func toHex(buff []byte) string {
	if buff == nil {
		return "<NIL>"
	}
	return "0x" + hex.EncodeToString(buff)
}

func toB64(buff []byte) string {
	if buff == nil {
		return "<NIL>"
	}
	return base64.StdEncoding.EncodeToString(buff)
}

func sortTxByNonce(txShardStore storage.Cacher) ([]*transaction.Transaction, [][]byte, error) {
	if txShardStore == nil {
		return nil, nil, process.ErrNilCacher
	}

	transactions := make([]*transaction.Transaction, 0)
	txHashes := make([][]byte, 0)

	mTxHashes := make(map[uint64][][]byte)
	mTransactions := make(map[uint64][]*transaction.Transaction)

	nonces := make([]uint64, 0)

	for _, key := range txShardStore.Keys() {
		val, _ := txShardStore.Peek(key)
		if val == nil {
			continue
		}

		tx, ok := val.(*transaction.Transaction)
		if !ok {
			continue
		}

		if mTxHashes[tx.Nonce] == nil {
			nonces = append(nonces, tx.Nonce)
			mTxHashes[tx.Nonce] = make([][]byte, 0)
			mTransactions[tx.Nonce] = make([]*transaction.Transaction, 0)
		}

		mTxHashes[tx.Nonce] = append(mTxHashes[tx.Nonce], key)
		mTransactions[tx.Nonce] = append(mTransactions[tx.Nonce], tx)
	}

	sort.Slice(nonces, func(i, j int) bool {
		return nonces[i] < nonces[j]
	})

	for _, nonce := range nonces {
		keys := mTxHashes[nonce]

		for idx, key := range keys {
			txHashes = append(txHashes, key)
			transactions = append(transactions, mTransactions[nonce][idx])
		}
	}

	return transactions, txHashes, nil
}

func (bp *blockProcessor) getNrTxsWithDst(dstShardId uint32) int {
	txPool := bp.dataPool.Transactions()
	if txPool == nil {
		return 0
	}

	sumTxs := 0

	for i := uint32(0); i < bp.shardCoordinator.NumberOfShards(); i++ {
		strCache := process.ShardCacherIdentifier(i, dstShardId)
		txStore := txPool.ShardDataStore(strCache)
		if txStore == nil {
			continue
		}
		sumTxs += txStore.Len()
	}

	return sumTxs
}

// CheckBlockValidity method checks if the given block is valid
func (bp *blockProcessor) CheckBlockValidity(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler) bool {
	if header == nil {
		log.Info(process.ErrNilBlockHeader.Error())
		return false
	}

	if blockChain == nil {
		log.Info(process.ErrNilBlockChain.Error())
		return false
	}

	if blockChain.GetCurrentBlockHeader() == nil {
		if header.GetNonce() == 1 { // first block after genesis
			if bytes.Equal(header.GetPrevHash(), blockChain.GetGenesisHeaderHash()) {
				// TODO add genesis block verification
				return true
			}

			log.Info(fmt.Sprintf("hash not match: local block hash is empty and node received block with previous hash %s\n",
				toB64(header.GetPrevHash())))

			return false
		}

		log.Info(fmt.Sprintf("nonce not match: local block nonce is 0 and node received block with nonce %d\n",
			header.GetNonce()))

		return false
	}

	if header.GetNonce() != blockChain.GetCurrentBlockHeader().GetNonce()+1 {
		log.Info(fmt.Sprintf("nonce not match: local block nonce is %d and node received block with nonce %d\n",
			blockChain.GetCurrentBlockHeader().GetNonce(), header.GetNonce()))

		return false
	}

	blockHeader, ok := blockChain.GetCurrentBlockHeader().(*block.Header)
	if !ok {
		log.Error(process.ErrWrongTypeAssertion.Error())
		return false
	}

	prevHeaderHash, err := bp.computeHeaderHash(blockHeader)
	if err != nil {
		log.Info(err.Error())
		return false
	}

	if !bytes.Equal(header.GetPrevHash(), prevHeaderHash) {
		log.Info(fmt.Sprintf("hash not match: local block hash is %s and node received block with previous hash %s\n",
			toB64(prevHeaderHash), toB64(header.GetPrevHash())))

		return false
	}

	if body != nil {
		// TODO add body verification here
	}

	return true
}

// MarshalizedDataForCrossShard prepares underlying data into a marshalized object according to destination
func (bp *blockProcessor) MarshalizedDataForCrossShard(body data.BodyHandler) (map[uint32][]byte, map[uint32][][]byte, error) {
	if body == nil {
		return nil, nil, process.ErrNilMiniBlocks
	}

	blockBody, ok := body.(block.Body)
	if !ok {
		return nil, nil, process.ErrWrongTypeAssertion
	}

	mrsData := make(map[uint32][]byte)
	mrsTxs := make(map[uint32][][]byte)
	bodies := make(map[uint32]block.Body)

	for i := 0; i < len(blockBody); i++ {
		miniblock := blockBody[i]
		if miniblock.ShardID == bp.shardCoordinator.SelfId() {
			//not taking into account miniblocks for current shard
			continue
		}
		bodies[miniblock.ShardID] = append(bodies[miniblock.ShardID], miniblock)

		for _, txHash := range miniblock.TxHashes {
			tx := bp.crossTxsForBlock[string(txHash)]
			if tx != nil {
				txMrs, err := bp.marshalizer.Marshal(tx)
				if err != nil {
					return nil, nil, process.ErrMarshalWithoutSuccess
				}
				mrsTxs[miniblock.ShardID] = append(mrsTxs[miniblock.ShardID], txMrs)
			}
		}
	}

	for shardId, subsetBlockBody := range bodies {
		buff, err := bp.marshalizer.Marshal(subsetBlockBody)
		if err != nil {
			return nil, nil, process.ErrMarshalWithoutSuccess
		}
		mrsData[shardId] = buff
	}

	return mrsData, mrsTxs, nil
}

func getTxs(txShardStore storage.Cacher) ([]*transaction.Transaction, [][]byte, error) {
	if txShardStore == nil {
		return nil, nil, process.ErrNilCacher
	}

	transactions := make([]*transaction.Transaction, 0)
	txHashes := make([][]byte, 0)

	for _, key := range txShardStore.Keys() {
		val, _ := txShardStore.Peek(key)
		if val == nil {
			continue
		}

		tx, ok := val.(*transaction.Transaction)
		if !ok {
			continue
		}

		txHashes = append(txHashes, key)
		transactions = append(transactions, tx)
	}

	return transactions, txHashes, nil
}
