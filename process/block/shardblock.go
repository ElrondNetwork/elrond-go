package block

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/core"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/serviceContainer"
	"github.com/ElrondNetwork/elrond-go-sandbox/data"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/ElrondNetwork/elrond-go-sandbox/dataRetriever"
	"github.com/ElrondNetwork/elrond-go-sandbox/hashing"
	"github.com/ElrondNetwork/elrond-go-sandbox/marshal"
	"github.com/ElrondNetwork/elrond-go-sandbox/process"
	"github.com/ElrondNetwork/elrond-go-sandbox/process/block/preprocess"
	"github.com/ElrondNetwork/elrond-go-sandbox/sharding"
)

const maxTransactionsInBlock = 15000
const metablockFinality = 1

type txShardInfo struct {
	senderShardID   uint32
	receiverShardID uint32
}

type txInfo struct {
	tx *transaction.Transaction
	*txShardInfo
	has bool
}

// shardProcessor implements shardProcessor interface and actually it tries to execute block
type shardProcessor struct {
	*baseProcessor
	dataPool          dataRetriever.PoolsHolder
	txProcessor       process.TransactionProcessor
	blocksTracker     process.BlocksTracker
	metaBlockFinality int

	onRequestMiniBlock    func(shardId uint32, mbHash []byte)
	chRcvAllMetaHdrs      chan bool
	mutUsedMetaHdrsHashes sync.Mutex
	usedMetaHdrsHashes    map[uint32][][]byte

	mutRequestedMetaHdrHashes sync.RWMutex
	requestedMetaHdrHashes    map[string]bool
	currHighestMetaHdrNonce   uint64
	metaHdrsFound             bool

	core         serviceContainer.Core
	txPreProcess process.PreProcessor
}

// NewShardProcessor creates a new shardProcessor object
func NewShardProcessor(
	core serviceContainer.Core,
	dataPool dataRetriever.PoolsHolder,
	store dataRetriever.StorageService,
	hasher hashing.Hasher,
	marshalizer marshal.Marshalizer,
	txProcessor process.TransactionProcessor,
	accounts state.AccountsAdapter,
	shardCoordinator sharding.Coordinator,
	forkDetector process.ForkDetector,
	blocksTracker process.BlocksTracker,
	startHeaders map[uint32]data.HeaderHandler,
	metaChainActive bool,
	requestHandler process.RequestHandler,
) (*shardProcessor, error) {

	err := checkProcessorNilParameters(
		accounts,
		forkDetector,
		hasher,
		marshalizer,
		store,
		shardCoordinator)
	if err != nil {
		return nil, err
	}

	if dataPool == nil {
		return nil, process.ErrNilDataPoolHolder
	}
	if txProcessor == nil {
		return nil, process.ErrNilTxProcessor
	}
	if blocksTracker == nil {
		return nil, process.ErrNilBlocksTracker
	}
	if requestHandler == nil {
		return nil, process.ErrNilRequestHandler
	}

	base := &baseProcessor{
		accounts:                      accounts,
		forkDetector:                  forkDetector,
		hasher:                        hasher,
		marshalizer:                   marshalizer,
		store:                         store,
		shardCoordinator:              shardCoordinator,
		onRequestHeaderHandlerByNonce: requestHandler.RequestHeaderByNonce,
	}
	err = base.setLastNotarizedHeadersSlice(startHeaders, metaChainActive)
	if err != nil {
		return nil, err
	}

	sp := shardProcessor{
		core:          core,
		baseProcessor: base,
		dataPool:      dataPool,
		txProcessor:   txProcessor,
		blocksTracker: blocksTracker,
	}

	sp.chRcvAllMetaHdrs = make(chan bool)

	transactionPool := sp.dataPool.Transactions()
	if transactionPool == nil {
		return nil, process.ErrNilTransactionPool
	}

	sp.onRequestMiniBlock = requestHandler.RequestMiniBlock
	sp.requestedMetaHdrHashes = make(map[string]bool)
	sp.usedMetaHdrsHashes = make(map[uint32][][]byte)

	metaBlockPool := sp.dataPool.MetaBlocks()
	if metaBlockPool == nil {
		return nil, process.ErrNilMetaBlockPool
	}
	metaBlockPool.RegisterHandler(sp.receivedMetaBlock)
	sp.onRequestHeaderHandler = requestHandler.RequestHeader

	miniBlockPool := sp.dataPool.MiniBlocks()
	if miniBlockPool == nil {
		return nil, process.ErrNilMiniBlockPool
	}
	miniBlockPool.RegisterHandler(sp.receivedMiniBlock)

	sp.metaBlockFinality = metablockFinality

	sp.txPreProcess, err = preprocess.NewTransactionPreprocessor(
		sp.dataPool.Transactions(),
		sp.store,
		sp.hasher,
		sp.marshalizer,
		sp.txProcessor,
		sp.shardCoordinator,
		accounts,
		requestHandler.RequestTransaction)
	if err != nil {
		return nil, err
	}

	return &sp, nil
}

// ProcessBlock processes a block. It returns nil if all ok or the specific error
func (sp *shardProcessor) ProcessBlock(
	chainHandler data.ChainHandler,
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
	haveTime func() time.Duration,
) error {

	if haveTime == nil {
		return process.ErrNilHaveTimeHandler
	}

	err := sp.checkBlockValidity(chainHandler, headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	header, ok := headerHandler.(*block.Header)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	body, ok := bodyHandler.(block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	err = sp.checkHeaderBodyCorrelation(header, body)
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("Total txs in pool: %d\n", GetNrTxsWithDst(header.ShardId, sp.dataPool, sp.shardCoordinator.NumberOfShards())))

	requestedTxs := sp.txPreProcess.RequestBlockTransactions(body)
	requestedMetaHdrs := sp.requestMetaHeaders(header)

	if haveTime() < 0 {
		return process.ErrTimeIsOut
	}

	err = sp.txPreProcess.IsDataPrepared(requestedTxs, haveTime)
	if err != nil {
		return err
	}

	if requestedMetaHdrs > 0 {
		log.Info(fmt.Sprintf("requested %d missing meta headers to confirm cross shard txs\n", requestedMetaHdrs))
		err = sp.waitForMetaHdrHashes(haveTime())
		sp.mutRequestedMetaHdrHashes.RLock()
		requestedMetaHdrHashes := len(sp.requestedMetaHdrHashes)
		sp.mutRequestedMetaHdrHashes.RUnlock()
		log.Info(fmt.Sprintf("received %d missing meta headers\n", requestedMetaHdrs-requestedMetaHdrHashes))
		if err != nil {
			return err
		}
	}

	err = sp.verifyCrossShardMiniBlockDstMe(header)
	if err != nil {
		return err
	}

	if sp.accounts.JournalLen() != 0 {
		return process.ErrAccountStateDirty
	}

	defer func() {
		if err != nil {
			sp.RevertAccountState()
		}
	}()

	err = sp.txPreProcess.ProcessBlockTransactions(body, header.Round, haveTime)
	if err != nil {
		return err
	}

	if !sp.verifyStateRoot(header.GetRootHash()) {
		err = process.ErrRootStateMissmatch
		return err
	}

	go sp.checkAndRequestIfMetaHeadersMissing(header.GetRound())

	return nil
}

// check if header has the same miniblocks as presented in body
func (sp *shardProcessor) checkHeaderBodyCorrelation(hdr *block.Header, body block.Body) error {
	mbHashesFromHdr := make(map[string]*block.MiniBlockHeader)
	for i := 0; i < len(hdr.MiniBlockHeaders); i++ {
		mbHashesFromHdr[string(hdr.MiniBlockHeaders[i].Hash)] = &hdr.MiniBlockHeaders[i]
	}

	if len(hdr.MiniBlockHeaders) != len(body) {
		return process.ErrHeaderBodyMismatch
	}

	for i := 0; i < len(body); i++ {
		miniBlock := body[i]

		mbBytes, err := sp.marshalizer.Marshal(miniBlock)
		if err != nil {
			return err
		}
		mbHash := sp.hasher.Compute(string(mbBytes))

		mbHdr, ok := mbHashesFromHdr[string(mbHash)]
		if !ok {
			return process.ErrHeaderBodyMismatch
		}

		if mbHdr.TxCount != uint32(len(miniBlock.TxHashes)) {
			return process.ErrHeaderBodyMismatch
		}

		if mbHdr.ReceiverShardID != miniBlock.ReceiverShardID {
			return process.ErrHeaderBodyMismatch
		}

		if mbHdr.SenderShardID != miniBlock.SenderShardID {
			return process.ErrHeaderBodyMismatch
		}
	}

	return nil
}

func (sp *shardProcessor) checkAndRequestIfMetaHeadersMissing(round uint32) error {
	orderedMetaBlocks, err := sp.getOrderedMetaBlocks(round)
	if err != nil {
		return err
	}

	sortedHdrs := make([]data.HeaderHandler, 0)
	for i := 0; i < len(orderedMetaBlocks); i++ {
		hdr, ok := orderedMetaBlocks[i].hdr.(*block.MetaBlock)
		if !ok {
			continue
		}
		sortedHdrs = append(sortedHdrs, hdr)
	}

	err = sp.requestHeadersIfMissing(sortedHdrs, sharding.MetachainShardId, round)
	if err != nil {
		log.Info(err.Error())
	}

	return nil
}

func (sp *shardProcessor) indexBlockIfNeeded(
	body data.BodyHandler,
	header data.HeaderHandler) {
	if sp.core == nil || sp.core.Indexer() == nil {
		return
	}

	txPool := sp.txPreProcess.GetAllCurrentUsedTxs()

	go sp.core.Indexer().SaveBlock(body, header, txPool)
}

// RestoreBlockIntoPools restores the TxBlock and MetaBlock into associated pools
func (sp *shardProcessor) RestoreBlockIntoPools(headerHandler data.HeaderHandler, bodyHandler data.BodyHandler) error {
	if bodyHandler == nil {
		return process.ErrNilTxBlockBody
	}

	body, ok := bodyHandler.(block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	miniBlockHashes := make(map[int][]byte, 0)
	restoredTxNr, err := sp.txPreProcess.RestoreTxBlockIntoPools(body, miniBlockHashes, sp.dataPool.MiniBlocks())
	go SubstractRestoredTxs(restoredTxNr)
	if err != nil {
		return err
	}

	err = sp.restoreMetaBlockIntoPool(miniBlockHashes)
	if err != nil {
		return err
	}

	sp.restoreLastNotarized()

	return nil
}

func (sp *shardProcessor) restoreMetaBlockIntoPool(miniBlockHashes map[int][]byte) error {
	metaBlockPool := sp.dataPool.MetaBlocks()
	if metaBlockPool == nil {
		return process.ErrNilMetaBlockPool
	}

	for _, metaBlockKey := range metaBlockPool.Keys() {
		if len(miniBlockHashes) == 0 {
			break
		}
		metaBlock, _ := metaBlockPool.Peek(metaBlockKey)
		if metaBlock == nil {
			return process.ErrNilMetaBlockHeader
		}

		hdr, _ := metaBlock.(data.HeaderHandler)
		if hdr == nil {
			return process.ErrWrongTypeAssertion
		}

		crossMiniBlockHashes := hdr.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
		for key := range miniBlockHashes {
			_, ok := crossMiniBlockHashes[string(miniBlockHashes[key])]
			if !ok {
				continue
			}

			hdr.SetMiniBlockProcessed(miniBlockHashes[key], false)
			delete(miniBlockHashes, key)
		}
	}

	//TODO: if miniBlockHashes were not found in meta pool then they should be in some already committed metablocks
	//So they should be searched in metablocks storer, these metablocks which contains them should be pull out in pools
	//and then these miniblocks should be set as not processed.

	return nil
}

// CreateBlockBody creates a a list of miniblocks by filling them with transactions out of the transactions pools
// as long as the transactions limit for the block has not been reached and there is still time to add transactions
func (sp *shardProcessor) CreateBlockBody(round uint32, haveTime func() bool) (data.BodyHandler, error) {
	miniBlocks, err := sp.createMiniBlocks(sp.shardCoordinator.NumberOfShards(), maxTransactionsInBlock, round, haveTime)

	if err != nil {
		return nil, err
	}

	return miniBlocks, nil
}

// CommitBlock commits the block in the blockchain if everything was checked successfully
func (sp *shardProcessor) CommitBlock(
	chainHandler data.ChainHandler,
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
) error {

	var err error
	defer func() {
		if err != nil {
			sp.RevertAccountState()
		}
	}()

	err = checkForNils(chainHandler, headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	header, ok := headerHandler.(*block.Header)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	buff, err := sp.marshalizer.Marshal(header)
	if err != nil {
		return err
	}

	headerHash := sp.hasher.Compute(string(buff))
	errNotCritical := sp.store.Put(dataRetriever.BlockHeaderUnit, headerHash, buff)
	if errNotCritical != nil {
		log.Error(errNotCritical.Error())
	}

	body, ok := bodyHandler.(block.Body)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	for i := 0; i < len(body); i++ {
		buff, err = sp.marshalizer.Marshal(body[i])
		if err != nil {
			return err
		}

		miniBlockHash := sp.hasher.Compute(string(buff))
		errNotCritical = sp.store.Put(dataRetriever.MiniBlockUnit, miniBlockHash, buff)
		if errNotCritical != nil {
			log.Error(errNotCritical.Error())
		}
	}

	headerNoncePool := sp.dataPool.HeadersNonces()
	if headerNoncePool == nil {
		err = process.ErrNilDataPoolHolder
		return err
	}

	_ = headerNoncePool.Put(headerHandler.GetNonce(), headerHash)

	err = sp.txPreProcess.SaveTxBlockToStorage(body)
	if err != nil {
		return err
	}

	_, err = sp.accounts.Commit()
	if err != nil {
		return err
	}

	sp.blocksTracker.AddBlock(header)

	log.Info(fmt.Sprintf("shardBlock with nonce %d and hash %s has been committed successfully\n",
		header.Nonce,
		core.ToB64(headerHash)))

	errNotCritical = sp.txPreProcess.RemoveTxBlockFromPools(body, sp.dataPool.MiniBlocks())
	if errNotCritical != nil {
		log.Debug(errNotCritical.Error())
	}

	processedMetaHdrs, errNotCritical := sp.getProcessedMetaBlocksFromPool(body)
	if errNotCritical != nil {
		log.Debug(errNotCritical.Error())
	}

	err = sp.saveLastNotarizedHeader(sharding.MetachainShardId, processedMetaHdrs)
	if err != nil {
		return err
	}

	errNotCritical = sp.removeProcessedMetablocksFromPool(processedMetaHdrs)
	if errNotCritical != nil {
		log.Debug(errNotCritical.Error())
	}

	errNotCritical = sp.forkDetector.AddHeader(header, headerHash, process.BHProcessed)
	if errNotCritical != nil {
		log.Debug(errNotCritical.Error())
	}

	err = chainHandler.SetCurrentBlockBody(body)
	if err != nil {
		return err
	}

	err = chainHandler.SetCurrentBlockHeader(header)
	if err != nil {
		return err
	}

	chainHandler.SetCurrentBlockHeaderHash(headerHash)
	sp.indexBlockIfNeeded(bodyHandler, headerHandler)

	// write data to log
	go DisplayLogInfo(header, body, headerHash, sp.shardCoordinator.NumberOfShards(), sp.shardCoordinator.SelfId(), sp.dataPool)

	return nil
}

// removeMetaBlockFromPool removes meta blocks from associated pool
func (sp *shardProcessor) getProcessedMetaBlocksFromPool(body block.Body) ([]data.HeaderHandler, error) {
	if body == nil {
		return nil, process.ErrNilTxBlockBody
	}

	miniBlockHashes := make(map[int][]byte, 0)
	for i := 0; i < len(body); i++ {
		miniBlock := body[i]
		if miniBlock.SenderShardID == sp.shardCoordinator.SelfId() {
			continue
		}

		buff, err := sp.marshalizer.Marshal(miniBlock)
		if err != nil {
			return nil, err
		}
		mbHash := sp.hasher.Compute(string(buff))
		miniBlockHashes[i] = mbHash
	}

	processedMetaHdrs := make([]data.HeaderHandler, 0)
	for _, metaBlockKey := range sp.dataPool.MetaBlocks().Keys() {
		metaBlock, _ := sp.dataPool.MetaBlocks().Peek(metaBlockKey)
		if metaBlock == nil {
			return processedMetaHdrs, process.ErrNilMetaBlockHeader
		}

		hdr, ok := metaBlock.(*block.MetaBlock)
		if !ok {
			return processedMetaHdrs, process.ErrWrongTypeAssertion
		}

		crossMiniBlockHashes := hdr.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
		for key := range miniBlockHashes {
			_, ok := crossMiniBlockHashes[string(miniBlockHashes[key])]
			if !ok {
				continue
			}

			hdr.SetMiniBlockProcessed(miniBlockHashes[key], true)
			delete(miniBlockHashes, key)
		}

		processedAll := true
		for key := range crossMiniBlockHashes {
			if !hdr.GetMiniBlockProcessed([]byte(key)) {
				processedAll = false
				break
			}
		}

		if processedAll {
			processedMetaHdrs = append(processedMetaHdrs, hdr)
		}
	}

	return processedMetaHdrs, nil
}

func (sp *shardProcessor) removeProcessedMetablocksFromPool(processedMetaHdrs []data.HeaderHandler) error {
	lastNoterizedMetaHdr, err := sp.getLastNotarizedHdr(sharding.MetachainShardId)
	if err != nil {
		return err
	}

	// processedMetaHdrs is also sorted
	for i := 0; i < len(processedMetaHdrs); i++ {
		hdr := processedMetaHdrs[i]

		// remove process finished
		if hdr.GetNonce() > lastNoterizedMetaHdr.GetNonce() {
			continue
		}

		errNotCritical := sp.blocksTracker.RemoveNotarisedBlocks(hdr)
		if errNotCritical != nil {
			log.Error(errNotCritical.Error())
		}

		// metablock was processed and finalized
		buff, err := sp.marshalizer.Marshal(hdr)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		key := sp.hasher.Compute(string(buff))
		err = sp.store.Put(dataRetriever.MetaBlockUnit, key, buff)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		sp.dataPool.MetaBlocks().Remove(key)
		log.Info(fmt.Sprintf("metablock with nonce %d has been processed completely and removed from pool\n",
			hdr.GetNonce()))
	}

	return nil
}

// receivedMetaBlock is a callback function when a new metablock was received
// upon receiving, it parses the new metablock and requests miniblocks and transactions
// which destination is the current shard
func (sp *shardProcessor) receivedMetaBlock(metaBlockHash []byte) {
	metaBlockCache := sp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return
	}

	metaHdrNoncesCache := sp.dataPool.MetaHeadersNonces()
	if metaHdrNoncesCache == nil && sp.metaBlockFinality > 0 {
		return
	}

	miniBlockCache := sp.dataPool.MiniBlocks()
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

	log.Info(fmt.Sprintf("received metablock with hash %s and nonce %d from network\n",
		core.ToB64(metaBlockHash),
		hdr.GetNonce()))

	sp.mutRequestedMetaHdrHashes.Lock()

	if !sp.metaHdrsFound {
		if sp.requestedMetaHdrHashes[string(metaBlockHash)] {
			delete(sp.requestedMetaHdrHashes, string(metaBlockHash))

			if sp.currHighestMetaHdrNonce < hdr.GetNonce() {
				sp.currHighestMetaHdrNonce = hdr.GetNonce()
			}
		}

		lenReqMetaHdrHashes := len(sp.requestedMetaHdrHashes)
		areFinalityAttestingHdrsInCache := true
		if lenReqMetaHdrHashes == 0 {
			// ask for finality attesting metahdr if it is not yet in cache
			for i := sp.currHighestMetaHdrNonce + 1; i <= sp.currHighestMetaHdrNonce+uint64(sp.metaBlockFinality); i++ {
				if !metaHdrNoncesCache.Has(i) {
					go sp.onRequestHeaderHandlerByNonce(sharding.MetachainShardId, i)
					areFinalityAttestingHdrsInCache = false
				}
			}
		}

		sp.metaHdrsFound = lenReqMetaHdrHashes == 0 && areFinalityAttestingHdrsInCache

		sp.mutRequestedMetaHdrHashes.Unlock()

		if lenReqMetaHdrHashes == 0 && areFinalityAttestingHdrsInCache {
			sp.chRcvAllMetaHdrs <- true
		}

	} else {
		sp.mutRequestedMetaHdrHashes.Unlock()
	}

	lastHdr, err := sp.getLastNotarizedHdr(sharding.MetachainShardId)
	if err != nil {
		return
	}
	if hdr.GetNonce() <= lastHdr.GetNonce() {
		return
	}
	if hdr.GetRound() <= lastHdr.GetRound() {
		return
	}

	crossMiniBlockHashes := hdr.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
	for key, senderShardId := range crossMiniBlockHashes {
		miniVal, _ := miniBlockCache.Peek([]byte(key))
		if miniVal == nil {
			//TODO: It should be analyzed if launching the next line(request) on go routine is better or not
			go sp.onRequestMiniBlock(senderShardId, []byte(key))
		}
	}
}

// receivedMiniBlock is a callback function when a new miniblock was received
// it will further ask for missing transactions
func (sp *shardProcessor) receivedMiniBlock(miniBlockHash []byte) {
	metaBlockCache := sp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return
	}

	miniBlockCache := sp.dataPool.MiniBlocks()
	if miniBlockCache == nil {
		return
	}

	val, ok := miniBlockCache.Peek(miniBlockHash)
	if !ok {
		return
	}

	miniBlock, ok := val.(block.MiniBlock)
	if !ok {
		return
	}

	_ = sp.txPreProcess.RequestTransactionsForMiniBlock(miniBlock)
}

func (sp *shardProcessor) requestMetaHeaders(hdr *block.Header) int {
	metaBlockCache := sp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return 0
	}

	sp.mutRequestedMetaHdrHashes.Lock()

	requestedMetaHdrs := 0
	sp.requestedMetaHdrHashes = make(map[string]bool)
	sp.currHighestMetaHdrNonce = uint64(0)
	hashes := make([][]byte, 0)
	for i := 0; i < len(hdr.MetaBlockHashes); i++ {
		val, ok := metaBlockCache.Peek(hdr.MetaBlockHashes[i])
		if !ok {
			hashes = append(hashes, hdr.MetaBlockHashes[i])
			sp.requestedMetaHdrHashes[string(hdr.MetaBlockHashes[i])] = true
		}

		header, ok := val.(data.HeaderHandler)
		if !ok {
			continue
		}

		if sp.currHighestMetaHdrNonce < header.GetNonce() {
			sp.currHighestMetaHdrNonce = header.GetNonce()
		}
	}

	if len(sp.requestedMetaHdrHashes) > 0 {
		sp.metaHdrsFound = false
	}

	sp.mutRequestedMetaHdrHashes.Unlock()

	for _, hash := range hashes {
		requestedMetaHdrs++
		sp.onRequestHeaderHandler(sharding.MetachainShardId, hash)
	}

	return requestedMetaHdrs
}

// processMiniBlockComplete - all transactions must be processed together, otherwise error
func (sp *shardProcessor) createAndProcessMiniBlockComplete(
	miniBlock *block.MiniBlock,
	round uint32,
	haveTime func() bool,
) error {

	snapshot := sp.accounts.JournalLen()
	err := sp.txPreProcess.ProcessMiniBlock(miniBlock, haveTime, round)
	if err != nil {
		log.Error(err.Error())
		errAccountState := sp.accounts.RevertToSnapshot(snapshot)
		if errAccountState != nil {
			// TODO: evaluate if reloading the trie from disk will might solve the problem
			log.Error(errAccountState.Error())
		}

		return err
	}

	return nil
}

func (sp *shardProcessor) verifyCrossShardMiniBlockDstMe(hdr *block.Header) error {
	mMiniBlockMeta, err := sp.getAllMiniBlockDstMeFromMeta(hdr.Round, hdr.MetaBlockHashes)
	if err != nil {
		return err
	}
	metablockCache := sp.dataPool.MetaBlocks()
	if metablockCache == nil {
		return process.ErrNilMetaBlockPool
	}

	currMetaBlocks := make([]data.HeaderHandler, 0)
	miniBlockDstMe := hdr.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
	for mbHash := range miniBlockDstMe {
		if _, ok := mMiniBlockMeta[mbHash]; !ok {
			return process.ErrCrossShardMBWithoutConfirmationFromMeta
		}

		hdr, ok := metablockCache.Peek(mMiniBlockMeta[mbHash])
		if !ok {
			return process.ErrNilMetaBlockHeader
		}

		metaHdr, ok := hdr.(data.HeaderHandler)
		if !ok {
			return process.ErrWrongTypeAssertion
		}

		currMetaBlocks = append(currMetaBlocks, metaHdr)
	}

	if len(currMetaBlocks) == 0 {
		return nil
	}

	err = sp.verifyIncludedMetaBlocksFinality(currMetaBlocks, hdr.Round)

	return err
}

func (sp *shardProcessor) verifyIncludedMetaBlocksFinality(currMetaBlocks []data.HeaderHandler, round uint32) error {
	orderedMetablocks, err := sp.getOrderedMetaBlocks(round)
	if err != nil {
		return err
	}

	sort.Slice(currMetaBlocks, func(i, j int) bool {
		return currMetaBlocks[i].GetNonce() < currMetaBlocks[j].GetNonce()
	})

	for i := 0; i < len(currMetaBlocks); i++ {
		isFinal := sp.isMetaHeaderFinal(currMetaBlocks[i], orderedMetablocks, 0)
		if !isFinal {
			return process.ErrMetaBlockNotFinal
		}
	}

	return nil
}

func (sp *shardProcessor) getAllMiniBlockDstMeFromMeta(round uint32, metaHashes [][]byte) (map[string][]byte, error) {
	metaBlockCache := sp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return nil, process.ErrNilMetaBlockPool
	}

	lastHdr, err := sp.getLastNotarizedHdr(sharding.MetachainShardId)
	if err != nil {
		return nil, err
	}

	mMiniBlockMeta := make(map[string][]byte)
	for _, metaHash := range metaHashes {
		val, _ := metaBlockCache.Peek(metaHash)
		if val == nil {
			continue
		}

		hdr, ok := val.(*block.MetaBlock)
		if !ok {
			continue
		}

		if hdr.GetRound() > round {
			continue
		}
		if hdr.GetRound() <= lastHdr.GetRound() {
			continue
		}
		if hdr.GetNonce() <= lastHdr.GetNonce() {
			continue
		}

		miniBlockDstMe := hdr.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
		for mbHash := range miniBlockDstMe {
			mMiniBlockMeta[mbHash] = metaHash
		}
	}

	return mMiniBlockMeta, nil
}

func (sp *shardProcessor) getOrderedMetaBlocks(round uint32) ([]*hashAndHdr, error) {
	metaBlockCache := sp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return nil, process.ErrNilMetaBlockPool
	}

	lastHdr, err := sp.getLastNotarizedHdr(sharding.MetachainShardId)
	if err != nil {
		return nil, err
	}

	orderedMetaBlocks := make([]*hashAndHdr, 0)
	for _, key := range metaBlockCache.Keys() {
		val, _ := metaBlockCache.Peek(key)
		if val == nil {
			continue
		}

		hdr, ok := val.(*block.MetaBlock)
		if !ok {
			continue
		}

		if hdr.GetRound() > round {
			continue
		}
		if hdr.GetRound() <= lastHdr.GetRound() {
			continue
		}
		if hdr.GetNonce() <= lastHdr.GetNonce() {
			continue
		}

		orderedMetaBlocks = append(orderedMetaBlocks, &hashAndHdr{hdr: hdr, hash: key})
	}

	// sort headers for each shard
	if len(orderedMetaBlocks) == 0 {
		return nil, process.ErrNoNewMetablocks
	}

	sort.Slice(orderedMetaBlocks, func(i, j int) bool {
		return orderedMetaBlocks[i].hdr.GetNonce() < orderedMetaBlocks[j].hdr.GetNonce()
	})

	return orderedMetaBlocks, nil
}

func (sp *shardProcessor) createAndprocessMiniBlocksFromHeader(
	hdr data.HeaderHandler,
	maxTxRemaining uint32,
	round uint32,
	haveTime func() bool,
) (block.MiniBlockSlice, uint32, bool) {
	// verification of hdr and miniblock validity is done before the function is called
	miniBlockCache := sp.dataPool.MiniBlocks()

	miniBlocks := make(block.MiniBlockSlice, 0)
	nrTxAdded := uint32(0)
	nrMBprocessed := 0
	// get mini block hashes which contain cross shard txs with destination in self shard
	crossMiniBlockHashes := hdr.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
	for key, senderShardId := range crossMiniBlockHashes {
		if !haveTime() {
			break
		}

		if hdr.GetMiniBlockProcessed([]byte(key)) {
			nrMBprocessed++
			continue
		}

		miniVal, _ := miniBlockCache.Peek([]byte(key))
		if miniVal == nil {
			go sp.onRequestMiniBlock(senderShardId, []byte(key))
			continue
		}

		miniBlock, ok := miniVal.(*block.MiniBlock)
		if !ok {
			continue
		}

		// overflow would happen if processing would continue
		txOverFlow := nrTxAdded+uint32(len(miniBlock.TxHashes)) > maxTxRemaining
		if txOverFlow {
			return miniBlocks, nrTxAdded, false
		}

		requestedTxs := sp.txPreProcess.RequestTransactionsForMiniBlock(*miniBlock)
		if requestedTxs > 0 {
			continue
		}

		err := sp.createAndProcessMiniBlockComplete(miniBlock, round, haveTime)
		if err != nil {
			continue
		}

		// all txs processed, add to processed miniblocks
		miniBlocks = append(miniBlocks, miniBlock)
		nrTxAdded = nrTxAdded + uint32(len(miniBlock.TxHashes))
		nrMBprocessed++
	}

	allMBsProcessed := nrMBprocessed == len(crossMiniBlockHashes)
	return miniBlocks, nrTxAdded, allMBsProcessed
}

// isMetaHeaderFinal verifies if meta is trully final, in order to not do rollbacks
func (sp *shardProcessor) isMetaHeaderFinal(currHdr data.HeaderHandler, sortedHdrs []*hashAndHdr, startPos int) bool {
	if currHdr == nil {
		return false
	}
	if sortedHdrs == nil {
		return false
	}

	// verify if there are "K" block after current to make this one final
	lastVerifiedHdr := currHdr
	nextBlocksVerified := 0

	for i := startPos; i < len(sortedHdrs); i++ {
		if nextBlocksVerified >= sp.metaBlockFinality {
			return true
		}

		// found a header with the next nonce
		tmpHdr := sortedHdrs[i].hdr
		if tmpHdr.GetNonce() == lastVerifiedHdr.GetNonce()+1 {
			err := sp.isHdrConstructionValid(tmpHdr, lastVerifiedHdr)
			if err != nil {
				continue
			}

			lastVerifiedHdr = tmpHdr
			nextBlocksVerified += 1
		}
	}

	if nextBlocksVerified >= sp.metaBlockFinality {
		return true
	}

	return false
}

// full verification through metachain header
func (sp *shardProcessor) createAndProcessCrossMiniBlocksDstMe(
	noShards uint32,
	maxTxInBlock int,
	round uint32,
	haveTime func() bool,
) (block.MiniBlockSlice, uint32, error) {

	metaBlockCache := sp.dataPool.MetaBlocks()
	if metaBlockCache == nil {
		return nil, 0, process.ErrNilMetaBlockPool
	}

	miniBlockCache := sp.dataPool.MiniBlocks()
	if miniBlockCache == nil {
		return nil, 0, process.ErrNilMiniBlockPool
	}

	txPool := sp.dataPool.Transactions()
	if txPool == nil {
		return nil, 0, process.ErrNilTransactionPool
	}

	miniBlocks := make(block.MiniBlockSlice, 0)
	nrTxAdded := uint32(0)

	orderedMetaBlocks, err := sp.getOrderedMetaBlocks(round)
	if err != nil {
		return nil, 0, err
	}

	log.Info(fmt.Sprintf("orderedMetaBlocks: %d \n", len(orderedMetaBlocks)))

	lastMetaHdr, err := sp.getLastNotarizedHdr(sharding.MetachainShardId)
	if err != nil {
		return nil, 0, err
	}

	// do processing in order
	usedMetaHdrsHashes := make([][]byte, 0)
	for i := 0; i < len(orderedMetaBlocks); i++ {
		if !haveTime() {
			log.Info(fmt.Sprintf("time is up after putting %d cross txs with destination to current shard \n", nrTxAdded))
			break
		}

		hdr, ok := orderedMetaBlocks[i].hdr.(*block.MetaBlock)
		if !ok {
			continue
		}

		err := sp.isHdrConstructionValid(hdr, lastMetaHdr)
		if err != nil {
			continue
		}

		isFinal := sp.isMetaHeaderFinal(hdr, orderedMetaBlocks, i+1)
		if !isFinal {
			continue
		}

		maxTxRemaining := uint32(maxTxInBlock) - nrTxAdded
		currMBProcessed, currTxsAdded, hdrProcessFinished := sp.createAndprocessMiniBlocksFromHeader(hdr, maxTxRemaining, round, haveTime)

		// all txs processed, add to processed miniblocks
		miniBlocks = append(miniBlocks, currMBProcessed...)
		nrTxAdded = nrTxAdded + currTxsAdded

		if currTxsAdded > 0 {
			usedMetaHdrsHashes = append(usedMetaHdrsHashes, orderedMetaBlocks[i].hash)
		}

		if !hdrProcessFinished {
			break
		}

		lastMetaHdr = hdr
	}

	sp.mutUsedMetaHdrsHashes.Lock()
	sp.usedMetaHdrsHashes[round] = usedMetaHdrsHashes
	sp.mutUsedMetaHdrsHashes.Unlock()

	return miniBlocks, nrTxAdded, nil
}

func (sp *shardProcessor) createMiniBlocks(
	noShards uint32,
	maxTxInBlock int,
	round uint32,
	haveTime func() bool,
) (block.Body, error) {

	miniBlocks := make(block.Body, 0)

	sp.txPreProcess.CreateBlockStarted()

	if sp.accounts.JournalLen() != 0 {
		return nil, process.ErrAccountStateDirty
	}

	if !haveTime() {
		log.Info(fmt.Sprintf("time is up after entered in createMiniBlocks method\n"))
		return miniBlocks, nil
	}

	txPool := sp.dataPool.Transactions()
	if txPool == nil {
		return nil, process.ErrNilTransactionPool
	}

	destMeMiniBlocks, txs, err := sp.createAndProcessCrossMiniBlocksDstMe(noShards, maxTxInBlock, round, haveTime)
	if err != nil {
		log.Info(err.Error())
	}

	log.Info(fmt.Sprintf("destMeMiniBlocks: %d and txs: %d\n", len(destMeMiniBlocks), txs))

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

	addedTxs := 0
	for i := 0; i < int(noShards); i++ {
		miniBlock, err := sp.txPreProcess.CreateAndProcessMiniBlock(sp.shardCoordinator.SelfId(), uint32(i), maxTxInBlock-addedTxs, haveTime, round)

		if len(miniBlock.TxHashes) > 0 {
			addedTxs += len(miniBlock.TxHashes)
			miniBlocks = append(miniBlocks, miniBlock)
		}

		if err != nil {
			return miniBlocks, nil
		}
	}

	log.Info(fmt.Sprintf("creating mini blocks has been finished: created %d mini blocks\n", len(miniBlocks)))
	return miniBlocks, nil
}

// CreateBlockHeader creates a miniblock header list given a block body
func (sp *shardProcessor) CreateBlockHeader(bodyHandler data.BodyHandler, round uint32, haveTime func() bool) (data.HeaderHandler, error) {
	// TODO: add PrevRandSeed and RandSeed when BLS signing is completed
	header := &block.Header{
		MiniBlockHeaders: make([]block.MiniBlockHeader, 0),
		RootHash:         sp.getRootHash(),
		ShardId:          sp.shardCoordinator.SelfId(),
		PrevRandSeed:     make([]byte, 0),
		RandSeed:         make([]byte, 0),
	}

	go sp.checkAndRequestIfMetaHeadersMissing(header.GetRound())

	if bodyHandler == nil {
		return header, nil
	}

	body, ok := bodyHandler.(block.Body)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	mbLen := len(body)
	totalTxCount := 0
	miniBlockHeaders := make([]block.MiniBlockHeader, mbLen)
	for i := 0; i < mbLen; i++ {
		txCount := len(body[i].TxHashes)
		totalTxCount += txCount
		mbBytes, err := sp.marshalizer.Marshal(body[i])
		if err != nil {
			return nil, err
		}
		mbHash := sp.hasher.Compute(string(mbBytes))

		miniBlockHeaders[i] = block.MiniBlockHeader{
			Hash:            mbHash,
			SenderShardID:   body[i].SenderShardID,
			ReceiverShardID: body[i].ReceiverShardID,
			TxCount:         uint32(txCount),
		}
	}

	header.MiniBlockHeaders = miniBlockHeaders
	header.TxCount = uint32(totalTxCount)

	sp.mutUsedMetaHdrsHashes.Lock()

	if usedMetaHdrsHashes, ok := sp.usedMetaHdrsHashes[round]; ok {
		header.MetaBlockHashes = usedMetaHdrsHashes
		delete(sp.usedMetaHdrsHashes, round)
	}

	sp.mutUsedMetaHdrsHashes.Unlock()

	return header, nil
}

func (sp *shardProcessor) waitForMetaHdrHashes(waitTime time.Duration) error {
	select {
	case <-sp.chRcvAllMetaHdrs:
		return nil
	case <-time.After(waitTime):
		return process.ErrTimeIsOut
	}
}

// MarshalizedDataToBroadcast prepares underlying data into a marshalized object according to destination
func (sp *shardProcessor) MarshalizedDataToBroadcast(
	header data.HeaderHandler,
	bodyHandler data.BodyHandler,
) (map[uint32][]byte, map[uint32][][]byte, error) {

	if bodyHandler == nil {
		return nil, nil, process.ErrNilMiniBlocks
	}

	body, ok := bodyHandler.(block.Body)
	if !ok {
		return nil, nil, process.ErrWrongTypeAssertion
	}

	mrsData := make(map[uint32][]byte)
	mrsTxs := make(map[uint32][][]byte)
	bodies := make(map[uint32]block.Body)

	for i := 0; i < len(body); i++ {
		miniblock := body[i]
		receiverShardId := miniblock.ReceiverShardID
		if receiverShardId == sp.shardCoordinator.SelfId() { // not taking into account miniblocks for current shard
			continue
		}

		bodies[receiverShardId] = append(bodies[receiverShardId], miniblock)

		currMrsTxs, err := sp.txPreProcess.CreateMarshalizedData(miniblock.TxHashes)
		if err != nil {
			return nil, nil, err
		}
		mrsTxs[receiverShardId] = append(mrsTxs[receiverShardId], currMrsTxs...)
	}

	for shardId, subsetBlockBody := range bodies {
		buff, err := sp.marshalizer.Marshal(subsetBlockBody)
		if err != nil {
			return nil, nil, process.ErrMarshalWithoutSuccess
		}
		mrsData[shardId] = buff
	}

	return mrsData, mrsTxs, nil
}

// DecodeBlockBody method decodes block body from a given byte array
func (sp *shardProcessor) DecodeBlockBody(dta []byte) data.BodyHandler {
	if dta == nil {
		return nil
	}

	var body block.Body

	err := sp.marshalizer.Unmarshal(&body, dta)
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	return body
}

// DecodeBlockHeader method decodes block header from a given byte array
func (sp *shardProcessor) DecodeBlockHeader(dta []byte) data.HeaderHandler {
	if dta == nil {
		return nil
	}

	var header block.Header

	err := sp.marshalizer.Unmarshal(&header, dta)
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	return &header
}
