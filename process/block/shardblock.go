package block

import (
	"bytes"
	"fmt"
	"math/big"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/queue"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/indexer"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/block/bootstrapStorage"
	"github.com/ElrondNetwork/elrond-go/process/block/processedMb"
)

var _ process.BlockProcessor = (*shardProcessor)(nil)

const timeBetweenCheckForEpochStart = 100 * time.Millisecond

// shardProcessor implements shardProcessor interface and actually it tries to execute block
type shardProcessor struct {
	*baseProcessor
	metaBlockFinality uint32
	chRcvAllMetaHdrs  chan bool

	processedMiniBlocks   *processedMb.ProcessedMiniBlockTracker
	userStatePruningQueue core.Queue
}

// NewShardProcessor creates a new shardProcessor object
func NewShardProcessor(arguments ArgShardProcessor) (*shardProcessor, error) {
	err := checkProcessorNilParameters(arguments.ArgBaseProcessor)
	if err != nil {
		return nil, err
	}

	if check.IfNil(arguments.DataComponents.Datapool()) {
		return nil, process.ErrNilDataPoolHolder
	}
	if check.IfNil(arguments.DataComponents.Datapool().Headers()) {
		return nil, process.ErrNilHeadersDataPool
	}
	if check.IfNil(arguments.DataComponents.Datapool().Transactions()) {
		return nil, process.ErrNilTransactionPool
	}

	genesisHdr := arguments.DataComponents.Blockchain().GetGenesisHeader()
	base := &baseProcessor{
		accountsDB:                    arguments.AccountsDB,
		blockSizeThrottler:            arguments.BlockSizeThrottler,
		forkDetector:                  arguments.ForkDetector,
		hasher:                        arguments.CoreComponents.Hasher(),
		marshalizer:                   arguments.CoreComponents.InternalMarshalizer(),
		store:                         arguments.DataComponents.StorageService(),
		shardCoordinator:              arguments.BootstrapComponents.ShardCoordinator(),
		nodesCoordinator:              arguments.NodesCoordinator,
		uint64Converter:               arguments.CoreComponents.Uint64ByteSliceConverter(),
		requestHandler:                arguments.RequestHandler,
		appStatusHandler:              arguments.CoreComponents.StatusHandler(),
		blockChainHook:                arguments.BlockChainHook,
		txCoordinator:                 arguments.TxCoordinator,
		roundHandler:                  arguments.CoreComponents.RoundHandler(),
		epochStartTrigger:             arguments.EpochStartTrigger,
		headerValidator:               arguments.HeaderValidator,
		bootStorer:                    arguments.BootStorer,
		blockTracker:                  arguments.BlockTracker,
		dataPool:                      arguments.DataComponents.Datapool(),
		stateCheckpointModulus:        arguments.Config.StateTriesConfig.CheckpointRoundsModulus,
		blockChain:                    arguments.DataComponents.Blockchain(),
		feeHandler:                    arguments.FeeHandler,
		outportHandler:                arguments.StatusComponents.OutportHandler(),
		tpsBenchmark:                  arguments.StatusComponents.TpsBenchmark(),
		genesisNonce:                  genesisHdr.GetNonce(),
		headerIntegrityVerifier:       arguments.BootstrapComponents.HeaderIntegrityVerifier(),
		historyRepo:                   arguments.HistoryRepository,
		epochNotifier:                 arguments.EpochNotifier,
		vmContainerFactory:            arguments.VMContainersFactory,
		vmContainer:                   arguments.VmContainer,
		processDataTriesOnCommitEpoch: arguments.Config.Debug.EpochStart.ProcessDataTrieOnCommitEpoch,
	}

	sp := shardProcessor{
		baseProcessor: base,
	}

	sp.txCounter = NewTransactionCounter()
	sp.requestBlockBodyHandler = &sp
	sp.blockProcessor = &sp

	sp.chRcvAllMetaHdrs = make(chan bool)

	sp.hdrsForCurrBlock = newHdrForBlock()
	sp.processedMiniBlocks = processedMb.NewProcessedMiniBlocks()

	headersPool := sp.dataPool.Headers()
	headersPool.RegisterHandler(sp.receivedMetaBlock)

	sp.metaBlockFinality = process.BlockFinality
	sp.userStatePruningQueue = queue.NewSliceQueue(arguments.Config.StateTriesConfig.UserStatePruningQueueSize)

	return &sp, nil
}

// ProcessBlock processes a block. It returns nil if all ok or the specific error
func (sp *shardProcessor) ProcessBlock(
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
	haveTime func() time.Duration,
) error {

	if haveTime == nil {
		return process.ErrNilHaveTimeHandler
	}

	err := sp.checkBlockValidity(headerHandler, bodyHandler)
	if err != nil {
		if err == process.ErrBlockHashDoesNotMatch {
			log.Debug("requested missing shard header",
				"hash", headerHandler.GetPrevHash(),
				"for shard", headerHandler.GetShardID(),
			)

			go sp.requestHandler.RequestShardHeader(headerHandler.GetShardID(), headerHandler.GetPrevHash())
		}

		return err
	}

	sp.epochNotifier.CheckEpoch(headerHandler)
	sp.requestHandler.SetEpoch(headerHandler.GetEpoch())

	log.Debug("started processing block",
		"epoch", headerHandler.GetEpoch(),
		"round", headerHandler.GetRound(),
		"nonce", headerHandler.GetNonce(),
	)

	header, ok := headerHandler.(*block.Header)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	go getMetricsFromBlockBody(body, sp.marshalizer, sp.appStatusHandler)

	err = sp.checkHeaderBodyCorrelation(header.MiniBlockHeaders, body)
	if err != nil {
		return err
	}

	txCounts, rewardCounts, unsignedCounts := sp.txCounter.getPoolCounts(sp.dataPool)
	log.Debug("total txs in pool", "counts", txCounts.String())
	log.Debug("total txs in rewards pool", "counts", rewardCounts.String())
	log.Debug("total txs in unsigned pool", "counts", unsignedCounts.String())

	go getMetricsFromHeader(header, uint64(txCounts.GetTotal()), sp.marshalizer, sp.appStatusHandler)

	sp.createBlockStarted()
	sp.blockChainHook.SetCurrentHeader(headerHandler)

	sp.txCoordinator.RequestBlockTransactions(body)
	requestedMetaHdrs, requestedFinalityAttestingMetaHdrs := sp.requestMetaHeaders(header)

	if haveTime() < 0 {
		return process.ErrTimeIsOut
	}

	err = sp.txCoordinator.IsDataPreparedForProcessing(haveTime)
	if err != nil {
		return err
	}

	haveMissingMetaHeaders := requestedMetaHdrs > 0 || requestedFinalityAttestingMetaHdrs > 0
	if haveMissingMetaHeaders {
		if requestedMetaHdrs > 0 {
			log.Debug("requested missing meta headers",
				"num headers", requestedMetaHdrs,
			)
		}
		if requestedFinalityAttestingMetaHdrs > 0 {
			log.Debug("requested missing finality attesting meta headers",
				"num finality meta headers", requestedFinalityAttestingMetaHdrs,
			)
		}

		err = sp.waitForMetaHdrHashes(haveTime())

		sp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
		missingMetaHdrs := sp.hdrsForCurrBlock.missingHdrs
		sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

		sp.hdrsForCurrBlock.resetMissingHdrs()

		if requestedMetaHdrs > 0 {
			log.Debug("received missing meta headers",
				"num headers", requestedMetaHdrs-missingMetaHdrs,
			)
		}

		if err != nil {
			return err
		}
	}

	err = sp.requestEpochStartInfo(header, haveTime)
	if err != nil {
		return err
	}

	if sp.accountsDB[state.UserAccountsState].JournalLen() != 0 {
		return process.ErrAccountStateDirty
	}

	defer func() {
		go sp.checkAndRequestIfMetaHeadersMissing()
	}()

	err = sp.checkEpochCorrectnessCrossChain()
	if err != nil {
		return err
	}

	err = sp.checkEpochCorrectness(header)
	if err != nil {
		return err
	}

	err = sp.checkMetaHeadersValidityAndFinality()
	if err != nil {
		return err
	}

	err = sp.verifyCrossShardMiniBlockDstMe(header)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			sp.RevertAccountState(header)
		}
	}()

	startTime := time.Now()
	err = sp.txCoordinator.ProcessBlockTransaction(body, haveTime)
	elapsedTime := time.Since(startTime)
	log.Debug("elapsed time to process block transaction",
		"time [s]", elapsedTime,
	)
	if err != nil {
		return err
	}

	err = sp.txCoordinator.VerifyCreatedBlockTransactions(header, body)
	if err != nil {
		return err
	}

	err = sp.txCoordinator.VerifyCreatedMiniBlocks(header, body)
	if err != nil {
		return err
	}

	err = sp.verifyFees(header)
	if err != nil {
		return err
	}

	if !sp.verifyStateRoot(header.GetRootHash()) {
		err = process.ErrRootStateDoesNotMatch
		return err
	}

	return nil
}

func (sp *shardProcessor) requestEpochStartInfo(header *block.Header, haveTime func() time.Duration) error {
	if !header.IsStartOfEpochBlock() {
		return nil
	}
	if sp.epochStartTrigger.MetaEpoch() >= header.GetEpoch() {
		return nil
	}
	if sp.epochStartTrigger.IsEpochStart() {
		return nil
	}

	go sp.requestHandler.RequestMetaHeader(header.EpochStartMetaHash)

	headersPool := sp.dataPool.Headers()
	for {
		time.Sleep(timeBetweenCheckForEpochStart)
		if haveTime() < 0 {
			break
		}

		if sp.epochStartTrigger.IsEpochStart() {
			return nil
		}

		epochStartMetaHdr, err := headersPool.GetHeaderByHash(header.EpochStartMetaHash)
		if err != nil {
			continue
		}

		_, _, err = headersPool.GetHeadersByNonceAndShardId(epochStartMetaHdr.GetNonce()+1, core.MetachainShardId)
		if err != nil {
			go sp.requestHandler.RequestMetaHeaderByNonce(epochStartMetaHdr.GetNonce() + 1)
			continue
		}

		return nil
	}

	return process.ErrTimeIsOut
}

// RevertStateToBlock recreates the state tries to the root hashes indicated by the provided header
func (sp *shardProcessor) RevertStateToBlock(header data.HeaderHandler) error {

	err := sp.accountsDB[state.UserAccountsState].RecreateTrie(header.GetRootHash())
	if err != nil {
		log.Debug("recreate trie with error for header",
			"nonce", header.GetNonce(),
			"hash", header.GetRootHash(),
			"error", err,
		)

		return err
	}

	err = sp.epochStartTrigger.RevertStateToBlock(header)
	if err != nil {
		log.Debug("revert epoch start trigger for header",
			"nonce", header.GetNonce(),
			"error", err,
		)
		return err
	}

	return nil
}

func (sp *shardProcessor) checkEpochCorrectness(
	header *block.Header,
) error {
	currentBlockHeader := sp.blockChain.GetCurrentBlockHeader()
	if check.IfNil(currentBlockHeader) {
		return nil
	}

	headerEpochBehindCurrentHeader := header.GetEpoch() < currentBlockHeader.GetEpoch()
	if headerEpochBehindCurrentHeader {
		return fmt.Errorf("%w proposed header with older epoch %d than blockchain epoch %d",
			process.ErrEpochDoesNotMatch, header.GetEpoch(), currentBlockHeader.GetEpoch())
	}

	isStartOfEpochButShouldNotBe := header.GetEpoch() == currentBlockHeader.GetEpoch() && header.IsStartOfEpochBlock()
	if isStartOfEpochButShouldNotBe {
		return fmt.Errorf("%w proposed header with same epoch %d as blockchain and it is of epoch start",
			process.ErrEpochDoesNotMatch, currentBlockHeader.GetEpoch())
	}

	incorrectStartOfEpochBlock := header.GetEpoch() != currentBlockHeader.GetEpoch() &&
		sp.epochStartTrigger.MetaEpoch() == currentBlockHeader.GetEpoch()
	if incorrectStartOfEpochBlock {
		return fmt.Errorf("%w proposed header with new epoch %d with trigger still in last epoch %d",
			process.ErrEpochDoesNotMatch, header.GetEpoch(), sp.epochStartTrigger.MetaEpoch())
	}

	isHeaderOfInvalidEpoch := header.GetEpoch() > sp.epochStartTrigger.MetaEpoch()
	if isHeaderOfInvalidEpoch {
		return fmt.Errorf("%w proposed header with epoch too high %d with trigger in epoch %d",
			process.ErrEpochDoesNotMatch, header.GetEpoch(), sp.epochStartTrigger.MetaEpoch())
	}

	isOldEpochAndShouldBeNew := sp.epochStartTrigger.IsEpochStart() &&
		header.GetRound() > sp.epochStartTrigger.EpochFinalityAttestingRound()+process.EpochChangeGracePeriod &&
		header.GetEpoch() < sp.epochStartTrigger.MetaEpoch() &&
		sp.epochStartTrigger.EpochStartRound() < sp.epochStartTrigger.EpochFinalityAttestingRound()
	if isOldEpochAndShouldBeNew {
		return fmt.Errorf("%w proposed header with epoch %d should be in epoch %d",
			process.ErrEpochDoesNotMatch, header.GetEpoch(), sp.epochStartTrigger.MetaEpoch())
	}

	isEpochStartMetaHashIncorrect := header.IsStartOfEpochBlock() &&
		!bytes.Equal(header.EpochStartMetaHash, sp.epochStartTrigger.EpochStartMetaHdrHash()) &&
		header.GetEpoch() == sp.epochStartTrigger.MetaEpoch()
	if isEpochStartMetaHashIncorrect {
		go sp.requestHandler.RequestMetaHeader(header.EpochStartMetaHash)
		log.Warn("epoch start meta hash mismatch", "proposed", header.EpochStartMetaHash, "calculated", sp.epochStartTrigger.EpochStartMetaHdrHash())
		return fmt.Errorf("%w proposed header with epoch %d has invalid epochStartMetaHash",
			process.ErrEpochDoesNotMatch, header.GetEpoch())
	}

	isNotEpochStartButShouldBe := header.GetEpoch() != currentBlockHeader.GetEpoch() &&
		!header.IsStartOfEpochBlock()
	if isNotEpochStartButShouldBe {
		return fmt.Errorf("%w proposed header with new epoch %d is not of type epoch start",
			process.ErrEpochDoesNotMatch, header.GetEpoch())
	}

	isOldEpochStart := header.IsStartOfEpochBlock() && header.GetEpoch() < sp.epochStartTrigger.MetaEpoch()
	if isOldEpochStart {
		metaBlock, err := process.GetMetaHeader(header.EpochStartMetaHash, sp.dataPool.Headers(), sp.marshalizer, sp.store)
		if err != nil {
			go sp.requestHandler.RequestStartOfEpochMetaBlock(header.GetEpoch())
			return fmt.Errorf("%w could not find epoch start metablock for epoch %d",
				err, header.GetEpoch())
		}

		isMetaBlockCorrect := metaBlock.IsStartOfEpochBlock() && metaBlock.GetEpoch() == header.GetEpoch()
		if !isMetaBlockCorrect {
			return fmt.Errorf("%w proposed header with epoch %d does not include correct start of epoch metaBlock %s",
				process.ErrEpochDoesNotMatch, header.GetEpoch(), header.EpochStartMetaHash)
		}
	}

	return nil
}

// SetNumProcessedObj will set the num of processed transactions
func (sp *shardProcessor) SetNumProcessedObj(numObj uint64) {
	sp.txCounter.totalTxs = numObj
}

// checkMetaHeadersValidity - checks if listed metaheaders are valid as construction
func (sp *shardProcessor) checkMetaHeadersValidityAndFinality() error {
	lastCrossNotarizedHeader, _, err := sp.blockTracker.GetLastCrossNotarizedHeader(core.MetachainShardId)
	if err != nil {
		return err
	}

	usedMetaHdrs := sp.sortHeadersForCurrentBlockByNonce(true)
	if len(usedMetaHdrs[core.MetachainShardId]) == 0 {
		return nil
	}

	for _, metaHdr := range usedMetaHdrs[core.MetachainShardId] {
		err = sp.headerValidator.IsHeaderConstructionValid(metaHdr, lastCrossNotarizedHeader)
		if err != nil {
			return fmt.Errorf("%w : checkMetaHeadersValidityAndFinality -> isHdrConstructionValid", err)
		}

		lastCrossNotarizedHeader = metaHdr
	}

	err = sp.checkMetaHdrFinality(lastCrossNotarizedHeader)
	if err != nil {
		return err
	}

	return nil
}

// check if shard headers are final by checking if newer headers were constructed upon them
func (sp *shardProcessor) checkMetaHdrFinality(header data.HeaderHandler) error {
	if check.IfNil(header) {
		return process.ErrNilBlockHeader
	}

	finalityAttestingMetaHdrs := sp.sortHeadersForCurrentBlockByNonce(false)

	lastVerifiedHdr := header
	// verify if there are "K" block after current to make this one final
	nextBlocksVerified := uint32(0)
	for _, metaHdr := range finalityAttestingMetaHdrs[core.MetachainShardId] {
		if nextBlocksVerified >= sp.metaBlockFinality {
			break
		}

		// found a header with the next nonce
		if metaHdr.GetNonce() == lastVerifiedHdr.GetNonce()+1 {
			err := sp.headerValidator.IsHeaderConstructionValid(metaHdr, lastVerifiedHdr)
			if err != nil {
				log.Debug("checkMetaHdrFinality -> isHdrConstructionValid",
					"error", err.Error())
				continue
			}

			lastVerifiedHdr = metaHdr
			nextBlocksVerified += 1
		}
	}

	if nextBlocksVerified < sp.metaBlockFinality {
		go sp.requestHandler.RequestMetaHeaderByNonce(lastVerifiedHdr.GetNonce())
		go sp.requestHandler.RequestMetaHeaderByNonce(lastVerifiedHdr.GetNonce() + 1)
		return process.ErrHeaderNotFinal
	}

	return nil
}

func (sp *shardProcessor) checkAndRequestIfMetaHeadersMissing() {
	orderedMetaBlocks, _ := sp.blockTracker.GetTrackedHeaders(core.MetachainShardId)

	err := sp.requestHeadersIfMissing(orderedMetaBlocks, core.MetachainShardId)
	if err != nil {
		log.Debug("checkAndRequestIfMetaHeadersMissing", "error", err.Error())
	}
}

func (sp *shardProcessor) indexBlockIfNeeded(
	body data.BodyHandler,
	headerHash []byte,
	header data.HeaderHandler,
	lastBlockHeader data.HeaderHandler,
) {
	if !sp.outportHandler.HasDrivers() {
		return
	}
	if check.IfNil(header) {
		return
	}
	if check.IfNil(body) {
		return
	}

	log.Debug("preparing to index block", "hash", headerHash, "nonce", header.GetNonce(), "round", header.GetRound())

	pool := &indexer.Pool{
		Txs:      sp.txCoordinator.GetAllCurrentUsedTxs(block.TxBlock),
		Scrs:     sp.txCoordinator.GetAllCurrentUsedTxs(block.SmartContractResultBlock),
		Rewards:  sp.txCoordinator.GetAllCurrentUsedTxs(block.RewardsBlock),
		Invalid:  sp.txCoordinator.GetAllCurrentUsedTxs(block.InvalidBlock),
		Receipts: sp.txCoordinator.GetAllCurrentUsedTxs(block.ReceiptBlock),
		Logs:     sp.txCoordinator.GetAllCurrentLogs(),
	}

	shardId := sp.shardCoordinator.SelfId()

	// TODO: remove if epoch start block needs to be validated by the new epoch nodes
	epoch := header.GetEpoch()
	if header.IsStartOfEpochBlock() && epoch > 0 {
		epoch = epoch - 1
	}

	pubKeys, err := sp.nodesCoordinator.GetConsensusValidatorsPublicKeys(
		header.GetPrevRandSeed(),
		header.GetRound(),
		shardId,
		epoch,
	)
	if err != nil {
		log.Debug("indexBlockIfNeeded: GetConsensusValidatorsPublicKeys",
			"hash", headerHash,
			"epoch", epoch,
			"error", err.Error())
		return
	}

	nodesCoordinatorShardID, err := sp.nodesCoordinator.ShardIdForEpoch(epoch)
	if err != nil {
		log.Debug("indexBlockIfNeeded: ShardIdForEpoch",
			"hash", headerHash,
			"epoch", epoch,
			"error", err.Error())
		return
	}

	if shardId != nodesCoordinatorShardID {
		log.Debug("indexBlockIfNeeded: shardId != nodesCoordinatorShardID",
			"epoch", epoch,
			"shardCoordinator.ShardID", shardId,
			"nodesCoordinator.ShardID", nodesCoordinatorShardID)
		return
	}

	signersIndexes, err := sp.nodesCoordinator.GetValidatorsIndexes(pubKeys, epoch)
	if err != nil {
		log.Error("indexBlockIfNeeded: GetValidatorsIndexes",
			"round", header.GetRound(),
			"nonce", header.GetNonce(),
			"hash", headerHash,
			"error", err.Error(),
		)
		return
	}

	args := &indexer.ArgsSaveBlockData{
		HeaderHash:             headerHash,
		Body:                   body,
		Header:                 header,
		SignersIndexes:         signersIndexes,
		NotarizedHeadersHashes: nil,
		TransactionsPool:       pool,
	}

	sp.outportHandler.SaveBlock(args)
	log.Debug("indexed block", "hash", headerHash, "nonce", header.GetNonce(), "round", header.GetRound())

	indexRoundInfo(sp.outportHandler, sp.nodesCoordinator, shardId, header, lastBlockHeader, signersIndexes)
}

// RestoreBlockIntoPools restores the TxBlock and MetaBlock into associated pools
func (sp *shardProcessor) RestoreBlockIntoPools(headerHandler data.HeaderHandler, bodyHandler data.BodyHandler) error {
	if check.IfNil(headerHandler) {
		return process.ErrNilBlockHeader
	}

	header, ok := headerHandler.(*block.Header)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	miniBlockHashes := header.MapMiniBlockHashesToShards()
	err := sp.restoreMetaBlockIntoPool(miniBlockHashes, header.MetaBlockHashes)
	if err != nil {
		return err
	}

	sp.restoreBlockBody(bodyHandler)

	sp.blockTracker.RemoveLastNotarizedHeaders()

	return nil
}

func (sp *shardProcessor) restoreMetaBlockIntoPool(mapMiniBlockHashes map[string]uint32, metaBlockHashes [][]byte) error {
	headersPool := sp.dataPool.Headers()

	mapMetaHashMiniBlockHashes := make(map[string][][]byte, len(metaBlockHashes))

	for _, metaBlockHash := range metaBlockHashes {
		metaBlock, errNotCritical := process.GetMetaHeaderFromStorage(metaBlockHash, sp.marshalizer, sp.store)
		if errNotCritical != nil {
			log.Debug("meta block is not fully processed yet and not committed in MetaBlockUnit",
				"hash", metaBlockHash)
			continue
		}

		processedMiniBlocks := metaBlock.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
		for mbHash := range processedMiniBlocks {
			mapMetaHashMiniBlockHashes[string(metaBlockHash)] = append(mapMetaHashMiniBlockHashes[string(metaBlockHash)], []byte(mbHash))
		}

		headersPool.AddHeader(metaBlockHash, metaBlock)

		err := sp.store.GetStorer(dataRetriever.MetaBlockUnit).Remove(metaBlockHash)
		if err != nil {
			log.Debug("unable to remove hash from MetaBlockUnit",
				"hash", metaBlockHash)
			return err
		}

		nonceToByteSlice := sp.uint64Converter.ToByteSlice(metaBlock.GetNonce())
		errNotCritical = sp.store.GetStorer(dataRetriever.MetaHdrNonceHashDataUnit).Remove(nonceToByteSlice)
		if errNotCritical != nil {
			log.Debug("error not critical",
				"error", errNotCritical.Error())
		}

		log.Trace("meta block has been restored successfully",
			"round", metaBlock.Round,
			"nonce", metaBlock.Nonce,
			"hash", metaBlockHash)
	}

	for metaBlockHash, miniBlockHashes := range mapMetaHashMiniBlockHashes {
		for _, miniBlockHash := range miniBlockHashes {
			sp.processedMiniBlocks.AddMiniBlockHash(metaBlockHash, string(miniBlockHash))
		}
	}

	for miniBlockHash := range mapMiniBlockHashes {
		sp.processedMiniBlocks.RemoveMiniBlockHash(miniBlockHash)
	}

	return nil
}

// CreateBlock creates the final block and header for the current round
func (sp *shardProcessor) CreateBlock(
	initialHdr data.HeaderHandler,
	haveTime func() bool,
) (data.HeaderHandler, data.BodyHandler, error) {
	if check.IfNil(initialHdr) {
		return nil, nil, process.ErrNilBlockHeader
	}
	shardHdr, ok := initialHdr.(*block.Header)
	if !ok {
		return nil, nil, process.ErrWrongTypeAssertion
	}

	sp.createBlockStarted()

	if sp.epochStartTrigger.IsEpochStart() {
		log.Debug("CreateBlock", "IsEpochStart", sp.epochStartTrigger.IsEpochStart(),
			"epoch start meta header hash", sp.epochStartTrigger.EpochStartMetaHdrHash())
		shardHdr.EpochStartMetaHash = sp.epochStartTrigger.EpochStartMetaHdrHash()
	}

	shardHdr.SetEpoch(sp.epochStartTrigger.MetaEpoch())
	sp.epochNotifier.CheckEpoch(shardHdr)
	sp.blockChainHook.SetCurrentHeader(shardHdr)
	shardHdr.SoftwareVersion = []byte(sp.headerIntegrityVerifier.GetVersion(shardHdr.Epoch))
	body, err := sp.createBlockBody(shardHdr, haveTime)
	if err != nil {
		return nil, nil, err
	}

	finalBody, err := sp.applyBodyToHeader(shardHdr, body)
	if err != nil {
		return nil, nil, err
	}

	for _, miniBlock := range finalBody.MiniBlocks {
		log.Trace("CreateBlock: miniblock",
			"sender shard", miniBlock.SenderShardID,
			"receiver shard", miniBlock.ReceiverShardID,
			"type", miniBlock.Type,
			"num txs", len(miniBlock.TxHashes))
	}

	return shardHdr, finalBody, nil
}

// createBlockBody creates a a list of miniblocks by filling them with transactions out of the transactions pools
// as long as the transactions limit for the block has not been reached and there is still time to add transactions
func (sp *shardProcessor) createBlockBody(shardHdr *block.Header, haveTime func() bool) (*block.Body, error) {
	sp.blockSizeThrottler.ComputeCurrentMaxSize()

	log.Debug("started creating block body",
		"epoch", shardHdr.GetEpoch(),
		"round", shardHdr.GetRound(),
		"nonce", shardHdr.GetNonce(),
	)

	miniBlocks, err := sp.createMiniBlocks(haveTime)
	if err != nil {
		return nil, err
	}

	sp.requestHandler.SetEpoch(shardHdr.GetEpoch())

	return miniBlocks, nil
}

// CommitBlock commits the block in the blockchain if everything was checked successfully
func (sp *shardProcessor) CommitBlock(
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
) error {
	var err error
	defer func() {
		if err != nil {
			sp.RevertAccountState(headerHandler)
		}
	}()

	err = checkForNils(headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	sp.store.SetEpochForPutOperation(headerHandler.GetEpoch())

	log.Debug("started committing block",
		"epoch", headerHandler.GetEpoch(),
		"round", headerHandler.GetRound(),
		"nonce", headerHandler.GetNonce(),
	)

	err = sp.checkBlockValidity(headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	header, ok := headerHandler.(*block.Header)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	err = sp.checkEpochCorrectnessCrossChain()
	if err != nil {
		return err
	}

	if header.IsStartOfEpochBlock() {
		sp.epochStartTrigger.SetProcessed(header, bodyHandler)
	}

	marshalizedHeader, err := sp.marshalizer.Marshal(header)
	if err != nil {
		return err
	}

	headerHash := sp.hasher.Compute(string(marshalizedHeader))

	sp.saveShardHeader(header, headerHash, marshalizedHeader)

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	sp.saveBody(body, header)

	processedMetaHdrs, err := sp.getOrderedProcessedMetaBlocksFromHeader(header)
	if err != nil {
		return err
	}

	err = sp.addProcessedCrossMiniBlocksFromHeader(header)
	if err != nil {
		return err
	}

	selfNotarizedHeaders, selfNotarizedHeadersHashes, err := sp.getHighestHdrForOwnShardFromMetachain(processedMetaHdrs)
	if err != nil {
		return err
	}

	err = sp.saveLastNotarizedHeader(core.MetachainShardId, processedMetaHdrs)
	if err != nil {
		return err
	}

	err = sp.commitAll()
	if err != nil {
		return err
	}

	log.Info("shard block has been committed successfully",
		"epoch", header.Epoch,
		"round", header.Round,
		"nonce", header.Nonce,
		"shard id", header.ShardID,
		"hash", headerHash,
	)

	errNotCritical := sp.updateCrossShardInfo(processedMetaHdrs)
	if errNotCritical != nil {
		log.Debug("updateCrossShardInfo", "error", errNotCritical.Error())
	}

	errNotCritical = sp.forkDetector.AddHeader(header, headerHash, process.BHProcessed, selfNotarizedHeaders, selfNotarizedHeadersHashes)
	if errNotCritical != nil {
		log.Debug("forkDetector.AddHeader", "error", errNotCritical.Error())
	}

	currentHeader, currentHeaderHash := getLastSelfNotarizedHeaderByItself(sp.blockChain)
	sp.blockTracker.AddSelfNotarizedHeader(sp.shardCoordinator.SelfId(), currentHeader, currentHeaderHash)

	lastSelfNotarizedHeader, lastSelfNotarizedHeaderHash := sp.getLastSelfNotarizedHeaderByMetachain()
	sp.blockTracker.AddSelfNotarizedHeader(core.MetachainShardId, lastSelfNotarizedHeader, lastSelfNotarizedHeaderHash)

	sp.notifyFinalMetaHdrs(processedMetaHdrs)

	sp.updateState(selfNotarizedHeaders, header)

	highestFinalBlockNonce := sp.forkDetector.GetHighestFinalBlockNonce()
	log.Debug("highest final shard block",
		"nonce", highestFinalBlockNonce,
		"shard", sp.shardCoordinator.SelfId(),
	)

	lastBlockHeader := sp.blockChain.GetCurrentBlockHeader()

	err = sp.blockChain.SetCurrentBlockHeader(header)
	if err != nil {
		return err
	}

	sp.blockChain.SetCurrentBlockHeaderHash(headerHash)
	sp.indexBlockIfNeeded(bodyHandler, headerHash, headerHandler, lastBlockHeader)
	sp.recordBlockInHistory(headerHash, headerHandler, bodyHandler)

	lastCrossNotarizedHeader, _, err := sp.blockTracker.GetLastCrossNotarizedHeader(core.MetachainShardId)
	if err != nil {
		return err
	}

	saveMetricsForCommittedShardBlock(
		sp.nodesCoordinator,
		sp.appStatusHandler,
		logger.DisplayByteSlice(headerHash),
		highestFinalBlockNonce,
		lastCrossNotarizedHeader,
		header,
	)

	headerInfo := bootstrapStorage.BootstrapHeaderInfo{
		ShardId: header.GetShardID(),
		Epoch:   header.GetEpoch(),
		Nonce:   header.GetNonce(),
		Hash:    headerHash,
	}

	nodesCoordinatorKey := sp.nodesCoordinator.GetSavedStateKey()
	epochStartKey := sp.epochStartTrigger.GetSavedStateKey()

	args := bootStorerDataArgs{
		headerInfo:                 headerInfo,
		round:                      header.Round,
		lastSelfNotarizedHeaders:   sp.getBootstrapHeadersInfo(selfNotarizedHeaders, selfNotarizedHeadersHashes),
		highestFinalBlockNonce:     sp.forkDetector.GetHighestFinalBlockNonce(),
		processedMiniBlocks:        sp.processedMiniBlocks.ConvertProcessedMiniBlocksMapToSlice(),
		nodesCoordinatorConfigKey:  nodesCoordinatorKey,
		epochStartTriggerConfigKey: epochStartKey,
	}

	sp.prepareDataForBootStorer(args)

	// write data to log
	go sp.txCounter.displayLogInfo(
		header,
		body,
		headerHash,
		sp.shardCoordinator.NumberOfShards(),
		sp.shardCoordinator.SelfId(),
		sp.dataPool,
		sp.appStatusHandler,
		sp.blockTracker,
	)

	sp.blockSizeThrottler.Succeed(header.Round)

	sp.displayPoolsInfo()

	errNotCritical = sp.removeTxsFromPools(bodyHandler)
	if errNotCritical != nil {
		log.Debug("removeTxsFromPools", "error", errNotCritical.Error())
	}

	sp.cleanupPools(headerHandler)

	return nil
}

func (sp *shardProcessor) notifyFinalMetaHdrs(processedMetaHeaders []data.HeaderHandler) {
	metaHeaders := make([]data.HeaderHandler, 0)
	metaHeadersHashes := make([][]byte, 0)

	for _, metaHeader := range processedMetaHeaders {
		metaHeaderHash, err := core.CalculateHash(sp.marshalizer, sp.hasher, metaHeader)
		if err != nil {
			log.Debug("shardProcessor.notifyFinalMetaHdrs", "error", err.Error())
			continue
		}

		metaHeaders = append(metaHeaders, metaHeader)
		metaHeadersHashes = append(metaHeadersHashes, metaHeaderHash)
	}

	if len(metaHeaders) > 0 {
		go sp.historyRepo.OnNotarizedBlocks(core.MetachainShardId, metaHeaders, metaHeadersHashes)
	}
}

func (sp *shardProcessor) displayPoolsInfo() {
	headersPool := sp.dataPool.Headers()
	miniBlocksPool := sp.dataPool.MiniBlocks()

	log.Trace("pools info",
		"shard", sp.shardCoordinator.SelfId(),
		"num headers", headersPool.GetNumHeaders(sp.shardCoordinator.SelfId()))

	log.Trace("pools info",
		"shard", core.MetachainShardId,
		"num headers", headersPool.GetNumHeaders(core.MetachainShardId))

	// numShardsToKeepHeaders represents the total number of shards for which shard node would keep tracking headers
	// (in this case this number is equal with: self shard + metachain)
	numShardsToKeepHeaders := 2
	capacity := headersPool.MaxSize() * numShardsToKeepHeaders
	log.Debug("pools info",
		"total headers", headersPool.Len(),
		"headers pool capacity", capacity,
		"total miniblocks", miniBlocksPool.Len(),
		"miniblocks pool capacity", miniBlocksPool.MaxSize(),
	)

	sp.displayMiniBlocksPool()
}

func (sp *shardProcessor) updateState(headers []data.HeaderHandler, currentHeader *block.Header) {
	sp.snapShotEpochStartFromMeta(currentHeader)

	for _, hdr := range headers {
		if sp.forkDetector.GetHighestFinalBlockNonce() < hdr.GetNonce() {
			break
		}

		prevHeader, errNotCritical := process.GetShardHeader(
			hdr.GetPrevHash(),
			sp.dataPool.Headers(),
			sp.marshalizer,
			sp.store,
		)
		if errNotCritical != nil {
			log.Debug("could not get shard header from storage")
			return
		}
		if hdr.IsStartOfEpochBlock() {
			sp.nodesCoordinator.ShuffleOutForEpoch(hdr.GetEpoch())
		}

		log.Trace("updateState: prevHeader",
			"shard", prevHeader.GetShardID(),
			"epoch", prevHeader.GetEpoch(),
			"round", prevHeader.GetRound(),
			"nonce", prevHeader.GetNonce(),
			"root hash", prevHeader.GetRootHash())

		log.Trace("updateState: currHeader",
			"shard", hdr.GetShardID(),
			"epoch", hdr.GetEpoch(),
			"round", hdr.GetRound(),
			"nonce", hdr.GetNonce(),
			"root hash", hdr.GetRootHash())

		sp.updateStateStorage(
			hdr,
			hdr.GetRootHash(),
			prevHeader.GetRootHash(),
			sp.accountsDB[state.UserAccountsState],
			sp.userStatePruningQueue,
		)
	}
}

func (sp *shardProcessor) snapShotEpochStartFromMeta(header *block.Header) {
	accounts := sp.accountsDB[state.UserAccountsState]
	if !accounts.IsPruningEnabled() {
		return
	}

	sp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	defer sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	for _, metaHash := range header.MetaBlockHashes {
		metaHdrInfo, ok := sp.hdrsForCurrBlock.hdrHashAndInfo[string(metaHash)]
		if !ok {
			continue
		}
		metaHdr, ok := metaHdrInfo.hdr.(*block.MetaBlock)
		if !ok {
			continue
		}
		if !metaHdr.IsStartOfEpochBlock() {
			continue
		}

		for _, epochStartShData := range metaHdr.EpochStart.LastFinalizedHeaders {
			if epochStartShData.ShardID != header.ShardID {
				continue
			}

			rootHash := epochStartShData.RootHash
			log.Debug("shard trie snapshot from epoch start shard data", "rootHash", rootHash)
			accounts.SnapshotState(rootHash)
			saveEpochStartEconomicsMetrics(sp.appStatusHandler, metaHdr)
			go func() {
				err := sp.commitTrieEpochRootHashIfNeeded(metaHdr, rootHash)
				if err != nil {
					log.Warn("couldn't commit trie checkpoint", "epoch", header.Epoch, "error", err)
				}
			}()
		}
	}
}

func (sp *shardProcessor) checkEpochCorrectnessCrossChain() error {
	currentHeader := sp.blockChain.GetCurrentBlockHeader()
	if check.IfNil(currentHeader) {
		return nil
	}
	if sp.epochStartTrigger.EpochStartRound() >= sp.epochStartTrigger.EpochFinalityAttestingRound() {
		return nil
	}

	lastSelfNotarizedHeader, _ := sp.getLastSelfNotarizedHeaderByMetachain()
	lastFinalizedRound := uint64(0)
	if !check.IfNil(lastSelfNotarizedHeader) {
		lastFinalizedRound = lastSelfNotarizedHeader.GetRound()
	}

	shouldRevertChain := false
	nonce := currentHeader.GetNonce()
	shouldEnterNewEpochRound := sp.epochStartTrigger.EpochFinalityAttestingRound() + process.EpochChangeGracePeriod

	for round := currentHeader.GetRound(); round > shouldEnterNewEpochRound && currentHeader.GetEpoch() < sp.epochStartTrigger.MetaEpoch(); round = currentHeader.GetRound() {
		if round <= lastFinalizedRound {
			break
		}

		shouldRevertChain = true
		prevHeader, err := process.GetShardHeader(
			currentHeader.GetPrevHash(),
			sp.dataPool.Headers(),
			sp.marshalizer,
			sp.store,
		)
		if err != nil {
			return err
		}

		nonce = currentHeader.GetNonce()
		currentHeader = prevHeader
	}

	if shouldRevertChain {
		log.Debug("blockchain is wrongly constructed",
			"reverted to nonce", nonce)

		sp.forkDetector.SetRollBackNonce(nonce)
		return process.ErrEpochDoesNotMatch
	}

	return nil
}

func (sp *shardProcessor) getLastSelfNotarizedHeaderByMetachain() (data.HeaderHandler, []byte) {
	if sp.forkDetector.GetHighestFinalBlockNonce() == sp.genesisNonce {
		return sp.blockChain.GetGenesisHeader(), sp.blockChain.GetGenesisHeaderHash()
	}

	hash := sp.forkDetector.GetHighestFinalBlockHash()
	header, err := process.GetShardHeader(hash, sp.dataPool.Headers(), sp.marshalizer, sp.store)
	if err != nil {
		log.Warn("getLastSelfNotarizedHeaderByMetachain.GetShardHeader", "error", err.Error(), "hash", hash, "nonce", sp.forkDetector.GetHighestFinalBlockNonce())
		return nil, nil
	}

	return header, hash
}

func (sp *shardProcessor) saveLastNotarizedHeader(shardId uint32, processedHdrs []data.HeaderHandler) error {
	lastCrossNotarizedHeader, lastCrossNotarizedHeaderHash, err := sp.blockTracker.GetLastCrossNotarizedHeader(shardId)
	if err != nil {
		return err
	}

	lenProcessedHdrs := len(processedHdrs)
	if lenProcessedHdrs > 0 {
		if lastCrossNotarizedHeader.GetNonce() < processedHdrs[lenProcessedHdrs-1].GetNonce() {
			lastCrossNotarizedHeader = processedHdrs[lenProcessedHdrs-1]
			lastCrossNotarizedHeaderHash, err = core.CalculateHash(sp.marshalizer, sp.hasher, lastCrossNotarizedHeader)
			if err != nil {
				return err
			}
		}
	}

	sp.blockTracker.AddCrossNotarizedHeader(shardId, lastCrossNotarizedHeader, lastCrossNotarizedHeaderHash)
	DisplayLastNotarized(sp.marshalizer, sp.hasher, lastCrossNotarizedHeader, shardId)

	return nil
}

// ApplyProcessedMiniBlocks will apply processed mini blocks
func (sp *shardProcessor) ApplyProcessedMiniBlocks(processedMiniBlocks *processedMb.ProcessedMiniBlockTracker) {
	sp.processedMiniBlocks = processedMiniBlocks
}

// CreateNewHeader creates a new header
func (sp *shardProcessor) CreateNewHeader(round uint64, nonce uint64) data.HeaderHandler {
	header := &block.Header{
		Nonce:           nonce,
		Round:           round,
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}

	return header
}

// getHighestHdrForOwnShardFromMetachain calculates the highest shard header notarized by metachain
func (sp *shardProcessor) getHighestHdrForOwnShardFromMetachain(
	processedHdrs []data.HeaderHandler,
) ([]data.HeaderHandler, [][]byte, error) {

	ownShIdHdrs := make([]data.HeaderHandler, 0, len(processedHdrs))

	for i := 0; i < len(processedHdrs); i++ {
		hdr, ok := processedHdrs[i].(*block.MetaBlock)
		if !ok {
			return nil, nil, process.ErrWrongTypeAssertion
		}

		hdrs := sp.getHighestHdrForShardFromMetachain(sp.shardCoordinator.SelfId(), hdr)
		ownShIdHdrs = append(ownShIdHdrs, hdrs...)
	}

	process.SortHeadersByNonce(ownShIdHdrs)

	ownShIdHdrsHashes := make([][]byte, len(ownShIdHdrs))
	for i := 0; i < len(ownShIdHdrs); i++ {
		hash, _ := core.CalculateHash(sp.marshalizer, sp.hasher, ownShIdHdrs[i])
		ownShIdHdrsHashes[i] = hash
	}

	return ownShIdHdrs, ownShIdHdrsHashes, nil
}

func (sp *shardProcessor) getHighestHdrForShardFromMetachain(shardId uint32, hdr *block.MetaBlock) []data.HeaderHandler {
	ownShIdHdr := make([]data.HeaderHandler, 0, len(hdr.ShardInfo))

	for _, shardInfo := range hdr.ShardInfo {
		if shardInfo.ShardID != shardId {
			continue
		}

		ownHdr, err := process.GetShardHeader(shardInfo.HeaderHash, sp.dataPool.Headers(), sp.marshalizer, sp.store)
		if err != nil {
			go sp.requestHandler.RequestShardHeader(shardInfo.ShardID, shardInfo.HeaderHash)

			log.Debug("requested missing shard header",
				"hash", shardInfo.HeaderHash,
				"shard", shardInfo.ShardID,
			)
			continue
		}

		ownShIdHdr = append(ownShIdHdr, ownHdr)
	}

	return data.TrimHeaderHandlerSlice(ownShIdHdr)
}

// getOrderedProcessedMetaBlocksFromHeader returns all the meta blocks fully processed
func (sp *shardProcessor) getOrderedProcessedMetaBlocksFromHeader(header *block.Header) ([]data.HeaderHandler, error) {
	if header == nil {
		return nil, process.ErrNilBlockHeader
	}

	miniBlockHashes := make(map[int][]byte, len(header.MiniBlockHeaders))
	for i := 0; i < len(header.MiniBlockHeaders); i++ {
		miniBlockHashes[i] = header.MiniBlockHeaders[i].Hash
	}

	log.Trace("cross mini blocks in body",
		"num miniblocks", len(miniBlockHashes),
	)

	processedMetaBlocks, err := sp.getOrderedProcessedMetaBlocksFromMiniBlockHashes(miniBlockHashes)
	if err != nil {
		return nil, err
	}

	return processedMetaBlocks, nil
}

func (sp *shardProcessor) addProcessedCrossMiniBlocksFromHeader(header *block.Header) error {
	if header == nil {
		return process.ErrNilBlockHeader
	}

	miniBlockHashes := make(map[int][]byte, len(header.MiniBlockHeaders))
	for i := 0; i < len(header.MiniBlockHeaders); i++ {
		miniBlockHashes[i] = header.MiniBlockHeaders[i].Hash
	}

	sp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	for _, metaBlockHash := range header.MetaBlockHashes {
		headerInfo, ok := sp.hdrsForCurrBlock.hdrHashAndInfo[string(metaBlockHash)]
		if !ok {
			sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()
			return fmt.Errorf("%w : addProcessedCrossMiniBlocksFromHeader metaBlockHash = %s",
				process.ErrMissingHeader, logger.DisplayByteSlice(metaBlockHash))
		}

		metaBlock, ok := headerInfo.hdr.(*block.MetaBlock)
		if !ok {
			sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()
			return process.ErrWrongTypeAssertion
		}

		crossMiniBlockHashes := metaBlock.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
		for key, miniBlockHash := range miniBlockHashes {
			_, ok = crossMiniBlockHashes[string(miniBlockHash)]
			if !ok {
				continue
			}

			sp.processedMiniBlocks.AddMiniBlockHash(string(metaBlockHash), string(miniBlockHash))

			delete(miniBlockHashes, key)
		}
	}
	sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	return nil
}

func (sp *shardProcessor) getOrderedProcessedMetaBlocksFromMiniBlockHashes(
	miniBlockHashes map[int][]byte,
) ([]data.HeaderHandler, error) {

	processedMetaHdrs := make([]data.HeaderHandler, 0, len(sp.hdrsForCurrBlock.hdrHashAndInfo))
	processedCrossMiniBlocksHashes := make(map[string]bool, len(sp.hdrsForCurrBlock.hdrHashAndInfo))

	sp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	for metaBlockHash, headerInfo := range sp.hdrsForCurrBlock.hdrHashAndInfo {
		if !headerInfo.usedInBlock {
			continue
		}

		metaBlock, ok := headerInfo.hdr.(*block.MetaBlock)
		if !ok {
			sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()
			return nil, process.ErrWrongTypeAssertion
		}

		log.Trace("meta header",
			"nonce", metaBlock.Nonce,
		)

		crossMiniBlockHashes := metaBlock.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
		for hash := range crossMiniBlockHashes {
			processedCrossMiniBlocksHashes[hash] = sp.processedMiniBlocks.IsMiniBlockProcessed(metaBlockHash, hash)
		}

		for key, miniBlockHash := range miniBlockHashes {
			_, ok = crossMiniBlockHashes[string(miniBlockHash)]
			if !ok {
				continue
			}

			processedCrossMiniBlocksHashes[string(miniBlockHash)] = true

			delete(miniBlockHashes, key)
		}

		log.Trace("cross mini blocks in meta header",
			"num miniblocks", len(crossMiniBlockHashes),
		)

		processedAll := true
		for hash := range crossMiniBlockHashes {
			if !processedCrossMiniBlocksHashes[hash] {
				processedAll = false
				break
			}
		}

		if processedAll {
			processedMetaHdrs = append(processedMetaHdrs, metaBlock)
		}
	}
	sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	process.SortHeadersByNonce(processedMetaHdrs)

	return processedMetaHdrs, nil
}

func (sp *shardProcessor) updateCrossShardInfo(processedMetaHdrs []data.HeaderHandler) error {
	lastCrossNotarizedHeader, _, err := sp.blockTracker.GetLastCrossNotarizedHeader(core.MetachainShardId)
	if err != nil {
		return err
	}

	// processedMetaHdrs is also sorted
	for i := 0; i < len(processedMetaHdrs); i++ {
		hdr := processedMetaHdrs[i]

		// remove process finished
		if hdr.GetNonce() > lastCrossNotarizedHeader.GetNonce() {
			continue
		}

		// metablock was processed and finalized
		marshalizedHeader, errMarshal := sp.marshalizer.Marshal(hdr)
		if errMarshal != nil {
			log.Debug("updateCrossShardInfo.Marshal", "error", errMarshal.Error())
			continue
		}

		headerHash := sp.hasher.Compute(string(marshalizedHeader))

		sp.saveMetaHeader(hdr, headerHash, marshalizedHeader)

		sp.processedMiniBlocks.RemoveMetaBlockHash(string(headerHash))
	}

	return nil
}

// receivedMetaBlock is a callback function when a new metablock was received
// upon receiving, it parses the new metablock and requests miniblocks and transactions
// which destination is the current shard
func (sp *shardProcessor) receivedMetaBlock(headerHandler data.HeaderHandler, metaBlockHash []byte) {
	metaBlock, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		return
	}

	log.Trace("received meta block from network",
		"round", metaBlock.Round,
		"nonce", metaBlock.Nonce,
		"hash", metaBlockHash,
	)

	sp.hdrsForCurrBlock.mutHdrsForBlock.Lock()

	haveMissingMetaHeaders := sp.hdrsForCurrBlock.missingHdrs > 0 || sp.hdrsForCurrBlock.missingFinalityAttestingHdrs > 0
	if haveMissingMetaHeaders {
		hdrInfoForHash := sp.hdrsForCurrBlock.hdrHashAndInfo[string(metaBlockHash)]
		headerInfoIsNotNil := hdrInfoForHash != nil
		headerIsMissing := headerInfoIsNotNil && check.IfNil(hdrInfoForHash.hdr)
		if headerIsMissing {
			hdrInfoForHash.hdr = metaBlock
			sp.hdrsForCurrBlock.missingHdrs--

			if metaBlock.Nonce > sp.hdrsForCurrBlock.highestHdrNonce[core.MetachainShardId] {
				sp.hdrsForCurrBlock.highestHdrNonce[core.MetachainShardId] = metaBlock.Nonce
			}
		}

		// attesting something
		if sp.hdrsForCurrBlock.missingHdrs == 0 {
			sp.hdrsForCurrBlock.missingFinalityAttestingHdrs = sp.requestMissingFinalityAttestingHeaders(
				core.MetachainShardId,
				sp.metaBlockFinality,
			)
			if sp.hdrsForCurrBlock.missingFinalityAttestingHdrs == 0 {
				log.Debug("received all missing finality attesting meta headers")
			}
		}

		missingMetaHdrs := sp.hdrsForCurrBlock.missingHdrs
		missingFinalityAttestingMetaHdrs := sp.hdrsForCurrBlock.missingFinalityAttestingHdrs
		sp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

		allMissingMetaHeadersReceived := missingMetaHdrs == 0 && missingFinalityAttestingMetaHdrs == 0
		if allMissingMetaHeadersReceived {
			sp.chRcvAllMetaHdrs <- true
		}
	} else {
		sp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()
	}

	go sp.requestMiniBlocksIfNeeded(headerHandler)
}

func (sp *shardProcessor) requestMetaHeaders(shardHeader *block.Header) (uint32, uint32) {
	_ = core.EmptyChannel(sp.chRcvAllMetaHdrs)

	if len(shardHeader.MetaBlockHashes) == 0 {
		return 0, 0
	}

	return sp.computeExistingAndRequestMissingMetaHeaders(shardHeader)
}

func (sp *shardProcessor) computeExistingAndRequestMissingMetaHeaders(header *block.Header) (uint32, uint32) {
	sp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	defer sp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

	for i := 0; i < len(header.MetaBlockHashes); i++ {
		hdr, err := process.GetMetaHeaderFromPool(
			header.MetaBlockHashes[i],
			sp.dataPool.Headers())

		if err != nil {
			sp.hdrsForCurrBlock.missingHdrs++
			sp.hdrsForCurrBlock.hdrHashAndInfo[string(header.MetaBlockHashes[i])] = &hdrInfo{
				hdr:         nil,
				usedInBlock: true,
			}
			go sp.requestHandler.RequestMetaHeader(header.MetaBlockHashes[i])
			continue
		}

		sp.hdrsForCurrBlock.hdrHashAndInfo[string(header.MetaBlockHashes[i])] = &hdrInfo{
			hdr:         hdr,
			usedInBlock: true,
		}

		if hdr.Nonce > sp.hdrsForCurrBlock.highestHdrNonce[core.MetachainShardId] {
			sp.hdrsForCurrBlock.highestHdrNonce[core.MetachainShardId] = hdr.Nonce
		}
	}

	if sp.hdrsForCurrBlock.missingHdrs == 0 {
		sp.hdrsForCurrBlock.missingFinalityAttestingHdrs = sp.requestMissingFinalityAttestingHeaders(
			core.MetachainShardId,
			sp.metaBlockFinality,
		)
	}

	return sp.hdrsForCurrBlock.missingHdrs, sp.hdrsForCurrBlock.missingFinalityAttestingHdrs
}

func (sp *shardProcessor) verifyCrossShardMiniBlockDstMe(header *block.Header) error {
	miniBlockMetaHashes, err := sp.getAllMiniBlockDstMeFromMeta(header)
	if err != nil {
		return err
	}

	crossMiniBlockHashes := header.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
	for hash := range crossMiniBlockHashes {
		if _, ok := miniBlockMetaHashes[hash]; !ok {
			return process.ErrCrossShardMBWithoutConfirmationFromMeta
		}
	}

	return nil
}

func (sp *shardProcessor) getAllMiniBlockDstMeFromMeta(header *block.Header) (map[string][]byte, error) {
	lastCrossNotarizedHeader, _, err := sp.blockTracker.GetLastCrossNotarizedHeader(core.MetachainShardId)
	if err != nil {
		return nil, err
	}

	miniBlockMetaHashes := make(map[string][]byte)

	sp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	for _, metaBlockHash := range header.MetaBlockHashes {
		headerInfo, ok := sp.hdrsForCurrBlock.hdrHashAndInfo[string(metaBlockHash)]
		if !ok {
			continue
		}
		metaBlock, ok := headerInfo.hdr.(*block.MetaBlock)
		if !ok {
			continue
		}
		if metaBlock.GetRound() > header.Round {
			continue
		}
		if metaBlock.GetRound() <= lastCrossNotarizedHeader.GetRound() {
			continue
		}
		if metaBlock.GetNonce() <= lastCrossNotarizedHeader.GetNonce() {
			continue
		}

		crossMiniBlockHashes := metaBlock.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())
		for hash := range crossMiniBlockHashes {
			miniBlockMetaHashes[hash] = metaBlockHash
		}
	}
	sp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	return miniBlockMetaHashes, nil
}

// full verification through metachain header
func (sp *shardProcessor) createAndProcessMiniBlocksDstMe(
	haveTime func() bool,
) (block.MiniBlockSlice, uint32, uint32, error) {
	log.Debug("createAndProcessMiniBlocksDstMe has been started")

	miniBlocks := make(block.MiniBlockSlice, 0)
	txsAdded := uint32(0)
	hdrsAdded := uint32(0)

	sw := core.NewStopWatch()
	sw.Start("ComputeLongestMetaChainFromLastNotarized")
	orderedMetaBlocks, orderedMetaBlocksHashes, err := sp.blockTracker.ComputeLongestMetaChainFromLastNotarized()
	sw.Stop("ComputeLongestMetaChainFromLastNotarized")
	log.Debug("measurements", sw.GetMeasurements()...)
	if err != nil {
		return nil, 0, 0, err
	}

	log.Debug("metablocks ordered",
		"num metablocks", len(orderedMetaBlocks),
	)

	lastMetaHdr, _, err := sp.blockTracker.GetLastCrossNotarizedHeader(core.MetachainShardId)
	if err != nil {
		return nil, 0, 0, err
	}

	// do processing in order
	sp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	for i := 0; i < len(orderedMetaBlocks); i++ {
		if !haveTime() {
			log.Debug("time is up after putting cross txs with destination to current shard",
				"num txs added", txsAdded,
			)
			break
		}

		if hdrsAdded >= process.MaxMetaHeadersAllowedInOneShardBlock {
			log.Debug("maximum meta headers allowed to be included in one shard block has been reached",
				"meta headers added", hdrsAdded,
			)
			break
		}

		currMetaHdr := orderedMetaBlocks[i]
		if currMetaHdr.GetNonce() > lastMetaHdr.GetNonce()+1 {
			log.Debug("skip searching",
				"last meta hdr nonce", lastMetaHdr.GetNonce(),
				"curr meta hdr nonce", currMetaHdr.GetNonce())
			break
		}

		if len(currMetaHdr.GetMiniBlockHeadersWithDst(sp.shardCoordinator.SelfId())) == 0 {
			sp.hdrsForCurrBlock.hdrHashAndInfo[string(orderedMetaBlocksHashes[i])] = &hdrInfo{hdr: currMetaHdr, usedInBlock: true}
			hdrsAdded++
			lastMetaHdr = currMetaHdr
			continue
		}

		processedMiniBlocksHashes := sp.processedMiniBlocks.GetProcessedMiniBlocksHashes(string(orderedMetaBlocksHashes[i]))
		currMBProcessed, currTxsAdded, hdrProcessFinished, errCreated := sp.txCoordinator.CreateMbsAndProcessCrossShardTransactionsDstMe(
			currMetaHdr,
			processedMiniBlocksHashes,
			haveTime)

		if errCreated != nil {
			return nil, 0, 0, errCreated
		}

		// all txs processed, add to processed miniblocks
		miniBlocks = append(miniBlocks, currMBProcessed...)
		txsAdded += currTxsAdded

		if currTxsAdded > 0 {
			sp.hdrsForCurrBlock.hdrHashAndInfo[string(orderedMetaBlocksHashes[i])] = &hdrInfo{hdr: currMetaHdr, usedInBlock: true}
			hdrsAdded++
		}

		if !hdrProcessFinished {
			log.Debug("meta block cannot be fully processed",
				"round", currMetaHdr.GetRound(),
				"nonce", currMetaHdr.GetNonce(),
				"hash", orderedMetaBlocksHashes[i])

			break
		}

		lastMetaHdr = currMetaHdr
	}
	sp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

	go sp.requestMetaHeadersIfNeeded(hdrsAdded, lastMetaHdr)

	for _, miniBlock := range miniBlocks {
		log.Debug("mini block info",
			"type", miniBlock.Type,
			"sender shard", miniBlock.SenderShardID,
			"receiver shard", miniBlock.ReceiverShardID,
			"txs added", len(miniBlock.TxHashes))
	}

	log.Debug("createAndProcessMiniBlocksDstMe has been finished",
		"num txs added", txsAdded,
		"num hdrs added", hdrsAdded)

	return miniBlocks, txsAdded, hdrsAdded, nil
}

func (sp *shardProcessor) requestMetaHeadersIfNeeded(hdrsAdded uint32, lastMetaHdr data.HeaderHandler) {
	log.Debug("meta headers added",
		"num", hdrsAdded,
		"highest nonce", lastMetaHdr.GetNonce(),
	)

	roundTooOld := sp.roundHandler.Index() > int64(lastMetaHdr.GetRound()+process.MaxRoundsWithoutNewBlockReceived)
	shouldRequestCrossHeaders := hdrsAdded == 0 && roundTooOld
	if shouldRequestCrossHeaders {
		fromNonce := lastMetaHdr.GetNonce() + 1
		toNonce := fromNonce + uint64(sp.metaBlockFinality)
		for nonce := fromNonce; nonce <= toNonce; nonce++ {
			sp.addHeaderIntoTrackerPool(nonce, core.MetachainShardId)
			sp.requestHandler.RequestMetaHeaderByNonce(nonce)
		}
	}
}

func (sp *shardProcessor) createMiniBlocks(haveTime func() bool) (*block.Body, error) {
	var miniBlocks block.MiniBlockSlice

	if sp.accountsDB[state.UserAccountsState].JournalLen() != 0 {
		log.Error("shardProcessor.createMiniBlocks", "error", process.ErrAccountStateDirty)
		return &block.Body{MiniBlocks: miniBlocks}, nil
	}

	if !haveTime() {
		log.Debug("shardProcessor.createMiniBlocks", "error", process.ErrTimeIsOut)
		return &block.Body{MiniBlocks: miniBlocks}, nil
	}

	startTime := time.Now()
	mbsToMe, numTxs, numMetaHeaders, err := sp.createAndProcessMiniBlocksDstMe(haveTime)
	elapsedTime := time.Since(startTime)
	log.Debug("elapsed time to create mbs to me",
		"time [s]", elapsedTime,
	)
	if err != nil {
		log.Debug("createAndProcessCrossMiniBlocksDstMe", "error", err.Error())
	}

	if len(mbsToMe) > 0 {
		miniBlocks = append(miniBlocks, mbsToMe...)

		log.Debug("processed miniblocks and txs with destination in self shard",
			"num miniblocks", len(mbsToMe),
			"num txs", numTxs,
			"num meta headers", numMetaHeaders,
		)
	}

	if sp.blockTracker.IsShardStuck(core.MetachainShardId) {
		log.Warn("shardProcessor.createMiniBlocks", "error", process.ErrShardIsStuck, "shard", core.MetachainShardId)

		interMBs := sp.txCoordinator.CreatePostProcessMiniBlocks()
		if len(interMBs) > 0 {
			miniBlocks = append(miniBlocks, interMBs...)
		}

		return &block.Body{MiniBlocks: miniBlocks}, nil
	}

	startTime = time.Now()
	mbsFromMe := sp.txCoordinator.CreateMbsAndProcessTransactionsFromMe(haveTime)
	elapsedTime = time.Since(startTime)
	log.Debug("elapsed time to create mbs from me",
		"time [s]", elapsedTime,
	)

	if len(mbsFromMe) > 0 {
		miniBlocks = append(miniBlocks, mbsFromMe...)

		numTxs = 0
		for _, mb := range mbsFromMe {
			numTxs += uint32(len(mb.TxHashes))
		}

		log.Debug("processed miniblocks and txs from self shard",
			"num miniblocks", len(mbsFromMe),
			"num txs", numTxs,
		)
	}

	log.Debug("creating mini blocks has been finished",
		"num miniblocks", len(miniBlocks),
	)
	return &block.Body{MiniBlocks: miniBlocks}, nil
}

// applyBodyToHeader creates a miniblock header list given a block body
func (sp *shardProcessor) applyBodyToHeader(shardHeader *block.Header, body *block.Body) (*block.Body, error) {
	sw := core.NewStopWatch()
	sw.Start("applyBodyToHeader")
	defer func() {
		sw.Stop("applyBodyToHeader")
		log.Debug("measurements", sw.GetMeasurements()...)
	}()

	shardHeader.MiniBlockHeaders = nil
	shardHeader.RootHash = sp.getRootHash()

	defer func() {
		go sp.checkAndRequestIfMetaHeadersMissing()
	}()

	if check.IfNil(body) {
		return nil, process.ErrNilBlockBody
	}

	var err error
	sw.Start("CreateReceiptsHash")
	shardHeader.ReceiptsHash, err = sp.txCoordinator.CreateReceiptsHash()
	sw.Stop("CreateReceiptsHash")
	if err != nil {
		return nil, err
	}

	newBody := deleteSelfReceiptsMiniBlocks(body)

	sw.Start("createMiniBlockHeaders")
	totalTxCount, miniBlockHeaders, err := sp.createMiniBlockHeaders(newBody)
	sw.Stop("createMiniBlockHeaders")
	if err != nil {
		return nil, err
	}

	shardHeader.MiniBlockHeaders = miniBlockHeaders
	shardHeader.TxCount = uint32(totalTxCount)
	shardHeader.AccumulatedFees = sp.feeHandler.GetAccumulatedFees()
	shardHeader.DeveloperFees = sp.feeHandler.GetDeveloperFees()

	sw.Start("sortHeaderHashesForCurrentBlockByNonce")
	metaBlockHashes := sp.sortHeaderHashesForCurrentBlockByNonce(true)
	sw.Stop("sortHeaderHashesForCurrentBlockByNonce")
	shardHeader.MetaBlockHashes = metaBlockHashes[core.MetachainShardId]

	err = sp.txCoordinator.VerifyCreatedMiniBlocks(shardHeader, newBody)
	if err != nil {
		return nil, err
	}

	sp.appStatusHandler.SetUInt64Value(core.MetricNumTxInBlock, uint64(totalTxCount))
	sp.appStatusHandler.SetUInt64Value(core.MetricNumMiniBlocks, uint64(len(body.MiniBlocks)))

	marshalizedBody, err := sp.marshalizer.Marshal(newBody)
	if err != nil {
		return nil, err
	}
	sp.blockSizeThrottler.Add(shardHeader.GetRound(), uint32(len(marshalizedBody)))

	return newBody, nil
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
	_ data.HeaderHandler,
	bodyHandler data.BodyHandler,
) (map[uint32][]byte, map[string][][]byte, error) {

	if check.IfNil(bodyHandler) {
		return nil, nil, process.ErrNilMiniBlocks
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		return nil, nil, process.ErrWrongTypeAssertion
	}

	mrsTxs := sp.txCoordinator.CreateMarshalizedData(body)

	bodies := make(map[uint32]block.MiniBlockSlice)
	for _, miniBlock := range body.MiniBlocks {
		if miniBlock.SenderShardID != sp.shardCoordinator.SelfId() ||
			miniBlock.ReceiverShardID == sp.shardCoordinator.SelfId() {
			continue
		}
		bodies[miniBlock.ReceiverShardID] = append(bodies[miniBlock.ReceiverShardID], miniBlock)
	}

	mrsData := make(map[uint32][]byte, len(bodies))
	for shardId, subsetBlockBody := range bodies {
		bodyForShard := block.Body{MiniBlocks: subsetBlockBody}
		buff, err := sp.marshalizer.Marshal(&bodyForShard)
		if err != nil {
			log.Error("shardProcessor.MarshalizedDataToBroadcast.Marshal", "error", err.Error())
			continue
		}
		mrsData[shardId] = buff
	}

	return mrsData, mrsTxs, nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (sp *shardProcessor) IsInterfaceNil() bool {
	return sp == nil
}

// GetBlockBodyFromPool returns block body from pool for a given header
func (sp *shardProcessor) GetBlockBodyFromPool(headerHandler data.HeaderHandler) (data.BodyHandler, error) {
	header, ok := headerHandler.(*block.Header)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	miniBlocksPool := sp.dataPool.MiniBlocks()
	var miniBlocks block.MiniBlockSlice

	for _, mbHeader := range header.MiniBlockHeaders {
		obj, hashInPool := miniBlocksPool.Get(mbHeader.Hash)
		if !hashInPool {
			continue
		}

		miniBlock, typeOk := obj.(*block.MiniBlock)
		if !typeOk {
			return nil, process.ErrWrongTypeAssertion
		}

		miniBlocks = append(miniBlocks, miniBlock)
	}

	return &block.Body{MiniBlocks: miniBlocks}, nil
}

func (sp *shardProcessor) getBootstrapHeadersInfo(
	selfNotarizedHeaders []data.HeaderHandler,
	selfNotarizedHeadersHashes [][]byte,
) []bootstrapStorage.BootstrapHeaderInfo {

	numSelfNotarizedHeaders := len(selfNotarizedHeaders)

	highestNonceInSelfNotarizedHeaders := uint64(0)
	if numSelfNotarizedHeaders > 0 {
		highestNonceInSelfNotarizedHeaders = selfNotarizedHeaders[numSelfNotarizedHeaders-1].GetNonce()
	}

	isFinalNonceHigherThanSelfNotarized := sp.forkDetector.GetHighestFinalBlockNonce() > highestNonceInSelfNotarizedHeaders
	if isFinalNonceHigherThanSelfNotarized {
		numSelfNotarizedHeaders++
	}

	if numSelfNotarizedHeaders == 0 {
		return nil
	}

	lastSelfNotarizedHeaders := make([]bootstrapStorage.BootstrapHeaderInfo, 0, numSelfNotarizedHeaders)

	for index := range selfNotarizedHeaders {
		headerInfo := bootstrapStorage.BootstrapHeaderInfo{
			ShardId: selfNotarizedHeaders[index].GetShardID(),
			Nonce:   selfNotarizedHeaders[index].GetNonce(),
			Hash:    selfNotarizedHeadersHashes[index],
		}

		lastSelfNotarizedHeaders = append(lastSelfNotarizedHeaders, headerInfo)
	}

	if isFinalNonceHigherThanSelfNotarized {
		headerInfo := bootstrapStorage.BootstrapHeaderInfo{
			ShardId: sp.shardCoordinator.SelfId(),
			Nonce:   sp.forkDetector.GetHighestFinalBlockNonce(),
			Hash:    sp.forkDetector.GetHighestFinalBlockHash(),
		}

		lastSelfNotarizedHeaders = append(lastSelfNotarizedHeaders, headerInfo)
	}

	return lastSelfNotarizedHeaders
}

func (sp *shardProcessor) removeStartOfEpochBlockDataFromPools(
	_ data.HeaderHandler,
	_ data.BodyHandler,
) error {
	return nil
}

// Close - closes all underlying components
func (sp *shardProcessor) Close() error {
	return sp.baseProcessor.Close()
}
