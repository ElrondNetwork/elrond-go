package block

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
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

var _ process.BlockProcessor = (*metaProcessor)(nil)

// metaProcessor implements metaProcessor interface and actually it tries to execute block
type metaProcessor struct {
	*baseProcessor
	scToProtocol                 process.SmartContractToProtocolHandler
	epochStartDataCreator        process.EpochStartDataCreator
	epochEconomics               process.EndOfEpochEconomics
	epochRewardsCreator          process.RewardsCreator
	validatorInfoCreator         process.EpochStartValidatorInfoCreator
	epochSystemSCProcessor       process.EpochStartSystemSCProcessor
	pendingMiniBlocksHandler     process.PendingMiniBlocksHandler
	validatorStatisticsProcessor process.ValidatorStatisticsProcessor
	shardsHeadersNonce           *sync.Map
	shardBlockFinality           uint32
	chRcvAllHdrs                 chan bool
	headersCounter               *headersCounter
	rewardsV2EnableEpoch         uint32
	userStatePruningQueue        core.Queue
	peerStatePruningQueue        core.Queue
}

// NewMetaProcessor creates a new metaProcessor object
func NewMetaProcessor(arguments ArgMetaProcessor) (*metaProcessor, error) {
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
	if check.IfNil(arguments.SCToProtocol) {
		return nil, process.ErrNilSCToProtocol
	}
	if check.IfNil(arguments.PendingMiniBlocksHandler) {
		return nil, process.ErrNilPendingMiniBlocksHandler
	}
	if check.IfNil(arguments.EpochStartDataCreator) {
		return nil, process.ErrNilEpochStartDataCreator
	}
	if check.IfNil(arguments.EpochEconomics) {
		return nil, process.ErrNilEpochEconomics
	}
	if check.IfNil(arguments.EpochRewardsCreator) {
		return nil, process.ErrNilRewardsCreator
	}
	if check.IfNil(arguments.EpochValidatorInfoCreator) {
		return nil, process.ErrNilEpochStartValidatorInfoCreator
	}
	if check.IfNil(arguments.ValidatorStatisticsProcessor) {
		return nil, process.ErrNilValidatorStatistics
	}
	if check.IfNil(arguments.EpochSystemSCProcessor) {
		return nil, process.ErrNilEpochStartSystemSCProcessor
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
		feeHandler:                    arguments.FeeHandler,
		nodesCoordinator:              arguments.NodesCoordinator,
		uint64Converter:               arguments.CoreComponents.Uint64ByteSliceConverter(),
		requestHandler:                arguments.RequestHandler,
		appStatusHandler:              arguments.CoreComponents.StatusHandler(),
		blockChainHook:                arguments.BlockChainHook,
		txCoordinator:                 arguments.TxCoordinator,
		epochStartTrigger:             arguments.EpochStartTrigger,
		headerValidator:               arguments.HeaderValidator,
		roundHandler:                  arguments.CoreComponents.RoundHandler(),
		bootStorer:                    arguments.BootStorer,
		blockTracker:                  arguments.BlockTracker,
		dataPool:                      arguments.DataComponents.Datapool(),
		blockChain:                    arguments.DataComponents.Blockchain(),
		stateCheckpointModulus:        arguments.Config.StateTriesConfig.CheckpointRoundsModulus,
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

	mp := metaProcessor{
		baseProcessor:                base,
		headersCounter:               NewHeaderCounter(),
		scToProtocol:                 arguments.SCToProtocol,
		pendingMiniBlocksHandler:     arguments.PendingMiniBlocksHandler,
		epochStartDataCreator:        arguments.EpochStartDataCreator,
		epochEconomics:               arguments.EpochEconomics,
		epochRewardsCreator:          arguments.EpochRewardsCreator,
		validatorStatisticsProcessor: arguments.ValidatorStatisticsProcessor,
		validatorInfoCreator:         arguments.EpochValidatorInfoCreator,
		epochSystemSCProcessor:       arguments.EpochSystemSCProcessor,
		rewardsV2EnableEpoch:         arguments.RewardsV2EnableEpoch,
	}

	log.Debug("metablock: enable epoch for staking v2", "epoch", mp.rewardsV2EnableEpoch)

	mp.txCounter = NewTransactionCounter()
	mp.requestBlockBodyHandler = &mp
	mp.blockProcessor = &mp

	mp.hdrsForCurrBlock = newHdrForBlock()

	headersPool := mp.dataPool.Headers()
	headersPool.RegisterHandler(mp.receivedShardHeader)

	mp.chRcvAllHdrs = make(chan bool)

	mp.shardBlockFinality = process.BlockFinality

	mp.shardsHeadersNonce = &sync.Map{}
	mp.userStatePruningQueue = queue.NewSliceQueue(arguments.Config.StateTriesConfig.UserStatePruningQueueSize)
	mp.peerStatePruningQueue = queue.NewSliceQueue(arguments.Config.StateTriesConfig.PeerStatePruningQueueSize)

	return &mp, nil
}

func (mp *metaProcessor) isRewardsV2Enabled(headerHandler data.HeaderHandler) bool {
	return headerHandler.GetEpoch() >= mp.rewardsV2EnableEpoch
}

// ProcessBlock processes a block. It returns nil if all ok or the specific error
func (mp *metaProcessor) ProcessBlock(
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
	haveTime func() time.Duration,
) error {

	if haveTime == nil {
		return process.ErrNilHaveTimeHandler
	}

	err := mp.checkBlockValidity(headerHandler, bodyHandler)
	if err != nil {
		if err == process.ErrBlockHashDoesNotMatch {
			log.Debug("requested missing meta header",
				"hash", headerHandler.GetPrevHash(),
				"for shard", headerHandler.GetShardID(),
			)

			go mp.requestHandler.RequestMetaHeader(headerHandler.GetPrevHash())
		}

		return err
	}

	mp.epochNotifier.CheckEpoch(headerHandler)
	mp.requestHandler.SetEpoch(headerHandler.GetEpoch())

	log.Debug("started processing block",
		"epoch", headerHandler.GetEpoch(),
		"round", headerHandler.GetRound(),
		"nonce", headerHandler.GetNonce())

	header, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	err = mp.checkHeaderBodyCorrelation(header.MiniBlockHeaders, body)
	if err != nil {
		return err
	}

	headersPool := mp.dataPool.Headers()
	numShardHeadersFromPool := 0
	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		numShardHeadersFromPool += headersPool.GetNumHeaders(shardID)
	}

	txCounts, rewardCounts, unsignedCounts := mp.txCounter.getPoolCounts(mp.dataPool)
	log.Debug("total txs in pool", "counts", txCounts.String())
	log.Debug("total txs in rewards pool", "counts", rewardCounts.String())
	log.Debug("total txs in unsigned pool", "counts", unsignedCounts.String())

	go getMetricsFromMetaHeader(
		header,
		mp.marshalizer,
		mp.appStatusHandler,
		numShardHeadersFromPool,
		mp.headersCounter.getNumShardMBHeadersTotalProcessed(),
	)

	defer func() {
		if err != nil {
			mp.RevertAccountState(header)
		}
	}()

	mp.createBlockStarted()
	mp.blockChainHook.SetCurrentHeader(headerHandler)
	mp.epochStartTrigger.Update(header.GetRound(), header.GetNonce())

	err = mp.checkEpochCorrectness(header)
	if err != nil {
		return err
	}

	if mp.accountsDB[state.UserAccountsState].JournalLen() != 0 {
		return process.ErrAccountStateDirty
	}

	err = mp.processIfFirstBlockAfterEpochStart()
	if err != nil {
		return err
	}

	if header.IsStartOfEpochBlock() {
		err = mp.processEpochStartMetaBlock(header, body)
		return err
	}

	mp.txCoordinator.RequestBlockTransactions(body)
	requestedShardHdrs, requestedFinalityAttestingShardHdrs := mp.requestShardHeaders(header)

	if haveTime() < 0 {
		return process.ErrTimeIsOut
	}

	err = mp.txCoordinator.IsDataPreparedForProcessing(haveTime)
	if err != nil {
		return err
	}

	haveMissingShardHeaders := requestedShardHdrs > 0 || requestedFinalityAttestingShardHdrs > 0
	if haveMissingShardHeaders {
		if requestedShardHdrs > 0 {
			log.Debug("requested missing shard headers",
				"num headers", requestedShardHdrs,
			)
		}
		if requestedFinalityAttestingShardHdrs > 0 {
			log.Debug("requested missing finality attesting shard headers",
				"num finality shard headers", requestedFinalityAttestingShardHdrs,
			)
		}

		err = mp.waitForBlockHeaders(haveTime())

		mp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
		missingShardHdrs := mp.hdrsForCurrBlock.missingHdrs
		mp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

		mp.hdrsForCurrBlock.resetMissingHdrs()

		if requestedShardHdrs > 0 {
			log.Debug("received missing shard headers",
				"num headers", requestedShardHdrs-missingShardHdrs,
			)
		}

		if err != nil {
			return err
		}
	}

	defer func() {
		go mp.checkAndRequestIfShardHeadersMissing()
	}()

	highestNonceHdrs, err := mp.checkShardHeadersValidity(header)
	if err != nil {
		return err
	}

	err = mp.checkShardHeadersFinality(highestNonceHdrs)
	if err != nil {
		return err
	}

	err = mp.verifyCrossShardMiniBlockDstMe(header)
	if err != nil {
		return err
	}

	err = mp.verifyTotalAccumulatedFeesInEpoch(header)
	if err != nil {
		return err
	}

	err = mp.txCoordinator.ProcessBlockTransaction(body, haveTime)
	if err != nil {
		return err
	}

	err = mp.txCoordinator.VerifyCreatedBlockTransactions(header, body)
	if err != nil {
		return err
	}

	err = mp.scToProtocol.UpdateProtocol(body, header.Nonce)
	if err != nil {
		return err
	}

	err = mp.verifyFees(header)
	if err != nil {
		return err
	}

	if !mp.verifyStateRoot(header.GetRootHash()) {
		err = process.ErrRootStateDoesNotMatch
		return err
	}

	err = mp.verifyValidatorStatisticsRootHash(header)
	if err != nil {
		return err
	}

	return nil
}

func (mp *metaProcessor) processEpochStartMetaBlock(
	header *block.MetaBlock,
	body *block.Body,
) error {
	err := mp.epochStartDataCreator.VerifyEpochStartDataForMetablock(header)
	if err != nil {
		return err
	}

	currentRootHash, err := mp.validatorStatisticsProcessor.RootHash()
	if err != nil {
		return err
	}

	allValidatorsInfo, err := mp.validatorStatisticsProcessor.GetValidatorInfoForRootHash(currentRootHash)
	if err != nil {
		return err
	}

	err = mp.validatorStatisticsProcessor.ProcessRatingsEndOfEpoch(allValidatorsInfo, header.Epoch)
	if err != nil {
		return err
	}

	computedEconomics, err := mp.epochEconomics.ComputeEndOfEpochEconomics(header)
	if err != nil {
		return err
	}

	if mp.isRewardsV2Enabled(header) {
		err = mp.epochSystemSCProcessor.ProcessSystemSmartContract(allValidatorsInfo, header.Nonce, header.Epoch)
		if err != nil {
			return err
		}

		err = mp.epochRewardsCreator.VerifyRewardsMiniBlocks(header, allValidatorsInfo, computedEconomics)
		if err != nil {
			return err
		}
	} else {
		err = mp.epochRewardsCreator.VerifyRewardsMiniBlocks(header, allValidatorsInfo, computedEconomics)
		if err != nil {
			return err
		}

		err = mp.epochSystemSCProcessor.ProcessSystemSmartContract(allValidatorsInfo, header.Nonce, header.Epoch)
		if err != nil {
			return err
		}
	}

	err = mp.epochSystemSCProcessor.ProcessDelegationRewards(body.MiniBlocks, mp.epochRewardsCreator.GetLocalTxCache())
	if err != nil {
		return err
	}

	err = mp.validatorInfoCreator.VerifyValidatorInfoMiniBlocks(body.MiniBlocks, allValidatorsInfo)
	if err != nil {
		return err
	}

	err = mp.validatorStatisticsProcessor.ResetValidatorStatisticsAtNewEpoch(allValidatorsInfo)
	if err != nil {
		return err
	}

	err = mp.epochEconomics.VerifyRewardsPerBlock(header, mp.epochRewardsCreator.GetProtocolSustainabilityRewards(), computedEconomics)
	if err != nil {
		return err
	}

	err = mp.verifyFees(header)
	if err != nil {
		return err
	}

	if !mp.verifyStateRoot(header.GetRootHash()) {
		err = process.ErrRootStateDoesNotMatch
		return err
	}

	err = mp.verifyValidatorStatisticsRootHash(header)
	if err != nil {
		return err
	}

	saveEpochStartEconomicsMetrics(mp.appStatusHandler, header)

	return nil
}

// SetNumProcessedObj will set the num of processed headers
func (mp *metaProcessor) SetNumProcessedObj(numObj uint64) {
	mp.headersCounter.shardMBHeadersTotalProcessed = numObj
}

func (mp *metaProcessor) checkEpochCorrectness(
	headerHandler data.HeaderHandler,
) error {
	currentBlockHeader := mp.blockChain.GetCurrentBlockHeader()
	if check.IfNil(currentBlockHeader) {
		return nil
	}

	isEpochIncorrect := headerHandler.GetEpoch() != currentBlockHeader.GetEpoch() &&
		mp.epochStartTrigger.Epoch() == currentBlockHeader.GetEpoch()
	if isEpochIncorrect {
		log.Warn("epoch does not match", "currentHeaderEpoch", currentBlockHeader.GetEpoch(), "receivedHeaderEpoch", headerHandler.GetEpoch(), "epochStartTrigger", mp.epochStartTrigger.Epoch())
		return process.ErrEpochDoesNotMatch
	}

	isEpochIncorrect = mp.epochStartTrigger.IsEpochStart() &&
		mp.epochStartTrigger.EpochStartRound() <= headerHandler.GetRound() &&
		headerHandler.GetEpoch() != currentBlockHeader.GetEpoch()+1
	if isEpochIncorrect {
		log.Warn("is epoch start and epoch does not match", "currentHeaderEpoch", currentBlockHeader.GetEpoch(), "receivedHeaderEpoch", headerHandler.GetEpoch(), "epochStartTrigger", mp.epochStartTrigger.Epoch())
		return process.ErrEpochDoesNotMatch
	}

	return nil
}

func (mp *metaProcessor) verifyCrossShardMiniBlockDstMe(header *block.MetaBlock) error {
	miniBlockShardsHashes, err := mp.getAllMiniBlockDstMeFromShards(header)
	if err != nil {
		return err
	}

	// if all miniblockshards hashes are in header miniblocks as well
	mapMetaMiniBlockHdrs := make(map[string]struct{}, len(header.MiniBlockHeaders))
	for _, metaMiniBlock := range header.MiniBlockHeaders {
		mapMetaMiniBlockHdrs[string(metaMiniBlock.Hash)] = struct{}{}
	}

	for hash := range miniBlockShardsHashes {
		if _, ok := mapMetaMiniBlockHdrs[hash]; !ok {
			return process.ErrCrossShardMBWithoutConfirmationFromMeta
		}
	}

	return nil
}

func (mp *metaProcessor) getAllMiniBlockDstMeFromShards(metaHdr *block.MetaBlock) (map[string][]byte, error) {
	miniBlockShardsHashes := make(map[string][]byte)

	mp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	defer mp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	for _, shardInfo := range metaHdr.ShardInfo {
		headerInfo, ok := mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardInfo.HeaderHash)]
		if !ok {
			continue
		}
		shardHeader, ok := headerInfo.hdr.(*block.Header)
		if !ok {
			continue
		}

		lastCrossNotarizedHeader, _, err := mp.blockTracker.GetLastCrossNotarizedHeader(shardInfo.ShardID)
		if err != nil {
			return nil, err
		}

		if shardHeader.GetRound() > metaHdr.Round {
			continue
		}
		if shardHeader.GetRound() <= lastCrossNotarizedHeader.GetRound() {
			continue
		}
		if shardHeader.GetNonce() <= lastCrossNotarizedHeader.GetNonce() {
			continue
		}

		crossMiniBlockHashes := shardHeader.GetMiniBlockHeadersWithDst(mp.shardCoordinator.SelfId())
		for hash := range crossMiniBlockHashes {
			miniBlockShardsHashes[hash] = shardInfo.HeaderHash
		}
	}

	return miniBlockShardsHashes, nil
}

func (mp *metaProcessor) checkAndRequestIfShardHeadersMissing() {
	orderedHdrsPerShard := mp.blockTracker.GetTrackedHeadersForAllShards()

	for i := uint32(0); i < mp.shardCoordinator.NumberOfShards(); i++ {
		err := mp.requestHeadersIfMissing(orderedHdrsPerShard[i], i)
		if err != nil {
			log.Debug("checkAndRequestIfShardHeadersMissing", "error", err.Error())
			continue
		}
	}
}

func (mp *metaProcessor) indexBlock(
	metaBlock data.HeaderHandler,
	headerHash []byte,
	body data.BodyHandler,
	lastMetaBlock data.HeaderHandler,
	notarizedHeadersHashes []string,
	rewardsTxs map[string]data.TransactionHandler,
) {
	if !mp.outportHandler.HasDrivers() {
		return
	}

	log.Debug("preparing to index block", "hash", headerHash, "nonce", metaBlock.GetNonce(), "round", metaBlock.GetRound())

	mp.outportHandler.UpdateTPS(mp.tpsBenchmark)

	pool := &indexer.Pool{
		Txs:     mp.txCoordinator.GetAllCurrentUsedTxs(block.TxBlock),
		Scrs:    mp.txCoordinator.GetAllCurrentUsedTxs(block.SmartContractResultBlock),
		Rewards: rewardsTxs,
		Logs:    mp.txCoordinator.GetAllCurrentLogs(),
	}

	publicKeys, err := mp.nodesCoordinator.GetConsensusValidatorsPublicKeys(
		metaBlock.GetPrevRandSeed(), metaBlock.GetRound(), core.MetachainShardId, metaBlock.GetEpoch(),
	)
	if err != nil {
		log.Debug("indexBlock: GetConsensusValidatorsPublicKeys",
			"hash", headerHash,
			"epoch", metaBlock.GetEpoch(),
			"error", err.Error())
		return
	}

	epoch := metaBlock.GetEpoch()
	shardCoordinatorShardID := mp.shardCoordinator.SelfId()
	nodesCoordinatorShardID, err := mp.nodesCoordinator.ShardIdForEpoch(epoch)
	if err != nil {
		log.Debug("indexBlock",
			"epoch", epoch,
			"error", err.Error())
		return
	}

	if shardCoordinatorShardID != nodesCoordinatorShardID {
		log.Debug("indexBlock",
			"epoch", epoch,
			"shardCoordinator.ShardID", shardCoordinatorShardID,
			"nodesCoordinator.ShardID", nodesCoordinatorShardID)
		return
	}

	signersIndexes, err := mp.nodesCoordinator.GetValidatorsIndexes(publicKeys, epoch)
	if err != nil {
		log.Debug("indexBlock: GetValidatorsIndexes",
			"hash", headerHash,
			"epoch", metaBlock.GetEpoch(),
			"error", err.Error())
		return
	}

	args := &indexer.ArgsSaveBlockData{
		HeaderHash:             headerHash,
		Body:                   body,
		Header:                 metaBlock,
		SignersIndexes:         signersIndexes,
		NotarizedHeadersHashes: notarizedHeadersHashes,
		TransactionsPool:       pool,
	}
	mp.outportHandler.SaveBlock(args)
	log.Debug("indexed block", "hash", headerHash, "nonce", metaBlock.GetNonce(), "round", metaBlock.GetRound())

	indexRoundInfo(mp.outportHandler, mp.nodesCoordinator, core.MetachainShardId, metaBlock, lastMetaBlock, signersIndexes)

	if metaBlock.GetNonce() != 1 && !metaBlock.IsStartOfEpochBlock() {
		return
	}

	indexValidatorsRating(mp.outportHandler, mp.validatorStatisticsProcessor, metaBlock)
}

// RestoreBlockIntoPools restores the block into associated pools
func (mp *metaProcessor) RestoreBlockIntoPools(headerHandler data.HeaderHandler, bodyHandler data.BodyHandler) error {
	if check.IfNil(headerHandler) {
		return process.ErrNilMetaBlockHeader
	}

	metaBlock, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	hdrHashes := make([][]byte, len(metaBlock.ShardInfo))
	for i := 0; i < len(metaBlock.ShardInfo); i++ {
		hdrHashes[i] = metaBlock.ShardInfo[i].HeaderHash
	}

	err := mp.pendingMiniBlocksHandler.RevertHeader(metaBlock)
	if err != nil {
		return err
	}

	headersPool := mp.dataPool.Headers()

	for _, hdrHash := range hdrHashes {
		shardHeader, errNotCritical := process.GetShardHeaderFromStorage(hdrHash, mp.marshalizer, mp.store)
		if errNotCritical != nil {
			log.Debug("shard header not found in BlockHeaderUnit",
				"hash", hdrHash,
			)
			continue
		}

		headersPool.AddHeader(hdrHash, shardHeader)

		hdrNonceHashDataUnit := dataRetriever.ShardHdrNonceHashDataUnit + dataRetriever.UnitType(shardHeader.GetShardID())
		storer := mp.store.GetStorer(hdrNonceHashDataUnit)
		nonceToByteSlice := mp.uint64Converter.ToByteSlice(shardHeader.GetNonce())
		errNotCritical = storer.Remove(nonceToByteSlice)
		if errNotCritical != nil {
			log.Debug("ShardHdrNonceHashDataUnit.Remove", "error", errNotCritical.Error())
		}

		mp.headersCounter.subtractRestoredMBHeaders(len(shardHeader.MiniBlockHeaders))
	}

	mp.restoreBlockBody(bodyHandler)

	mp.blockTracker.RemoveLastNotarizedHeaders()

	return nil
}

// CreateBlock creates the final block and header for the current round
func (mp *metaProcessor) CreateBlock(
	initialHdr data.HeaderHandler,
	haveTime func() bool,
) (data.HeaderHandler, data.BodyHandler, error) {
	if check.IfNil(initialHdr) {
		return nil, nil, process.ErrNilBlockHeader
	}

	metaHdr, ok := initialHdr.(*block.MetaBlock)
	if !ok {
		return nil, nil, process.ErrWrongTypeAssertion
	}

	mp.epochStartTrigger.Update(initialHdr.GetRound(), initialHdr.GetNonce())
	metaHdr.SetEpoch(mp.epochStartTrigger.Epoch())
	metaHdr.SoftwareVersion = []byte(mp.headerIntegrityVerifier.GetVersion(metaHdr.Epoch))
	mp.epochNotifier.CheckEpoch(metaHdr)
	mp.blockChainHook.SetCurrentHeader(initialHdr)

	var body data.BodyHandler

	if mp.accountsDB[state.UserAccountsState].JournalLen() != 0 {
		return nil, nil, process.ErrAccountStateDirty
	}

	err := mp.processIfFirstBlockAfterEpochStart()
	if err != nil {
		return nil, nil, err
	}

	if mp.epochStartTrigger.IsEpochStart() {
		err = mp.updateEpochStartHeader(metaHdr)
		if err != nil {
			return nil, nil, err
		}

		body, err = mp.createEpochStartBody(metaHdr)
		if err != nil {
			return nil, nil, err
		}
	} else {
		body, err = mp.createBlockBody(metaHdr, haveTime)
		if err != nil {
			return nil, nil, err
		}
	}

	body, err = mp.applyBodyToHeader(metaHdr, body)
	if err != nil {
		return nil, nil, err
	}

	mp.requestHandler.SetEpoch(metaHdr.GetEpoch())

	return metaHdr, body, nil
}

func (mp *metaProcessor) isPreviousBlockEpochStart() (uint32, bool) {
	blockHeader := mp.blockChain.GetCurrentBlockHeader()
	if check.IfNil(blockHeader) {
		blockHeader = mp.blockChain.GetGenesisHeader()
	}

	return blockHeader.GetEpoch(), blockHeader.IsStartOfEpochBlock()
}

func (mp *metaProcessor) processIfFirstBlockAfterEpochStart() error {
	epoch, isPreviousEpochStart := mp.isPreviousBlockEpochStart()
	if !isPreviousEpochStart {
		return nil
	}

	nodesForcedToStay, err := mp.validatorStatisticsProcessor.SaveNodesCoordinatorUpdates(epoch)
	if err != nil {
		return err
	}

	err = mp.epochSystemSCProcessor.ToggleUnStakeUnBond(nodesForcedToStay)
	if err != nil {
		return err
	}

	return nil
}

func (mp *metaProcessor) updateEpochStartHeader(metaHdr *block.MetaBlock) error {
	sw := core.NewStopWatch()
	sw.Start("createEpochStartForMetablock")
	defer func() {
		sw.Stop("createEpochStartForMetablock")
		log.Debug("epochStartHeaderDataCreation", sw.GetMeasurements()...)
	}()

	epochStart, err := mp.epochStartDataCreator.CreateEpochStartData()
	if err != nil {
		return err
	}

	metaHdr.EpochStart = *epochStart

	totalAccumulatedFeesInEpoch := big.NewInt(0)
	totalDevFeesInEpoch := big.NewInt(0)
	currentHeader := mp.blockChain.GetCurrentBlockHeader()
	if !check.IfNil(currentHeader) && !currentHeader.IsStartOfEpochBlock() {
		prevMetaHdr, ok := currentHeader.(*block.MetaBlock)
		if !ok {
			return process.ErrWrongTypeAssertion
		}
		totalAccumulatedFeesInEpoch = big.NewInt(0).Set(prevMetaHdr.AccumulatedFeesInEpoch)
		totalDevFeesInEpoch = big.NewInt(0).Set(prevMetaHdr.DevFeesInEpoch)
	}

	metaHdr.AccumulatedFeesInEpoch = totalAccumulatedFeesInEpoch
	metaHdr.DevFeesInEpoch = totalDevFeesInEpoch
	economicsData, err := mp.epochEconomics.ComputeEndOfEpochEconomics(metaHdr)
	if err != nil {
		return err
	}

	metaHdr.EpochStart.Economics = *economicsData

	saveEpochStartEconomicsMetrics(mp.appStatusHandler, metaHdr)

	return nil
}

func (mp *metaProcessor) createEpochStartBody(metaBlock *block.MetaBlock) (data.BodyHandler, error) {
	mp.createBlockStarted()

	log.Debug("started creating epoch start block body",
		"epoch", metaBlock.GetEpoch(),
		"round", metaBlock.GetRound(),
		"nonce", metaBlock.GetNonce(),
	)

	currentRootHash, err := mp.validatorStatisticsProcessor.RootHash()
	if err != nil {
		return nil, err
	}

	allValidatorsInfo, err := mp.validatorStatisticsProcessor.GetValidatorInfoForRootHash(currentRootHash)
	if err != nil {
		return nil, err
	}

	err = mp.validatorStatisticsProcessor.ProcessRatingsEndOfEpoch(allValidatorsInfo, metaBlock.Epoch)
	if err != nil {
		return nil, err
	}

	var rewardMiniBlocks block.MiniBlockSlice
	if mp.isRewardsV2Enabled(metaBlock) {
		err = mp.epochSystemSCProcessor.ProcessSystemSmartContract(allValidatorsInfo, metaBlock.Nonce, metaBlock.Epoch)
		if err != nil {
			return nil, err
		}

		rewardMiniBlocks, err = mp.epochRewardsCreator.CreateRewardsMiniBlocks(metaBlock, allValidatorsInfo, &metaBlock.EpochStart.Economics)
		if err != nil {
			return nil, err
		}
	} else {
		rewardMiniBlocks, err = mp.epochRewardsCreator.CreateRewardsMiniBlocks(metaBlock, allValidatorsInfo, &metaBlock.EpochStart.Economics)
		if err != nil {
			return nil, err
		}

		err = mp.epochSystemSCProcessor.ProcessSystemSmartContract(allValidatorsInfo, metaBlock.Nonce, metaBlock.Epoch)
		if err != nil {
			return nil, err
		}
	}

	metaBlock.EpochStart.Economics.RewardsForProtocolSustainability.Set(mp.epochRewardsCreator.GetProtocolSustainabilityRewards())

	err = mp.epochSystemSCProcessor.ProcessDelegationRewards(rewardMiniBlocks, mp.epochRewardsCreator.GetLocalTxCache())
	if err != nil {
		return nil, err
	}

	validatorMiniBlocks, err := mp.validatorInfoCreator.CreateValidatorInfoMiniBlocks(allValidatorsInfo)
	if err != nil {
		return nil, err
	}

	err = mp.validatorStatisticsProcessor.ResetValidatorStatisticsAtNewEpoch(allValidatorsInfo)
	if err != nil {
		return nil, err
	}

	finalMiniBlocks := make([]*block.MiniBlock, 0)
	finalMiniBlocks = append(finalMiniBlocks, rewardMiniBlocks...)
	finalMiniBlocks = append(finalMiniBlocks, validatorMiniBlocks...)

	return &block.Body{MiniBlocks: finalMiniBlocks}, nil
}

// createBlockBody creates block body of metachain
func (mp *metaProcessor) createBlockBody(metaBlock *block.MetaBlock, haveTime func() bool) (data.BodyHandler, error) {
	mp.createBlockStarted()
	mp.blockSizeThrottler.ComputeCurrentMaxSize()

	log.Debug("started creating meta block body",
		"epoch", metaBlock.GetEpoch(),
		"round", metaBlock.GetRound(),
		"nonce", metaBlock.GetNonce(),
	)

	miniBlocks, err := mp.createMiniBlocks(haveTime)
	if err != nil {
		return nil, err
	}

	err = mp.scToProtocol.UpdateProtocol(miniBlocks, metaBlock.Nonce)
	if err != nil {
		return nil, err
	}

	return miniBlocks, nil
}

func (mp *metaProcessor) createMiniBlocks(
	haveTime func() bool,
) (*block.Body, error) {
	var miniBlocks block.MiniBlockSlice

	if !haveTime() {
		log.Debug("metaProcessor.createMiniBlocks", "error", process.ErrTimeIsOut)
		return &block.Body{MiniBlocks: miniBlocks}, nil
	}

	mbsToMe, numTxs, numShardHeaders, err := mp.createAndProcessCrossMiniBlocksDstMe(haveTime)
	if err != nil {
		log.Debug("createAndProcessCrossMiniBlocksDstMe", "error", err.Error())
	}

	if len(mbsToMe) > 0 {
		miniBlocks = append(miniBlocks, mbsToMe...)

		log.Debug("processed miniblocks and txs with destination in self shard",
			"num miniblocks", len(mbsToMe),
			"num txs", numTxs,
			"num shard headers", numShardHeaders,
		)
	}

	mbsFromMe := mp.txCoordinator.CreateMbsAndProcessTransactionsFromMe(haveTime)
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
		"miniblocks created", len(miniBlocks),
	)

	return &block.Body{MiniBlocks: miniBlocks}, nil
}

func (mp *metaProcessor) isGenesisShardBlockAndFirstMeta(shardHdrNonce uint64) bool {
	return shardHdrNonce == mp.genesisNonce && check.IfNil(mp.blockChain.GetCurrentBlockHeader())
}

// full verification through metachain header
func (mp *metaProcessor) createAndProcessCrossMiniBlocksDstMe(
	haveTime func() bool,
) (block.MiniBlockSlice, uint32, uint32, error) {

	var miniBlocks block.MiniBlockSlice
	txsAdded := uint32(0)
	hdrsAdded := uint32(0)

	sw := core.NewStopWatch()
	sw.Start("ComputeLongestShardsChainsFromLastNotarized")
	orderedHdrs, orderedHdrsHashes, _, err := mp.blockTracker.ComputeLongestShardsChainsFromLastNotarized()
	sw.Stop("ComputeLongestShardsChainsFromLastNotarized")
	log.Debug("measurements ComputeLongestShardsChainsFromLastNotarized", sw.GetMeasurements()...)
	if err != nil {
		return nil, 0, 0, err
	}

	log.Debug("shard headers ordered",
		"num shard headers", len(orderedHdrs),
	)

	lastShardHdr, err := mp.getLastCrossNotarizedShardHdrs()
	if err != nil {
		return nil, 0, 0, err
	}

	maxShardHeadersFromSameShard := core.MaxUint32(
		process.MinShardHeadersFromSameShardInOneMetaBlock,
		process.MaxShardHeadersAllowedInOneMetaBlock/mp.shardCoordinator.NumberOfShards(),
	)
	maxShardHeadersAllowedInOneMetaBlock := maxShardHeadersFromSameShard * mp.shardCoordinator.NumberOfShards()
	hdrsAddedForShard := make(map[uint32]uint32)

	mp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	for i := 0; i < len(orderedHdrs); i++ {
		if !haveTime() {
			log.Debug("time is up after putting cross txs with destination to current shard",
				"num txs", txsAdded,
			)
			break
		}

		if hdrsAdded >= maxShardHeadersAllowedInOneMetaBlock {
			log.Debug("maximum shard headers allowed to be included in one meta block has been reached",
				"shard headers added", hdrsAdded,
			)
			break
		}

		currShardHdr := orderedHdrs[i]
		if currShardHdr.GetNonce() > lastShardHdr[currShardHdr.GetShardID()].GetNonce()+1 {
			log.Trace("skip searching",
				"shard", currShardHdr.GetShardID(),
				"last shard hdr nonce", lastShardHdr[currShardHdr.GetShardID()].GetNonce(),
				"curr shard hdr nonce", currShardHdr.GetNonce())
			continue
		}

		if hdrsAddedForShard[currShardHdr.GetShardID()] >= maxShardHeadersFromSameShard {
			log.Trace("maximum shard headers from same shard allowed to be included in one meta block has been reached",
				"shard", currShardHdr.GetShardID(),
				"shard headers added", hdrsAddedForShard[currShardHdr.GetShardID()],
			)
			continue
		}

		if len(currShardHdr.GetMiniBlockHeadersWithDst(mp.shardCoordinator.SelfId())) == 0 {
			mp.hdrsForCurrBlock.hdrHashAndInfo[string(orderedHdrsHashes[i])] = &hdrInfo{hdr: currShardHdr, usedInBlock: true}
			hdrsAdded++
			hdrsAddedForShard[currShardHdr.GetShardID()]++
			lastShardHdr[currShardHdr.GetShardID()] = currShardHdr
			continue
		}

		snapshot := mp.accountsDB[state.UserAccountsState].JournalLen()
		currMBProcessed, currTxsAdded, hdrProcessFinished, createErr := mp.txCoordinator.CreateMbsAndProcessCrossShardTransactionsDstMe(
			currShardHdr,
			nil,
			haveTime)

		if createErr != nil {
			return nil, 0, 0, createErr
		}

		if !hdrProcessFinished {
			log.Debug("shard header cannot be fully processed",
				"round", currShardHdr.GetRound(),
				"nonce", currShardHdr.GetNonce(),
				"hash", orderedHdrsHashes[i])

			// shard header must be processed completely
			errAccountState := mp.accountsDB[state.UserAccountsState].RevertToSnapshot(snapshot)
			if errAccountState != nil {
				// TODO: evaluate if reloading the trie from disk will might solve the problem
				log.Warn("accounts.RevertToSnapshot", "error", errAccountState.Error())
			}
			continue
		}

		// all txs processed, add to processed miniblocks
		miniBlocks = append(miniBlocks, currMBProcessed...)
		txsAdded += currTxsAdded

		mp.hdrsForCurrBlock.hdrHashAndInfo[string(orderedHdrsHashes[i])] = &hdrInfo{hdr: currShardHdr, usedInBlock: true}
		hdrsAdded++
		hdrsAddedForShard[currShardHdr.GetShardID()]++

		lastShardHdr[currShardHdr.GetShardID()] = currShardHdr
	}
	mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

	go mp.requestShardHeadersIfNeeded(hdrsAddedForShard, lastShardHdr)

	return miniBlocks, txsAdded, hdrsAdded, nil
}

func (mp *metaProcessor) requestShardHeadersIfNeeded(
	hdrsAddedForShard map[uint32]uint32,
	lastShardHdr map[uint32]data.HeaderHandler,
) {
	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		log.Debug("shard headers added",
			"shard", shardID,
			"num", hdrsAddedForShard[shardID],
			"highest nonce", lastShardHdr[shardID].GetNonce())

		roundTooOld := mp.roundHandler.Index() > int64(lastShardHdr[shardID].GetRound()+process.MaxRoundsWithoutNewBlockReceived)
		shouldRequestCrossHeaders := hdrsAddedForShard[shardID] == 0 && roundTooOld
		if shouldRequestCrossHeaders {
			fromNonce := lastShardHdr[shardID].GetNonce() + 1
			toNonce := fromNonce + uint64(mp.shardBlockFinality)
			for nonce := fromNonce; nonce <= toNonce; nonce++ {
				mp.addHeaderIntoTrackerPool(nonce, shardID)
				mp.requestHandler.RequestShardHeaderByNonce(shardID, nonce)
			}
		}
	}
}

// CommitBlock commits the block in the blockchain if everything was checked successfully
func (mp *metaProcessor) CommitBlock(
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
) error {
	var err error
	defer func() {
		if err != nil {
			mp.RevertAccountState(headerHandler)
		}
	}()

	err = checkForNils(headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	log.Debug("started committing block",
		"epoch", headerHandler.GetEpoch(),
		"round", headerHandler.GetRound(),
		"nonce", headerHandler.GetNonce(),
	)

	err = mp.checkBlockValidity(headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	mp.store.SetEpochForPutOperation(headerHandler.GetEpoch())

	header, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	marshalizedHeader, err := mp.marshalizer.Marshal(header)
	if err != nil {
		return err
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return err
	}

	// must be called before commitEpochStart
	rewardsTxs := mp.getRewardsTxs(header, body)

	mp.commitEpochStart(header, body)
	headerHash := mp.hasher.Compute(string(marshalizedHeader))
	mp.saveMetaHeader(header, headerHash, marshalizedHeader)
	mp.saveBody(body, header)

	err = mp.commitAll()
	if err != nil {
		return err
	}

	mp.validatorStatisticsProcessor.DisplayRatings(header.GetEpoch())

	err = mp.saveLastNotarizedHeader(header)
	if err != nil {
		return err
	}

	err = mp.pendingMiniBlocksHandler.AddProcessedHeader(header)
	if err != nil {
		return err
	}

	log.Info("meta block has been committed successfully",
		"epoch", header.Epoch,
		"round", header.Round,
		"nonce", header.Nonce,
		"hash", headerHash)

	notarizedHeadersHashes, errNotCritical := mp.updateCrossShardInfo(header)
	if errNotCritical != nil {
		log.Debug("updateCrossShardInfo", "error", errNotCritical.Error())
	}

	errNotCritical = mp.forkDetector.AddHeader(header, headerHash, process.BHProcessed, nil, nil)
	if errNotCritical != nil {
		log.Debug("forkDetector.AddHeader", "error", errNotCritical.Error())
	}

	currentHeader, currentHeaderHash := getLastSelfNotarizedHeaderByItself(mp.blockChain)
	mp.blockTracker.AddSelfNotarizedHeader(mp.shardCoordinator.SelfId(), currentHeader, currentHeaderHash)

	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		lastSelfNotarizedHeader, lastSelfNotarizedHeaderHash := mp.getLastSelfNotarizedHeaderByShard(header, shardID)
		mp.blockTracker.AddSelfNotarizedHeader(shardID, lastSelfNotarizedHeader, lastSelfNotarizedHeaderHash)
	}

	go mp.historyRepo.OnNotarizedBlocks(mp.shardCoordinator.SelfId(), []data.HeaderHandler{currentHeader}, [][]byte{currentHeaderHash})

	log.Debug("highest final meta block",
		"nonce", mp.forkDetector.GetHighestFinalBlockNonce(),
	)

	lastMetaBlock := mp.blockChain.GetCurrentBlockHeader()
	mp.updateState(lastMetaBlock)

	err = mp.blockChain.SetCurrentBlockHeader(header)
	if err != nil {
		return err
	}

	mp.blockChain.SetCurrentBlockHeaderHash(headerHash)

	if !check.IfNil(lastMetaBlock) && lastMetaBlock.IsStartOfEpochBlock() {
		mp.blockTracker.CleanupInvalidCrossHeaders(header.Epoch, header.Round)
	}

	mp.tpsBenchmark.Update(lastMetaBlock)

	mp.indexBlock(header, headerHash, body, lastMetaBlock, notarizedHeadersHashes, rewardsTxs)
	mp.recordBlockInHistory(headerHash, headerHandler, bodyHandler)

	highestFinalBlockNonce := mp.forkDetector.GetHighestFinalBlockNonce()
	saveMetricsForCommitMetachainBlock(mp.appStatusHandler, header, headerHash, mp.nodesCoordinator, highestFinalBlockNonce)

	headersPool := mp.dataPool.Headers()
	numShardHeadersFromPool := 0
	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		numShardHeadersFromPool += headersPool.GetNumHeaders(shardID)
	}

	go mp.headersCounter.displayLogInfo(
		header,
		body,
		headerHash,
		numShardHeadersFromPool,
		mp.blockTracker,
		uint64(mp.roundHandler.TimeDuration().Seconds()),
	)

	headerInfo := bootstrapStorage.BootstrapHeaderInfo{
		ShardId: header.GetShardID(),
		Epoch:   header.GetEpoch(),
		Nonce:   header.GetNonce(),
		Hash:    headerHash,
	}

	nodesCoordinatorKey := mp.nodesCoordinator.GetSavedStateKey()
	epochStartKey := mp.epochStartTrigger.GetSavedStateKey()

	args := bootStorerDataArgs{
		headerInfo:                 headerInfo,
		round:                      header.Round,
		lastSelfNotarizedHeaders:   mp.getLastSelfNotarizedHeaders(),
		nodesCoordinatorConfigKey:  nodesCoordinatorKey,
		epochStartTriggerConfigKey: epochStartKey,
		pendingMiniBlocks:          mp.getPendingMiniBlocks(),
		highestFinalBlockNonce:     highestFinalBlockNonce,
	}

	mp.prepareDataForBootStorer(args)

	mp.blockSizeThrottler.Succeed(header.Round)

	mp.displayPoolsInfo()

	errNotCritical = mp.removeTxsFromPools(bodyHandler)
	if errNotCritical != nil {
		log.Debug("removeTxsFromPools", "error", errNotCritical.Error())
	}

	mp.cleanupPools(headerHandler)

	return nil
}

func (mp *metaProcessor) updateCrossShardInfo(metaBlock *block.MetaBlock) ([]string, error) {
	mp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	defer mp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	notarizedHeadersHashes := make([]string, 0)
	for i := 0; i < len(metaBlock.ShardInfo); i++ {
		shardHeaderHash := metaBlock.ShardInfo[i].HeaderHash
		headerInfo, ok := mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardHeaderHash)]
		if !ok {
			return nil, fmt.Errorf("%w : updateCrossShardInfo shardHeaderHash = %s",
				process.ErrMissingHeader, logger.DisplayByteSlice(shardHeaderHash))
		}

		shardHeader, ok := headerInfo.hdr.(*block.Header)
		if !ok {
			return nil, process.ErrWrongTypeAssertion
		}

		mp.updateShardHeadersNonce(shardHeader.ShardID, shardHeader.Nonce)

		marshalizedShardHeader, err := mp.marshalizer.Marshal(shardHeader)
		if err != nil {
			return nil, err
		}

		notarizedHeadersHashes = append(notarizedHeadersHashes, hex.EncodeToString(shardHeaderHash))

		mp.saveShardHeader(shardHeader, shardHeaderHash, marshalizedShardHeader)
	}

	mp.saveMetricCrossCheckBlockHeight()

	return notarizedHeadersHashes, nil
}

func (mp *metaProcessor) displayPoolsInfo() {
	headersPool := mp.dataPool.Headers()
	miniBlocksPool := mp.dataPool.MiniBlocks()

	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		log.Trace("pools info",
			"shard", shardID,
			"num headers", headersPool.GetNumHeaders(shardID))
	}

	log.Trace("pools info",
		"shard", core.MetachainShardId,
		"num headers", headersPool.GetNumHeaders(core.MetachainShardId))

	// numShardsToKeepHeaders represents the total number of shards for which meta node would keep tracking headers
	// (in this case this number is equal with: number of shards + metachain (self shard))
	numShardsToKeepHeaders := int(mp.shardCoordinator.NumberOfShards()) + 1
	capacity := headersPool.MaxSize() * numShardsToKeepHeaders
	log.Debug("pools info",
		"total headers", headersPool.Len(),
		"headers pool capacity", capacity,
		"total miniblocks", miniBlocksPool.Len(),
		"miniblocks pool capacity", miniBlocksPool.MaxSize(),
	)

	mp.displayMiniBlocksPool()
}

func (mp *metaProcessor) updateState(lastMetaBlock data.HeaderHandler) {
	if check.IfNil(lastMetaBlock) {
		log.Debug("updateState nil header")
		return
	}

	mp.validatorStatisticsProcessor.SetLastFinalizedRootHash(lastMetaBlock.GetValidatorStatsRootHash())

	prevHeader, errNotCritical := process.GetMetaHeader(
		lastMetaBlock.GetPrevHash(),
		mp.dataPool.Headers(),
		mp.marshalizer,
		mp.store,
	)
	if errNotCritical != nil {
		log.Debug("could not get meta header from storage")
		return
	}

	if lastMetaBlock.IsStartOfEpochBlock() {
		log.Debug("trie snapshot", "rootHash", lastMetaBlock.GetRootHash())
		mp.accountsDB[state.UserAccountsState].SnapshotState(lastMetaBlock.GetRootHash())
		mp.accountsDB[state.PeerAccountsState].SnapshotState(lastMetaBlock.GetValidatorStatsRootHash())
		go func() {
			metaBlock, ok := lastMetaBlock.(*block.MetaBlock)
			if !ok {
				log.Warn("cannot commit Trie Epoch Root Hash: lastMetaBlock is not *block.MetaBlock")
				return
			}
			err := mp.commitTrieEpochRootHashIfNeeded(metaBlock, lastMetaBlock.GetRootHash())
			if err != nil {
				log.Warn("couldn't commit trie checkpoint", "epoch", metaBlock.Epoch, "error", err)
			}
		}()
	}

	mp.updateStateStorage(
		lastMetaBlock,
		lastMetaBlock.GetRootHash(),
		prevHeader.GetRootHash(),
		mp.accountsDB[state.UserAccountsState],
		mp.userStatePruningQueue,
	)

	mp.updateStateStorage(
		lastMetaBlock,
		lastMetaBlock.GetValidatorStatsRootHash(),
		prevHeader.GetValidatorStatsRootHash(),
		mp.accountsDB[state.PeerAccountsState],
		mp.peerStatePruningQueue,
	)
}

func (mp *metaProcessor) getLastSelfNotarizedHeaderByShard(
	metaBlock *block.MetaBlock,
	shardID uint32,
) (data.HeaderHandler, []byte) {

	lastNotarizedMetaHeader, lastNotarizedMetaHeaderHash, err := mp.blockTracker.GetLastSelfNotarizedHeader(shardID)
	if err != nil {
		log.Warn("getLastSelfNotarizedHeaderByShard.GetLastSelfNotarizedHeader",
			"shard", shardID,
			"error", err.Error())
		return nil, nil
	}

	maxNotarizedNonce := lastNotarizedMetaHeader.GetNonce()
	for _, shardData := range metaBlock.ShardInfo {
		if shardData.ShardID != shardID {
			continue
		}

		mp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
		headerInfo, ok := mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardData.HeaderHash)]
		mp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()
		if !ok {
			log.Debug("getLastSelfNotarizedHeaderByShard",
				"error", process.ErrMissingHeader,
				"hash", shardData.HeaderHash)
			continue
		}

		shardHeader, ok := headerInfo.hdr.(*block.Header)
		if !ok {
			log.Debug("getLastSelfNotarizedHeaderByShard",
				"error", process.ErrWrongTypeAssertion,
				"hash", shardData.HeaderHash)
			continue
		}

		for _, metaHash := range shardHeader.MetaBlockHashes {
			metaHeader, errGet := process.GetMetaHeader(
				metaHash,
				mp.dataPool.Headers(),
				mp.marshalizer,
				mp.store,
			)
			if errGet != nil {
				log.Trace("getLastSelfNotarizedHeaderByShard.GetMetaHeader", "error", errGet.Error())
				continue
			}

			if metaHeader.Nonce > maxNotarizedNonce {
				maxNotarizedNonce = metaHeader.Nonce
				lastNotarizedMetaHeader = metaHeader
				lastNotarizedMetaHeaderHash = metaHash
			}
		}
	}

	if lastNotarizedMetaHeader != nil {
		log.Debug("last notarized meta header in shard",
			"shard", shardID,
			"epoch", lastNotarizedMetaHeader.GetEpoch(),
			"round", lastNotarizedMetaHeader.GetRound(),
			"nonce", lastNotarizedMetaHeader.GetNonce(),
			"hash", lastNotarizedMetaHeaderHash,
		)
	}

	return lastNotarizedMetaHeader, lastNotarizedMetaHeaderHash
}

// ApplyProcessedMiniBlocks will do nothing on meta processor
func (mp *metaProcessor) ApplyProcessedMiniBlocks(_ *processedMb.ProcessedMiniBlockTracker) {
}

// getRewardsTxs must be called before method commitEpoch start because when commit is done rewards txs are removed from pool and saved in storage
func (mp *metaProcessor) getRewardsTxs(header *block.MetaBlock, body *block.Body) (rewardsTx map[string]data.TransactionHandler) {
	if !mp.outportHandler.HasDrivers() {
		return
	}
	if !header.IsStartOfEpochBlock() {
		return
	}

	rewardsTx = mp.epochRewardsCreator.GetRewardsTxs(body)
	return rewardsTx
}

func (mp *metaProcessor) commitEpochStart(header *block.MetaBlock, body *block.Body) {
	if header.IsStartOfEpochBlock() {
		mp.epochStartTrigger.SetProcessed(header, body)
		go mp.epochRewardsCreator.SaveTxBlockToStorage(header, body)
		go mp.validatorInfoCreator.SaveValidatorInfoBlocksToStorage(header, body)
	} else {
		currentHeader := mp.blockChain.GetCurrentBlockHeader()
		if !check.IfNil(currentHeader) && currentHeader.IsStartOfEpochBlock() {
			mp.epochStartTrigger.SetFinalityAttestingRound(header.GetRound())
			mp.nodesCoordinator.ShuffleOutForEpoch(currentHeader.GetEpoch())
		}
	}
}

// RevertStateToBlock recreates the state tries to the root hashes indicated by the provided header
func (mp *metaProcessor) RevertStateToBlock(header data.HeaderHandler) error {
	err := mp.accountsDB[state.UserAccountsState].RecreateTrie(header.GetRootHash())
	if err != nil {
		log.Debug("recreate trie with error for header",
			"nonce", header.GetNonce(),
			"hash", header.GetRootHash(),
			"error", err.Error(),
		)

		return err
	}

	err = mp.validatorStatisticsProcessor.RevertPeerState(header)
	if err != nil {
		log.Debug("revert peer state with error for header",
			"nonce", header.GetNonce(),
			"validators root hash", header.GetValidatorStatsRootHash(),
			"error", err.Error(),
		)

		return err
	}

	err = mp.epochStartTrigger.RevertStateToBlock(header)
	if err != nil {
		log.Debug("revert epoch start trigger for header",
			"nonce", header.GetNonce(),
			"error", err,
		)
		return err
	}

	return nil
}

func (mp *metaProcessor) updateShardHeadersNonce(key uint32, value uint64) {
	valueStoredI, ok := mp.shardsHeadersNonce.Load(key)
	if !ok {
		mp.shardsHeadersNonce.Store(key, value)
		return
	}

	valueStored, ok := valueStoredI.(uint64)
	if !ok {
		mp.shardsHeadersNonce.Store(key, value)
		return
	}

	if valueStored < value {
		mp.shardsHeadersNonce.Store(key, value)
	}
}

func (mp *metaProcessor) saveMetricCrossCheckBlockHeight() {
	crossCheckBlockHeight := ""
	for i := uint32(0); i < mp.shardCoordinator.NumberOfShards(); i++ {
		heightValue := uint64(0)

		valueStoredI, isValueInMap := mp.shardsHeadersNonce.Load(i)
		if isValueInMap {
			valueStored, ok := valueStoredI.(uint64)
			if ok {
				heightValue = valueStored
			}
		}

		crossCheckBlockHeight += fmt.Sprintf("%d: %d, ", i, heightValue)
	}

	mp.appStatusHandler.SetStringValue(core.MetricCrossCheckBlockHeight, crossCheckBlockHeight)
}

func (mp *metaProcessor) saveLastNotarizedHeader(header *block.MetaBlock) error {
	lastCrossNotarizedHeaderForShard := make(map[uint32]*hashAndHdr, mp.shardCoordinator.NumberOfShards())
	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		lastCrossNotarizedHeader, lastCrossNotarizedHeaderHash, err := mp.blockTracker.GetLastCrossNotarizedHeader(shardID)
		if err != nil {
			return err
		}

		lastCrossNotarizedHeaderForShard[shardID] = &hashAndHdr{hdr: lastCrossNotarizedHeader, hash: lastCrossNotarizedHeaderHash}
	}

	mp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	for i := 0; i < len(header.ShardInfo); i++ {
		shardHeaderHash := header.ShardInfo[i].HeaderHash
		headerInfo, ok := mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardHeaderHash)]
		if !ok {
			mp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()
			return fmt.Errorf("%w : saveLastNotarizedHeader shardHeaderHash = %s",
				process.ErrMissingHeader, logger.DisplayByteSlice(shardHeaderHash))
		}

		shardHeader, ok := headerInfo.hdr.(*block.Header)
		if !ok {
			mp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()
			return process.ErrWrongTypeAssertion
		}

		if lastCrossNotarizedHeaderForShard[shardHeader.ShardID].hdr.GetNonce() < shardHeader.Nonce {
			lastCrossNotarizedHeaderForShard[shardHeader.ShardID] = &hashAndHdr{hdr: shardHeader, hash: shardHeaderHash}
		}
	}
	mp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		hdr := lastCrossNotarizedHeaderForShard[shardID].hdr
		hash := lastCrossNotarizedHeaderForShard[shardID].hash
		mp.blockTracker.AddCrossNotarizedHeader(shardID, hdr, hash)
		DisplayLastNotarized(mp.marshalizer, mp.hasher, hdr, shardID)
	}

	return nil
}

func (mp *metaProcessor) getLastCrossNotarizedShardHdrs() (map[uint32]data.HeaderHandler, error) {
	mp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	defer mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

	lastCrossNotarizedHeader := make(map[uint32]data.HeaderHandler, mp.shardCoordinator.NumberOfShards())
	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		lastCrossNotarizedHeaderForShard, hash, err := mp.blockTracker.GetLastCrossNotarizedHeader(shardID)
		if err != nil {
			return nil, err
		}

		log.Debug("lastCrossNotarizedHeader for shard", "shardID", shardID, "hash", hash)
		lastCrossNotarizedHeader[shardID] = lastCrossNotarizedHeaderForShard
		usedInBlock := mp.isGenesisShardBlockAndFirstMeta(lastCrossNotarizedHeaderForShard.GetNonce())
		mp.hdrsForCurrBlock.hdrHashAndInfo[string(hash)] = &hdrInfo{hdr: lastCrossNotarizedHeaderForShard, usedInBlock: usedInBlock}
	}

	return lastCrossNotarizedHeader, nil
}

// check if shard headers were signed and constructed correctly and returns headers which has to be
// checked for finality
func (mp *metaProcessor) checkShardHeadersValidity(metaHdr *block.MetaBlock) (map[uint32]data.HeaderHandler, error) {
	lastCrossNotarizedHeader, err := mp.getLastCrossNotarizedShardHdrs()
	if err != nil {
		return nil, err
	}

	usedShardHdrs := mp.sortHeadersForCurrentBlockByNonce(true)
	highestNonceHdrs := make(map[uint32]data.HeaderHandler, len(usedShardHdrs))

	if len(usedShardHdrs) == 0 {
		return highestNonceHdrs, nil
	}

	for shardID, hdrsForShard := range usedShardHdrs {
		for _, shardHdr := range hdrsForShard {
			if !mp.isGenesisShardBlockAndFirstMeta(shardHdr.GetNonce()) {
				err = mp.headerValidator.IsHeaderConstructionValid(shardHdr, lastCrossNotarizedHeader[shardID])
				if err != nil {
					return nil, fmt.Errorf("%w : checkShardHeadersValidity -> isHdrConstructionValid", err)
				}
			}

			lastCrossNotarizedHeader[shardID] = shardHdr
			highestNonceHdrs[shardID] = shardHdr
		}
	}

	mp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	defer mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

	for _, shardData := range metaHdr.ShardInfo {
		headerInfo, ok := mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardData.HeaderHash)]
		if !ok {
			return nil, fmt.Errorf("%w : checkShardHeadersValidity -> hash not found %s ", process.ErrHeaderShardDataMismatch, shardData.HeaderHash)
		}
		actualHdr := headerInfo.hdr
		shardHdr, ok := actualHdr.(*block.Header)
		if !ok {
			return nil, process.ErrWrongTypeAssertion
		}

		if len(shardData.ShardMiniBlockHeaders) != len(shardHdr.MiniBlockHeaders) {
			return nil, process.ErrHeaderShardDataMismatch
		}
		if shardData.AccumulatedFees.Cmp(shardHdr.AccumulatedFees) != 0 {
			return nil, process.ErrAccumulatedFeesDoNotMatch
		}
		if shardData.DeveloperFees.Cmp(shardHdr.DeveloperFees) != 0 {
			return nil, process.ErrDeveloperFeesDoNotMatch
		}

		mapMiniBlockHeadersInMetaBlock := make(map[string]struct{})
		for _, shardMiniBlockHdr := range shardData.ShardMiniBlockHeaders {
			mapMiniBlockHeadersInMetaBlock[string(shardMiniBlockHdr.Hash)] = struct{}{}
		}

		for _, actualMiniBlockHdr := range shardHdr.MiniBlockHeaders {
			if _, hashExists := mapMiniBlockHeadersInMetaBlock[string(actualMiniBlockHdr.Hash)]; !hashExists {
				return nil, process.ErrHeaderShardDataMismatch
			}
		}
	}

	return highestNonceHdrs, nil
}

// check if shard headers are final by checking if newer headers were constructed upon them
func (mp *metaProcessor) checkShardHeadersFinality(
	highestNonceHdrs map[uint32]data.HeaderHandler,
) error {
	finalityAttestingShardHdrs := mp.sortHeadersForCurrentBlockByNonce(false)

	var errFinal error

	for shardId, lastVerifiedHdr := range highestNonceHdrs {
		if lastVerifiedHdr == nil || lastVerifiedHdr.IsInterfaceNil() {
			return process.ErrNilBlockHeader
		}
		if lastVerifiedHdr.GetShardID() != shardId {
			return process.ErrShardIdMissmatch
		}
		isGenesisShardBlockAndFirstMeta := mp.isGenesisShardBlockAndFirstMeta(lastVerifiedHdr.GetNonce())
		if isGenesisShardBlockAndFirstMeta {
			continue
		}

		// verify if there are "K" block after current to make this one final
		nextBlocksVerified := uint32(0)
		for _, shardHdr := range finalityAttestingShardHdrs[shardId] {
			if nextBlocksVerified >= mp.shardBlockFinality {
				break
			}

			// found a header with the next nonce
			if shardHdr.GetNonce() == lastVerifiedHdr.GetNonce()+1 {
				err := mp.headerValidator.IsHeaderConstructionValid(shardHdr, lastVerifiedHdr)
				if err != nil {
					log.Debug("checkShardHeadersFinality -> isHdrConstructionValid",
						"error", err.Error())
					continue
				}

				lastVerifiedHdr = shardHdr
				nextBlocksVerified += 1
			}
		}

		if nextBlocksVerified < mp.shardBlockFinality {
			go mp.requestHandler.RequestShardHeaderByNonce(lastVerifiedHdr.GetShardID(), lastVerifiedHdr.GetNonce())
			go mp.requestHandler.RequestShardHeaderByNonce(lastVerifiedHdr.GetShardID(), lastVerifiedHdr.GetNonce()+1)
			errFinal = process.ErrHeaderNotFinal
		}
	}

	return errFinal
}

// receivedShardHeader is a call back function which is called when a new header
// is added in the headers pool
func (mp *metaProcessor) receivedShardHeader(headerHandler data.HeaderHandler, shardHeaderHash []byte) {
	shardHeader, ok := headerHandler.(*block.Header)
	if !ok {
		return
	}

	log.Trace("received shard header from network",
		"shard", shardHeader.ShardID,
		"round", shardHeader.Round,
		"nonce", shardHeader.Nonce,
		"hash", shardHeaderHash,
	)

	mp.hdrsForCurrBlock.mutHdrsForBlock.Lock()

	haveMissingShardHeaders := mp.hdrsForCurrBlock.missingHdrs > 0 || mp.hdrsForCurrBlock.missingFinalityAttestingHdrs > 0
	if haveMissingShardHeaders {
		hdrInfoForHash := mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardHeaderHash)]
		headerInfoIsNotNil := hdrInfoForHash != nil
		headerIsMissing := headerInfoIsNotNil && check.IfNil(hdrInfoForHash.hdr)
		if headerIsMissing {
			hdrInfoForHash.hdr = shardHeader
			mp.hdrsForCurrBlock.missingHdrs--

			if shardHeader.Nonce > mp.hdrsForCurrBlock.highestHdrNonce[shardHeader.ShardID] {
				mp.hdrsForCurrBlock.highestHdrNonce[shardHeader.ShardID] = shardHeader.Nonce
			}
		}

		if mp.hdrsForCurrBlock.missingHdrs == 0 {
			mp.hdrsForCurrBlock.missingFinalityAttestingHdrs = mp.requestMissingFinalityAttestingShardHeaders()
			if mp.hdrsForCurrBlock.missingFinalityAttestingHdrs == 0 {
				log.Debug("received all missing finality attesting shard headers")
			}
		}

		missingShardHdrs := mp.hdrsForCurrBlock.missingHdrs
		missingFinalityAttestingShardHdrs := mp.hdrsForCurrBlock.missingFinalityAttestingHdrs
		mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

		allMissingShardHeadersReceived := missingShardHdrs == 0 && missingFinalityAttestingShardHdrs == 0
		if allMissingShardHeadersReceived {
			mp.chRcvAllHdrs <- true
		}
	} else {
		mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()
	}

	go mp.requestMiniBlocksIfNeeded(headerHandler)
}

// requestMissingFinalityAttestingShardHeaders requests the headers needed to accept the current selected headers for
// processing the current block. It requests the shardBlockFinality headers greater than the highest shard header,
// for each shard, related to the block which should be processed
// this method should be called only under the mutex protection: hdrsForCurrBlock.mutHdrsForBlock
func (mp *metaProcessor) requestMissingFinalityAttestingShardHeaders() uint32 {
	missingFinalityAttestingShardHeaders := uint32(0)

	for shardId := uint32(0); shardId < mp.shardCoordinator.NumberOfShards(); shardId++ {
		missingFinalityAttestingHeaders := mp.requestMissingFinalityAttestingHeaders(
			shardId,
			mp.shardBlockFinality,
		)

		missingFinalityAttestingShardHeaders += missingFinalityAttestingHeaders
	}

	return missingFinalityAttestingShardHeaders
}

func (mp *metaProcessor) requestShardHeaders(metaBlock *block.MetaBlock) (uint32, uint32) {
	_ = core.EmptyChannel(mp.chRcvAllHdrs)

	if len(metaBlock.ShardInfo) == 0 {
		return 0, 0
	}

	return mp.computeExistingAndRequestMissingShardHeaders(metaBlock)
}

func (mp *metaProcessor) computeExistingAndRequestMissingShardHeaders(metaBlock *block.MetaBlock) (uint32, uint32) {
	mp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	defer mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

	for _, shardData := range metaBlock.ShardInfo {
		if shardData.Nonce == mp.genesisNonce {
			lastCrossNotarizedHeaderForShard, hash, err := mp.blockTracker.GetLastCrossNotarizedHeader(shardData.ShardID)
			if err != nil {
				log.Warn("computeExistingAndRequestMissingShardHeaders.GetLastCrossNotarizedHeader", "error", err.Error())
				continue
			}
			if !bytes.Equal(hash, shardData.HeaderHash) {
				log.Warn("genesis hash missmatch",
					"last notarized nonce", lastCrossNotarizedHeaderForShard.GetNonce(),
					"last notarized hash", hash,
					"genesis nonce", mp.genesisNonce,
					"genesis hash", shardData.HeaderHash)
			}
			continue
		}

		hdr, err := process.GetShardHeaderFromPool(
			shardData.HeaderHash,
			mp.dataPool.Headers())

		if err != nil {
			mp.hdrsForCurrBlock.missingHdrs++
			mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardData.HeaderHash)] = &hdrInfo{
				hdr:         nil,
				usedInBlock: true,
			}
			go mp.requestHandler.RequestShardHeader(shardData.ShardID, shardData.HeaderHash)
			continue
		}

		mp.hdrsForCurrBlock.hdrHashAndInfo[string(shardData.HeaderHash)] = &hdrInfo{
			hdr:         hdr,
			usedInBlock: true,
		}

		if hdr.Nonce > mp.hdrsForCurrBlock.highestHdrNonce[shardData.ShardID] {
			mp.hdrsForCurrBlock.highestHdrNonce[shardData.ShardID] = hdr.Nonce
		}
	}

	if mp.hdrsForCurrBlock.missingHdrs == 0 {
		mp.hdrsForCurrBlock.missingFinalityAttestingHdrs = mp.requestMissingFinalityAttestingShardHeaders()
	}

	return mp.hdrsForCurrBlock.missingHdrs, mp.hdrsForCurrBlock.missingFinalityAttestingHdrs
}

func (mp *metaProcessor) createShardInfo() ([]block.ShardData, error) {
	var shardInfo []block.ShardData
	if mp.epochStartTrigger.IsEpochStart() {
		return shardInfo, nil
	}

	mp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	for hdrHash, headerInfo := range mp.hdrsForCurrBlock.hdrHashAndInfo {
		if !headerInfo.usedInBlock {
			continue
		}

		shardHdr, ok := headerInfo.hdr.(*block.Header)
		if !ok {
			return nil, process.ErrWrongTypeAssertion
		}

		shardData := block.ShardData{}
		shardData.TxCount = shardHdr.TxCount
		shardData.ShardID = shardHdr.ShardID
		shardData.HeaderHash = []byte(hdrHash)
		shardData.Round = shardHdr.Round
		shardData.PrevHash = shardHdr.PrevHash
		shardData.Nonce = shardHdr.Nonce
		shardData.PrevRandSeed = shardHdr.PrevRandSeed
		shardData.PubKeysBitmap = shardHdr.PubKeysBitmap
		shardData.NumPendingMiniBlocks = uint32(len(mp.pendingMiniBlocksHandler.GetPendingMiniBlocks(shardData.ShardID)))
		header, _, err := mp.blockTracker.GetLastSelfNotarizedHeader(shardHdr.ShardID)
		if err != nil {
			return nil, err
		}
		shardData.LastIncludedMetaNonce = header.GetNonce()
		shardData.AccumulatedFees = shardHdr.AccumulatedFees
		shardData.DeveloperFees = shardHdr.DeveloperFees

		if len(shardHdr.MiniBlockHeaders) > 0 {
			shardData.ShardMiniBlockHeaders = make([]block.MiniBlockHeader, 0, len(shardHdr.MiniBlockHeaders))
		}

		for i := 0; i < len(shardHdr.MiniBlockHeaders); i++ {
			shardMiniBlockHeader := block.MiniBlockHeader{}
			shardMiniBlockHeader.SenderShardID = shardHdr.MiniBlockHeaders[i].SenderShardID
			shardMiniBlockHeader.ReceiverShardID = shardHdr.MiniBlockHeaders[i].ReceiverShardID
			shardMiniBlockHeader.Hash = shardHdr.MiniBlockHeaders[i].Hash
			shardMiniBlockHeader.TxCount = shardHdr.MiniBlockHeaders[i].TxCount
			shardMiniBlockHeader.Type = shardHdr.MiniBlockHeaders[i].Type

			shardData.ShardMiniBlockHeaders = append(shardData.ShardMiniBlockHeaders, shardMiniBlockHeader)
		}

		shardInfo = append(shardInfo, shardData)
	}
	mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()

	log.Debug("created shard data",
		"size", len(shardInfo),
	)
	return shardInfo, nil
}

func (mp *metaProcessor) verifyTotalAccumulatedFeesInEpoch(metaHdr *block.MetaBlock) error {
	computedTotalFees, computedTotalDevFees, err := mp.computeAccumulatedFeesInEpoch(metaHdr)
	if err != nil {
		return err
	}

	if computedTotalFees.Cmp(metaHdr.AccumulatedFeesInEpoch) != 0 {
		return fmt.Errorf("%w, got %v, computed %v", process.ErrAccumulatedFeesInEpochDoNotMatch, metaHdr.AccumulatedFeesInEpoch, computedTotalFees)
	}

	if computedTotalDevFees.Cmp(metaHdr.DevFeesInEpoch) != 0 {
		return fmt.Errorf("%w, got %v, computed %v", process.ErrDevFeesInEpochDoNotMatch, metaHdr.DevFeesInEpoch, computedTotalDevFees)
	}

	return nil
}

func (mp *metaProcessor) computeAccumulatedFeesInEpoch(metaHdr *block.MetaBlock) (*big.Int, *big.Int, error) {
	currentlyAccumulatedFeesInEpoch := big.NewInt(0)
	currentDevFeesInEpoch := big.NewInt(0)

	lastHdr := mp.blockChain.GetCurrentBlockHeader()
	if !check.IfNil(lastHdr) {
		lastMeta, ok := lastHdr.(*block.MetaBlock)
		if !ok {
			return nil, nil, process.ErrWrongTypeAssertion
		}

		if !lastHdr.IsStartOfEpochBlock() {
			currentlyAccumulatedFeesInEpoch = big.NewInt(0).Set(lastMeta.AccumulatedFeesInEpoch)
			currentDevFeesInEpoch = big.NewInt(0).Set(lastMeta.DevFeesInEpoch)
		}
	}

	currentlyAccumulatedFeesInEpoch.Add(currentlyAccumulatedFeesInEpoch, metaHdr.GetAccumulatedFees())
	currentDevFeesInEpoch.Add(currentDevFeesInEpoch, metaHdr.GetDeveloperFees())
	for _, shardData := range metaHdr.ShardInfo {
		currentlyAccumulatedFeesInEpoch.Add(currentlyAccumulatedFeesInEpoch, shardData.AccumulatedFees)
		currentDevFeesInEpoch.Add(currentDevFeesInEpoch, shardData.DeveloperFees)
	}

	return currentlyAccumulatedFeesInEpoch, currentDevFeesInEpoch, nil
}

// applyBodyToHeader creates a miniblock header list given a block body
func (mp *metaProcessor) applyBodyToHeader(metaHdr *block.MetaBlock, bodyHandler data.BodyHandler) (data.BodyHandler, error) {
	sw := core.NewStopWatch()
	sw.Start("applyBodyToHeader")
	defer func() {
		sw.Stop("applyBodyToHeader")

		log.Debug("measurements", sw.GetMeasurements()...)
	}()

	if check.IfNil(bodyHandler) {
		return nil, process.ErrNilBlockBody
	}

	var err error
	defer func() {
		go mp.checkAndRequestIfShardHeadersMissing()
	}()

	sw.Start("createShardInfo")
	shardInfo, err := mp.createShardInfo()
	sw.Stop("createShardInfo")
	if err != nil {
		return nil, err
	}

	metaHdr.Epoch = mp.epochStartTrigger.Epoch()
	metaHdr.ShardInfo = shardInfo
	metaHdr.RootHash = mp.getRootHash()
	metaHdr.TxCount = getTxCount(shardInfo)
	metaHdr.AccumulatedFees = mp.feeHandler.GetAccumulatedFees()
	metaHdr.DeveloperFees = mp.feeHandler.GetDeveloperFees()

	metaHdr.AccumulatedFeesInEpoch, metaHdr.DevFeesInEpoch, err = mp.computeAccumulatedFeesInEpoch(metaHdr)
	if err != nil {
		return nil, err
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		err = process.ErrWrongTypeAssertion
		return nil, err
	}

	sw.Start("CreateReceiptsHash")
	metaHdr.ReceiptsHash, err = mp.txCoordinator.CreateReceiptsHash()
	sw.Stop("CreateReceiptsHash")
	if err != nil {
		return nil, err
	}

	totalTxCount, miniBlockHeaders, err := mp.createMiniBlockHeaders(body)
	if err != nil {
		return nil, err
	}

	metaHdr.MiniBlockHeaders = miniBlockHeaders
	metaHdr.TxCount += uint32(totalTxCount)

	sw.Start("UpdatePeerState")
	mp.prepareBlockHeaderInternalMapForValidatorProcessor()
	metaHdr.ValidatorStatsRootHash, err = mp.validatorStatisticsProcessor.UpdatePeerState(metaHdr, mp.hdrsForCurrBlock.getHdrHashMap())
	sw.Stop("UpdatePeerState")
	if err != nil {
		return nil, err
	}

	marshalizedBody, err := mp.marshalizer.Marshal(body)
	if err != nil {
		return nil, err
	}
	mp.blockSizeThrottler.Add(metaHdr.GetRound(), uint32(len(marshalizedBody)))

	return body, nil
}

func (mp *metaProcessor) prepareBlockHeaderInternalMapForValidatorProcessor() {
	currentBlockHeader := mp.blockChain.GetCurrentBlockHeader()
	currentBlockHeaderHash := mp.blockChain.GetCurrentBlockHeaderHash()

	if check.IfNil(currentBlockHeader) {
		currentBlockHeader = mp.blockChain.GetGenesisHeader()
		currentBlockHeaderHash = mp.blockChain.GetGenesisHeaderHash()
	}

	mp.hdrsForCurrBlock.mutHdrsForBlock.Lock()
	mp.hdrsForCurrBlock.hdrHashAndInfo[string(currentBlockHeaderHash)] = &hdrInfo{false, currentBlockHeader}
	mp.hdrsForCurrBlock.mutHdrsForBlock.Unlock()
}

func (mp *metaProcessor) verifyValidatorStatisticsRootHash(header *block.MetaBlock) error {
	mp.prepareBlockHeaderInternalMapForValidatorProcessor()
	validatorStatsRH, err := mp.validatorStatisticsProcessor.UpdatePeerState(header, mp.hdrsForCurrBlock.getHdrHashMap())
	if err != nil {
		return err
	}

	if !bytes.Equal(validatorStatsRH, header.GetValidatorStatsRootHash()) {
		log.Debug("validator stats root hash mismatch",
			"computed", validatorStatsRH,
			"received", header.GetValidatorStatsRootHash(),
		)
		return fmt.Errorf("%s, metachain, computed: %s, received: %s, meta header nonce: %d",
			process.ErrValidatorStatsRootHashDoesNotMatch,
			logger.DisplayByteSlice(validatorStatsRH),
			logger.DisplayByteSlice(header.GetValidatorStatsRootHash()),
			header.Nonce,
		)
	}

	return nil
}

func (mp *metaProcessor) waitForBlockHeaders(waitTime time.Duration) error {
	select {
	case <-mp.chRcvAllHdrs:
		return nil
	case <-time.After(waitTime):
		return process.ErrTimeIsOut
	}
}

// CreateNewHeader creates a new header
func (mp *metaProcessor) CreateNewHeader(round uint64, nonce uint64) data.HeaderHandler {
	metaHeader := &block.MetaBlock{
		Nonce:                  nonce,
		Round:                  round,
		AccumulatedFees:        big.NewInt(0),
		AccumulatedFeesInEpoch: big.NewInt(0),
		DeveloperFees:          big.NewInt(0),
		DevFeesInEpoch:         big.NewInt(0),
	}

	mp.epochStartTrigger.Update(round, nonce)

	return metaHeader
}

// MarshalizedDataToBroadcast prepares underlying data into a marshalized object according to destination
func (mp *metaProcessor) MarshalizedDataToBroadcast(
	hdr data.HeaderHandler,
	bodyHandler data.BodyHandler,
) (map[uint32][]byte, map[string][][]byte, error) {
	if check.IfNil(hdr) {
		return nil, nil, process.ErrNilMetaBlockHeader
	}
	if check.IfNil(bodyHandler) {
		return nil, nil, process.ErrNilMiniBlocks
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		return nil, nil, process.ErrWrongTypeAssertion
	}

	var mrsTxs map[string][][]byte
	if hdr.IsStartOfEpochBlock() {
		mrsTxs = mp.epochRewardsCreator.CreateMarshalizedData(body)
	} else {
		mrsTxs = mp.txCoordinator.CreateMarshalizedData(body)
	}

	bodies := make(map[uint32]block.MiniBlockSlice)
	for _, miniBlock := range body.MiniBlocks {
		if miniBlock.SenderShardID != mp.shardCoordinator.SelfId() ||
			miniBlock.ReceiverShardID == mp.shardCoordinator.SelfId() {
			continue
		}
		bodies[miniBlock.ReceiverShardID] = append(bodies[miniBlock.ReceiverShardID], miniBlock)
	}

	mrsData := make(map[uint32][]byte, len(bodies))
	for shardId, subsetBlockBody := range bodies {
		buff, err := mp.marshalizer.Marshal(&block.Body{MiniBlocks: subsetBlockBody})
		if err != nil {
			log.Error("metaProcessor.MarshalizedDataToBroadcast.Marshal", "error", err.Error())
			continue
		}
		mrsData[shardId] = buff
	}

	return mrsData, mrsTxs, nil
}

func getTxCount(shardInfo []block.ShardData) uint32 {
	txs := uint32(0)
	for i := 0; i < len(shardInfo); i++ {
		for j := 0; j < len(shardInfo[i].ShardMiniBlockHeaders); j++ {
			txs += shardInfo[i].ShardMiniBlockHeaders[j].TxCount
		}
	}

	return txs
}

// IsInterfaceNil returns true if there is no value under the interface
func (mp *metaProcessor) IsInterfaceNil() bool {
	return mp == nil
}

// GetBlockBodyFromPool returns block body from pool for a given header
func (mp *metaProcessor) GetBlockBodyFromPool(headerHandler data.HeaderHandler) (data.BodyHandler, error) {
	metaBlock, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	miniBlocksPool := mp.dataPool.MiniBlocks()
	var miniBlocks block.MiniBlockSlice

	for _, mbHeader := range metaBlock.MiniBlockHeaders {
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

func (mp *metaProcessor) getPendingMiniBlocks() []bootstrapStorage.PendingMiniBlocksInfo {
	pendingMiniBlocksInfo := make([]bootstrapStorage.PendingMiniBlocksInfo, mp.shardCoordinator.NumberOfShards())

	for shardID := uint32(0); shardID < mp.shardCoordinator.NumberOfShards(); shardID++ {
		pendingMiniBlocksInfo[shardID] = bootstrapStorage.PendingMiniBlocksInfo{
			MiniBlocksHashes: mp.pendingMiniBlocksHandler.GetPendingMiniBlocks(shardID),
			ShardID:          shardID,
		}
	}

	return pendingMiniBlocksInfo
}

func (mp *metaProcessor) removeStartOfEpochBlockDataFromPools(
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
) error {

	if !headerHandler.IsStartOfEpochBlock() {
		return nil
	}

	metaBlock, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	mp.epochRewardsCreator.RemoveBlockDataFromPools(metaBlock, body)
	mp.validatorInfoCreator.RemoveBlockDataFromPools(metaBlock, body)

	return nil
}

// Close - closes all underlying components
func (mp *metaProcessor) Close() error {
	return mp.baseProcessor.Close()
}
