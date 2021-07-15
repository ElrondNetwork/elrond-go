package block

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"time"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	nodeFactory "github.com/ElrondNetwork/elrond-go/cmd/node/factory"
	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/dblookupext"
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/batch"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/display"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/block/bootstrapStorage"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage/storageUnit"
)

var log = logger.GetOrCreate("process/block")

type hashAndHdr struct {
	hdr  data.HeaderHandler
	hash []byte
}

type nonceAndHashInfo struct {
	hash  []byte
	nonce uint64
}

type hdrInfo struct {
	usedInBlock bool
	hdr         data.HeaderHandler
}

type baseProcessor struct {
	shardCoordinator        sharding.Coordinator
	nodesCoordinator        sharding.NodesCoordinator
	accountsDB              map[state.AccountsDbIdentifier]state.AccountsAdapter
	forkDetector            process.ForkDetector
	hasher                  hashing.Hasher
	marshalizer             marshal.Marshalizer
	store                   dataRetriever.StorageService
	uint64Converter         typeConverters.Uint64ByteSliceConverter
	blockSizeThrottler      process.BlockSizeThrottler
	epochStartTrigger       process.EpochStartTriggerHandler
	headerValidator         process.HeaderConstructionValidator
	blockChainHook          process.BlockChainHookHandler
	txCoordinator           process.TransactionCoordinator
	roundHandler            consensus.RoundHandler
	bootStorer              process.BootStorer
	requestBlockBodyHandler process.RequestBlockBodyHandler
	requestHandler          process.RequestHandler
	blockTracker            process.BlockTracker
	dataPool                dataRetriever.PoolsHolder
	feeHandler              process.TransactionFeeHandler
	blockChain              data.ChainHandler
	hdrsForCurrBlock        *hdrForBlock
	genesisNonce            uint64

	versionedHeaderFactory       nodeFactory.VersionedHeaderFactory
	headerIntegrityVerifier      process.HeaderIntegrityVerifier
	scheduledTxsExecutionHandler process.ScheduledTxsExecutionHandler

	appStatusHandler       core.AppStatusHandler
	stateCheckpointModulus uint
	blockProcessor         blockProcessor
	txCounter              *transactionCounter

	indexer            process.Indexer
	tpsBenchmark       statistics.TPSBenchmark
	historyRepo        dblookupext.HistoryRepository
	epochNotifier      process.EpochNotifier
	vmContainerFactory process.VirtualMachinesContainerFactory
	vmContainer        process.VirtualMachinesContainer
}

type bootStorerDataArgs struct {
	headerInfo                 bootstrapStorage.BootstrapHeaderInfo
	lastSelfNotarizedHeaders   []bootstrapStorage.BootstrapHeaderInfo
	round                      uint64
	highestFinalBlockNonce     uint64
	pendingMiniBlocks          []bootstrapStorage.PendingMiniBlocksInfo
	processedMiniBlocks        []bootstrapStorage.MiniBlocksInMeta
	nodesCoordinatorConfigKey  []byte
	epochStartTriggerConfigKey []byte
}

func checkForNils(
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
) error {
	if check.IfNil(headerHandler) {
		return process.ErrNilBlockHeader
	}
	if check.IfNil(bodyHandler) {
		return process.ErrNilBlockBody
	}
	return nil
}

// checkBlockValidity method checks if the given block is valid
func (bp *baseProcessor) checkBlockValidity(
	headerHandler data.HeaderHandler,
	bodyHandler data.BodyHandler,
) error {

	err := checkForNils(headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	currentBlockHeader := bp.blockChain.GetCurrentBlockHeader()

	if check.IfNil(currentBlockHeader) {
		if headerHandler.GetNonce() == bp.genesisNonce+1 { // first block after genesis
			if bytes.Equal(headerHandler.GetPrevHash(), bp.blockChain.GetGenesisHeaderHash()) {
				// TODO: add genesis block verification
				return nil
			}

			log.Debug("hash does not match",
				"local block hash", bp.blockChain.GetGenesisHeaderHash(),
				"received previous hash", headerHandler.GetPrevHash())

			return process.ErrBlockHashDoesNotMatch
		}

		log.Debug("nonce does not match",
			"local block nonce", 0,
			"received nonce", headerHandler.GetNonce())

		return process.ErrWrongNonceInBlock
	}

	if headerHandler.GetRound() <= currentBlockHeader.GetRound() {
		log.Debug("round does not match",
			"local block round", currentBlockHeader.GetRound(),
			"received block round", headerHandler.GetRound())

		return process.ErrLowerRoundInBlock
	}

	if headerHandler.GetNonce() != currentBlockHeader.GetNonce()+1 {
		log.Debug("nonce does not match",
			"local block nonce", currentBlockHeader.GetNonce(),
			"received nonce", headerHandler.GetNonce())

		return process.ErrWrongNonceInBlock
	}

	if !bytes.Equal(headerHandler.GetPrevHash(), bp.blockChain.GetCurrentBlockHeaderHash()) {
		log.Debug("hash does not match",
			"local block hash", bp.blockChain.GetCurrentBlockHeaderHash(),
			"received previous hash", headerHandler.GetPrevHash())

		return process.ErrBlockHashDoesNotMatch
	}

	if !bytes.Equal(headerHandler.GetPrevRandSeed(), currentBlockHeader.GetRandSeed()) {
		log.Debug("random seed does not match",
			"local random seed", currentBlockHeader.GetRandSeed(),
			"received previous random seed", headerHandler.GetPrevRandSeed())

		return process.ErrRandSeedDoesNotMatch
	}

	// verification of epoch
	if headerHandler.GetEpoch() < currentBlockHeader.GetEpoch() {
		return process.ErrEpochDoesNotMatch
	}

	return nil
}

// verifyStateRoot verifies the state root hash given as parameter against the
// Merkle trie root hash stored for accounts and returns if equal or not
func (bp *baseProcessor) verifyStateRoot(rootHash []byte) bool {
	trieRootHash, err := bp.accountsDB[state.UserAccountsState].RootHash()
	if err != nil {
		log.Debug("verify account.RootHash", "error", err.Error())
	}

	return bytes.Equal(trieRootHash, rootHash)
}

// getRootHash returns the accounts merkle tree root hash
func (bp *baseProcessor) getRootHash() []byte {
	rootHash, err := bp.accountsDB[state.UserAccountsState].RootHash()
	if err != nil {
		log.Trace("get account.RootHash", "error", err.Error())
	}

	return rootHash
}

func (bp *baseProcessor) requestHeadersIfMissing(
	sortedHdrs []data.HeaderHandler,
	shardId uint32,
) error {

	prevHdr, _, err := bp.blockTracker.GetLastCrossNotarizedHeader(shardId)
	if err != nil {
		return err
	}

	lastNotarizedHdrRound := prevHdr.GetRound()
	lastNotarizedHdrNonce := prevHdr.GetNonce()

	missingNonces := make([]uint64, 0)
	for i := 0; i < len(sortedHdrs); i++ {
		currHdr := sortedHdrs[i]
		if check.IfNil(currHdr) {
			continue
		}

		hdrTooOld := currHdr.GetRound() <= lastNotarizedHdrRound
		if hdrTooOld {
			continue
		}

		maxNumNoncesToAdd := process.MaxHeaderRequestsAllowed - int(int64(prevHdr.GetNonce())-int64(lastNotarizedHdrNonce))
		if maxNumNoncesToAdd <= 0 {
			break
		}

		noncesDiff := int64(currHdr.GetNonce()) - int64(prevHdr.GetNonce())
		nonces := addMissingNonces(noncesDiff, prevHdr.GetNonce(), maxNumNoncesToAdd)
		missingNonces = append(missingNonces, nonces...)

		prevHdr = currHdr
	}

	maxNumNoncesToAdd := process.MaxHeaderRequestsAllowed - int(int64(prevHdr.GetNonce())-int64(lastNotarizedHdrNonce))
	if maxNumNoncesToAdd > 0 {
		lastRound := bp.roundHandler.Index() - 1
		roundsDiff := lastRound - int64(prevHdr.GetRound())
		nonces := addMissingNonces(roundsDiff, prevHdr.GetNonce(), maxNumNoncesToAdd)
		missingNonces = append(missingNonces, nonces...)
	}

	for _, nonce := range missingNonces {
		bp.addHeaderIntoTrackerPool(nonce, shardId)
		go bp.requestHeaderByShardAndNonce(shardId, nonce)
	}

	return nil
}

func addMissingNonces(diff int64, lastNonce uint64, maxNumNoncesToAdd int) []uint64 {
	missingNonces := make([]uint64, 0)

	if diff < 2 {
		return missingNonces
	}

	numNonces := uint64(diff) - 1
	startNonce := lastNonce + 1
	endNonce := startNonce + numNonces

	for nonce := startNonce; nonce < endNonce; nonce++ {
		missingNonces = append(missingNonces, nonce)
		if len(missingNonces) >= maxNumNoncesToAdd {
			break
		}
	}

	return missingNonces
}

func displayHeader(headerHandler data.HeaderHandler) []*display.LineData {
	var valStatRootHash, epochStartMetaHash, scheduledRootHash []byte
	metaHeader, isMetaHeader := headerHandler.(data.MetaHeaderHandler)
	if isMetaHeader {
		valStatRootHash = metaHeader.GetValidatorStatsRootHash()
	} else {
		shardHeader, isShardHeader := headerHandler.(data.ShardHeaderHandler)
		if isShardHeader {
			epochStartMetaHash = shardHeader.GetEpochStartMetaHash()
		}
	}
	additionalData := headerHandler.GetAdditionalData()
	if !check.IfNil(additionalData) {
		scheduledRootHash = additionalData.GetScheduledRootHash()
	}
	return []*display.LineData{
		display.NewLineData(false, []string{
			"",
			"ChainID",
			logger.DisplayByteSlice(headerHandler.GetChainID())}),
		display.NewLineData(false, []string{
			"",
			"Epoch",
			fmt.Sprintf("%d", headerHandler.GetEpoch())}),
		display.NewLineData(false, []string{
			"",
			"Round",
			fmt.Sprintf("%d", headerHandler.GetRound())}),
		display.NewLineData(false, []string{
			"",
			"TimeStamp",
			fmt.Sprintf("%d", headerHandler.GetTimeStamp())}),
		display.NewLineData(false, []string{
			"",
			"Nonce",
			fmt.Sprintf("%d", headerHandler.GetNonce())}),
		display.NewLineData(false, []string{
			"",
			"Prev hash",
			logger.DisplayByteSlice(headerHandler.GetPrevHash())}),
		display.NewLineData(false, []string{
			"",
			"Prev rand seed",
			logger.DisplayByteSlice(headerHandler.GetPrevRandSeed())}),
		display.NewLineData(false, []string{
			"",
			"Rand seed",
			logger.DisplayByteSlice(headerHandler.GetRandSeed())}),
		display.NewLineData(false, []string{
			"",
			"Pub keys bitmap",
			hex.EncodeToString(headerHandler.GetPubKeysBitmap())}),
		display.NewLineData(false, []string{
			"",
			"Signature",
			logger.DisplayByteSlice(headerHandler.GetSignature())}),
		display.NewLineData(false, []string{
			"",
			"Leader's Signature",
			logger.DisplayByteSlice(headerHandler.GetLeaderSignature())}),
		display.NewLineData(false, []string{
			"",
			"Scheduled root hash",
			logger.DisplayByteSlice(scheduledRootHash)}),
		display.NewLineData(false, []string{
			"",
			"Root hash",
			logger.DisplayByteSlice(headerHandler.GetRootHash())}),
		display.NewLineData(false, []string{
			"",
			"Validator stats root hash",
			logger.DisplayByteSlice(valStatRootHash)}),
		display.NewLineData(false, []string{
			"",
			"Receipts hash",
			logger.DisplayByteSlice(headerHandler.GetReceiptsHash())}),
		display.NewLineData(true, []string{
			"",
			"Epoch start meta hash",
			logger.DisplayByteSlice(epochStartMetaHash)}),
	}
}

// checkProcessorNilParameters will check the input parameters for nil values
func checkProcessorNilParameters(arguments ArgBaseProcessor) error {

	for key := range arguments.AccountsDB {
		if check.IfNil(arguments.AccountsDB[key]) {
			return process.ErrNilAccountsAdapter
		}
	}
	if check.IfNil(arguments.DataComponents) {
		return process.ErrNilDataComponentsHolder
	}
	if check.IfNil(arguments.CoreComponents) {
		return process.ErrNilCoreComponentsHolder
	}
	if check.IfNil(arguments.BootstrapComponents) {
		return process.ErrNilBootstrapComponentsHolder
	}
	if check.IfNil(arguments.StatusComponents) {
		return process.ErrNilStatusComponentsHolder
	}
	if check.IfNil(arguments.ForkDetector) {
		return process.ErrNilForkDetector
	}
	if check.IfNil(arguments.CoreComponents.Hasher()) {
		return process.ErrNilHasher
	}
	if check.IfNil(arguments.CoreComponents.InternalMarshalizer()) {
		return process.ErrNilMarshalizer
	}
	if check.IfNil(arguments.DataComponents.StorageService()) {
		return process.ErrNilStorage
	}
	if check.IfNil(arguments.BootstrapComponents.ShardCoordinator()) {
		return process.ErrNilShardCoordinator
	}
	if check.IfNil(arguments.NodesCoordinator) {
		return process.ErrNilNodesCoordinator
	}
	if check.IfNil(arguments.CoreComponents.Uint64ByteSliceConverter()) {
		return process.ErrNilUint64Converter
	}
	if check.IfNil(arguments.RequestHandler) {
		return process.ErrNilRequestHandler
	}
	if check.IfNil(arguments.EpochStartTrigger) {
		return process.ErrNilEpochStartTrigger
	}
	if check.IfNil(arguments.CoreComponents.RoundHandler()) {
		return process.ErrNilRoundHandler
	}
	if check.IfNil(arguments.BootStorer) {
		return process.ErrNilStorage
	}
	if check.IfNil(arguments.BlockChainHook) {
		return process.ErrNilBlockChainHook
	}
	if check.IfNil(arguments.TxCoordinator) {
		return process.ErrNilTransactionCoordinator
	}
	if check.IfNil(arguments.HeaderValidator) {
		return process.ErrNilHeaderValidator
	}
	if check.IfNil(arguments.BlockTracker) {
		return process.ErrNilBlockTracker
	}
	if check.IfNil(arguments.FeeHandler) {
		return process.ErrNilEconomicsFeeHandler
	}
	if check.IfNil(arguments.DataComponents.Blockchain()) {
		return process.ErrNilBlockChain
	}
	if check.IfNil(arguments.BlockSizeThrottler) {
		return process.ErrNilBlockSizeThrottler
	}
	if check.IfNil(arguments.StatusComponents.ElasticIndexer()) {
		return process.ErrNilIndexer
	}
	if check.IfNil(arguments.StatusComponents.TpsBenchmark()) {
		return process.ErrNilTpsBenchmark
	}
	if check.IfNil(arguments.HistoryRepository) {
		return process.ErrNilHistoryRepository
	}
	if check.IfNil(arguments.BootstrapComponents.HeaderIntegrityVerifier()) {
		return process.ErrNilHeaderIntegrityVerifier
	}
	if check.IfNil(arguments.EpochNotifier) {
		return process.ErrNilEpochNotifier
	}
	if check.IfNil(arguments.CoreComponents.StatusHandler()) {
		return process.ErrNilAppStatusHandler
	}
	if check.IfNil(arguments.ScheduledTxsExecutionHandler) {
		return process.ErrNilScheduledTxsExecutionHandler
	}
	if check.IfNil(arguments.BootstrapComponents.VersionedHeaderFactory()) {
		return process.ErrNilVersionedHeaderFactory
	}
	return nil
}

func (bp *baseProcessor) createBlockStarted() error {
	bp.hdrsForCurrBlock.resetMissingHdrs()
	bp.hdrsForCurrBlock.initMaps()
	bp.txCoordinator.CreateBlockStarted()
	bp.feeHandler.CreateBlockStarted()

	err := bp.txCoordinator.AddIntermediateTransactions(bp.scheduledTxsExecutionHandler.GetScheduledSCRs())
	if err != nil {
		return err
	}

	return nil
}

func (bp *baseProcessor) verifyFees(header data.HeaderHandler) error {
	if header.GetAccumulatedFees().Cmp(bp.feeHandler.GetAccumulatedFees()) != 0 {
		return process.ErrAccumulatedFeesDoNotMatch
	}
	if header.GetDeveloperFees().Cmp(bp.feeHandler.GetDeveloperFees()) != 0 {
		return process.ErrDeveloperFeesDoNotMatch
	}

	return nil
}

//TODO: remove bool parameter and give instead the set to sort
func (bp *baseProcessor) sortHeadersForCurrentBlockByNonce(usedInBlock bool) map[uint32][]data.HeaderHandler {
	hdrsForCurrentBlock := make(map[uint32][]data.HeaderHandler)

	bp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	for _, headerInfo := range bp.hdrsForCurrBlock.hdrHashAndInfo {
		if headerInfo.usedInBlock != usedInBlock {
			continue
		}

		hdrsForCurrentBlock[headerInfo.hdr.GetShardID()] = append(hdrsForCurrentBlock[headerInfo.hdr.GetShardID()], headerInfo.hdr)
	}
	bp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	// sort headers for each shard
	for _, hdrsForShard := range hdrsForCurrentBlock {
		process.SortHeadersByNonce(hdrsForShard)
	}

	return hdrsForCurrentBlock
}

func (bp *baseProcessor) sortHeaderHashesForCurrentBlockByNonce(usedInBlock bool) map[uint32][][]byte {
	hdrsForCurrentBlockInfo := make(map[uint32][]*nonceAndHashInfo)

	bp.hdrsForCurrBlock.mutHdrsForBlock.RLock()
	for metaBlockHash, headerInfo := range bp.hdrsForCurrBlock.hdrHashAndInfo {
		if headerInfo.usedInBlock != usedInBlock {
			continue
		}

		hdrsForCurrentBlockInfo[headerInfo.hdr.GetShardID()] = append(hdrsForCurrentBlockInfo[headerInfo.hdr.GetShardID()],
			&nonceAndHashInfo{nonce: headerInfo.hdr.GetNonce(), hash: []byte(metaBlockHash)})
	}
	bp.hdrsForCurrBlock.mutHdrsForBlock.RUnlock()

	for _, hdrsForShard := range hdrsForCurrentBlockInfo {
		if len(hdrsForShard) > 1 {
			sort.Slice(hdrsForShard, func(i, j int) bool {
				return hdrsForShard[i].nonce < hdrsForShard[j].nonce
			})
		}
	}

	hdrsHashesForCurrentBlock := make(map[uint32][][]byte, len(hdrsForCurrentBlockInfo))
	for shardId, hdrsForShard := range hdrsForCurrentBlockInfo {
		for _, hdrForShard := range hdrsForShard {
			hdrsHashesForCurrentBlock[shardId] = append(hdrsHashesForCurrentBlock[shardId], hdrForShard.hash)
		}
	}

	return hdrsHashesForCurrentBlock
}

func (bp *baseProcessor) createMiniBlockHeaderHandlers(body *block.Body) (int, []data.MiniBlockHeaderHandler, error) {
	if len(body.MiniBlocks) == 0 {
		return 0, nil, nil
	}

	totalTxCount := 0
	miniBlockHeaderHandlers := make([]data.MiniBlockHeaderHandler, len(body.MiniBlocks))

	for i := 0; i < len(body.MiniBlocks); i++ {
		txCount := len(body.MiniBlocks[i].TxHashes)
		totalTxCount += txCount

		miniBlockHash, err := core.CalculateHash(bp.marshalizer, bp.hasher, body.MiniBlocks[i])
		if err != nil {
			return 0, nil, err
		}

		miniBlockHeaderHandlers[i] = &block.MiniBlockHeader{
			Hash:            miniBlockHash,
			SenderShardID:   body.MiniBlocks[i].SenderShardID,
			ReceiverShardID: body.MiniBlocks[i].ReceiverShardID,
			TxCount:         uint32(txCount),
			Type:            body.MiniBlocks[i].Type,
		}
	}

	return totalTxCount, miniBlockHeaderHandlers, nil
}

// check if header has the same miniblocks as presented in body
func (bp *baseProcessor) checkHeaderBodyCorrelation(miniBlockHeaders []data.MiniBlockHeaderHandler, body *block.Body) error {
	mbHashesFromHdr := make(map[string]data.MiniBlockHeaderHandler, len(miniBlockHeaders))
	for i := 0; i < len(miniBlockHeaders); i++ {
		mbHashesFromHdr[string(miniBlockHeaders[i].GetHash())] = miniBlockHeaders[i]
	}

	if len(miniBlockHeaders) != len(body.MiniBlocks) {
		return process.ErrHeaderBodyMismatch
	}

	for i := 0; i < len(body.MiniBlocks); i++ {
		miniBlock := body.MiniBlocks[i]
		if miniBlock == nil {
			return process.ErrNilMiniBlock
		}

		mbHash, err := core.CalculateHash(bp.marshalizer, bp.hasher, miniBlock)
		if err != nil {
			return err
		}

		mbHdr, ok := mbHashesFromHdr[string(mbHash)]
		if !ok {
			return process.ErrHeaderBodyMismatch
		}

		if mbHdr.GetTxCount() != uint32(len(miniBlock.TxHashes)) {
			return process.ErrHeaderBodyMismatch
		}

		if mbHdr.GetReceiverShardID() != miniBlock.ReceiverShardID {
			return process.ErrHeaderBodyMismatch
		}

		if mbHdr.GetSenderShardID() != miniBlock.SenderShardID {
			return process.ErrHeaderBodyMismatch
		}
	}

	return nil
}

// requestMissingFinalityAttestingHeaders requests the headers needed to accept the current selected headers for
// processing the current block. It requests the finality headers greater than the highest header, for given shard,
// related to the block which should be processed
// this method should be called only under the mutex protection: hdrsForCurrBlock.mutHdrsForBlock
func (bp *baseProcessor) requestMissingFinalityAttestingHeaders(
	shardID uint32,
	finality uint32,
) uint32 {
	requestedHeaders := uint32(0)
	missingFinalityAttestingHeaders := uint32(0)

	highestHdrNonce := bp.hdrsForCurrBlock.highestHdrNonce[shardID]
	if highestHdrNonce == uint64(0) {
		return missingFinalityAttestingHeaders
	}

	headersPool := bp.dataPool.Headers()
	lastFinalityAttestingHeader := highestHdrNonce + uint64(finality)
	for i := highestHdrNonce + 1; i <= lastFinalityAttestingHeader; i++ {
		headers, headersHashes, err := headersPool.GetHeadersByNonceAndShardId(i, shardID)
		if err != nil {
			missingFinalityAttestingHeaders++
			requestedHeaders++
			go bp.requestHeaderByShardAndNonce(shardID, i)
			continue
		}

		for index := range headers {
			bp.hdrsForCurrBlock.hdrHashAndInfo[string(headersHashes[index])] = &hdrInfo{
				hdr:         headers[index],
				usedInBlock: false,
			}
		}
	}

	if requestedHeaders > 0 {
		log.Debug("requested missing finality attesting headers",
			"num headers", requestedHeaders,
			"shard", shardID)
	}

	return missingFinalityAttestingHeaders
}

func (bp *baseProcessor) requestHeaderByShardAndNonce(shardID uint32, nonce uint64) {
	if shardID == core.MetachainShardId {
		bp.requestHandler.RequestMetaHeaderByNonce(nonce)
	} else {
		bp.requestHandler.RequestShardHeaderByNonce(shardID, nonce)
	}
}

func (bp *baseProcessor) cleanupPools(headerHandler data.HeaderHandler) {
	bp.cleanupBlockTrackerPools(headerHandler)

	noncesToFinal := bp.getNoncesToFinal(headerHandler)

	bp.removeHeadersBehindNonceFromPools(
		true,
		bp.shardCoordinator.SelfId(),
		bp.forkDetector.GetHighestFinalBlockNonce())

	if bp.shardCoordinator.SelfId() == core.MetachainShardId {
		for shardID := uint32(0); shardID < bp.shardCoordinator.NumberOfShards(); shardID++ {
			bp.cleanupPoolsForCrossShard(shardID, noncesToFinal)
		}
	} else {
		bp.cleanupPoolsForCrossShard(core.MetachainShardId, noncesToFinal)
	}
}

func (bp *baseProcessor) cleanupPoolsForCrossShard(
	shardID uint32,
	noncesToFinal uint64,
) {
	crossNotarizedHeader, _, err := bp.blockTracker.GetCrossNotarizedHeader(shardID, noncesToFinal)
	if err != nil {
		log.Warn("cleanupPoolsForCrossShard",
			"shard", shardID,
			"nonces to final", noncesToFinal,
			"error", err.Error())
		return
	}

	bp.removeHeadersBehindNonceFromPools(
		false,
		shardID,
		crossNotarizedHeader.GetNonce(),
	)
}

func (bp *baseProcessor) removeHeadersBehindNonceFromPools(
	shouldRemoveBlockBody bool,
	shardId uint32,
	nonce uint64,
) {
	if nonce <= 1 {
		return
	}

	headersPool := bp.dataPool.Headers()
	nonces := headersPool.Nonces(shardId)
	for _, nonceFromCache := range nonces {
		if nonceFromCache >= nonce {
			continue
		}

		if shouldRemoveBlockBody {
			bp.removeBlocksBody(nonceFromCache, shardId)
		}

		headersPool.RemoveHeaderByNonceAndShardId(nonceFromCache, shardId)
	}
}

func (bp *baseProcessor) removeBlocksBody(nonce uint64, shardId uint32) {
	headersPool := bp.dataPool.Headers()
	headers, _, err := headersPool.GetHeadersByNonceAndShardId(nonce, shardId)
	if err != nil {
		return
	}

	for _, header := range headers {
		errNotCritical := bp.removeBlockBodyOfHeader(header)
		if errNotCritical != nil {
			log.Debug("RemoveBlockDataFromPool", "error", errNotCritical.Error())
		}
	}
}

func (bp *baseProcessor) removeBlockBodyOfHeader(headerHandler data.HeaderHandler) error {
	bodyHandler, err := bp.requestBlockBodyHandler.GetBlockBodyFromPool(headerHandler)
	if err != nil {
		return err
	}

	return bp.removeBlockDataFromPools(headerHandler, bodyHandler)
}

func (bp *baseProcessor) removeBlockDataFromPools(headerHandler data.HeaderHandler, bodyHandler data.BodyHandler) error {
	body, ok := bodyHandler.(*block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	err := bp.txCoordinator.RemoveBlockDataFromPool(body)
	if err != nil {
		return err
	}

	err = bp.blockProcessor.removeStartOfEpochBlockDataFromPools(headerHandler, bodyHandler)
	if err != nil {
		return err
	}

	return nil
}

func (bp *baseProcessor) removeTxsFromPools(bodyHandler data.BodyHandler) error {
	body, ok := bodyHandler.(*block.Body)
	if !ok {
		return process.ErrWrongTypeAssertion
	}

	return bp.txCoordinator.RemoveTxsFromPool(body)
}

func (bp *baseProcessor) cleanupBlockTrackerPools(headerHandler data.HeaderHandler) {
	noncesToFinal := bp.getNoncesToFinal(headerHandler)

	bp.cleanupBlockTrackerPoolsForShard(bp.shardCoordinator.SelfId(), noncesToFinal)

	if bp.shardCoordinator.SelfId() == core.MetachainShardId {
		for shardID := uint32(0); shardID < bp.shardCoordinator.NumberOfShards(); shardID++ {
			bp.cleanupBlockTrackerPoolsForShard(shardID, noncesToFinal)
		}
	} else {
		bp.cleanupBlockTrackerPoolsForShard(core.MetachainShardId, noncesToFinal)
	}
}

func (bp *baseProcessor) cleanupBlockTrackerPoolsForShard(shardID uint32, noncesToFinal uint64) {
	selfNotarizedHeader, _, errSelfNotarized := bp.blockTracker.GetSelfNotarizedHeader(shardID, noncesToFinal)
	if errSelfNotarized != nil {
		log.Warn("cleanupBlockTrackerPoolsForShard.GetSelfNotarizedHeader",
			"shard", shardID,
			"nonces to final", noncesToFinal,
			"error", errSelfNotarized.Error())
		return
	}

	selfNotarizedNonce := selfNotarizedHeader.GetNonce()

	crossNotarizedNonce := uint64(0)
	if shardID != bp.shardCoordinator.SelfId() {
		crossNotarizedHeader, _, errCrossNotarized := bp.blockTracker.GetCrossNotarizedHeader(shardID, noncesToFinal)
		if errCrossNotarized != nil {
			log.Warn("cleanupBlockTrackerPoolsForShard.GetCrossNotarizedHeader",
				"shard", shardID,
				"nonces to final", noncesToFinal,
				"error", errCrossNotarized.Error())
			return
		}

		crossNotarizedNonce = crossNotarizedHeader.GetNonce()
	}

	bp.blockTracker.CleanupHeadersBehindNonce(
		shardID,
		selfNotarizedNonce,
		crossNotarizedNonce,
	)

	log.Trace("cleanupBlockTrackerPoolsForShard.CleanupHeadersBehindNonce",
		"shard", shardID,
		"self notarized nonce", selfNotarizedNonce,
		"cross notarized nonce", crossNotarizedNonce,
		"nonces to final", noncesToFinal)
}

func (bp *baseProcessor) prepareDataForBootStorer(args bootStorerDataArgs) {
	lastCrossNotarizedHeaders := bp.getLastCrossNotarizedHeaders()

	bootData := bootstrapStorage.BootstrapData{
		LastHeader:                 args.headerInfo,
		LastCrossNotarizedHeaders:  lastCrossNotarizedHeaders,
		LastSelfNotarizedHeaders:   args.lastSelfNotarizedHeaders,
		PendingMiniBlocks:          args.pendingMiniBlocks,
		ProcessedMiniBlocks:        args.processedMiniBlocks,
		HighestFinalBlockNonce:     args.highestFinalBlockNonce,
		NodesCoordinatorConfigKey:  args.nodesCoordinatorConfigKey,
		EpochStartTriggerConfigKey: args.epochStartTriggerConfigKey,
	}

	startTime := time.Now()

	err := bp.bootStorer.Put(int64(args.round), bootData)
	if err != nil {
		log.Warn("cannot save boot data in storage",
			"error", err.Error())
	}

	elapsedTime := time.Since(startTime)
	if elapsedTime >= core.PutInStorerMaxTime {
		log.Warn("saveDataForBootStorer", "elapsed time", elapsedTime)
	}
}

func (bp *baseProcessor) getLastCrossNotarizedHeaders() []bootstrapStorage.BootstrapHeaderInfo {
	lastCrossNotarizedHeaders := make([]bootstrapStorage.BootstrapHeaderInfo, 0, bp.shardCoordinator.NumberOfShards()+1)

	for shardID := uint32(0); shardID < bp.shardCoordinator.NumberOfShards(); shardID++ {
		bootstrapHeaderInfo := bp.getLastCrossNotarizedHeadersForShard(shardID)
		if bootstrapHeaderInfo != nil {
			lastCrossNotarizedHeaders = append(lastCrossNotarizedHeaders, *bootstrapHeaderInfo)
		}
	}

	bootstrapHeaderInfo := bp.getLastCrossNotarizedHeadersForShard(core.MetachainShardId)
	if bootstrapHeaderInfo != nil {
		lastCrossNotarizedHeaders = append(lastCrossNotarizedHeaders, *bootstrapHeaderInfo)
	}

	if len(lastCrossNotarizedHeaders) == 0 {
		return nil
	}

	return trimSliceBootstrapHeaderInfo(lastCrossNotarizedHeaders)
}

func (bp *baseProcessor) getLastCrossNotarizedHeadersForShard(shardID uint32) *bootstrapStorage.BootstrapHeaderInfo {
	lastCrossNotarizedHeader, lastCrossNotarizedHeaderHash, err := bp.blockTracker.GetLastCrossNotarizedHeader(shardID)
	if err != nil {
		log.Warn("getLastCrossNotarizedHeadersForShard",
			"shard", shardID,
			"error", err.Error())
		return nil
	}

	if lastCrossNotarizedHeader.GetNonce() == 0 {
		return nil
	}

	headerInfo := &bootstrapStorage.BootstrapHeaderInfo{
		ShardId: lastCrossNotarizedHeader.GetShardID(),
		Nonce:   lastCrossNotarizedHeader.GetNonce(),
		Hash:    lastCrossNotarizedHeaderHash,
	}

	return headerInfo
}

func (bp *baseProcessor) getLastSelfNotarizedHeaders() []bootstrapStorage.BootstrapHeaderInfo {
	lastSelfNotarizedHeaders := make([]bootstrapStorage.BootstrapHeaderInfo, 0, bp.shardCoordinator.NumberOfShards()+1)

	for shardID := uint32(0); shardID < bp.shardCoordinator.NumberOfShards(); shardID++ {
		bootstrapHeaderInfo := bp.getLastSelfNotarizedHeadersForShard(shardID)
		if bootstrapHeaderInfo != nil {
			lastSelfNotarizedHeaders = append(lastSelfNotarizedHeaders, *bootstrapHeaderInfo)
		}
	}

	bootstrapHeaderInfo := bp.getLastSelfNotarizedHeadersForShard(core.MetachainShardId)
	if bootstrapHeaderInfo != nil {
		lastSelfNotarizedHeaders = append(lastSelfNotarizedHeaders, *bootstrapHeaderInfo)
	}

	if len(lastSelfNotarizedHeaders) == 0 {
		return nil
	}

	return trimSliceBootstrapHeaderInfo(lastSelfNotarizedHeaders)
}

func (bp *baseProcessor) getLastSelfNotarizedHeadersForShard(shardID uint32) *bootstrapStorage.BootstrapHeaderInfo {
	lastSelfNotarizedHeader, lastSelfNotarizedHeaderHash, err := bp.blockTracker.GetLastSelfNotarizedHeader(shardID)
	if err != nil {
		log.Warn("getLastSelfNotarizedHeadersForShard",
			"shard", shardID,
			"error", err.Error())
		return nil
	}

	if lastSelfNotarizedHeader.GetNonce() == 0 {
		return nil
	}

	headerInfo := &bootstrapStorage.BootstrapHeaderInfo{
		ShardId: lastSelfNotarizedHeader.GetShardID(),
		Nonce:   lastSelfNotarizedHeader.GetNonce(),
		Hash:    lastSelfNotarizedHeaderHash,
	}

	return headerInfo
}

func deleteSelfReceiptsMiniBlocks(body *block.Body) *block.Body {
	newBody := &block.Body{}
	for _, mb := range body.MiniBlocks {
		isInShardUnsignedMB := mb.ReceiverShardID == mb.SenderShardID &&
			(mb.Type == block.ReceiptBlock || mb.Type == block.SmartContractResultBlock)
		if isInShardUnsignedMB {
			continue
		}

		newBody.MiniBlocks = append(newBody.MiniBlocks, mb)
	}

	return newBody
}

func (bp *baseProcessor) getNoncesToFinal(headerHandler data.HeaderHandler) uint64 {
	currentBlockNonce := bp.genesisNonce
	if !check.IfNil(headerHandler) {
		currentBlockNonce = headerHandler.GetNonce()
	}

	noncesToFinal := uint64(0)
	finalBlockNonce := bp.forkDetector.GetHighestFinalBlockNonce()
	if currentBlockNonce > finalBlockNonce {
		noncesToFinal = currentBlockNonce - finalBlockNonce
	}

	return noncesToFinal
}

// DecodeBlockBody method decodes block body from a given byte array
func (bp *baseProcessor) DecodeBlockBody(dta []byte) data.BodyHandler {
	body := &block.Body{}
	if dta == nil {
		return body
	}

	err := bp.marshalizer.Unmarshal(body, dta)
	if err != nil {
		log.Debug("DecodeBlockBody.Unmarshal", "error", err.Error())
		return nil
	}

	return body
}

// DecodeBlockHeader method decodes block header from a given byte array
func (bp *baseProcessor) DecodeBlockHeader(dta []byte) data.HeaderHandler {
	if dta == nil {
		return nil
	}

	header := bp.blockChain.CreateNewHeader()

	err := bp.marshalizer.Unmarshal(header, dta)
	if err != nil {
		log.Debug("DecodeBlockHeader.Unmarshal", "error", err.Error())
		return nil
	}

	return header
}

func (bp *baseProcessor) saveBody(body *block.Body, header data.HeaderHandler, headerHash []byte) {
	startTime := time.Now()

	errNotCritical := bp.txCoordinator.SaveTxsToStorage(body)
	if errNotCritical != nil {
		log.Warn("saveBody.SaveTxsToStorage", "error", errNotCritical.Error())
	}
	log.Trace("saveBody.SaveTxsToStorage", "time", time.Since(startTime))

	var marshalizedMiniBlock []byte
	for i := 0; i < len(body.MiniBlocks); i++ {
		marshalizedMiniBlock, errNotCritical = bp.marshalizer.Marshal(body.MiniBlocks[i])
		if errNotCritical != nil {
			log.Warn("saveBody.Marshal", "error", errNotCritical.Error())
			continue
		}

		miniBlockHash := bp.hasher.Compute(string(marshalizedMiniBlock))
		errNotCritical = bp.store.Put(dataRetriever.MiniBlockUnit, miniBlockHash, marshalizedMiniBlock)
		if errNotCritical != nil {
			log.Warn("saveBody.Put -> MiniBlockUnit", "error", errNotCritical.Error())
		}
		log.Trace("saveBody.Put -> MiniBlockUnit", "time", time.Since(startTime))
	}

	marshalizedReceipts, errNotCritical := bp.txCoordinator.CreateMarshalizedReceipts()
	if errNotCritical != nil {
		log.Warn("saveBody.CreateMarshalizedReceipts", "error", errNotCritical.Error())
	} else {
		if len(marshalizedReceipts) > 0 {
			errNotCritical = bp.store.Put(dataRetriever.ReceiptsUnit, header.GetReceiptsHash(), marshalizedReceipts)
			if errNotCritical != nil {
				log.Warn("saveBody.Put -> ReceiptsUnit", "error", errNotCritical.Error())
			}
		}
	}

	scheduledRootHash := bp.scheduledTxsExecutionHandler.GetScheduledRootHash()
	mapScheduledSCRs := bp.scheduledTxsExecutionHandler.GetScheduledSCRs()
	marshalizedRootHashAndScheduledSCRs, errNotCritical := bp.getMarshalizedScheduledRootHashAndSCRs(scheduledRootHash, mapScheduledSCRs)
	if errNotCritical != nil {
		log.Warn("saveBody.getMarshalizedScheduledSCRs", "error", errNotCritical.Error())
	} else {
		if len(marshalizedRootHashAndScheduledSCRs) > 0 {
			errNotCritical = bp.store.Put(dataRetriever.ScheduledSCRsUnit, headerHash, marshalizedRootHashAndScheduledSCRs)
			if errNotCritical != nil {
				log.Warn("saveBody.Put -> ScheduledSCRsUnit", "error", errNotCritical.Error())
			}
		}
	}

	elapsedTime := time.Since(startTime)
	if elapsedTime >= core.PutInStorerMaxTime {
		log.Warn("saveBody", "elapsed time", elapsedTime)
	}
}

func (bp *baseProcessor) saveShardHeader(header data.HeaderHandler, headerHash []byte, marshalizedHeader []byte) {
	startTime := time.Now()

	nonceToByteSlice := bp.uint64Converter.ToByteSlice(header.GetNonce())
	hdrNonceHashDataUnit := dataRetriever.ShardHdrNonceHashDataUnit + dataRetriever.UnitType(header.GetShardID())

	errNotCritical := bp.store.Put(hdrNonceHashDataUnit, nonceToByteSlice, headerHash)
	if errNotCritical != nil {
		log.Warn(fmt.Sprintf("saveHeader.Put -> ShardHdrNonceHashDataUnit_%d", header.GetShardID()),
			"error", errNotCritical.Error(),
		)
	}

	errNotCritical = bp.store.Put(dataRetriever.BlockHeaderUnit, headerHash, marshalizedHeader)
	if errNotCritical != nil {
		log.Warn("saveHeader.Put -> BlockHeaderUnit", "error", errNotCritical.Error())
	}

	elapsedTime := time.Since(startTime)
	if elapsedTime >= core.PutInStorerMaxTime {
		log.Warn("saveShardHeader", "elapsed time", elapsedTime)
	}
}

func (bp *baseProcessor) saveMetaHeader(header data.HeaderHandler, headerHash []byte, marshalizedHeader []byte) {
	startTime := time.Now()

	nonceToByteSlice := bp.uint64Converter.ToByteSlice(header.GetNonce())

	errNotCritical := bp.store.Put(dataRetriever.MetaHdrNonceHashDataUnit, nonceToByteSlice, headerHash)
	if errNotCritical != nil {
		log.Warn("saveMetaHeader.Put -> MetaHdrNonceHashDataUnit", "error", errNotCritical.Error())
	}

	errNotCritical = bp.store.Put(dataRetriever.MetaBlockUnit, headerHash, marshalizedHeader)
	if errNotCritical != nil {
		log.Warn("saveMetaHeader.Put -> MetaBlockUnit", "error", errNotCritical.Error())
	}

	elapsedTime := time.Since(startTime)
	if elapsedTime >= core.PutInStorerMaxTime {
		log.Warn("saveMetaHeader", "elapsed time", elapsedTime)
	}
}

func getLastSelfNotarizedHeaderByItself(chainHandler data.ChainHandler) (data.HeaderHandler, []byte) {
	currentHeader := chainHandler.GetCurrentBlockHeader()
	if check.IfNil(currentHeader) {
		return chainHandler.GetGenesisHeader(), chainHandler.GetGenesisHeaderHash()
	}

	currentBlockHash := chainHandler.GetCurrentBlockHeaderHash()

	return currentHeader, currentBlockHash
}

func (bp *baseProcessor) updateStateStorage(
	finalHeader data.HeaderHandler,
	currRootHash []byte,
	prevRootHash []byte,
	accounts state.AccountsAdapter,
	statePruningQueue core.Queue,
) {
	if !accounts.IsPruningEnabled() {
		return
	}

	// TODO generate checkpoint on a trigger
	if bp.stateCheckpointModulus != 0 {
		if finalHeader.GetNonce()%uint64(bp.stateCheckpointModulus) == 0 {
			log.Debug("trie checkpoint", "currRootHash", currRootHash)
			accounts.SetStateCheckpoint(currRootHash)
		}
	}

	if bytes.Equal(prevRootHash, currRootHash) {
		return
	}

	rootHashToBePruned := statePruningQueue.Add(prevRootHash)
	if len(rootHashToBePruned) == 0 {
		return
	}

	accounts.CancelPrune(rootHashToBePruned, data.NewRoot)
	accounts.PruneTrie(rootHashToBePruned, data.OldRoot)
}

// RevertAccountState reverts the account state for cleanup failed process
func (bp *baseProcessor) RevertAccountState() {
	for key := range bp.accountsDB {
		err := bp.accountsDB[key].RevertToSnapshot(0)
		if err != nil {
			log.Debug("RevertToSnapshot", "error", err.Error())
		}
	}

	bp.revertScheduledRootHashAndSCRs()
}

func (bp *baseProcessor) revertScheduledRootHashAndSCRs() {
	header, headerHash := bp.getCurrentHeaderAndHash()
	process.SetScheduledRootHashAndSCRs(
		headerHash,
		header.GetRootHash(),
		make(map[block.Type][]data.TransactionHandler),
		bp.store,
		bp.marshalizer,
		bp.scheduledTxsExecutionHandler)
}

func (bp *baseProcessor) getCurrentHeaderAndHash() (data.HeaderHandler, []byte) {
	headerHandler := bp.blockChain.GetCurrentBlockHeader()
	headerHash := bp.blockChain.GetCurrentBlockHeaderHash()
	if check.IfNil(headerHandler) {
		headerHandler = bp.blockChain.GetGenesisHeader()
		headerHash = bp.blockChain.GetGenesisHeaderHash()
	}

	return headerHandler, headerHash
}

// GetAccountsDBSnapshot returns the account snapshot
func (bp *baseProcessor) GetAccountsDBSnapshot() map[state.AccountsDbIdentifier]int {
	snapshots := make(map[state.AccountsDbIdentifier]int)
	for key := range bp.accountsDB {
		snapshots[key] = bp.accountsDB[key].JournalLen()
	}

	return snapshots
}

// RevertAccountsDBToSnapshot reverts the accountsDB to the given snapshot
func (bp *baseProcessor) RevertAccountsDBToSnapshot(accountsSnapshot map[state.AccountsDbIdentifier]int) {
	for key := range bp.accountsDB {
		err := bp.accountsDB[key].RevertToSnapshot(accountsSnapshot[key])
		if err != nil {
			log.Debug("RevertAccountsDBToSnapshot", "error", err.Error())
		}
	}
}

func (bp *baseProcessor) commitAll() error {
	for key := range bp.accountsDB {
		_, err := bp.accountsDB[key].Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

// PruneStateOnRollback recreates the state tries to the root hashes indicated by the provided headers
func (bp *baseProcessor) PruneStateOnRollback(currHeader data.HeaderHandler, currHeaderHash []byte, prevHeader data.HeaderHandler, prevHeaderHash []byte) {
	for key := range bp.accountsDB {
		if !bp.accountsDB[key].IsPruningEnabled() {
			continue
		}

		rootHash, prevRootHash := bp.getRootHashes(currHeader, prevHeader, key)

		if key == state.UserAccountsState {
			rootHash = process.GetScheduledRootHash(currHeaderHash, rootHash, bp.store, bp.marshalizer)
			prevRootHash = process.GetScheduledRootHash(prevHeaderHash, prevRootHash, bp.store, bp.marshalizer)
		}

		if bytes.Equal(rootHash, prevRootHash) {
			continue
		}

		bp.accountsDB[key].CancelPrune(prevRootHash, data.OldRoot)
		bp.accountsDB[key].PruneTrie(rootHash, data.NewRoot)
	}
}

func (bp *baseProcessor) getRootHashes(currHeader data.HeaderHandler, prevHeader data.HeaderHandler, identifier state.AccountsDbIdentifier) ([]byte, []byte) {
	switch identifier {
	case state.UserAccountsState:
		return currHeader.GetRootHash(), prevHeader.GetRootHash()
	case state.PeerAccountsState:
		currMetaHeader, ok := currHeader.(data.MetaHeaderHandler)
		if !ok {
			return []byte{}, []byte{}
		}
		prevMetaHeader, ok := prevHeader.(data.MetaHeaderHandler)
		if !ok {
			return []byte{}, []byte{}
		}
		return currMetaHeader.GetValidatorStatsRootHash(), prevMetaHeader.GetValidatorStatsRootHash()
	default:
		return []byte{}, []byte{}
	}
}

func (bp *baseProcessor) displayMiniBlocksPool() {
	miniBlocksPool := bp.dataPool.MiniBlocks()

	for _, hash := range miniBlocksPool.Keys() {
		value, ok := miniBlocksPool.Get(hash)
		if !ok {
			log.Debug("displayMiniBlocksPool: mini block not found", "hash", logger.DisplayByteSlice(hash))
			continue
		}

		miniBlock, ok := value.(*block.MiniBlock)
		if !ok {
			log.Debug("displayMiniBlocksPool: wrong type assertion", "hash", logger.DisplayByteSlice(hash))
			continue
		}

		log.Trace("mini block in pool",
			"hash", logger.DisplayByteSlice(hash),
			"type", miniBlock.Type,
			"sender", miniBlock.SenderShardID,
			"receiver", miniBlock.ReceiverShardID,
			"num txs", len(miniBlock.TxHashes))
	}
}

// trimSliceBootstrapHeaderInfo creates a copy of the provided slice without the excess capacity
func trimSliceBootstrapHeaderInfo(in []bootstrapStorage.BootstrapHeaderInfo) []bootstrapStorage.BootstrapHeaderInfo {
	if len(in) == 0 {
		return []bootstrapStorage.BootstrapHeaderInfo{}
	}
	ret := make([]bootstrapStorage.BootstrapHeaderInfo, len(in))
	copy(ret, in)
	return ret
}

func (bp *baseProcessor) restoreBlockBody(bodyHandler data.BodyHandler) {
	if check.IfNil(bodyHandler) {
		log.Debug("restoreMiniblocks nil bodyHandler")
		return
	}

	body, ok := bodyHandler.(*block.Body)
	if !ok {
		log.Debug("restoreMiniblocks wrong type assertion for bodyHandler")
		return
	}

	restoredTxNr, errNotCritical := bp.txCoordinator.RestoreBlockDataFromStorage(body)
	if errNotCritical != nil {
		log.Debug("restoreBlockBody RestoreBlockDataFromStorage", "error", errNotCritical.Error())
	}

	go bp.txCounter.subtractRestoredTxs(restoredTxNr)
}

func (bp *baseProcessor) requestMiniBlocksIfNeeded(headerHandler data.HeaderHandler) {
	lastCrossNotarizedHeader, _, err := bp.blockTracker.GetLastCrossNotarizedHeader(headerHandler.GetShardID())
	if err != nil {
		log.Debug("requestMiniBlocksIfNeeded.GetLastCrossNotarizedHeader",
			"shard", headerHandler.GetShardID(),
			"error", err.Error())
		return
	}

	isHeaderOutOfRequestRange := headerHandler.GetNonce() > lastCrossNotarizedHeader.GetNonce()+process.MaxHeadersToRequestInAdvance
	if isHeaderOutOfRequestRange {
		return
	}

	waitTime := core.ExtraDelayForRequestBlockInfo
	roundDifferences := bp.roundHandler.Index() - int64(headerHandler.GetRound())
	if roundDifferences > 1 {
		waitTime = 0
	}

	// waiting for late broadcast of mini blocks and transactions to be done and received
	time.Sleep(waitTime)

	bp.txCoordinator.RequestMiniBlocks(headerHandler)
}

func (bp *baseProcessor) recordBlockInHistory(blockHeaderHash []byte, blockHeader data.HeaderHandler, blockBody data.BodyHandler) {
	scrResultsFromPool := bp.txCoordinator.GetAllCurrentUsedTxs(block.SmartContractResultBlock)
	receiptsFromPool := bp.txCoordinator.GetAllCurrentUsedTxs(block.ReceiptBlock)

	err := bp.historyRepo.RecordBlock(blockHeaderHash, blockHeader, blockBody, scrResultsFromPool, receiptsFromPool)
	if err != nil {
		log.Error("historyRepo.RecordBlock()", "blockHeaderHash", blockHeaderHash, "error", err.Error())
	}
}

func (bp *baseProcessor) addHeaderIntoTrackerPool(nonce uint64, shardID uint32) {
	headersPool := bp.dataPool.Headers()
	headers, hashes, err := headersPool.GetHeadersByNonceAndShardId(nonce, shardID)
	if err != nil {
		log.Trace("baseProcessor.addHeaderIntoTrackerPool", "error", err.Error())
		return
	}

	for i := 0; i < len(headers); i++ {
		bp.blockTracker.AddTrackedHeader(headers[i], hashes[i])
	}
}

func (bp *baseProcessor) commitTrieEpochRootHashIfNeeded(metaBlock *block.MetaBlock, rootHash []byte) error {
	trieEpochRootHashStorageUnit := bp.store.GetStorer(dataRetriever.TrieEpochRootHashUnit)
	if check.IfNil(trieEpochRootHashStorageUnit) {
		return nil
	}
	_, isStorerDisabled := trieEpochRootHashStorageUnit.(*storageUnit.NilStorer)
	if isStorerDisabled {
		return nil
	}

	userAccountsDb := bp.accountsDB[state.UserAccountsState]
	if userAccountsDb == nil {
		return fmt.Errorf("%w for user accounts state", process.ErrNilAccountsAdapter)
	}

	epochBytes := bp.uint64Converter.ToByteSlice(uint64(metaBlock.Epoch))

	err := trieEpochRootHashStorageUnit.Put(epochBytes, rootHash)
	if err != nil {
		return err
	}

	allLeavesChan, err := userAccountsDb.GetAllLeaves(rootHash)
	if err != nil {
		return err
	}

	balanceSum := big.NewInt(0)
	for leaf := range allLeavesChan {
		userAccount, errUnmarshal := unmarshalUserAccount(leaf.Key(), leaf.Value(), bp.marshalizer)
		if errUnmarshal != nil {
			log.Trace("cannot unmarshal user account. it may be a code leaf", "error", errUnmarshal)
			continue
		}
		balanceSum.Add(balanceSum, userAccount.GetBalance())
	}

	log.Debug("sum of addresses in shard at epoch start",
		"shard", bp.shardCoordinator.SelfId(),
		"epoch", metaBlock.Epoch,
		"sum", balanceSum.String())

	return nil
}

func unmarshalUserAccount(address []byte, userAccountsBytes []byte, marshalizer marshal.Marshalizer) (state.UserAccountHandler, error) {
	userAccount, err := state.NewUserAccount(address)
	if err != nil {
		return nil, err
	}
	err = marshalizer.Unmarshal(userAccount, userAccountsBytes)
	if err != nil {
		return nil, err
	}

	return userAccount, nil
}

// Close - closes all underlying components
func (bp *baseProcessor) Close() error {
	var err1, err2 error
	if !check.IfNil(bp.vmContainer) {
		err1 = bp.vmContainer.Close()
	}
	if !check.IfNil(bp.vmContainerFactory) {
		err2 = bp.vmContainerFactory.Close()
	}
	if err1 != nil || err2 != nil {
		return fmt.Errorf("vmContainer close error: %v, vmContainerFactory close error: %v", err1, err2)
	}

	return nil
}

// ProcessScheduledBlock processes a scheduled block
func (bp *baseProcessor) ProcessScheduledBlock(_ data.HeaderHandler, _ data.BodyHandler, haveTime func() time.Duration) error {
	var err error
	defer func() {
		if err != nil {
			// TODO: check if possible to revert only the scheduled
			bp.RevertAccountState()
		}
	}()

	startTime := time.Now()
	err = bp.scheduledTxsExecutionHandler.ExecuteAll(haveTime)
	elapsedTime := time.Since(startTime)
	log.Debug("elapsed time to execute all scheduled transactions",
		"time [s]", elapsedTime,
	)
	if err != nil {
		return err
	}

	rootHash, err := bp.accountsDB[state.UserAccountsState].RootHash()
	if err != nil {
		return err
	}

	bp.scheduledTxsExecutionHandler.SetScheduledRootHash(rootHash)

	return nil
}

func (bp *baseProcessor) getMarshalizedScheduledRootHashAndSCRs(
	scheduledRootHash []byte,
	mapScheduledSCRs map[block.Type][]data.TransactionHandler,
) ([]byte, error) {
	scrsBatch := &batch.Batch{}
	scrsBatch.Data = append(scrsBatch.Data, scheduledRootHash)
	for blockType, scrs := range mapScheduledSCRs {
		if len(scrs) == 0 {
			continue
		}

		scheduledSCRs := &process.ScheduledSCRs{
			BlockType:  blockType,
			TxHandlers: make([]data.TransactionHandler, len(scrs)),
		}

		for scrIndex, scr := range scrs {
			scheduledSCRs.TxHandlers[scrIndex] = scr
		}

		marshalizedScheduledSCRs, err := bp.marshalizer.Marshal(scheduledSCRs)
		if err != nil {
			return nil, err
		}

		scrsBatch.Data = append(scrsBatch.Data, marshalizedScheduledSCRs)
	}

	if len(scrsBatch.Data) <= 1 {
		return make([]byte, 0), nil
	}

	return bp.marshalizer.Marshal(scrsBatch)
}
