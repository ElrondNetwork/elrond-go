package sync

import (
	"context"
	"strings"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/storage"
)

// MetaBootstrap implements the bootstrap mechanism
type MetaBootstrap struct {
	*baseBootstrap
	epochBootstrapper           process.EpochBootstrapper
	validatorStatisticsDBSyncer process.AccountsDBSyncer
	validatorAccountsDB         state.AccountsAdapter
}

// NewMetaBootstrap creates a new Bootstrap object
func NewMetaBootstrap(arguments ArgMetaBootstrapper) (*MetaBootstrap, error) {
	if check.IfNil(arguments.PoolsHolder) {
		return nil, process.ErrNilPoolsHolder
	}
	if check.IfNil(arguments.PoolsHolder.Headers()) {
		return nil, process.ErrNilMetaBlocksPool
	}
	if check.IfNil(arguments.EpochBootstrapper) {
		return nil, process.ErrNilEpochStartTrigger
	}
	if check.IfNil(arguments.EpochHandler) {
		return nil, process.ErrNilEpochHandler
	}
	if check.IfNil(arguments.ValidatorStatisticsDBSyncer) {
		return nil, process.ErrNilAccountsDBSyncer
	}
	if check.IfNil(arguments.ValidatorAccountsDB) {
		return nil, process.ErrNilPeerAccountsAdapter
	}

	err := checkBootstrapNilParameters(arguments.ArgBaseBootstrapper)
	if err != nil {
		return nil, err
	}

	base := &baseBootstrap{
		chainHandler:         arguments.ChainHandler,
		blockProcessor:       arguments.BlockProcessor,
		store:                arguments.Store,
		headers:              arguments.PoolsHolder.Headers(),
		roundHandler:         arguments.RoundHandler,
		waitTime:             arguments.WaitTime,
		hasher:               arguments.Hasher,
		marshalizer:          arguments.Marshalizer,
		forkDetector:         arguments.ForkDetector,
		requestHandler:       arguments.RequestHandler,
		shardCoordinator:     arguments.ShardCoordinator,
		accounts:             arguments.Accounts,
		blackListHandler:     arguments.BlackListHandler,
		networkWatcher:       arguments.NetworkWatcher,
		bootStorer:           arguments.BootStorer,
		storageBootstrapper:  arguments.StorageBootstrapper,
		epochHandler:         arguments.EpochHandler,
		miniBlocksProvider:   arguments.MiniblocksProvider,
		uint64Converter:      arguments.Uint64Converter,
		poolsHolder:          arguments.PoolsHolder,
		statusHandler:        arguments.AppStatusHandler,
		outportHandler:       arguments.OutportHandler,
		accountsDBSyncer:     arguments.AccountsDBSyncer,
		currentEpochProvider: arguments.CurrentEpochProvider,
		isInImportMode:       arguments.IsInImportMode,
	}

	if base.isInImportMode {
		log.Warn("using always-not-synced status because the node is running in import-db")
	}

	boot := MetaBootstrap{
		baseBootstrap:               base,
		epochBootstrapper:           arguments.EpochBootstrapper,
		validatorStatisticsDBSyncer: arguments.ValidatorStatisticsDBSyncer,
		validatorAccountsDB:         arguments.ValidatorAccountsDB,
	}

	base.blockBootstrapper = &boot
	base.syncStarter = &boot
	base.getHeaderFromPool = boot.getMetaHeaderFromPool
	base.requestMiniBlocks = boot.requestMiniBlocksFromHeaderWithNonceIfMissing

	//placed in struct fields for performance reasons
	base.headerStore = boot.store.GetStorer(dataRetriever.MetaBlockUnit)
	base.headerNonceHashStore = boot.store.GetStorer(dataRetriever.MetaHdrNonceHashDataUnit)

	base.init()

	return &boot, nil
}

func (boot *MetaBootstrap) getBlockBody(headerHandler data.HeaderHandler) (data.BodyHandler, error) {
	header, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	hashes := make([][]byte, len(header.MiniBlockHeaders))
	for i := 0; i < len(header.MiniBlockHeaders); i++ {
		hashes[i] = header.MiniBlockHeaders[i].Hash
	}

	miniBlocksAndHashes, missingMiniBlocksHashes := boot.miniBlocksProvider.GetMiniBlocks(hashes)
	if len(missingMiniBlocksHashes) > 0 {
		return nil, process.ErrMissingBody
	}

	miniBlocks := make([]*block.MiniBlock, len(miniBlocksAndHashes))
	for index, miniBlockAndHash := range miniBlocksAndHashes {
		miniBlocks[index] = miniBlockAndHash.Miniblock
	}

	return &block.Body{MiniBlocks: miniBlocks}, nil
}

// StartSyncingBlocks method will start syncing blocks as a go routine
func (boot *MetaBootstrap) StartSyncingBlocks() {
	// when a node starts it first tries to bootstrap from storage, if there already exist a database saved
	errNotCritical := boot.storageBootstrapper.LoadFromStorage()
	if errNotCritical != nil {
		log.Debug("syncFromStorer", "error", errNotCritical.Error())
	} else {
		_, numHdrs := updateMetricsFromStorage(boot.store, boot.uint64Converter, boot.marshalizer, boot.statusHandler, boot.storageBootstrapper.GetHighestBlockNonce())
		boot.blockProcessor.SetNumProcessedObj(numHdrs)

		boot.setLastEpochStartRound()
	}

	var ctx context.Context
	ctx, boot.cancelFunc = context.WithCancel(context.Background())
	go boot.syncBlocks(ctx)
}

func (boot *MetaBootstrap) setLastEpochStartRound() {
	hdr := boot.chainHandler.GetCurrentBlockHeader()
	if check.IfNil(hdr) || hdr.GetEpoch() < 1 {
		return
	}

	epochIdentifier := core.EpochStartIdentifier(hdr.GetEpoch())
	epochStartHdr, err := boot.headerStore.Get([]byte(epochIdentifier))
	if err != nil {
		return
	}

	epochStartMetaBlock := &block.MetaBlock{}
	err = boot.marshalizer.Unmarshal(epochStartMetaBlock, epochStartHdr)
	if err != nil {
		return
	}

	boot.epochBootstrapper.SetCurrentEpochStartRound(epochStartMetaBlock.GetRound())
}

// SyncBlock method actually does the synchronization. It requests the next block header from the pool
// and if it is not found there it will be requested from the network. After the header is received,
// it requests the block body in the same way(pool and than, if it is not found in the pool, from network).
// If either header and body are received the ProcessBlock and CommitBlock method will be called successively.
// These methods will execute the block and its transactions. Finally if everything works, the block will be committed
// in the blockchain, and all this mechanism will be reiterated for the next block.
func (boot *MetaBootstrap) SyncBlock() error {
	err := boot.syncBlock()
	isErrGetNodeFromDB := err != nil && strings.Contains(err.Error(), core.GetNodeFromDBErrorString)
	if isErrGetNodeFromDB {
		errSync := boot.syncAccountsDBs()
		if errSync != nil {
			log.Debug("SyncBlock syncTrie", "error", errSync)
		}
	}

	return err
}

func (boot *MetaBootstrap) syncAccountsDBs() error {
	var err error

	err = boot.syncValidatorAccountsState()
	if err != nil {
		return err
	}

	err = boot.syncUserAccountsState()
	if err != nil {
		return err
	}

	return nil
}

func (boot *MetaBootstrap) syncValidatorAccountsState() error {
	rootHash, err := boot.validatorAccountsDB.RootHash()
	if err != nil {
		return err
	}

	log.Warn("base sync: started syncValidatorAccountsState")
	return boot.validatorStatisticsDBSyncer.SyncAccounts(rootHash)
}

// Close closes the synchronization loop
func (boot *MetaBootstrap) Close() error {
	if check.IfNil(boot.baseBootstrap) {
		return nil
	}

	return boot.baseBootstrap.Close()
}

// requestHeaderWithNonce method requests a block header from network when it is not found in the pool
func (boot *MetaBootstrap) requestHeaderWithNonce(nonce uint64) {
	boot.setRequestedHeaderNonce(&nonce)
	log.Debug("requesting meta header from network",
		"nonce", nonce,
		"probable highest nonce", boot.forkDetector.ProbableHighestNonce(),
	)
	boot.requestHandler.RequestMetaHeaderByNonce(nonce)
}

// requestHeaderWithHash method requests a block header from network when it is not found in the pool
func (boot *MetaBootstrap) requestHeaderWithHash(hash []byte) {
	boot.setRequestedHeaderHash(hash)
	log.Debug("requesting meta header from network",
		"hash", hash,
		"probable highest nonce", boot.forkDetector.ProbableHighestNonce(),
	)
	boot.requestHandler.RequestMetaHeader(hash)
}

// getHeaderWithNonceRequestingIfMissing method gets the header with a given nonce from pool. If it is not found there, it will
// be requested from network
func (boot *MetaBootstrap) getHeaderWithNonceRequestingIfMissing(nonce uint64) (data.HeaderHandler, error) {
	hdr, _, err := process.GetMetaHeaderFromPoolWithNonce(
		nonce,
		boot.headers)
	if err != nil {
		_ = core.EmptyChannel(boot.chRcvHdrNonce)
		boot.requestHeaderWithNonce(nonce)
		err = boot.waitForHeaderNonce()
		if err != nil {
			return nil, err
		}

		hdr, _, err = process.GetMetaHeaderFromPoolWithNonce(
			nonce,
			boot.headers)
		if err != nil {
			return nil, err
		}
	}

	return hdr, nil
}

// getHeaderWithHashRequestingIfMissing method gets the header with a given hash from pool. If it is not found there,
// it will be requested from network
func (boot *MetaBootstrap) getHeaderWithHashRequestingIfMissing(hash []byte) (data.HeaderHandler, error) {
	hdr, err := process.GetMetaHeader(hash, boot.headers, boot.marshalizer, boot.store)
	if err != nil {
		_ = core.EmptyChannel(boot.chRcvHdrHash)
		boot.requestHeaderWithHash(hash)
		err = boot.waitForHeaderHash()
		if err != nil {
			return nil, err
		}

		hdr, err = process.GetMetaHeaderFromPool(hash, boot.headers)
		if err != nil {
			return nil, err
		}
	}

	return hdr, nil
}

func (boot *MetaBootstrap) getPrevHeader(
	header data.HeaderHandler,
	headerStore storage.Storer,
) (data.HeaderHandler, error) {

	prevHash := header.GetPrevHash()
	buffHeader, err := headerStore.Get(prevHash)
	if err != nil {
		return nil, err
	}

	prevHeader := &block.MetaBlock{}
	err = boot.marshalizer.Unmarshal(prevHeader, buffHeader)
	if err != nil {
		return nil, err
	}

	return prevHeader, nil
}

func (boot *MetaBootstrap) getCurrHeader() (data.HeaderHandler, error) {
	blockHeader := boot.chainHandler.GetCurrentBlockHeader()
	if check.IfNil(blockHeader) {
		return nil, process.ErrNilBlockHeader
	}

	header, ok := blockHeader.(*block.MetaBlock)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	return header, nil
}

func (boot *MetaBootstrap) haveHeaderInPoolWithNonce(nonce uint64) bool {
	_, _, err := process.GetMetaHeaderFromPoolWithNonce(
		nonce,
		boot.headers)

	return err == nil
}

func (boot *MetaBootstrap) getMetaHeaderFromPool(headerHash []byte) (data.HeaderHandler, error) {
	return process.GetMetaHeaderFromPool(headerHash, boot.headers)
}

func (boot *MetaBootstrap) getBlockBodyRequestingIfMissing(headerHandler data.HeaderHandler) (data.BodyHandler, error) {
	header, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	hashes := make([][]byte, len(header.MiniBlockHeaders))
	for i := 0; i < len(header.MiniBlockHeaders); i++ {
		hashes[i] = header.MiniBlockHeaders[i].Hash
	}

	boot.setRequestedMiniBlocks(nil)

	miniBlockSlice, err := boot.getMiniBlocksRequestingIfMissing(hashes)
	if err != nil {
		return nil, err
	}

	blockBody := &block.Body{MiniBlocks: miniBlockSlice}

	return blockBody, nil
}

func (boot *MetaBootstrap) requestMiniBlocksFromHeaderWithNonceIfMissing(headerHandler data.HeaderHandler) {
	nextBlockNonce := boot.getNonceForNextBlock()
	maxNonce := core.MinUint64(nextBlockNonce+process.MaxHeadersToRequestInAdvance-1, boot.forkDetector.ProbableHighestNonce())
	if headerHandler.GetNonce() < nextBlockNonce || headerHandler.GetNonce() > maxNonce {
		return
	}

	header, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		log.Warn("cannot convert headerHandler in block.MetaBlock")
		return
	}

	hashes := make([][]byte, 0)
	for i := 0; i < len(header.MiniBlockHeaders); i++ {
		hashes = append(hashes, header.MiniBlockHeaders[i].Hash)
	}

	_, missingMiniBlocksHashes := boot.miniBlocksProvider.GetMiniBlocksFromPool(hashes)
	if len(missingMiniBlocksHashes) > 0 {
		log.Trace("requesting in advance mini blocks",
			"num miniblocks", len(missingMiniBlocksHashes),
			"header nonce", header.Nonce,
		)
		boot.requestHandler.RequestMiniBlocks(boot.shardCoordinator.SelfId(), missingMiniBlocksHashes)
	}
}

func (boot *MetaBootstrap) isForkTriggeredByMeta() bool {
	return false
}

func (boot *MetaBootstrap) requestHeaderByNonce(nonce uint64) {
	boot.requestHandler.RequestMetaHeaderByNonce(nonce)
}
