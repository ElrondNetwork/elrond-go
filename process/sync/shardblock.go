package sync

import (
	"context"
	"math"
	"strings"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/storage"
)

// ShardBootstrap implements the bootstrap mechanism
type ShardBootstrap struct {
	*baseBootstrap
}

// NewShardBootstrap creates a new Bootstrap object
func NewShardBootstrap(arguments ArgShardBootstrapper) (*ShardBootstrap, error) {
	if check.IfNil(arguments.PoolsHolder) {
		return nil, process.ErrNilPoolsHolder
	}
	if check.IfNil(arguments.PoolsHolder.Headers()) {
		return nil, process.ErrNilHeadersDataPool
	}
	if check.IfNil(arguments.PoolsHolder.MiniBlocks()) {
		return nil, process.ErrNilTxBlockBody
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

	boot := ShardBootstrap{
		baseBootstrap: base,
	}

	base.blockBootstrapper = &boot
	base.syncStarter = &boot
	base.getHeaderFromPool = boot.getShardHeaderFromPool
	base.requestMiniBlocks = boot.requestMiniBlocksFromHeaderWithNonceIfMissing

	//placed in struct fields for performance reasons
	base.headerStore = boot.store.GetStorer(dataRetriever.BlockHeaderUnit)
	hdrNonceHashDataUnit := dataRetriever.ShardHdrNonceHashDataUnit + dataRetriever.UnitType(boot.shardCoordinator.SelfId())
	base.headerNonceHashStore = boot.store.GetStorer(hdrNonceHashDataUnit)

	base.init()

	return &boot, nil
}

func (boot *ShardBootstrap) getBlockBody(headerHandler data.HeaderHandler) (data.BodyHandler, error) {
	header, ok := headerHandler.(*block.Header)
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
func (boot *ShardBootstrap) StartSyncingBlocks() {
	errNotCritical := boot.storageBootstrapper.LoadFromStorage()
	if errNotCritical != nil {
		log.Debug("boot.syncFromStorer",
			"error", errNotCritical.Error(),
		)
	} else {
		numTxs, _ := updateMetricsFromStorage(boot.store, boot.uint64Converter, boot.marshalizer, boot.statusHandler, boot.storageBootstrapper.GetHighestBlockNonce())
		boot.blockProcessor.SetNumProcessedObj(numTxs)
	}

	var ctx context.Context
	ctx, boot.cancelFunc = context.WithCancel(context.Background())
	go boot.syncBlocks(ctx)
}

// SyncBlock method actually does the synchronization. It requests the next block header from the pool
// and if it is not found there it will be requested from the network. After the header is received,
// it requests the block body in the same way(pool and than, if it is not found in the pool, from network).
// If either header and body are received the ProcessBlock and CommitBlock method will be called successively.
// These methods will execute the block and its transactions. Finally if everything works, the block will be committed
// in the blockchain, and all this mechanism will be reiterated for the next block.
func (boot *ShardBootstrap) SyncBlock() error {
	err := boot.syncBlock()
	isErrGetNodeFromDB := err != nil && strings.Contains(err.Error(), core.GetNodeFromDBErrorString)
	if isErrGetNodeFromDB {
		errSync := boot.syncUserAccountsState()
		if errSync != nil {
			log.Debug("SyncBlock syncTrie", "error", errSync)
		}
	}

	return err
}

// Close closes the synchronization loop
func (boot *ShardBootstrap) Close() error {
	if check.IfNil(boot.baseBootstrap) {
		return nil
	}

	return boot.baseBootstrap.Close()
}

// requestHeaderWithNonce method requests a block header from network when it is not found in the pool
func (boot *ShardBootstrap) requestHeaderWithNonce(nonce uint64) {
	boot.setRequestedHeaderNonce(&nonce)
	log.Debug("requesting shard header from network",
		"nonce", nonce,
		"probable highest nonce", boot.forkDetector.ProbableHighestNonce(),
	)
	boot.requestHandler.RequestShardHeaderByNonce(boot.shardCoordinator.SelfId(), nonce)
}

// requestHeaderWithHash method requests a block header from network when it is not found in the pool
func (boot *ShardBootstrap) requestHeaderWithHash(hash []byte) {
	boot.setRequestedHeaderHash(hash)
	log.Debug("requesting shard header from network",
		"hash", hash,
		"probable highest nonce", boot.forkDetector.ProbableHighestNonce(),
	)
	boot.requestHandler.RequestShardHeader(boot.shardCoordinator.SelfId(), hash)
}

// getHeaderWithNonceRequestingIfMissing method gets the header with a given nonce from pool. If it is not found there, it will
// be requested from network
func (boot *ShardBootstrap) getHeaderWithNonceRequestingIfMissing(nonce uint64) (data.HeaderHandler, error) {
	hdr, _, err := process.GetShardHeaderFromPoolWithNonce(
		nonce,
		boot.shardCoordinator.SelfId(),
		boot.headers)
	if err != nil {
		_ = core.EmptyChannel(boot.chRcvHdrNonce)
		boot.requestHeaderWithNonce(nonce)
		err = boot.waitForHeaderNonce()
		if err != nil {
			return nil, err
		}

		hdr, _, err = process.GetShardHeaderFromPoolWithNonce(
			nonce,
			boot.shardCoordinator.SelfId(),
			boot.headers)
		if err != nil {
			return nil, err
		}
	}

	return hdr, nil
}

// getHeaderWithHashRequestingIfMissing method gets the header with a given hash from pool. If it is not found there,
// it will be requested from network
func (boot *ShardBootstrap) getHeaderWithHashRequestingIfMissing(hash []byte) (data.HeaderHandler, error) {
	hdr, err := process.GetShardHeader(hash, boot.headers, boot.marshalizer, boot.store)
	if err != nil {
		_ = core.EmptyChannel(boot.chRcvHdrHash)
		boot.requestHeaderWithHash(hash)
		err = boot.waitForHeaderHash()
		if err != nil {
			return nil, err
		}

		hdr, err = process.GetShardHeaderFromPool(hash, boot.headers)
		if err != nil {
			return nil, err
		}
	}

	return hdr, nil
}

func (boot *ShardBootstrap) getPrevHeader(
	header data.HeaderHandler,
	headerStore storage.Storer,
) (data.HeaderHandler, error) {

	prevHash := header.GetPrevHash()
	buffHeader, err := headerStore.Get(prevHash)
	if err != nil {
		return nil, err
	}

	prevHeader := &block.Header{}
	err = boot.marshalizer.Unmarshal(prevHeader, buffHeader)
	if err != nil {
		return nil, err
	}

	return prevHeader, nil
}

func (boot *ShardBootstrap) getCurrHeader() (data.HeaderHandler, error) {
	blockHeader := boot.chainHandler.GetCurrentBlockHeader()
	if check.IfNil(blockHeader) {
		return nil, process.ErrNilBlockHeader
	}

	header, ok := blockHeader.(*block.Header)
	if !ok {
		return nil, process.ErrWrongTypeAssertion
	}

	return header, nil
}

func (boot *ShardBootstrap) haveHeaderInPoolWithNonce(nonce uint64) bool {
	_, _, err := process.GetShardHeaderFromPoolWithNonce(
		nonce,
		boot.shardCoordinator.SelfId(),
		boot.headers)

	return err == nil
}

func (boot *ShardBootstrap) getShardHeaderFromPool(headerHash []byte) (data.HeaderHandler, error) {
	return process.GetShardHeaderFromPool(headerHash, boot.headers)
}

func (boot *ShardBootstrap) requestMiniBlocksFromHeaderWithNonceIfMissing(headerHandler data.HeaderHandler) {
	nextBlockNonce := boot.getNonceForNextBlock()
	maxNonce := core.MinUint64(nextBlockNonce+process.MaxHeadersToRequestInAdvance-1, boot.forkDetector.ProbableHighestNonce())
	if headerHandler.GetNonce() < nextBlockNonce || headerHandler.GetNonce() > maxNonce {
		return
	}

	header, ok := headerHandler.(*block.Header)
	if !ok {
		log.Warn("cannot convert headerHandler in block.Header")
		return
	}

	hashes := make([][]byte, 0, len(header.MiniBlockHeaders))
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

func (boot *ShardBootstrap) getBlockBodyRequestingIfMissing(headerHandler data.HeaderHandler) (data.BodyHandler, error) {
	header, ok := headerHandler.(*block.Header)
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

func (boot *ShardBootstrap) isForkTriggeredByMeta() bool {
	return boot.forkInfo.IsDetected &&
		boot.forkInfo.Nonce != math.MaxUint64 &&
		boot.forkInfo.Round == process.MinForkRound &&
		boot.forkInfo.Hash != nil
}

func (boot *ShardBootstrap) requestHeaderByNonce(nonce uint64) {
	boot.requestHandler.RequestShardHeaderByNonce(boot.shardCoordinator.SelfId(), nonce)
}
