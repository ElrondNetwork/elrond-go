package storageBootstrap

import (
	"fmt"
	"github.com/ElrondNetwork/elrond-go/data/block"

	"github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/block/bootstrapStorage"
	"github.com/ElrondNetwork/elrond-go/process/block/processedMb"
	"github.com/ElrondNetwork/elrond-go/process/sync"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
)

var log = logger.GetOrCreate("process/sync")

// ArgsBaseStorageBootstrapper is structure used to create a new storage bootstrapper
type ArgsBaseStorageBootstrapper struct {
	BootStorer                   process.BootStorer
	ForkDetector                 process.ForkDetector
	BlockProcessor               process.BlockProcessor
	ChainHandler                 data.ChainHandler
	Marshalizer                  marshal.Marshalizer
	Store                        dataRetriever.StorageService
	Uint64Converter              typeConverters.Uint64ByteSliceConverter
	BootstrapRoundIndex          uint64
	ShardCoordinator             sharding.Coordinator
	NodesCoordinator             sharding.NodesCoordinator
	EpochStartTrigger            process.EpochStartTriggerHandler
	BlockTracker                 process.BlockTracker
	ChainID                      string
	ScheduledTxsExecutionHandler process.ScheduledTxsExecutionHandler
}

// ArgsShardStorageBootstrapper is structure used to create a new storage bootstrapper for shard
type ArgsShardStorageBootstrapper struct {
	ArgsBaseStorageBootstrapper
}

// ArgsMetaStorageBootstrapper is structure used to create a new storage bootstrapper for metachain
type ArgsMetaStorageBootstrapper struct {
	ArgsBaseStorageBootstrapper
	PendingMiniBlocksHandler process.PendingMiniBlocksHandler
}

type storageBootstrapper struct {
	bootStorer                   process.BootStorer
	forkDetector                 process.ForkDetector
	blkExecutor                  process.BlockProcessor
	blkc                         data.ChainHandler
	marshalizer                  marshal.Marshalizer
	store                        dataRetriever.StorageService
	uint64Converter              typeConverters.Uint64ByteSliceConverter
	shardCoordinator             sharding.Coordinator
	nodesCoordinator             sharding.NodesCoordinator
	epochStartTrigger            process.EpochStartTriggerHandler
	blockTracker                 process.BlockTracker
	bootstrapRoundIndex          uint64
	bootstrapper                 storageBootstrapperHandler
	headerNonceHashStore         storage.Storer
	highestNonce                 uint64
	chainID                      string
	scheduledTxsExecutionHandler process.ScheduledTxsExecutionHandler
}

func (st *storageBootstrapper) loadBlocks() error {
	var err error
	var headerInfo bootstrapStorage.BootstrapData

	minRound := uint64(0)
	if !check.IfNil(st.blkc.GetGenesisHeader()) {
		minRound = st.blkc.GetGenesisHeader().GetRound()
	}

	round := st.bootStorer.GetHighestRound()
	if round <= int64(minRound) {
		log.Debug("Load blocks does nothing as start from genesis")
		err = st.bootStorer.SaveLastRound(0)
		log.LogIfError(
			err,
			"function", "storageBootstrapper.loadBlocks",
			"operation", "SaveLastRound",
		)

		return process.ErrNotEnoughValidBlocksInStorage
	}
	storageHeadersInfo := make([]bootstrapStorage.BootstrapData, 0)

	log.Debug("Load blocks started...")

	for {
		headerInfo, err = st.bootStorer.Get(round)
		if err != nil {
			break
		}

		if round == headerInfo.LastRound {
			err = sync.ErrCorruptBootstrapFromStorageDb
			break
		}

		storageHeadersInfo = append(storageHeadersInfo, headerInfo)

		if uint64(round) > st.bootstrapRoundIndex {
			round = headerInfo.LastRound
			continue
		}

		err = st.applyHeaderInfo(headerInfo)
		if err != nil {
			round = headerInfo.LastRound
			continue
		}

		var bootInfos []bootstrapStorage.BootstrapData
		bootInfos, err = st.getBootInfos(headerInfo)
		if err != nil {
			round = headerInfo.LastRound
			continue
		}

		err = st.applyBootInfos(bootInfos)
		if err != nil {
			round = headerInfo.LastRound
			continue
		}

		break
	}

	if err != nil {
		log.Warn("bootstrapper", "error", err)
		st.restoreBlockChainToGenesis()
		err = st.bootStorer.SaveLastRound(0)
		log.LogIfError(
			err,
			"function", "storageBootstrapper.loadBlocks",
			"operation", "SaveLastRound after restoreBlockChainToGenesis",
		)

		return process.ErrNotEnoughValidBlocksInStorage
	}

	log.Debug("storageBootstrapper.loadBlocks",
		"LastHeader", st.displayBoostrapHeaderInfo(headerInfo.LastHeader),
		"LastCrossNotarizedHeaders", st.displayBootstrapHeaders(headerInfo.LastCrossNotarizedHeaders),
		"LastSelfNotarizedHeaders", st.displayBootstrapHeaders(headerInfo.LastSelfNotarizedHeaders),
		"HighestFinalBlockNonce", headerInfo.HighestFinalBlockNonce,
		"NodesCoordinatorConfigKey", headerInfo.NodesCoordinatorConfigKey,
		"EpochStartTriggerConfigKey", headerInfo.EpochStartTriggerConfigKey,
	)

	st.bootstrapper.applyNumPendingMiniBlocks(headerInfo.PendingMiniBlocks)

	processedMiniBlocks := processedMb.NewProcessedMiniBlocks()
	processedMiniBlocks.ConvertSliceToProcessedMiniBlocksMap(headerInfo.ProcessedMiniBlocks)
	processedMiniBlocks.DisplayProcessedMiniBlocks()

	st.blkExecutor.ApplyProcessedMiniBlocks(processedMiniBlocks)

	st.cleanupStorageForHigherNonceIfExist()

	for i := 0; i < len(storageHeadersInfo)-1; i++ {
		st.cleanupStorage(storageHeadersInfo[i].LastHeader)
		st.bootstrapper.cleanupNotarizedStorage(storageHeadersInfo[i].LastHeader.Hash)
	}

	err = st.bootStorer.SaveLastRound(round)
	if err != nil {
		log.Debug("cannot save last round in storage ", "error", err.Error())
	}

	process.SetScheduledRootHashAndSCRs(
		headerInfo.LastHeader.Hash,
		st.bootstrapper.getRootHash(headerInfo.LastHeader.Hash),
		make(map[block.Type][]data.TransactionHandler),
		st.store,
		st.marshalizer,
		st.scheduledTxsExecutionHandler)

	st.highestNonce = headerInfo.LastHeader.Nonce

	return nil
}

func (st *storageBootstrapper) displayBootstrapHeaders(hdrs []bootstrapStorage.BootstrapHeaderInfo) string {
	str := "["
	for _, h := range hdrs {
		str += st.displayBoostrapHeaderInfo(h)
	}

	str += "]"
	return str
}

func (st *storageBootstrapper) displayBoostrapHeaderInfo(hinfo bootstrapStorage.BootstrapHeaderInfo) string {
	return fmt.Sprintf("shard %d, nonce %d, epoch %d, hash %s",
		hinfo.ShardId, hinfo.Nonce, hinfo.Epoch, logger.DisplayByteSlice(hinfo.Hash))
}

func (st *storageBootstrapper) cleanupStorageForHigherNonceIfExist() {
	round := st.bootStorer.GetHighestRound()
	bootstrapData, err := st.bootStorer.Get(round)
	if err != nil {
		log.Debug("cleanupStorageForHigherNonceIfExist.Get",
			"round", round,
			"error", err.Error())
		return
	}

	highestBlockNonce := bootstrapData.LastHeader.GetNonce()
	header, hash, err := st.bootstrapper.getHeaderWithNonce(highestBlockNonce+1, st.shardCoordinator.SelfId())
	if err != nil {
		log.Trace("cleanupStorageForHigherNonceIfExist.getHeaderWithNonce",
			"shard", st.shardCoordinator.SelfId(),
			"nonce", highestBlockNonce+1,
			"error", err)
		return
	}

	headerInfo := bootstrapStorage.BootstrapHeaderInfo{
		ShardId: header.GetShardID(),
		Epoch:   header.GetEpoch(),
		Nonce:   header.GetNonce(),
		Hash:    hash,
	}

	st.cleanupStorage(headerInfo)
	st.bootstrapper.cleanupNotarizedStorage(headerInfo.Hash)
}

// GetHighestBlockNonce will return nonce of last block loaded from storage
func (st *storageBootstrapper) GetHighestBlockNonce() uint64 {
	return st.highestNonce
}

func (st *storageBootstrapper) applyHeaderInfo(hdrInfo bootstrapStorage.BootstrapData) error {
	headerHash := hdrInfo.LastHeader.Hash
	headerFromStorage, err := st.bootstrapper.getHeader(headerHash)
	if err != nil {
		log.Debug("cannot get header ", "nonce", hdrInfo.LastHeader.Nonce, "error", err.Error())
		return err
	}

	if string(headerFromStorage.GetChainID()) != st.chainID {
		log.Debug("chain ID missmatch for header with nonce", "nonce", headerFromStorage.GetNonce(),
			"reference", []byte(st.chainID),
			"fromStorage", headerFromStorage.GetChainID())
		return process.ErrInvalidChainID
	}

	rootHash := process.GetScheduledRootHash(headerHash, headerFromStorage.GetRootHash(), st.store, st.marshalizer)

	err = st.blkExecutor.RevertStateToBlock(headerFromStorage, rootHash)
	if err != nil {
		log.Debug("cannot recreate trie for header with nonce", "nonce", headerFromStorage.GetNonce())
		return err
	}

	err = st.applyBlock(headerFromStorage, headerHash)
	if err != nil {
		log.Debug("cannot apply block for header ", "nonce", headerFromStorage.GetNonce(), "error", err.Error())
		return err
	}

	return nil
}

func (st *storageBootstrapper) getBootInfos(hdrInfo bootstrapStorage.BootstrapData) ([]bootstrapStorage.BootstrapData, error) {
	highestFinalBlockNonce := hdrInfo.HighestFinalBlockNonce
	highestBlockNonce := hdrInfo.LastHeader.Nonce

	lastRound := hdrInfo.LastRound
	bootInfos := []bootstrapStorage.BootstrapData{hdrInfo}

	log.Debug("block info from storage",
		"highest block nonce", highestBlockNonce,
		"highest final block nonce", highestFinalBlockNonce,
		"last round", lastRound)

	if highestFinalBlockNonce == highestBlockNonce {
		return bootInfos, nil
	}

	lowestNonce := uint64(core.MaxInt64(int64(highestFinalBlockNonce)-1, 1))
	for highestBlockNonce > lowestNonce {
		strHdrI, err := st.bootStorer.Get(lastRound)
		if err != nil {
			log.Debug("cannot load header info from storage ", "error", err.Error())
			return nil, err
		}

		bootInfos = append(bootInfos, strHdrI)
		highestBlockNonce = strHdrI.LastHeader.Nonce

		lastRound = strHdrI.LastRound
		if lastRound == 0 {
			break
		}
	}

	return bootInfos, nil
}

func (st *storageBootstrapper) applyBootInfos(bootInfos []bootstrapStorage.BootstrapData) error {
	var err error

	defer func() {
		if err != nil {
			st.blockTracker.RestoreToGenesis()
			st.forkDetector.RestoreToGenesis()
		}
	}()

	for i := len(bootInfos) - 1; i >= 0; i-- {
		log.Debug("apply header",
			"shard", bootInfos[i].LastHeader.ShardId,
			"epoch", bootInfos[i].LastHeader.Epoch,
			"nonce", bootInfos[i].LastHeader.Nonce)

		err = st.bootstrapper.applyCrossNotarizedHeaders(bootInfos[i].LastCrossNotarizedHeaders)
		if err != nil {
			log.Debug("cannot apply cross notarized headers", "error", err.Error())
			return err
		}

		var selfNotarizedHeaders []data.HeaderHandler
		var selfNotarizedHeadersHashes [][]byte
		selfNotarizedHeaders, selfNotarizedHeadersHashes, err = st.bootstrapper.applySelfNotarizedHeaders(bootInfos[i].LastSelfNotarizedHeaders)
		if err != nil {
			log.Warn("cannot apply self notarized headers", "error", err.Error())
		}

		var header data.HeaderHandler
		header, err = st.bootstrapper.getHeader(bootInfos[i].LastHeader.Hash)
		if err != nil {
			log.Debug("cannot get header", "hash", bootInfos[i].LastHeader.Hash, "error", err.Error())
			return err
		}

		log.Debug("add header to fork detector",
			"shard", header.GetShardID(),
			"round", header.GetRound(),
			"nonce", header.GetNonce(),
			"hash", bootInfos[i].LastHeader.Hash)

		err = st.forkDetector.AddHeader(header, bootInfos[i].LastHeader.Hash, process.BHProcessed, selfNotarizedHeaders, selfNotarizedHeadersHashes)
		if err != nil {
			log.Warn("cannot add header to fork detector", "error", err.Error())
		}

		if i > 0 {
			log.Debug("added self notarized header in block tracker",
				"shard", header.GetShardID(),
				"round", header.GetRound(),
				"nonce", header.GetNonce(),
				"hash", bootInfos[i].LastHeader.Hash)

			st.blockTracker.AddSelfNotarizedHeader(st.shardCoordinator.SelfId(), header, bootInfos[i].LastHeader.Hash)
		}

		st.blockTracker.AddTrackedHeader(header, bootInfos[i].LastHeader.Hash)
	}

	if len(bootInfos) == 1 {
		st.forkDetector.SetFinalToLastCheckpoint()
	}

	err = st.nodesCoordinator.LoadState(bootInfos[0].NodesCoordinatorConfigKey)
	if err != nil {
		log.Debug("cannot load nodes coordinator state", "error", err.Error())
		return err
	}

	err = st.epochStartTrigger.LoadState(bootInfos[0].EpochStartTriggerConfigKey)
	if err != nil {
		log.Debug("cannot load epoch start trigger state", "error", err.Error())
		return err
	}

	return nil
}

func (st *storageBootstrapper) cleanupStorage(headerInfo bootstrapStorage.BootstrapHeaderInfo) {
	log.Debug("cleanup storage")

	nonceToByteSlice := st.uint64Converter.ToByteSlice(headerInfo.Nonce)
	err := st.headerNonceHashStore.Remove(nonceToByteSlice)
	if err != nil {
		log.Debug("block was not removed from storage",
			"shradId", headerInfo.ShardId,
			"nonce", headerInfo.Nonce,
			"hash", headerInfo.Hash,
			"error", err.Error())
		return
	}

	log.Debug("block was removed from storage",
		"shradId", headerInfo.ShardId,
		"nonce", headerInfo.Nonce,
		"hash", headerInfo.Hash)
}

func (st *storageBootstrapper) applyBlock(header data.HeaderHandler, headerHash []byte) error {
	err := st.blkc.SetCurrentBlockHeader(header)
	if err != nil {
		return err
	}

	st.blkc.SetCurrentBlockHeaderHash(headerHash)

	return nil
}

func (st *storageBootstrapper) restoreBlockChainToGenesis() {
	genesisHeader := st.blkc.GetGenesisHeader()
	err := st.blkExecutor.RevertStateToBlock(genesisHeader, genesisHeader.GetRootHash())
	if err != nil {
		log.Debug("cannot recreate trie for header with nonce", "nonce", genesisHeader.GetNonce())
	}

	err = st.blkc.SetCurrentBlockHeader(nil)
	if err != nil {
		log.Debug("cannot set current block header", "error", err.Error())
	}

	st.blkc.SetCurrentBlockHeaderHash(nil)
}

func checkBaseStorageBootrstrapperArguments(args ArgsBaseStorageBootstrapper) error {
	if check.IfNil(args.BootStorer) {
		return process.ErrNilBootStorer
	}
	if check.IfNil(args.ForkDetector) {
		return process.ErrNilForkDetector
	}
	if check.IfNil(args.BlockProcessor) {
		return process.ErrNilBlockProcessor
	}
	if check.IfNil(args.ChainHandler) {
		return process.ErrNilBlockChain
	}
	if check.IfNil(args.Marshalizer) {
		return process.ErrNilMarshalizer
	}
	if check.IfNil(args.Store) {
		return process.ErrNilStore
	}
	if check.IfNil(args.Uint64Converter) {
		return process.ErrNilUint64Converter
	}
	if check.IfNil(args.ShardCoordinator) {
		return process.ErrNilShardCoordinator
	}
	if check.IfNil(args.NodesCoordinator) {
		return process.ErrNilNodesCoordinator
	}
	if check.IfNil(args.EpochStartTrigger) {
		return process.ErrNilEpochStartTrigger
	}
	if check.IfNil(args.BlockTracker) {
		return process.ErrNilBlockTracker
	}
	if check.IfNil(args.ScheduledTxsExecutionHandler) {
		return process.ErrNilScheduledTxsExecutionHandler
	}

	return nil
}
