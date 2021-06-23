package sync

import (
	"bytes"
	"math"
	"sync"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/update"
)

var _ update.HeaderSyncHandler = (*headersToSync)(nil)

const waitTimeForHeaders = time.Minute

type headersToSync struct {
	mutMeta                sync.Mutex
	epochStartMetaBlock    *block.MetaBlock
	unFinishedMetaBlocks   map[string]*block.MetaBlock
	firstPendingMetaBlocks map[string]*block.MetaBlock
	missingMetaBlocks      map[string]struct{}
	missingMetaNonces      map[uint64]struct{}
	foundMetaNonces        map[uint64]string
	chReceivedAll          chan bool
	store                  dataRetriever.StorageService
	metaBlockPool          dataRetriever.HeadersPool
	epochHandler           update.EpochStartVerifier
	marshalizer            marshal.Marshalizer
	hasher                 hashing.Hasher
	stopSyncing            bool
	epochToSync            uint32
	requestHandler         process.RequestHandler
	uint64Converter        typeConverters.Uint64ByteSliceConverter
	shardCoordinator       sharding.Coordinator
}

// ArgsNewHeadersSyncHandler defines the arguments needed for the new header syncer
type ArgsNewHeadersSyncHandler struct {
	StorageService   dataRetriever.StorageService
	Cache            dataRetriever.HeadersPool
	Marshalizer      marshal.Marshalizer
	Hasher           hashing.Hasher
	EpochHandler     update.EpochStartVerifier
	RequestHandler   process.RequestHandler
	Uint64Converter  typeConverters.Uint64ByteSliceConverter
	ShardCoordinator sharding.Coordinator
}

// NewHeadersSyncHandler creates a new header syncer
func NewHeadersSyncHandler(args ArgsNewHeadersSyncHandler) (*headersToSync, error) {
	if check.IfNil(args.StorageService) {
		return nil, update.ErrNilStorage
	}
	if check.IfNil(args.Cache) {
		return nil, update.ErrNilCacher
	}
	if check.IfNil(args.EpochHandler) {
		return nil, update.ErrNilEpochHandler
	}
	if check.IfNil(args.Marshalizer) {
		return nil, update.ErrNilMarshalizer
	}
	if check.IfNil(args.RequestHandler) {
		return nil, update.ErrNilRequestHandler
	}
	if check.IfNil(args.Uint64Converter) {
		return nil, update.ErrNilUint64Converter
	}
	if check.IfNil(args.ShardCoordinator) {
		return nil, update.ErrNilShardCoordinator
	}
	if check.IfNil(args.Hasher) {
		return nil, update.ErrNilHasher
	}

	h := &headersToSync{
		mutMeta:                sync.Mutex{},
		epochStartMetaBlock:    &block.MetaBlock{},
		chReceivedAll:          make(chan bool),
		store:                  args.StorageService,
		metaBlockPool:          args.Cache,
		epochHandler:           args.EpochHandler,
		stopSyncing:            true,
		requestHandler:         args.RequestHandler,
		marshalizer:            args.Marshalizer,
		hasher:                 args.Hasher,
		unFinishedMetaBlocks:   make(map[string]*block.MetaBlock),
		firstPendingMetaBlocks: make(map[string]*block.MetaBlock),
		missingMetaBlocks:      make(map[string]struct{}),
		missingMetaNonces:      make(map[uint64]struct{}),
		uint64Converter:        args.Uint64Converter,
		shardCoordinator:       args.ShardCoordinator,
	}

	h.metaBlockPool.RegisterHandler(h.receivedMetaBlockFirstPending)
	h.metaBlockPool.RegisterHandler(h.receivedUnFinishedMetaBlocks)

	return h, nil
}

func (h *headersToSync) receivedMetaBlockFirstPending(headerHandler data.HeaderHandler, hash []byte) {
	h.mutMeta.Lock()
	if h.stopSyncing || len(h.missingMetaBlocks) == 0 {
		h.mutMeta.Unlock()
		return
	}

	metaHeader, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		h.mutMeta.Unlock()
		return
	}

	if _, ok = h.missingMetaBlocks[string(hash)]; !ok {
		h.mutMeta.Unlock()
		return
	}

	delete(h.missingMetaBlocks, string(hash))
	h.firstPendingMetaBlocks[string(hash)] = metaHeader

	if len(h.missingMetaBlocks) > 0 {
		h.mutMeta.Unlock()
		return
	}

	h.mutMeta.Unlock()
	h.chReceivedAll <- true
}

func (h *headersToSync) receivedUnFinishedMetaBlocks(headerHandler data.HeaderHandler, hash []byte) {
	h.mutMeta.Lock()
	if h.stopSyncing || len(h.missingMetaNonces) == 0 {
		h.mutMeta.Unlock()
		return
	}

	meta, ok := headerHandler.(*block.MetaBlock)
	if !ok {
		h.mutMeta.Unlock()
		return
	}

	if _, ok = h.missingMetaNonces[meta.GetNonce()]; !ok {
		h.mutMeta.Unlock()
		return
	}

	attestingHash, okHash := h.foundMetaNonces[meta.GetNonce()+1]
	attestingHdr, okHdr := h.unFinishedMetaBlocks[attestingHash]

	isTheNeededMeta := okHash && okHdr && bytes.Equal(attestingHdr.GetPrevHash(), hash)
	if !isTheNeededMeta {
		h.requestHandler.RequestMetaHeaderByNonce(meta.GetNonce())
		h.requestHandler.RequestMetaHeaderByNonce(meta.GetNonce() + 1)
		h.mutMeta.Unlock()
		return
	}

	delete(h.missingMetaNonces, meta.GetNonce())
	h.unFinishedMetaBlocks[string(hash)] = meta
	h.foundMetaNonces[meta.GetNonce()] = string(hash)

	if len(h.missingMetaNonces) > 0 {
		h.mutMeta.Unlock()
		return
	}

	h.mutMeta.Unlock()
	h.chReceivedAll <- true
}

// SyncUnFinishedMetaHeaders syncs and validates all the unFinished metaHeaders for each shard
func (h *headersToSync) SyncUnFinishedMetaHeaders(epoch uint32) error {
	// TODO: do this with context.Context
	err := h.syncEpochStartMetaHeader(epoch, waitTimeForHeaders)
	if err != nil {
		return err
	}

	err = h.syncFirstPendingMetaBlocks(waitTimeForHeaders)
	if err != nil {
		return err
	}

	err = h.syncAllNeededMetaHeaders(waitTimeForHeaders)
	if err != nil {
		return err
	}

	return nil
}

// SyncEpochStartMetaHeader syncs and validates an epoch start metaHeader
func (h *headersToSync) syncEpochStartMetaHeader(epoch uint32, waitTime time.Duration) error {
	log.Debug("headersToSync.syncEpochStartMetaHeader started") //TODO remove this
	defer func() {
		h.mutMeta.Lock()
		h.stopSyncing = true
		h.mutMeta.Unlock()
		log.Debug("headersToSync.syncEpochStartMetaHeader ended") //TODO remove this
	}()

	h.epochToSync = epoch
	epochStartId := core.EpochStartIdentifier(epoch)
	meta, err := process.GetMetaHeaderFromStorage([]byte(epochStartId), h.marshalizer, h.store)
	if err != nil {
		h.mutMeta.Lock()
		h.stopSyncing = false
		h.requestHandler.RequestStartOfEpochMetaBlock(epoch)
		h.mutMeta.Unlock()

		startTime := time.Now()
		for {
			time.Sleep(time.Millisecond)
			elapsedTime := time.Since(startTime)
			if elapsedTime > waitTime {
				return process.ErrTimeIsOut
			}

			if !h.epochHandler.IsEpochStart() {
				continue
			}

			meta, err = process.GetMetaHeaderFromStorage([]byte(epochStartId), h.marshalizer, h.store)
			if err != nil {
				continue
			}

			h.mutMeta.Lock()
			h.epochStartMetaBlock = meta
			h.mutMeta.Unlock()

			break
		}

		err = WaitFor(h.chReceivedAll, waitTime)
		if err != nil {
			log.Warn("timeOut for requesting epoch metaHdr")
			return err
		}

		return nil
	}

	h.mutMeta.Lock()
	h.epochStartMetaBlock = meta
	h.mutMeta.Unlock()

	return nil
}

func (h *headersToSync) syncFirstPendingMetaBlocks(waitTime time.Duration) error {
	log.Debug("headersToSync.syncFirstPendingMetaBlocks started") //TODO remove this
	defer func() {
		h.mutMeta.Lock()
		h.stopSyncing = true
		h.mutMeta.Unlock()
		log.Debug("headersToSync.syncFirstPendingMetaBlocks ended") //TODO remove this
	}()

	h.mutMeta.Lock()

	epochStart := h.epochStartMetaBlock

	h.firstPendingMetaBlocks = make(map[string]*block.MetaBlock)
	h.missingMetaBlocks = make(map[string]struct{})
	for _, shardData := range epochStart.EpochStart.LastFinalizedHeaders {
		metaHash := string(shardData.FirstPendingMetaBlock)
		if _, ok := h.firstPendingMetaBlocks[metaHash]; ok {
			continue
		}
		if _, ok := h.missingMetaBlocks[metaHash]; ok {
			continue
		}

		metaHdr, err := process.GetMetaHeader([]byte(metaHash), h.metaBlockPool, h.marshalizer, h.store)
		if err != nil || check.IfNil(metaHdr) {
			h.missingMetaBlocks[metaHash] = struct{}{}
			continue
		}

		h.firstPendingMetaBlocks[metaHash] = metaHdr
	}

	_ = core.EmptyChannel(h.chReceivedAll)
	for metaHash := range h.missingMetaBlocks {
		h.stopSyncing = false
		h.requestHandler.RequestMetaHeader([]byte(metaHash))
	}
	requested := len(h.missingMetaBlocks) > 0
	h.mutMeta.Unlock()

	if requested {
		err := WaitFor(h.chReceivedAll, waitTime)
		if err != nil {
			log.Warn("timeOut for requesting first pending metaHeaders")
			return err
		}
	}

	return nil
}

func (h *headersToSync) syncAllNeededMetaHeaders(waitTime time.Duration) error {
	log.Debug("headersToSync.syncAllNeededMetaHeaders started") //TODO remove this
	defer func() {
		h.mutMeta.Lock()
		h.stopSyncing = true
		h.mutMeta.Unlock()
		log.Debug("headersToSync.syncAllNeededMetaHeaders ended") //TODO remove this
	}()

	h.mutMeta.Lock()

	err := h.computeMissingNonce(h.epochStartMetaBlock)
	if err != nil {
		h.mutMeta.Unlock()
		return err
	}

	_ = core.EmptyChannel(h.chReceivedAll)
	for nonce := range h.missingMetaNonces {
		h.stopSyncing = false
		h.requestHandler.RequestMetaHeaderByNonce(nonce)
	}

	requested := len(h.missingMetaNonces) > 0
	h.mutMeta.Unlock()

	if requested {
		errWaitFor := WaitFor(h.chReceivedAll, waitTime)
		if errWaitFor != nil {
			log.Warn("timeOut for requesting all unFinished metaBlocks")
			return errWaitFor
		}
	}

	return nil
}

func (h *headersToSync) computeMissingNonce(epochStart *block.MetaBlock) error {
	h.missingMetaNonces = make(map[uint64]struct{})
	h.foundMetaNonces = make(map[uint64]string)

	epochStartNonce := epochStart.Nonce
	epochStartHash, err := core.CalculateHash(h.marshalizer, h.hasher, epochStart)
	if err != nil {
		return err
	}

	h.foundMetaNonces[epochStartNonce] = string(epochStartHash)
	h.unFinishedMetaBlocks[string(epochStartHash)] = epochStart

	for hash, meta := range h.firstPendingMetaBlocks {
		h.unFinishedMetaBlocks[hash] = meta
		h.foundMetaNonces[meta.Nonce] = hash
	}

	lowestPendingNonce := h.lowestPendingNonceFrom(h.firstPendingMetaBlocks)
	for nonce := epochStartNonce - 1; nonce >= lowestPendingNonce+1; nonce-- {
		_, ok := h.foundMetaNonces[nonce]
		if ok {
			continue
		}

		attestingHash, ok := h.foundMetaNonces[nonce+1]
		if !ok {
			h.missingMetaNonces[nonce] = struct{}{}
			continue
		}
		attestingMeta, ok := h.unFinishedMetaBlocks[attestingHash]
		if !ok {
			h.missingMetaNonces[nonce] = struct{}{}
			continue
		}
		metaHdr, errGetMetaHeader := process.GetMetaHeader(attestingMeta.GetPrevHash(), h.metaBlockPool, h.marshalizer, h.store)
		if errGetMetaHeader != nil {
			h.missingMetaNonces[nonce] = struct{}{}
			continue
		}

		h.foundMetaNonces[nonce] = string(attestingMeta.GetPrevHash())
		h.unFinishedMetaBlocks[string(attestingMeta.GetPrevHash())] = metaHdr
	}

	return nil
}

func (h *headersToSync) lowestPendingNonceFrom(metaBlocks map[string]*block.MetaBlock) uint64 {
	lowestNonce := uint64(math.MaxUint64)
	for _, metaBlock := range metaBlocks {
		if lowestNonce > metaBlock.GetNonce() {
			lowestNonce = metaBlock.GetNonce()
		}
	}
	return lowestNonce
}

// GetEpochStartMetaBlock returns the synced epoch start metaBlock
func (h *headersToSync) GetEpochStartMetaBlock() (*block.MetaBlock, error) {
	h.mutMeta.Lock()
	meta := h.epochStartMetaBlock
	h.mutMeta.Unlock()

	if meta.IsStartOfEpochBlock() || meta.Nonce == 0 {
		return meta, nil
	}

	return nil, update.ErrNotSynced
}

// GetUnFinishedMetaBlocks returns the synced metablock
func (h *headersToSync) GetUnFinishedMetaBlocks() (map[string]*block.MetaBlock, error) {
	h.mutMeta.Lock()
	unFinished := make(map[string]*block.MetaBlock)
	for hash, meta := range h.unFinishedMetaBlocks {
		unFinished[hash] = meta
	}
	h.mutMeta.Unlock()

	return unFinished, nil
}

// IsInterfaceNil returns true if underlying object is nil
func (h *headersToSync) IsInterfaceNil() bool {
	return h == nil
}
