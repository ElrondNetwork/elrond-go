package sync_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	goSync "sync"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/consensus/round"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/blockchain"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/process/sync"
	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/storage/memorydb"
	"github.com/ElrondNetwork/elrond-go/storage/storageUnit"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/stretchr/testify/assert"
)

// waitTime defines the time in milliseconds until node waits the requested info from the network
const waitTime = 100 * time.Millisecond

type removedFlags struct {
	flagHdrRemovedFromHeaders      bool
	flagHdrRemovedFromStorage      bool
	flagHdrRemovedFromForkDetector bool
}

func createMockPools() *testscommon.PoolsHolderStub {
	pools := testscommon.NewPoolsHolderStub()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		return &mock.HeadersCacherStub{}
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		cs := &testscommon.CacherStub{
			GetCalled: func(key []byte) (value interface{}, ok bool) {
				return nil, false
			},
			RegisterHandlerCalled: func(i func(key []byte, value interface{})) {},
		}
		return cs
	}

	return pools
}

func createStore() *mock.ChainStorerMock {
	return &mock.ChainStorerMock{
		GetStorerCalled: func(unitType dataRetriever.UnitType) storage.Storer {
			return &testscommon.StorerStub{
				GetCalled: func(key []byte) ([]byte, error) {
					return nil, process.ErrMissingHeader
				},
				RemoveCalled: func(key []byte) error {
					return nil
				},
			}
		},
	}
}

func generateTestCache() storage.Cacher {
	cache, _ := storageUnit.NewCache(storageUnit.CacheConfig{Type: storageUnit.LRUCache, Capacity: 1000, Shards: 1, SizeInBytes: 0})
	return cache
}

func generateTestUnit() storage.Storer {
	storer, _ := storageUnit.NewStorageUnit(
		generateTestCache(),
		memorydb.New(),
	)

	return storer
}

func createFullStore() dataRetriever.StorageService {
	store := dataRetriever.NewChainStorer()
	store.AddStorer(dataRetriever.TransactionUnit, generateTestUnit())
	store.AddStorer(dataRetriever.RewardTransactionUnit, generateTestUnit())
	store.AddStorer(dataRetriever.MiniBlockUnit, generateTestUnit())
	store.AddStorer(dataRetriever.MetaBlockUnit, generateTestUnit())
	store.AddStorer(dataRetriever.PeerChangesUnit, generateTestUnit())
	store.AddStorer(dataRetriever.BlockHeaderUnit, generateTestUnit())
	store.AddStorer(dataRetriever.ShardHdrNonceHashDataUnit, generateTestUnit())
	store.AddStorer(dataRetriever.ReceiptsUnit, generateTestUnit())
	return store
}

func createBlockProcessor(blk data.ChainHandler) *mock.BlockProcessorMock {
	blockProcessorMock := &mock.BlockProcessorMock{
		ProcessBlockCalled: func(hdr data.HeaderHandler, bdy data.BodyHandler, haveTime func() time.Duration) error {
			_ = blk.SetCurrentBlockHeader(hdr.(*block.Header))
			return nil
		},
		RevertAccountStateCalled: func(header data.HeaderHandler) {
		},
		CommitBlockCalled: func(header data.HeaderHandler, body data.BodyHandler) error {
			return nil
		},
	}

	return blockProcessorMock
}

func createForkDetector(removedNonce uint64, remFlags *removedFlags) process.ForkDetector {
	return &mock.ForkDetectorMock{
		RemoveHeaderCalled: func(nonce uint64, hash []byte) {
			if nonce == removedNonce {
				remFlags.flagHdrRemovedFromForkDetector = true
			}
		},
		GetHighestFinalBlockNonceCalled: func() uint64 {
			return removedNonce
		},
		ProbableHighestNonceCalled: func() uint64 {
			return uint64(0)
		},
		GetNotarizedHeaderHashCalled: func(nonce uint64) []byte {
			return nil
		},
	}
}

func initBlockchain() *mock.BlockChainMock {
	blkc := &mock.BlockChainMock{
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{
				Nonce:     uint64(0),
				Signature: []byte("genesis signature"),
				RandSeed:  []byte{0},
			}
		},
		GetGenesisHeaderHashCalled: func() []byte {
			return []byte("genesis header hash")
		},
	}

	return blkc
}

func initNetworkWatcher() process.NetworkConnectionWatcher {
	return &mock.NetworkConnectionWatcherStub{
		IsConnectedToTheNetworkCalled: func() bool {
			return true
		},
	}
}

func initRoundHandler() consensus.RoundHandler {
	rnd, _ := round.NewRound(
		time.Now(),
		time.Now(),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	return rnd
}

func CreateShardBootstrapMockArguments() sync.ArgShardBootstrapper {
	argsBaseBootstrapper := sync.ArgBaseBootstrapper{
		PoolsHolder:          createMockPools(),
		Store:                createStore(),
		ChainHandler:         initBlockchain(),
		RoundHandler:         &mock.RoundHandlerMock{},
		BlockProcessor:       &mock.BlockProcessorMock{},
		WaitTime:             waitTime,
		Hasher:               &mock.HasherMock{},
		Marshalizer:          &mock.MarshalizerMock{},
		ForkDetector:         &mock.ForkDetectorMock{},
		RequestHandler:       &testscommon.RequestHandlerStub{},
		ShardCoordinator:     mock.NewOneShardCoordinatorMock(),
		Accounts:             &testscommon.AccountsStub{},
		BlackListHandler:     &mock.BlackListHandlerStub{},
		NetworkWatcher:       initNetworkWatcher(),
		BootStorer:           &mock.BoostrapStorerMock{},
		StorageBootstrapper:  &mock.StorageBootstrapperMock{},
		EpochHandler:         &mock.EpochStartTriggerStub{},
		MiniblocksProvider:   &mock.MiniBlocksProviderStub{},
		Uint64Converter:      &mock.Uint64ByteSliceConverterMock{},
		AppStatusHandler:     &mock.AppStatusHandlerStub{},
		OutportHandler:       &testscommon.OutportStub{},
		AccountsDBSyncer:     &mock.AccountsDBSyncerStub{},
		CurrentEpochProvider: &testscommon.CurrentEpochProviderStub{},
	}

	argsShardBootstrapper := sync.ArgShardBootstrapper{
		ArgBaseBootstrapper: argsBaseBootstrapper,
	}

	return argsShardBootstrapper
}

//------- NewShardBootstrap

func TestNewShardBootstrap_NilPoolsHolderShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.PoolsHolder = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilPoolsHolder, err)
}

func TestNewShardBootstrap_NilAccountsDBSyncerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.AccountsDBSyncer = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilAccountsDBSyncer, err)
}

func TestNewShardBootstrap_PoolsHolderRetNilOnHeadersShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		return nil
	}
	args.PoolsHolder = pools

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilHeadersDataPool, err)
}

func TestNewShardBootstrap_PoolsHolderRetNilOnTxBlockBodyShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	pools := createMockPools()
	pools.MiniBlocksCalled = func() storage.Cacher {
		return nil
	}
	args.PoolsHolder = pools

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilTxBlockBody, err)
}

func TestNewShardBootstrap_NilStoreShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.Store = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilStore, err)
}

func TestNewShardBootstrap_NilAppStatusHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.AppStatusHandler = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilAppStatusHandler, err)
}

func TestNewShardBootstrap_NilBlockchainShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.ChainHandler = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilBlockChain, err)
}

func TestNewShardBootstrap_NilRoundHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.RoundHandler = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilRoundHandler, err)
}

func TestNewShardBootstrap_NilBlockProcessorShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.BlockProcessor = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilBlockProcessor, err)
}

func TestNewShardBootstrap_NilHasherShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.Hasher = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilHasher, err)
}

func TestNewShardBootstrap_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.Marshalizer = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilMarshalizer, err)
}

func TestNewShardBootstrap_NilForkDetectorShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.ForkDetector = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilForkDetector, err)
}

func TestNewShardBootstrap_NilRequestHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.RequestHandler = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilRequestHandler, err)
}

func TestNewShardBootstrap_NilShardCoordinatorShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.ShardCoordinator = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilShardCoordinator, err)
}

func TestNewShardBootstrap_NilAccountsAdapterShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.Accounts = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilAccountsAdapter, err)
}

func TestNewShardBootstrap_NilBlackListHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.BlackListHandler = nil

	bs, err := sync.NewShardBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilBlackListCacher, err)
}

func TestNewShardBootstrap_OkValsShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	wasCalled := 0

	pools := testscommon.NewPoolsHolderStub()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}

		sds.AddCalled = func(headerHash []byte, header data.HeaderHandler) {
			assert.Fail(t, "should have not reached this point")
		}
		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
			wasCalled++
		}
		return sds
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		cs := testscommon.NewCacherStub()
		cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {
			wasCalled++
		}

		return cs
	}
	args.PoolsHolder = pools
	args.IsInImportMode = true
	bs, err := sync.NewShardBootstrap(args)

	assert.NotNil(t, bs)
	assert.Nil(t, err)
	assert.Equal(t, 2, wasCalled)
	assert.False(t, bs.IsInterfaceNil())
	assert.True(t, bs.IsInImportMode())

	args.IsInImportMode = false
	bs, err = sync.NewShardBootstrap(args)

	assert.NotNil(t, bs)
	assert.Nil(t, err)
	assert.False(t, bs.IsInImportMode())
}

//------- processing

func TestBootstrap_SyncBlockShouldCallForkChoice(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1, PubKeysBitmap: []byte("X")}
	blockBodyUnit := &testscommon.StorerStub{
		GetCalled: func(key []byte) (i []byte, e error) {
			return nil, nil
		},
	}

	store := dataRetriever.NewChainStorer()
	store.AddStorer(dataRetriever.MiniBlockUnit, blockBodyUnit)
	args.Store = store

	blkc, _ := blockchain.NewBlockChain(&mock.AppStatusHandlerStub{
		SetUInt64ValueHandler: func(key string, value uint64) {},
	})

	_ = blkc.SetGenesisHeader(&block.Header{})
	_ = blkc.SetCurrentBlockHeader(&hdr)
	args.ChainHandler = blkc

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return &process.ForkInfo{
			IsDetected: true,
			Nonce:      90,
			Round:      90,
			Hash:       []byte("hash"),
		}
	}
	forkDetector.RemoveHeaderCalled = func(nonce uint64, hash []byte) {
	}
	forkDetector.GetHighestFinalBlockNonceCalled = func() uint64 {
		return hdr.Nonce
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 100
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()
	args.BlockProcessor = createBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewShardBootstrap(args)
	r := bs.SyncBlock()

	assert.Equal(t, process.ErrNilHeadersStorage, r)
}

func TestBootstrap_ShouldReturnTimeIsOutWhenMissingHeader(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 100
	}
	forkDetector.GetNotarizedHeaderHashCalled = func(nonce uint64) []byte {
		return nil
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(2*100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)
	args.BlockProcessor = createBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewShardBootstrap(args)
	r := bs.SyncBlock()

	assert.Equal(t, process.ErrTimeIsOut, r)
}

func TestBootstrap_ShouldReturnTimeIsOutWhenMissingBody(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1, PubKeysBitmap: []byte("X")}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	hash := []byte("aaa")

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			if bytes.Equal(hash, key) {
				return &block.Header{Nonce: 2}, nil
			}

			return nil, errors.New("err")
		}
		return sds
	}
	args.PoolsHolder = pools

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 2
	}
	forkDetector.GetHighestFinalBlockNonceCalled = func() uint64 {
		return 1
	}
	forkDetector.GetNotarizedHeaderHashCalled = func(nonce uint64) []byte {
		return nil
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(2*100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)
	bs.RequestHeaderWithNonce(2)
	r := bs.SyncBlock()

	assert.Equal(t, process.ErrTimeIsOut, r)
}

func TestBootstrap_ShouldNotNeedToSync(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1, Round: 0}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc
	args.BlockProcessor = createBlockProcessor(args.ChainHandler)

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.GetHighestFinalBlockNonceCalled = func() uint64 {
		return hdr.Nonce
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 1
	}
	forkDetector.GetNotarizedHeaderHashCalled = func(nonce uint64) []byte {
		return nil
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewShardBootstrap(args)

	bs.StartSyncingBlocks()
	time.Sleep(200 * time.Millisecond)
	_ = bs.Close()
}

func TestBootstrap_SyncShouldSyncOneBlock(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1, Round: 0}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc
	args.BlockProcessor = createBlockProcessor(args.ChainHandler)

	hash := []byte("aaa")

	mutDataAvailable := goSync.RWMutex{}
	dataAvailable := false

	pools := testscommon.NewPoolsHolderStub()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			mutDataAvailable.RLock()
			defer mutDataAvailable.RUnlock()

			if bytes.Equal(hash, key) && dataAvailable {
				return &block.Header{
					Nonce:         2,
					Round:         1,
					BlockBodyType: block.TxBlock,
					RootHash:      []byte("bbb")}, nil
			}

			return nil, errors.New("err")
		}

		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
		}

		return sds
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		cs := testscommon.NewCacherStub()
		cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {
		}
		cs.GetCalled = func(key []byte) (value interface{}, ok bool) {
			if bytes.Equal([]byte("bbb"), key) && dataAvailable {
				return make(block.MiniBlockSlice, 0), true
			}

			return nil, false
		}

		return cs
	}
	args.PoolsHolder = pools

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.GetHighestFinalBlockNonceCalled = func() uint64 {
		return hdr.Nonce
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 2
	}
	forkDetector.GetNotarizedHeaderHashCalled = func(nonce uint64) []byte {
		return nil
	}
	args.ForkDetector = forkDetector

	account := &testscommon.AccountsStub{}
	account.RootHashCalled = func() ([]byte, error) {
		return nil, nil
	}
	args.Accounts = account
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(200*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)
	bs.StartSyncingBlocks()

	time.Sleep(200 * time.Millisecond)

	mutDataAvailable.Lock()
	dataAvailable = true
	mutDataAvailable.Unlock()

	time.Sleep(500 * time.Millisecond)

	_ = bs.Close()
}

func TestBootstrap_ShouldReturnNilErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc
	args.BlockProcessor = createBlockProcessor(args.ChainHandler)

	hash := []byte("aaa")
	header := &block.Header{
		Nonce:         2,
		Round:         1,
		BlockBodyType: block.TxBlock,
		RootHash:      []byte("bbb")}

	pools := testscommon.NewPoolsHolderStub()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 2 {
				return []data.HeaderHandler{header}, [][]byte{hash}, nil
			}

			return nil, nil, errors.New("err")
		}

		return sds
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		cs := testscommon.NewCacherStub()
		cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {
		}
		cs.GetCalled = func(key []byte) (value interface{}, ok bool) {
			if bytes.Equal([]byte("bbb"), key) {
				return make(block.MiniBlockSlice, 0), true
			}

			return nil, false
		}

		return cs
	}
	args.PoolsHolder = pools

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 2
	}
	forkDetector.GetNotarizedHeaderHashCalled = func(nonce uint64) []byte {
		return nil
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(2*100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)
	r := bs.SyncBlock()

	assert.Nil(t, r)
}

func TestBootstrap_SyncBlockShouldReturnErrorWhenProcessBlockFailed(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1, PubKeysBitmap: []byte("X")}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	blockProcessor := createBlockProcessor(args.ChainHandler)
	blockProcessor.ProcessBlockCalled = func(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
		return process.ErrBlockHashDoesNotMatch
	}
	args.BlockProcessor = blockProcessor

	hash := []byte("aaa")
	header := &block.Header{
		Nonce:         2,
		Round:         1,
		BlockBodyType: block.TxBlock,
		RootHash:      []byte("bbb")}

	pools := testscommon.NewPoolsHolderStub()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 2 {
				return []data.HeaderHandler{header}, [][]byte{hash}, nil
			}
			return nil, nil, errors.New("err")
		}

		return sds
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		cs := testscommon.NewCacherStub()
		cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {
		}
		cs.GetCalled = func(key []byte) (value interface{}, ok bool) {
			if bytes.Equal([]byte("bbb"), key) {
				return make(block.MiniBlockSlice, 0), true
			}

			return nil, false
		}

		return cs
	}
	args.PoolsHolder = pools

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.GetHighestFinalBlockNonceCalled = func() uint64 {
		return hdr.Nonce
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 2
	}
	forkDetector.RemoveHeaderCalled = func(nonce uint64, hash []byte) {}
	forkDetector.GetNotarizedHeaderHashCalled = func(nonce uint64) []byte {
		return nil
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(2*100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)

	err := bs.SyncBlock()
	assert.Equal(t, process.ErrBlockHashDoesNotMatch, err)
}

func TestBootstrap_GetNodeStateShouldReturnSynchronizedWhenCurrentBlockIsNilAndRoundIndexIsZero(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	forkDetector := &mock.ForkDetectorMock{
		CheckForkCalled: func() *process.ForkInfo {
			return process.NewForkInfo()
		},
		ProbableHighestNonceCalled: func() uint64 {
			return 0
		},
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewShardBootstrap(args)
	bs.ComputeNodeState()

	assert.Equal(t, core.NsSynchronized, bs.GetNodeState())
}

func TestBootstrap_GetNodeStateShouldReturnNotSynchronizedWhenCurrentBlockIsNilAndRoundIndexIsGreaterThanZero(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 1
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)
	bs.ComputeNodeState()

	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
}

func TestBootstrap_GetNodeStateShouldReturnSynchronizedWhenNodeIsSynced(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 0}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 0
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewShardBootstrap(args)
	bs.ComputeNodeState()

	assert.Equal(t, core.NsSynchronized, bs.GetNodeState())
}

func TestBootstrap_GetNodeStateShouldReturnNotSynchronizedWhenNodeIsNotSynced(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 0}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 1
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)
	bs.ComputeNodeState()

	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
}

func TestBootstrap_GetNodeStateShouldReturnNotSynchronizedWhenForkIsDetectedAndItReceivesTheSameWrongHeader(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr1 := block.Header{Nonce: 1, Round: 2, PubKeysBitmap: []byte("A")}
	hash1 := []byte("hash1")

	hdr2 := block.Header{Nonce: 1, Round: 1, PubKeysBitmap: []byte("B")}
	hash2 := []byte("hash2")

	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr1
	}
	args.ChainHandler = blkc

	selfNotarizedHeaders := []data.HeaderHandler{
		&hdr2,
	}
	selfNotarizedHeadersHashes := [][]byte{
		hash2,
	}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{
			RegisterHandlerCalled: func(func(header data.HeaderHandler, key []byte)) {},
			GetHeaderByHashCalled: func(key []byte) (handler data.HeaderHandler, e error) {
				if bytes.Equal(key, hash1) {
					return &hdr1, nil
				}
				if bytes.Equal(key, hash2) {
					return &hdr2, nil
				}

				return nil, errors.New("err")
			},
		}
		return sds
	}
	args.PoolsHolder = pools
	args.RoundHandler = &mock.RoundHandlerMock{RoundIndex: 2}
	args.ForkDetector, _ = sync.NewShardForkDetector(
		args.RoundHandler,
		&mock.BlackListHandlerStub{},
		&mock.BlockTrackerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)

	_ = args.ForkDetector.AddHeader(&hdr1, hash1, process.BHProcessed, nil, nil)
	_ = args.ForkDetector.AddHeader(&hdr2, hash2, process.BHNotarized, selfNotarizedHeaders, selfNotarizedHeadersHashes)

	bs.ComputeNodeState()
	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
	assert.True(t, bs.IsForkDetected())

	if bs.GetNodeState() == core.NsNotSynchronized && bs.IsForkDetected() {
		args.ForkDetector.RemoveHeader(hdr1.GetNonce(), hash1)
		bs.ReceivedHeaders(&hdr1, hash1)
		_ = args.ForkDetector.AddHeader(&hdr1, hash1, process.BHProcessed, nil, nil)
	}

	bs.ComputeNodeState()
	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
	assert.True(t, bs.IsForkDetected())
}

func TestBootstrap_GetNodeStateShouldReturnSynchronizedWhenForkIsDetectedAndItReceivesTheGoodHeader(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr1 := block.Header{Nonce: 1, Round: 2, PubKeysBitmap: []byte("A")}
	hash1 := []byte("hash1")

	hdr2 := block.Header{Nonce: 1, Round: 1, PubKeysBitmap: []byte("B")}
	hash2 := []byte("hash2")

	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr2
	}
	args.ChainHandler = blkc

	selfNotarizedHeaders := []data.HeaderHandler{
		&hdr2,
	}
	selfNotarizedHeadersHashes := [][]byte{
		hash2,
	}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{
			RegisterHandlerCalled: func(func(header data.HeaderHandler, key []byte)) {},
			GetHeaderByHashCalled: func(key []byte) (handler data.HeaderHandler, e error) {
				if bytes.Equal(key, hash1) {
					return &hdr1, nil
				}
				if bytes.Equal(key, hash2) {
					return &hdr2, nil
				}

				return nil, errors.New("err")
			},
		}
		return sds
	}
	args.PoolsHolder = pools

	args.RoundHandler = &mock.RoundHandlerMock{RoundIndex: 2}
	args.ForkDetector, _ = sync.NewShardForkDetector(
		args.RoundHandler,
		&mock.BlackListHandlerStub{},
		&mock.BlockTrackerMock{},
		0,
	)

	bs, _ := sync.NewShardBootstrap(args)

	_ = args.ForkDetector.AddHeader(&hdr1, hash1, process.BHProcessed, nil, nil)
	_ = args.ForkDetector.AddHeader(&hdr2, hash2, process.BHNotarized, selfNotarizedHeaders, selfNotarizedHeadersHashes)

	bs.ComputeNodeState()
	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
	assert.True(t, bs.IsForkDetected())

	if bs.GetNodeState() == core.NsNotSynchronized && bs.IsForkDetected() {
		args.ForkDetector.RemoveHeader(hdr1.GetNonce(), hash1)
		bs.ReceivedHeaders(&hdr2, hash2)
		_ = args.ForkDetector.AddHeader(&hdr2, hash2, process.BHProcessed, selfNotarizedHeaders, selfNotarizedHeadersHashes)
		bs.SetNodeStateCalculated(false)
	}

	bs.ComputeNodeState()
	assert.Equal(t, core.NsSynchronized, bs.GetNodeState())
	assert.False(t, bs.IsForkDetected())
}

func TestBootstrap_GetHeaderFromPoolShouldReturnNil(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewShardBootstrap(args)
	hdr, _, _ := process.GetShardHeaderFromPoolWithNonce(0, 0, args.PoolsHolder.Headers())

	assert.NotNil(t, bs)
	assert.Nil(t, hdr)
}

func TestBootstrap_GetHeaderFromPoolShouldReturnHeader(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := &block.Header{Nonce: 0}
	hash := []byte("aaa")

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}

		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 0 {
				return []data.HeaderHandler{hdr}, [][]byte{hash}, nil
			}
			return nil, nil, errors.New("err")
		}

		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
		}

		return sds
	}
	args.PoolsHolder = pools
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewShardBootstrap(args)
	hdr2, _, _ := process.GetShardHeaderFromPoolWithNonce(0, 0, pools.Headers())

	assert.NotNil(t, bs)
	assert.True(t, hdr == hdr2)
}

func TestShardGetBlockFromPoolShouldReturnBlock(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	mbsAndHashes := make([]*block.MiniblockAndHash, 0)
	args.RoundHandler = initRoundHandler()
	args.MiniblocksProvider = &mock.MiniBlocksProviderStub{
		GetMiniBlocksCalled: func(hashes [][]byte) ([]*block.MiniblockAndHash, [][]byte) {
			return mbsAndHashes, nil
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	mbHashes := make([][]byte, 0)
	mbHashes = append(mbHashes, []byte("aaaa"))
	gotMbsAndHashes, _ := bs.GetMiniBlocks(mbHashes)

	assert.True(t, reflect.DeepEqual(mbsAndHashes, gotMbsAndHashes))
}

//------- testing received headers

func TestBootstrap_ReceivedHeadersFoundInPoolShouldAddToForkDetector(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	addedHash := []byte("hash")
	addedHdr := &block.Header{}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
		}
		sds.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			if bytes.Equal(key, addedHash) {
				return addedHdr, nil
			}

			return nil, errors.New("err")
		}

		return sds
	}
	args.PoolsHolder = pools

	wasAdded := false

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.AddHeaderCalled = func(header data.HeaderHandler, hash []byte, state process.BlockHeaderState, selfNotarizedHeaders []data.HeaderHandler, selfNotarizedHeadersHashes [][]byte) error {
		if state == process.BHProcessed {
			return errors.New("processed")
		}

		if !bytes.Equal(hash, addedHash) {
			return errors.New("hash mismatch")
		}

		if !reflect.DeepEqual(header, addedHdr) {
			return errors.New("header mismatch")
		}

		wasAdded = true
		return nil
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 0
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewShardBootstrap(args)
	bs.ReceivedHeaders(addedHdr, addedHash)

	assert.True(t, wasAdded)
}

//------- RollBack

func TestBootstrap_RollBackNilBlockchainHeaderShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	bs, _ := sync.NewShardBootstrap(args)
	err := bs.RollBack(false)

	assert.Equal(t, process.ErrNilBlockHeader, err)
}

func TestBootstrap_RollBackNilParamHeaderShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	blkc := initBlockchain()
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return nil
	}
	args.ChainHandler = blkc

	bs, _ := sync.NewShardBootstrap(args)
	err := bs.RollBack(false)

	assert.Equal(t, process.ErrNilBlockHeader, err)
}

func TestBootstrap_RollBackIsNotEmptyShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	newHdrHash := []byte("new hdr hash")
	newHdrNonce := uint64(6)

	remFlags := &removedFlags{}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{
			RemoveHeaderByHashCalled: func(key []byte) {
				if bytes.Equal(key, newHdrHash) {
					remFlags.flagHdrRemovedFromHeaders = true
				}
			},
		}
		return sds
	}
	args.PoolsHolder = pools

	blkc := initBlockchain()
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &block.Header{
			PubKeysBitmap: []byte("X"),
			Nonce:         newHdrNonce,
		}
	}
	args.ChainHandler = blkc
	args.ForkDetector = createForkDetector(newHdrNonce, remFlags)

	bs, _ := sync.NewShardBootstrap(args)
	err := bs.RollBack(false)

	assert.Equal(t, sync.ErrRollBackBehindFinalHeader, err)
}

func TestBootstrap_RollBackIsEmptyCallRollBackOneBlockOkValsShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	//retain if the remove process from different storage locations has been called
	remFlags := &removedFlags{}
	currentHdrNonce := uint64(8)
	currentHdrHash := []byte("current header hash")

	//define prev tx block body "strings" as in this test there are a lot of stubs that
	//constantly need to check some defined symbols
	//prevTxBlockBodyHash := []byte("prev block body hash")
	prevTxBlockBodyBytes := []byte("prev block body bytes")
	prevTxBlockBody := &block.Body{}

	//define prev header "strings"
	prevHdrHash := []byte("prev header hash")
	prevHdrBytes := []byte("prev header bytes")
	prevHdrRootHash := []byte("prev header root hash")
	prevHdr := &block.Header{
		Signature: []byte("sig of the prev header as to be unique in this context"),
		RootHash:  prevHdrRootHash,
	}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{
			RemoveHeaderByHashCalled: func(key []byte) {
				if bytes.Equal(key, currentHdrHash) {
					remFlags.flagHdrRemovedFromHeaders = true
				}
			},
		}
		return sds
	}
	args.PoolsHolder = pools

	//a mock blockchain with special header and tx block bodies stubs (defined above)
	blkc := &mock.BlockChainMock{}
	hdr := &block.Header{
		Nonce: currentHdrNonce,
		//empty bitmap
		PrevHash: prevHdrHash,
	}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return hdr
	}
	blkc.SetCurrentBlockHeaderCalled = func(handler data.HeaderHandler) error {
		hdr = prevHdr
		return nil
	}

	hdrHash := make([]byte, 0)
	blkc.GetCurrentBlockHeaderHashCalled = func() []byte {
		return hdrHash
	}
	blkc.SetCurrentBlockHeaderHashCalled = func(i []byte) {
		hdrHash = i
	}
	args.ChainHandler = blkc
	args.Store = &mock.ChainStorerMock{
		GetStorerCalled: func(unitType dataRetriever.UnitType) storage.Storer {
			return &testscommon.StorerStub{
				GetCalled: func(key []byte) ([]byte, error) {
					return prevHdrBytes, nil
				},
				RemoveCalled: func(key []byte) error {
					remFlags.flagHdrRemovedFromStorage = true
					return nil
				},
			}
		},
	}
	args.BlockProcessor = &mock.BlockProcessorMock{
		RestoreBlockIntoPoolsCalled: func(header data.HeaderHandler, body data.BodyHandler) error {
			return nil
		},
	}
	args.Hasher = &mock.HasherStub{
		ComputeCalled: func(s string) []byte {
			return currentHdrHash
		},
	}
	args.Marshalizer = &mock.MarshalizerStub{
		MarshalCalled: func(obj interface{}) ([]byte, error) {
			return []byte("X"), nil
		},
		UnmarshalCalled: func(obj interface{}, buff []byte) error {
			if bytes.Equal(buff, prevHdrBytes) {
				_, ok := obj.(*block.Header)
				if !ok {
					return nil
				}

				//bytes represent a header (strings are returns from hdrUnit.Get which is also a stub here)
				//copy only defined fields
				obj.(*block.Header).Signature = prevHdr.Signature
				obj.(*block.Header).RootHash = prevHdrRootHash
				return nil
			}
			if bytes.Equal(buff, prevTxBlockBodyBytes) {
				//bytes represent a tx block body (strings are returns from txBlockUnit.Get which is also a stub here)
				//copy only defined fields
				_, ok := obj.(*block.Body)
				if !ok {
					return nil
				}

				obj.(*block.Body).MiniBlocks = prevTxBlockBody.MiniBlocks
				return nil
			}

			return nil
		},
	}
	args.ForkDetector = createForkDetector(currentHdrNonce, remFlags)
	args.Accounts = &testscommon.AccountsStub{
		RecreateTrieCalled: func(rootHash []byte) error {
			return nil
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	bs.SetForkNonce(currentHdrNonce)
	err := bs.RollBack(true)

	assert.Nil(t, err)
	assert.True(t, remFlags.flagHdrRemovedFromHeaders)
	assert.True(t, remFlags.flagHdrRemovedFromStorage)
	assert.True(t, remFlags.flagHdrRemovedFromForkDetector)
	assert.Equal(t, blkc.GetCurrentBlockHeader(), prevHdr)
	assert.Equal(t, blkc.GetCurrentBlockHeaderHash(), prevHdrHash)
}

func TestBootstrap_RollbackIsEmptyCallRollBackOneBlockToGenesisShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	//retain if the remove process from different storage locations has been called
	remFlags := &removedFlags{}

	currentHdrNonce := uint64(1)
	currentHdrHash := []byte("current header hash")

	//define prev tx block body "strings" as in this test there are a lot of stubs that
	//constantly need to check some defined symbols
	//prevTxBlockBodyHash := []byte("prev block body hash")
	prevTxBlockBodyBytes := []byte("prev block body bytes")
	prevTxBlockBody := &block.Body{}

	//define prev header "strings"
	prevHdrHash := []byte("prev header hash")
	prevHdrBytes := []byte("prev header bytes")
	prevHdrRootHash := []byte("prev header root hash")
	prevHdr := &block.Header{
		Signature: []byte("sig of the prev header as to be unique in this context"),
		RootHash:  prevHdrRootHash,
	}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{
			RemoveHeaderByHashCalled: func(key []byte) {
				if bytes.Equal(key, currentHdrHash) {
					remFlags.flagHdrRemovedFromHeaders = true
				}
			},
		}
		return sds
	}
	args.PoolsHolder = pools

	//a mock blockchain with special header and tx block bodies stubs (defined above)
	blkc := &mock.BlockChainMock{
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return prevHdr
		},
	}
	hdr := &block.Header{
		Nonce: currentHdrNonce,
		//empty bitmap
		PrevHash: prevHdrHash,
	}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return hdr
	}
	blkc.SetCurrentBlockHeaderCalled = func(handler data.HeaderHandler) error {
		hdr = nil
		return nil
	}

	hdrHash := make([]byte, 0)
	blkc.GetCurrentBlockHeaderHashCalled = func() []byte {
		return hdrHash
	}
	blkc.SetCurrentBlockHeaderHashCalled = func(i []byte) {
		hdrHash = nil
	}
	args.ChainHandler = blkc
	args.Store = &mock.ChainStorerMock{
		GetStorerCalled: func(unitType dataRetriever.UnitType) storage.Storer {
			return &testscommon.StorerStub{
				GetCalled: func(key []byte) ([]byte, error) {
					return prevHdrBytes, nil
				},
				RemoveCalled: func(key []byte) error {
					remFlags.flagHdrRemovedFromStorage = true
					return nil
				},
			}
		},
	}
	args.BlockProcessor = &mock.BlockProcessorMock{
		RestoreBlockIntoPoolsCalled: func(header data.HeaderHandler, body data.BodyHandler) error {
			return nil
		},
	}
	args.Hasher = &mock.HasherStub{
		ComputeCalled: func(s string) []byte {
			return currentHdrHash
		},
	}
	args.Marshalizer = &mock.MarshalizerStub{
		MarshalCalled: func(obj interface{}) ([]byte, error) {
			return []byte("X"), nil
		},
		UnmarshalCalled: func(obj interface{}, buff []byte) error {
			if bytes.Equal(buff, prevHdrBytes) {
				_, ok := obj.(*block.Header)
				if !ok {
					return nil
				}

				//bytes represent a header (strings are returns from hdrUnit.Get which is also a stub here)
				//copy only defined fields
				obj.(*block.Header).Signature = prevHdr.Signature
				obj.(*block.Header).RootHash = prevHdrRootHash
				return nil
			}
			if bytes.Equal(buff, prevTxBlockBodyBytes) {
				//bytes represent a tx block body (strings are returns from txBlockUnit.Get which is also a stub here)
				//copy only defined fields
				_, ok := obj.(*block.Body)
				if !ok {
					return nil
				}

				obj.(*block.Body).MiniBlocks = prevTxBlockBody.MiniBlocks
				return nil
			}

			return nil
		},
	}
	args.ForkDetector = createForkDetector(currentHdrNonce, remFlags)
	args.Accounts = &testscommon.AccountsStub{
		RecreateTrieCalled: func(rootHash []byte) error {
			return nil
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	bs.SetForkNonce(currentHdrNonce)
	err := bs.RollBack(true)

	assert.Nil(t, err)
	assert.True(t, remFlags.flagHdrRemovedFromHeaders)
	assert.True(t, remFlags.flagHdrRemovedFromStorage)
	assert.True(t, remFlags.flagHdrRemovedFromForkDetector)
	assert.Nil(t, blkc.GetCurrentBlockHeader())
	assert.Nil(t, blkc.GetCurrentBlockHeaderHash())
}

//------- GetTxBodyHavingHash

func TestBootstrap_GetTxBodyHavingHashReturnsFromCacherShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	mbh := []byte("requested hash")
	requestedHash := make([][]byte, 0)
	requestedHash = append(requestedHash, mbh)
	mbsAndHashes := make([]*block.MiniblockAndHash, 0)

	blkc, _ := blockchain.NewBlockChain(&mock.AppStatusHandlerStub{
		SetUInt64ValueHandler: func(key string, value uint64) {},
	})
	args.ChainHandler = blkc
	args.MiniblocksProvider = &mock.MiniBlocksProviderStub{
		GetMiniBlocksCalled: func(hashes [][]byte) ([]*block.MiniblockAndHash, [][]byte) {
			for _, hash := range hashes {
				if bytes.Equal(hash, mbh) {
					return mbsAndHashes, nil
				}
			}

			return nil, nil
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	gotMbsAndHashes, _ := bs.GetMiniBlocks(requestedHash)

	assert.True(t, reflect.DeepEqual(gotMbsAndHashes, mbsAndHashes))
}

func TestBootstrap_GetTxBodyHavingHashNotFoundInCacherOrStorageShouldRetEmptySlice(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	mbh := []byte("requested hash")
	requestedHash := make([][]byte, 0)
	requestedHash = append(requestedHash, mbh)

	txBlockUnit := &testscommon.StorerStub{
		GetCalled: func(key []byte) (i []byte, e error) {
			return nil, errors.New("not found")
		},
	}

	blkc, _ := blockchain.NewBlockChain(&mock.AppStatusHandlerStub{
		SetUInt64ValueHandler: func(key string, value uint64) {},
	})
	args.ChainHandler = blkc
	args.Store = createFullStore()
	args.Store.AddStorer(dataRetriever.TransactionUnit, txBlockUnit)

	bs, _ := sync.NewShardBootstrap(args)
	gotMbsAndHashes, _ := bs.GetMiniBlocks(requestedHash)

	assert.Equal(t, 0, len(gotMbsAndHashes))
}

func TestBootstrap_GetTxBodyHavingHashFoundInStorageShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	mbh := []byte("requested hash")
	requestedHash := make([][]byte, 0)
	requestedHash = append(requestedHash, mbh)
	mbsAndHashes := make([]*block.MiniblockAndHash, 0)

	blkc, _ := blockchain.NewBlockChain(&mock.AppStatusHandlerStub{
		SetUInt64ValueHandler: func(key string, value uint64) {},
	})

	args.ChainHandler = blkc
	args.Store = createFullStore()
	args.MiniblocksProvider = &mock.MiniBlocksProviderStub{
		GetMiniBlocksCalled: func(hashes [][]byte) ([]*block.MiniblockAndHash, [][]byte) {
			for _, hash := range hashes {
				if bytes.Equal(hash, mbh) {
					return mbsAndHashes, nil
				}
			}

			return nil, nil
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	gotMbsAndHashes, _ := bs.GetMiniBlocks(requestedHash)

	assert.Equal(t, mbsAndHashes, gotMbsAndHashes)
}

func TestBootstrap_AddSyncStateListenerShouldAppendAnotherListener(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.BlockProcessor = createBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewShardBootstrap(args)
	f1 := func(bool) {}
	f2 := func(bool) {}
	f3 := func(bool) {}
	bs.AddSyncStateListener(f1)
	bs.AddSyncStateListener(f2)
	bs.AddSyncStateListener(f3)

	assert.Equal(t, 3, len(bs.SyncStateListeners()))
}

func TestBootstrap_NotifySyncStateListenersShouldNotify(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	args.BlockProcessor = createBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewShardBootstrap(args)

	mutex := goSync.RWMutex{}
	mutex.RLock()
	calls := 0
	mutex.RUnlock()
	var wg goSync.WaitGroup

	f1 := func(bool) {
		mutex.Lock()
		calls++
		mutex.Unlock()
		wg.Done()
	}

	f2 := func(bool) {
		mutex.Lock()
		calls++
		mutex.Unlock()
		wg.Done()
	}

	f3 := func(bool) {
		mutex.Lock()
		calls++
		mutex.Unlock()
		wg.Done()
	}

	wg.Add(3)

	bs.AddSyncStateListener(f1)
	bs.AddSyncStateListener(f2)
	bs.AddSyncStateListener(f3)

	bs.NotifySyncStateListeners()

	wg.Wait()

	assert.Equal(t, 3, calls)
}

func TestShardBootstrap_RequestMiniBlocksFromHeaderWithNonceIfMissing(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	requestDataWasCalled := false
	hdrHash := []byte("hash")
	hdr := &block.Header{Round: 5, Nonce: 1}
	pools := testscommon.NewPoolsHolderStub()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
		}
		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			return []data.HeaderHandler{hdr}, [][]byte{[]byte("hash")}, nil
		}
		sds.GetHeaderByHashCalled = func(hash []byte) (handler data.HeaderHandler, err error) {
			if bytes.Equal(hash, hdrHash) {
				return hdr, nil
			}
			return nil, nil
		}

		return sds
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		cs := testscommon.NewCacherStub()
		cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {
		}

		return cs
	}
	args.PoolsHolder = pools

	blkc := initBlockchain()
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &block.Header{Round: 10}
	}
	args.ChainHandler = blkc
	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return uint64(5)
	}
	args.ForkDetector = forkDetector

	store := createStore()
	store.GetCalled = func(unitType dataRetriever.UnitType, key []byte) ([]byte, error) {
		nonceToBytes := mock.NewNonceHashConverterMock().ToByteSlice(uint64(1))
		if bytes.Equal(key, nonceToBytes) {
			return []byte("hdr"), nil
		}
		if bytes.Equal(key, []byte("hdr")) {
			newHdr := block.Header{}
			mshlzdHdr, _ := json.Marshal(newHdr)
			return mshlzdHdr, nil
		}

		return nil, nil
	}
	store.GetAllCalled = func(unitType dataRetriever.UnitType, keys [][]byte) (map[string][]byte, error) {
		mapToRet := make(map[string][]byte)
		mb := block.MiniBlock{ReceiverShardID: 1, SenderShardID: 0}
		mshlzdMb, _ := json.Marshal(mb)
		mapToRet["mb1"] = mshlzdMb
		return mapToRet, nil
	}
	args.Store = store
	args.RequestHandler = &testscommon.RequestHandlerStub{
		RequestMiniBlocksHandlerCalled: func(destShardID uint32, miniblocksHashes [][]byte) {
			requestDataWasCalled = true
		},
	}
	args.MiniblocksProvider = &mock.MiniBlocksProviderStub{
		GetMiniBlocksFromPoolCalled: func(hashes [][]byte) ([]*block.MiniblockAndHash, [][]byte) {
			return make([]*block.MiniblockAndHash, 0), [][]byte{[]byte("hash")}
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	bs.RequestMiniBlocksFromHeaderWithNonceIfMissing(hdr)

	assert.True(t, requestDataWasCalled)
}

func TestShardBootstrap_DoJobOnSyncBlockFailShouldNotResetProbableHighestNonceWhenAreNotEnoughErrorsPerNonce(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	wasCalled := false
	forkDetectorMock := &mock.ForkDetectorMock{
		ResetProbableHighestNonceCalled: func() {
			wasCalled = true
		},
	}
	args.ForkDetector = forkDetectorMock
	args.ChainHandler = &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 1}
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	bs.SetNumSyncedWithErrorsForNonce(2, 8)
	bs.DoJobOnSyncBlockFail(nil, nil, errors.New("error"))

	assert.False(t, wasCalled)
}

func TestShardBootstrap_DoJobOnSyncBlockFailShouldNotResetProbableHighestNonceWhenIsNotInProperRound(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	wasCalled := false
	forkDetectorMock := &mock.ForkDetectorMock{
		ResetProbableHighestNonceCalled: func() {
			wasCalled = true
		},
	}
	args.ForkDetector = forkDetectorMock
	args.ChainHandler = &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 1}
		},
	}

	roundHandlerMock := &mock.RoundHandlerMock{}
	roundHandlerMock.RoundIndex = 1
	args.RoundHandler = roundHandlerMock

	bs, _ := sync.NewShardBootstrap(args)
	bs.SetNumSyncedWithErrorsForNonce(2, 9)
	bs.DoJobOnSyncBlockFail(nil, nil, errors.New("error"))

	assert.False(t, wasCalled)
}

func TestShardBootstrap_DoJobOnSyncBlockFailShouldResetProbableHighestNonce(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	wasCalled := false
	forkDetectorMock := &mock.ForkDetectorMock{
		GetHighestFinalBlockNonceCalled: func() uint64 {
			return 1
		},
		ResetProbableHighestNonceCalled: func() {
			wasCalled = true
		},
	}
	args.ForkDetector = forkDetectorMock
	args.ChainHandler = &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 1}
		},
	}

	bs, _ := sync.NewShardBootstrap(args)
	bs.SetNumSyncedWithErrorsForNonce(2, 9)
	bs.DoJobOnSyncBlockFail(nil, nil, errors.New("error"))

	assert.True(t, wasCalled)
}

func TestShardBootstrap_CleanNoncesSyncedWithErrorsBehindFinalShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()
	forkDetectorMock := &mock.ForkDetectorMock{
		GetHighestFinalBlockNonceCalled: func() uint64 {
			return 3
		},
	}
	args.ForkDetector = forkDetectorMock

	bs, _ := sync.NewShardBootstrap(args)
	bs.SetNumSyncedWithErrorsForNonce(1, 7)
	bs.SetNumSyncedWithErrorsForNonce(2, 8)
	bs.SetNumSyncedWithErrorsForNonce(3, 9)

	assert.Equal(t, 3, bs.GetMapNonceSyncedWithErrorsLen())

	bs.CleanNoncesSyncedWithErrorsBehindFinal()

	assert.Equal(t, 1, bs.GetMapNonceSyncedWithErrorsLen())
	assert.Equal(t, uint32(9), bs.GetNumSyncedWithErrorsForNonce(3))
}

func TestShardBootstrap_SyncBlockGetNodeDBErrorShouldSync(t *testing.T) {
	t.Parallel()

	args := CreateShardBootstrapMockArguments()

	hdr := block.Header{Nonce: 1, PubKeysBitmap: []byte("X")}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	errGetNodeFromDB := errors.New(core.GetNodeFromDBErrorString)
	blockProcessor := createBlockProcessor(args.ChainHandler)
	blockProcessor.ProcessBlockCalled = func(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
		return errGetNodeFromDB
	}
	args.BlockProcessor = blockProcessor

	hash := []byte("aaa")
	header := &block.Header{
		Nonce:         2,
		Round:         1,
		BlockBodyType: block.TxBlock,
		RootHash:      []byte("bbb")}

	pools := testscommon.NewPoolsHolderStub()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 2 {
				return []data.HeaderHandler{header}, [][]byte{hash}, nil
			}
			return nil, nil, errors.New("err")
		}

		return sds
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		cs := testscommon.NewCacherStub()
		cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {
		}
		cs.GetCalled = func(key []byte) (value interface{}, ok bool) {
			if bytes.Equal([]byte("bbb"), key) {
				return make(block.MiniBlockSlice, 0), true
			}

			return nil, false
		}

		return cs
	}
	args.PoolsHolder = pools

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.GetHighestFinalBlockNonceCalled = func() uint64 {
		return hdr.Nonce
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 2
	}
	forkDetector.RemoveHeaderCalled = func(nonce uint64, hash []byte) {}
	forkDetector.GetNotarizedHeaderHashCalled = func(nonce uint64) []byte {
		return nil
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(
		time.Now(),
		time.Now().Add(2*100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)

	syncCalled := false
	args.AccountsDBSyncer = &mock.AccountsDBSyncerStub{
		SyncAccountsCalled: func(rootHash []byte) error {
			syncCalled = true
			return nil
		}}
	args.Accounts = &testscommon.AccountsStub{RootHashCalled: func() ([]byte, error) {
		return []byte("roothash"), nil
	}}

	bs, _ := sync.NewShardBootstrap(args)

	err := bs.SyncBlock()
	assert.Equal(t, errGetNodeFromDB, err)
	assert.True(t, syncCalled)
}

func TestShardBootstrap_NilInnerBootstrapperClose(t *testing.T) {
	t.Parallel()

	bootstrapper := &sync.ShardBootstrap{}
	assert.Nil(t, bootstrapper.Close())
}
