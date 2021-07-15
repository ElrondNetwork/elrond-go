package sync_test

import (
	"bytes"
	"errors"
	"reflect"
	goSync "sync"
	"testing"
	"time"

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
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/stretchr/testify/assert"
)

func createMetaBlockProcessor(blk data.ChainHandler) *mock.BlockProcessorMock {
	blockProcessorMock := &mock.BlockProcessorMock{
		ProcessBlockCalled: func(hdr data.HeaderHandler, bdy data.BodyHandler, haveTime func() time.Duration) error {
			_ = blk.SetCurrentBlockHeader(hdr.(*block.MetaBlock))
			return nil
		},
		RevertAccountStateCalled: func() {
		},
		CommitBlockCalled: func(header data.HeaderHandler, body data.BodyHandler) error {
			return nil
		},
		ProcessScheduledBlockCalled: func(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
			return nil
		},
	}

	return blockProcessorMock
}

func createMetaStore() dataRetriever.StorageService {
	store := dataRetriever.NewChainStorer()
	store.AddStorer(dataRetriever.MetaBlockUnit, generateTestUnit())
	store.AddStorer(dataRetriever.ShardHdrNonceHashDataUnit, generateTestUnit())
	return store
}

func CreateMetaBootstrapMockArguments() sync.ArgMetaBootstrapper {
	argsBaseBootstrapper := sync.ArgBaseBootstrapper{
		PoolsHolder:                  createMockPools(),
		Store:                        createStore(),
		ChainHandler:                 initBlockchain(),
		RoundHandler:                 &mock.RoundHandlerMock{},
		BlockProcessor:               &mock.BlockProcessorMock{},
		WaitTime:                     waitTime,
		Hasher:                       &mock.HasherMock{},
		Marshalizer:                  &mock.MarshalizerMock{},
		ForkDetector:                 &mock.ForkDetectorMock{},
		RequestHandler:               &testscommon.RequestHandlerStub{},
		ShardCoordinator:             mock.NewOneShardCoordinatorMock(),
		Accounts:                     &testscommon.AccountsStub{},
		BlackListHandler:             &mock.BlackListHandlerStub{},
		NetworkWatcher:               initNetworkWatcher(),
		BootStorer:                   &mock.BoostrapStorerMock{},
		StorageBootstrapper:          &mock.StorageBootstrapperMock{},
		EpochHandler:                 &mock.EpochStartTriggerStub{},
		MiniblocksProvider:           &mock.MiniBlocksProviderStub{},
		Uint64Converter:              &mock.Uint64ByteSliceConverterMock{},
		AppStatusHandler:             &mock.AppStatusHandlerStub{},
		Indexer:                      &mock.IndexerMock{},
		AccountsDBSyncer:             &mock.AccountsDBSyncerStub{},
		ScheduledTxsExecutionHandler: &testscommon.ScheduledTxsExecutionStub{},
	}

	argsMetaBootstrapper := sync.ArgMetaBootstrapper{
		ArgBaseBootstrapper:         argsBaseBootstrapper,
		EpochBootstrapper:           &mock.EpochStartTriggerStub{},
		ValidatorAccountsDB:         &testscommon.AccountsStub{},
		ValidatorStatisticsDBSyncer: &mock.AccountsDBSyncerStub{},
	}

	return argsMetaBootstrapper
}

//------- NewMetaBootstrap

func TestNewMetaBootstrap_NilPoolsHolderShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.PoolsHolder = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilPoolsHolder, err)
}

func TestNewMetaBootstrap_NilValidatorDBShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.ValidatorAccountsDB = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilPeerAccountsAdapter, err)
}

func TestNewMetaBootstrap_NilValidatorDBSyncerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.ValidatorStatisticsDBSyncer = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilAccountsDBSyncer, err)
}

func TestNewMetaBootstrap_NilUserDBSyncerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.AccountsDBSyncer = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilAccountsDBSyncer, err)
}

func TestNewMetaBootstrap_PoolsHolderRetNilOnHeadersShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		return nil
	}
	args.PoolsHolder = pools

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilMetaBlocksPool, err)
}

func TestNewMetaBootstrap_NilStoreShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.Store = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilStore, err)
}

func TestNewMetaBootstrap_NilAppStatusHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.AppStatusHandler = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilAppStatusHandler, err)
}

func TestNewMetaBootstrap_NilBlockchainShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.ChainHandler = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilBlockChain, err)
}

func TestNewMetaBootstrap_NilRoundHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.RoundHandler = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilRoundHandler, err)
}

func TestNewMetaBootstrap_NilBlockProcessorShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.BlockProcessor = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilBlockProcessor, err)
}

func TestNewMetaBootstrap_NilHasherShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.Hasher = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilHasher, err)
}

func TestNewMetaBootstrap_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.Marshalizer = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilMarshalizer, err)
}

func TestNewMetaBootstrap_NilForkDetectorShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.ForkDetector = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilForkDetector, err)
}

func TestNewMetaBootstrap_NilRequestHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.RequestHandler = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilRequestHandler, err)
}

func TestNewMetaBootstrap_NilShardCoordinatorShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.ShardCoordinator = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilShardCoordinator, err)
}

func TestNewMetaBootstrap_NilAccountsAdapterShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.Accounts = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilAccountsAdapter, err)
}

func TestNewMetaBootstrap_NilBlackListHandlerShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.BlackListHandler = nil

	bs, err := sync.NewMetaBootstrap(args)

	assert.Nil(t, bs)
	assert.Equal(t, process.ErrNilBlackListCacher, err)
}

func TestNewMetaBootstrap_OkValsShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	wasCalled := 0

	pools := createMockPools()
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
	args.PoolsHolder = pools
	args.IsInImportMode = true
	bs, err := sync.NewMetaBootstrap(args)

	assert.NotNil(t, bs)
	assert.Nil(t, err)
	assert.Equal(t, 1, wasCalled)
	assert.False(t, bs.IsInterfaceNil())
	assert.True(t, bs.IsInImportMode())

	args.IsInImportMode = false
	bs, err = sync.NewMetaBootstrap(args)
	assert.NotNil(t, bs)
	assert.Nil(t, err)
	assert.False(t, bs.IsInImportMode())
}

//------- processing

func TestMetaBootstrap_SyncBlockShouldCallRollBack(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	hdr := block.MetaBlock{Nonce: 1, PubKeysBitmap: []byte("X")}

	args.Store = createMetaStore()

	blkc, _ := blockchain.NewMetaChain(&mock.AppStatusHandlerStub{})
	_ = blkc.SetGenesisHeader(&block.MetaBlock{})
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
	args.BlockProcessor = createMetaBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewMetaBootstrap(args)
	r := bs.SyncBlock()

	assert.Equal(t, process.ErrNilHeadersNonceHashStorage, r)
}

func TestMetaBootstrap_ShouldReturnTimeIsOutWhenMissingHeader(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	hdr := block.MetaBlock{Nonce: 1}
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
	args.RoundHandler, _ = round.NewRound(time.Now(),
		time.Now().Add(2*100*time.Millisecond),
		100*time.Millisecond,
		&mock.SyncTimerMock{},
		0,
	)
	args.BlockProcessor = createMetaBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewMetaBootstrap(args)
	r := bs.SyncBlock()

	assert.Equal(t, process.ErrTimeIsOut, r)
}

func TestMetaBootstrap_ShouldNotNeedToSync(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	hdr := block.MetaBlock{Nonce: 1, Round: 0}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc
	args.BlockProcessor = createMetaBlockProcessor(args.ChainHandler)

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.GetHighestFinalBlockNonceCalled = func() uint64 {
		return hdr.Nonce
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 0
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewMetaBootstrap(args)

	bs.StartSyncingBlocks()
	time.Sleep(200 * time.Millisecond)
	_ = bs.Close()
}

func TestMetaBootstrap_SyncShouldSyncOneBlock(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	hdr := block.MetaBlock{Nonce: 1, Round: 0}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc
	args.BlockProcessor = createMetaBlockProcessor(args.ChainHandler)

	mutDataAvailable := goSync.RWMutex{}
	dataAvailable := false
	hash := []byte("aaa")

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}

		sds.GetHeaderByHashCalled = func(hashS []byte) (handler data.HeaderHandler, e error) {
			mutDataAvailable.RLock()
			defer mutDataAvailable.RUnlock()

			if bytes.Equal(hash, hashS) && dataAvailable {
				return &block.MetaBlock{
					Nonce:    2,
					Round:    1,
					RootHash: []byte("bbb")}, nil
			}

			return nil, errors.New("err")
		}
		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
			time.Sleep(10 * time.Millisecond)
		}

		return sds
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
		return 0
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

	bs, _ := sync.NewMetaBootstrap(args)
	bs.StartSyncingBlocks()

	time.Sleep(200 * time.Millisecond)

	mutDataAvailable.Lock()
	dataAvailable = true
	mutDataAvailable.Unlock()

	time.Sleep(500 * time.Millisecond)

	_ = bs.Close()
}

func TestMetaBootstrap_ShouldReturnNilErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	args.BlockProcessor = createMetaBlockProcessor(args.ChainHandler)

	hdr := block.MetaBlock{Nonce: 1}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	hash := []byte("aaa")
	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 2 {
				return []data.HeaderHandler{&block.MetaBlock{
					Nonce:    2,
					Round:    1,
					RootHash: []byte("bbb")}}, [][]byte{hash}, nil
			}

			return nil, nil, errors.New("err")
		}
		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
		}

		return sds
	}
	pools.MiniBlocksCalled = func() storage.Cacher {
		sds := &testscommon.CacherStub{
			HasOrAddCalled: func(key []byte, value interface{}, sizeInBytes int) (has, added bool) {
				return false, true
			},
			RegisterHandlerCalled: func(func(key []byte, value interface{})) {},
			PeekCalled: func(key []byte) (value interface{}, ok bool) {
				return nil, false
			},
			RemoveCalled: func(key []byte) {
			},
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

	bs, _ := sync.NewMetaBootstrap(args)
	r := bs.SyncBlock()

	assert.Nil(t, r)
}

func TestMetaBootstrap_SyncBlockShouldReturnErrorWhenProcessBlockFailed(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	hdr := block.MetaBlock{Nonce: 1, PubKeysBitmap: []byte("X")}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	blockProcessor := createMetaBlockProcessor(args.ChainHandler)
	blockProcessor.ProcessBlockCalled = func(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
		return process.ErrBlockHashDoesNotMatch
	}
	args.BlockProcessor = blockProcessor

	hash := []byte("aaa")
	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 2 {
				return []data.HeaderHandler{&block.MetaBlock{
					Nonce:    2,
					Round:    1,
					RootHash: []byte("bbb")}}, [][]byte{hash}, nil
			}

			return nil, nil, errors.New("err")
		}

		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
		}

		return sds
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

	bs, _ := sync.NewMetaBootstrap(args)
	err := bs.SyncBlock()

	assert.Equal(t, process.ErrBlockHashDoesNotMatch, err)
}

func TestMetaBootstrap_GetNodeStateShouldReturnSynchronizedWhenCurrentBlockIsNilAndRoundIndexIsZero(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	forkDetector := &mock.ForkDetectorMock{CheckForkCalled: func() *process.ForkInfo {
		return process.NewForkInfo()
	}}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 0
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(time.Now(), time.Now(), 200*time.Millisecond, &mock.SyncTimerMock{}, 0)

	bs, err := sync.NewMetaBootstrap(args)
	assert.Nil(t, err)

	bs.ComputeNodeState()
	assert.Equal(t, core.NsSynchronized, bs.GetNodeState())
}

func TestMetaBootstrap_GetNodeStateShouldReturnNotSynchronizedWhenCurrentBlockIsNilAndRoundIndexIsGreaterThanZero(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 1
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(time.Now(), time.Now().Add(100*time.Millisecond), 100*time.Millisecond, &mock.SyncTimerMock{}, 0)

	bs, _ := sync.NewMetaBootstrap(args)
	bs.ComputeNodeState()

	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
}

func TestMetaBootstrap_GetNodeStateShouldReturnSynchronizedWhenNodeIsSynced(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	hdr := block.MetaBlock{Nonce: 0}
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

	bs, _ := sync.NewMetaBootstrap(args)
	bs.ComputeNodeState()

	assert.Equal(t, core.NsSynchronized, bs.GetNodeState())
}

func TestMetaBootstrap_GetNodeStateShouldReturnNotSynchronizedWhenNodeIsNotSynced(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	hdr := block.MetaBlock{Nonce: 0}
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
	args.RoundHandler, _ = round.NewRound(time.Now(), time.Now().Add(100*time.Millisecond), 100*time.Millisecond, &mock.SyncTimerMock{}, 0)

	bs, _ := sync.NewMetaBootstrap(args)
	bs.ComputeNodeState()

	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
}

func TestMetaBootstrap_GetNodeStateShouldReturnNotSynchronizedWhenForkIsDetectedAndItReceivesTheSameWrongHeader(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	hdr1 := block.MetaBlock{Nonce: 1, Round: 2, PubKeysBitmap: []byte("A")}
	hash1 := []byte("hash1")

	hdr2 := block.MetaBlock{Nonce: 1, Round: 1, PubKeysBitmap: []byte("B")}
	hash2 := []byte("hash2")

	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr1
	}
	args.ChainHandler = blkc

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			if bytes.Equal(key, hash1) {
				return &hdr1, nil
			}
			if bytes.Equal(key, hash2) {
				return &hdr2, nil
			}

			return nil, errors.New("err")
		}
		return sds
	}
	args.PoolsHolder = pools
	args.RoundHandler = &mock.RoundHandlerMock{RoundIndex: 2}
	args.ForkDetector, _ = sync.NewMetaForkDetector(
		args.RoundHandler,
		&mock.BlackListHandlerStub{},
		&mock.BlockTrackerMock{},
		0,
	)

	bs, _ := sync.NewMetaBootstrap(args)

	_ = args.ForkDetector.AddHeader(&hdr1, hash1, process.BHProcessed, nil, nil)
	_ = args.ForkDetector.AddHeader(&hdr2, hash2, process.BHReceived, nil, nil)

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

func TestMetaBootstrap_GetNodeStateShouldReturnSynchronizedWhenForkIsDetectedAndItReceivesTheGoodHeader(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	hdr1 := block.MetaBlock{Nonce: 1, Round: 2, PubKeysBitmap: []byte("A")}
	hash1 := []byte("hash1")

	hdr2 := block.MetaBlock{Nonce: 1, Round: 1, PubKeysBitmap: []byte("B")}
	hash2 := []byte("hash2")

	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr2
	}
	args.ChainHandler = blkc

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			if bytes.Equal(key, hash1) {
				return &hdr1, nil
			}
			if bytes.Equal(key, hash2) {
				return &hdr2, nil
			}

			return nil, errors.New("err")
		}
		return sds
	}
	args.PoolsHolder = pools
	args.RoundHandler = &mock.RoundHandlerMock{RoundIndex: 2}
	args.ForkDetector, _ = sync.NewMetaForkDetector(
		args.RoundHandler,
		&mock.BlackListHandlerStub{},
		&mock.BlockTrackerMock{},
		0,
	)

	bs, _ := sync.NewMetaBootstrap(args)

	_ = args.ForkDetector.AddHeader(&hdr1, hash1, process.BHProcessed, nil, nil)
	_ = args.ForkDetector.AddHeader(&hdr2, hash2, process.BHReceived, nil, nil)

	bs.ComputeNodeState()
	assert.Equal(t, core.NsNotSynchronized, bs.GetNodeState())
	assert.True(t, bs.IsForkDetected())

	if bs.GetNodeState() == core.NsNotSynchronized && bs.IsForkDetected() {
		args.ForkDetector.RemoveHeader(hdr1.GetNonce(), hash1)
		bs.ReceivedHeaders(&hdr2, hash2)
		_ = args.ForkDetector.AddHeader(&hdr2, hash2, process.BHProcessed, nil, nil)
		bs.SetNodeStateCalculated(false)
	}

	time.Sleep(500 * time.Millisecond)

	bs.ComputeNodeState()
	assert.Equal(t, core.NsSynchronized, bs.GetNodeState())
	assert.False(t, bs.IsForkDetected())
}

func TestMetaBootstrap_GetHeaderFromPoolShouldReturnNil(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	pools := createMockPools()
	args.PoolsHolder = pools

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.CheckForkCalled = func() *process.ForkInfo {
		return process.NewForkInfo()
	}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 0
	}
	args.ForkDetector = forkDetector
	args.RoundHandler, _ = round.NewRound(time.Now(), time.Now(), 200*time.Millisecond, &mock.SyncTimerMock{}, 0)

	bs, _ := sync.NewMetaBootstrap(args)
	hdr, _, _ := process.GetMetaHeaderFromPoolWithNonce(0, pools.HeadersCalled())
	time.Sleep(500 * time.Millisecond)

	assert.NotNil(t, bs)
	assert.Nil(t, hdr)
}

func TestMetaBootstrap_GetHeaderFromPoolShouldReturnHeader(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	hdr := &block.MetaBlock{Nonce: 0}

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

	forkDetector := &mock.ForkDetectorMock{}
	forkDetector.ProbableHighestNonceCalled = func() uint64 {
		return 0
	}
	args.ForkDetector = forkDetector
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewMetaBootstrap(args)
	hdr2, _, _ := process.GetMetaHeaderFromPoolWithNonce(0, pools.HeadersCalled())

	assert.NotNil(t, bs)
	assert.True(t, hdr == hdr2)
}

//------- testing received headers

func TestMetaBootstrap_ReceivedHeadersFoundInPoolShouldAddToForkDetector(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	addedHash := []byte("hash")
	addedHdr := &block.MetaBlock{}

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

	shardCoordinator := mock.NewMultipleShardsCoordinatorMock()
	shardCoordinator.CurrentShard = core.MetachainShardId
	args.ShardCoordinator = shardCoordinator
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewMetaBootstrap(args)
	bs.ReceivedHeaders(addedHdr, addedHash)
	time.Sleep(500 * time.Millisecond)

	assert.True(t, wasAdded)
}

func TestMetaBootstrap_ReceivedHeadersNotFoundInPoolShouldNotAddToForkDetector(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	addedHash := []byte("hash")
	addedHdr := &block.MetaBlock{Nonce: 1}

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
	args.ForkDetector = forkDetector

	headerStorage := &testscommon.StorerStub{}
	headerStorage.GetCalled = func(key []byte) (i []byte, e error) {
		if bytes.Equal(key, addedHash) {
			buff, _ := args.Marshalizer.Marshal(addedHdr)

			return buff, nil
		}

		return nil, nil
	}
	args.Store = createMetaStore()
	args.Store.AddStorer(dataRetriever.MetaBlockUnit, headerStorage)
	args.ChainHandler, _ = blockchain.NewBlockChain(&mock.AppStatusHandlerStub{})
	args.RoundHandler = initRoundHandler()

	bs, _ := sync.NewMetaBootstrap(args)
	bs.ReceivedHeaders(addedHdr, addedHash)
	time.Sleep(500 * time.Millisecond)

	assert.False(t, wasAdded)
}

//------- RollBack

func TestMetaBootstrap_RollBackNilBlockchainHeaderShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	bs, _ := sync.NewMetaBootstrap(args)
	err := bs.RollBack(false)

	assert.Equal(t, process.ErrNilBlockHeader, err)
}

func TestMetaBootstrap_RollBackNilParamHeaderShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	blkc := initBlockchain()
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return nil
	}
	args.ChainHandler = blkc

	bs, _ := sync.NewMetaBootstrap(args)
	err := bs.RollBack(false)

	assert.Equal(t, process.ErrNilBlockHeader, err)
}

func TestMetaBootstrap_RollBackIsNotEmptyShouldErr(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

	newHdrHash := []byte("new hdr hash")
	newHdrNonce := uint64(6)

	remFlags := &removedFlags{}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.RemoveHeaderByHashCalled = func(headerHash []byte) {
			if bytes.Equal(headerHash, newHdrHash) {
				remFlags.flagHdrRemovedFromHeaders = true
			}
		}
		return sds
	}
	args.PoolsHolder = pools

	blkc := initBlockchain()
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &block.MetaBlock{
			PubKeysBitmap: []byte("X"),
			Nonce:         newHdrNonce,
		}
	}
	args.ChainHandler = blkc
	args.ForkDetector = createForkDetector(newHdrNonce, remFlags)

	bs, _ := sync.NewMetaBootstrap(args)
	err := bs.RollBack(false)

	assert.Equal(t, sync.ErrRollBackBehindFinalHeader, err)
}

func TestMetaBootstrap_RollBackIsEmptyCallRollBackOneBlockOkValsShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

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
	prevHdr := &block.MetaBlock{
		Signature: []byte("sig of the prev header as to be unique in this context"),
		RootHash:  prevHdrRootHash,
	}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.RemoveHeaderByHashCalled = func(headerHash []byte) {
			if bytes.Equal(headerHash, currentHdrHash) {
				remFlags.flagHdrRemovedFromHeaders = true
			}
		}
		return sds
	}
	args.PoolsHolder = pools

	//a mock blockchain with special header and tx block bodies stubs (defined above)
	blkc := &mock.BlockChainMock{}
	hdr := &block.MetaBlock{
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
				//bytes represent a header (strings are returns from hdrUnit.Get which is also a stub here)
				//copy only defined fields
				_, ok := obj.(*block.MetaBlock)
				if !ok {
					return nil
				}

				obj.(*block.MetaBlock).Signature = prevHdr.Signature
				obj.(*block.MetaBlock).RootHash = prevHdrRootHash
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

	bs, _ := sync.NewMetaBootstrap(args)
	bs.SetForkNonce(currentHdrNonce)

	err := bs.RollBack(true)

	assert.Nil(t, err)
	assert.True(t, remFlags.flagHdrRemovedFromHeaders)
	assert.True(t, remFlags.flagHdrRemovedFromStorage)
	assert.True(t, remFlags.flagHdrRemovedFromForkDetector)
	assert.Equal(t, blkc.GetCurrentBlockHeader(), prevHdr)
	assert.Equal(t, blkc.GetCurrentBlockHeaderHash(), prevHdrHash)
}

func TestMetaBootstrap_RollBackIsEmptyCallRollBackOneBlockToGenesisShouldWork(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()

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
	prevHdr := &block.MetaBlock{
		Signature: []byte("sig of the prev header as to be unique in this context"),
		RootHash:  prevHdrRootHash,
	}

	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.RemoveHeaderByHashCalled = func(headerHash []byte) {
			if bytes.Equal(headerHash, currentHdrHash) {
				remFlags.flagHdrRemovedFromHeaders = true
			}
		}
		return sds
	}
	args.PoolsHolder = pools

	blkc := &mock.BlockChainMock{
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return prevHdr
		},
	}
	hdr := &block.MetaBlock{
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
				obj.(*block.MetaBlock).Signature = prevHdr.Signature
				obj.(*block.MetaBlock).RootHash = prevHdrRootHash
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

	bs, _ := sync.NewMetaBootstrap(args)
	bs.SetForkNonce(currentHdrNonce)
	err := bs.RollBack(true)

	assert.Nil(t, err)
	assert.True(t, remFlags.flagHdrRemovedFromHeaders)
	assert.True(t, remFlags.flagHdrRemovedFromStorage)
	assert.True(t, remFlags.flagHdrRemovedFromForkDetector)
	assert.Nil(t, blkc.GetCurrentBlockHeader())
	assert.Nil(t, blkc.GetCurrentBlockHeaderHash())
}

func TestMetaBootstrap_AddSyncStateListenerShouldAppendAnotherListener(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.BlockProcessor = createMetaBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewMetaBootstrap(args)
	f1 := func(bool) {}
	f2 := func(bool) {}
	f3 := func(bool) {}
	bs.AddSyncStateListener(f1)
	bs.AddSyncStateListener(f2)
	bs.AddSyncStateListener(f3)

	assert.Equal(t, 3, len(bs.SyncStateListeners()))
}

func TestMetaBootstrap_NotifySyncStateListenersShouldNotify(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	args.BlockProcessor = createMetaBlockProcessor(args.ChainHandler)

	bs, _ := sync.NewMetaBootstrap(args)

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

func TestMetaBootstrap_NilInnerBootstrapperClose(t *testing.T) {
	t.Parallel()

	bootstrapper := &sync.MetaBootstrap{}
	assert.Nil(t, bootstrapper.Close())
}

func TestMetaBootstrap_SyncBlockErrGetNodeDBShouldSyncAccounts(t *testing.T) {
	t.Parallel()

	args := CreateMetaBootstrapMockArguments()
	hdr := block.MetaBlock{Nonce: 1, PubKeysBitmap: []byte("X")}
	blkc := &mock.BlockChainMock{}
	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &hdr
	}
	args.ChainHandler = blkc

	errGetNodeFromDB := errors.New(core.GetNodeFromDBErrorString)
	blockProcessor := createMetaBlockProcessor(args.ChainHandler)
	blockProcessor.ProcessBlockCalled = func(header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
		return errGetNodeFromDB
	}
	args.BlockProcessor = blockProcessor

	hash := []byte("aaa")
	pools := createMockPools()
	pools.HeadersCalled = func() dataRetriever.HeadersPool {
		sds := &mock.HeadersCacherStub{}
		sds.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 2 {
				return []data.HeaderHandler{&block.MetaBlock{
					Nonce:    2,
					Round:    1,
					RootHash: []byte("bbb")}}, [][]byte{hash}, nil
			}

			return nil, nil, errors.New("err")
		}

		sds.RegisterHandlerCalled = func(func(header data.HeaderHandler, key []byte)) {
		}

		return sds
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
	accountsSyncCalled := false
	args.AccountsDBSyncer = &mock.AccountsDBSyncerStub{
		SyncAccountsCalled: func(rootHash []byte) error {
			accountsSyncCalled = true
			return nil
		}}
	validatorSyncCalled := false
	args.ValidatorStatisticsDBSyncer = &mock.AccountsDBSyncerStub{
		SyncAccountsCalled: func(rootHash []byte) error {
			validatorSyncCalled = true
			return nil
		}}
	args.Accounts = &testscommon.AccountsStub{RootHashCalled: func() ([]byte, error) {
		return []byte("roothash"), nil
	}}
	args.ValidatorAccountsDB = &testscommon.AccountsStub{RootHashCalled: func() ([]byte, error) {
		return []byte("roothash"), nil
	}}

	bs, _ := sync.NewMetaBootstrap(args)
	err := bs.SyncBlock()

	assert.Equal(t, errGetNodeFromDB, err)
	assert.True(t, accountsSyncCalled)
	assert.True(t, validatorSyncCalled)
}
