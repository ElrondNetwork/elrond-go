package block_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/keyValStorage"
	"github.com/ElrondNetwork/elrond-go/core/queue"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/blockchain"
	"github.com/ElrondNetwork/elrond-go/data/rewardTx"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters/uint64ByteSlice"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	blproc "github.com/ElrondNetwork/elrond-go/process/block"
	"github.com/ElrondNetwork/elrond-go/process/block/bootstrapStorage"
	"github.com/ElrondNetwork/elrond-go/process/coordinator"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/storage/memorydb"
	"github.com/ElrondNetwork/elrond-go/storage/storageUnit"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/dblookupext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func haveTime() time.Duration {
	return 2000 * time.Millisecond
}

func createTestBlockchain() *mock.BlockChainMock {
	return &mock.BlockChainMock{GetGenesisHeaderCalled: func() data.HeaderHandler {
		return &block.Header{Nonce: 0}
	}}
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

func createShardedDataChacherNotifier(
	handler data.TransactionHandler,
	testHash []byte,
) func() dataRetriever.ShardedDataCacherNotifier {
	return func() dataRetriever.ShardedDataCacherNotifier {
		return &testscommon.ShardedDataStub{
			ShardDataStoreCalled: func(id string) (c storage.Cacher) {
				return &testscommon.CacherStub{
					PeekCalled: func(key []byte) (value interface{}, ok bool) {
						if reflect.DeepEqual(key, testHash) {
							return handler, true
						}
						return nil, false
					},
					KeysCalled: func() [][]byte {
						return [][]byte{[]byte("key1"), []byte("key2")}
					},
					LenCalled: func() int {
						return 0
					},
					MaxSizeCalled: func() int {
						return 1000
					},
				}
			},
			RemoveSetOfDataFromPoolCalled: func(keys [][]byte, id string) {},
			SearchFirstDataCalled: func(key []byte) (value interface{}, ok bool) {
				if reflect.DeepEqual(key, []byte("tx1_hash")) {
					return handler, true
				}
				return nil, false
			},
			AddDataCalled: func(key []byte, data interface{}, sizeInBytes int, cacheId string) {
			},
		}
	}
}

func initDataPool(testHash []byte) *testscommon.PoolsHolderStub {
	rwdTx := &rewardTx.RewardTx{
		Round:   1,
		Epoch:   0,
		Value:   big.NewInt(10),
		RcvAddr: []byte("receiver"),
	}
	txCalled := createShardedDataChacherNotifier(&transaction.Transaction{Nonce: 10}, testHash)
	unsignedTxCalled := createShardedDataChacherNotifier(&transaction.Transaction{Nonce: 10}, testHash)
	rewardTransactionsCalled := createShardedDataChacherNotifier(rwdTx, testHash)

	sdp := &testscommon.PoolsHolderStub{
		TransactionsCalled:         txCalled,
		UnsignedTransactionsCalled: unsignedTxCalled,
		RewardTransactionsCalled:   rewardTransactionsCalled,
		MetaBlocksCalled: func() storage.Cacher {
			return &testscommon.CacherStub{
				GetCalled: func(key []byte) (value interface{}, ok bool) {
					if reflect.DeepEqual(key, []byte("tx1_hash")) {
						return &transaction.Transaction{Nonce: 10}, true
					}
					return nil, false
				},
				KeysCalled: func() [][]byte {
					return nil
				},
				LenCalled: func() int {
					return 0
				},
				MaxSizeCalled: func() int {
					return 1000
				},
				PeekCalled: func(key []byte) (value interface{}, ok bool) {
					if reflect.DeepEqual(key, []byte("tx1_hash")) {
						return &transaction.Transaction{Nonce: 10}, true
					}
					return nil, false
				},
				RegisterHandlerCalled: func(i func(key []byte, value interface{})) {},
				RemoveCalled:          func(key []byte) {},
			}
		},
		MiniBlocksCalled: func() storage.Cacher {
			cs := testscommon.NewCacherStub()
			cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {
			}
			cs.GetCalled = func(key []byte) (value interface{}, ok bool) {
				if bytes.Equal([]byte("bbb"), key) {
					return make(block.MiniBlockSlice, 0), true
				}

				return nil, false
			}
			cs.PeekCalled = func(key []byte) (value interface{}, ok bool) {
				if bytes.Equal([]byte("bbb"), key) {
					return make(block.MiniBlockSlice, 0), true
				}

				return nil, false
			}
			cs.RegisterHandlerCalled = func(i func(key []byte, value interface{})) {}
			cs.RemoveCalled = func(key []byte) {}
			cs.LenCalled = func() int {
				return 0
			}
			cs.MaxSizeCalled = func() int {
				return 300
			}
			cs.KeysCalled = func() [][]byte {
				return nil
			}
			return cs
		},
		HeadersCalled: func() dataRetriever.HeadersPool {
			cs := &mock.HeadersCacherStub{}
			cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
			}
			cs.GetHeaderByHashCalled = func(hash []byte) (data.HeaderHandler, error) {
				return nil, process.ErrMissingHeader
			}
			cs.RemoveHeaderByHashCalled = func(key []byte) {
			}
			cs.LenCalled = func() int {
				return 0
			}
			cs.MaxSizeCalled = func() int {
				return 1000
			}
			cs.NoncesCalled = func(shardId uint32) []uint64 {
				return nil
			}
			return cs
		},
	}

	return sdp
}

func initStore() *dataRetriever.ChainStorer {
	store := dataRetriever.NewChainStorer()
	store.AddStorer(dataRetriever.TransactionUnit, generateTestUnit())
	store.AddStorer(dataRetriever.MiniBlockUnit, generateTestUnit())
	store.AddStorer(dataRetriever.RewardTransactionUnit, generateTestUnit())
	store.AddStorer(dataRetriever.MetaBlockUnit, generateTestUnit())
	store.AddStorer(dataRetriever.PeerChangesUnit, generateTestUnit())
	store.AddStorer(dataRetriever.BlockHeaderUnit, generateTestUnit())
	store.AddStorer(dataRetriever.ShardHdrNonceHashDataUnit, generateTestUnit())
	store.AddStorer(dataRetriever.MetaHdrNonceHashDataUnit, generateTestUnit())
	store.AddStorer(dataRetriever.ReceiptsUnit, generateTestUnit())
	store.AddStorer(dataRetriever.TrieEpochRootHashUnit, generateTestUnit())
	return store
}

func createDummyMetaBlock(destShardId uint32, senderShardId uint32, miniBlockHashes ...[]byte) *block.MetaBlock {
	metaBlock := &block.MetaBlock{
		ShardInfo: []block.ShardData{
			{
				ShardID:               senderShardId,
				ShardMiniBlockHeaders: make([]block.MiniBlockHeader, len(miniBlockHashes)),
			},
		},
	}

	for idx, mbHash := range miniBlockHashes {
		metaBlock.ShardInfo[0].ShardMiniBlockHeaders[idx].ReceiverShardID = destShardId
		metaBlock.ShardInfo[0].ShardMiniBlockHeaders[idx].SenderShardID = senderShardId
		metaBlock.ShardInfo[0].ShardMiniBlockHeaders[idx].Hash = mbHash
	}

	return metaBlock
}

func createDummyMiniBlock(
	txHash string,
	marshalizer marshal.Marshalizer,
	hasher hashing.Hasher,
	destShardId uint32,
	senderShardId uint32) (*block.MiniBlock, []byte) {

	miniblock := &block.MiniBlock{
		TxHashes:        [][]byte{[]byte(txHash)},
		ReceiverShardID: destShardId,
		SenderShardID:   senderShardId,
	}

	buff, _ := marshalizer.Marshal(miniblock)
	hash := hasher.Compute(string(buff))

	return miniblock, hash
}

func isInTxHashes(searched []byte, list [][]byte) bool {
	for _, txHash := range list {
		if bytes.Equal(txHash, searched) {
			return true
		}
	}
	return false
}

type wrongBody struct {
}

func (wr *wrongBody) Clone() data.BodyHandler {
	wrCopy := *wr

	return &wrCopy
}

func (wr *wrongBody) IntegrityAndValidity() error {
	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (wr *wrongBody) IsInterfaceNil() bool {
	return wr == nil
}

func createComponentHolderMocks() (
	*mock.CoreComponentsMock,
	*mock.DataComponentsMock,
	*mock.BootstrapComponentsMock,
	*mock.StatusComponentsMock,
) {
	blkc, _ := blockchain.NewBlockChain(&mock.AppStatusHandlerStub{})
	_ = blkc.SetGenesisHeader(&block.Header{Nonce: 0})

	coreComponents := &mock.CoreComponentsMock{
		IntMarsh:            &mock.MarshalizerMock{},
		Hash:                &mock.HasherStub{},
		UInt64ByteSliceConv: &mock.Uint64ByteSliceConverterMock{},
		StatusField:         &mock.AppStatusHandlerStub{},
		RoundField:          &mock.RoundHandlerMock{},
	}

	dataComponents := &mock.DataComponentsMock{
		Storage:    initStore(),
		DataPool:   initDataPool([]byte("")),
		BlockChain: blkc,
	}

	boostrapComponents := &mock.BootstrapComponentsMock{
		Coordinator:          mock.NewOneShardCoordinatorMock(),
		HdrIntegrityVerifier: &mock.HeaderIntegrityVerifierStub{},
	}

	statusComponents := &mock.StatusComponentsMock{
		Outport:      &testscommon.OutportStub{},
		TPSBenchmark: &testscommon.TpsBenchmarkMock{},
	}

	return coreComponents, dataComponents, boostrapComponents, statusComponents
}

func CreateMockArguments(
	coreComponents *mock.CoreComponentsMock,
	dataComponents *mock.DataComponentsMock,
	bootstrapComponents *mock.BootstrapComponentsMock,
	statusComponents *mock.StatusComponentsMock,
) blproc.ArgShardProcessor {
	nodesCoordinator := mock.NewNodesCoordinatorMock()
	argsHeaderValidator := blproc.ArgsHeaderValidator{
		Hasher:      &mock.HasherMock{},
		Marshalizer: &mock.MarshalizerMock{},
	}
	headerValidator, _ := blproc.NewHeaderValidator(argsHeaderValidator)

	startHeaders := createGenesisBlocks(mock.NewOneShardCoordinatorMock())

	accountsDb := make(map[state.AccountsDbIdentifier]state.AccountsAdapter)
	accountsDb[state.UserAccountsState] = &testscommon.AccountsStub{}

	arguments := blproc.ArgShardProcessor{
		ArgBaseProcessor: blproc.ArgBaseProcessor{
			CoreComponents:      coreComponents,
			DataComponents:      dataComponents,
			BootstrapComponents: bootstrapComponents,
			StatusComponents:    statusComponents,
			Config:              config.Config{},
			AccountsDB:          accountsDb,
			ForkDetector:        &mock.ForkDetectorMock{},
			NodesCoordinator:    nodesCoordinator,
			FeeHandler:          &mock.FeeAccumulatorStub{},
			RequestHandler:      &testscommon.RequestHandlerStub{},
			BlockChainHook:      &mock.BlockChainHookHandlerMock{},
			TxCoordinator:       &mock.TransactionCoordinatorMock{},
			EpochStartTrigger:   &mock.EpochStartTriggerStub{},
			HeaderValidator:     headerValidator,
			BootStorer: &mock.BoostrapStorerMock{
				PutCalled: func(round int64, bootData bootstrapStorage.BootstrapData) error {
					return nil
				},
			},
			BlockTracker:       mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders),
			BlockSizeThrottler: &mock.BlockSizeThrottlerStub{},
			Version:            "softwareVersion",
			HistoryRepository:  &dblookupext.HistoryRepositoryStub{},
			EpochNotifier:      &mock.EpochNotifierStub{},
		},
	}

	return arguments
}

func createMockTransactionCoordinatorArguments(
	accountAdapter state.AccountsAdapter,
	poolsHolder dataRetriever.PoolsHolder,
	preProcessorsContainer process.PreProcessorsContainer,
) coordinator.ArgTransactionCoordinator {
	argsTransactionCoordinator := coordinator.ArgTransactionCoordinator{
		Hasher:                            &mock.HasherMock{},
		Marshalizer:                       &mock.MarshalizerMock{},
		ShardCoordinator:                  mock.NewMultiShardsCoordinatorMock(3),
		Accounts:                          accountAdapter,
		MiniBlockPool:                     poolsHolder.MiniBlocks(),
		RequestHandler:                    &testscommon.RequestHandlerStub{},
		PreProcessors:                     preProcessorsContainer,
		InterProcessors:                   &mock.InterimProcessorContainerMock{},
		GasHandler:                        &mock.GasHandlerMock{},
		FeeHandler:                        &mock.FeeAccumulatorStub{},
		BlockSizeComputation:              &mock.BlockSizeComputationStub{},
		BalanceComputation:                &mock.BalanceComputationStub{},
		EconomicsFee:                      &mock.FeeHandlerStub{},
		TxTypeHandler:                     &testscommon.TxTypeHandlerMock{},
		BlockGasAndFeesReCheckEnableEpoch: 0,
		TransactionsLogProcessor:          &mock.TxLogsProcessorStub{},
	}

	return argsTransactionCoordinator
}

func TestBlockProcessor_CheckBlockValidity(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.Hash = &mock.HasherMock{}
	blkc := createTestBlockchain()
	dataComponents.BlockChain = blkc
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	bp, _ := blproc.NewShardProcessor(arguments)

	body := &block.Body{}
	hdr := &block.Header{}
	hdr.Nonce = 1
	hdr.Round = 1
	hdr.TimeStamp = 0
	hdr.PrevHash = []byte("X")
	err := bp.CheckBlockValidity(hdr, body)
	assert.Equal(t, process.ErrBlockHashDoesNotMatch, err)

	hdr.PrevHash = []byte("")
	err = bp.CheckBlockValidity(hdr, body)
	assert.Nil(t, err)

	hdr.Nonce = 2
	err = bp.CheckBlockValidity(hdr, body)
	assert.Equal(t, process.ErrWrongNonceInBlock, err)

	blkc.GetCurrentBlockHeaderCalled = func() data.HeaderHandler {
		return &block.Header{Round: 1, Nonce: 1}
	}
	prevHash := []byte("X")
	blkc.GetCurrentBlockHeaderHashCalled = func() []byte {
		return prevHash
	}
	hdr = &block.Header{}

	err = bp.CheckBlockValidity(hdr, body)
	assert.Equal(t, process.ErrLowerRoundInBlock, err)

	hdr.Round = 2
	hdr.Nonce = 1
	err = bp.CheckBlockValidity(hdr, body)
	assert.Equal(t, process.ErrWrongNonceInBlock, err)

	hdr.Nonce = 2
	hdr.PrevHash = []byte("XX")
	err = bp.CheckBlockValidity(hdr, body)
	assert.Equal(t, process.ErrBlockHashDoesNotMatch, err)

	hdr.PrevHash = blkc.GetCurrentBlockHeaderHash()
	hdr.PrevRandSeed = []byte("X")
	err = bp.CheckBlockValidity(hdr, body)
	assert.Equal(t, process.ErrRandSeedDoesNotMatch, err)

	hdr.PrevRandSeed = []byte("")
	err = bp.CheckBlockValidity(hdr, body)
	assert.Nil(t, err)
}

func TestVerifyStateRoot_ShouldWork(t *testing.T) {
	t.Parallel()
	rootHash := []byte("root hash to be tested")
	accounts := &testscommon.AccountsStub{
		RootHashCalled: func() ([]byte, error) {
			return rootHash, nil
		},
	}

	arguments := CreateMockArguments(createComponentHolderMocks())
	arguments.AccountsDB[state.UserAccountsState] = accounts
	bp, _ := blproc.NewShardProcessor(arguments)

	assert.True(t, bp.VerifyStateRoot(rootHash))
}

// ------- RevertState
func TestBaseProcessor_RevertStateRecreateTrieFailsShouldErr(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("err")
	arguments := CreateMockArguments(createComponentHolderMocks())
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RecreateTrieCalled: func(rootHash []byte) error {
			return expectedErr
		},
	}

	bp, _ := blproc.NewShardProcessor(arguments)

	hdr := block.Header{Nonce: 37}
	err := bp.RevertStateToBlock(&hdr)
	assert.Equal(t, expectedErr, err)
}

// removeHeadersBehindNonceFromPools
func TestBaseProcessor_RemoveHeadersBehindNonceFromPools(t *testing.T) {
	t.Parallel()

	removeFromDataPoolWasCalled := false
	dataPool := initDataPool([]byte(""))
	dataPool.HeadersCalled = func() dataRetriever.HeadersPool {
		cs := &mock.HeadersCacherStub{}
		cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
		}
		cs.GetHeaderByHashCalled = func(hash []byte) (handler data.HeaderHandler, err error) {
			return nil, err
		}
		cs.RemoveHeaderByHashCalled = func(key []byte) {
		}
		cs.LenCalled = func() int {
			return 0
		}
		cs.MaxSizeCalled = func() int {
			return 1000
		}
		cs.NoncesCalled = func(shardId uint32) []uint64 {
			return []uint64{1, 2, 3}
		}
		cs.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) ([]data.HeaderHandler, [][]byte, error) {
			hdrs := make([]data.HeaderHandler, 0)
			hdrs = append(hdrs, &block.Header{Nonce: 2})
			return hdrs, nil, nil
		}

		return cs
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	dataComponents.DataPool = dataPool
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.TxCoordinator = &mock.TransactionCoordinatorMock{
		RemoveBlockDataFromPoolCalled: func(body *block.Body) error {
			removeFromDataPoolWasCalled = true
			return nil
		},
	}
	bp, _ := blproc.NewShardProcessor(arguments)

	bp.RemoveHeadersBehindNonceFromPools(true, 0, 4)

	assert.True(t, removeFromDataPoolWasCalled)
}

// ------- ComputeNewNoncePrevHash

func TestBlockProcessor_computeHeaderHashMarshalizerFail1ShouldErr(t *testing.T) {
	t.Parallel()
	marshalizer := &mock.MarshalizerStub{}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.IntMarsh = marshalizer
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	bp, _ := blproc.NewShardProcessor(arguments)
	hdr, txBlock := createTestHdrTxBlockBody()
	expectedError := errors.New("marshalizer fail")
	marshalizer.MarshalCalled = func(obj interface{}) (bytes []byte, e error) {
		if hdr == obj {
			return nil, expectedError
		}

		if reflect.DeepEqual(txBlock, obj) {
			return []byte("txBlockBodyMarshalized"), nil
		}
		return nil, nil
	}
	_, err := bp.ComputeHeaderHash(hdr)
	assert.Equal(t, expectedError, err)
}

func TestBlockPorcessor_ComputeNewNoncePrevHashShouldWork(t *testing.T) {
	t.Parallel()
	marshalizer := &mock.MarshalizerStub{}
	hasher := &mock.HasherStub{}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.IntMarsh = marshalizer
	coreComponents.Hash = hasher
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	bp, _ := blproc.NewShardProcessor(arguments)
	hdr, txBlock := createTestHdrTxBlockBody()
	marshalizer.MarshalCalled = func(obj interface{}) (bytes []byte, e error) {
		if hdr == obj {
			return []byte("hdrHeaderMarshalized"), nil
		}
		if reflect.DeepEqual(txBlock, obj) {
			return []byte("txBlockBodyMarshalized"), nil
		}
		return nil, nil
	}
	hasher.ComputeCalled = func(s string) []byte {
		if s == "hdrHeaderMarshalized" {
			return []byte("hdr hash")
		}
		if s == "txBlockBodyMarshalized" {
			return []byte("tx block body hash")
		}
		return nil
	}
	_, err := bp.ComputeHeaderHash(hdr)
	assert.Nil(t, err)
}

func createShardProcessHeadersToSaveLastNotarized(
	highestNonce uint64,
	genesisHdr data.HeaderHandler,
	hasher hashing.Hasher,
	marshalizer marshal.Marshalizer,
) []data.HeaderHandler {
	rootHash := []byte("roothash")
	processedHdrs := make([]data.HeaderHandler, 0)

	headerMarsh, _ := marshalizer.Marshal(genesisHdr)
	headerHash := hasher.Compute(string(headerMarsh))

	for i := uint64(1); i <= highestNonce; i++ {
		hdr := &block.Header{
			Nonce:         i,
			Round:         i,
			Signature:     rootHash,
			RandSeed:      rootHash,
			PrevRandSeed:  rootHash,
			PubKeysBitmap: rootHash,
			RootHash:      rootHash,
			PrevHash:      headerHash}
		processedHdrs = append(processedHdrs, hdr)

		headerMarsh, _ = marshalizer.Marshal(hdr)
		headerHash = hasher.Compute(string(headerMarsh))
	}

	return processedHdrs
}

func createMetaProcessHeadersToSaveLastNoterized(
	highestNonce uint64,
	genesisHdr data.HeaderHandler,
	hasher hashing.Hasher,
	marshalizer marshal.Marshalizer,
) []data.HeaderHandler {
	rootHash := []byte("roothash")
	processedHdrs := make([]data.HeaderHandler, 0)

	headerMarsh, _ := marshalizer.Marshal(genesisHdr)
	headerHash := hasher.Compute(string(headerMarsh))

	for i := uint64(1); i <= highestNonce; i++ {
		hdr := &block.MetaBlock{
			Nonce:         i,
			Round:         i,
			Signature:     rootHash,
			RandSeed:      rootHash,
			PrevRandSeed:  rootHash,
			PubKeysBitmap: rootHash,
			RootHash:      rootHash,
			PrevHash:      headerHash}
		processedHdrs = append(processedHdrs, hdr)

		headerMarsh, _ = marshalizer.Marshal(hdr)
		headerHash = hasher.Compute(string(headerMarsh))
	}

	return processedHdrs
}

func TestBaseProcessor_SaveLastNotarizedInOneShardHdrsSliceForShardIsNil(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.Hash = &mock.HasherMock{}
	coreComponents.IntMarsh = &mock.MarshalizerMock{}
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	sp, _ := blproc.NewShardProcessor(arguments)
	prHdrs := createShardProcessHeadersToSaveLastNotarized(10, &block.Header{}, mock.HasherMock{}, &mock.MarshalizerMock{})

	err := sp.SaveLastNotarizedHeader(2, prHdrs)

	assert.Equal(t, process.ErrNotarizedHeadersSliceForShardIsNil, err)
}

func TestBaseProcessor_SaveLastNotarizedInMultiShardHdrsSliceForShardIsNil(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.Hash = &mock.HasherMock{}
	coreComponents.IntMarsh = &mock.MarshalizerMock{}
	bootstrapComponents.Coordinator = shardCoordinator
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	sp, _ := blproc.NewShardProcessor(arguments)

	prHdrs := createShardProcessHeadersToSaveLastNotarized(10, &block.Header{}, mock.HasherMock{}, &mock.MarshalizerMock{})

	err := sp.SaveLastNotarizedHeader(6, prHdrs)

	assert.Equal(t, process.ErrNotarizedHeadersSliceForShardIsNil, err)
}

func TestBaseProcessor_SaveLastNotarizedHdrShardGood(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.Hash = &mock.HasherMock{}
	coreComponents.IntMarsh = &mock.MarshalizerMock{}
	bootstrapComponents.Coordinator = shardCoordinator
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	sp, _ := blproc.NewShardProcessor(arguments)
	argsHeaderValidator := blproc.ArgsHeaderValidator{
		Hasher:      coreComponents.Hasher(),
		Marshalizer: coreComponents.InternalMarshalizer(),
	}
	headerValidator, _ := blproc.NewHeaderValidator(argsHeaderValidator)
	sp.SetHeaderValidator(headerValidator)

	genesisBlcks := createGenesisBlocks(shardCoordinator)

	highestNonce := uint64(10)
	shardId := uint32(0)
	prHdrs := createShardProcessHeadersToSaveLastNotarized(
		highestNonce,
		genesisBlcks[shardId],
		coreComponents.Hasher(),
		coreComponents.InternalMarshalizer())

	err := sp.SaveLastNotarizedHeader(shardId, prHdrs)
	assert.Nil(t, err)

	assert.Equal(t, highestNonce, sp.LastNotarizedHdrForShard(shardId).GetNonce())
}

func TestBaseProcessor_SaveLastNotarizedHdrMetaGood(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.Hash = &mock.HasherMock{}
	coreComponents.IntMarsh = &mock.MarshalizerMock{}
	bootstrapComponents.Coordinator = shardCoordinator
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	sp, _ := blproc.NewShardProcessor(arguments)

	argsHeaderValidator := blproc.ArgsHeaderValidator{
		Hasher:      coreComponents.Hasher(),
		Marshalizer: coreComponents.InternalMarshalizer(),
	}
	headerValidator, _ := blproc.NewHeaderValidator(argsHeaderValidator)
	sp.SetHeaderValidator(headerValidator)

	genesisBlcks := createGenesisBlocks(shardCoordinator)

	highestNonce := uint64(10)
	prHdrs := createMetaProcessHeadersToSaveLastNoterized(
		highestNonce,
		genesisBlcks[core.MetachainShardId],
		coreComponents.Hasher(),
		coreComponents.InternalMarshalizer())

	err := sp.SaveLastNotarizedHeader(core.MetachainShardId, prHdrs)
	assert.Nil(t, err)

	assert.Equal(t, highestNonce, sp.LastNotarizedHdrForShard(core.MetachainShardId).GetNonce())
}

func TestShardProcessor_ProcessBlockEpochDoesNotMatchShouldErr(t *testing.T) {
	t.Parallel()
	blockChain := &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{
				Epoch: 2,
			}
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}
	coreComponents, dataComponents, bootstrapComponents, statusComponents := CreateCoreComponentsMultiShard()
	dataComponents.BlockChain = blockChain
	arguments := CreateMockArgumentsMultiShard(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	sp, _ := blproc.NewShardProcessor(arguments)
	header := &block.Header{Round: 10, Nonce: 1}

	blk := &block.Body{}
	err := sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })

	assert.True(t, errors.Is(err, process.ErrEpochDoesNotMatch))
}

func TestShardProcessor_ProcessBlockEpochDoesNotMatchShouldErr2(t *testing.T) {
	t.Parallel()

	randSeed := []byte("randseed")
	blockChain := &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{
				Epoch:           1,
				RandSeed:        randSeed,
				AccumulatedFees: big.NewInt(0),
				DeveloperFees:   big.NewInt(0),
			}
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := CreateCoreComponentsMultiShard()
	dataComponents.BlockChain = blockChain
	arguments := CreateMockArgumentsMultiShard(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.EpochStartTrigger = &mock.EpochStartTriggerStub{
		EpochCalled: func() uint32 {
			return 1
		},
	}

	sp, _ := blproc.NewShardProcessor(arguments)
	header := &block.Header{Round: 10, Nonce: 1, Epoch: 5, RandSeed: randSeed, PrevRandSeed: randSeed}

	blk := &block.Body{}
	err := sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })

	assert.True(t, errors.Is(err, process.ErrEpochDoesNotMatch))
}

func TestShardProcessor_ProcessBlockEpochDoesNotMatchShouldErr3(t *testing.T) {
	t.Parallel()

	randSeed := []byte("randseed")
	blockChain := &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{
				Epoch:    3,
				RandSeed: randSeed,
			}
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := CreateCoreComponentsMultiShard()
	dataComponents.BlockChain = blockChain
	arguments := CreateMockArgumentsMultiShard(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.EpochStartTrigger = &mock.EpochStartTriggerStub{
		EpochCalled: func() uint32 {
			return 2
		},
		IsEpochStartCalled: func() bool {
			return true
		},
	}

	sp, _ := blproc.NewShardProcessor(arguments)
	header := &block.Header{Round: 10, Nonce: 1, Epoch: 5, RandSeed: randSeed, PrevRandSeed: randSeed}

	blk := &block.Body{}
	err := sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })

	assert.True(t, errors.Is(err, process.ErrEpochDoesNotMatch))
}

func TestShardProcessor_ProcessBlockEpochDoesNotMatchShouldErrMetaHashDoesNotMatch(t *testing.T) {
	t.Parallel()

	randSeed := []byte("randseed")
	chain := &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{
				Epoch:    2,
				RandSeed: randSeed,
			}
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}
	hasher := &mock.HasherStub{ComputeCalled: func(s string) []byte {
		return nil
	}}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := CreateCoreComponentsMultiShard()
	dataComponents.BlockChain = chain
	coreComponents.Hash = hasher
	arguments := CreateMockArgumentsMultiShard(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	epochStartTrigger := &mock.EpochStartTriggerStub{
		EpochCalled: func() uint32 {
			return 2
		},
		MetaEpochCalled: func() uint32 {
			return 3
		},
		IsEpochStartCalled: func() bool {
			return true
		},
		EpochFinalityAttestingRoundCalled: func() uint64 {
			return 100
		},
	}
	arguments.EpochStartTrigger = epochStartTrigger

	sp, _ := blproc.NewShardProcessor(arguments)
	rootHash, _ := arguments.AccountsDB[state.UserAccountsState].RootHash()
	epochStartHash := []byte("epochStartHash")
	header := &block.Header{
		Round:              10,
		Nonce:              1,
		Epoch:              3,
		RandSeed:           randSeed,
		PrevRandSeed:       randSeed,
		EpochStartMetaHash: epochStartHash,
		RootHash:           rootHash,
		AccumulatedFees:    big.NewInt(0),
		DeveloperFees:      big.NewInt(0),
	}

	blk := &block.Body{}
	err := sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })
	assert.True(t, errors.Is(err, process.ErrEpochDoesNotMatch))

	epochStartTrigger.EpochStartMetaHdrHashCalled = func() []byte {
		return header.EpochStartMetaHash
	}
	err = sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })
	assert.Nil(t, err)
}

func TestShardProcessor_ProcessBlockEpochDoesNotMatchShouldErrMetaHashDoesNotMatchForOldEpoch(t *testing.T) {
	t.Parallel()

	randSeed := []byte("randseed")
	chain := &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.Header{
				Epoch:    2,
				RandSeed: randSeed,
			}
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}
	hasher := &mock.HasherStub{ComputeCalled: func(s string) []byte {
		return nil
	}}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := CreateCoreComponentsMultiShard()
	coreComponents.Hash = hasher
	dataComponents.BlockChain = chain
	arguments := CreateMockArgumentsMultiShard(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	arguments.EpochStartTrigger = &mock.EpochStartTriggerStub{
		EpochCalled: func() uint32 {
			return 5
		},
		MetaEpochCalled: func() uint32 {
			return 6
		},
		IsEpochStartCalled: func() bool {
			return true
		},
		EpochFinalityAttestingRoundCalled: func() uint64 {
			return 100
		},
	}

	sp, _ := blproc.NewShardProcessor(arguments)
	rootHash, _ := arguments.AccountsDB[state.UserAccountsState].RootHash()
	epochStartHash := []byte("epochStartHash")
	header := &block.Header{
		Round:              10,
		Nonce:              1,
		Epoch:              3,
		RandSeed:           randSeed,
		PrevRandSeed:       randSeed,
		EpochStartMetaHash: epochStartHash,
		RootHash:           rootHash,
		AccumulatedFees:    big.NewInt(0),
		DeveloperFees:      big.NewInt(0),
	}

	blk := &block.Body{}
	err := sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })
	assert.True(t, errors.Is(err, process.ErrMissingHeader))

	metaHdr := &block.MetaBlock{}
	metaHdrData, _ := coreComponents.InternalMarshalizer().Marshal(metaHdr)
	_ = dataComponents.StorageService().Put(dataRetriever.MetaBlockUnit, header.EpochStartMetaHash, metaHdrData)

	err = sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })
	assert.True(t, errors.Is(err, process.ErrEpochDoesNotMatch))

	metaHdr = &block.MetaBlock{Epoch: 3, EpochStart: block.EpochStart{
		LastFinalizedHeaders: []block.EpochStartShardData{{}},
		Economics:            block.Economics{},
	}}
	metaHdrData, _ = coreComponents.InternalMarshalizer().Marshal(metaHdr)
	_ = dataComponents.StorageService().Put(dataRetriever.MetaBlockUnit, header.EpochStartMetaHash, metaHdrData)

	err = sp.ProcessBlock(header, blk, func() time.Duration { return time.Second })
	assert.Nil(t, err)
}

func TestBlockProcessor_PruneStateOnRollbackPrunesPeerTrieIfAccPruneIsDisabled(t *testing.T) {
	t.Parallel()

	pruningCalled := 0
	peerAccDb := &testscommon.AccountsStub{
		PruneTrieCalled: func(rootHash []byte, identifier data.TriePruningIdentifier) {
			pruningCalled++
		},
		CancelPruneCalled: func(rootHash []byte, identifier data.TriePruningIdentifier) {
			pruningCalled++
		},
		IsPruningEnabledCalled: func() bool {
			return true
		},
	}

	arguments := CreateMockArguments(createComponentHolderMocks())
	arguments.AccountsDB[state.PeerAccountsState] = peerAccDb
	bp, _ := blproc.NewShardProcessor(arguments)

	prevHeader := &block.MetaBlock{
		RootHash:               []byte("prevRootHash"),
		ValidatorStatsRootHash: []byte("prevValidatorRootHash"),
	}
	currHeader := &block.MetaBlock{
		RootHash:               []byte("prevRootHash"),
		ValidatorStatsRootHash: []byte("currValidatorRootHash"),
	}

	bp.PruneStateOnRollback(currHeader, prevHeader)
	assert.Equal(t, 2, pruningCalled)
}

func TestBlockProcessor_PruneStateOnRollbackPrunesPeerTrieIfSameRootHashButDifferentValidatorRootHash(t *testing.T) {
	t.Parallel()

	pruningCalled := 0
	peerAccDb := &testscommon.AccountsStub{
		PruneTrieCalled: func(rootHash []byte, identifier data.TriePruningIdentifier) {
			pruningCalled++
		},
		CancelPruneCalled: func(rootHash []byte, identifier data.TriePruningIdentifier) {
			pruningCalled++
		},
		IsPruningEnabledCalled: func() bool {
			return true
		},
	}

	accDb := &testscommon.AccountsStub{
		PruneTrieCalled: func(rootHash []byte, identifier data.TriePruningIdentifier) {
			pruningCalled++
		},
		CancelPruneCalled: func(rootHash []byte, identifier data.TriePruningIdentifier) {
			pruningCalled++
		},
		IsPruningEnabledCalled: func() bool {
			return true
		},
	}

	arguments := CreateMockArguments(createComponentHolderMocks())
	arguments.AccountsDB[state.PeerAccountsState] = peerAccDb
	arguments.AccountsDB[state.UserAccountsState] = accDb
	bp, _ := blproc.NewShardProcessor(arguments)

	prevHeader := &block.MetaBlock{
		RootHash:               []byte("prevRootHash"),
		ValidatorStatsRootHash: []byte("prevValidatorRootHash"),
	}
	currHeader := &block.MetaBlock{
		RootHash:               []byte("prevRootHash"),
		ValidatorStatsRootHash: []byte("currValidatorRootHash"),
	}

	bp.PruneStateOnRollback(currHeader, prevHeader)
	assert.Equal(t, 2, pruningCalled)
}

func TestBlockProcessor_RequestHeadersIfMissingShouldWorkWhenSortedHeadersListIsEmpty(t *testing.T) {
	t.Parallel()

	var requestedNonces []uint64
	var mutRequestedNonces sync.Mutex

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	roundHandler := &mock.RoundHandlerMock{}
	coreComponents.RoundField = roundHandler
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	requestHandlerStub := &testscommon.RequestHandlerStub{
		RequestMetaHeaderByNonceCalled: func(nonce uint64) {
			mutRequestedNonces.Lock()
			requestedNonces = append(requestedNonces, nonce)
			mutRequestedNonces.Unlock()
		},
	}
	arguments.RequestHandler = requestHandlerStub
	sp, _ := blproc.NewShardProcessor(arguments)

	sortedHeaders := make([]data.HeaderHandler, 0)

	requestedNonces = make([]uint64, 0)
	roundHandler.RoundIndex = process.MaxHeaderRequestsAllowed + 5
	_ = sp.RequestHeadersIfMissing(sortedHeaders, core.MetachainShardId)
	time.Sleep(100 * time.Millisecond)
	mutRequestedNonces.Lock()
	sort.Slice(requestedNonces, func(i, j int) bool {
		return requestedNonces[i] < requestedNonces[j]
	})
	mutRequestedNonces.Unlock()
	expectedNonces := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	assert.Equal(t, expectedNonces, requestedNonces)

	requestedNonces = make([]uint64, 0)
	roundHandler.RoundIndex = 5
	_ = sp.RequestHeadersIfMissing(sortedHeaders, core.MetachainShardId)
	time.Sleep(100 * time.Millisecond)
	mutRequestedNonces.Lock()
	sort.Slice(requestedNonces, func(i, j int) bool {
		return requestedNonces[i] < requestedNonces[j]
	})
	mutRequestedNonces.Unlock()
	expectedNonces = []uint64{1, 2, 3}
	assert.Equal(t, expectedNonces, requestedNonces)
}

func TestBlockProcessor_RequestHeadersIfMissingShouldWork(t *testing.T) {
	t.Parallel()

	var requestedNonces []uint64
	var mutRequestedNonces sync.Mutex

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	roundHandler := &mock.RoundHandlerMock{}
	coreComponents.RoundField = roundHandler
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	requestHandlerStub := &testscommon.RequestHandlerStub{
		RequestMetaHeaderByNonceCalled: func(nonce uint64) {
			mutRequestedNonces.Lock()
			requestedNonces = append(requestedNonces, nonce)
			mutRequestedNonces.Unlock()
		},
	}
	arguments.RequestHandler = requestHandlerStub
	sp, _ := blproc.NewShardProcessor(arguments)

	sortedHeaders := make([]data.HeaderHandler, 0)

	crossNotarizedHeader := &block.MetaBlock{
		Nonce: 5,
		Round: 5,
	}
	arguments.BlockTracker.AddCrossNotarizedHeader(core.MetachainShardId, crossNotarizedHeader, []byte("hash"))

	hdr1 := &block.MetaBlock{
		Nonce: 1,
		Round: 1,
	}
	sortedHeaders = append(sortedHeaders, hdr1)

	hdr2 := &block.MetaBlock{
		Nonce: 8,
		Round: 8,
	}
	sortedHeaders = append(sortedHeaders, hdr2)

	hdr3 := &block.MetaBlock{
		Nonce: 10,
		Round: 10,
	}
	sortedHeaders = append(sortedHeaders, hdr3)

	requestedNonces = make([]uint64, 0)
	roundHandler.RoundIndex = 15
	_ = sp.RequestHeadersIfMissing(sortedHeaders, core.MetachainShardId)
	time.Sleep(100 * time.Millisecond)
	mutRequestedNonces.Lock()
	sort.Slice(requestedNonces, func(i, j int) bool {
		return requestedNonces[i] < requestedNonces[j]
	})
	mutRequestedNonces.Unlock()
	expectedNonces := []uint64{6, 7, 9, 11, 12, 13}
	assert.Equal(t, expectedNonces, requestedNonces)

	requestedNonces = make([]uint64, 0)
	roundHandler.RoundIndex = process.MaxHeaderRequestsAllowed + 10
	_ = sp.RequestHeadersIfMissing(sortedHeaders, core.MetachainShardId)
	time.Sleep(100 * time.Millisecond)
	mutRequestedNonces.Lock()
	sort.Slice(requestedNonces, func(i, j int) bool {
		return requestedNonces[i] < requestedNonces[j]
	})
	mutRequestedNonces.Unlock()
	expectedNonces = []uint64{6, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}
	assert.Equal(t, expectedNonces, requestedNonces)
}

func TestBlockProcessor_RequestHeadersIfMissingShouldAddHeaderIntoTrackerPool(t *testing.T) {
	t.Parallel()

	var addedNonces []uint64
	poolsHolderStub := initDataPool([]byte(""))
	poolsHolderStub.HeadersCalled = func() dataRetriever.HeadersPool {
		return &mock.HeadersCacherStub{
			GetHeaderByNonceAndShardIdCalled: func(hdrNonce uint64, shardId uint32) ([]data.HeaderHandler, [][]byte, error) {
				addedNonces = append(addedNonces, hdrNonce)
				return []data.HeaderHandler{&block.MetaBlock{Nonce: 1}}, [][]byte{[]byte("hash")}, nil
			},
		}
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	dataComponents.DataPool = poolsHolderStub
	roundHandler := &mock.RoundHandlerMock{}
	coreComponents.RoundField = roundHandler
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	sp, _ := blproc.NewShardProcessor(arguments)

	sortedHeaders := make([]data.HeaderHandler, 0)

	crossNotarizedHeader := &block.MetaBlock{
		Nonce: 5,
		Round: 5,
	}
	arguments.BlockTracker.AddCrossNotarizedHeader(core.MetachainShardId, crossNotarizedHeader, []byte("hash"))

	hdr1 := &block.MetaBlock{
		Nonce: 1,
		Round: 1,
	}
	sortedHeaders = append(sortedHeaders, hdr1)

	hdr2 := &block.MetaBlock{
		Nonce: 8,
		Round: 8,
	}
	sortedHeaders = append(sortedHeaders, hdr2)

	hdr3 := &block.MetaBlock{
		Nonce: 10,
		Round: 10,
	}
	sortedHeaders = append(sortedHeaders, hdr3)

	addedNonces = make([]uint64, 0)

	roundHandler.RoundIndex = 12
	_ = sp.RequestHeadersIfMissing(sortedHeaders, core.MetachainShardId)

	expectedAddedNonces := []uint64{6, 7, 9}
	assert.Equal(t, expectedAddedNonces, addedNonces)
}

func TestAddHeaderIntoTrackerPool_ShouldWork(t *testing.T) {
	t.Parallel()

	var wasCalled bool
	shardID := core.MetachainShardId
	nonce := uint64(1)
	poolsHolderStub := initDataPool([]byte(""))
	poolsHolderStub.HeadersCalled = func() dataRetriever.HeadersPool {
		return &mock.HeadersCacherStub{
			GetHeaderByNonceAndShardIdCalled: func(hdrNonce uint64, shardId uint32) ([]data.HeaderHandler, [][]byte, error) {
				if hdrNonce == nonce && shardId == shardID {
					wasCalled = true
					return []data.HeaderHandler{&block.MetaBlock{Nonce: 1}}, [][]byte{[]byte("hash")}, nil
				}

				return nil, nil, errors.New("error")
			},
		}
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	dataComponents.DataPool = poolsHolderStub
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	sp, _ := blproc.NewShardProcessor(arguments)

	wasCalled = false
	sp.AddHeaderIntoTrackerPool(nonce+1, shardID)
	assert.False(t, wasCalled)

	wasCalled = false
	sp.AddHeaderIntoTrackerPool(nonce, shardID)
	assert.True(t, wasCalled)
}

func TestBaseProcessor_commitTrieEpochRootHashIfNeededNilStorerShouldNotErr(t *testing.T) {
	t.Parallel()

	epoch := uint32(37)

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	store := dataRetriever.NewChainStorer()
	dataComponents.Storage = store
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	sp, _ := blproc.NewShardProcessor(arguments)

	mb := &block.MetaBlock{Epoch: epoch}
	err := sp.CommitTrieEpochRootHashIfNeeded(mb, []byte("root"))
	require.NoError(t, err)
}

func TestBaseProcessor_commitTrieEpochRootHashIfNeededDisabledStorerShouldNotErr(t *testing.T) {
	t.Parallel()

	epoch := uint32(37)

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	dataComponents.Storage.AddStorer(dataRetriever.TrieEpochRootHashUnit, &storageUnit.NilStorer{})
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	sp, _ := blproc.NewShardProcessor(arguments)

	mb := &block.MetaBlock{Epoch: epoch}
	err := sp.CommitTrieEpochRootHashIfNeeded(mb, []byte("root"))
	require.NoError(t, err)
}

func TestBaseProcessor_commitTrieEpochRootHashIfNeededCannotFindUserAccountStateShouldErr(t *testing.T) {
	t.Parallel()

	epoch := uint32(37)

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB = map[state.AccountsDbIdentifier]state.AccountsAdapter{}

	sp, _ := blproc.NewShardProcessor(arguments)

	mb := &block.MetaBlock{Epoch: epoch}
	err := sp.CommitTrieEpochRootHashIfNeeded(mb, []byte("root"))
	require.True(t, errors.Is(err, process.ErrNilAccountsAdapter))
}

func TestBaseProcessor_commitTrieEpochRootHashIfNeededShouldWork(t *testing.T) {
	t.Parallel()

	epoch := uint32(37)
	rootHash := []byte("root-hash")

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	coreComponents.UInt64ByteSliceConv = uint64ByteSlice.NewBigEndianConverter()
	store := dataRetriever.NewChainStorer()
	store.AddStorer(dataRetriever.TrieEpochRootHashUnit,
		&testscommon.StorerStub{
			PutCalled: func(key, data []byte) error {
				restoredEpoch, err := coreComponents.UInt64ByteSliceConv.ToUint64(key)
				require.NoError(t, err)
				require.Equal(t, epoch, uint32(restoredEpoch))
				return nil
			},
		},
	)
	dataComponents.Storage = store

	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB = map[state.AccountsDbIdentifier]state.AccountsAdapter{
		state.UserAccountsState: &testscommon.AccountsStub{
			RootHashCalled: func() ([]byte, error) {
				return rootHash, nil
			},
			GetAllLeavesCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
				channel := make(chan core.KeyValueHolder)
				close(channel)
				return channel, nil
			},
		},
	}

	sp, _ := blproc.NewShardProcessor(arguments)

	mb := &block.MetaBlock{Epoch: epoch}
	err := sp.CommitTrieEpochRootHashIfNeeded(mb, []byte("root"))
	require.NoError(t, err)
}

func TestBaseProcessor_commitTrieEpochRootHashIfNeededShouldUseDataTrieIfNeededWork(t *testing.T) {
	t.Parallel()

	var processDataTrieTests = []struct {
		processDataTrie        bool
		calledWithUserRootHash bool
	}{
		{false, false},
		{true, true},
	}

	for _, tt := range processDataTrieTests {
		epoch := uint32(37)
		rootHash := []byte("userAcc-root-hash")

		coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
		coreComponents.UInt64ByteSliceConv = uint64ByteSlice.NewBigEndianConverter()
		coreComponents.IntMarsh = &mock.MarshalizerStub{
			UnmarshalCalled: func(obj interface{}, buff []byte) error {
				userAccount := obj.(state.UserAccountHandler)
				userAccount.SetRootHash(rootHash)
				return nil
			},
		}

		store := dataRetriever.NewChainStorer()
		store.AddStorer(dataRetriever.TrieEpochRootHashUnit,
			&testscommon.StorerStub{
				PutCalled: func(key, data []byte) error {
					restoredEpoch, err := coreComponents.UInt64ByteSliceConv.ToUint64(key)
					require.NoError(t, err)
					require.Equal(t, epoch, uint32(restoredEpoch))
					return nil
				},
			},
		)
		dataComponents.Storage = store
		calledWithUserAccountRootHash := false
		arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
		arguments.AccountsDB = map[state.AccountsDbIdentifier]state.AccountsAdapter{
			state.UserAccountsState: &testscommon.AccountsStub{
				GetAllLeavesCalled: func(rh []byte) (chan core.KeyValueHolder, error) {
					channel := make(chan core.KeyValueHolder)
					if bytes.Equal(rootHash, rh) {
						calledWithUserAccountRootHash = true
						close(channel)
						return channel, nil
					}

					go func() {
						channel <- keyValStorage.NewKeyValStorage([]byte("address"), []byte("bytes"))
						close(channel)
					}()

					return channel, nil
				},
			},
		}

		arguments.Config.Debug.EpochStart.ProcessDataTrieOnCommitEpoch = tt.processDataTrie
		sp, _ := blproc.NewShardProcessor(arguments)

		mb := &block.MetaBlock{Epoch: epoch}
		err := sp.CommitTrieEpochRootHashIfNeeded(mb, []byte("root"))
		require.NoError(t, err)

		require.Equal(t, tt.calledWithUserRootHash, calledWithUserAccountRootHash)
	}
}

func TestBaseProcessor_updateState(t *testing.T) {
	t.Parallel()

	var pruneRootHash []byte
	var cancelPruneRootHash []byte

	poolMock := testscommon.NewPoolsHolderMock()

	numHeaders := 5
	headers := make([]block.Header, numHeaders)
	for i := 0; i < numHeaders; i++ {
		headers[i] = block.Header{Nonce: uint64(i), RootHash: []byte(strconv.Itoa(i))}
	}

	hdrStore := &testscommon.StorerStub{
		GetCalled: func(key []byte) ([]byte, error) {
			if len(headers) != 0 {
				header := headers[0]
				headers = headers[1:]
				return json.Marshal(header)
			}

			return nil, nil
		},
	}

	storer := &mock.ChainStorerMock{
		GetStorerCalled: func(unitType dataRetriever.UnitType) storage.Storer {
			return hdrStore
		},
	}

	shardC := mock.NewMultiShardsCoordinatorMock(3)

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createComponentHolderMocks()
	dataComponents.DataPool = poolMock
	dataComponents.Storage = storer
	bootstrapComponents.Coordinator = shardC
	arguments := CreateMockArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	arguments.BlockTracker = &mock.BlockTrackerMock{}
	arguments.Config.StateTriesConfig.CheckpointRoundsModulus = 2
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		IsPruningEnabledCalled: func() bool {
			return true
		},
		PruneTrieCalled: func(rootHashParam []byte, identifier data.TriePruningIdentifier) {
			pruneRootHash = rootHashParam
		},
		CancelPruneCalled: func(rootHash []byte, identifier data.TriePruningIdentifier) {
			cancelPruneRootHash = rootHash
		},
	}
	sp, _ := blproc.NewShardProcessor(arguments)

	pruningQueue := queue.NewSliceQueue(uint(numHeaders - 1))

	prevRootHash := []byte("rootHash")
	for i := range headers {
		sp.UpdateState(
			&headers[i],
			headers[i].RootHash,
			prevRootHash,
			arguments.AccountsDB[state.UserAccountsState],
			pruningQueue,
		)
		prevRootHash = headers[i].RootHash

		if i < numHeaders-1 {
			assert.Equal(t, 0, len(pruneRootHash))
			assert.Equal(t, 0, len(cancelPruneRootHash))
		}
	}

	assert.Equal(t, []byte("rootHash"), pruneRootHash)
	assert.Equal(t, []byte("rootHash"), cancelPruneRootHash)
}
