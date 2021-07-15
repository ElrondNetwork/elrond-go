package block_test

import (
	"bytes"
	"errors"
	"math/big"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/blockchain"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/process"
	blproc "github.com/ElrondNetwork/elrond-go/process/block"
	"github.com/ElrondNetwork/elrond-go/process/block/bootstrapStorage"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/dblookupext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockComponentHolders() (
	*mock.CoreComponentsMock,
	*mock.DataComponentsMock,
	*mock.BootstrapComponentsMock,
	*mock.StatusComponentsMock,
) {
	mdp := initDataPool([]byte("tx_hash"))

	coreComponents := &mock.CoreComponentsMock{
		IntMarsh:            &mock.MarshalizerMock{},
		Hash:                &mock.HasherStub{},
		UInt64ByteSliceConv: &mock.Uint64ByteSliceConverterMock{},
		StatusField:         &mock.AppStatusHandlerStub{},
		RoundField:          &mock.RoundHandlerMock{RoundTimeDuration: time.Second},
	}

	dataComponents := &mock.DataComponentsMock{
		Storage:    &mock.ChainStorerMock{},
		DataPool:   mdp,
		BlockChain: createTestBlockchain(),
	}
	boostrapComponents := &mock.BootstrapComponentsMock{
		Coordinator:          mock.NewOneShardCoordinatorMock(),
		HdrIntegrityVerifier: &mock.HeaderIntegrityVerifierStub{},
		VersionedHdrFactory:  &testscommon.VersionedHeaderFactoryStub{},
	}

	statusComponents := &mock.StatusComponentsMock{
		Indexer:      &mock.IndexerMock{},
		TPSBenchmark: &testscommon.TpsBenchmarkMock{},
	}

	return coreComponents, dataComponents, boostrapComponents, statusComponents
}

func createMockMetaArguments(
	coreComponents *mock.CoreComponentsMock,
	dataComponents *mock.DataComponentsMock,
	bootstrapComponents *mock.BootstrapComponentsMock,
	statusComponents *mock.StatusComponentsMock,
) blproc.ArgMetaProcessor {

	argsHeaderValidator := blproc.ArgsHeaderValidator{
		Hasher:      &mock.HasherStub{},
		Marshalizer: &mock.MarshalizerMock{},
	}
	headerValidator, _ := blproc.NewHeaderValidator(argsHeaderValidator)

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	accountsDb := make(map[state.AccountsDbIdentifier]state.AccountsAdapter)
	accountsDb[state.UserAccountsState] = &testscommon.AccountsStub{
		CommitCalled: func() ([]byte, error) {
			return nil, nil
		},
		RootHashCalled: func() ([]byte, error) {
			return nil, nil
		},
	}
	accountsDb[state.PeerAccountsState] = &testscommon.AccountsStub{
		CommitCalled: func() ([]byte, error) {
			return nil, nil
		},
	}

	arguments := blproc.ArgMetaProcessor{
		ArgBaseProcessor: blproc.ArgBaseProcessor{
			CoreComponents:      coreComponents,
			DataComponents:      dataComponents,
			BootstrapComponents: bootstrapComponents,
			StatusComponents:    statusComponents,
			AccountsDB:          accountsDb,
			ForkDetector:        &mock.ForkDetectorMock{},
			NodesCoordinator:    mock.NewNodesCoordinatorMock(),
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
			BlockTracker:                 mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders),
			BlockSizeThrottler:           &mock.BlockSizeThrottlerStub{},
			HistoryRepository:            &dblookupext.HistoryRepositoryStub{},
			EpochNotifier:                &mock.EpochNotifierStub{},
			ScheduledTxsExecutionHandler: &testscommon.ScheduledTxsExecutionStub{},
		},
		SCToProtocol:                 &mock.SCToProtocolStub{},
		PendingMiniBlocksHandler:     &mock.PendingMiniBlocksHandlerStub{},
		EpochStartDataCreator:        &mock.EpochStartDataCreatorStub{},
		EpochEconomics:               &mock.EpochEconomicsStub{},
		EpochRewardsCreator:          &mock.EpochRewardsCreatorStub{},
		EpochValidatorInfoCreator:    &mock.EpochValidatorInfoCreatorStub{},
		ValidatorStatisticsProcessor: &mock.ValidatorStatisticsProcessorStub{},
		EpochSystemSCProcessor:       &mock.EpochStartSystemSCStub{},
	}
	return arguments
}

func createMetaBlockHeader() *block.MetaBlock {
	hdr := block.MetaBlock{
		Nonce:                  1,
		Round:                  1,
		PrevHash:               []byte(""),
		Signature:              []byte("signature"),
		PubKeysBitmap:          []byte("pubKeysBitmap"),
		RootHash:               []byte("rootHash"),
		ShardInfo:              make([]block.ShardData, 0),
		TxCount:                1,
		PrevRandSeed:           make([]byte, 0),
		RandSeed:               make([]byte, 0),
		AccumulatedFeesInEpoch: big.NewInt(0),
		AccumulatedFees:        big.NewInt(0),
		DevFeesInEpoch:         big.NewInt(0),
		DeveloperFees:          big.NewInt(0),
	}

	shardMiniBlockHeaders := make([]block.MiniBlockHeader, 0)
	shardMiniBlockHeader := block.MiniBlockHeader{
		Hash:            []byte("mb_hash1"),
		ReceiverShardID: 0,
		SenderShardID:   0,
		TxCount:         1,
	}
	shardMiniBlockHeaders = append(shardMiniBlockHeaders, shardMiniBlockHeader)
	shardData := block.ShardData{
		Nonce:                 1,
		ShardID:               0,
		HeaderHash:            []byte("hdr_hash1"),
		TxCount:               1,
		ShardMiniBlockHeaders: shardMiniBlockHeaders,
	}
	hdr.ShardInfo = append(hdr.ShardInfo, shardData)

	return &hdr
}

func createGenesisBlocks(shardCoordinator sharding.Coordinator) map[uint32]data.HeaderHandler {
	genesisBlocks := make(map[uint32]data.HeaderHandler)
	for ShardID := uint32(0); ShardID < shardCoordinator.NumberOfShards(); ShardID++ {
		genesisBlocks[ShardID] = createGenesisBlock(ShardID)
	}

	genesisBlocks[core.MetachainShardId] = createGenesisMetaBlock()

	return genesisBlocks
}

func createGenesisBlock(ShardID uint32) *block.Header {
	rootHash := []byte("roothash")
	return &block.Header{
		Nonce:         0,
		Round:         0,
		Signature:     rootHash,
		RandSeed:      rootHash,
		PrevRandSeed:  rootHash,
		ShardID:       ShardID,
		PubKeysBitmap: rootHash,
		RootHash:      rootHash,
		PrevHash:      rootHash,
	}
}

func createGenesisMetaBlock() *block.MetaBlock {
	rootHash := []byte("roothash")
	return &block.MetaBlock{
		Nonce:         0,
		Round:         0,
		Signature:     rootHash,
		RandSeed:      rootHash,
		PrevRandSeed:  rootHash,
		PubKeysBitmap: rootHash,
		RootHash:      rootHash,
		PrevHash:      rootHash,
	}
}

func setLastNotarizedHdr(
	noOfShards uint32,
	round uint64,
	nonce uint64,
	randSeed []byte,
	lastNotarizedHdrs map[uint32][]data.HeaderHandler,
	blockTracker process.BlockTracker,
) {
	for i := uint32(0); i < noOfShards; i++ {
		lastHdr := &block.Header{Round: round,
			Nonce:    nonce,
			RandSeed: randSeed,
			ShardID:  i}
		lastNotarizedHdrsCount := len(lastNotarizedHdrs[i])
		if lastNotarizedHdrsCount > 0 {
			lastNotarizedHdrs[i][lastNotarizedHdrsCount-1] = lastHdr
		} else {
			lastNotarizedHdrs[i] = append(lastNotarizedHdrs[i], lastHdr)
		}
		blockTracker.AddCrossNotarizedHeader(i, lastHdr, nil)
	}
}

// ------- NewMetaProcessor

func TestNewMetaProcessor_NilAccountsAdapterShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.AccountsDB[state.UserAccountsState] = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilAccountsAdapter, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilDataPoolShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = nil
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilDataPoolHolder, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilHeadersDataPoolShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = &testscommon.PoolsHolderStub{
		HeadersCalled: func() dataRetriever.HeadersPool {
			return nil
		},
	}
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilHeadersDataPool, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilSCToProtocolShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.SCToProtocol = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilSCToProtocol, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilForkDetectorShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.ForkDetector = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilForkDetector, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilEpochStartDataCreatorShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.EpochStartDataCreator = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilEpochStartDataCreator, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilEpochEconomicsShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.EpochEconomics = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilEpochEconomics, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilEpochRewardsCreatorShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.EpochRewardsCreator = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilRewardsCreator, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilEpochValidatorInfoCreatorShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.EpochValidatorInfoCreator = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilEpochStartValidatorInfoCreator, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilValidatorStatisticsProcessorShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.ValidatorStatisticsProcessor = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilValidatorStatistics, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilEpochSystemSCProcessorShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.EpochSystemSCProcessor = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilEpochStartSystemSCProcessor, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilShardCoordinatorShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	bootstrapComponents.Coordinator = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilShardCoordinator, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilHasherShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	coreComponents.Hash = nil
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilHasher, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	coreComponents.IntMarsh = nil
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilMarshalizer, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilChainStorerShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.Storage = nil
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilStorage, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilRequestHeaderHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.RequestHandler = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilRequestHandler, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilTxCoordinatorShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.TxCoordinator = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilTransactionCoordinator, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilEpochStartShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.EpochStartTrigger = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilEpochStartTrigger, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilPendingMiniBlocksShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.PendingMiniBlocksHandler = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilPendingMiniBlocksHandler, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilBlockSizeThrottlerShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.BlockSizeThrottler = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilBlockSizeThrottler, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_NilScheduledTxsExecutionHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.ScheduledTxsExecutionHandler = nil

	be, err := blproc.NewMetaProcessor(arguments)
	assert.Equal(t, process.ErrNilScheduledTxsExecutionHandler, err)
	assert.Nil(t, be)
}

func TestNewMetaProcessor_OkValsShouldWork(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())

	mp, err := blproc.NewMetaProcessor(arguments)
	assert.Nil(t, err)
	assert.NotNil(t, mp)
}

// ------- CheckHeaderBodyCorrelation

func TestMetaProcessor_CheckHeaderBodyCorrelationReceiverMissmatch(t *testing.T) {
	t.Parallel()

	hdr, body := createOneHeaderOneBody()
	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	hdr.MiniBlockHeaders[0].ReceiverShardID = body.MiniBlocks[0].ReceiverShardID + 1
	err := mp.CheckHeaderBodyCorrelation(hdr, body)
	assert.Equal(t, process.ErrHeaderBodyMismatch, err)
}

func TestMetaProcessor_CheckHeaderBodyCorrelationSenderMissmatch(t *testing.T) {
	t.Parallel()

	hdr, body := createOneHeaderOneBody()
	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	hdr.MiniBlockHeaders[0].SenderShardID = body.MiniBlocks[0].SenderShardID + 1
	err := mp.CheckHeaderBodyCorrelation(hdr, body)
	assert.Equal(t, process.ErrHeaderBodyMismatch, err)
}

func TestMetaProcessor_CheckHeaderBodyCorrelationTxCountMissmatch(t *testing.T) {
	t.Parallel()

	hdr, body := createOneHeaderOneBody()
	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	hdr.MiniBlockHeaders[0].TxCount = uint32(len(body.MiniBlocks[0].TxHashes) + 1)
	err := mp.CheckHeaderBodyCorrelation(hdr, body)
	assert.Equal(t, process.ErrHeaderBodyMismatch, err)
}

func TestMetaProcessor_CheckHeaderBodyCorrelationHashMissmatch(t *testing.T) {
	t.Parallel()

	hdr, body := createOneHeaderOneBody()
	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	hdr.MiniBlockHeaders[0].Hash = []byte("wrongHash")
	err := mp.CheckHeaderBodyCorrelation(hdr, body)
	assert.Equal(t, process.ErrHeaderBodyMismatch, err)
}

func TestMetaProcessor_CheckHeaderBodyCorrelationShouldPass(t *testing.T) {
	t.Parallel()

	hdr, body := createOneHeaderOneBody()
	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	err := mp.CheckHeaderBodyCorrelation(hdr, body)
	assert.Nil(t, err)
}

func TestMetaProcessor_CheckHeaderBodyCorrelationNilMiniBlock(t *testing.T) {
	t.Parallel()

	hdr, body := createOneHeaderOneBody()
	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	body.MiniBlocks[0] = nil

	err := mp.CheckHeaderBodyCorrelation(hdr, body)
	assert.NotNil(t, err)
	assert.Equal(t, process.ErrNilMiniBlock, err)
}

// ------- ProcessBlock

func TestMetaProcessor_ProcessBlockWithNilHeaderShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())

	mp, _ := blproc.NewMetaProcessor(arguments)
	blk := &block.Body{}

	err := mp.ProcessBlock(nil, blk, haveTime)
	assert.Equal(t, process.ErrNilBlockHeader, err)
}

func TestMetaProcessor_ProcessBlockWithNilBlockBodyShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	err := mp.ProcessBlock(&block.MetaBlock{}, nil, haveTime)
	assert.Equal(t, process.ErrNilBlockBody, err)
}

func TestMetaProcessor_ProcessBlockWithNilHaveTimeFuncShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)
	blk := &block.Body{}

	err := mp.ProcessBlock(&block.MetaBlock{}, blk, nil)
	assert.Equal(t, process.ErrNilHaveTimeHandler, err)
}

func TestMetaProcessor_ProcessWithDirtyAccountShouldErr(t *testing.T) {
	t.Parallel()

	// set accounts dirty
	journalLen := func() int { return 3 }
	revToSnapshot := func(snapshot int) error { return nil }
	hdr := block.MetaBlock{
		Nonce:         1,
		PubKeysBitmap: []byte("0100101"),
		PrevHash:      []byte(""),
		Signature:     []byte("signature"),
		RootHash:      []byte("roothash"),
	}
	body := &block.Body{}
	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		JournalLenCalled:       journalLen,
		RevertToSnapshotCalled: revToSnapshot,
	}
	mp, _ := blproc.NewMetaProcessor(arguments)

	// should return err
	err := mp.ProcessBlock(&hdr, body, haveTime)
	assert.NotNil(t, err)
	assert.Equal(t, err, process.ErrAccountStateDirty)
}

func TestMetaProcessor_ProcessWithHeaderNotFirstShouldErr(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	hdr := &block.MetaBlock{
		Nonce: 2,
	}
	body := &block.Body{}
	err := mp.ProcessBlock(hdr, body, haveTime)
	assert.Equal(t, process.ErrWrongNonceInBlock, err)
}

func TestMetaProcessor_ProcessWithHeaderNotCorrectNonceShouldErr(t *testing.T) {
	t.Parallel()

	blkc, _ := blockchain.NewMetaChain(&mock.AppStatusHandlerStub{})
	_ = blkc.SetCurrentBlockHeader(
		&block.MetaBlock{
			Round: 1,
			Nonce: 1,
		},
	)
	_ = blkc.SetGenesisHeader(&block.MetaBlock{Nonce: 0})
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.BlockChain = blkc
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	mp, _ := blproc.NewMetaProcessor(arguments)
	hdr := &block.MetaBlock{
		Round: 3,
		Nonce: 3,
	}
	body := &block.Body{}

	err := mp.ProcessBlock(hdr, body, haveTime)
	assert.Equal(t, process.ErrWrongNonceInBlock, err)
}

func TestMetaProcessor_ProcessWithHeaderNotCorrectPrevHashShouldErr(t *testing.T) {
	t.Parallel()

	blkc, _ := blockchain.NewMetaChain(&mock.AppStatusHandlerStub{})
	_ = blkc.SetCurrentBlockHeader(
		&block.MetaBlock{
			Round: 1,
			Nonce: 1,
		},
	)
	_ = blkc.SetGenesisHeader(&block.MetaBlock{Nonce: 0})
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.BlockChain = blkc
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	mp, _ := blproc.NewMetaProcessor(arguments)
	hdr := &block.MetaBlock{
		Round:    2,
		Nonce:    2,
		PrevHash: []byte("X"),
	}

	body := &block.Body{}

	err := mp.ProcessBlock(hdr, body, haveTime)
	assert.Equal(t, process.ErrBlockHashDoesNotMatch, err)
}

func TestMetaProcessor_ProcessBlockWithErrOnVerifyStateRootCallShouldRevertState(t *testing.T) {
	t.Parallel()

	blkc, _ := blockchain.NewMetaChain(&mock.AppStatusHandlerStub{})
	_ = blkc.SetCurrentBlockHeader(
		&block.MetaBlock{
			Nonce:                  0,
			AccumulatedFeesInEpoch: big.NewInt(0),
			DevFeesInEpoch:         big.NewInt(0),
		},
	)
	_ = blkc.SetGenesisHeader(&block.MetaBlock{Nonce: 0})
	hdr := createMetaBlockHeader()
	body := &block.Body{}
	// set accounts not dirty
	journalLen := func() int { return 0 }
	wasCalled := false
	revertToSnapshot := func(snapshot int) error {
		wasCalled = true
		return nil
	}
	rootHashCalled := func() ([]byte, error) {
		return []byte("rootHashX"), nil
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.BlockChain = blkc
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		JournalLenCalled:       journalLen,
		RevertToSnapshotCalled: revertToSnapshot,
		RootHashCalled:         rootHashCalled,
	}
	mp, _ := blproc.NewMetaProcessor(arguments)

	go func() {
		mp.ChRcvAllHdrs() <- true
	}()

	// should return err
	mp.SetShardBlockFinality(0)
	hdr.ShardInfo = make([]block.ShardData, 0)
	err := mp.ProcessBlock(hdr, body, haveTime)

	assert.Equal(t, process.ErrRootStateDoesNotMatch, err)
	assert.True(t, wasCalled)
}

// ------- requestFinalMissingHeader
func TestMetaProcessor_RequestFinalMissingHeaderShouldPass(t *testing.T) {
	t.Parallel()

	mdp := initDataPool([]byte("tx_hash"))
	accounts := &testscommon.AccountsStub{}
	accounts.RevertToSnapshotCalled = func(snapshot int) error {
		return nil
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = mdp
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(3)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)
	mp.AddHdrHashToRequestedList(&block.Header{}, []byte("header_hash"))
	mp.SetHighestHdrNonceForCurrentBlock(0, 1)
	mp.SetHighestHdrNonceForCurrentBlock(1, 2)
	mp.SetHighestHdrNonceForCurrentBlock(2, 3)
	res := mp.RequestMissingFinalityAttestingShardHeaders()
	assert.Equal(t, res, uint32(3))
}

// ------- CommitBlock

func TestMetaProcessor_CommitBlockMarshalizerFailForHeaderShouldErr(t *testing.T) {
	t.Parallel()

	accounts := &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			return nil
		},
	}
	errMarshalizer := errors.New("failure")
	hdr := createMetaBlockHeader()
	body := &block.Body{}
	marshalizer := &mock.MarshalizerStub{
		MarshalCalled: func(obj interface{}) (i []byte, e error) {
			if reflect.DeepEqual(obj, hdr) {
				return nil, errMarshalizer
			}

			return []byte("obj"), nil
		},
		UnmarshalCalled: func(obj interface{}, buff []byte) error {
			return nil
		},
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	coreComponents.IntMarsh = marshalizer
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = accounts
	mp, _ := blproc.NewMetaProcessor(arguments)
	err := mp.CommitBlock(hdr, body)
	assert.Equal(t, errMarshalizer, err)
}

func TestMetaProcessor_CommitBlockStorageFailsForHeaderShouldErr(t *testing.T) {
	t.Parallel()

	wasCalled := false
	errPersister := errors.New("failure")
	marshalizer := &mock.MarshalizerMock{}
	accounts := &testscommon.AccountsStub{
		CommitCalled: func() (i []byte, e error) {
			return nil, nil
		},
	}
	hdr := createMetaBlockHeader()
	body := &block.Body{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	hdrUnit := &testscommon.StorerStub{
		PutCalled: func(key, data []byte) error {
			wasCalled = true
			wg.Done()
			return errPersister
		},
		GetCalled: func(key []byte) (i []byte, e error) {
			hdrBuff, _ := marshalizer.Marshal(&block.MetaBlock{})
			return hdrBuff, nil
		},
	}
	store := initStore()
	store.AddStorer(dataRetriever.MetaBlockUnit, hdrUnit)
	blkc, _ := blockchain.NewMetaChain(&mock.AppStatusHandlerStub{})
	genesisHeader := &block.MetaBlock{Nonce: 0}
	_ = blkc.SetGenesisHeader(genesisHeader)

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.Storage = store
	dataComponents.BlockChain = blkc
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = accounts
	arguments.AccountsDB[state.PeerAccountsState] = accounts
	arguments.ForkDetector = &mock.ForkDetectorMock{
		AddHeaderCalled: func(header data.HeaderHandler, hash []byte, state process.BlockHeaderState, selfNotarizedHeaders []data.HeaderHandler, selfNotarizedHeadersHashes [][]byte) error {
			return nil
		},
		GetHighestFinalBlockNonceCalled: func() uint64 {
			return 0
		},
	}
	blockTrackerMock := mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), createGenesisBlocks(bootstrapComponents.ShardCoordinator()))
	blockTrackerMock.GetCrossNotarizedHeaderCalled = func(shardID uint32, offset uint64) (data.HeaderHandler, []byte, error) {
		return &block.Header{}, []byte("hash"), nil
	}
	arguments.BlockTracker = blockTrackerMock
	mp, _ := blproc.NewMetaProcessor(arguments)

	mp.SetHdrForCurrentBlock([]byte("hdr_hash1"), &block.Header{}, true)
	err := mp.CommitBlock(hdr, body)
	wg.Wait()
	assert.True(t, wasCalled)
	assert.Nil(t, err)
}

func TestMetaProcessor_CommitBlockNoTxInPoolShouldErr(t *testing.T) {
	t.Parallel()

	mdp := initDataPool([]byte("tx_hash"))
	hdr := createMetaBlockHeader()
	body := &block.Body{}
	accounts := &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			return nil
		},
		CommitCalled: func() ([]byte, error) {
			return nil, nil
		},
	}
	fd := &mock.ForkDetectorMock{}
	hasher := &mock.HasherStub{}
	store := initStore()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = mdp
	dataComponents.Storage = store
	coreComponents.Hash = hasher
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = accounts
	arguments.ForkDetector = fd
	mp, _ := blproc.NewMetaProcessor(arguments)

	mdp.HeadersCalled = func() dataRetriever.HeadersPool {
		return &mock.HeadersCacherStub{
			MaxSizeCalled: func() int {
				return 1000
			},
		}
	}

	err := mp.CommitBlock(hdr, body)
	assert.True(t, errors.Is(err, process.ErrMissingHeader))
}

func TestMetaProcessor_CommitBlockOkValsShouldWork(t *testing.T) {
	t.Parallel()

	mdp := initDataPool([]byte("tx_hash"))
	rootHash := []byte("rootHash")
	hdr := createMetaBlockHeader()
	body := &block.Body{}
	accounts := &testscommon.AccountsStub{
		CommitCalled: func() (i []byte, e error) {
			return rootHash, nil
		},
		RootHashCalled: func() ([]byte, error) {
			return rootHash, nil
		},
	}
	forkDetectorAddCalled := false
	fd := &mock.ForkDetectorMock{
		AddHeaderCalled: func(header data.HeaderHandler, hash []byte, state process.BlockHeaderState, selfNotarizedHeaders []data.HeaderHandler, selfNotarizedHeadersHashes [][]byte) error {
			if header == hdr {
				forkDetectorAddCalled = true
				return nil
			}

			return errors.New("should have not got here")
		},
		GetHighestFinalBlockNonceCalled: func() uint64 {
			return 0
		},
	}
	hasher := &mock.HasherStub{}
	blockHeaderUnit := &testscommon.StorerStub{
		PutCalled: func(key, data []byte) error {
			return nil
		},
	}
	store := initStore()
	store.AddStorer(dataRetriever.BlockHeaderUnit, blockHeaderUnit)

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = mdp
	dataComponents.Storage = store
	dataComponents.BlockChain = &mock.BlockChainMock{
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}

	coreComponents.Hash = hasher
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = accounts
	arguments.AccountsDB[state.PeerAccountsState] = accounts
	arguments.ForkDetector = fd
	blockTrackerMock := mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), createGenesisBlocks(bootstrapComponents.ShardCoordinator()))
	blockTrackerMock.GetCrossNotarizedHeaderCalled = func(shardID uint32, offset uint64) (data.HeaderHandler, []byte, error) {
		return &block.Header{}, []byte("hash"), nil
	}
	arguments.BlockTracker = blockTrackerMock

	mp, _ := blproc.NewMetaProcessor(arguments)

	mdp.HeadersCalled = func() dataRetriever.HeadersPool {
		cs := &mock.HeadersCacherStub{}
		cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
		}
		cs.GetHeaderByHashCalled = func(hash []byte) (handler data.HeaderHandler, e error) {
			return &block.Header{}, nil
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
	}

	mp.SetHdrForCurrentBlock([]byte("hdr_hash1"), &block.Header{}, true)
	err := mp.CommitBlock(hdr, body)
	assert.Nil(t, err)
	assert.True(t, forkDetectorAddCalled)
	// this should sleep as there is an async call to display current header and block in CommitBlock
	time.Sleep(time.Second)
}

func TestBlockProc_RequestTransactionFromNetwork(t *testing.T) {
	t.Parallel()

	mdp := initDataPool([]byte("tx_hash"))

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = mdp
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)

	mdp.HeadersCalled = func() dataRetriever.HeadersPool {
		cs := &mock.HeadersCacherStub{}
		cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
		}
		cs.GetHeaderByHashCalled = func(hash []byte) (handler data.HeaderHandler, e error) {
			return nil, errors.New("err")
		}
		cs.MaxSizeCalled = func() int {
			return 1000
		}
		return cs
	}

	header := createMetaBlockHeader()
	hdrsRequested, _ := mp.RequestBlockHeaders(header)
	assert.Equal(t, uint32(1), hdrsRequested)
}

func TestMetaProcessor_ApplyBodyToHeaderShouldWork(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = initDataPool([]byte("tx_hash"))
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		JournalLenCalled: func() int {
			return 0
		},
		RootHashCalled: func() ([]byte, error) {
			return []byte("root"), nil
		},
	}

	mp, _ := blproc.NewMetaProcessor(arguments)

	hdr := &block.MetaBlock{}
	_, err := mp.ApplyBodyToHeader(hdr, &block.Body{})
	assert.Nil(t, err)
}

func TestMetaProcessor_ApplyBodyToHeaderShouldSetEpochStart(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = initDataPool([]byte("tx_hash"))
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		JournalLenCalled: func() int {
			return 0
		},
		RootHashCalled: func() ([]byte, error) {
			return []byte("root"), nil
		},
	}

	mp, _ := blproc.NewMetaProcessor(arguments)

	metaBlk := &block.MetaBlock{TimeStamp: 12345}
	body := &block.Body{MiniBlocks: []*block.MiniBlock{{Type: 0}}}
	_, err := mp.ApplyBodyToHeader(metaBlk, body)
	assert.Nil(t, err)
}

func TestMetaProcessor_CommitBlockShouldRevertAccountStateWhenErr(t *testing.T) {
	t.Parallel()

	// set accounts dirty
	journalEntries := 3
	revToSnapshot := func(snapshot int) error {
		journalEntries = 0
		return nil
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = initDataPool([]byte("tx_hash"))
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: revToSnapshot,
	}
	mp, _ := blproc.NewMetaProcessor(arguments)

	err := mp.CommitBlock(nil, nil)
	assert.NotNil(t, err)
	assert.Equal(t, 0, journalEntries)
}

func TestMetaProcessor_RevertStateRevertPeerStateFailsShouldErr(t *testing.T) {
	expectedErr := errors.New("err")
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = initDataPool([]byte("tx_hash"))
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{}
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RecreateTrieCalled: func(rootHash []byte) error {
			return nil
		},
	}
	arguments.ValidatorStatisticsProcessor = &mock.ValidatorStatisticsProcessorStub{
		RevertPeerStateCalled: func(header data.MetaHeaderHandler) error {
			return expectedErr
		},
	}
	mp, err := blproc.NewMetaProcessor(arguments)
	require.Nil(t, err)
	require.NotNil(t, mp)

	hdr := block.MetaBlock{Nonce: 37}
	err = mp.RevertStateToBlock(&hdr, hdr.RootHash)
	require.Equal(t, expectedErr, err)
}

func TestMetaProcessor_RevertStateShouldWork(t *testing.T) {
	recreateTrieWasCalled := false
	revertePeerStateWasCalled := false

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = initDataPool([]byte("tx_hash"))
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)

	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RecreateTrieCalled: func(rootHash []byte) error {
			recreateTrieWasCalled = true
			return nil
		},
	}
	arguments.ValidatorStatisticsProcessor = &mock.ValidatorStatisticsProcessorStub{
		RevertPeerStateCalled: func(header data.MetaHeaderHandler) error {
			revertePeerStateWasCalled = true
			return nil
		},
	}
	mp, _ := blproc.NewMetaProcessor(arguments)

	hdr := block.MetaBlock{Nonce: 37}
	err := mp.RevertStateToBlock(&hdr, hdr.RootHash)
	assert.Nil(t, err)
	assert.True(t, revertePeerStateWasCalled)
	assert.True(t, recreateTrieWasCalled)
}

func TestMetaProcessor_MarshalizedDataToBroadcastShouldWork(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)

	msh, mstx, err := mp.MarshalizedDataToBroadcast(&block.MetaBlock{}, &block.Body{})
	assert.Nil(t, err)
	assert.NotNil(t, msh)
	assert.NotNil(t, mstx)
}

// ------- receivedHeader

func TestMetaProcessor_ReceivedHeaderShouldDecreaseMissing(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)

	// add 3 tx hashes on requested list
	hdrHash1 := []byte("hdr hash 1")
	hdrHash2 := []byte("hdr hash 2")
	hdrHash3 := []byte("hdr hash 3")

	hdr2 := &block.Header{Nonce: 2}

	mp.AddHdrHashToRequestedList(nil, hdrHash1)
	mp.AddHdrHashToRequestedList(nil, hdrHash2)
	mp.AddHdrHashToRequestedList(nil, hdrHash3)

	// received txHash2
	pool.Headers().AddHeader(hdrHash2, hdr2)

	time.Sleep(100 * time.Millisecond)

	assert.True(t, mp.IsHdrMissing(hdrHash1))
	assert.False(t, mp.IsHdrMissing(hdrHash2))
	assert.True(t, mp.IsHdrMissing(hdrHash3))
}

// ------- createShardInfo

func TestMetaProcessor_CreateShardInfoShouldWorkNoHdrAddedNotValid(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	// we will have a 3 hdrs in pool
	hdrHash1 := []byte("hdr hash 1")
	hdrHash2 := []byte("hdr hash 2")
	hdrHash3 := []byte("hdr hash 3")

	mbHash1 := []byte("mb hash 1")
	mbHash2 := []byte("mb hash 2")
	mbHash3 := []byte("mb hash 3")

	miniBlockHeader1 := block.MiniBlockHeader{Hash: mbHash1}
	miniBlockHeader2 := block.MiniBlockHeader{Hash: mbHash2}
	miniBlockHeader3 := block.MiniBlockHeader{Hash: mbHash3}

	miniBlockHeaders1 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader1)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader2)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader3)

	miniBlockHeaders2 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader1)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader2)

	miniBlockHeaders3 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders3 = append(miniBlockHeaders3, miniBlockHeader1)

	// put the existing headers inside datapool
	pool.Headers().AddHeader(hdrHash1, &block.Header{
		Round:            1,
		Nonce:            45,
		ShardID:          0,
		MiniBlockHeaders: miniBlockHeaders1})
	pool.Headers().AddHeader(hdrHash2, &block.Header{
		Round:            2,
		Nonce:            45,
		ShardID:          1,
		MiniBlockHeaders: miniBlockHeaders2})
	pool.Headers().AddHeader(hdrHash3, &block.Header{
		Round:            3,
		Nonce:            45,
		ShardID:          2,
		MiniBlockHeaders: miniBlockHeaders3})

	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	round := uint64(10)
	shardInfo, err := mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(shardInfo))

	metaHdr := &block.MetaBlock{Round: round}
	_, err = mp.CreateBlockBody(metaHdr, func() bool {
		return true
	})
	assert.Nil(t, err)
	shardInfo, err = mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(shardInfo))
}

func TestMetaProcessor_CreateShardInfoShouldWorkNoHdrAddedNotFinal(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	// we will have a 3 hdrs in pool
	hdrHash1 := []byte("hdr hash 1")
	hdrHash2 := []byte("hdr hash 2")
	hdrHash3 := []byte("hdr hash 3")

	mbHash1 := []byte("mb hash 1")
	mbHash2 := []byte("mb hash 2")
	mbHash3 := []byte("mb hash 3")

	miniBlockHeader1 := block.MiniBlockHeader{Hash: mbHash1}
	miniBlockHeader2 := block.MiniBlockHeader{Hash: mbHash2}
	miniBlockHeader3 := block.MiniBlockHeader{Hash: mbHash3}

	miniBlockHeaders1 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader1)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader2)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader3)

	miniBlockHeaders2 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader1)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader2)

	miniBlockHeaders3 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders3 = append(miniBlockHeaders3, miniBlockHeader1)

	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	haveTimeHandler := func() bool { return true }

	prevRandSeed := []byte("prevrand")
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 9, 44, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	// put the existing headers inside datapool
	prevHash, _ := mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(0).(*block.Header))
	hdr1 := &block.Header{
		Round:            10,
		Nonce:            45,
		ShardID:          0,
		PrevRandSeed:     prevRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders1}
	pool.Headers().AddHeader(hdrHash1, hdr1)
	arguments.BlockTracker.AddTrackedHeader(hdr1, hdrHash1)

	prevHash, _ = mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(1).(*block.Header))
	hdr2 := &block.Header{
		Round:            20,
		Nonce:            45,
		ShardID:          1,
		PrevRandSeed:     prevRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders2}
	pool.Headers().AddHeader(hdrHash2, hdr2)
	arguments.BlockTracker.AddTrackedHeader(hdr2, hdrHash2)

	prevHash, _ = mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(2).(*block.Header))
	hdr3 := &block.Header{
		Round:            30,
		Nonce:            45,
		ShardID:          2,
		PrevRandSeed:     prevRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders3}
	pool.Headers().AddHeader(hdrHash3, hdr3)
	arguments.BlockTracker.AddTrackedHeader(hdr3, hdrHash3)

	mp.SetShardBlockFinality(0)
	round := uint64(40)
	shardInfo, err := mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(shardInfo))

	metaHdr := &block.MetaBlock{Round: round}
	_, err = mp.CreateBlockBody(metaHdr, haveTimeHandler)
	assert.Nil(t, err)
	shardInfo, err = mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(shardInfo))
}

func TestMetaProcessor_CreateShardInfoShouldWorkHdrsAdded(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	// we will have a 3 hdrs in pool
	hdrHash1 := []byte("hdr hash 1")
	hdrHash2 := []byte("hdr hash 2")
	hdrHash3 := []byte("hdr hash 3")

	hdrHash11 := []byte("hdr hash 11")
	hdrHash22 := []byte("hdr hash 22")
	hdrHash33 := []byte("hdr hash 33")

	mbHash1 := []byte("mb hash 1")
	mbHash2 := []byte("mb hash 2")
	mbHash3 := []byte("mb hash 3")

	miniBlockHeader1 := block.MiniBlockHeader{Hash: mbHash1}
	miniBlockHeader2 := block.MiniBlockHeader{Hash: mbHash2}
	miniBlockHeader3 := block.MiniBlockHeader{Hash: mbHash3}

	miniBlockHeaders1 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader1)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader2)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader3)

	miniBlockHeaders2 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader1)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader2)

	miniBlockHeaders3 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders3 = append(miniBlockHeaders3, miniBlockHeader1)

	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}
	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	haveTimeHandler := func() bool { return true }

	prevRandSeed := []byte("prevrand")
	currRandSeed := []byte("currrand")
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 9, 44, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	headers := make([]*block.Header, 0)

	// put the existing headers inside datapool

	// header shard 0
	prevHash, _ := mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(0).(*block.Header))
	headers = append(headers, &block.Header{
		Round:            10,
		Nonce:            45,
		ShardID:          0,
		PrevRandSeed:     prevRandSeed,
		RandSeed:         currRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders1})

	prevHash, _ = mp.ComputeHeaderHash(headers[0])
	headers = append(headers, &block.Header{
		Round:            11,
		Nonce:            46,
		ShardID:          0,
		PrevRandSeed:     currRandSeed,
		RandSeed:         []byte("nextrand"),
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders1})

	pool.Headers().AddHeader(hdrHash1, headers[0])
	pool.Headers().AddHeader(hdrHash11, headers[1])
	arguments.BlockTracker.AddTrackedHeader(headers[0], hdrHash1)

	// header shard 1
	prevHash, _ = mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(1).(*block.Header))
	headers = append(headers, &block.Header{
		Round:            10,
		Nonce:            45,
		ShardID:          1,
		PrevRandSeed:     prevRandSeed,
		RandSeed:         currRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders2})

	prevHash, _ = mp.ComputeHeaderHash(headers[2])
	headers = append(headers, &block.Header{
		Round:            11,
		Nonce:            46,
		ShardID:          1,
		PrevRandSeed:     currRandSeed,
		RandSeed:         []byte("nextrand"),
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders2})

	pool.Headers().AddHeader(hdrHash2, headers[2])
	pool.Headers().AddHeader(hdrHash22, headers[3])
	arguments.BlockTracker.AddTrackedHeader(headers[2], hdrHash2)

	// header shard 2
	prevHash, _ = mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(2).(*block.Header))
	headers = append(headers, &block.Header{
		Round:            10,
		Nonce:            45,
		ShardID:          2,
		PrevRandSeed:     prevRandSeed,
		RandSeed:         currRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders3})

	prevHash, _ = mp.ComputeHeaderHash(headers[4])
	headers = append(headers, &block.Header{
		Round:            11,
		Nonce:            46,
		ShardID:          2,
		PrevRandSeed:     currRandSeed,
		RandSeed:         []byte("nextrand"),
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders3})

	pool.Headers().AddHeader(hdrHash3, headers[4])
	pool.Headers().AddHeader(hdrHash33, headers[5])
	arguments.BlockTracker.AddTrackedHeader(headers[4], hdrHash3)

	mp.SetShardBlockFinality(1)
	round := uint64(15)
	shardInfo, err := mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(shardInfo))

	metaHdr := &block.MetaBlock{Round: round}
	_, err = mp.CreateBlockBody(metaHdr, haveTimeHandler)
	assert.Nil(t, err)
	shardInfo, err = mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(shardInfo))
}

func TestMetaProcessor_CreateShardInfoEmptyBlockHDRRoundTooHigh(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	// we will have a 3 hdrs in pool
	hdrHash1 := []byte("hdr hash 1")
	hdrHash2 := []byte("hdr hash 2")
	hdrHash3 := []byte("hdr hash 3")

	hdrHash11 := []byte("hdr hash 11")
	hdrHash22 := []byte("hdr hash 22")
	hdrHash33 := []byte("hdr hash 33")

	mbHash1 := []byte("mb hash 1")
	mbHash2 := []byte("mb hash 2")
	mbHash3 := []byte("mb hash 3")

	miniBlockHeader1 := block.MiniBlockHeader{Hash: mbHash1}
	miniBlockHeader2 := block.MiniBlockHeader{Hash: mbHash2}
	miniBlockHeader3 := block.MiniBlockHeader{Hash: mbHash3}

	miniBlockHeaders1 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader1)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader2)
	miniBlockHeaders1 = append(miniBlockHeaders1, miniBlockHeader3)

	miniBlockHeaders2 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader1)
	miniBlockHeaders2 = append(miniBlockHeaders2, miniBlockHeader2)

	miniBlockHeaders3 := make([]block.MiniBlockHeader, 0)
	miniBlockHeaders3 = append(miniBlockHeaders3, miniBlockHeader1)

	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	haveTimeHandler := func() bool { return true }

	prevRandSeed := []byte("prevrand")
	currRandSeed := []byte("currrand")
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 9, 44, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	headers := make([]*block.Header, 0)

	// put the existing headers inside datapool

	// header shard 0
	prevHash, _ := mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(0).(*block.Header))
	headers = append(headers, &block.Header{
		Round:            10,
		Nonce:            45,
		ShardID:          0,
		PrevRandSeed:     prevRandSeed,
		RandSeed:         currRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders1})

	prevHash, _ = mp.ComputeHeaderHash(headers[0])
	headers = append(headers, &block.Header{
		Round:            11,
		Nonce:            46,
		ShardID:          0,
		PrevRandSeed:     currRandSeed,
		RandSeed:         []byte("nextrand"),
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders1})

	pool.Headers().AddHeader(hdrHash1, headers[0])
	pool.Headers().AddHeader(hdrHash11, headers[1])
	arguments.BlockTracker.AddTrackedHeader(headers[0], hdrHash1)

	// header shard 1
	prevHash, _ = mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(1).(*block.Header))
	headers = append(headers, &block.Header{
		Round:            10,
		Nonce:            45,
		ShardID:          1,
		PrevRandSeed:     prevRandSeed,
		RandSeed:         currRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders2})

	prevHash, _ = mp.ComputeHeaderHash(headers[2])
	headers = append(headers, &block.Header{
		Round:            11,
		Nonce:            46,
		ShardID:          1,
		PrevRandSeed:     currRandSeed,
		RandSeed:         []byte("nextrand"),
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders2})

	pool.Headers().AddHeader(hdrHash2, headers[2])
	pool.Headers().AddHeader(hdrHash22, headers[3])
	arguments.BlockTracker.AddTrackedHeader(headers[2], hdrHash2)

	// header shard 2
	prevHash, _ = mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(2).(*block.Header))
	headers = append(headers, &block.Header{
		Round:            10,
		Nonce:            45,
		ShardID:          2,
		PrevRandSeed:     prevRandSeed,
		RandSeed:         currRandSeed,
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders3})

	prevHash, _ = mp.ComputeHeaderHash(headers[4])
	headers = append(headers, &block.Header{
		Round:            11,
		Nonce:            46,
		ShardID:          2,
		PrevRandSeed:     currRandSeed,
		RandSeed:         []byte("nextrand"),
		PrevHash:         prevHash,
		MiniBlockHeaders: miniBlockHeaders3})

	pool.Headers().AddHeader(hdrHash3, headers[4])
	pool.Headers().AddHeader(hdrHash33, headers[5])
	arguments.BlockTracker.AddTrackedHeader(headers[4], hdrHash3)

	mp.SetShardBlockFinality(1)
	round := uint64(20)
	shardInfo, err := mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(shardInfo))

	metaHdr := &block.MetaBlock{Round: round}
	_, err = mp.CreateBlockBody(metaHdr, haveTimeHandler)
	assert.Nil(t, err)
	shardInfo, err = mp.CreateShardInfo()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(shardInfo))
}

func TestMetaProcessor_RestoreBlockIntoPoolsShouldErrNilMetaBlockHeader(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.Storage = initStore()
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)

	err := mp.RestoreBlockIntoPools(nil, nil)
	assert.NotNil(t, err)
	assert.Equal(t, err, process.ErrNilMetaBlockHeader)
}

func TestMetaProcessor_RestoreBlockIntoPoolsShouldWork(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	marshalizerMock := &mock.MarshalizerMock{}
	body := &block.Body{}
	hdr := block.Header{Nonce: 1}
	buffHdr, _ := marshalizerMock.Marshal(&hdr)
	hdrHash := []byte("hdr_hash1")

	store := &mock.ChainStorerMock{
		GetStorerCalled: func(unitType dataRetriever.UnitType) storage.Storer {
			return &testscommon.StorerStub{
				RemoveCalled: func(key []byte) error {
					return nil
				},
				GetCalled: func(key []byte) ([]byte, error) {
					return buffHdr, nil
				},
			}
		},
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = store
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)

	mhdr := createMetaBlockHeader()

	err := mp.RestoreBlockIntoPools(mhdr, body)

	hdrFromPool, _ := pool.Headers().GetHeaderByHash(hdrHash)
	assert.Nil(t, err)
	assert.Equal(t, &hdr, hdrFromPool)
}

func TestMetaProcessor_CreateLastNotarizedHdrs(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	coreComponents.Hash = &mock.HasherMock{}
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	prevRandSeed := []byte("prevrand")
	currRandSeed := []byte("currrand")
	notarizedHdrs := mp.NotarizedHdrs()
	firstNonce := uint64(44)
	setLastNotarizedHdr(noOfShards, 9, firstNonce, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	// put the existing headers inside datapool

	// header shard 0
	prevHash, _ := mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(0).(*block.Header))
	prevHdr := &block.Header{
		Round:        10,
		Nonce:        45,
		ShardID:      0,
		PrevRandSeed: prevRandSeed,
		RandSeed:     currRandSeed,
		PrevHash:     prevHash,
		RootHash:     []byte("prevRootHash")}

	prevHash, _ = mp.ComputeHeaderHash(prevHdr)
	currHdr := &block.Header{
		Round:        11,
		Nonce:        46,
		ShardID:      0,
		PrevRandSeed: currRandSeed,
		RandSeed:     []byte("nextrand"),
		PrevHash:     prevHash,
		RootHash:     []byte("currRootHash")}
	currHash, _ := mp.ComputeHeaderHash(currHdr)
	prevHash, _ = mp.ComputeHeaderHash(prevHdr)

	metaHdr := &block.MetaBlock{Round: 15}
	shDataCurr := block.ShardData{ShardID: 0, HeaderHash: currHash}
	metaHdr.ShardInfo = make([]block.ShardData, 0)
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataCurr)
	shDataPrev := block.ShardData{ShardID: 0, HeaderHash: prevHash}
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataPrev)

	// test header not in pool and defer called
	err := mp.SaveLastNotarizedHeader(metaHdr)
	assert.True(t, errors.Is(err, process.ErrMissingHeader))
	assert.Equal(t, firstNonce, mp.LastNotarizedHdrForShard(currHdr.ShardID).GetNonce())

	// wrong header type in pool and defer called
	pool.Headers().AddHeader(currHash, metaHdr)
	pool.Headers().AddHeader(prevHash, prevHdr)
	mp.SetHdrForCurrentBlock(currHash, metaHdr, true)
	mp.SetHdrForCurrentBlock(prevHash, prevHdr, true)

	err = mp.SaveLastNotarizedHeader(metaHdr)
	assert.Equal(t, process.ErrWrongTypeAssertion, err)
	assert.Equal(t, firstNonce, mp.LastNotarizedHdrForShard(currHdr.ShardID).GetNonce())

	// put headers in pool
	pool.Headers().AddHeader(currHash, currHdr)
	pool.Headers().AddHeader(prevHash, prevHdr)
	_ = mp.CreateBlockStarted()
	mp.SetHdrForCurrentBlock(currHash, currHdr, true)
	mp.SetHdrForCurrentBlock(prevHash, prevHdr, true)

	err = mp.SaveLastNotarizedHeader(metaHdr)
	assert.Nil(t, err)
	assert.Equal(t, currHdr, mp.LastNotarizedHdrForShard(currHdr.ShardID))
}

func TestMetaProcessor_CheckShardHeadersValidity(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	coreComponents.Hash = &mock.HasherMock{}
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)

	argsHeaderValidator := blproc.ArgsHeaderValidator{
		Hasher:      coreComponents.Hash,
		Marshalizer: coreComponents.InternalMarshalizer(),
	}
	arguments.HeaderValidator, _ = blproc.NewHeaderValidator(argsHeaderValidator)

	mp, _ := blproc.NewMetaProcessor(arguments)

	prevRandSeed := []byte("prevrand")
	currRandSeed := []byte("currrand")
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 9, 44, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	// put the existing headers inside datapool

	// header shard 0
	prevHash, _ := mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(0).(*block.Header))
	prevHdr := &block.Header{
		Round:           10,
		Nonce:           45,
		ShardID:         0,
		PrevRandSeed:    prevRandSeed,
		RandSeed:        currRandSeed,
		PrevHash:        prevHash,
		RootHash:        []byte("prevRootHash"),
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}

	prevHash, _ = mp.ComputeHeaderHash(prevHdr)
	currHdr := &block.Header{
		Round:           11,
		Nonce:           46,
		ShardID:         0,
		PrevRandSeed:    currRandSeed,
		RandSeed:        []byte("nextrand"),
		PrevHash:        prevHash,
		RootHash:        []byte("currRootHash"),
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}
	currHash, _ := mp.ComputeHeaderHash(currHdr)
	pool.Headers().AddHeader(currHash, currHdr)
	prevHash, _ = mp.ComputeHeaderHash(prevHdr)
	pool.Headers().AddHeader(prevHash, prevHdr)
	wrongCurrHdr := &block.Header{
		Round:           11,
		Nonce:           48,
		ShardID:         0,
		PrevRandSeed:    currRandSeed,
		RandSeed:        []byte("nextrand"),
		PrevHash:        prevHash,
		RootHash:        []byte("currRootHash"),
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}
	wrongCurrHash, _ := mp.ComputeHeaderHash(wrongCurrHdr)
	pool.Headers().AddHeader(wrongCurrHash, wrongCurrHdr)

	metaHdr := &block.MetaBlock{Round: 20}
	shDataCurr := block.ShardData{ShardID: 0, HeaderHash: wrongCurrHash}
	metaHdr.ShardInfo = make([]block.ShardData, 0)
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataCurr)
	shDataPrev := block.ShardData{ShardID: 0, HeaderHash: prevHash}
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataPrev)

	mp.SetHdrForCurrentBlock(wrongCurrHash, wrongCurrHdr, true)
	mp.SetHdrForCurrentBlock(prevHash, prevHdr, true)

	_, err := mp.CheckShardHeadersValidity(metaHdr)
	assert.True(t, errors.Is(err, process.ErrWrongNonceInBlock))

	shDataCurr = block.ShardData{
		ShardID:         0,
		HeaderHash:      currHash,
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}
	metaHdr.ShardInfo = make([]block.ShardData, 0)
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataCurr)
	shDataPrev = block.ShardData{
		ShardID:         0,
		HeaderHash:      prevHash,
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataPrev)

	_ = mp.CreateBlockStarted()
	mp.SetHdrForCurrentBlock(currHash, currHdr, true)
	mp.SetHdrForCurrentBlock(prevHash, prevHdr, true)

	highestNonceHdrs, err := mp.CheckShardHeadersValidity(metaHdr)
	assert.Nil(t, err)
	assert.NotNil(t, highestNonceHdrs)
	assert.Equal(t, currHdr.Nonce, highestNonceHdrs[currHdr.ShardID].GetNonce())
}

func TestMetaProcessor_CheckShardHeadersValidityWrongNonceFromLastNoted(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	prevRandSeed := []byte("prevrand")
	currRandSeed := []byte("currrand")
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 9, 44, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	// put the existing headers inside datapool
	currHdr := &block.Header{
		Round:        11,
		Nonce:        46,
		ShardID:      0,
		PrevRandSeed: currRandSeed,
		RandSeed:     []byte("nextrand"),
		PrevHash:     []byte("prevhash"),
		RootHash:     []byte("currRootHash")}
	currHash := []byte("currHash")
	pool.Headers().AddHeader(currHash, currHdr)
	metaHdr := &block.MetaBlock{Round: 20}

	shDataCurr := block.ShardData{ShardID: 0, HeaderHash: currHash}
	metaHdr.ShardInfo = make([]block.ShardData, 0)
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataCurr)

	mp.SetHdrForCurrentBlock(currHash, currHdr, true)

	highestNonceHdrs, err := mp.CheckShardHeadersValidity(metaHdr)
	assert.Nil(t, highestNonceHdrs)
	assert.True(t, errors.Is(err, process.ErrWrongNonceInBlock))
}

func TestMetaProcessor_CheckShardHeadersValidityRoundZeroLastNoted(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()

	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	dataComponents.BlockChain, _ = blockchain.NewMetaChain(&mock.AppStatusHandlerStub{})
	_ = dataComponents.Blockchain().SetGenesisHeader(&block.MetaBlock{Nonce: 0})
	_ = dataComponents.Blockchain().SetCurrentBlockHeader(&block.MetaBlock{Nonce: 1})
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	prevRandSeed := startHeaders[0].GetRandSeed()
	currRandSeed := []byte("currrand")
	prevHash, _ := mp.ComputeHeaderHash(startHeaders[0])
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 0, 0, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	// put the existing headers inside datapool
	currHdr := &block.Header{
		Round:           1,
		Nonce:           1,
		ShardID:         0,
		PrevRandSeed:    prevRandSeed,
		RandSeed:        currRandSeed,
		PrevHash:        prevHash,
		RootHash:        []byte("currRootHash"),
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}
	currHash := []byte("currhash")
	metaHdr := &block.MetaBlock{Round: 20}

	shDataCurr := block.ShardData{
		ShardID:         0,
		HeaderHash:      currHash,
		AccumulatedFees: big.NewInt(0),
		DeveloperFees:   big.NewInt(0),
	}
	metaHdr.ShardInfo = make([]block.ShardData, 0)
	metaHdr.ShardInfo = append(metaHdr.ShardInfo, shDataCurr)

	highestNonceHdrs, err := mp.CheckShardHeadersValidity(metaHdr)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(highestNonceHdrs))
	assert.Nil(t, err)

	pool.Headers().AddHeader(currHash, currHdr)
	mp.SetHdrForCurrentBlock(currHash, currHdr, true)
	highestNonceHdrs, err = mp.CheckShardHeadersValidity(metaHdr)
	assert.NotNil(t, highestNonceHdrs)
	assert.Nil(t, err)
	assert.Equal(t, currHdr.Nonce, highestNonceHdrs[currHdr.ShardID].GetNonce())
}

func TestMetaProcessor_CheckShardHeadersFinality(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	prevRandSeed := []byte("prevrand")
	currRandSeed := []byte("currrand")
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 9, 44, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	// put the existing headers inside datapool

	// header shard 0
	prevHash, _ := mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(0).(*block.Header))
	prevHdr := &block.Header{
		Round:        10,
		Nonce:        45,
		ShardID:      0,
		PrevRandSeed: prevRandSeed,
		RandSeed:     currRandSeed,
		PrevHash:     prevHash,
		RootHash:     []byte("prevRootHash")}

	prevHash, _ = mp.ComputeHeaderHash(prevHdr)
	currHdr := &block.Header{
		Round:        11,
		Nonce:        46,
		ShardID:      0,
		PrevRandSeed: currRandSeed,
		RandSeed:     []byte("nextrand"),
		PrevHash:     prevHash,
		RootHash:     []byte("currRootHash")}

	nextWrongHdr := &block.Header{
		Round:        11,
		Nonce:        44,
		ShardID:      0,
		PrevRandSeed: currRandSeed,
		RandSeed:     []byte("nextrand"),
		PrevHash:     prevHash,
		RootHash:     []byte("currRootHash")}
	prevHash, _ = mp.ComputeHeaderHash(nextWrongHdr)
	pool.Headers().AddHeader(prevHash, nextWrongHdr)

	mp.SetShardBlockFinality(0)
	metaHdr := &block.MetaBlock{Round: 1}

	highestNonceHdrs := make(map[uint32]data.HeaderHandler)
	for i := uint32(0); i < noOfShards; i++ {
		highestNonceHdrs[i] = nil
	}

	err := mp.CheckShardHeadersFinality(highestNonceHdrs)
	assert.Equal(t, process.ErrNilBlockHeader, err)

	for i := uint32(0); i < noOfShards; i++ {
		highestNonceHdrs[i] = mp.LastNotarizedHdrForShard(i)
	}

	// should work for empty highest nonce hdrs - no hdrs added this round to metablock
	err = mp.CheckShardHeadersFinality(nil)
	assert.Nil(t, err)

	mp.SetShardBlockFinality(0)
	highestNonceHdrs = make(map[uint32]data.HeaderHandler)
	highestNonceHdrs[0] = currHdr
	err = mp.CheckShardHeadersFinality(highestNonceHdrs)
	assert.Nil(t, err)

	mp.SetShardBlockFinality(1)
	err = mp.CheckShardHeadersFinality(highestNonceHdrs)
	assert.Equal(t, process.ErrHeaderNotFinal, err)

	prevHash, _ = mp.ComputeHeaderHash(currHdr)
	nextHdr := &block.Header{
		Round:        12,
		Nonce:        47,
		ShardID:      0,
		PrevRandSeed: []byte("nextrand"),
		RandSeed:     []byte("nextnextrand"),
		PrevHash:     prevHash,
		RootHash:     []byte("currRootHash")}

	nextHash, _ := mp.ComputeHeaderHash(nextHdr)
	pool.Headers().AddHeader(nextHash, nextHdr)
	mp.SetHdrForCurrentBlock(nextHash, nextHdr, false)

	metaHdr.Round = 20
	err = mp.CheckShardHeadersFinality(highestNonceHdrs)
	assert.Nil(t, err)
}

func TestMetaProcessor_IsHdrConstructionValid(t *testing.T) {
	t.Parallel()

	pool := testscommon.NewPoolsHolderMock()
	noOfShards := uint32(5)
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = pool
	dataComponents.Storage = initStore()
	bootstrapComponents.Coordinator = mock.NewMultiShardsCoordinatorMock(noOfShards)
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RevertToSnapshotCalled: func(snapshot int) error {
			assert.Fail(t, "revert should have not been called")
			return nil
		},
		JournalLenCalled: func() int {
			return 0
		},
	}

	startHeaders := createGenesisBlocks(bootstrapComponents.ShardCoordinator())
	arguments.BlockTracker = mock.NewBlockTrackerMock(bootstrapComponents.ShardCoordinator(), startHeaders)
	mp, _ := blproc.NewMetaProcessor(arguments)

	prevRandSeed := []byte("prevrand")
	currRandSeed := []byte("currrand")
	notarizedHdrs := mp.NotarizedHdrs()
	setLastNotarizedHdr(noOfShards, 9, 44, prevRandSeed, notarizedHdrs, arguments.BlockTracker)

	// put the existing headers inside datapool

	// header shard 0
	prevHash, _ := mp.ComputeHeaderHash(mp.LastNotarizedHdrForShard(0).(*block.Header))
	prevHdr := &block.Header{
		Round:        10,
		Nonce:        45,
		ShardID:      0,
		PrevRandSeed: prevRandSeed,
		RandSeed:     currRandSeed,
		PrevHash:     prevHash,
		RootHash:     []byte("prevRootHash")}

	prevHash, _ = mp.ComputeHeaderHash(prevHdr)
	currHdr := &block.Header{
		Round:        11,
		Nonce:        46,
		ShardID:      0,
		PrevRandSeed: currRandSeed,
		RandSeed:     []byte("nextrand"),
		PrevHash:     prevHash,
		RootHash:     []byte("currRootHash")}

	err := mp.IsHdrConstructionValid(nil, prevHdr)
	assert.Equal(t, err, process.ErrNilBlockHeader)

	err = mp.IsHdrConstructionValid(currHdr, nil)
	assert.Equal(t, err, process.ErrNilBlockHeader)

	currHdr.Nonce = 0
	err = mp.IsHdrConstructionValid(currHdr, prevHdr)
	assert.Equal(t, err, process.ErrWrongNonceInBlock)

	currHdr.Nonce = 46
	prevHdr.Nonce = 45
	prevHdr.Round = currHdr.Round + 1
	err = mp.IsHdrConstructionValid(currHdr, prevHdr)
	assert.Equal(t, err, process.ErrLowerRoundInBlock)

	prevHdr.Round = currHdr.Round - 1
	currHdr.Nonce = prevHdr.Nonce + 2
	err = mp.IsHdrConstructionValid(currHdr, prevHdr)
	assert.Equal(t, err, process.ErrWrongNonceInBlock)

	currHdr.Nonce = prevHdr.Nonce + 1
	currHdr.PrevHash = []byte("wronghash")
	err = mp.IsHdrConstructionValid(currHdr, prevHdr)
	assert.Equal(t, err, process.ErrBlockHashDoesNotMatch)

	prevHdr.RandSeed = []byte("randomwrong")
	currHdr.PrevHash, _ = mp.ComputeHeaderHash(prevHdr)
	err = mp.IsHdrConstructionValid(currHdr, prevHdr)
	assert.Equal(t, err, process.ErrRandSeedDoesNotMatch)

	currHdr.PrevHash = prevHash
	prevHdr.RandSeed = currRandSeed
	prevHdr.RootHash = []byte("prevRootHash")
	err = mp.IsHdrConstructionValid(currHdr, prevHdr)
	assert.Nil(t, err)
}

func TestMetaProcessor_DecodeBlockBody(t *testing.T) {
	t.Parallel()

	marshalizerMock := &mock.MarshalizerMock{}
	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)
	b := &block.Body{}
	message, err := marshalizerMock.Marshal(b)
	assert.Nil(t, err)

	bodyNil := &block.Body{}
	dcdBlk := mp.DecodeBlockBody(nil)
	assert.Equal(t, bodyNil, dcdBlk)

	dcdBlk = mp.DecodeBlockBody(message)
	assert.Equal(t, b, dcdBlk)
}

func TestMetaProcessor_DecodeBlockHeader(t *testing.T) {
	t.Parallel()

	marshalizerMock := &mock.MarshalizerMock{}
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.BlockChain = &mock.BlockChainMock{
		CreateNewHeaderCalled: func() data.HeaderHandler {
			return &block.MetaBlock{}
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.MetaBlock{Nonce: 0}
		},
	}
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)
	hdr := &block.MetaBlock{}
	hdr.Nonce = 1
	hdr.TimeStamp = uint64(0)
	hdr.Signature = []byte("A")
	hdr.AccumulatedFees = new(big.Int)
	hdr.AccumulatedFeesInEpoch = new(big.Int)
	_, err := marshalizerMock.Marshal(hdr)
	assert.Nil(t, err)

	message, err := marshalizerMock.Marshal(hdr)
	assert.Nil(t, err)

	dcdHdr := mp.DecodeBlockHeader(nil)
	assert.Nil(t, dcdHdr)

	dcdHdr = mp.DecodeBlockHeader(message)
	assert.Equal(t, hdr, dcdHdr)
	assert.Equal(t, []byte("A"), dcdHdr.GetSignature())
}

func TestMetaProcessor_UpdateShardsHeadersNonce_ShouldWork(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)

	numberOfShards := uint32(4)
	type DataForMap struct {
		ShardID     uint32
		HeaderNonce uint64
	}
	testData := []DataForMap{
		{uint32(0), uint64(100)},
		{uint32(1), uint64(200)},
		{uint32(2), uint64(300)},
		{uint32(3), uint64(400)},
		{uint32(0), uint64(400)},
	}

	for i := range testData {
		mp.UpdateShardsHeadersNonce(testData[i].ShardID, testData[i].HeaderNonce)
	}

	shardsHeadersNonce := mp.GetShardsHeadersNonce()

	mapDates := make([]uint64, 0)

	// Get all data from map and put then in a slice
	for i := uint32(0); i < numberOfShards; i++ {
		mapDataI, _ := shardsHeadersNonce.Load(i)
		mapDates = append(mapDates, mapDataI.(uint64))

	}

	// Check data from map is stored correctly
	expectedData := []uint64{400, 200, 300, 400}
	for i := 0; i < int(numberOfShards); i++ {
		assert.Equal(t, expectedData[i], mapDates[i])
	}
}

func TestMetaProcessor_CreateMiniBlocksJournalLenNotZeroShouldReturnEmptyBody(t *testing.T) {
	t.Parallel()

	accntAdapter := &testscommon.AccountsStub{
		JournalLenCalled: func() int {
			return 1
		},
	}
	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.AccountsDB[state.UserAccountsState] = accntAdapter
	mp, _ := blproc.NewMetaProcessor(arguments)
	round := uint64(10)
	metaHdr := &block.MetaBlock{Round: round}

	bodyHandler, err := mp.CreateBlockBody(metaHdr, func() bool { return true })
	assert.Nil(t, err)
	assert.Equal(t, &block.Body{}, bodyHandler)
}

func TestMetaProcessor_CreateMiniBlocksNoTimeShouldReturnEmptyBody(t *testing.T) {
	t.Parallel()

	arguments := createMockMetaArguments(createMockComponentHolders())
	mp, _ := blproc.NewMetaProcessor(arguments)
	round := uint64(10)
	metaHdr := &block.MetaBlock{Round: round}

	bodyHandler, err := mp.CreateBlockBody(metaHdr, func() bool { return false })
	assert.Nil(t, err)
	assert.Equal(t, &block.Body{}, bodyHandler)
}

func TestMetaProcessor_CreateMiniBlocksDestMe(t *testing.T) {
	t.Parallel()

	hash1 := []byte("hash1")
	hdr1 := &block.Header{
		Nonce:            1,
		Round:            1,
		PrevRandSeed:     []byte("roothash"),
		MiniBlockHeaders: []block.MiniBlockHeader{{Hash: hash1, SenderShardID: 1}},
	}
	hdrHash1Bytes := []byte("hdr_hash1")
	hdr2 := &block.Header{Nonce: 2, Round: 2}
	hdrHash2Bytes := []byte("hdr_hash2")
	expectedMiniBlock1 := &block.MiniBlock{TxHashes: [][]byte{hash1}}
	expectedMiniBlock2 := &block.MiniBlock{TxHashes: [][]byte{[]byte("hash2")}}
	dPool := initDataPool([]byte("tx_hash"))
	dPool.TransactionsCalled = func() dataRetriever.ShardedDataCacherNotifier {
		return testscommon.NewShardedDataStub()
	}
	dPool.HeadersCalled = func() dataRetriever.HeadersPool {
		cs := &mock.HeadersCacherStub{}
		cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
		}
		cs.GetHeaderByNonceAndShardIdCalled = func(hdrNonce uint64, shardId uint32) (handlers []data.HeaderHandler, i [][]byte, e error) {
			if hdrNonce == 1 {
				return []data.HeaderHandler{hdr1}, [][]byte{hdrHash1Bytes}, nil
			}
			if hdrNonce == 2 {
				return []data.HeaderHandler{hdr2}, [][]byte{hdrHash2Bytes}, nil
			}
			return nil, nil, errors.New("err")
		}
		cs.LenCalled = func() int {
			return 0
		}
		cs.NoncesCalled = func(shardId uint32) []uint64 {
			return []uint64{1, 2}
		}
		cs.MaxSizeCalled = func() int {
			return 1000
		}
		return cs
	}

	txCoordinator := &mock.TransactionCoordinatorMock{
		CreateMbsAndProcessCrossShardTransactionsDstMeCalled: func(header data.HeaderHandler, processedMiniBlocksHashes map[string]struct{}, haveTime func() bool) (slices block.MiniBlockSlice, u uint32, b bool, err error) {
			return block.MiniBlockSlice{expectedMiniBlock1}, 0, true, nil
		},
		CreateMbsAndProcessTransactionsFromMeCalled: func(haveTime func() bool) block.MiniBlockSlice {
			return block.MiniBlockSlice{expectedMiniBlock2}
		},
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = dPool
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.TxCoordinator = txCoordinator
	arguments.BlockTracker.AddTrackedHeader(hdr1, hdrHash1Bytes)

	mp, _ := blproc.NewMetaProcessor(arguments)
	round := uint64(10)

	metaHdr := &block.MetaBlock{Round: round}
	bodyHandler, err := mp.CreateBlockBody(metaHdr, func() bool { return true })
	b, _ := bodyHandler.(*block.Body)

	t.Logf("The block: %#v", b)
	assert.Equal(t, expectedMiniBlock1, b.MiniBlocks[0])
	assert.Equal(t, expectedMiniBlock2, b.MiniBlocks[1])
	assert.Nil(t, err)
}

func TestMetaProcessor_ProcessBlockWrongHeaderShouldErr(t *testing.T) {
	t.Parallel()

	journalLen := func() int { return 0 }
	revToSnapshot := func(snapshot int) error { return nil }
	hdr := block.MetaBlock{
		Nonce:         1,
		PubKeysBitmap: []byte("0100101"),
		PrevHash:      []byte(""),
		Signature:     []byte("signature"),
		RootHash:      []byte("roothash"),
		ShardInfo: []block.ShardData{
			{
				ShardID:               0,
				ShardMiniBlockHeaders: []block.MiniBlockHeader{{Hash: []byte("hash1")}},
			},
			{
				ShardID:               1,
				ShardMiniBlockHeaders: []block.MiniBlockHeader{{Hash: []byte("hash2")}},
			},
		},
	}
	body := &block.Body{
		MiniBlocks: []*block.MiniBlock{
			{
				TxHashes:        [][]byte{[]byte("hashTx")},
				ReceiverShardID: 0,
			},
		},
	}
	arguments := createMockMetaArguments(createMockComponentHolders())
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		JournalLenCalled:       journalLen,
		RevertToSnapshotCalled: revToSnapshot,
	}
	mp, _ := blproc.NewMetaProcessor(arguments)

	// should return err
	err := mp.ProcessBlock(&hdr, body, haveTime)
	assert.Equal(t, process.ErrHeaderBodyMismatch, err)
}

func TestMetaProcessor_ProcessBlockNoShardHeadersReceivedShouldErr(t *testing.T) {
	t.Parallel()

	hash1 := []byte("hash1")
	hash2 := []byte("hash2")
	hasher := &mock.HasherStub{}
	hasher.ComputeCalled = func(s string) []byte {
		return []byte("hash1")
	}
	journalLen := func() int { return 0 }
	revToSnapshot := func(snapshot int) error { return nil }
	hdr := block.MetaBlock{
		Nonce:         1,
		PubKeysBitmap: []byte("0100101"),
		PrevHash:      []byte(""),
		Signature:     []byte("signature"),
		RootHash:      []byte("roothash"),
		ShardInfo: []block.ShardData{
			{
				Nonce:                 1,
				ShardID:               0,
				ShardMiniBlockHeaders: []block.MiniBlockHeader{{Hash: []byte("hashTx"), TxCount: 1}},
				TxCount:               1,
			},
			{
				Nonce:                 1,
				ShardID:               0,
				ShardMiniBlockHeaders: []block.MiniBlockHeader{{Hash: hash2, TxCount: 1}},
				TxCount:               1,
			},
		},
		MiniBlockHeaders: []block.MiniBlockHeader{{Hash: hash1, SenderShardID: 0, TxCount: 1}},
	}
	body := &block.Body{
		MiniBlocks: []*block.MiniBlock{
			{
				TxHashes:        [][]byte{hash1},
				ReceiverShardID: 0,
			},
		},
	}
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	coreComponents.Hash = hasher
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		JournalLenCalled:       journalLen,
		RevertToSnapshotCalled: revToSnapshot,
	}
	mp, _ := blproc.NewMetaProcessor(arguments)

	err := mp.ProcessBlock(&hdr, body, haveTime)
	assert.Equal(t, process.ErrTimeIsOut, err)
}

func TestMetaProcessor_VerifyCrossShardMiniBlocksDstMe(t *testing.T) {
	t.Parallel()

	hash1 := []byte("hash1")
	hash2 := []byte("hash2")
	hdrHash1Bytes := []byte("hdr_hash1")
	hdrHash2Bytes := []byte("hdr_hash2")
	miniBlock1 := &block.MiniBlock{TxHashes: [][]byte{hash1}}
	miniBlock2 := &block.MiniBlock{TxHashes: [][]byte{hash2}}
	dPool := initDataPool([]byte("tx_hash"))
	dPool.TransactionsCalled = func() dataRetriever.ShardedDataCacherNotifier {
		return testscommon.NewShardedDataStub()
	}
	dPool.HeadersCalled = func() dataRetriever.HeadersPool {
		cs := &mock.HeadersCacherStub{}
		cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
		}
		cs.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			if bytes.Equal(hdrHash1Bytes, key) {
				return &block.Header{
					Nonce:            1,
					Round:            1,
					PrevRandSeed:     []byte("roothash"),
					MiniBlockHeaders: []block.MiniBlockHeader{{Hash: hash1, SenderShardID: 1}},
				}, nil
			}
			if bytes.Equal(hdrHash2Bytes, key) {
				return &block.Header{Nonce: 2, Round: 2}, nil
			}
			return nil, errors.New("err")
		}
		cs.LenCalled = func() int {
			return 0
		}
		cs.NoncesCalled = func(shardId uint32) []uint64 {
			return []uint64{1, 2}
		}
		cs.MaxSizeCalled = func() int {
			return 1000
		}
		return cs
	}

	txCoordinator := &mock.TransactionCoordinatorMock{
		CreateMbsAndProcessCrossShardTransactionsDstMeCalled: func(header data.HeaderHandler, processedMiniBlocksHashes map[string]struct{}, haveTime func() bool) (slices block.MiniBlockSlice, u uint32, b bool, err error) {
			return block.MiniBlockSlice{miniBlock1}, 0, true, nil
		},
		CreateMbsAndProcessTransactionsFromMeCalled: func(haveTime func() bool) block.MiniBlockSlice {
			return block.MiniBlockSlice{miniBlock2}
		},
	}

	hdr := &block.MetaBlock{
		Nonce:         1,
		PubKeysBitmap: []byte("0100101"),
		PrevHash:      []byte(""),
		Signature:     []byte("signature"),
		Round:         2,
		RootHash:      []byte("roothash"),
		ShardInfo: []block.ShardData{
			{
				HeaderHash:            hdrHash1Bytes,
				ShardID:               0,
				ShardMiniBlockHeaders: []block.MiniBlockHeader{{Hash: hash1, TxCount: 1}},
				TxCount:               1,
			},
			{
				ShardID:               0,
				ShardMiniBlockHeaders: []block.MiniBlockHeader{{Hash: hash2, TxCount: 1}},
				TxCount:               1,
			},
		},
		MiniBlockHeaders: []block.MiniBlockHeader{{Hash: hash1, SenderShardID: 0, TxCount: 1}},
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = dPool
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.TxCoordinator = txCoordinator

	mp, _ := blproc.NewMetaProcessor(arguments)
	round := uint64(10)

	metaHdr := &block.MetaBlock{Round: round}
	_, err := mp.CreateBlockBody(metaHdr, func() bool { return true })
	assert.Nil(t, err)

	err = mp.VerifyCrossShardMiniBlockDstMe(hdr)
	assert.Nil(t, err)
}

func TestMetaProcessor_CreateBlockCreateHeaderProcessBlock(t *testing.T) {
	t.Parallel()

	hash := []byte("hash1")
	hdrHash1Bytes := []byte("hdr_hash1")
	hrdHash2Bytes := []byte("hdr_hash2")
	hasher := &mock.HasherStub{}
	hasher.ComputeCalled = func(s string) []byte {
		return hash
	}
	miniBlock1 := &block.MiniBlock{TxHashes: [][]byte{hash}}
	dPool := initDataPool([]byte("tx_hash"))
	dPool.TransactionsCalled = func() dataRetriever.ShardedDataCacherNotifier {
		return testscommon.NewShardedDataStub()
	}
	dPool.HeadersCalled = func() dataRetriever.HeadersPool {
		cs := &mock.HeadersCacherStub{}
		cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
		}
		cs.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			if bytes.Equal(hdrHash1Bytes, key) {
				return &block.MetaBlock{
					PrevHash:         []byte("hash1"),
					Nonce:            1,
					Round:            1,
					PrevRandSeed:     []byte("roothash"),
					MiniBlockHeaders: []block.MiniBlockHeader{{Hash: []byte("hash1"), SenderShardID: 1}},
				}, nil
			}
			if bytes.Equal(hrdHash2Bytes, key) {
				return &block.Header{Nonce: 2, Round: 2}, nil
			}
			return nil, errors.New("err")
		}
		cs.LenCalled = func() int {
			return 0
		}
		cs.NoncesCalled = func(shardId uint32) []uint64 {
			return []uint64{1, 2}
		}
		cs.MaxSizeCalled = func() int {
			return 1000
		}
		return cs
	}

	txCoordinator := &mock.TransactionCoordinatorMock{
		CreateMbsAndProcessCrossShardTransactionsDstMeCalled: func(header data.HeaderHandler, processedMiniBlocksHashes map[string]struct{}, haveTime func() bool) (slices block.MiniBlockSlice, u uint32, b bool, err error) {
			return block.MiniBlockSlice{miniBlock1}, 0, true, nil
		},
	}

	blkc := &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.MetaBlock{Nonce: 0, AccumulatedFeesInEpoch: big.NewInt(0), DevFeesInEpoch: big.NewInt(0)}
		},
		GetCurrentBlockHeaderHashCalled: func() []byte {
			return hash
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}
	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	coreComponents.Hash = hasher
	dataComponents.DataPool = dPool
	dataComponents.BlockChain = blkc
	bootstrapComponents.VersionedHdrFactory = &testscommon.VersionedHeaderFactoryStub{
		CreateCalled: func(epoch uint32) data.HeaderHandler {
			return &block.MetaBlock{
				Epoch: 0,
			}
		},
	}
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.TxCoordinator = txCoordinator

	mp, _ := blproc.NewMetaProcessor(arguments)
	round := uint64(10)
	nonce := uint64(5)

	metaHdr := &block.MetaBlock{Round: round}
	bodyHandler, err := mp.CreateBlockBody(metaHdr, func() bool { return true })
	require.Nil(t, err)

	headerHandler, err := mp.CreateNewHeader(round, nonce)
	require.Nil(t, err)
	require.NotNil(t, headerHandler)

	err = headerHandler.SetRound(uint64(1))
	require.Nil(t, err)

	err = headerHandler.SetNonce(1)
	require.Nil(t, err)

	err = headerHandler.SetPrevHash(hash)
	require.Nil(t, err)

	err = headerHandler.SetAccumulatedFees(big.NewInt(0))
	require.Nil(t, err)

	err = mp.ProcessBlock(headerHandler, bodyHandler, func() time.Duration { return time.Second })
	require.Nil(t, err)
}

func TestMetaProcessor_RequestShardHeadersIfNeededShouldAddHeaderIntoTrackerPool(t *testing.T) {
	t.Parallel()

	var addedNonces []uint64
	poolsHolderStub := initDataPool([]byte(""))
	poolsHolderStub.HeadersCalled = func() dataRetriever.HeadersPool {
		return &mock.HeadersCacherStub{
			GetHeaderByNonceAndShardIdCalled: func(hdrNonce uint64, shardId uint32) ([]data.HeaderHandler, [][]byte, error) {
				addedNonces = append(addedNonces, hdrNonce)
				return []data.HeaderHandler{&block.Header{Nonce: 1}}, [][]byte{[]byte("hash")}, nil
			},
		}
	}

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	dataComponents.DataPool = poolsHolderStub
	roundHandlerMock := &mock.RoundHandlerMock{}
	coreComponents.RoundField = roundHandlerMock
	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	mp, _ := blproc.NewMetaProcessor(arguments)

	roundHandlerMock.RoundIndex = 20
	header := &block.Header{
		Round: 9,
		Nonce: 5,
	}

	hdrsAddedForShard := make(map[uint32]uint32)
	lastShardHdr := make(map[uint32]data.HeaderHandler)
	lastShardHdr[header.ShardID] = header

	mp.RequestShardHeadersIfNeeded(hdrsAddedForShard, lastShardHdr)

	expectedAddedNonces := []uint64{6, 7}
	assert.Equal(t, expectedAddedNonces, addedNonces)
}

func TestMetaProcessor_CreateAndProcessBlockCallsProcessAfterFirstEpoch(t *testing.T) {
	t.Parallel()

	coreComponents, dataComponents, bootstrapComponents, statusComponents := createMockComponentHolders()
	hash := []byte("hash1")
	hdrHash1Bytes := []byte("hdr_hash1")
	hrdHash2Bytes := []byte("hdr_hash2")
	hasher := &mock.HasherStub{}
	hasher.ComputeCalled = func(s string) []byte {
		return hash
	}
	coreComponents.TxSignHasherField = hasher

	miniBlock1 := &block.MiniBlock{TxHashes: [][]byte{hash}}
	dPool := initDataPool([]byte("tx_hash"))
	dPool.TransactionsCalled = func() dataRetriever.ShardedDataCacherNotifier {
		return testscommon.NewShardedDataStub()
	}
	dPool.HeadersCalled = func() dataRetriever.HeadersPool {
		cs := &mock.HeadersCacherStub{}
		cs.RegisterHandlerCalled = func(i func(header data.HeaderHandler, key []byte)) {
		}
		cs.GetHeaderByHashCalled = func(key []byte) (handler data.HeaderHandler, e error) {
			if bytes.Equal(hdrHash1Bytes, key) {
				return &block.Header{
					PrevHash:         []byte("hash1"),
					Nonce:            1,
					Round:            1,
					PrevRandSeed:     []byte("roothash"),
					MiniBlockHeaders: []block.MiniBlockHeader{{Hash: []byte("hash1"), SenderShardID: 1}},
				}, nil
			}
			if bytes.Equal(hrdHash2Bytes, key) {
				return &block.Header{Nonce: 2, Round: 2}, nil
			}
			return nil, errors.New("err")
		}
		cs.LenCalled = func() int {
			return 0
		}
		cs.NoncesCalled = func(shardId uint32) []uint64 {
			return []uint64{1, 2}
		}
		cs.MaxSizeCalled = func() int {
			return 1000
		}
		return cs
	}

	txCoordinator := &mock.TransactionCoordinatorMock{
		CreateMbsAndProcessCrossShardTransactionsDstMeCalled: func(header data.HeaderHandler, processedMiniBlocksHashes map[string]struct{}, haveTime func() bool) (slices block.MiniBlockSlice, u uint32, b bool, err error) {
			return block.MiniBlockSlice{miniBlock1}, 0, true, nil
		},
	}

	blkc := &mock.BlockChainMock{
		GetCurrentBlockHeaderCalled: func() data.HeaderHandler {
			return &block.MetaBlock{
				Nonce:                  0,
				AccumulatedFeesInEpoch: big.NewInt(0),
				DevFeesInEpoch:         big.NewInt(0),
				EpochStart: block.EpochStart{
					LastFinalizedHeaders: []block.EpochStartShardData{{}},
					Economics:            block.Economics{},
				},
			}
		},
		GetCurrentBlockHeaderHashCalled: func() []byte {
			return hash
		},
		GetGenesisHeaderCalled: func() data.HeaderHandler {
			return &block.Header{Nonce: 0}
		},
	}

	arguments := createMockMetaArguments(coreComponents, dataComponents, bootstrapComponents, statusComponents)
	arguments.TxCoordinator = txCoordinator
	dataComponents.DataPool = dPool
	dataComponents.BlockChain = blkc
	calledSaveNodesCoordinator := false
	arguments.ValidatorStatisticsProcessor = &mock.ValidatorStatisticsProcessorStub{
		SaveNodesCoordinatorUpdatesCalled: func(epoch uint32) (bool, error) {
			calledSaveNodesCoordinator = true
			return true, nil
		},
	}

	toggleCalled := false
	arguments.EpochSystemSCProcessor = &mock.EpochStartSystemSCStub{
		ToggleUnStakeUnBondCalled: func(value bool) error {
			toggleCalled = true
			assert.Equal(t, value, true)
			return nil
		},
	}

	mp, _ := blproc.NewMetaProcessor(arguments)
	metaHdr := &block.MetaBlock{}
	headerHandler, bodyHandler, err := mp.CreateBlock(metaHdr, func() bool { return true })
	assert.Nil(t, err)
	assert.True(t, toggleCalled, calledSaveNodesCoordinator)

	err = headerHandler.SetRound(uint64(1))
	assert.Nil(t, err)

	err = headerHandler.SetNonce(1)
	assert.Nil(t, err)

	err = headerHandler.SetPrevHash(hash)
	assert.Nil(t, err)

	err = headerHandler.SetAccumulatedFees(big.NewInt(0))
	assert.Nil(t, err)

	metaHeaderHandler, _ := headerHandler.(data.MetaHeaderHandler)
	err = metaHeaderHandler.SetAccumulatedFeesInEpoch(big.NewInt(0))
	assert.Nil(t, err)

	toggleCalled = false
	calledSaveNodesCoordinator = false
	err = mp.ProcessBlock(headerHandler, bodyHandler, func() time.Duration { return time.Second })
	assert.Nil(t, err)
	assert.True(t, toggleCalled, calledSaveNodesCoordinator)
}

func TestMetaProcessor_CreateNewHeaderErrWrongTypeAssertion(t *testing.T) {
	t.Parallel()

	cc, dc, _, sc := createMockComponentHolders()

	boostrapComponents := &mock.BootstrapComponentsMock{
		Coordinator:          mock.NewOneShardCoordinatorMock(),
		HdrIntegrityVerifier: &mock.HeaderIntegrityVerifierStub{},
		VersionedHdrFactory: &testscommon.VersionedHeaderFactoryStub{
			CreateCalled: func(epoch uint32) data.HeaderHandler {
				return &block.Header{}
			},
		},
	}

	arguments := createMockMetaArguments(cc, dc, boostrapComponents, sc)

	mp, err := blproc.NewMetaProcessor(arguments)
	assert.Nil(t, err)

	h, err := mp.CreateNewHeader(1, 1)

	assert.Equal(t, process.ErrWrongTypeAssertion, err)
	assert.Nil(t, h)
}

func TestMetaProcessor_CreateNewHeaderValsOK(t *testing.T) {
	t.Parallel()

	rootHash := []byte("root")
	round := uint64(7)
	nonce := uint64(8)
	epoch := uint32(5)

	coreComponents, dataComponents, _, statusComponents := createMockComponentHolders()

	boostrapComponents := &mock.BootstrapComponentsMock{
		Coordinator:          mock.NewOneShardCoordinatorMock(),
		HdrIntegrityVerifier: &mock.HeaderIntegrityVerifierStub{},
		VersionedHdrFactory: &testscommon.VersionedHeaderFactoryStub{
			CreateCalled: func(epoch uint32) data.HeaderHandler {
				return &block.Header{}
			},
		},
	}

	boostrapComponents.VersionedHdrFactory = &testscommon.VersionedHeaderFactoryStub{
		CreateCalled: func(epoch uint32) data.HeaderHandler {
			return &block.MetaBlock{
				Epoch: epoch,
			}
		},
	}

	arguments := createMockMetaArguments(coreComponents, dataComponents, boostrapComponents, statusComponents)
	arguments.AccountsDB[state.UserAccountsState] = &testscommon.AccountsStub{
		RootHashCalled: func() ([]byte, error) {
			return rootHash, nil
		},
	}
	arguments.EpochStartTrigger = &mock.EpochStartTriggerStub{
		EpochCalled: func() uint32 {
			return epoch
		},
	}

	mp, err := blproc.NewMetaProcessor(arguments)
	assert.Nil(t, err)

	h, err := mp.CreateNewHeader(round, nonce)
	assert.Nil(t, err)
	assert.IsType(t, &block.MetaBlock{}, h)
	assert.Equal(t, epoch, h.GetEpoch())
	assert.Equal(t, round, h.GetRound())
	assert.Equal(t, nonce, h.GetNonce())

	zeroInt := big.NewInt(0)
	metaHeader := h.(*block.MetaBlock)
	assert.Equal(t, zeroInt, metaHeader.AccumulatedFeesInEpoch)
	assert.Equal(t, zeroInt, metaHeader.AccumulatedFees)
	assert.Equal(t, zeroInt, metaHeader.DeveloperFees)
	assert.Equal(t, zeroInt, metaHeader.DevFeesInEpoch)
}
