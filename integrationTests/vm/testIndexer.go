package vm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	elasticIndexer "github.com/ElrondNetwork/elastic-indexer-go"
	indexerTypes "github.com/ElrondNetwork/elastic-indexer-go/data"
	elasticProcessor "github.com/ElrondNetwork/elastic-indexer-go/process"
	"github.com/ElrondNetwork/elastic-indexer-go/process/accounts"
	blockProc "github.com/ElrondNetwork/elastic-indexer-go/process/block"
	"github.com/ElrondNetwork/elastic-indexer-go/process/logsevents"
	"github.com/ElrondNetwork/elastic-indexer-go/process/miniblocks"
	"github.com/ElrondNetwork/elastic-indexer-go/process/statistics"
	"github.com/ElrondNetwork/elastic-indexer-go/process/transactions"
	"github.com/ElrondNetwork/elastic-indexer-go/process/validators"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/indexer"
	"github.com/ElrondNetwork/elrond-go/data/receipt"
	"github.com/ElrondNetwork/elrond-go/data/smartContractResult"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/integrationTests/mock"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/stretchr/testify/require"
)

type testIndexer struct {
	indexer          process.Indexer
	indexerData      map[string]*bytes.Buffer
	marshalizer      marshal.Marshalizer
	hasher           hashing.Hasher
	shardCoordinator sharding.Coordinator
	mutex            sync.RWMutex
	saveDoneChan     chan struct{}
	t                testing.TB
}

const timeoutSave = 100 * time.Second

// CreateTestIndexer -
func CreateTestIndexer(
	t testing.TB,
	coordinator sharding.Coordinator,
	economicsDataHandler process.EconomicsDataHandler,
	hasResults bool,
) *testIndexer {
	ti := &testIndexer{
		indexerData: map[string]*bytes.Buffer{},
		mutex:       sync.RWMutex{},
	}

	dispatcher, err := elasticIndexer.NewDataDispatcher(100)
	require.Nil(t, err)

	dispatcher.StartIndexData()

	txFeeCalculator, ok := economicsDataHandler.(process.TransactionFeeCalculator)
	require.True(t, ok)

	ep := ti.createElasticProcessor(coordinator, txFeeCalculator, hasResults)

	arguments := elasticIndexer.ArgDataIndexer{
		Marshalizer:        testMarshalizer,
		NodesCoordinator:   &mock.NodesCoordinatorMock{},
		EpochStartNotifier: &mock.EpochStartNotifierStub{},
		ShardCoordinator:   coordinator,
		ElasticProcessor:   ep,
		DataDispatcher:     dispatcher,
	}

	te, err := elasticIndexer.NewDataIndexer(arguments)
	require.Nil(t, err)

	ti.indexer = te
	ti.shardCoordinator = coordinator
	ti.marshalizer = testMarshalizer
	ti.hasher = testHasher
	ti.t = t
	ti.saveDoneChan = make(chan struct{})

	return ti
}

func (ti *testIndexer) createElasticProcessor(
	shardCoordinator sharding.Coordinator,
	transactionFeeCalculator process.TransactionFeeCalculator,
	hasResults bool,
) elasticIndexer.ElasticProcessor {
	databaseClient := ti.createDatabaseClient(hasResults)

	enabledIndexes := []string{"transactions", "scresults", "receipts"}
	enabledIndexesMap := make(map[string]struct{})
	for _, index := range enabledIndexes {
		enabledIndexesMap[index] = struct{}{}
	}

	transactionProc, _ := transactions.NewTransactionsProcessor(&transactions.ArgsTransactionProcessor{
		AddressPubkeyConverter: pubkeyConv,
		TxFeeCalculator:        transactionFeeCalculator,
		ShardCoordinator:       shardCoordinator,
		Hasher:                 testHasher,
		Marshalizer:            testMarshalizer,
		IsInImportMode:         false,
	})
	ap, _ := accounts.NewAccountsProcessor(18, testMarshalizer, pubkeyConv, &testscommon.AccountsStub{})
	bp, _ := blockProc.NewBlockProcessor(testHasher, testMarshalizer)
	mp, _ := miniblocks.NewMiniblocksProcessor(shardCoordinator.SelfId(), testHasher, testMarshalizer)
	sp := statistics.NewStatisticsProcessor()
	vp, _ := validators.NewValidatorsProcessor(pubkeyConv)
	lp, _ := logsevents.NewLogsAndEventsProcessor(shardCoordinator, pubkeyConv, testMarshalizer)

	esIndexerArgs := &elasticProcessor.ArgElasticProcessor{
		UseKibana:         false,
		SelfShardID:       shardCoordinator.SelfId(),
		IndexTemplates:    nil,
		IndexPolicies:     nil,
		EnabledIndexes:    enabledIndexesMap,
		TransactionsProc:  transactionProc,
		AccountsProc:      ap,
		BlockProc:         bp,
		MiniblocksProc:    mp,
		StatisticsProc:    sp,
		ValidatorsProc:    vp,
		LogsAndEventsProc: lp,
		DBClient:          databaseClient,
	}

	esProcessor, _ := elasticProcessor.NewElasticProcessor(esIndexerArgs)

	return esProcessor
}

// SaveTransaction -
func (ti *testIndexer) SaveTransaction(
	tx data.TransactionHandler,
	mbType block.Type,
	intermediateTxs []data.TransactionHandler,
) {
	txHash, _ := core.CalculateHash(ti.marshalizer, ti.hasher, tx)

	sndShardID := ti.shardCoordinator.ComputeId(tx.GetSndAddr())
	rcvShardID := ti.shardCoordinator.ComputeId(tx.GetRcvAddr())

	bigTxMb := &block.MiniBlock{
		Type:            mbType,
		TxHashes:        [][]byte{txHash},
		SenderShardID:   sndShardID,
		ReceiverShardID: rcvShardID,
	}

	blk := &block.Body{
		MiniBlocks: []*block.MiniBlock{
			bigTxMb,
		},
	}

	txsPool := &indexer.Pool{
		Txs:      make(map[string]data.TransactionHandler),
		Scrs:     make(map[string]data.TransactionHandler),
		Rewards:  nil,
		Invalid:  make(map[string]data.TransactionHandler),
		Receipts: make(map[string]data.TransactionHandler),
	}

	if mbType == block.InvalidBlock {
		txsPool.Invalid[string(txHash)] = tx
		bigTxMb.ReceiverShardID = sndShardID
	} else {
		txsPool.Txs[string(txHash)] = tx
	}

	for _, intTx := range intermediateTxs {
		sndShardID = ti.shardCoordinator.ComputeId(intTx.GetSndAddr())
		rcvShardID = ti.shardCoordinator.ComputeId(intTx.GetRcvAddr())

		intTxHash, _ := core.CalculateHash(ti.marshalizer, ti.hasher, intTx)

		var mb block.MiniBlock
		mb.SenderShardID = sndShardID

		switch intTx.(type) {
		case *receipt.Receipt:
			mb.Type = block.ReceiptBlock
			mb.ReceiverShardID = sndShardID
			txsPool.Receipts[string(intTxHash)] = intTx
		case *smartContractResult.SmartContractResult:
			mb.Type = block.SmartContractResultBlock
			mb.ReceiverShardID = rcvShardID
			txsPool.Scrs[string(intTxHash)] = intTx
		default:
			continue
		}

		mb.TxHashes = [][]byte{intTxHash}
		blk.MiniBlocks = append(blk.MiniBlocks, &mb)

	}

	header := &block.Header{
		ShardID: ti.shardCoordinator.SelfId(),
	}

	args := &indexer.ArgsSaveBlockData{
		Body:             blk,
		Header:           header,
		TransactionsPool: txsPool,
	}
	ti.indexer.SaveBlock(args)

	select {
	case <-ti.saveDoneChan:
		return
	case <-time.After(timeoutSave):
		require.Fail(ti.t, "save indexer item timeout")
	}
}

func (ti *testIndexer) createDatabaseClient(hasResults bool) elasticProcessor.DatabaseClientHandler {
	done := true
	if hasResults {
		done = false
	}
	doBulkRequest := func(buff *bytes.Buffer, index string) error {
		ti.mutex.Lock()
		defer ti.mutex.Unlock()

		ti.indexerData[index] = buff
		if !done {
			done = true
			return nil
		}
		ti.saveDoneChan <- struct{}{}
		return nil
	}

	dbwm := &mock.DatabaseWriterStub{
		DoBulkRequestCalled: doBulkRequest,
	}

	return dbwm
}

// GetIndexerPreparedTransaction -
func (ti *testIndexer) GetIndexerPreparedTransaction(t *testing.T) *indexerTypes.Transaction {
	ti.mutex.RLock()
	txData, ok := ti.indexerData["transactions"]
	ti.mutex.RUnlock()

	require.True(t, ok)

	split := bytes.Split(txData.Bytes(), []byte("\n"))
	require.True(t, len(split) > 2)

	newTx := &indexerTypes.Transaction{}
	err := json.Unmarshal(split[1], newTx)
	require.Nil(t, err)

	if newTx.Receiver != "" {
		ti.printReceipt()
		ti.putSCRSInTx(newTx)
		return newTx
	}

	splitAgain := bytes.Split(split[1], []byte(`"upsert":`))
	require.True(t, len(split) > 1)

	ss := splitAgain[1][:len(splitAgain[1])-1]
	err = json.Unmarshal(ss, &newTx)
	require.Nil(t, err)

	return newTx
}

func (ti *testIndexer) printReceipt() {
	ti.mutex.RLock()
	receipts, ok := ti.indexerData["receipts"]
	ti.mutex.RUnlock()

	if !ok {
		return
	}

	split := bytes.Split(receipts.Bytes(), []byte("\n"))
	require.True(ti.t, len(split) > 2)

	newSCR := &indexerTypes.Receipt{}
	err := json.Unmarshal(split[1], newSCR)
	require.Nil(ti.t, err)

	fmt.Println(string(split[1]))
}

func (ti *testIndexer) putSCRSInTx(tx *indexerTypes.Transaction) {
	ti.mutex.RLock()
	scrData, ok := ti.indexerData["scresults"]
	ti.mutex.RUnlock()

	if !ok {
		return
	}

	split := bytes.Split(scrData.Bytes(), []byte("\n"))
	require.True(ti.t, len(split) > 2)

	for idx := 1; idx < len(split); idx += 2 {
		newSCR := &indexerTypes.ScResult{}
		err := json.Unmarshal(split[1], newSCR)
		require.Nil(ti.t, err)

		if newSCR.Receiver != "" {
			tx.SmartContractResults = append(tx.SmartContractResults, newSCR)
		}
	}

}
