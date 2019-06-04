package indexer_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/core"

	"github.com/ElrondNetwork/elrond-go-sandbox/core/indexer"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/logger"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/mock"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/gin-gonic/gin/json"
	"github.com/stretchr/testify/assert"
)

var (
	url              = "http://localhost:9300"
	port             = ":9300"
	shardCoordinator = mock.ShardCoordinatorMock{}
	marshalizer      = &mock.MarshalizerMock{}
	hasher           = mock.HasherMock{}
	log              = logger.DefaultLogger()
	username         = "username"
	password         = "password"
)

func newTestBlockHeader() *block.Header {
	return &block.Header{
		Nonce:            10,
		PrevHash:         []byte("prev hash"),
		PrevRandSeed:     []byte("prev rand seed"),
		RandSeed:         []byte("rand seed"),
		PubKeysBitmap:    []byte("pub keys bitmap"),
		ShardId:          5,
		TimeStamp:        1024,
		Round:            6,
		Epoch:            4,
		BlockBodyType:    block.TxBlock,
		Signature:        []byte("signature"),
		MiniBlockHeaders: nil,
		PeerChanges:      nil,
		RootHash:         []byte("root hash"),
		TxCount:          3,
	}
}

func newTestMetaBlock() *block.MetaBlock {
	shardData := block.ShardData{
		ShardId:               1,
		HeaderHash:            []byte{1},
		ShardMiniBlockHeaders: []block.ShardMiniBlockHeader{},
		TxCount:               100,
	}
	return &block.MetaBlock{
		Nonce:     1,
		Round:     2,
		TxCount:   100,
		ShardInfo: []block.ShardData{shardData},
	}
}

func newTestBlockBody() block.Body {
	return block.Body{
		{[][]byte{[]byte("tx1"), []byte("tx2")}, 2, 2},
		{[][]byte{[]byte("tx3")}, 4, 1},
	}
}

func newTestTxPool() map[string]*transaction.Transaction {
	txPool := make(map[string]*transaction.Transaction)

	txPool["tx1"] = &transaction.Transaction{
		Nonce:     uint64(1),
		Value:     big.NewInt(1),
		RcvAddr:   []byte("receiver_address1"),
		SndAddr:   []byte("sender_address1"),
		GasPrice:  uint64(10000),
		GasLimit:  uint64(1000),
		Data:      []byte("tx_data1"),
		Signature: []byte("signature1"),
		Challenge: []byte("challange1"),
	}

	txPool["tx2"] = &transaction.Transaction{
		Nonce:     uint64(2),
		Value:     big.NewInt(2),
		RcvAddr:   []byte("receiver_address2"),
		SndAddr:   []byte("sender_address2"),
		GasPrice:  uint64(10000),
		GasLimit:  uint64(1000),
		Data:      []byte("tx_data2"),
		Signature: []byte("signature2"),
		Challenge: []byte("challange2"),
	}

	txPool["tx3"] = &transaction.Transaction{
		Nonce:     uint64(3),
		Value:     big.NewInt(3),
		RcvAddr:   []byte("receiver_address3"),
		SndAddr:   []byte("sender_address3"),
		GasPrice:  uint64(10000),
		GasLimit:  uint64(1000),
		Data:      []byte("tx_data3"),
		Signature: []byte("signature3"),
		Challenge: []byte("challange3"),
	}

	return txPool
}

func TestElasticIndexer_NewIndexerWithNilUrlShouldError(t *testing.T) {
	ei, err := indexer.NewElasticIndexer("", username, password, shardCoordinator, marshalizer, hasher, log)

	assert.Nil(t, ei)
	assert.Equal(t, core.ErrNilUrl, err)
}

func TestElasticIndexer_NewIndexerWithNilShardCoordinatorShouldError(t *testing.T) {
	ei, err := indexer.NewElasticIndexer("a", username, password, nil, marshalizer, hasher, log)

	assert.Nil(t, ei)
	assert.Equal(t, core.ErrNilCoordinator, err)
}

func TestElasticIndexer_NewIndexerWithNilMarsharlizerShouldError(t *testing.T) {
	ei, err := indexer.NewElasticIndexer("a", username, password, shardCoordinator, nil, hasher, log)

	assert.Nil(t, ei)
	assert.Equal(t, core.ErrNilMarshalizer, err)
}

func TestElasticIndexer_NewIndexerWithNilHasherShouldError(t *testing.T) {
	ei, err := indexer.NewElasticIndexer("a", username, password, shardCoordinator, marshalizer, nil, log)

	assert.Nil(t, ei)
	assert.Equal(t, core.ErrNilHasher, err)
}

func TestElasticIndexer_NewIndexerWithNilLoggerShouldError(t *testing.T) {
	ei, err := indexer.NewElasticIndexer("a", username, password, shardCoordinator, marshalizer, hasher, nil)

	assert.Nil(t, ei)
	assert.Equal(t, core.ErrNilLogger, err)
}

func TestElasticIndexer_NewIndexerWithCorrectParamsShouldWork(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocks" {
			w.WriteHeader(http.StatusOK)
		}
		if r.URL.Path == "/transactions" {
			w.WriteHeader(http.StatusOK)
		}
		if r.URL.Path == "/tps" {
			w.WriteHeader(http.StatusOK)
		}
	}))

	ei, err := indexer.NewElasticIndexer(ts.URL, username, password, shardCoordinator, marshalizer, hasher, log)

	assert.NotNil(t, ei)
	assert.Nil(t, err)
}

func TestElasticIndexer_CheckAndCreateIndexShouldWorkIfIndexExists(t *testing.T) {
	blocksFunctionCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocks" {
			w.WriteHeader(http.StatusOK)
			blocksFunctionCount++
		}
	}))

	ei := indexer.NewTestElasticIndexer(ts.URL, username, password, shardCoordinator, marshalizer, hasher, log)

	err := ei.CheckAndCreateIndex("blocks", nil)

	assert.Nil(t, err)
	assert.Equal(t, 1, blocksFunctionCount)
}

func TestElasticIndexer_CheckAndCreateIndexShouldCreateIndexIfItDoesNotExist(t *testing.T) {
	blocksFunctionCount := 0
	putFunctionCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			w.WriteHeader(http.StatusOK)
			putFunctionCount++
			return
		}
		if r.URL.Path == "/blocks" {
			w.WriteHeader(http.StatusNotFound)
			blocksFunctionCount++
			return
		}
	}))

	ei := indexer.NewTestElasticIndexer(ts.URL, username, password, shardCoordinator, marshalizer, hasher, log)

	err := ei.CheckAndCreateIndex("blocks", nil)

	assert.Nil(t, err)
	assert.Equal(t, 1, blocksFunctionCount)
	assert.Equal(t, 1, putFunctionCount)
}

func TestElasticIndexer_CreateIndexShouldCreateIndexIfItDoesNotExist(t *testing.T) {
	blocksFunctionCount := 0
	putFunctionCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			w.WriteHeader(http.StatusOK)
			putFunctionCount++
			return
		}
		if r.URL.Path == "/blocks" {
			w.WriteHeader(http.StatusNotFound)
			blocksFunctionCount++
			return
		}
	}))

	ei := indexer.NewTestElasticIndexer(ts.URL, username, password, shardCoordinator, marshalizer, hasher, log)

	err := ei.CreateIndex("blocks", nil)

	assert.Nil(t, err)
	assert.Equal(t, 0, blocksFunctionCount)
	assert.Equal(t, 1, putFunctionCount)
}

func TestNewElasticIndexerIncorrectUrl(t *testing.T) {
	url := string([]byte{1, 2, 3})

	ind, err := indexer.NewElasticIndexer(url, username, password, shardCoordinator, marshalizer, hasher, log)
	assert.Nil(t, ind)
	assert.NotNil(t, err)
}

func TestElasticIndexer_getSerializedElasticBlockAndHeaderHash(t *testing.T) {
	ei := indexer.NewTestElasticIndexer(url, username, password, shardCoordinator, marshalizer, hasher, log)
	header := newTestBlockHeader()

	serializedBlock, headerHash := ei.GetSerializedElasticBlockAndHeaderHash(header)

	h, _ := marshalizer.Marshal(header)
	expectedHeaderHash := hasher.Compute(string(h))
	assert.Equal(t, expectedHeaderHash, headerHash)

	elasticBlock := indexer.Block{
		Nonce:         header.Nonce,
		ShardID:       header.ShardId,
		Hash:          hex.EncodeToString(headerHash),
		Proposer:      hex.EncodeToString([]byte("mock proposer")),
		PubKeyBitmap:  hex.EncodeToString(header.PubKeysBitmap),
		Size:          int64(len(h)),
		Timestamp:     time.Duration(header.TimeStamp),
		TxCount:       header.TxCount,
		StateRootHash: hex.EncodeToString(header.RootHash),
		PrevHash:      hex.EncodeToString(header.PrevHash),
	}
	expectedSerializedBlock, _ := json.Marshal(elasticBlock)
	assert.Equal(t, expectedSerializedBlock, serializedBlock)
}

func TestElasticIndexer_buildTransactionBulks(t *testing.T) {
	ei := indexer.NewTestElasticIndexer(url, username, password, shardCoordinator, marshalizer, hasher, log)

	header := newTestBlockHeader()
	body := newTestBlockBody()
	txPool := newTestTxPool()

	bulks := ei.BuildTransactionBulks(body, header, txPool)

	for _, bulk := range bulks {
		assert.NotNil(t, bulk)
	}
}

func TestElasticIndexer_serializeBulkTx(t *testing.T) {
	ei := indexer.NewTestElasticIndexer(url, username, password, shardCoordinator, marshalizer, hasher, log)

	header := newTestBlockHeader()
	body := newTestBlockBody()
	txPool := newTestTxPool()

	bulks := ei.BuildTransactionBulks(body, header, txPool)

	serializedTx := ei.SerializeBulkTx(bulks[0])

	var buff bytes.Buffer
	for _, tx := range bulks[0] {
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s", "_type" : "%s" } }%s`, tx.Hash, "_doc", "\n"))
		serializedTx, _ := json.Marshal(tx)
		serializedTx = append(serializedTx, "\n"...)
		buff.Grow(len(meta) + len(serializedTx))
		buff.Write(meta)
		buff.Write(serializedTx)
	}

	assert.Equal(t, buff, serializedTx)
}

func TestElasticIndexer_UpdateTPS(t *testing.T) {
	var output bytes.Buffer
	log.SetOutput(&output)
	ei := indexer.NewTestElasticIndexer(url, username, password, shardCoordinator, marshalizer, hasher, log)

	tpsBench := mock.TpsBenchmarkMock{}
	tpsBench.Update(newTestMetaBlock())

	ei.UpdateTPS(&tpsBench)
	assert.Empty(t, output.String())
}

func TestElasticIndexer_UpdateTPSNil(t *testing.T) {
	var output bytes.Buffer
	log.SetOutput(&output)
	ei := indexer.NewTestElasticIndexer(url, username, password, shardCoordinator, marshalizer, hasher, log)

	ei.UpdateTPS(nil)
	assert.NotEmpty(t, output.String())
}

func TestElasticIndexer_SerializeShardInfo(t *testing.T) {
	ei := indexer.NewTestElasticIndexer(url, username, password, shardCoordinator, marshalizer, hasher, log)

	tpsBench := mock.TpsBenchmarkMock{}
	tpsBench.UpdateWithShardStats(newTestMetaBlock())

	for _, shardInfo := range tpsBench.ShardStatistics() {
		serializedInfo, meta := ei.SerializeShardInfo(shardInfo)
		assert.NotNil(t, serializedInfo)
		assert.NotNil(t, meta)
	}
}
