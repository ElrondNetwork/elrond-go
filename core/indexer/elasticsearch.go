package indexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/core/logger"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/statistics"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/ElrondNetwork/elrond-go-sandbox/hashing"
	"github.com/ElrondNetwork/elrond-go-sandbox/marshal"
	"github.com/ElrondNetwork/elrond-go-sandbox/sharding"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/gin-gonic/gin/json"
)

const txBulkSize = 2500
const txIndex = "transactions"
const blockIndex = "blocks"
const tpsIndex = "tps"

const metachainTpsDocID = "meta"
const shardTpsDocIDPrefix = "shard"

type elasticIndexer struct {
	db               *elasticsearch.Client
	shardCoordinator sharding.Coordinator
	marshalizer      marshal.Marshalizer
	hasher           hashing.Hasher
	logger           *logger.Logger
}

// NewElasticIndexer SHOULD UPDATE COMMENT
func NewElasticIndexer(url string, shardCoordinator sharding.Coordinator, marshalizer marshal.Marshalizer,
	hasher hashing.Hasher, logger *logger.Logger) (Indexer, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{url},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	indexer := &elasticIndexer{es, shardCoordinator,
		marshalizer, hasher, logger}

	err = indexer.checkAndCreateIndex(blockIndex, nil)
	if err != nil {
		return nil, err
	}

	err = indexer.checkAndCreateIndex(txIndex, timestampMapping())
	if err != nil {
		return nil, err
	}

	err = indexer.checkAndCreateIndex(tpsIndex, nil)
	if err != nil {
		return nil, err
	}

	return indexer, nil
}

func (ei *elasticIndexer) checkAndCreateIndex(index string, body io.Reader) error {
	res, err := ei.db.Indices.Exists([]string{index})
	if err != nil {
		return err
	}
	// Indices.Exists actually does a HEAD request to the elastic index.
	// A status code of 200 actually means the index exists so we
	//  don't need to do nothing.
	if res.StatusCode == http.StatusOK {
		return nil
	}
	// A status code of 404 means the index does not exist so we create it
	if res.StatusCode == http.StatusNotFound {
		fmt.Println("Status code", res.String())
		err = ei.createIndex(index, body)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ei *elasticIndexer) createIndex(index string, body io.Reader) error {
	var err error
	var res *esapi.Response

	if body != nil {
		res, err = ei.db.Indices.Create(index,
			ei.db.Indices.Create.WithBody(body))
	} else {
		res, err = ei.db.Indices.Create(index)
	}

	if err != nil {
		return err
	}
	if res.IsError() {
		// Resource already exists
		if res.StatusCode == 400 {
			return nil
		}
		ei.logger.Warn(res.String())
		return ErrCannotCreateIndex
	}

	return nil
}

// SaveBlock will build
func (ei *elasticIndexer) SaveBlock(body block.Body, header *block.Header, txPool map[string]*transaction.Transaction) {
	go ei.saveHeader(header)

	if len(body) == 0 {
		fmt.Println("elasticsearch - no miniblocks")
		return
	}
	go ei.saveTransactions(body, header, txPool)
}

func (ei *elasticIndexer) getSerializedElasticBlockAndHeaderHash(header *block.Header) ([]byte, []byte) {
	h, err := ei.marshalizer.Marshal(header)
	if err != nil {
		ei.logger.Warn("could not marshal header")
		return nil, nil
	}

	headerHash := ei.hasher.Compute(string(h))
	elasticBlock := Block{
		Nonce:   header.Nonce,
		ShardID: header.ShardId,
		Hash:    hex.EncodeToString(headerHash),
		// TODO: We should add functionality for proposer and validators
		Proposer: hex.EncodeToString([]byte("mock proposer")),
		//Validators: "mock validators",
		PubKeyBitmap:  hex.EncodeToString(header.PubKeysBitmap),
		Size:          int64(len(h)),
		Timestamp:     time.Duration(header.TimeStamp),
		TxCount:       header.TxCount,
		StateRootHash: hex.EncodeToString(header.RootHash),
		PrevHash:      hex.EncodeToString(header.PrevHash),
	}
	serializedBlock, err := json.Marshal(elasticBlock)
	if err != nil {
		ei.logger.Warn("could not marshal elastic header")
		return nil, nil
	}

	return serializedBlock, headerHash
}

func (ei *elasticIndexer) saveHeader(header *block.Header) {
	var buff bytes.Buffer

	serializedBlock, headerHash := ei.getSerializedElasticBlockAndHeaderHash(header)

	buff.Grow(len(serializedBlock))
	buff.Write(serializedBlock)

	req := esapi.IndexRequest{
		Index:      blockIndex,
		DocumentID: hex.EncodeToString(headerHash),
		Body:       bytes.NewReader(buff.Bytes()),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), ei.db)
	if err != nil {
		ei.logger.Warn("Could not index block header: %s", err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		fmt.Println(res.String())
		ei.logger.Warn("error from elasticsearch indexing bulk of transactions")
	}
}

func (ei *elasticIndexer) serializeBulkTx(bulk []Transaction) bytes.Buffer {
	var buff bytes.Buffer
	for _, tx := range bulk {
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s", "_type" : "%s" } }%s`, tx.Hash, "_doc", "\n"))
		serializedTx, err := json.Marshal(tx)
		if err != nil {
			ei.logger.Warn("could not serialize transaction, will skip indexing: ", tx.Hash)
			continue
		}
		// append a newline foreach element
		serializedTx = append(serializedTx, "\n"...)

		buff.Grow(len(meta) + len(serializedTx))
		buff.Write(meta)
		buff.Write(serializedTx)
	}
	return buff
}

func (ei *elasticIndexer) saveTransactions(body block.Body, header *block.Header, txPool map[string]*transaction.Transaction) {
	bulks := ei.buildTransactionBulks(body, header, txPool)

	for _, bulk := range bulks {

		buff := ei.serializeBulkTx(bulk)

		res, err := ei.db.Bulk(bytes.NewReader(buff.Bytes()), ei.db.Bulk.WithIndex(txIndex))
		fmt.Println(res.String())

		if err != nil {
			ei.logger.Warn("error indexing bulk of transactions")
		}
		if res.IsError() {
			fmt.Println(res.String())
			ei.logger.Warn("error from elasticsearch indexing bulk of transactions")
		}
	}
}

// buildTransactionBulks creates bulks of maximum txBulkSize transactions to be indexed together
//  using the elasticsearch bulk API
func (ei *elasticIndexer) buildTransactionBulks(body block.Body, header *block.Header, txPool map[string]*transaction.Transaction) [][]Transaction {
	processedTxCount := 0
	bulks := make([][]Transaction, (header.GetTxCount()/txBulkSize)+1)
	blockMarshal, _ := ei.marshalizer.Marshal(header)
	blockHash := ei.hasher.Compute(string(blockMarshal))

	for _, mb := range body {
		mbMarshal, err := ei.marshalizer.Marshal(mb)
		if err != nil {
			ei.logger.Warn("could not marshal miniblock")
			continue
		}
		mbHash := ei.hasher.Compute(string(mbMarshal))

		mbTxStatus := "Pending"
		if ei.shardCoordinator.SelfId() == mb.ReceiverShardID {
			mbTxStatus = "Success"
		}

		for _, txHash := range mb.TxHashes {
			processedTxCount++

			currentBulk := processedTxCount / txBulkSize
			currentTx, ok := txPool[string(txHash)]
			if !ok {
				ei.logger.Warn("elasticsearch could not find tx hash in pool")
				continue
			}

			if ei.shardCoordinator.SelfId() == mb.SenderShardID {

			}
			bulks[currentBulk] = append(bulks[currentBulk], Transaction{
				Hash:          hex.EncodeToString(txHash),
				MBHash:        hex.EncodeToString(mbHash),
				BlockHash:     hex.EncodeToString(blockHash),
				Nonce:         currentTx.Nonce,
				Value:         currentTx.Value,
				Receiver:      hex.EncodeToString(currentTx.RcvAddr),
				Sender:        hex.EncodeToString(currentTx.SndAddr),
				ReceiverShard: mb.ReceiverShardID,
				SenderShard:   mb.SenderShardID,
				GasPrice:      currentTx.GasPrice,
				GasLimit:      currentTx.GasLimit,
				Data:          hex.EncodeToString(currentTx.Data),
				Signature:     hex.EncodeToString(currentTx.Signature),
				Timestamp:     time.Duration(header.TimeStamp),
				Status:        mbTxStatus,
			})
		}
	}
	return bulks
}

// SaveMetaBlock will build
func (ei *elasticIndexer) SaveMetaBlock(metaBlock *block.MetaBlock, headerPool map[string]*block.Header) {

}

func (ei *elasticIndexer) serializeShardInfo(shardInfo statistics.ShardStatistic) ([]byte, []byte) {
	meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s%d", "_type" : "%s" } }%s`,
		shardTpsDocIDPrefix, shardInfo.ShardID(), tpsIndex, "\n"))

	bigTxCount := big.NewInt(int64(shardInfo.AverageBlockTxCount()))
	shardTPS := TPS{
		ShardID:               shardInfo.ShardID(),
		LiveTPS:               shardInfo.LiveTPS(),
		PeakTPS:               shardInfo.PeakTPS(),
		AverageTPS:            shardInfo.AverageTPS(),
		AverageBlockTxCount:   bigTxCount,
		CurrentBlockNonce:     shardInfo.CurrentBlockNonce(),
		LastBlockTxCount:      shardInfo.LastBlockTxCount(),
		TotalProcessedTxCount: shardInfo.TotalProcessedTxCount(),
	}

	serializedInfo, err := json.Marshal(shardTPS)
	if err != nil {
		ei.logger.Warn("could not serialize tps info, will skip indexing tps this shard")
		return nil, nil
	}
	// append a newline foreach element in the bulk we create
	serializedInfo = append(serializedInfo, "\n"...)

	return serializedInfo, meta
}

// UpdateTPS updates the tps and statistics into elasticsearch index
func (ei *elasticIndexer) UpdateTPS(tpsBenchmark statistics.TPSBenchmark) {
	if tpsBenchmark == nil {
		ei.logger.Warn("update tps called, but the tpsBenchmark is nil")
		return
	}

	var buff bytes.Buffer

	meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s", "_type" : "%s" } }%s`, metachainTpsDocID, tpsIndex, "\n"))
	generalInfo := TPS{
		LiveTPS:    tpsBenchmark.LiveTPS(),
		PeakTPS:    tpsBenchmark.PeakTPS(),
		NrOfShards: tpsBenchmark.NrOfShards(),
		// TODO: This value is still mocked, it should be removed if we cannot populate it correctly
		NrOfNodes:             100,
		BlockNumber:           tpsBenchmark.BlockNumber(),
		RoundNumber:           tpsBenchmark.RoundNumber(),
		RoundTime:             tpsBenchmark.RoundTime(),
		AverageBlockTxCount:   tpsBenchmark.AverageBlockTxCount(),
		LastBlockTxCount:      tpsBenchmark.LastBlockTxCount(),
		TotalProcessedTxCount: tpsBenchmark.TotalProcessedTxCount(),
	}

	serializedInfo, err := json.Marshal(generalInfo)
	if err != nil {
		ei.logger.Warn("could not serialize tps info, will skip indexing tps this round")
		return
	}
	// append a newline foreach element in the bulk we create
	serializedInfo = append(serializedInfo, "\n"...)

	buff.Grow(len(meta) + len(serializedInfo))
	buff.Write(meta)
	buff.Write(serializedInfo)

	for _, shardInfo := range tpsBenchmark.ShardStatistics() {
		serializedInfo, meta := ei.serializeShardInfo(shardInfo)
		if serializedInfo == nil {
			continue
		}

		buff.Grow(len(meta) + len(serializedInfo))
		buff.Write(meta)
		buff.Write(serializedInfo)

		res, err := ei.db.Bulk(bytes.NewReader(buff.Bytes()), ei.db.Bulk.WithIndex(tpsIndex))
		if err != nil {
			ei.logger.Warn("error indexing tps information")
		}
		if res.IsError() {
			fmt.Println(res.String())
			ei.logger.Warn("error from elasticsearch indexing tps information")
		}
	}
}

func timestampMapping() io.Reader {
	return strings.NewReader(`{"mappings": {"properties": {"timestamp": {"type": "date"}}}}`)
}
