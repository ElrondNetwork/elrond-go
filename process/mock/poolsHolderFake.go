package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/data"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/dataPool"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/shardedData"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/typeConverters/uint64ByteSlice"
	"github.com/ElrondNetwork/elrond-go-sandbox/storage"
)

type PoolsHolderFake struct {
	transactions      data.ShardedDataCacherNotifier
	headers           storage.Cacher
	metaBlocks        storage.Cacher
	hdrNonces         data.Uint64Cacher
	miniBlocks        storage.Cacher
	peerChangesBlocks storage.Cacher
	headerStatistics  storage.Cacher
}

func NewPoolsHolderFake() *PoolsHolderFake {
	phf := &PoolsHolderFake{}
	phf.transactions, _ = shardedData.NewShardedData(storage.CacheConfig{Size: 10000, Type: storage.LRUCache})
	phf.headers, _ = storage.NewCache(storage.LRUCache, 10000)
	phf.metaBlocks, _ = storage.NewCache(storage.LRUCache, 10000)
	cacheHdrNonces, _ := storage.NewCache(storage.LRUCache, 10000)
	phf.hdrNonces, _ = dataPool.NewNonceToHashCacher(
		cacheHdrNonces,
		uint64ByteSlice.NewBigEndianConverter(),
	)
	phf.miniBlocks, _ = storage.NewCache(storage.LRUCache, 10000)
	phf.peerChangesBlocks, _ = storage.NewCache(storage.LRUCache, 10000)
	phf.headerStatistics, _ = storage.NewCache(storage.LRUCache, 10)
	return phf
}

func (phf *PoolsHolderFake) Transactions() data.ShardedDataCacherNotifier {
	return phf.transactions
}

func (phf *PoolsHolderFake) Headers() storage.Cacher {
	return phf.headers
}

func (phf *PoolsHolderFake) HeadersNonces() data.Uint64Cacher {
	return phf.hdrNonces
}

func (phf *PoolsHolderFake) MiniBlocks() storage.Cacher {
	return phf.miniBlocks
}

func (phf *PoolsHolderFake) PeerChangesBlocks() storage.Cacher {
	return phf.peerChangesBlocks
}

func (phf *PoolsHolderFake) MetaBlocks() storage.Cacher {
	return phf.metaBlocks
}

func (phf *PoolsHolderFake) HeaderStatistics() storage.Cacher {
	return phf.headerStatistics
}
