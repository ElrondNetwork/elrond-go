package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/dataRetriever"
	"github.com/ElrondNetwork/elrond-go-sandbox/storage"
)

type PoolsHolderStub struct {
	HeadersCalled           func() storage.Cacher
	HeadersNoncesCalled     func() dataRetriever.Uint64Cacher
	PeerChangesBlocksCalled func() storage.Cacher
	TransactionsCalled      func() dataRetriever.ShardedDataCacherNotifier
	MiniBlocksCalled        func() storage.Cacher
	MetaBlocksCalled        func() storage.Cacher
	HeaderStatisticsCalled  func() storage.Cacher
}

func (phs *PoolsHolderStub) Headers() storage.Cacher {
	return phs.HeadersCalled()
}

func (phs *PoolsHolderStub) HeadersNonces() dataRetriever.Uint64Cacher {
	return phs.HeadersNoncesCalled()
}

func (phs *PoolsHolderStub) PeerChangesBlocks() storage.Cacher {
	return phs.PeerChangesBlocksCalled()
}

func (phs *PoolsHolderStub) Transactions() dataRetriever.ShardedDataCacherNotifier {
	return phs.TransactionsCalled()
}

func (phs *PoolsHolderStub) MiniBlocks() storage.Cacher {
	return phs.MiniBlocksCalled()
}

func (phs *PoolsHolderStub) MetaBlocks() storage.Cacher {
	return phs.MetaBlocksCalled()
}

func (phs *PoolsHolderStub) HeaderStatistics() storage.Cacher {
	return phs.HeaderStatisticsCalled()
}
