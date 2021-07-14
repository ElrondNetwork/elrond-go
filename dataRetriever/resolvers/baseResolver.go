package resolvers

import "github.com/ElrondNetwork/elrond-go/dataRetriever"

type baseResolver struct {
	dataRetriever.TopicResolverSender
}

// SetNumPeersToQuery will set the number of intra shard and cross shard number of peer to query
func (res *baseResolver) SetNumPeersToQuery(intra int, cross int) {
	res.TopicResolverSender.SetNumPeersToQuery(intra, cross)
}

// NumPeersToQuery will return the number of intra shard and cross shard number of peer to query
func (res *baseResolver) NumPeersToQuery() (int, int) {
	return res.TopicResolverSender.NumPeersToQuery()
}

// SetResolverDebugHandler will set a resolver debug handler
func (res *baseResolver) SetResolverDebugHandler(handler dataRetriever.ResolverDebugHandler) error {
	return res.TopicResolverSender.SetResolverDebugHandler(handler)
}

// Close returns nil
func (res *baseResolver) Close() error {
	return nil
}
