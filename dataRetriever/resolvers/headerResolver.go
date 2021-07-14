package resolvers

import (
	"github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/resolvers/epochproviders/disabled"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
)

var log = logger.GetOrCreate("dataretriever/resolvers")

var _ dataRetriever.HeaderResolver = (*HeaderResolver)(nil)

// ArgHeaderResolver is the argument structure used to create new HeaderResolver instance
type ArgHeaderResolver struct {
	SenderResolver       dataRetriever.TopicResolverSender
	Headers              dataRetriever.HeadersPool
	HdrStorage           storage.Storer
	HeadersNoncesStorage storage.Storer
	Marshalizer          marshal.Marshalizer
	NonceConverter       typeConverters.Uint64ByteSliceConverter
	ShardCoordinator     sharding.Coordinator
	AntifloodHandler     dataRetriever.P2PAntifloodHandler
	Throttler            dataRetriever.ResolverThrottler
	IsFullHistoryNode    bool
}

// HeaderResolver is a wrapper over Resolver that is specialized in resolving headers requests
type HeaderResolver struct {
	baseStorageResolver
	*baseResolver
	messageProcessor
	headers          dataRetriever.HeadersPool
	hdrNoncesStorage storage.Storer
	nonceConverter   typeConverters.Uint64ByteSliceConverter
	epochHandler     dataRetriever.EpochHandler
	shardCoordinator sharding.Coordinator
}

// NewHeaderResolver creates a new header resolver
func NewHeaderResolver(arg ArgHeaderResolver) (*HeaderResolver, error) {
	if check.IfNil(arg.SenderResolver) {
		return nil, dataRetriever.ErrNilResolverSender
	}
	if check.IfNil(arg.Headers) {
		return nil, dataRetriever.ErrNilHeadersDataPool
	}
	if check.IfNil(arg.HdrStorage) {
		return nil, dataRetriever.ErrNilHeadersStorage
	}
	if check.IfNil(arg.HeadersNoncesStorage) {
		return nil, dataRetriever.ErrNilHeadersNoncesStorage
	}
	if check.IfNil(arg.Marshalizer) {
		return nil, dataRetriever.ErrNilMarshalizer
	}
	if check.IfNil(arg.NonceConverter) {
		return nil, dataRetriever.ErrNilUint64ByteSliceConverter
	}
	if check.IfNil(arg.ShardCoordinator) {
		return nil, dataRetriever.ErrNilShardCoordinator
	}
	if check.IfNil(arg.AntifloodHandler) {
		return nil, dataRetriever.ErrNilAntifloodHandler
	}
	if check.IfNil(arg.Throttler) {
		return nil, dataRetriever.ErrNilThrottler
	}

	epochHandler := disabled.NewEpochHandler()
	hdrResolver := &HeaderResolver{
		baseResolver: &baseResolver{
			TopicResolverSender: arg.SenderResolver,
		},
		headers:             arg.Headers,
		baseStorageResolver: createBaseStorageResolver(arg.HdrStorage, arg.IsFullHistoryNode),
		hdrNoncesStorage:    arg.HeadersNoncesStorage,
		nonceConverter:      arg.NonceConverter,
		epochHandler:        epochHandler,
		shardCoordinator:    arg.ShardCoordinator,
		messageProcessor: messageProcessor{
			marshalizer:      arg.Marshalizer,
			antifloodHandler: arg.AntifloodHandler,
			topic:            arg.SenderResolver.RequestTopic(),
			throttler:        arg.Throttler,
		},
	}

	return hdrResolver, nil
}

// SetEpochHandler sets the epoch handler for this component
func (hdrRes *HeaderResolver) SetEpochHandler(epochHandler dataRetriever.EpochHandler) error {
	if check.IfNil(epochHandler) {
		return dataRetriever.ErrNilEpochHandler
	}

	hdrRes.epochHandler = epochHandler
	return nil
}

// ProcessReceivedMessage will be the callback func from the p2p.Messenger and will be called each time a new message was received
// (for the topic this validator was registered to, usually a request topic)
func (hdrRes *HeaderResolver) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	err := hdrRes.canProcessMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	hdrRes.throttler.StartProcessing()
	defer hdrRes.throttler.EndProcessing()

	rd, err := hdrRes.parseReceivedMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	var buff []byte

	switch rd.Type {
	case dataRetriever.HashType:
		buff, err = hdrRes.resolveHeaderFromHash(rd)
	case dataRetriever.NonceType:
		buff, err = hdrRes.resolveHeaderFromNonce(rd)
	case dataRetriever.EpochType:
		buff, err = hdrRes.resolveHeaderFromEpoch(rd.Value)
	default:
		return dataRetriever.ErrResolveTypeUnknown
	}
	if err != nil {
		hdrRes.ResolverDebugHandler().LogFailedToResolveData(
			hdrRes.topic,
			rd.Value,
			err,
		)
		return err
	}

	if buff == nil {
		hdrRes.ResolverDebugHandler().LogFailedToResolveData(
			hdrRes.topic,
			rd.Value,
			dataRetriever.ErrMissingData,
		)

		log.Trace("missing data",
			"data", rd)
		return nil
	}

	hdrRes.ResolverDebugHandler().LogSucceededToResolveData(hdrRes.topic, rd.Value)

	return hdrRes.Send(buff, message.Peer())
}

func (hdrRes *HeaderResolver) resolveHeaderFromNonce(rd *dataRetriever.RequestData) ([]byte, error) {
	// key is now an encoded nonce (uint64)
	nonce, err := hdrRes.nonceConverter.ToUint64(rd.Value)
	if err != nil {
		return nil, dataRetriever.ErrInvalidNonceByteSlice
	}

	epoch := rd.Epoch

	//header-nonces storer contains un-pruned data so it is safe to search like this
	hash, err := hdrRes.hdrNoncesStorage.SearchFirst(rd.Value)
	if err != nil {
		log.Trace("hdrNoncesStorage.Get from calculated epoch", "error", err.Error())
		// Search the nonce-key pair in data pool
		var hdrBytes []byte
		hdrBytes, err = hdrRes.searchInCache(nonce)
		if err != nil {
			return nil, err
		}

		return hdrBytes, nil
	}

	newRd := &dataRetriever.RequestData{
		Type:  rd.Type,
		Value: hash,
		Epoch: epoch,
	}

	return hdrRes.resolveHeaderFromHash(newRd)
}

func (hdrRes *HeaderResolver) searchInCache(nonce uint64) ([]byte, error) {
	headers, _, err := hdrRes.headers.GetHeadersByNonceAndShardId(nonce, hdrRes.TargetShardID())
	if err != nil {
		return nil, err
	}

	hdr := headers[len(headers)-1]
	buff, err := hdrRes.marshalizer.Marshal(hdr)
	if err != nil {
		return nil, err
	}

	return buff, nil
}

// resolveHeaderFromHash resolves a header using its key (header hash)
func (hdrRes *HeaderResolver) resolveHeaderFromHash(rd *dataRetriever.RequestData) ([]byte, error) {
	value, err := hdrRes.headers.GetHeaderByHash(rd.Value)
	if err != nil {
		return hdrRes.getFromStorage(rd.Value, rd.Epoch)
	}

	return hdrRes.marshalizer.Marshal(value)
}

// resolveHeaderFromEpoch resolves a header using its key based on epoch
func (hdrRes *HeaderResolver) resolveHeaderFromEpoch(key []byte) ([]byte, error) {
	actualKey := key

	isUnknownEpoch, err := core.IsUnknownEpochIdentifier(key)
	if err != nil {
		return nil, err
	}
	if isUnknownEpoch {
		actualKey = []byte(core.EpochStartIdentifier(hdrRes.epochHandler.MetaEpoch()))
	}

	return hdrRes.searchFirst(actualKey)
}

// RequestDataFromHash requests a header from other peers having input the hdr hash
func (hdrRes *HeaderResolver) RequestDataFromHash(hash []byte, epoch uint32) error {
	return hdrRes.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.HashType,
			Value: hash,
			Epoch: epoch,
		},
		[][]byte{hash},
	)
}

// RequestDataFromNonce requests a header from other peers having input the hdr nonce
func (hdrRes *HeaderResolver) RequestDataFromNonce(nonce uint64, epoch uint32) error {
	return hdrRes.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.NonceType,
			Value: hdrRes.nonceConverter.ToByteSlice(nonce),
			Epoch: epoch,
		},
		[][]byte{hdrRes.nonceConverter.ToByteSlice(nonce)},
	)
}

// RequestDataFromEpoch requests a header from other peers having input the epoch
func (hdrRes *HeaderResolver) RequestDataFromEpoch(identifier []byte) error {
	return hdrRes.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.EpochType,
			Value: identifier,
		},
		[][]byte{identifier},
	)
}

// IsInterfaceNil returns true if there is no value under the interface
func (hdrRes *HeaderResolver) IsInterfaceNil() bool {
	return hdrRes == nil
}
