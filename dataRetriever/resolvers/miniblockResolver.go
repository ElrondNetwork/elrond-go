package resolvers

import (
	"fmt"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/batch"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/requestHandlers"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/storage"
)

var _ dataRetriever.MiniBlocksResolver = (*miniblockResolver)(nil)
var _ requestHandlers.HashSliceResolver = (*miniblockResolver)(nil)

// ArgMiniblockResolver is the argument structure used to create a new miniblockResolver instance
type ArgMiniblockResolver struct {
	SenderResolver    dataRetriever.TopicResolverSender
	MiniBlockPool     storage.Cacher
	MiniBlockStorage  storage.Storer
	Marshalizer       marshal.Marshalizer
	AntifloodHandler  dataRetriever.P2PAntifloodHandler
	Throttler         dataRetriever.ResolverThrottler
	DataPacker        dataRetriever.DataPacker
	IsFullHistoryNode bool
}

// miniblockResolver is a wrapper over Resolver that is specialized in resolving miniblocks requests
// TODO extract common functionality between this and transactionResolver
type miniblockResolver struct {
	*baseResolver
	messageProcessor
	baseStorageResolver
	miniBlockPool storage.Cacher
	dataPacker    dataRetriever.DataPacker
}

// NewMiniblockResolver creates a miniblock resolver
func NewMiniblockResolver(arg ArgMiniblockResolver) (*miniblockResolver, error) {
	if check.IfNil(arg.SenderResolver) {
		return nil, dataRetriever.ErrNilResolverSender
	}
	if check.IfNil(arg.MiniBlockPool) {
		return nil, dataRetriever.ErrNilMiniblocksPool
	}
	if check.IfNil(arg.MiniBlockStorage) {
		return nil, dataRetriever.ErrNilMiniblocksStorage
	}
	if check.IfNil(arg.Marshalizer) {
		return nil, dataRetriever.ErrNilMarshalizer
	}
	if check.IfNil(arg.AntifloodHandler) {
		return nil, dataRetriever.ErrNilAntifloodHandler
	}
	if check.IfNil(arg.Throttler) {
		return nil, dataRetriever.ErrNilThrottler
	}
	if check.IfNil(arg.DataPacker) {
		return nil, dataRetriever.ErrNilDataPacker
	}

	mbResolver := &miniblockResolver{
		baseResolver: &baseResolver{
			TopicResolverSender: arg.SenderResolver,
		},
		miniBlockPool:       arg.MiniBlockPool,
		baseStorageResolver: createBaseStorageResolver(arg.MiniBlockStorage, arg.IsFullHistoryNode),
		dataPacker:          arg.DataPacker,
		messageProcessor: messageProcessor{
			marshalizer:      arg.Marshalizer,
			antifloodHandler: arg.AntifloodHandler,
			topic:            arg.SenderResolver.RequestTopic(),
			throttler:        arg.Throttler,
		},
	}

	return mbResolver, nil
}

// ProcessReceivedMessage will be the callback func from the p2p.Messenger and will be called each time a new message was received
// (for the topic this validator was registered to, usually a request topic)
func (mbRes *miniblockResolver) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	err := mbRes.canProcessMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	mbRes.throttler.StartProcessing()
	defer mbRes.throttler.EndProcessing()

	rd, err := mbRes.parseReceivedMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	switch rd.Type {
	case dataRetriever.HashType:
		err = mbRes.resolveMbRequestByHash(rd.Value, message.Peer(), rd.Epoch)
	case dataRetriever.HashArrayType:
		err = mbRes.resolveMbRequestByHashArray(rd.Value, message.Peer(), rd.Epoch)
	default:
		err = dataRetriever.ErrRequestTypeNotImplemented
	}

	if err != nil {
		err = fmt.Errorf("%w for hash %s", err, logger.DisplayByteSlice(rd.Value))
	}

	return err
}

func (mbRes *miniblockResolver) resolveMbRequestByHash(hash []byte, pid core.PeerID, epoch uint32) error {
	mb, err := mbRes.fetchMbAsByteSlice(hash, epoch)
	if err != nil {
		return err
	}

	b := &batch.Batch{
		Data: [][]byte{mb},
	}
	buffToSend, err := mbRes.marshalizer.Marshal(b)
	if err != nil {
		return err
	}

	return mbRes.Send(buffToSend, pid)
}

func (mbRes *miniblockResolver) fetchMbAsByteSlice(hash []byte, epoch uint32) ([]byte, error) {
	value, ok := mbRes.miniBlockPool.Peek(hash)
	if ok {
		return mbRes.marshalizer.Marshal(value)
	}

	buff, err := mbRes.getFromStorage(hash, epoch)
	if err != nil {
		mbRes.ResolverDebugHandler().LogFailedToResolveData(
			mbRes.topic,
			hash,
			err,
		)

		return nil, err
	}

	mbRes.ResolverDebugHandler().LogSucceededToResolveData(mbRes.topic, hash)

	return buff, nil
}

func (mbRes *miniblockResolver) resolveMbRequestByHashArray(mbBuff []byte, pid core.PeerID, epoch uint32) error {
	b := batch.Batch{}
	err := mbRes.marshalizer.Unmarshal(&b, mbBuff)
	if err != nil {
		return err
	}
	hashes := b.Data

	var errFetch error
	errorsFound := 0
	mbsBuffSlice := make([][]byte, 0, len(hashes))
	for _, hash := range hashes {
		mb, errTemp := mbRes.fetchMbAsByteSlice(hash, epoch)
		if errTemp != nil {
			errFetch = fmt.Errorf("%w for hash %s", errTemp, logger.DisplayByteSlice(hash))
			log.Trace("fetchMbAsByteSlice missing",
				"error", errFetch.Error(),
				"hash", hash)
			errorsFound++

			continue
		}
		mbsBuffSlice = append(mbsBuffSlice, mb)
	}

	buffsToSend, errPack := mbRes.dataPacker.PackDataInChunks(mbsBuffSlice, maxBuffToSendBulkMiniblocks)
	if errPack != nil {
		return errPack
	}

	for _, buff := range buffsToSend {
		errSend := mbRes.Send(buff, pid)
		if errSend != nil {
			return errSend
		}
	}

	if errFetch != nil {
		errFetch = fmt.Errorf("resolveMbRequestByHashArray last error %w from %d encountered errors", errFetch, errorsFound)
	}

	return errFetch
}

// RequestDataFromHash requests a block body from other peers having input the block body hash
func (mbRes *miniblockResolver) RequestDataFromHash(hash []byte, epoch uint32) error {
	return mbRes.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.HashType,
			Value: hash,
			Epoch: epoch,
		},
		[][]byte{hash},
	)
}

// RequestDataFromHashArray requests a block body from other peers having input the block body hash
func (mbRes *miniblockResolver) RequestDataFromHashArray(hashes [][]byte, epoch uint32) error {
	b := &batch.Batch{
		Data: hashes,
	}
	batchBytes, err := mbRes.marshalizer.Marshal(b)

	if err != nil {
		return err
	}

	return mbRes.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.HashArrayType,
			Value: batchBytes,
			Epoch: epoch,
		},
		hashes,
	)
}

// IsInterfaceNil returns true if there is no value under the interface
func (mbRes *miniblockResolver) IsInterfaceNil() bool {
	return mbRes == nil
}
