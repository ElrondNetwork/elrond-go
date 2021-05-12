package resolvers

import (
	"fmt"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/batch"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/storage"
)

// ArgPeerHeartbeatResolver is the argument structure used to create a new peer heartbeat resolver instance
type ArgPeerHeartbeatResolver struct {
	SenderResolver     dataRetriever.TopicResolverSender
	PeerHeartbeatsPool storage.Cacher
	Marshalizer        marshal.Marshalizer
	AntifloodHandler   dataRetriever.P2PAntifloodHandler
	Throttler          dataRetriever.ResolverThrottler
	DataPacker         dataRetriever.DataPacker
}

// peerHeartbeatResolver is a wrapper over Resolver that is specialized in resolving peer heartbeat requests
type peerheartbeatResolver struct {
	*baseResolver
	messageProcessor
	peerHeartbeatsPool storage.Cacher
	dataPacker         dataRetriever.DataPacker
}

// NewPeerHeartbeatResolver creates a peer heartbeat resolver
func NewPeerHeartbeatResolver(arg ArgPeerHeartbeatResolver) (*peerheartbeatResolver, error) {
	if check.IfNil(arg.SenderResolver) {
		return nil, dataRetriever.ErrNilResolverSender
	}
	if check.IfNil(arg.PeerHeartbeatsPool) {
		return nil, dataRetriever.ErrNilPeerHeartbeatPool
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

	phbResolver := &peerheartbeatResolver{
		baseResolver: &baseResolver{
			TopicResolverSender: arg.SenderResolver,
		},
		peerHeartbeatsPool: arg.PeerHeartbeatsPool,
		dataPacker:         arg.DataPacker,
		messageProcessor: messageProcessor{
			marshalizer:      arg.Marshalizer,
			antifloodHandler: arg.AntifloodHandler,
			topic:            arg.SenderResolver.RequestTopic(),
			throttler:        arg.Throttler,
		},
	}

	return phbResolver, nil
}

// ProcessReceivedMessage will be the callback func from the p2p.Messenger and will be called each time a new message was received
// (for the topic this validator was registered to, usually a request topic)
func (res *peerheartbeatResolver) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	err := res.canProcessMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	res.throttler.StartProcessing()
	defer res.throttler.EndProcessing()

	rd, err := res.parseReceivedMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	switch rd.Type {
	case dataRetriever.PubkeyArrayType:
		err = res.resolveRequests(rd.Value, message.Peer())
	default:
		err = dataRetriever.ErrRequestTypeNotImplemented
	}

	if err != nil {
		err = fmt.Errorf("%w for value %s", err, logger.DisplayByteSlice(rd.Value))
	}

	return err
}

func (res *peerheartbeatResolver) fetchPhbAsByteSlice(pk []byte) ([]byte, error) {
	value, ok := res.peerHeartbeatsPool.Peek(pk)
	if ok {
		return res.marshalizer.Marshal(value)
	}

	return nil, dataRetriever.ErrNotFound
}

func (res *peerheartbeatResolver) resolveRequests(mbBuff []byte, pid core.PeerID) error {
	b := batch.Batch{}
	err := res.marshalizer.Unmarshal(&b, mbBuff)
	if err != nil {
		return err
	}
	pubKeys := b.Data

	var errFetch error
	errorsFound := 0
	dataSlice := make([][]byte, 0, len(pubKeys))
	for _, pk := range pubKeys {
		phb, errTemp := res.fetchPhbAsByteSlice(pk)
		if errTemp != nil {
			errFetch = fmt.Errorf("%w for public key %s", errTemp, logger.DisplayByteSlice(pk))
			log.Trace("fetchPhbAsByteSlice missing",
				"error", errFetch.Error(),
				"pk", pk)
			errorsFound++

			continue
		}
		dataSlice = append(dataSlice, phb)
	}

	buffsToSend, errPack := res.dataPacker.PackDataInChunks(dataSlice, maxBuffToSendBulkMiniblocks)
	if errPack != nil {
		return errPack
	}

	for _, buff := range buffsToSend {
		errSend := res.Send(buff, pid)
		if errSend != nil {
			return errSend
		}
	}

	if errFetch != nil {
		errFetch = fmt.Errorf("resolveRequests last error %w from %d encountered errors", errFetch, errorsFound)
	}

	return errFetch
}

// RequestDataFromPublicKeyArray requests a set of peer heartbeat data from other peers having input the array
// of needed public keys
func (res *peerheartbeatResolver) RequestDataFromPublicKeyArray(publicKeys [][]byte, _ uint32) error {
	b := &batch.Batch{
		Data: publicKeys,
	}
	batchBytes, err := res.marshalizer.Marshal(b)

	if err != nil {
		return err
	}

	return res.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.PubkeyArrayType,
			Value: batchBytes,
		},
		publicKeys,
	)
}

// IsInterfaceNil returns true if there is no value under the interface
func (res *peerheartbeatResolver) IsInterfaceNil() bool {
	return res == nil
}
