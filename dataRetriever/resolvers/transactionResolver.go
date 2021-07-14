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

var _ requestHandlers.HashSliceResolver = (*TxResolver)(nil)
var _ dataRetriever.Resolver = (*TxResolver)(nil)

// maxBuffToSendBulkTransactions represents max buffer size to send in bytes
const maxBuffToSendBulkTransactions = 1 << 18 //256KB

// maxBuffToSendBulkMiniblocks represents max buffer size to send in bytes
const maxBuffToSendBulkMiniblocks = 1 << 18 //256KB

// ArgTxResolver is the argument structure used to create new TxResolver instance
type ArgTxResolver struct {
	SenderResolver    dataRetriever.TopicResolverSender
	TxPool            dataRetriever.ShardedDataCacherNotifier
	TxStorage         storage.Storer
	Marshalizer       marshal.Marshalizer
	DataPacker        dataRetriever.DataPacker
	AntifloodHandler  dataRetriever.P2PAntifloodHandler
	Throttler         dataRetriever.ResolverThrottler
	IsFullHistoryNode bool
}

// TxResolver is a wrapper over Resolver that is specialized in resolving transaction requests
type TxResolver struct {
	*baseResolver
	messageProcessor
	baseStorageResolver
	txPool     dataRetriever.ShardedDataCacherNotifier
	dataPacker dataRetriever.DataPacker
}

// NewTxResolver creates a new transaction resolver
func NewTxResolver(arg ArgTxResolver) (*TxResolver, error) {
	if check.IfNil(arg.SenderResolver) {
		return nil, dataRetriever.ErrNilResolverSender
	}
	if check.IfNil(arg.TxPool) {
		return nil, dataRetriever.ErrNilTxDataPool
	}
	if check.IfNil(arg.TxStorage) {
		return nil, dataRetriever.ErrNilTxStorage
	}
	if check.IfNil(arg.Marshalizer) {
		return nil, dataRetriever.ErrNilMarshalizer
	}
	if check.IfNil(arg.DataPacker) {
		return nil, dataRetriever.ErrNilDataPacker
	}
	if check.IfNil(arg.AntifloodHandler) {
		return nil, dataRetriever.ErrNilAntifloodHandler
	}
	if check.IfNil(arg.Throttler) {
		return nil, dataRetriever.ErrNilThrottler
	}

	txResolver := &TxResolver{
		baseResolver: &baseResolver{
			TopicResolverSender: arg.SenderResolver,
		},
		txPool:              arg.TxPool,
		baseStorageResolver: createBaseStorageResolver(arg.TxStorage, arg.IsFullHistoryNode),
		dataPacker:          arg.DataPacker,
		messageProcessor: messageProcessor{
			marshalizer:      arg.Marshalizer,
			antifloodHandler: arg.AntifloodHandler,
			topic:            arg.SenderResolver.RequestTopic(),
			throttler:        arg.Throttler,
		},
	}

	return txResolver, nil
}

// ProcessReceivedMessage will be the callback func from the p2p.Messenger and will be called each time a new message was received
// (for the topic this validator was registered to, usually a request topic)
func (txRes *TxResolver) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	err := txRes.canProcessMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	txRes.throttler.StartProcessing()
	defer txRes.throttler.EndProcessing()

	rd, err := txRes.parseReceivedMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	switch rd.Type {
	case dataRetriever.HashType:
		err = txRes.resolveTxRequestByHash(rd.Value, message.Peer(), rd.Epoch)
	case dataRetriever.HashArrayType:
		err = txRes.resolveTxRequestByHashArray(rd.Value, message.Peer(), rd.Epoch)
	default:
		err = dataRetriever.ErrRequestTypeNotImplemented
	}

	if err != nil {
		err = fmt.Errorf("%w for hash %s", err, logger.DisplayByteSlice(rd.Value))
	}

	return err
}

func (txRes *TxResolver) resolveTxRequestByHash(hash []byte, pid core.PeerID, epoch uint32) error {
	//TODO this can be optimized by searching in corresponding datapool (taken by topic name)
	tx, err := txRes.fetchTxAsByteSlice(hash, epoch)
	if err != nil {
		return err
	}

	b := &batch.Batch{
		Data: [][]byte{tx},
	}
	buff, err := txRes.marshalizer.Marshal(b)
	if err != nil {
		return err
	}

	return txRes.Send(buff, pid)
}

func (txRes *TxResolver) fetchTxAsByteSlice(hash []byte, epoch uint32) ([]byte, error) {
	value, ok := txRes.txPool.SearchFirstData(hash)
	if ok {
		return txRes.marshalizer.Marshal(value)
	}

	buff, err := txRes.getFromStorage(hash, epoch)
	if err != nil {
		txRes.ResolverDebugHandler().LogFailedToResolveData(
			txRes.topic,
			hash,
			err,
		)

		return nil, err
	}

	txRes.ResolverDebugHandler().LogSucceededToResolveData(txRes.topic, hash)

	return buff, nil
}

func (txRes *TxResolver) resolveTxRequestByHashArray(hashesBuff []byte, pid core.PeerID, epoch uint32) error {
	//TODO this can be optimized by searching in corresponding datapool (taken by topic name)
	b := batch.Batch{}
	err := txRes.marshalizer.Unmarshal(&b, hashesBuff)
	if err != nil {
		return err
	}
	hashes := b.Data

	var errFetch error
	errorsFound := 0
	txsBuffSlice := make([][]byte, 0, len(hashes))
	for _, hash := range hashes {
		tx, errTemp := txRes.fetchTxAsByteSlice(hash, epoch)
		if errTemp != nil {
			errFetch = fmt.Errorf("%w for hash %s", errTemp, logger.DisplayByteSlice(hash))
			//it might happen to error on a tx (maybe it is missing) but should continue
			// as to send back as many as it can
			log.Trace("fetchTxAsByteSlice missing",
				"error", errFetch.Error(),
				"hash", hash)
			errorsFound++

			continue
		}
		txsBuffSlice = append(txsBuffSlice, tx)
	}

	buffsToSend, errPack := txRes.dataPacker.PackDataInChunks(txsBuffSlice, maxBuffToSendBulkTransactions)
	if errPack != nil {
		return errPack
	}

	for _, buff := range buffsToSend {
		errSend := txRes.Send(buff, pid)
		if errSend != nil {
			return errSend
		}
	}

	if errFetch != nil {
		errFetch = fmt.Errorf("resolveTxRequestByHashArray last error %w from %d encountered errors", errFetch, errorsFound)
	}

	return errFetch
}

// RequestDataFromHash requests a transaction from other peers having input the tx hash
func (txRes *TxResolver) RequestDataFromHash(hash []byte, epoch uint32) error {
	return txRes.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.HashType,
			Value: hash,
			Epoch: epoch,
		},
		[][]byte{hash},
	)
}

// RequestDataFromHashArray requests a list of tx hashes from other peers
func (txRes *TxResolver) RequestDataFromHashArray(hashes [][]byte, epoch uint32) error {
	b := &batch.Batch{
		Data: hashes,
	}
	buffHashes, err := txRes.marshalizer.Marshal(b)
	if err != nil {
		return err
	}

	return txRes.SendOnRequestTopic(
		&dataRetriever.RequestData{
			Type:  dataRetriever.HashArrayType,
			Value: buffHashes,
			Epoch: epoch,
		},
		hashes,
	)
}

// IsInterfaceNil returns true if there is no value under the interface
func (txRes *TxResolver) IsInterfaceNil() bool {
	return txRes == nil
}
