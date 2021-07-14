package interceptors

import (
	"bytes"
	"sync"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/batch"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/process"
)

type baseDataInterceptor struct {
	throttler            process.InterceptorThrottler
	antifloodHandler     process.P2PAntifloodHandler
	topic                string
	currentPeerId        core.PeerID
	processor            process.InterceptorProcessor
	mutDebugHandler      sync.RWMutex
	debugHandler         process.InterceptedDebugger
	preferredPeersHolder process.PreferredPeersHolderHandler
	marshalizer          marshal.Marshalizer
	factory              process.InterceptedDataFactory
}

func (bdi *baseDataInterceptor) checkMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	if message == nil {
		return process.ErrNilMessage
	}
	if message.Data() == nil {
		return process.ErrNilDataToProcess
	}

	if !bdi.shouldSkipAntifloodChecks(fromConnectedPeer, message) {
		err := bdi.antifloodHandler.CanProcessMessage(message, fromConnectedPeer)
		if err != nil {
			return err
		}
		err = bdi.antifloodHandler.CanProcessMessagesOnTopic(fromConnectedPeer, bdi.topic, 1, uint64(len(message.Data())), message.SeqNo())
		if err != nil {
			return err
		}

		if !bdi.throttler.CanProcess() {
			return process.ErrSystemBusy
		}
	}

	bdi.throttler.StartProcessing()
	return nil
}

func (bdi *baseDataInterceptor) shouldSkipAntifloodChecks(fromConnectedPeer core.PeerID, message p2p.MessageP2P) bool {
	if bdi.isMessageFromSelfToSelf(fromConnectedPeer, message) {
		return true
	}

	return bdi.preferredPeersHolder.Contains(fromConnectedPeer)
}

func (bdi *baseDataInterceptor) isMessageFromSelfToSelf(fromConnectedPeer core.PeerID, message p2p.MessageP2P) bool {
	return bytes.Equal(message.Signature(), message.From()) &&
		bytes.Equal(message.From(), bdi.currentPeerId.Bytes()) &&
		fromConnectedPeer == bdi.currentPeerId
}

func (bdi *baseDataInterceptor) processInterceptedData(data process.InterceptedData, msg p2p.MessageP2P) {
	err := bdi.processor.Validate(data, msg.Peer())
	if err != nil {
		log.Trace("intercepted data is not valid",
			"hash", data.Hash(),
			"type", data.Type(),
			"pid", p2p.MessageOriginatorPid(msg),
			"seq no", p2p.MessageOriginatorSeq(msg),
			"data", data.String(),
			"error", err.Error(),
		)
		bdi.processDebugInterceptedData(data, err)

		return
	}

	err = bdi.processor.Save(data, msg.Peer(), bdi.topic)
	if err != nil {
		log.Trace("intercepted data can not be processed",
			"hash", data.Hash(),
			"type", data.Type(),
			"pid", p2p.MessageOriginatorPid(msg),
			"seq no", p2p.MessageOriginatorSeq(msg),
			"data", data.String(),
			"error", err.Error(),
		)
		bdi.processDebugInterceptedData(data, err)

		return
	}

	log.Trace("intercepted data is processed",
		"hash", data.Hash(),
		"type", data.Type(),
		"pid", p2p.MessageOriginatorPid(msg),
		"seq no", p2p.MessageOriginatorSeq(msg),
		"data", data.String(),
	)
	bdi.processDebugInterceptedData(data, err)
}

func (bdi *baseDataInterceptor) processDebugInterceptedData(interceptedData process.InterceptedData, err error) {
	identifiers := interceptedData.Identifiers()
	bdi.debugHandler.LogProcessedHashes(bdi.topic, identifiers, err)
}

func (bdi *baseDataInterceptor) receivedDebugInterceptedData(interceptedData process.InterceptedData) {
	identifiers := interceptedData.Identifiers()
	bdi.debugHandler.LogReceivedHashes(bdi.topic, identifiers)
}

// SetInterceptedDebugHandler will set a new intercepted debug handler
func (bdi *baseDataInterceptor) SetInterceptedDebugHandler(handler process.InterceptedDebugger) error {
	if check.IfNil(handler) {
		return process.ErrNilDebugger
	}

	bdi.mutDebugHandler.Lock()
	bdi.debugHandler = handler
	bdi.mutDebugHandler.Unlock()

	return nil
}

func (bdi *baseDataInterceptor) unmarshalReceivedMessage(
	message p2p.MessageP2P,
	fromConnectedPeer core.PeerID,
) ([][]byte, *batch.Batch, error) {

	b := &batch.Batch{}
	err := bdi.marshalizer.Unmarshal(b, message.Data())
	if err != nil {
		bdi.throttler.EndProcessing()

		//this situation is so severe that we need to black list de peers
		bdi.blackListPeers("unmarshalable data", err, message.Peer(), fromConnectedPeer)

		return nil, nil, err
	}

	multiDataBuff := b.Data
	lenMultiData := len(multiDataBuff)
	if lenMultiData == 0 {
		bdi.throttler.EndProcessing()
		return nil, nil, process.ErrNoDataInMessage
	}

	return multiDataBuff, b, nil
}

func (bdi *baseDataInterceptor) preProcessMessage(
	message p2p.MessageP2P,
	fromConnectedPeer core.PeerID,
) ([][]byte, *batch.Batch, error) {

	err := bdi.checkMessage(message, fromConnectedPeer)
	if err != nil {
		return nil, nil, err
	}

	multiDataBuff, b, err := bdi.unmarshalReceivedMessage(message, fromConnectedPeer)
	if err != nil {
		return nil, nil, err
	}

	err = bdi.antifloodHandler.CanProcessMessagesOnTopic(
		fromConnectedPeer,
		bdi.topic,
		uint32(len(multiDataBuff)),
		uint64(len(message.Data())),
		message.SeqNo(),
	)
	if err != nil {
		bdi.throttler.EndProcessing()
		return nil, nil, err
	}

	return multiDataBuff, b, nil
}

func (bdi *baseDataInterceptor) interceptedData(dataBuff []byte, originator core.PeerID, fromConnectedPeer core.PeerID) (process.InterceptedData, error) {
	interceptedData, err := bdi.factory.Create(dataBuff)
	if err != nil {
		//this situation is so severe that we need to black list de peers
		bdi.blackListPeers("can not create object from received bytes", err, originator, fromConnectedPeer)

		return nil, err
	}

	bdi.receivedDebugInterceptedData(interceptedData)

	err = interceptedData.CheckValidity()
	if err != nil {
		bdi.processDebugInterceptedData(interceptedData, err)

		isWrongVersion := err == process.ErrInvalidTransactionVersion || err == process.ErrInvalidChainID
		if isWrongVersion {
			//this situation is so severe that we need to black list de peers
			bdi.blackListPeers("wrong version of received intercepted data", err, originator, fromConnectedPeer)
		}

		return nil, err
	}

	return interceptedData, nil
}

func (bdi *baseDataInterceptor) blackListPeers(cause string, err error, peers ...core.PeerID) {
	reason := cause + ", topic " + bdi.topic
	if err != nil {
		reason += ", error " + err.Error()
	}

	for _, p := range peers {
		bdi.antifloodHandler.BlacklistPeer(p, reason, core.InvalidMessageBlacklistDuration)
	}
}

// RegisterHandler registers a callback function to be notified on received data
func (bdi *baseDataInterceptor) RegisterHandler(handler func(topic string, hash []byte, data interface{})) {
	bdi.processor.RegisterHandler(handler)
}
