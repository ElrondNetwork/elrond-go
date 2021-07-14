package interceptors

import (
	"sync"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/debug/resolver"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/interceptors/disabled"
)

var log = logger.GetOrCreate("process/interceptors")

// ArgMultiDataInterceptor is the argument for the multi-data interceptor
type ArgMultiDataInterceptor struct {
	ArgSingleDataInterceptor
	Marshalizer marshal.Marshalizer
}

// MultiDataInterceptor is used for intercepting packed multi data
type MultiDataInterceptor struct {
	*baseDataInterceptor
	whiteListRequest   process.WhiteListHandler
	mutChunksProcessor sync.RWMutex
	chunksProcessor    process.InterceptedChunksProcessor
}

// NewMultiDataInterceptor hooks a new interceptor for packed multi data
func NewMultiDataInterceptor(arg ArgMultiDataInterceptor) (*MultiDataInterceptor, error) {
	err := checkArguments(arg.ArgSingleDataInterceptor)
	if err != nil {
		return nil, err
	}
	if check.IfNil(arg.Processor) {
		return nil, process.ErrNilInterceptedDataProcessor
	}
	if check.IfNil(arg.Marshalizer) {
		return nil, process.ErrNilMarshalizer
	}

	interceptor := &MultiDataInterceptor{
		baseDataInterceptor: &baseDataInterceptor{
			throttler:            arg.Throttler,
			antifloodHandler:     arg.AntifloodHandler,
			topic:                arg.Topic,
			currentPeerId:        arg.CurrentPeerId,
			processor:            arg.Processor,
			preferredPeersHolder: arg.PreferredPeersHolder,
			debugHandler:         resolver.NewDisabledInterceptorResolver(),
			marshalizer:          arg.Marshalizer,
			factory:              arg.DataFactory,
		},
		whiteListRequest: arg.WhiteListRequest,
		chunksProcessor:  disabled.NewDisabledInterceptedChunksProcessor(),
	}

	return interceptor, nil
}

// ProcessReceivedMessage is the callback func from the p2p.Messenger and will be called each time a new message was received
// (for the topic this validator was registered to)
func (mdi *MultiDataInterceptor) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	multiDataBuff, b, err := mdi.preProcessMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	mdi.mutChunksProcessor.RLock()
	checkChunksRes, err := mdi.chunksProcessor.CheckBatch(b, mdi.whiteListRequest)
	mdi.mutChunksProcessor.RUnlock()
	if err != nil {
		mdi.throttler.EndProcessing()
		return err
	}

	isIncompleteChunk := checkChunksRes.IsChunk && !checkChunksRes.HaveAllChunks
	if isIncompleteChunk {
		mdi.throttler.EndProcessing()
		return nil
	}
	isCompleteChunk := checkChunksRes.IsChunk && checkChunksRes.HaveAllChunks
	if isCompleteChunk {
		multiDataBuff = [][]byte{checkChunksRes.CompleteBuffer}
	}

	listInterceptedData := make([]process.InterceptedData, len(multiDataBuff))
	errOriginator := mdi.antifloodHandler.IsOriginatorEligibleForTopic(message.Peer(), mdi.topic)

	for index, dataBuff := range multiDataBuff {
		var interceptedData process.InterceptedData
		interceptedData, err = mdi.interceptedData(dataBuff, message.Peer(), fromConnectedPeer)
		listInterceptedData[index] = interceptedData
		if err != nil {
			mdi.throttler.EndProcessing()
			return err
		}

		isWhiteListed := mdi.whiteListRequest.IsWhiteListed(interceptedData)
		if !isWhiteListed && errOriginator != nil {
			mdi.throttler.EndProcessing()
			log.Trace("got message from peer on topic only for validators", "originator",
				p2p.PeerIdToShortString(message.Peer()),
				"topic", mdi.topic,
				"err", errOriginator)
			return errOriginator
		}

		isForCurrentShard := interceptedData.IsForCurrentShard()
		shouldProcess := isForCurrentShard || isWhiteListed
		if !shouldProcess {
			log.Trace("intercepted data should not be processed",
				"pid", p2p.MessageOriginatorPid(message),
				"seq no", p2p.MessageOriginatorSeq(message),
				"topic", message.Topic(),
				"hash", interceptedData.Hash(),
				"is for this shard", isForCurrentShard,
				"is white listed", isWhiteListed,
			)
			mdi.throttler.EndProcessing()
			return process.ErrInterceptedDataNotForCurrentShard
		}
	}

	go func() {
		for _, interceptedData := range listInterceptedData {
			mdi.processInterceptedData(interceptedData, message)
		}
		mdi.throttler.EndProcessing()
	}()

	return nil
}

// SetChunkProcessor sets the intercepted chunks processor
func (mdi *MultiDataInterceptor) SetChunkProcessor(processor process.InterceptedChunksProcessor) error {
	if check.IfNil(processor) {
		return process.ErrNilChunksProcessor
	}

	mdi.mutChunksProcessor.Lock()
	mdi.chunksProcessor = processor
	mdi.mutChunksProcessor.Unlock()

	return nil
}

// Close will call the chunk processor's close method
func (mdi *MultiDataInterceptor) Close() error {
	mdi.mutChunksProcessor.RLock()
	defer mdi.mutChunksProcessor.RUnlock()

	return mdi.chunksProcessor.Close()
}

// IsInterfaceNil returns true if there is no value under the interface
func (mdi *MultiDataInterceptor) IsInterfaceNil() bool {
	return mdi == nil
}
