package interceptors

import (
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/debug/resolver"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/process"
)

// ArgSingleDataInterceptor is the argument for the single-data interceptor
type ArgSingleDataInterceptor struct {
	Topic                string
	DataFactory          process.InterceptedDataFactory
	Processor            process.InterceptorProcessor
	Throttler            process.InterceptorThrottler
	AntifloodHandler     process.P2PAntifloodHandler
	WhiteListRequest     process.WhiteListHandler
	PreferredPeersHolder process.PreferredPeersHolderHandler
	CurrentPeerId        core.PeerID
}

// SingleDataInterceptor is used for intercepting packed multi data
type SingleDataInterceptor struct {
	*baseDataInterceptor
	whiteListRequest process.WhiteListHandler
}

// NewSingleDataInterceptor hooks a new interceptor for single data
func NewSingleDataInterceptor(arg ArgSingleDataInterceptor) (*SingleDataInterceptor, error) {
	err := checkArguments(arg)
	if err != nil {
		return nil, err
	}
	if check.IfNil(arg.Processor) {
		return nil, process.ErrNilInterceptedDataProcessor
	}

	interceptor := &SingleDataInterceptor{
		baseDataInterceptor: &baseDataInterceptor{
			throttler:            arg.Throttler,
			antifloodHandler:     arg.AntifloodHandler,
			topic:                arg.Topic,
			currentPeerId:        arg.CurrentPeerId,
			processor:            arg.Processor,
			debugHandler:         resolver.NewDisabledInterceptorResolver(),
			preferredPeersHolder: arg.PreferredPeersHolder,
			factory:              arg.DataFactory,
		},
		whiteListRequest: arg.WhiteListRequest,
	}

	return interceptor, nil
}

func checkArguments(arg ArgSingleDataInterceptor) error {
	if len(arg.Topic) == 0 {
		return process.ErrEmptyTopic
	}
	if check.IfNil(arg.DataFactory) {
		return process.ErrNilInterceptedDataFactory
	}
	if check.IfNil(arg.Throttler) {
		return process.ErrNilInterceptorThrottler
	}
	if check.IfNil(arg.AntifloodHandler) {
		return process.ErrNilAntifloodHandler
	}
	if check.IfNil(arg.WhiteListRequest) {
		return process.ErrNilWhiteListHandler
	}
	if len(arg.CurrentPeerId) == 0 {
		return process.ErrEmptyPeerID
	}
	if check.IfNil(arg.PreferredPeersHolder) {
		return process.ErrNilPreferredPeersHolder
	}

	return nil
}

// ProcessReceivedMessage is the callback func from the p2p.Messenger and will be called each time a new message was received
// (for the topic this validator was registered to)
func (sdi *SingleDataInterceptor) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	sdi.mutDebugHandler.RLock()
	defer sdi.mutDebugHandler.RUnlock()

	err := sdi.checkMessage(message, fromConnectedPeer)
	if err != nil {
		return err
	}

	interceptedData, err := sdi.interceptedData(message.Data(), message.Peer(), fromConnectedPeer)
	if err != nil {
		sdi.throttler.EndProcessing()

		return err
	}

	errOriginator := sdi.antifloodHandler.IsOriginatorEligibleForTopic(message.Peer(), sdi.topic)
	isWhiteListed := sdi.whiteListRequest.IsWhiteListed(interceptedData)
	if !isWhiteListed && errOriginator != nil {
		log.Trace("got message from peer on topic only for validators",
			"originator", p2p.PeerIdToShortString(message.Peer()), "topic",
			sdi.topic, "err", errOriginator)
		sdi.throttler.EndProcessing()
		return errOriginator
	}

	isForCurrentShard := interceptedData.IsForCurrentShard()
	shouldProcess := isForCurrentShard || isWhiteListed
	if !shouldProcess {
		sdi.throttler.EndProcessing()
		log.Trace("intercepted data is for other shards",
			"pid", p2p.MessageOriginatorPid(message),
			"seq no", p2p.MessageOriginatorSeq(message),
			"topic", message.Topic(),
			"hash", interceptedData.Hash(),
			"is for current shard", isForCurrentShard,
			"is white listed", isWhiteListed,
		)

		return nil
	}

	go func() {
		sdi.processInterceptedData(interceptedData, message)
		sdi.throttler.EndProcessing()
	}()

	return nil
}

// Close returns nil
func (sdi *SingleDataInterceptor) Close() error {
	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (sdi *SingleDataInterceptor) IsInterfaceNil() bool {
	return sdi == nil
}
