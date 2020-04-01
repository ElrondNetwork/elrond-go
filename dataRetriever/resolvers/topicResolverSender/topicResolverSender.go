package topicResolverSender

import (
	"fmt"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/message"
)

// topicRequestSuffix represents the topic name suffix
const topicRequestSuffix = "_REQUEST"

const minPeersToQuery = 2

// ArgTopicResolverSender is the argument structure used to create new TopicResolverSender instance
type ArgTopicResolverSender struct {
	Messenger           dataRetriever.MessageHandler
	TopicName           string
	PeerListCreator     dataRetriever.PeerListCreator
	Marshalizer         marshal.Marshalizer
	Randomizer          dataRetriever.IntRandomizer
	TargetShardId       uint32
	OutputAntiflooder   dataRetriever.P2PAntifloodHandler
	RequestDebugHandler dataRetriever.RequestDebugHandler
	NumIntraShardPeers  int
	NumCrossShardPeers  int
}

type topicResolverSender struct {
	messenger           dataRetriever.MessageHandler
	marshalizer         marshal.Marshalizer
	topicName           string
	peerListCreator     dataRetriever.PeerListCreator
	randomizer          dataRetriever.IntRandomizer
	targetShardId       uint32
	outputAntiflooder   dataRetriever.P2PAntifloodHandler
	requestDebugHandler dataRetriever.RequestDebugHandler
	numIntraShardPeers  int
	numCrossShardPeers  int
}

// NewTopicResolverSender returns a new topic resolver instance
func NewTopicResolverSender(arg ArgTopicResolverSender) (*topicResolverSender, error) {
	if check.IfNil(arg.Messenger) {
		return nil, dataRetriever.ErrNilMessenger
	}
	if check.IfNil(arg.Marshalizer) {
		return nil, dataRetriever.ErrNilMarshalizer
	}
	if check.IfNil(arg.Randomizer) {
		return nil, dataRetriever.ErrNilRandomizer
	}
	if check.IfNil(arg.PeerListCreator) {
		return nil, dataRetriever.ErrNilPeerListCreator
	}
	if check.IfNil(arg.OutputAntiflooder) {
		return nil, dataRetriever.ErrNilAntifloodHandler
	}
	if check.IfNil(arg.RequestDebugHandler) {
		return nil, dataRetriever.ErrNilRequestDebugHandler
	}
	if arg.NumIntraShardPeers < 0 {
		return nil, fmt.Errorf("%w for NumIntraShardPeers as the value should be greater or equal than 0",
			dataRetriever.ErrInvalidValue)
	}
	if arg.NumCrossShardPeers < 0 {
		return nil, fmt.Errorf("%w for NumCrossShardPeers as the value should be greater or equal than 0",
			dataRetriever.ErrInvalidValue)
	}
	if arg.NumCrossShardPeers+arg.NumIntraShardPeers < minPeersToQuery {
		return nil, fmt.Errorf("%w for NumCrossShardPeers, NumIntraShardPeers as their sum should be greater or equal than %d",
			dataRetriever.ErrInvalidValue, minPeersToQuery)
	}

	resolver := &topicResolverSender{
		messenger:           arg.Messenger,
		topicName:           arg.TopicName,
		peerListCreator:     arg.PeerListCreator,
		marshalizer:         arg.Marshalizer,
		randomizer:          arg.Randomizer,
		targetShardId:       arg.TargetShardId,
		outputAntiflooder:   arg.OutputAntiflooder,
		numIntraShardPeers:  arg.NumIntraShardPeers,
		numCrossShardPeers:  arg.NumCrossShardPeers,
		requestDebugHandler: arg.RequestDebugHandler,
	}

	return resolver, nil
}

// SendOnRequestTopic is used to send request data over channels (topics) to other peers
// This method only sends the request, the received data should be handled by interceptors
func (trs *topicResolverSender) SendOnRequestTopic(rd *dataRetriever.RequestData, originalHashes [][]byte) error {
	buff, err := trs.marshalizer.Marshal(rd)
	if err != nil {
		return err
	}

	topicToSendRequest := trs.topicName + topicRequestSuffix

	crossPeers := trs.peerListCreator.PeerList()
	numSentCross := trs.sendOnTopic(crossPeers, topicToSendRequest, buff, trs.numCrossShardPeers)

	intraPeers := trs.peerListCreator.IntraShardPeerList()
	numSentIntra := trs.sendOnTopic(intraPeers, topicToSendRequest, buff, trs.numIntraShardPeers)

	trs.callDebugHandler(originalHashes, numSentIntra, numSentCross)

	if numSentCross+numSentIntra == 0 {
		return fmt.Errorf("%w, topic: %s", dataRetriever.ErrSendRequest, trs.topicName)
	}

	return nil
}

func (trs *topicResolverSender) callDebugHandler(originalHashes [][]byte, numSentIntra int, numSentCross int) {
	if !trs.requestDebugHandler.Enabled() {
		//this check prevents a useless range when using a mock implementation
		return
	}

	for _, hash := range originalHashes {
		trs.requestDebugHandler.RequestedData(trs.topicName, hash, numSentIntra, numSentCross)
	}
}

func createIndexList(listLength int) []int {
	indexes := make([]int, listLength)
	for i := 0; i < listLength; i++ {
		indexes[i] = i
	}

	return indexes
}

func (trs *topicResolverSender) sendOnTopic(peerList []p2p.PeerID, topicToSendRequest string, buff []byte, maxToSend int) int {
	if len(peerList) == 0 || maxToSend == 0 {
		return 0
	}

	indexes := createIndexList(len(peerList))
	shuffledIndexes := fisherYatesShuffle(indexes, trs.randomizer)

	msgSentCounter := 0
	for idx := range shuffledIndexes {
		peer := peerList[idx]

		err := trs.sendToConnectedPeer(topicToSendRequest, buff, peer)
		if err != nil {
			continue
		}

		msgSentCounter++
		if msgSentCounter == maxToSend {
			break
		}
	}

	return msgSentCounter
}

// Send is used to send an array buffer to a connected peer
// It is used when replying to a request
func (trs *topicResolverSender) Send(buff []byte, peer p2p.PeerID) error {
	return trs.sendToConnectedPeer(trs.topicName, buff, peer)
}

func (trs *topicResolverSender) sendToConnectedPeer(topic string, buff []byte, peer p2p.PeerID) error {
	msg := &message.Message{
		DataField:   buff,
		PeerField:   peer,
		TopicsField: []string{topic},
	}

	err := trs.outputAntiflooder.CanProcessMessage(msg, peer)
	if err != nil {
		return fmt.Errorf("%w while sending %d bytes to peer %s",
			err,
			len(buff),
			p2p.PeerIdToShortString(peer),
		)
	}

	return trs.messenger.SendToConnectedPeer(topic, buff, peer)
}

// RequestTopic returns the topic with the request suffix used for sending requests
func (trs *topicResolverSender) RequestTopic() string {
	return trs.topicName + topicRequestSuffix
}

// TargetShardID returns the target shard ID for this resolver should serve data
func (trs *topicResolverSender) TargetShardID() uint32 {
	return trs.targetShardId
}

func fisherYatesShuffle(indexes []int, randomizer dataRetriever.IntRandomizer) []int {
	newIndexes := make([]int, len(indexes))
	copy(newIndexes, indexes)

	for i := len(newIndexes) - 1; i > 0; i-- {
		j := randomizer.Intn(i + 1)
		newIndexes[i], newIndexes[j] = newIndexes[j], newIndexes[i]
	}

	return newIndexes
}

// IsInterfaceNil returns true if there is no value under the interface
func (trs *topicResolverSender) IsInterfaceNil() bool {
	return trs == nil
}
