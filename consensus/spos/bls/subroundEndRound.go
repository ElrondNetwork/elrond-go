package bls

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/ElrondNetwork/elrond-go/consensus"
	"github.com/ElrondNetwork/elrond-go/consensus/spos"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/display"
)

type subroundEndRound struct {
	*spos.Subround
	processingThresholdPercentage int
	displayStatistics             func()
	appStatusHandler              core.AppStatusHandler
	mutProcessingEndRound         sync.Mutex
}

// NewSubroundEndRound creates a subroundEndRound object
func NewSubroundEndRound(
	baseSubround *spos.Subround,
	extend func(subroundId int),
	processingThresholdPercentage int,
	displayStatistics func(),
	appStatusHandler core.AppStatusHandler,
) (*subroundEndRound, error) {
	err := checkNewSubroundEndRoundParams(
		baseSubround,
	)
	if err != nil {
		return nil, err
	}

	srEndRound := subroundEndRound{
		baseSubround,
		processingThresholdPercentage,
		displayStatistics,
		appStatusHandler,
		sync.Mutex{},
	}
	srEndRound.Job = srEndRound.doEndRoundJob
	srEndRound.Check = srEndRound.doEndRoundConsensusCheck
	srEndRound.Extend = extend

	return &srEndRound, nil
}

func checkNewSubroundEndRoundParams(
	baseSubround *spos.Subround,
) error {
	if baseSubround == nil {
		return spos.ErrNilSubround
	}
	if baseSubround.ConsensusState == nil {
		return spos.ErrNilConsensusState
	}

	err := spos.ValidateConsensusCore(baseSubround.ConsensusCoreHandler)

	return err
}

// receivedBlockHeaderFinalInfo method is called when a block header final info is received
func (sr *subroundEndRound) receivedBlockHeaderFinalInfo(cnsDta *consensus.Message) bool {
	node := string(cnsDta.PubKey)

	if !sr.IsConsensusDataSet() {
		return false
	}

	if !sr.IsNodeLeaderInCurrentRound(node) { // is NOT this node leader in current round?
		sr.PeerHonestyHandler().ChangeScore(
			node,
			spos.GetConsensusTopicID(sr.ShardCoordinator()),
			spos.LeaderPeerHonestyDecreaseFactor,
		)

		return false
	}

	if sr.IsSelfLeaderInCurrentRound() {
		return false
	}

	if !sr.IsConsensusDataEqual(cnsDta.BlockHeaderHash) {
		return false
	}

	if !sr.CanProcessReceivedMessage(cnsDta, sr.RoundHandler().Index(), sr.Current()) {
		return false
	}

	if !sr.isBlockHeaderFinalInfoValid(cnsDta) {
		return false
	}

	log.Debug("step 3: block header final info has been received",
		"PubKeysBitmap", cnsDta.PubKeysBitmap,
		"AggregateSignature", cnsDta.AggregateSignature,
		"LeaderSignature", cnsDta.LeaderSignature)

	sr.PeerHonestyHandler().ChangeScore(
		node,
		spos.GetConsensusTopicID(sr.ShardCoordinator()),
		spos.LeaderPeerHonestyIncreaseFactor,
	)

	return sr.doEndRoundJobByParticipant(cnsDta)
}

func (sr *subroundEndRound) isBlockHeaderFinalInfoValid(cnsDta *consensus.Message) bool {
	if check.IfNil(sr.Header) {
		return false
	}

	header := sr.Header.ShallowClone()
	err := header.SetPubKeysBitmap(cnsDta.PubKeysBitmap)
	if err != nil {
		log.Debug("isBlockHeaderFinalInfoValid.SetPubKeysBitmap", "error", err.Error())
		return false
	}

	err = header.SetSignature(cnsDta.AggregateSignature)
	if err != nil {
		log.Debug("isBlockHeaderFinalInfoValid.SetSignature", "error", err.Error())
		return false
	}

	err = header.SetLeaderSignature(cnsDta.LeaderSignature)
	if err != nil {
		log.Debug("isBlockHeaderFinalInfoValid.SetLeaderSignature", "error", err.Error())
		return false
	}

	err = sr.HeaderSigVerifier().VerifyLeaderSignature(header)
	if err != nil {
		log.Debug("isBlockHeaderFinalInfoValid.VerifyLeaderSignature", "error", err.Error())
		return false
	}

	err = sr.HeaderSigVerifier().VerifySignature(header)
	if err != nil {
		log.Debug("isBlockHeaderFinalInfoValid.VerifySignature", "error", err.Error())
		return false
	}

	return true
}

func (sr *subroundEndRound) receivedHeader(headerHandler data.HeaderHandler) {
	if sr.ConsensusGroup() == nil || sr.IsSelfLeaderInCurrentRound() {
		return
	}

	sr.AddReceivedHeader(headerHandler)

	sr.doEndRoundJobByParticipant(nil)
}

// doEndRoundJob method does the job of the subround EndRound
func (sr *subroundEndRound) doEndRoundJob() bool {
	if !sr.IsSelfLeaderInCurrentRound() {
		if sr.IsNodeInConsensusGroup(sr.SelfPubKey()) {
			err := sr.prepareBroadcastBlockDataForValidator()
			if err != nil {
				log.Warn("validator in consensus group preparing for delayed broadcast",
					"error", err.Error())
			}
		}

		return sr.doEndRoundJobByParticipant(nil)
	}

	return sr.doEndRoundJobByLeader()
}

func (sr *subroundEndRound) doEndRoundJobByLeader() bool {
	bitmap := sr.GenerateBitmap(SrSignature)
	err := sr.checkSignaturesValidity(bitmap)
	if err != nil {
		log.Debug("doEndRoundJobByLeader.checkSignaturesValidity", "error", err.Error())
		return false
	}

	// Aggregate sig and add it to the block
	sig, err := sr.MultiSigner().AggregateSigs(bitmap)
	if err != nil {
		log.Debug("doEndRoundJobByLeader.AggregateSigs", "error", err.Error())
		return false
	}

	err = sr.Header.SetPubKeysBitmap(bitmap)
	if err != nil {
		log.Debug("doEndRoundJobByLeader.SetPubKeysBitmap", "error", err.Error())
		return false
	}

	err = sr.Header.SetSignature(sig)
	if err != nil {
		log.Debug("doEndRoundJobByLeader.SetSignature", "error", err.Error())
		return false
	}

	// Header is complete so the leader can sign it
	leaderSignature, err := sr.signBlockHeader()
	if err != nil {
		log.Error(err.Error())
		return false
	}

	err = sr.Header.SetLeaderSignature(leaderSignature)
	if err != nil {
		log.Debug("doEndRoundJobByLeader.SetLeaderSignature", "error", err.Error())
		return false
	}

	ok := sr.ScheduledProcessor().IsProcessedOKWithTimeout()
	if !ok {
		return false
	}

	roundHandler := sr.RoundHandler()
	if roundHandler.RemainingTime(roundHandler.TimeStamp(), roundHandler.TimeDuration()) < 0 {
		log.Debug("doEndRoundJob: time is out -> cancel broadcasting final info and header",
			"round time stamp", roundHandler.TimeStamp(),
			"current time", time.Now())
		return false
	}

	// broadcast header and final info section

	// create and broadcast header final info
	sr.createAndBroadcastHeaderFinalInfo()

	// broadcast header
	err = sr.BroadcastMessenger().BroadcastHeader(sr.Header)
	if err != nil {
		log.Debug("doEndRoundJobByLeader.BroadcastHeader", "error", err.Error())
	}

	startTime := time.Now()
	err = sr.BlockProcessor().CommitBlock(sr.Header, sr.Body)
	elapsedTime := time.Since(startTime)
	if elapsedTime >= core.CommitMaxTime {
		log.Warn("doEndRoundJobByLeader.CommitBlock", "elapsed time", elapsedTime)
	} else {
		log.Debug("elapsed time to commit block",
			"time [s]", elapsedTime,
		)
	}
	if err != nil {
		log.Debug("doEndRoundJobByLeader.CommitBlock", "error", err)
		return false
	}

	sr.SetStatus(sr.Current(), spos.SsFinished)

	sr.displayStatistics()

	log.Debug("step 3: Body and Header have been committed and header has been broadcast")

	err = sr.broadcastBlockDataLeader()
	if err != nil {
		log.Debug("doEndRoundJobByLeader.broadcastBlockDataLeader", "error", err.Error())
	}

	msg := fmt.Sprintf("Added proposed block with nonce  %d  in blockchain", sr.Header.GetNonce())
	log.Debug(display.Headline(msg, sr.SyncTimer().FormattedCurrentTime(), "+"))

	sr.updateMetricsForLeader()

	return true
}

func (sr *subroundEndRound) createAndBroadcastHeaderFinalInfo() {
	cnsMsg := consensus.NewConsensusMessage(
		sr.GetData(),
		nil,
		nil,
		nil,
		[]byte(sr.SelfPubKey()),
		nil,
		int(MtBlockHeaderFinalInfo),
		sr.RoundHandler().Index(),
		sr.ChainID(),
		sr.Header.GetPubKeysBitmap(),
		sr.Header.GetSignature(),
		sr.Header.GetLeaderSignature(),
		sr.CurrentPid(),
	)

	err := sr.BroadcastMessenger().BroadcastConsensusMessage(cnsMsg)
	if err != nil {
		log.Debug("doEndRoundJob.BroadcastConsensusMessage", "error", err.Error())
		return
	}

	log.Debug("step 3: block header final info has been sent",
		"PubKeysBitmap", sr.Header.GetPubKeysBitmap(),
		"AggregateSignature", sr.Header.GetSignature(),
		"LeaderSignature", sr.Header.GetLeaderSignature())
}

func (sr *subroundEndRound) doEndRoundJobByParticipant(cnsDta *consensus.Message) bool {
	sr.mutProcessingEndRound.Lock()
	defer sr.mutProcessingEndRound.Unlock()

	if sr.RoundCanceled {
		return false
	}
	if !sr.IsConsensusDataSet() {
		return false
	}
	if !sr.IsSubroundFinished(sr.Previous()) {
		return false
	}
	if sr.IsSubroundFinished(sr.Current()) {
		return false
	}

	haveHeader, header := sr.haveConsensusHeaderWithFullInfo(cnsDta)
	if !haveHeader {
		return false
	}

	defer func() {
		sr.SetProcessingBlock(false)
	}()

	sr.SetProcessingBlock(true)

	shouldNotCommitBlock := sr.ExtendedCalled || int64(header.GetRound()) < sr.RoundHandler().Index()
	if shouldNotCommitBlock {
		log.Debug("canceled round, extended has been called or round index has been changed",
			"round", sr.RoundHandler().Index(),
			"subround", sr.Name(),
			"header round", header.GetRound(),
			"extended called", sr.ExtendedCalled,
		)
		return false
	}

	if sr.isOutOfTime() {
		return false
	}

	ok := sr.ScheduledProcessor().IsProcessedOKWithTimeout()
	if !ok {
		return false
	}

	startTime := time.Now()
	err := sr.BlockProcessor().CommitBlock(header, sr.Body)
	elapsedTime := time.Since(startTime)
	if elapsedTime >= core.CommitMaxTime {
		log.Warn("doEndRoundJobByParticipant.CommitBlock", "elapsed time", elapsedTime)
	} else {
		log.Debug("elapsed time to commit block",
			"time [s]", elapsedTime,
		)
	}
	if err != nil {
		log.Debug("doEndRoundJobByParticipant.CommitBlock", "error", err.Error())
		return false
	}

	sr.SetStatus(sr.Current(), spos.SsFinished)

	if sr.IsNodeInConsensusGroup(sr.SelfPubKey()) {
		err = sr.setHeaderForValidator(header)
		if err != nil {
			log.Warn("doEndRoundJobByParticipant", "error", err.Error())
		}
	}

	sr.displayStatistics()

	log.Debug("step 3: Body and Header have been committed")

	headerTypeMsg := "received"
	if cnsDta != nil {
		headerTypeMsg = "assembled"
	}

	msg := fmt.Sprintf("Added %s block with nonce  %d  in blockchain", headerTypeMsg, header.GetNonce())
	log.Debug(display.Headline(msg, sr.SyncTimer().FormattedCurrentTime(), "-"))
	return true
}

func (sr *subroundEndRound) haveConsensusHeaderWithFullInfo(cnsDta *consensus.Message) (bool, data.HeaderHandler) {
	if cnsDta == nil {
		return sr.isConsensusHeaderReceived()
	}

	if check.IfNil(sr.Header) {
		return false, nil
	}

	header := sr.Header.ShallowClone()
	err := header.SetPubKeysBitmap(cnsDta.PubKeysBitmap)
	if err != nil {
		return false, nil
	}

	err = header.SetSignature(cnsDta.AggregateSignature)
	if err != nil {
		return false, nil
	}

	err = header.SetLeaderSignature(cnsDta.LeaderSignature)
	if err != nil {
		return false, nil
	}

	return true, header
}

func (sr *subroundEndRound) isConsensusHeaderReceived() (bool, data.HeaderHandler) {
	if check.IfNil(sr.Header) {
		return false, nil
	}

	consensusHeaderHash, err := core.CalculateHash(sr.Marshalizer(), sr.Hasher(), sr.Header)
	if err != nil {
		log.Debug("isConsensusHeaderReceived: calculate consensus header hash", "error", err.Error())
		return false, nil
	}

	receivedHeaders := sr.GetReceivedHeaders()

	var receivedHeaderHash []byte
	for index := range receivedHeaders {
		receivedHeader := receivedHeaders[index].ShallowClone()
		err = receivedHeader.SetLeaderSignature(nil)
		if err != nil {
			log.Debug("isConsensusHeaderReceived - SetLeaderSignature", "error", err.Error())
			return false, nil
		}

		err = receivedHeader.SetPubKeysBitmap(nil)
		if err != nil {
			log.Debug("isConsensusHeaderReceived - SetPubKeysBitmap", "error", err.Error())
			return false, nil
		}

		err = receivedHeader.SetSignature(nil)
		if err != nil {
			log.Debug("isConsensusHeaderReceived - SetSignature", "error", err.Error())
			return false, nil
		}

		receivedHeaderHash, err = core.CalculateHash(sr.Marshalizer(), sr.Hasher(), receivedHeader)
		if err != nil {
			log.Debug("isConsensusHeaderReceived: calculate received header hash", "error", err.Error())
			return false, nil
		}

		if bytes.Equal(receivedHeaderHash, consensusHeaderHash) {
			return true, receivedHeaders[index]
		}
	}

	return false, nil
}

func (sr *subroundEndRound) signBlockHeader() ([]byte, error) {
	headerClone := sr.Header.ShallowClone()
	err := headerClone.SetLeaderSignature(nil)
	if err != nil {
		return nil, err
	}

	marshalizedHdr, err := sr.Marshalizer().Marshal(headerClone)
	if err != nil {
		return nil, err
	}

	return sr.SingleSigner().Sign(sr.PrivateKey(), marshalizedHdr)
}

func (sr *subroundEndRound) updateMetricsForLeader() {
	sr.appStatusHandler.Increment(core.MetricCountAcceptedBlocks)
	sr.appStatusHandler.SetStringValue(core.MetricConsensusRoundState,
		fmt.Sprintf("valid block produced in %f sec", time.Since(sr.RoundHandler().TimeStamp()).Seconds()))
}

func (sr *subroundEndRound) broadcastBlockDataLeader() error {
	miniBlocks, transactions, err := sr.BlockProcessor().MarshalizedDataToBroadcast(sr.Header, sr.Body)
	if err != nil {
		return err
	}

	return sr.BroadcastMessenger().BroadcastBlockDataLeader(sr.Header, miniBlocks, transactions)
}

func (sr *subroundEndRound) setHeaderForValidator(header data.HeaderHandler) error {
	idx, err := sr.SelfConsensusGroupIndex()
	if err != nil {
		return err
	}

	// todo: avoid calling MarshalizeDataToBroadcast twice for validators
	miniBlocks, transactions, err := sr.BlockProcessor().MarshalizedDataToBroadcast(sr.Header, sr.Body)
	if err != nil {
		return err
	}

	go sr.BroadcastMessenger().PrepareBroadcastHeaderValidator(header, miniBlocks, transactions, idx)

	return nil
}

func (sr *subroundEndRound) prepareBroadcastBlockDataForValidator() error {
	idx, err := sr.SelfConsensusGroupIndex()
	if err != nil {
		return err
	}

	miniBlocks, transactions, err := sr.BlockProcessor().MarshalizedDataToBroadcast(sr.Header, sr.Body)
	if err != nil {
		return err
	}

	go sr.BroadcastMessenger().PrepareBroadcastBlockDataValidator(sr.Header, miniBlocks, transactions, idx)

	return nil
}

// doEndRoundConsensusCheck method checks if the consensus is achieved
func (sr *subroundEndRound) doEndRoundConsensusCheck() bool {
	if sr.RoundCanceled {
		return false
	}

	if sr.IsSubroundFinished(sr.Current()) {
		return true
	}

	return false
}

func (sr *subroundEndRound) checkSignaturesValidity(bitmap []byte) error {
	nbBitsBitmap := len(bitmap) * 8
	consensusGroup := sr.ConsensusGroup()
	consensusGroupSize := len(consensusGroup)
	size := consensusGroupSize

	if consensusGroupSize > nbBitsBitmap {
		size = nbBitsBitmap
	}

	for i := 0; i < size; i++ {
		indexRequired := (bitmap[i/8] & (1 << uint16(i%8))) > 0
		if !indexRequired {
			continue
		}

		pubKey := consensusGroup[i]
		isSigJobDone, err := sr.JobDone(pubKey, SrSignature)
		if err != nil {
			return err
		}

		if !isSigJobDone {
			return spos.ErrNilSignature
		}

		signature, err := sr.MultiSigner().SignatureShare(uint16(i))
		if err != nil {
			return err
		}

		err = sr.MultiSigner().VerifySignatureShare(uint16(i), signature, sr.GetData(), bitmap)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sr *subroundEndRound) isOutOfTime() bool {
	startTime := sr.RoundTimeStamp
	maxTime := sr.RoundHandler().TimeDuration() * time.Duration(sr.processingThresholdPercentage) / 100
	if sr.RoundHandler().RemainingTime(startTime, maxTime) < 0 {
		log.Debug("canceled round, time is out",
			"round", sr.SyncTimer().FormattedCurrentTime(), sr.RoundHandler().Index(),
			"subround", sr.Name())

		sr.RoundCanceled = true
		return true
	}

	return false
}
