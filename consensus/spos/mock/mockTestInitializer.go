package mock

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus"
	"github.com/ElrondNetwork/elrond-go-sandbox/crypto"
	"github.com/ElrondNetwork/elrond-go-sandbox/data"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"time"
)

func InitChronologyHandlerMock() consensus.ChronologyHandler {
	chr := &ChronologyHandlerMock{}
	return chr
}

func InitBlockProcessorMock() *BlockProcessorMock {
	blockProcessorMock := &BlockProcessorMock{}
	blockProcessorMock.CreateBlockCalled = func(round int32, haveTime func() bool) (data.BodyHandler, error) {
		emptyBlock := make(block.Body, 0)

		return emptyBlock, nil
	}
	blockProcessorMock.CommitBlockCalled = func(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler) error {
		return nil
	}
	blockProcessorMock.RevertAccountStateCalled = func() {}
	blockProcessorMock.ProcessBlockCalled = func(blockChain data.ChainHandler, header data.HeaderHandler, body data.BodyHandler, haveTime func() time.Duration) error {
		return nil
	}
	blockProcessorMock.GetRootHashCalled = func() []byte {
		return []byte{}
	}
	blockProcessorMock.CreateBlockHeaderCalled = func(body data.BodyHandler) (header data.HeaderHandler, e error) {
		return &block.Header{RootHash: blockProcessorMock.GetRootHashCalled()}, nil
	}
	return blockProcessorMock
}

func InitMultiSignerMock() *BelNevMock {
	multiSigner := NewMultiSigner()
	multiSigner.CreateCommitmentMock = func() ([]byte, []byte) {
		return []byte("commSecret"), []byte("commitment")
	}
	multiSigner.VerifySignatureShareMock = func(index uint16, sig []byte, bitmap []byte) error {
		return nil
	}
	multiSigner.VerifyMock = func(bitmap []byte) error {
		return nil
	}
	multiSigner.AggregateSigsMock = func(bitmap []byte) ([]byte, error) {
		return []byte("aggregatedSig"), nil
	}
	multiSigner.AggregateCommitmentsMock = func(bitmap []byte) error {
		return nil
	}
	multiSigner.CreateSignatureShareMock = func(bitmap []byte) ([]byte, error) {
		return []byte("partialSign"), nil
	}
	return multiSigner
}

func InitKeys() (*KeyGenMock, *PrivateKeyMock, *PublicKeyMock) {
	toByteArrayMock := func() ([]byte, error) {
		return []byte("byteArray"), nil
	}
	privKeyMock := &PrivateKeyMock{
		ToByteArrayMock: toByteArrayMock,
	}
	pubKeyMock := &PublicKeyMock{
		ToByteArrayMock: toByteArrayMock,
	}
	privKeyFromByteArr := func(b []byte) (crypto.PrivateKey, error) {
		return privKeyMock, nil
	}
	pubKeyFromByteArr := func(b []byte) (crypto.PublicKey, error) {
		return pubKeyMock, nil
	}
	keyGenMock := &KeyGenMock{
		PrivateKeyFromByteArrayMock: privKeyFromByteArr,
		PublicKeyFromByteArrayMock:  pubKeyFromByteArr,
	}
	return keyGenMock, privKeyMock, pubKeyMock
}

func InitContainer() *ConsensusDataContainerMock {

	blockChain := &BlockChainMock{}
	blockProcessorMock := InitBlockProcessorMock()
	bootstraperMock := &BootstraperMock{}

	chronologyHandlerMock := InitChronologyHandlerMock()
	//consensusState := &ConsensusStateMock{}
	hasherMock := HasherMock{}
	marshalizerMock := MarshalizerMock{}
	multiSignerMock := InitMultiSignerMock()
	rounderMock := &RounderMock{}
	shardCoordinatorMock := ShardCoordinatorMock{}
	syncTimerMock := SyncTimerMock{}
	validatorGroupSelector := ValidatorGroupSelectorMock{}

	container := &ConsensusDataContainerMock{
		blockChain,
		blockProcessorMock,
		bootstraperMock,
		chronologyHandlerMock,
		nil,
		hasherMock,
		marshalizerMock,
		multiSignerMock,
		rounderMock,
		shardCoordinatorMock,
		syncTimerMock,
		validatorGroupSelector,
	}

	return container
}
