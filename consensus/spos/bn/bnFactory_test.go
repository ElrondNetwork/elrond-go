package bn_test

import (
	"testing"

	"github.com/ElrondNetwork/elrond-go-sandbox/consensus"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos/bn"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos/mock"
	"github.com/stretchr/testify/assert"
)

func initFactoryWithContainer(container *mock.ConsensusCoreMock) bn.Factory {
	worker := initWorker()
	consensusState := initConsensusState()

	fct, _ := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	return fct
}

func initFactory() bn.Factory {
	container := mock.InitConsensusCore()
	return initFactoryWithContainer(container)
}

func TestFactory_GetMessageTypeName(t *testing.T) {
	t.Parallel()

	r := bn.GetStringValue(bn.MtBlockBody)
	assert.Equal(t, "(BLOCK_BODY)", r)

	r = bn.GetStringValue(bn.MtBlockHeader)
	assert.Equal(t, "(BLOCK_HEADER)", r)

	r = bn.GetStringValue(bn.MtCommitmentHash)
	assert.Equal(t, "(COMMITMENT_HASH)", r)

	r = bn.GetStringValue(bn.MtBitmap)
	assert.Equal(t, "(BITMAP)", r)

	r = bn.GetStringValue(bn.MtCommitment)
	assert.Equal(t, "(COMMITMENT)", r)

	r = bn.GetStringValue(bn.MtSignature)
	assert.Equal(t, "(SIGNATURE)", r)

	r = bn.GetStringValue(bn.MtUnknown)
	assert.Equal(t, "(UNKNOWN)", r)

	r = bn.GetStringValue(spos.MessageType(-1))
	assert.Equal(t, "Undefined message type", r)
}

func TestFactory_NewFactoryNilContainerShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	worker := initWorker()

	fct, err := bn.NewFactory(
		nil,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilConsensusDataContainer, err)
}

func TestFactory_NewFactoryNilConsensusStateShouldFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	worker := initWorker()

	fct, err := bn.NewFactory(
		container,
		nil,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilConsensusState, err)
}
func TestFactory_NewFactoryNilBlockchainShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetBlockchain(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilBlockChain, err)
}

func TestFactory_NewFactoryNilBlockProcessorShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetBlockProcessor(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilBlockProcessor, err)
}

func TestFactory_NewFactoryNilBootstraperShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetBootStrapper(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilBlootstraper, err)
}

func TestFactory_NewFactoryNilChronologyHandlerShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetChronology(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilChronologyHandler, err)
}

func TestFactory_NewFactoryNilHasherShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetHasher(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilHasher, err)
}

func TestFactory_NewFactoryNilMarshalizerShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetMarshalizer(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilMarshalizer, err)
}

func TestFactory_NewFactoryNilMultiSignerShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetMultiSigner(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilMultiSigner, err)
}

func TestFactory_NewFactoryNilRounderShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetRounder(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilRounder, err)
}

func TestFactory_NewFactoryNilShardCoordinatorShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetShardCoordinator(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilShardCoordinator, err)
}

func TestFactory_NewFactoryNilSyncTimerShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetSyncTimer(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_NewFactoryNilValidatorGroupSelectorShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()
	worker := initWorker()
	container.SetValidatorGroupSelector(nil)

	fct, err := bn.NewFactory(
		container,
		consensusState,
		worker,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilValidatorGroupSelector, err)
}

func TestFactory_NewFactoryNilWorkerShouldFail(t *testing.T) {
	t.Parallel()

	consensusState := initConsensusState()
	container := mock.InitConsensusCore()

	fct, err := bn.NewFactory(
		container,
		consensusState,
		nil,
	)

	assert.Nil(t, fct)
	assert.Equal(t, spos.ErrNilWorker, err)
}

func TestFactory_NewFactoryShouldWork(t *testing.T) {
	t.Parallel()

	fct := *initFactory()

	assert.NotNil(t, fct)
}

func TestFactory_GenerateSubroundStartRoundShouldFailWhenNewSubroundFail(t *testing.T) {
	t.Parallel()

	fct := *initFactory()
	fct.Worker().SetConsensusStateChangedChannels(nil)

	err := fct.GenerateStartRoundSubround()

	assert.Equal(t, spos.ErrNilChannel, err)
}

func TestFactory_GenerateSubroundStartRoundShouldFailWhenNewSubroundStartRoundFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	fct := *initFactoryWithContainer(container)
	container.SetSyncTimer(nil)

	err := fct.GenerateStartRoundSubround()

	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_GenerateSubroundBlockShouldFailWhenNewSubroundFail(t *testing.T) {
	t.Parallel()

	fct := *initFactory()
	fct.Worker().SetConsensusStateChangedChannels(nil)

	err := fct.GenerateBlockSubround()

	assert.Equal(t, spos.ErrNilChannel, err)
}

func TestFactory_GenerateSubroundBlockShouldFailWhenNewSubroundBlockFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	fct := *initFactoryWithContainer(container)
	container.SetSyncTimer(nil)

	err := fct.GenerateBlockSubround()

	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_GenerateSubroundCommitmentHashShouldFailWhenNewSubroundFail(t *testing.T) {
	t.Parallel()

	fct := *initFactory()
	fct.Worker().SetConsensusStateChangedChannels(nil)

	err := fct.GenerateCommitmentHashSubround()

	assert.Equal(t, spos.ErrNilChannel, err)
}

func TestFactory_GenerateSubroundCommitmentHashShouldFailWhenNewSubroundCommitmentHashFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	fct := *initFactoryWithContainer(container)
	container.SetSyncTimer(nil)

	err := fct.GenerateCommitmentHashSubround()

	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_GenerateSubroundBitmapShouldFailWhenNewSubroundFail(t *testing.T) {
	t.Parallel()

	fct := *initFactory()
	fct.Worker().SetConsensusStateChangedChannels(nil)

	err := fct.GenerateBitmapSubround()

	assert.Equal(t, spos.ErrNilChannel, err)
}

func TestFactory_GenerateSubroundBitmapShouldFailWhenNewSubroundBitmapFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	fct := *initFactoryWithContainer(container)
	container.SetSyncTimer(nil)

	err := fct.GenerateBitmapSubround()

	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_GenerateSubroundCommitmentShouldFailWhenNewSubroundFail(t *testing.T) {
	t.Parallel()

	fct := *initFactory()
	fct.Worker().SetConsensusStateChangedChannels(nil)

	err := fct.GenerateCommitmentSubround()

	assert.Equal(t, spos.ErrNilChannel, err)
}

func TestFactory_GenerateSubroundCommitmentShouldFailWhenNewSubroundCommitmentFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	fct := *initFactoryWithContainer(container)
	container.SetSyncTimer(nil)

	err := fct.GenerateCommitmentSubround()

	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_GenerateSubroundSignatureShouldFailWhenNewSubroundFail(t *testing.T) {
	t.Parallel()

	fct := *initFactory()
	fct.Worker().SetConsensusStateChangedChannels(nil)

	err := fct.GenerateSignatureSubround()

	assert.Equal(t, spos.ErrNilChannel, err)
}

func TestFactory_GenerateSubroundSignatureShouldFailWhenNewSubroundSignatureFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	fct := *initFactoryWithContainer(container)
	container.SetSyncTimer(nil)

	err := fct.GenerateSignatureSubround()

	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_GenerateSubroundEndRoundShouldFailWhenNewSubroundFail(t *testing.T) {
	t.Parallel()

	fct := *initFactory()
	fct.Worker().SetConsensusStateChangedChannels(nil)

	err := fct.GenerateEndRoundSubround()

	assert.Equal(t, spos.ErrNilChannel, err)
}

func TestFactory_GenerateSubroundEndRoundShouldFailWhenNewSubroundEndRoundFail(t *testing.T) {
	t.Parallel()

	container := mock.InitConsensusCore()
	fct := *initFactoryWithContainer(container)
	container.SetSyncTimer(nil)

	err := fct.GenerateEndRoundSubround()

	assert.Equal(t, spos.ErrNilSyncTimer, err)
}

func TestFactory_GenerateSubroundsShouldWork(t *testing.T) {
	t.Parallel()

	subroundHandlers := 0

	chrm := &mock.ChronologyHandlerMock{}
	chrm.AddSubroundCalled = func(subroundHandler consensus.SubroundHandler) {
		subroundHandlers++
	}
	container := mock.InitConsensusCore()
	container.SetChronology(chrm)
	fct := *initFactoryWithContainer(container)

	_ = fct.GenerateSubrounds()

	assert.Equal(t, 7, subroundHandlers)
}
