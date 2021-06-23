package genesis

import (
	"errors"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStateExporter(t *testing.T) {
	tests := []struct {
		name            string
		args            ArgsNewStateExporter
		requiresErrorIs bool
		exError         error
	}{
		{
			name: "NilCoordinator",
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         nil,
				Hasher:                   &mock.HasherStub{},
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "test",
			},
			exError: data.ErrNilShardCoordinator,
		},
		{
			name: "NilStateSyncer",
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              nil,
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "test",
			},
			exError: update.ErrNilStateSyncer,
		},
		{
			name: "NilMarshalizer",
			args: ArgsNewStateExporter{
				Marshalizer:              nil,
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "test",
			},
			exError: data.ErrNilMarshalizer,
		},
		{
			name: "NilHardforkStorer",
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           nil,
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "test",
			},
			exError: update.ErrNilHardforkStorer,
		},
		{
			name: "NilHasher",
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   nil,
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "test",
			},
			exError: update.ErrNilHasher,
		},
		{
			name:            "NilAddressPubKeyConverter",
			requiresErrorIs: true,
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   nil,
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "test",
			},
			exError: update.ErrNilPubKeyConverter,
		},
		{
			name:            "NilValidatorPubKeyConverter",
			requiresErrorIs: true,
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: nil,
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "test",
			},
			exError: update.ErrNilPubKeyConverter,
		},
		{
			name: "NilGenesisNodesSetupHandler",
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: nil,
				ExportFolder:             "test",
			},
			exError: update.ErrNilGenesisNodesSetupHandler,
		},
		{
			name: "EmptyExportFolder",
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
				ExportFolder:             "",
			},
			exError: update.ErrEmptyExportFolderPath,
		},
		{
			name: "Ok",
			args: ArgsNewStateExporter{
				Marshalizer:              &mock.MarshalizerMock{},
				ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
				StateSyncer:              &mock.SyncStateStub{},
				HardforkStorer:           &mock.HardforkStorerStub{},
				Hasher:                   &mock.HasherStub{},
				AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
				ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
				ExportFolder:             "test",
				GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
			},
			exError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStateExporter(tt.args)
			if tt.requiresErrorIs {
				require.True(t, errors.Is(err, tt.exError))
			} else {
				require.Equal(t, err, tt.exError)
			}
		})
	}
}

func TestExportAll(t *testing.T) {
	t.Parallel()

	testFolderName := "testFiles"
	testPath := "./" + testFolderName
	defer func() {
		_ = os.RemoveAll(testPath)
	}()

	metaBlock := &block.MetaBlock{Round: 2, ChainID: []byte("chainId")}
	unFinishedMetaBlocks := map[string]*block.MetaBlock{
		"hash": {Round: 1, ChainID: []byte("chainId")},
	}
	miniBlock := &block.MiniBlock{}
	tx := &transaction.Transaction{Nonce: 1, Value: big.NewInt(100), SndAddr: []byte("snd"), RcvAddr: []byte("rcv")}
	stateSyncer := &mock.SyncStateStub{
		GetEpochStartMetaBlockCalled: func() (block *block.MetaBlock, err error) {
			return metaBlock, nil
		},
		GetUnFinishedMetaBlocksCalled: func() (map[string]*block.MetaBlock, error) {
			return unFinishedMetaBlocks, nil
		},
		GetAllMiniBlocksCalled: func() (m map[string]*block.MiniBlock, err error) {
			mbs := make(map[string]*block.MiniBlock)
			mbs["mb"] = miniBlock
			return mbs, nil
		},
		GetAllTransactionsCalled: func() (m map[string]data.TransactionHandler, err error) {
			mt := make(map[string]data.TransactionHandler)
			mt["tx"] = tx
			return mt, nil
		},
	}

	defer func() {
		_ = os.RemoveAll("./" + testFolderName + "/")
	}()

	transactionsWereWrote := false
	miniblocksWereWrote := false
	epochStartMetablockWasWrote := false
	unFinishedMetablocksWereWrote := false
	hs := &mock.HardforkStorerStub{
		WriteCalled: func(identifier string, key []byte, value []byte) error {
			switch identifier {
			case TransactionsIdentifier:
				transactionsWereWrote = true
			case MiniBlocksIdentifier:
				miniblocksWereWrote = true
			case EpochStartMetaBlockIdentifier:
				epochStartMetablockWasWrote = true
			case UnFinishedMetaBlocksIdentifier:
				unFinishedMetablocksWereWrote = true

			}

			return nil
		},
	}

	args := ArgsNewStateExporter{
		ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
		Marshalizer:              &mock.MarshalizerMock{},
		StateSyncer:              stateSyncer,
		HardforkStorer:           hs,
		Hasher:                   &mock.HasherMock{},
		AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
		ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
		ExportFolder:             "test",
		GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
	}

	stateExporter, _ := NewStateExporter(args)
	require.False(t, check.IfNil(stateExporter))

	err := stateExporter.ExportAll(1)
	require.Nil(t, err)

	assert.True(t, transactionsWereWrote)
	assert.True(t, miniblocksWereWrote)
	assert.True(t, epochStartMetablockWasWrote)
	assert.True(t, unFinishedMetablocksWereWrote)
}

func TestStateExport_ExportUnfinishedMetaBlocksShouldWork(t *testing.T) {
	t.Parallel()

	unFinishedMetaBlocks := map[string]*block.MetaBlock{
		"hash": {Round: 1, ChainID: []byte("chainId")},
	}
	stateSyncer := &mock.SyncStateStub{
		GetUnFinishedMetaBlocksCalled: func() (map[string]*block.MetaBlock, error) {
			return unFinishedMetaBlocks, nil
		},
	}

	unFinishedMetablocksWereWrote := false
	hs := &mock.HardforkStorerStub{
		WriteCalled: func(identifier string, key []byte, value []byte) error {
			if strings.Compare(identifier, UnFinishedMetaBlocksIdentifier) == 0 {
				unFinishedMetablocksWereWrote = true
			}
			return nil
		},
	}

	args := ArgsNewStateExporter{
		ShardCoordinator:         mock.NewOneShardCoordinatorMock(),
		Marshalizer:              &mock.MarshalizerMock{},
		StateSyncer:              stateSyncer,
		HardforkStorer:           hs,
		Hasher:                   &mock.HasherMock{},
		AddressPubKeyConverter:   &mock.PubkeyConverterStub{},
		ValidatorPubKeyConverter: &mock.PubkeyConverterStub{},
		ExportFolder:             "test",
		GenesisNodesSetupHandler: &mock.GenesisNodesSetupHandlerStub{},
	}

	stateExporter, _ := NewStateExporter(args)
	require.False(t, check.IfNil(stateExporter))

	err := stateExporter.exportUnFinishedMetaBlocks()
	require.Nil(t, err)

	assert.True(t, unFinishedMetablocksWereWrote)
}
