package trieExport

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/keyValStorage"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTrieExport_InvalidExportFolderShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)
	assert.True(t, check.IfNil(trieExporter))
	assert.Equal(t, update.ErrEmptyExportFolderPath, err)
}

func TestNewTrieExport_NilShardCoordinatorShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"testFilesExportNodes",
		nil,
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)
	assert.True(t, check.IfNil(trieExporter))
	assert.Equal(t, data.ErrNilShardCoordinator, err)
}

func TestNewTrieExport_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		nil,
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)
	assert.Nil(t, trieExporter)
	assert.Equal(t, data.ErrNilMarshalizer, err)
}

func TestNewTrieExport_NilHardforkStorerShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		nil,
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)
	assert.True(t, check.IfNil(trieExporter))
	assert.Equal(t, update.ErrNilHardforkStorer, err)
}

func TestNewTrieExport_NilValidatorPubKeyConverterShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		nil,
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)
	assert.True(t, check.IfNil(trieExporter))
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), update.ErrNilPubKeyConverter.Error()))
}

func TestNewTrieExport_NilAddressPubKeyConverterShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		nil,
		&mock.GenesisNodesSetupHandlerStub{},
	)
	assert.True(t, check.IfNil(trieExporter))
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), update.ErrNilPubKeyConverter.Error()))
}

func TestNewTrieExport_NilGenesisNodesSetupHandlerShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		nil,
	)
	assert.True(t, check.IfNil(trieExporter))
	assert.Equal(t, update.ErrNilGenesisNodesSetupHandler, err)
}

func TestNewTrieExport(t *testing.T) {
	t.Parallel()

	trieExporter, err := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)
	assert.Nil(t, err)
	assert.False(t, check.IfNil(trieExporter))
}

func TestTrieExport_ExportValidatorTrieInvalidTrieRootHashShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	expectedErr := fmt.Errorf("rootHash err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, expectedErr
		},
	}

	err := trieExporter.ExportValidatorTrie(tr, context.Background())
	assert.Equal(t, expectedErr, err)
}

func TestTrieExport_ExportValidatorTrieGetAllLeavesOnChannelErrShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	expectedErr := fmt.Errorf("getAllLeavesOnChannel err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, expectedErr
		},
	}

	err := trieExporter.ExportValidatorTrie(tr, context.Background())
	assert.Equal(t, expectedErr, err)
}

func TestTrieExport_ExportMainTrieInvalidIdentifierShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	rootHashes, err := trieExporter.ExportMainTrie("invalid identifier", &mock.TrieStub{}, context.Background())
	assert.Nil(t, rootHashes)
	assert.NotNil(t, err)
}

func TestTrieExport_ExportMainTrieInvalidTrieRootHashShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	expectedErr := fmt.Errorf("rootHash err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, expectedErr
		},
	}

	rootHashes, err := trieExporter.ExportMainTrie("a@1@8", tr, context.Background())
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, rootHashes)
}

func TestTrieExport_ExportMainTrieGetAllLeavesOnChannelErrShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	expectedErr := fmt.Errorf("getAllLeavesOnChannel err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, expectedErr
		},
	}

	rootHashes, err := trieExporter.ExportMainTrie("a@1@8", tr, context.Background())
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, rootHashes)
}

func TestTrieExport_ExportMainTrieInvalidShardIdShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, nil
		},
	}

	rootHashes, err := trieExporter.ExportMainTrie("a@5@8", tr, context.Background())
	assert.Equal(t, sharding.ErrInvalidShardId, err)
	assert.Nil(t, rootHashes)
}

func TestTrieExport_ExportMainTrieHardforkStorerWriteErrShouldErr(t *testing.T) {
	t.Parallel()

	expectedErr := fmt.Errorf("hardforkStorer write err")
	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{
			WriteCalled: func(_ string, _ []byte, _ []byte) error {
				return expectedErr
			},
		},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, nil
		},
	}

	rootHashes, err := trieExporter.ExportMainTrie("a@0@8", tr, context.Background())
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, rootHashes)
}

func TestTrieExport_ExportMainTrieShouldWork(t *testing.T) {
	t.Parallel()

	marshalizer := &mock.MarshalizerMock{}
	writeCalled := 0
	finishIdentifierCalled := 0

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		marshalizer,
		&mock.HardforkStorerStub{
			WriteCalled: func(_ string, _ []byte, _ []byte) error {
				writeCalled++
				return nil
			},
			FinishedIdentifierCalled: func(_ string) error {
				finishIdentifierCalled++
				return nil
			},
		},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	expectedRootHash := []byte("rootHash")
	account1 := state.NewEmptyUserAccount()
	account2 := state.NewEmptyUserAccount()
	account2.RootHash = expectedRootHash

	serializedAcc1, err := marshalizer.Marshal(account1)
	assert.Nil(t, err)
	serializedAcc2, err := marshalizer.Marshal(account2)
	assert.Nil(t, err)

	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			leavesChannel := make(chan core.KeyValueHolder, 3)
			leavesChannel <- keyValStorage.NewKeyValStorage([]byte("key1"), []byte("val1"))
			leavesChannel <- keyValStorage.NewKeyValStorage([]byte("key2"), serializedAcc1)
			leavesChannel <- keyValStorage.NewKeyValStorage([]byte("key3"), serializedAcc2)
			close(leavesChannel)
			return leavesChannel, nil
		},
	}

	rootHashes, err := trieExporter.ExportMainTrie("a@0@8", tr, context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 4, writeCalled)
	assert.Equal(t, 1, finishIdentifierCalled)
	assert.Equal(t, 1, len(rootHashes))
	assert.Equal(t, expectedRootHash, rootHashes[0])
}

func TestTrieExport_ExportDataTrieInvalidIdentifierShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	err := trieExporter.ExportDataTrie("invalid identifier", &mock.TrieStub{}, context.Background())
	assert.NotNil(t, err)
}

func TestTrieExport_ExportDataTrieInvalidTrieRootHashShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	expectedErr := fmt.Errorf("rootHash err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, expectedErr
		},
	}

	err := trieExporter.ExportDataTrie("a@1@8", tr, context.Background())
	assert.Equal(t, expectedErr, err)
}

func TestTrieExport_ExportDataTrieGetAllLeavesOnChannelErrShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	expectedErr := fmt.Errorf("getAllLeavesOnChannel err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, expectedErr
		},
	}

	err := trieExporter.ExportDataTrie("a@1@8", tr, context.Background())
	assert.Equal(t, expectedErr, err)
}

func TestTrieExport_ExportDataTrieInvalidShardIdShouldErr(t *testing.T) {
	t.Parallel()

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, nil
		},
	}

	err := trieExporter.ExportDataTrie("a@5@8", tr, context.Background())
	assert.Equal(t, sharding.ErrInvalidShardId, err)
}

func TestTrieExport_ExportDataTrieHardforkStorerWriteErrShouldErr(t *testing.T) {
	t.Parallel()

	expectedErr := fmt.Errorf("hardforkStorer write err")
	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		&mock.HardforkStorerStub{
			WriteCalled: func(_ string, _ []byte, _ []byte) error {
				return expectedErr
			},
		},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, nil
		},
	}

	err := trieExporter.ExportDataTrie("a@0@8", tr, context.Background())
	assert.Equal(t, expectedErr, err)
}

func TestTrieExport_ExportDataTrieShouldWork(t *testing.T) {
	t.Parallel()

	marshalizer := &mock.MarshalizerMock{}
	writeCalled := 0
	finishIdentifierCalled := 0

	trieExporter, _ := NewTrieExport(
		"testFilesExportNodes",
		mock.NewOneShardCoordinatorMock(),
		marshalizer,
		&mock.HardforkStorerStub{
			WriteCalled: func(_ string, _ []byte, _ []byte) error {
				writeCalled++
				return nil
			},
			FinishedIdentifierCalled: func(_ string) error {
				finishIdentifierCalled++
				return nil
			},
		},
		&mock.PubkeyConverterStub{},
		&mock.PubkeyConverterStub{},
		&mock.GenesisNodesSetupHandlerStub{},
	)

	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			leavesChannel := make(chan core.KeyValueHolder, 3)
			leavesChannel <- keyValStorage.NewKeyValStorage([]byte("key1"), []byte("val1"))
			leavesChannel <- keyValStorage.NewKeyValStorage([]byte("key2"), []byte("val2"))
			leavesChannel <- keyValStorage.NewKeyValStorage([]byte("key3"), []byte("val3"))
			close(leavesChannel)
			return leavesChannel, nil
		},
	}

	err := trieExporter.ExportDataTrie("a@0@8", tr, context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 4, writeCalled)
	assert.Equal(t, 1, finishIdentifierCalled)
}

func TestTrieExport_ExportTrieShouldExportNodesSetupJson(t *testing.T) {
	t.Parallel()

	testFolderName := "testFilesExportNodes"
	_ = os.Mkdir(testFolderName, 0777)

	defer func() {
		_ = os.RemoveAll(testFolderName)
	}()

	hs := &mock.HardforkStorerStub{
		WriteCalled: func(identifier string, key []byte, value []byte) error {
			return nil
		},
	}

	pubKeyConv := &mock.PubkeyConverterStub{
		EncodeCalled: func(pkBytes []byte) string {
			return string(pkBytes)
		},
	}

	trie := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return []byte{}, nil
		},
		GetAllLeavesOnChannelCalled: func(rootHash []byte) (chan core.KeyValueHolder, error) {
			ch := make(chan core.KeyValueHolder)

			mm := &mock.MarshalizerMock{}
			valInfo := &state.ValidatorInfo{List: string(core.EligibleList)}
			pacB, _ := mm.Marshal(valInfo)

			go func() {
				ch <- keyValStorage.NewKeyValStorage([]byte("test"), pacB)
				close(ch)
			}()

			return ch, nil
		},
	}

	stateExporter, err := NewTrieExport(
		testFolderName,
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		hs,
		pubKeyConv,
		pubKeyConv,
		&mock.GenesisNodesSetupHandlerStub{},
	)
	require.NoError(t, err)

	require.False(t, check.IfNil(stateExporter))

	err = stateExporter.ExportValidatorTrie(trie, context.Background())
	require.NoError(t, err)
}

func TestStateExport_ExportNodesSetupJsonShouldExportKeysInAlphabeticalOrder(t *testing.T) {
	t.Parallel()

	testFolderName := "testFilesExportNodes2"
	_ = os.Mkdir(testFolderName, 0777)

	defer func() {
		_ = os.RemoveAll(testFolderName)
	}()

	hs := &mock.HardforkStorerStub{
		WriteCalled: func(identifier string, key []byte, value []byte) error {
			return nil
		},
	}

	pubKeyConv := &mock.PubkeyConverterStub{
		EncodeCalled: func(pkBytes []byte) string {
			return string(pkBytes)
		},
	}

	stateExporter, err := NewTrieExport(
		testFolderName,
		mock.NewOneShardCoordinatorMock(),
		&mock.MarshalizerMock{},
		hs,
		pubKeyConv,
		pubKeyConv,
		&mock.GenesisNodesSetupHandlerStub{},
	)
	require.NoError(t, err)

	require.False(t, check.IfNil(stateExporter))

	vals := make(map[uint32][]*state.ValidatorInfo)
	val50 := &state.ValidatorInfo{ShardId: 5, PublicKey: []byte("aaa"), List: string(core.EligibleList)}
	val51 := &state.ValidatorInfo{ShardId: 5, PublicKey: []byte("bbb"), List: string(core.EligibleList)}
	val10 := &state.ValidatorInfo{ShardId: 5, PublicKey: []byte("ccc"), List: string(core.EligibleList)}
	val11 := &state.ValidatorInfo{ShardId: 5, PublicKey: []byte("ddd"), List: string(core.EligibleList)}
	val00 := &state.ValidatorInfo{ShardId: 5, PublicKey: []byte("aaaaaa"), List: string(core.EligibleList)}
	val01 := &state.ValidatorInfo{ShardId: 5, PublicKey: []byte("bbbbbb"), List: string(core.EligibleList)}
	vals[1] = []*state.ValidatorInfo{val50, val51}
	vals[0] = []*state.ValidatorInfo{val00, val01}
	vals[2] = []*state.ValidatorInfo{val10, val11}
	err = stateExporter.exportNodesSetupJson(vals)
	require.Nil(t, err)

	var nodesSetup sharding.NodesSetup

	nsBytes, err := ioutil.ReadFile(filepath.Join(testFolderName, core.NodesSetupJsonFileName))
	require.NoError(t, err)

	err = json.Unmarshal(nsBytes, &nodesSetup)
	require.NoError(t, err)

	initialNodes := nodesSetup.InitialNodes

	// results should be in alphabetical order, sorted by public key
	require.Equal(t, string(val50.PublicKey), initialNodes[0].PubKey) // aaa
	require.Equal(t, string(val00.PublicKey), initialNodes[1].PubKey) // aaaaaa
	require.Equal(t, string(val51.PublicKey), initialNodes[2].PubKey) // bbb
	require.Equal(t, string(val01.PublicKey), initialNodes[3].PubKey) // bbbbbb
	require.Equal(t, string(val10.PublicKey), initialNodes[4].PubKey) // ccc
	require.Equal(t, string(val11.PublicKey), initialNodes[5].PubKey) // ddd
}
