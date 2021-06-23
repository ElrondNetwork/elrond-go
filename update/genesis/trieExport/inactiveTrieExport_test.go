package trieExport

import (
	"context"
	"fmt"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/keyValStorage"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/stretchr/testify/assert"
)

func TestNewInactiveTrieExporter_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	ite, err := NewInactiveTrieExporter(nil)
	assert.True(t, check.IfNil(ite))
	assert.Equal(t, data.ErrNilMarshalizer, err)
}

func TestNewInactiveTrieExporter(t *testing.T) {
	t.Parallel()

	ite, err := NewInactiveTrieExporter(&mock.MarshalizerMock{})
	assert.Nil(t, err)
	assert.NotNil(t, ite)
}

func TestInactiveTrieExport_ExportValidatorTrieDoesNothing(t *testing.T) {
	t.Parallel()

	ite, _ := NewInactiveTrieExporter(&mock.MarshalizerMock{})
	err := ite.ExportValidatorTrie(nil, nil)
	assert.Nil(t, err)
}

func TestInactiveTrieExport_ExportDataTrieDoesNothing(t *testing.T) {
	t.Parallel()

	ite, _ := NewInactiveTrieExporter(&mock.MarshalizerMock{})
	err := ite.ExportDataTrie("", nil, nil)
	assert.Nil(t, err)
}

func TestInactiveTrieExport_ExportMainTrieInvalidTrieRootHashShouldErr(t *testing.T) {
	t.Parallel()

	ite, _ := NewInactiveTrieExporter(&mock.MarshalizerMock{})

	expectedErr := fmt.Errorf("rootHash err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, expectedErr
		},
	}

	rootHashes, err := ite.ExportMainTrie("", tr, context.Background())
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, rootHashes)
}

func TestInactiveTrieExport_ExportMainTrieGetAllLeavesOnChannelErrShouldErr(t *testing.T) {
	t.Parallel()

	ite, _ := NewInactiveTrieExporter(&mock.MarshalizerMock{})

	expectedErr := fmt.Errorf("getAllLeavesOnChannel err")
	tr := &mock.TrieStub{
		RootCalled: func() ([]byte, error) {
			return nil, nil
		},
		GetAllLeavesOnChannelCalled: func(_ []byte) (chan core.KeyValueHolder, error) {
			return nil, expectedErr
		},
	}

	rootHashes, err := ite.ExportMainTrie("", tr, context.Background())
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, rootHashes)
}

func TestInactiveTrieExport_ExportMainTrieShouldReturnDataTrieRootHashes(t *testing.T) {
	t.Parallel()

	marshalizer := &mock.MarshalizerMock{}
	trieExporter, _ := NewInactiveTrieExporter(marshalizer)

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

	rootHashes, err := trieExporter.ExportMainTrie("", tr, context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(rootHashes))
	assert.Equal(t, expectedRootHash, rootHashes[0])
}
