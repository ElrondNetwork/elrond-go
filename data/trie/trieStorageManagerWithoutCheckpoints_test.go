package trie_test

import (
	"testing"

	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/mock"
	"github.com/ElrondNetwork/elrond-go/data/trie"
	"github.com/ElrondNetwork/elrond-go/data/trie/hashesHolder"
	"github.com/stretchr/testify/assert"
)

func getNewTrieStorageManagerArgs() trie.NewTrieStorageManagerArgs {
	return trie.NewTrieStorageManagerArgs{
		DB:                     mock.NewMemDbMock(),
		Marshalizer:            &mock.MarshalizerMock{},
		Hasher:                 &mock.HasherMock{},
		SnapshotDbConfig:       config.DBConfig{},
		GeneralConfig:          config.TrieStorageManagerConfig{},
		CheckpointHashesHolder: hashesHolder.NewCheckpointHashesHolder(10, 32),
	}
}

func TestNewTrieStorageManagerWithoutCheckpointsNilDb(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	args.DB = nil
	ts, err := trie.NewTrieStorageManagerWithoutCheckpoints(args)
	assert.Nil(t, ts)
	assert.Equal(t, trie.ErrNilDatabase, err)
}

func TestNewTrieStorageManagerWithoutCheckpointsNilMarshalizer(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	args.Marshalizer = nil
	ts, err := trie.NewTrieStorageManagerWithoutCheckpoints(args)
	assert.Nil(t, ts)
	assert.Equal(t, trie.ErrNilMarshalizer, err)
}

func TestNewTrieStorageManagerWithoutCheckpointsNilHasher(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	args.Hasher = nil
	ts, err := trie.NewTrieStorageManagerWithoutCheckpoints(args)
	assert.Nil(t, ts)
	assert.Equal(t, trie.ErrNilHasher, err)
}

func TestNewTrieStorageManagerWithoutCheckpointsOkVals(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	ts, err := trie.NewTrieStorageManagerWithoutCheckpoints(args)
	assert.Nil(t, err)
	assert.NotNil(t, ts)
}

func TestTrieStorageManagerWithoutCheckpoints_SetCheckpoint(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	ts, _ := trie.NewTrieStorageManagerWithoutCheckpoints(args)

	ts.SetCheckpoint([]byte("rootHash"), nil)
	assert.Equal(t, uint32(0), ts.PruningBlockingOperations())

	chLeaves := make(chan core.KeyValueHolder)
	ts.SetCheckpoint([]byte("rootHash"), chLeaves)
	assert.Equal(t, uint32(0), ts.PruningBlockingOperations())

	select {
	case <-chLeaves:
	default:
		assert.Fail(t, "unclosed channel")
	}
}

func TestTrieStorageManagerWithoutCheckpoints_AddDirtyCheckpointHashes(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	ts, _ := trie.NewTrieStorageManagerWithoutCheckpoints(args)

	assert.False(t, ts.AddDirtyCheckpointHashes([]byte("rootHash"), nil))
}

func TestTrieStorageManagerWithoutCheckpoints_Remove(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	ts, _ := trie.NewTrieStorageManagerWithoutCheckpoints(args)

	key := []byte("key")
	value := []byte("value")

	_ = args.DB.Put(key, value)
	hashes := make(data.ModifiedHashes)
	hashes[string(value)] = struct{}{}
	hashes[string(key)] = struct{}{}

	val, err := args.DB.Get(key)
	assert.Nil(t, err)
	assert.NotNil(t, val)

	err = ts.Remove(key)
	assert.Nil(t, err)

	val, err = args.DB.Get(key)
	assert.Nil(t, val)
	assert.NotNil(t, err)
}

func TestNewTrieStorageManagerCreatesDisabledCheckpointHashesHolder(t *testing.T) {
	t.Parallel()

	args := getNewTrieStorageManagerArgs()
	args.CheckpointHashesHolder = nil
	ts, err := trie.NewTrieStorageManagerWithoutCheckpoints(args)
	assert.NotNil(t, ts)
	assert.Nil(t, err)
}
