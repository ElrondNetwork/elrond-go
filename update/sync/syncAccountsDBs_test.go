package sync

import (
	"bytes"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSyncState_NilTrieSyncersShouldErr(t *testing.T) {
	t.Parallel()

	args := ArgsNewSyncAccountsDBsHandler{
		AccountsDBsSyncers: nil,
		ActiveAccountsDBs:  nil,
	}

	triesSyncHandler, err := NewSyncAccountsDBsHandler(args)
	require.Nil(t, triesSyncHandler)
	require.Equal(t, update.ErrNilAccountsDBSyncContainer, err)
}

func TestNewSyncState(t *testing.T) {
	t.Parallel()

	metaRootHash := []byte("metaRootHash")
	shardRootHash := []byte("shardDataRootHash")
	metaStateSynced := false
	shardStateSynced := false

	args := ArgsNewSyncAccountsDBsHandler{
		AccountsDBsSyncers: &mock.AccountsDBSyncersStub{
			GetCalled: func(key string) (syncer update.AccountsDBSyncer, err error) {
				return &mock.AccountsDBSyncerStub{
					SyncAccountsCalled: func(rootHash []byte, shardId uint32) error {
						if bytes.Equal(rootHash, metaRootHash) {
							metaStateSynced = true
						}
						if bytes.Equal(rootHash, shardRootHash) {
							shardStateSynced = true
						}
						return nil
					},
				}, nil
			},
		},
		ActiveAccountsDBs: make(map[state.AccountsDbIdentifier]state.AccountsAdapter),
	}

	args.ActiveAccountsDBs[state.UserAccountsState] = &mock.AccountsStub{
		RecreateAllTriesCalled: func(rootHash []byte) (map[string]data.Trie, error) {
			tries := make(map[string]data.Trie)
			tries[string(rootHash)] = &mock.TrieStub{}
			return tries, nil
		},
	}

	args.ActiveAccountsDBs[state.PeerAccountsState] = &mock.AccountsStub{
		RecreateAllTriesCalled: func(rootHash []byte) (map[string]data.Trie, error) {
			tries := make(map[string]data.Trie)
			tries[string(rootHash)] = &mock.TrieStub{}
			return tries, nil
		},
	}

	triesSyncHandler, err := NewSyncAccountsDBsHandler(args)
	require.Nil(t, err)

	metaBlock := &block.MetaBlock{
		Nonce: 1, Epoch: 1, RootHash: metaRootHash,
		EpochStart: block.EpochStart{
			LastFinalizedHeaders: []block.EpochStartShardData{
				{ShardID: 0, RootHash: shardRootHash},
			},
		},
	}

	err = triesSyncHandler.SyncTriesFrom(metaBlock, 0)
	require.Nil(t, err)

	assert.True(t, metaStateSynced)
	assert.True(t, shardStateSynced)

	err = triesSyncHandler.SyncTriesFrom(metaBlock, core.MetachainShardId)
	require.Nil(t, err)

	err = triesSyncHandler.SyncTriesFrom(metaBlock, 1)
	require.Equal(t, update.ErrInvalidOwnShardId, err)
}
