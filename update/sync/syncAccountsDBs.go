package sync

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/genesis"
)

var _ update.EpochStartTriesSyncHandler = (*syncAccountsDBs)(nil)

type syncAccountsDBs struct {
	accountsBDsSyncers update.AccountsDBSyncContainer
	activeAccountsDBs  map[state.AccountsDbIdentifier]state.AccountsAdapter
	mutSynced          sync.Mutex
	synced             bool
}

// ArgsNewSyncAccountsDBsHandler is the argument structured to create a sync tries handler
type ArgsNewSyncAccountsDBsHandler struct {
	AccountsDBsSyncers update.AccountsDBSyncContainer
	ActiveAccountsDBs  map[state.AccountsDbIdentifier]state.AccountsAdapter
}

// NewSyncAccountsDBsHandler creates a new syncAccountsDBs
func NewSyncAccountsDBsHandler(args ArgsNewSyncAccountsDBsHandler) (*syncAccountsDBs, error) {
	if check.IfNil(args.AccountsDBsSyncers) {
		return nil, update.ErrNilAccountsDBSyncContainer
	}

	st := &syncAccountsDBs{
		accountsBDsSyncers: args.AccountsDBsSyncers,
		activeAccountsDBs:  make(map[state.AccountsDbIdentifier]state.AccountsAdapter),
		synced:             false,
		mutSynced:          sync.Mutex{},
	}
	for key, value := range args.ActiveAccountsDBs {
		st.activeAccountsDBs[key] = value
	}

	return st, nil
}

// SyncTriesFrom syncs all the state tries from an epoch start metachain
func (st *syncAccountsDBs) SyncTriesFrom(meta *block.MetaBlock, ownShardId uint32) error {
	if !meta.IsStartOfEpochBlock() && meta.Nonce > 0 {
		return update.ErrNotEpochStartBlock
	}

	st.mutSynced.Lock()
	st.synced = false
	st.mutSynced.Unlock()

	numHeadersToSync := len(meta.EpochStart.LastFinalizedHeaders) + 1
	tempMetaShardId := numHeadersToSync - 1

	invalidShardId := int(ownShardId) > len(meta.EpochStart.LastFinalizedHeaders)-1 && ownShardId != core.MetachainShardId
	if invalidShardId {
		return update.ErrInvalidOwnShardId
	}

	if ownShardId == core.MetachainShardId {
		ownShardId = uint32(tempMetaShardId)
	}

	for i := 0; i < numHeadersToSync; i++ {
		headerToSync := (i + int(ownShardId)) % numHeadersToSync

		if headerToSync == tempMetaShardId {
			err := st.syncMeta(meta)
			if err != nil {
				return err
			}

			continue
		}

		err := st.syncShard(meta.EpochStart.LastFinalizedHeaders[headerToSync])
		if err != nil {
			return err
		}
	}

	st.mutSynced.Lock()
	st.synced = true
	st.mutSynced.Unlock()

	return nil
}

func (st *syncAccountsDBs) syncMeta(meta *block.MetaBlock) error {
	err := st.syncAccountsOfType(
		genesis.UserAccount,
		state.UserAccountsState,
		core.MetachainShardId,
		meta.RootHash,
	)
	if err != nil {
		return fmt.Errorf("%w UserAccount, shard: meta", err)
	}

	err = st.syncAccountsOfType(
		genesis.ValidatorAccount,
		state.PeerAccountsState,
		core.MetachainShardId,
		meta.ValidatorStatsRootHash,
	)
	if err != nil {
		return fmt.Errorf("%w ValidatorAccount, shard: meta", err)
	}

	return nil
}

func (st *syncAccountsDBs) syncShard(shardData block.EpochStartShardData) error {
	err := st.syncAccountsOfType(
		genesis.UserAccount,
		state.UserAccountsState,
		shardData.ShardID,
		shardData.RootHash,
	)
	if err != nil {
		return fmt.Errorf("%w UserAccount, shard: %d", err, shardData.ShardID)
	}
	return nil
}

func (st *syncAccountsDBs) syncAccountsOfType(
	accountType genesis.Type,
	trieID state.AccountsDbIdentifier,
	shardId uint32,
	rootHash []byte,
) error {
	accAdapterIdentifier := genesis.CreateTrieIdentifier(shardId, accountType)

	log.Debug("syncing accounts",
		"type", accAdapterIdentifier,
		"shard ID", shardId,
		"root hash", rootHash,
	)

	accountsDBSyncer, err := st.accountsBDsSyncers.Get(accAdapterIdentifier)
	if err != nil {
		return err
	}

	success := st.tryRecreateTrie(
		shardId,
		accAdapterIdentifier,
		trieID,
		rootHash,
		accountsDBSyncer.GetTrieExporter(),
		accountType,
	)
	if success {
		return nil
	}

	return accountsDBSyncer.SyncAccounts(rootHash, shardId)
}

func (st *syncAccountsDBs) tryRecreateTrie(
	shardId uint32,
	id string,
	trieID state.AccountsDbIdentifier,
	rootHash []byte,
	trieExporter update.TrieExporter,
	accountType genesis.Type,
) bool {
	activeTrie := st.activeAccountsDBs[trieID]
	if check.IfNil(activeTrie) {
		return false
	}

	ctx := context.Background()
	tries, err := activeTrie.RecreateAllTries(rootHash, ctx)
	if err != nil {
		return false
	}

	for _, recreatedTrie := range tries {
		err = recreatedTrie.Commit()
		if err != nil {
			return false
		}
	}

	// TODO investigate if this can raise an OOM issue
	for hash, tr := range tries {
		if bytes.Equal(rootHash, []byte(hash)) {
			err = exportTrie(trieExporter, id, tr, ctx, accountType)
			if err != nil {
				return false
			}

			continue
		}

		dataTrieIdentifier := genesis.CreateTrieIdentifier(shardId, genesis.DataTrie)
		identifier := genesis.AddRootHashToIdentifier(dataTrieIdentifier, hash)
		err = trieExporter.ExportDataTrie(identifier, tr, ctx)
		if err != nil {
			return false
		}
	}

	return true
}

func exportTrie(
	trieExporter update.TrieExporter,
	id string,
	tr data.Trie,
	ctx context.Context,
	accountType genesis.Type,
) error {
	if accountType == genesis.UserAccount {
		_, err := trieExporter.ExportMainTrie(id, tr, ctx)
		return err
	}

	return trieExporter.ExportValidatorTrie(tr, ctx)
}

// IsInterfaceNil returns nil if underlying object is nil
func (st *syncAccountsDBs) IsInterfaceNil() bool {
	return st == nil
}
