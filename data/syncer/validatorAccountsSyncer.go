package syncer

import (
	"context"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data/trie/statistics"
	"github.com/ElrondNetwork/elrond-go/epochStart"
	"github.com/ElrondNetwork/elrond-go/process/factory"
)

var _ epochStart.AccountsDBSyncer = (*validatorAccountsSyncer)(nil)

type validatorAccountsSyncer struct {
	*baseAccountsSyncer
}

// ArgsNewValidatorAccountsSyncer defines the arguments needed for the new account syncer
type ArgsNewValidatorAccountsSyncer struct {
	ArgsNewBaseAccountsSyncer
}

// NewValidatorAccountsSyncer creates a validator account syncer
func NewValidatorAccountsSyncer(args ArgsNewValidatorAccountsSyncer) (*validatorAccountsSyncer, error) {
	err := checkArgs(args.ArgsNewBaseAccountsSyncer)
	if err != nil {
		return nil, err
	}

	b := &baseAccountsSyncer{
		hasher:                    args.Hasher,
		marshalizer:               args.Marshalizer,
		dataTries:                 make(map[string]struct{}),
		trieStorageManager:        args.TrieStorageManager,
		requestHandler:            args.RequestHandler,
		timeout:                   args.Timeout,
		shardId:                   core.MetachainShardId,
		cacher:                    args.Cacher,
		rootHash:                  nil,
		maxTrieLevelInMemory:      args.MaxTrieLevelInMemory,
		name:                      "peer accounts",
		maxHardCapForMissingNodes: args.MaxHardCapForMissingNodes,
		trieSyncerVersion:         args.TrieSyncerVersion,
		trieExporter:              args.TrieExporter,
	}

	u := &validatorAccountsSyncer{
		baseAccountsSyncer: b,
	}

	return u, nil
}

// SyncAccounts will launch the syncing method to gather all the data needed for validatorAccounts - it is a blocking method
func (v *validatorAccountsSyncer) SyncAccounts(rootHash []byte, _ uint32) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tss := statistics.NewTrieSyncStatistics()
	go v.printStatistics(tss, ctx)

	mainTrie, err := v.syncMainTrie(rootHash, factory.ValidatorTrieNodesTopic, tss, ctx)
	if err != nil {
		return err
	}

	return v.trieExporter.ExportValidatorTrie(mainTrie, ctx)
}
