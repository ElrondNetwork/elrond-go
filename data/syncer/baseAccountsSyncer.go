package syncer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/trie"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/update"
)

type baseAccountsSyncer struct {
	hasher                    hashing.Hasher
	marshalizer               marshal.Marshalizer
	dataTries                 map[string]struct{}
	mutex                     sync.Mutex
	trieStorageManager        data.StorageManager
	requestHandler            trie.RequestHandler
	timeout                   time.Duration
	shardId                   uint32
	cacher                    storage.Cacher
	rootHash                  []byte
	maxTrieLevelInMemory      uint
	name                      string
	maxHardCapForMissingNodes int
	trieSyncerVersion         int
	trieExporter              update.TrieExporter
}

const timeBetweenStatisticsPrints = time.Second * 2

// ArgsNewBaseAccountsSyncer defines the arguments needed for the new account syncer
type ArgsNewBaseAccountsSyncer struct {
	Hasher                    hashing.Hasher
	Marshalizer               marshal.Marshalizer
	TrieStorageManager        data.StorageManager
	RequestHandler            trie.RequestHandler
	Timeout                   time.Duration
	Cacher                    storage.Cacher
	MaxTrieLevelInMemory      uint
	MaxHardCapForMissingNodes int
	TrieSyncerVersion         int
	TrieExporter              update.TrieExporter
}

func checkArgs(args ArgsNewBaseAccountsSyncer) error {
	if check.IfNil(args.Hasher) {
		return state.ErrNilHasher
	}
	if check.IfNil(args.Marshalizer) {
		return state.ErrNilMarshalizer
	}
	if check.IfNil(args.TrieStorageManager) {
		return state.ErrNilStorageManager
	}
	if check.IfNil(args.RequestHandler) {
		return state.ErrNilRequestHandler
	}
	if check.IfNil(args.Cacher) {
		return state.ErrNilCacher
	}
	if args.MaxHardCapForMissingNodes < 1 {
		return state.ErrInvalidMaxHardCapForMissingNodes
	}
	if check.IfNil(args.TrieExporter) {
		return state.ErrNilTrieExporter
	}

	return trie.CheckTrieSyncerVersion(args.TrieSyncerVersion)
}

func (b *baseAccountsSyncer) syncMainTrie(
	rootHash []byte,
	trieTopic string,
	ssh data.SyncStatisticsHandler,
	ctx context.Context,
) (data.Trie, error) {
	b.rootHash = rootHash

	dataTrie, err := trie.NewTrie(b.trieStorageManager, b.marshalizer, b.hasher, b.maxTrieLevelInMemory)
	if err != nil {
		return nil, err
	}

	b.dataTries[string(rootHash)] = struct{}{}
	arg := trie.ArgTrieSyncer{
		RequestHandler:                 b.requestHandler,
		InterceptedNodes:               b.cacher,
		DB:                             b.trieStorageManager.Database(),
		Marshalizer:                    b.marshalizer,
		Hasher:                         b.hasher,
		ShardId:                        b.shardId,
		Topic:                          trieTopic,
		TrieSyncStatistics:             ssh,
		TimeoutBetweenTrieNodesCommits: b.timeout,
		MaxHardCapForMissingNodes:      b.maxHardCapForMissingNodes,
	}
	trieSyncer, err := trie.CreateTrieSyncer(arg, b.trieSyncerVersion)
	if err != nil {
		return nil, err
	}

	err = trieSyncer.StartSyncing(rootHash, ctx)
	if err != nil {
		return nil, err
	}

	return dataTrie.Recreate(rootHash)
}

func (b *baseAccountsSyncer) printStatistics(ssh data.SyncStatisticsHandler, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info("finished trie sync", "name", b.name, "num received", ssh.NumReceived(), "num missing", ssh.NumMissing())
			return
		case <-time.After(timeBetweenStatisticsPrints):
			log.Info("trie sync in progress",
				"name", b.name,
				"num received", ssh.NumReceived(),
				"num missing", ssh.NumMissing(),
				"intercepted trie nodes cache", fmt.Sprintf("len: %d, size: %s", b.cacher.Len(), core.ConvertBytes(b.cacher.SizeInBytesContained())))
		}
	}
}

// GetTrieExporter returns the trie exporter
func (b *baseAccountsSyncer) GetTrieExporter() update.TrieExporter {
	return b.trieExporter
}

// IsInterfaceNil returns true if underlying object is nil
func (b *baseAccountsSyncer) IsInterfaceNil() bool {
	return b == nil
}
