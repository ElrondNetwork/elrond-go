package sync

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/state"
	dataTransaction "github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/genesis/trieExport"
	"github.com/ElrondNetwork/elrond-go/update/mock"
	"github.com/stretchr/testify/require"
)

func createHeaderSyncHandler(retErr bool) update.HeaderSyncHandler {
	meta := &block.MetaBlock{
		Nonce: 1, Epoch: 1, RootHash: []byte("metaRootHash"),
		EpochStart: block.EpochStart{
			LastFinalizedHeaders: []block.EpochStartShardData{
				{
					ShardID:                 0,
					RootHash:                []byte("shardDataRootHash"),
					PendingMiniBlockHeaders: []block.MiniBlockHeader{{Hash: []byte("hash")}},
					FirstPendingMetaBlock:   []byte("firstPending"),
				},
			},
		},
	}
	args := createMockHeadersSyncHandlerArgs()
	args.StorageService = &mock.ChainStorerMock{GetStorerCalled: func(unitType dataRetriever.UnitType) storage.Storer {
		return &mock.StorerStub{
			GetCalled: func(key []byte) (bytes []byte, err error) {
				if retErr {
					return nil, errors.New("err")
				}

				return json.Marshal(meta)
			},
		}
	}}

	if !retErr {
		args.StorageService = initStore()
		byteArray := args.Uint64Converter.ToByteSlice(meta.Nonce)
		_ = args.StorageService.Put(dataRetriever.MetaHdrNonceHashDataUnit, byteArray, []byte("firstPending"))
		marshaledData, _ := json.Marshal(meta)
		_ = args.StorageService.Put(dataRetriever.MetaBlockUnit, []byte("firstPending"), marshaledData)

		_ = args.StorageService.Put(dataRetriever.MetaBlockUnit, []byte(core.EpochStartIdentifier(meta.Epoch)), marshaledData)
	}

	headersSyncHandler, _ := NewHeadersSyncHandler(args)
	return headersSyncHandler
}

func createPendingMiniBlocksSyncHandler() update.EpochStartPendingMiniBlocksSyncHandler {
	txHash := []byte("txHash")
	mb := &block.MiniBlock{TxHashes: [][]byte{txHash}}
	args := ArgsNewPendingMiniBlocksSyncer{
		Storage: &mock.StorerStub{},
		Cache: &testscommon.CacherStub{
			RegisterHandlerCalled: func(f func(key []byte, val interface{})) {},
			PeekCalled: func(key []byte) (value interface{}, ok bool) {
				return mb, true
			},
		},
		Marshalizer:    &mock.MarshalizerFake{},
		RequestHandler: &mock.RequestHandlerStub{},
	}

	pendingMiniBlocksSyncer, _ := NewPendingMiniBlocksSyncer(args)
	return pendingMiniBlocksSyncer
}

func createPendingTxSyncHandler() update.PendingTransactionsSyncHandler {
	args := createMockArgs()
	args.Storages = &mock.ChainStorerMock{
		GetStorerCalled: func(unitType dataRetriever.UnitType) storage.Storer {
			return &mock.StorerStub{
				GetCalled: func(key []byte) (bytes []byte, err error) {
					tx := &dataTransaction.Transaction{
						Nonce: 1, Value: big.NewInt(10), SndAddr: []byte("snd"), RcvAddr: []byte("rcv"),
					}
					return json.Marshal(tx)
				},
			}
		},
	}

	pendingTxsSyncer, _ := NewPendingTransactionsSyncer(args)
	return pendingTxsSyncer
}

func createSyncTrieState(retErr bool) update.EpochStartTriesSyncHandler {
	args := ArgsNewSyncAccountsDBsHandler{
		AccountsDBsSyncers: &mock.AccountsDBSyncersStub{
			GetCalled: func(key string) (syncer update.AccountsDBSyncer, err error) {
				return &mock.AccountsDBSyncerStub{
					SyncAccountsCalled: func(rootHash []byte, _ uint32) error {
						if retErr {
							return errors.New("err")
						}
						return nil
					},
					GetTrieExporterCalled: func() update.TrieExporter {
						trieExporter, _ := trieExport.NewInactiveTrieExporter(&mock.MarshalizerMock{})
						return trieExporter
					},
				}, nil
			},
		},
		ActiveAccountsDBs: make(map[state.AccountsDbIdentifier]state.AccountsAdapter),
	}

	args.ActiveAccountsDBs[state.UserAccountsState] = &mock.AccountsStub{
		RecreateAllTriesCalled: func(rootHash []byte) (map[string]data.Trie, error) {
			tries := make(map[string]data.Trie)
			tries[string(rootHash)] = &mock.TrieStub{
				CommitCalled: func() error {
					if retErr {
						return errors.New("err")
					}
					return nil
				},
			}
			return tries, nil
		},
	}

	args.ActiveAccountsDBs[state.PeerAccountsState] = &mock.AccountsStub{
		RecreateAllTriesCalled: func(rootHash []byte) (map[string]data.Trie, error) {
			tries := make(map[string]data.Trie)
			tries[string(rootHash)] = &mock.TrieStub{
				CommitCalled: func() error {
					if retErr {
						return errors.New("err")
					}
					return nil
				},
			}
			return tries, nil
		},
	}

	triesSyncHandler, _ := NewSyncAccountsDBsHandler(args)
	return triesSyncHandler
}

func TestNewSyncState_Ok(t *testing.T) {
	t.Parallel()

	args := ArgsNewSyncState{
		Headers:      createHeaderSyncHandler(false),
		Tries:        createSyncTrieState(false),
		MiniBlocks:   createPendingMiniBlocksSyncHandler(),
		Transactions: createPendingTxSyncHandler(),
	}

	ss, err := NewSyncState(args)
	require.Nil(t, err)
	require.False(t, ss.IsInterfaceNil())

	err = ss.SyncAllState(1, 0)
	require.Nil(t, err)
}

func TestNewSyncState_CannotSyncHeaderErr(t *testing.T) {
	t.Parallel()

	args := ArgsNewSyncState{
		Headers:      createHeaderSyncHandler(true),
		Tries:        createSyncTrieState(false),
		MiniBlocks:   createPendingMiniBlocksSyncHandler(),
		Transactions: createPendingTxSyncHandler(),
	}

	ss, err := NewSyncState(args)
	require.Nil(t, err)

	err = ss.SyncAllState(1, 0)
	require.NotNil(t, err)
}

func TestNewSyncState_CannotSyncTriesErr(t *testing.T) {
	t.Parallel()

	args := ArgsNewSyncState{
		Headers:      createHeaderSyncHandler(false),
		Tries:        createSyncTrieState(true),
		MiniBlocks:   createPendingMiniBlocksSyncHandler(),
		Transactions: createPendingTxSyncHandler(),
	}

	ss, err := NewSyncState(args)
	require.Nil(t, err)

	err = ss.SyncAllState(1, 0)
	require.NotNil(t, err)
}

func TestSyncState_SyncAllStatePendingMiniBlocksErr(t *testing.T) {
	t.Parallel()

	localErr := errors.New("err")
	args := ArgsNewSyncState{
		Headers: &mock.HeaderSyncHandlerStub{
			SyncUnFinishedMetaHeadersCalled: func(epoch uint32) error {
				return nil
			},
			GetEpochStartMetaBlockCalled: func() (metaBlock *block.MetaBlock, err error) {
				return &block.MetaBlock{}, nil
			},
		},
		Tries: &mock.EpochStartTriesSyncHandlerMock{},
		MiniBlocks: &mock.EpochStartPendingMiniBlocksSyncHandlerMock{
			SyncPendingMiniBlocksFromMetaCalled: func(meta *block.MetaBlock, unFinished map[string]*block.MetaBlock, ctx context.Context) error {
				return localErr
			},
		},
		Transactions: &mock.PendingTransactionsSyncHandlerMock{},
	}

	ss, err := NewSyncState(args)
	require.Nil(t, err)

	err = ss.SyncAllState(0, 0)
	require.True(t, errors.Is(err, localErr))
}

func TestSyncState_SyncAllStateGetMiniBlocksErr(t *testing.T) {
	t.Parallel()

	localErr := errors.New("err")
	args := ArgsNewSyncState{
		Headers: &mock.HeaderSyncHandlerStub{
			SyncUnFinishedMetaHeadersCalled: func(epoch uint32) error {
				return nil
			},
			GetEpochStartMetaBlockCalled: func() (metaBlock *block.MetaBlock, err error) {
				return &block.MetaBlock{}, nil
			},
		},
		Tries: &mock.EpochStartTriesSyncHandlerMock{},
		MiniBlocks: &mock.EpochStartPendingMiniBlocksSyncHandlerMock{
			GetMiniBlocksCalled: func() (m map[string]*block.MiniBlock, err error) {
				return nil, localErr
			},
		},
		Transactions: &mock.PendingTransactionsSyncHandlerMock{},
	}

	ss, err := NewSyncState(args)
	require.Nil(t, err)

	err = ss.SyncAllState(0, 0)
	require.True(t, errors.Is(err, localErr))
}

func TestSyncState_SyncAllStateSyncTxsErr(t *testing.T) {
	t.Parallel()

	localErr := errors.New("err")
	args := ArgsNewSyncState{
		Headers: &mock.HeaderSyncHandlerStub{
			SyncUnFinishedMetaHeadersCalled: func(epoch uint32) error {
				return nil
			},
			GetEpochStartMetaBlockCalled: func() (metaBlock *block.MetaBlock, err error) {
				return &block.MetaBlock{}, nil
			},
		},
		Tries:      &mock.EpochStartTriesSyncHandlerMock{},
		MiniBlocks: &mock.EpochStartPendingMiniBlocksSyncHandlerMock{},
		Transactions: &mock.PendingTransactionsSyncHandlerMock{
			SyncPendingTransactionsForCalled: func(miniBlocks map[string]*block.MiniBlock, epoch uint32, ctx context.Context) error {
				return localErr
			},
		},
	}

	ss, err := NewSyncState(args)
	require.Nil(t, err)

	err = ss.SyncAllState(0, 0)
	require.True(t, errors.Is(err, localErr))
}
