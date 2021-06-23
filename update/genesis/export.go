package genesis

import (
	"encoding/json"
	"fmt"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/update"
)

var _ update.ExportHandler = (*stateExport)(nil)

// ArgsNewStateExporter defines the arguments needed to create new state exporter
type ArgsNewStateExporter struct {
	ShardCoordinator         sharding.Coordinator
	StateSyncer              update.StateSyncer
	Marshalizer              marshal.Marshalizer
	Hasher                   hashing.Hasher
	HardforkStorer           update.HardforkStorer
	ExportFolder             string
	AddressPubKeyConverter   core.PubkeyConverter
	ValidatorPubKeyConverter core.PubkeyConverter
	GenesisNodesSetupHandler update.GenesisNodesSetupHandler
}

type stateExport struct {
	stateSyncer              update.StateSyncer
	shardCoordinator         sharding.Coordinator
	marshalizer              marshal.Marshalizer
	hasher                   hashing.Hasher
	hardforkStorer           update.HardforkStorer
	exportFolder             string
	addressPubKeyConverter   core.PubkeyConverter
	validatorPubKeyConverter core.PubkeyConverter
	genesisNodesSetupHandler update.GenesisNodesSetupHandler
}

var log = logger.GetOrCreate("update/genesis")

// NewStateExporter exports all the data at a specific moment to a hardfork storer
func NewStateExporter(args ArgsNewStateExporter) (*stateExport, error) {
	if check.IfNil(args.ShardCoordinator) {
		return nil, data.ErrNilShardCoordinator
	}
	if check.IfNil(args.StateSyncer) {
		return nil, update.ErrNilStateSyncer
	}
	if check.IfNil(args.Marshalizer) {
		return nil, data.ErrNilMarshalizer
	}
	if check.IfNil(args.Hasher) {
		return nil, update.ErrNilHasher
	}
	if check.IfNil(args.HardforkStorer) {
		return nil, update.ErrNilHardforkStorer
	}
	if len(args.ExportFolder) == 0 {
		return nil, update.ErrEmptyExportFolderPath
	}
	if check.IfNil(args.AddressPubKeyConverter) {
		return nil, fmt.Errorf("%w for address", update.ErrNilPubKeyConverter)
	}
	if check.IfNil(args.ValidatorPubKeyConverter) {
		return nil, fmt.Errorf("%w for validators", update.ErrNilPubKeyConverter)
	}
	if check.IfNil(args.GenesisNodesSetupHandler) {
		return nil, update.ErrNilGenesisNodesSetupHandler
	}

	se := &stateExport{
		stateSyncer:              args.StateSyncer,
		shardCoordinator:         args.ShardCoordinator,
		marshalizer:              args.Marshalizer,
		hasher:                   args.Hasher,
		hardforkStorer:           args.HardforkStorer,
		exportFolder:             args.ExportFolder,
		addressPubKeyConverter:   args.AddressPubKeyConverter,
		validatorPubKeyConverter: args.ValidatorPubKeyConverter,
		genesisNodesSetupHandler: args.GenesisNodesSetupHandler,
	}

	return se, nil
}

// ExportAll syncs and exports all the data from every shard for a certain epoch start block
func (se *stateExport) ExportAll(epoch uint32) error {
	err := se.stateSyncer.SyncAllState(epoch, se.shardCoordinator.SelfId())
	if err != nil {
		return err
	}

	defer func() {
		errClose := se.hardforkStorer.Close()
		log.LogIfError(errClose)
	}()

	err = se.exportEpochStartMetaBlock()
	if err != nil {
		return err
	}

	err = se.exportUnFinishedMetaBlocks()
	if err != nil {
		return err
	}

	err = se.exportAllMiniBlocks()
	if err != nil {
		return err
	}

	err = se.exportAllTransactions()
	if err != nil {
		return err
	}

	return nil
}

func (se *stateExport) exportAllTransactions() error {
	toExportTransactions, err := se.stateSyncer.GetAllTransactions()
	if err != nil {
		return err
	}

	log.Debug("Starting export for transactions", "len", len(toExportTransactions))
	for key, tx := range toExportTransactions {
		errExport := se.exportTx(key, tx)
		if errExport != nil {
			return errExport
		}
	}

	return se.hardforkStorer.FinishedIdentifier(TransactionsIdentifier)
}

func (se *stateExport) exportAllMiniBlocks() error {
	toExportMBs, err := se.stateSyncer.GetAllMiniBlocks()
	if err != nil {
		return err
	}

	log.Debug("Starting export for miniBlocks", "len", len(toExportMBs))
	for key, mb := range toExportMBs {
		errExport := se.exportMBs(key, mb)
		if errExport != nil {
			return errExport
		}
	}

	return se.hardforkStorer.FinishedIdentifier(MiniBlocksIdentifier)
}

func (se *stateExport) exportEpochStartMetaBlock() error {
	metaBlock, err := se.stateSyncer.GetEpochStartMetaBlock()
	if err != nil {
		return err
	}

	log.Debug("Starting export for epoch start metaBlock")
	err = se.exportMetaBlock(metaBlock, EpochStartMetaBlockIdentifier)
	if err != nil {
		return err
	}

	err = se.hardforkStorer.FinishedIdentifier(EpochStartMetaBlockIdentifier)
	if err != nil {
		return err
	}

	return nil
}

func (se *stateExport) exportUnFinishedMetaBlocks() error {
	unFinishedMetaBlocks, err := se.stateSyncer.GetUnFinishedMetaBlocks()
	if err != nil {
		return err
	}

	log.Debug("Starting export for unFinished metaBlocks", "len", len(unFinishedMetaBlocks))
	for _, metaBlock := range unFinishedMetaBlocks {
		errExportMetaBlock := se.exportMetaBlock(metaBlock, UnFinishedMetaBlocksIdentifier)
		if errExportMetaBlock != nil {
			return errExportMetaBlock
		}
	}

	err = se.hardforkStorer.FinishedIdentifier(UnFinishedMetaBlocksIdentifier)
	if err != nil {
		return err
	}

	return nil
}

func (se *stateExport) exportMetaBlock(metaBlock *block.MetaBlock, identifier string) error {
	jsonData, err := json.Marshal(metaBlock)
	if err != nil {
		return err
	}

	metaHash := se.hasher.Compute(string(jsonData))
	versionKey := CreateVersionKey(metaBlock, metaHash)
	err = se.hardforkStorer.Write(identifier, []byte(versionKey), jsonData)
	if err != nil {
		return err
	}

	log.Debug("Exported metaBlock",
		"identifier", identifier,
		"version key", versionKey,
		"hash", metaHash,
		"epoch", metaBlock.Epoch,
		"round", metaBlock.Round,
		"nonce", metaBlock.Nonce,
		"start of epoch block", metaBlock.Nonce == 0 || metaBlock.IsStartOfEpochBlock(),
		"rootHash", metaBlock.RootHash,
	)

	return nil
}

func (se *stateExport) exportMBs(key string, mb *block.MiniBlock) error {
	marshaledData, err := json.Marshal(mb)
	if err != nil {
		return err
	}

	keyToSave := CreateMiniBlockKey(key)

	err = se.hardforkStorer.Write(MiniBlocksIdentifier, []byte(keyToSave), marshaledData)
	if err != nil {
		return err
	}

	return nil
}

func (se *stateExport) exportTx(key string, tx data.TransactionHandler) error {
	marshaledData, err := json.Marshal(tx)
	if err != nil {
		return err
	}

	keyToSave := CreateTransactionKey(key, tx)

	err = se.hardforkStorer.Write(TransactionsIdentifier, []byte(keyToSave), marshaledData)
	if err != nil {
		return err
	}

	return nil
}

// IsInterfaceNil returns true if underlying object is nil
func (se *stateExport) IsInterfaceNil() bool {
	return se == nil
}
