package trieExport

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/genesis"
)

const (
	numOfTokens      = 4
	accountTypeIndex = 3
	shardIdIndex     = 2
)

var log = logger.GetOrCreate("update/genesis")

type trieExport struct {
	exportFolder             string
	shardCoordinator         sharding.Coordinator
	marshalizer              marshal.Marshalizer
	hardforkStorer           update.HardforkStorer
	validatorPubKeyConverter core.PubkeyConverter
	addressPubKeyConverter   core.PubkeyConverter
	genesisNodesSetupHandler update.GenesisNodesSetupHandler
}

// NewTrieExport creates a new trieExport structure
func NewTrieExport(
	exportFolder string,
	shardCoordinator sharding.Coordinator,
	marshalizer marshal.Marshalizer,
	hardforkStorer update.HardforkStorer,
	validatorPubKeyConverter core.PubkeyConverter,
	addressPubKeyConverter core.PubkeyConverter,
	genesisNodesSetupHandler update.GenesisNodesSetupHandler,
) (*trieExport, error) {
	if len(exportFolder) == 0 {
		return nil, update.ErrEmptyExportFolderPath
	}
	if check.IfNil(shardCoordinator) {
		return nil, data.ErrNilShardCoordinator
	}
	if check.IfNil(marshalizer) {
		return nil, data.ErrNilMarshalizer
	}
	if check.IfNil(hardforkStorer) {
		return nil, update.ErrNilHardforkStorer
	}
	if check.IfNil(validatorPubKeyConverter) {
		return nil, fmt.Errorf("%w for validators", update.ErrNilPubKeyConverter)
	}
	if check.IfNil(addressPubKeyConverter) {
		return nil, fmt.Errorf("%w for address", update.ErrNilPubKeyConverter)
	}
	if check.IfNil(genesisNodesSetupHandler) {
		return nil, update.ErrNilGenesisNodesSetupHandler
	}

	return &trieExport{
		exportFolder:             exportFolder,
		shardCoordinator:         shardCoordinator,
		marshalizer:              marshalizer,
		hardforkStorer:           hardforkStorer,
		validatorPubKeyConverter: validatorPubKeyConverter,
		addressPubKeyConverter:   addressPubKeyConverter,
		genesisNodesSetupHandler: genesisNodesSetupHandler,
	}, nil
}

// ExportValidatorTrie exports the validator info from the validator trie
func (te *trieExport) ExportValidatorTrie(trie data.Trie, ctx context.Context) error {
	log.Debug("started validator trie export")
	rootHash, err := trie.RootHash()
	if err != nil {
		return err
	}

	leavesChannel, err := trie.GetAllLeavesOnChannel(rootHash, ctx)
	if err != nil {
		return err
	}

	var validatorData map[uint32][]*state.ValidatorInfo
	validatorData, err = getValidatorDataFromLeaves(leavesChannel, te.shardCoordinator, te.marshalizer)
	if err != nil {
		return err
	}

	nodesSetupFilePath := filepath.Join(te.exportFolder, core.NodesSetupJsonFileName)
	err = te.exportNodesSetupJson(validatorData)
	if err != nil {
		return fmt.Errorf("%w, hardfork nodesSetup.json not exported, file path %s", err, nodesSetupFilePath)
	}

	log.Debug("hardfork nodesSetup.json exported successfully", "file path", nodesSetupFilePath)
	return nil
}

// ExportMainTrie exports the main trie, and returns the root hashes for the data tries
func (te *trieExport) ExportMainTrie(key string, trie data.Trie, ctx context.Context) ([][]byte, error) {
	leavesChannel, accType, shId, identifier, err := te.getExportLeavesParameters(key, trie, ctx)
	if err != nil {
		return nil, err
	}

	return te.exportAccountLeaves(leavesChannel, accType, shId, identifier)
}

// ExportDataTrie exports the given data trie
func (te *trieExport) ExportDataTrie(key string, trie data.Trie, ctx context.Context) error {
	leavesChannel, accType, shId, identifier, err := te.getExportLeavesParameters(key, trie, ctx)
	if err != nil {
		return err
	}

	return te.exportDataTries(leavesChannel, accType, shId, identifier)
}

func (te *trieExport) getExportLeavesParameters(
	key string,
	trie data.Trie,
	ctx context.Context,
) (chan core.KeyValueHolder, genesis.Type, uint32, string, error) {
	identifier := "trie@" + key

	accType, shId, err := getTrieTypeAndShId(identifier)
	if err != nil {
		return nil, 0, 0, "", err
	}

	rootHash, err := trie.RootHash()
	if err != nil {
		return nil, 0, 0, "", err
	}

	leavesChannel, err := trie.GetAllLeavesOnChannel(rootHash, ctx)
	if err != nil {
		return nil, 0, 0, "", err
	}

	if shId > te.shardCoordinator.NumberOfShards() && shId != core.MetachainShardId {
		return nil, 0, 0, "", sharding.ErrInvalidShardId
	}

	rootHashKey := createRootHashKey(key)

	err = te.hardforkStorer.Write(identifier, []byte(rootHashKey), rootHash)
	if err != nil {
		return nil, 0, 0, "", err
	}

	log.Debug("exporting trie",
		"identifier", identifier,
		"root hash", rootHash,
	)

	return leavesChannel, accType, shId, identifier, nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (te *trieExport) IsInterfaceNil() bool {
	return te == nil
}

// getTrieTypeAndShId returns the type and shard Id for a given account according to the saved key
func getTrieTypeAndShId(key string) (genesis.Type, uint32, error) {
	splitString := strings.Split(key, "@")
	if len(splitString) < numOfTokens {
		return genesis.UserAccount, 0, update.ErrUnknownType
	}

	accTypeInt64, err := strconv.ParseInt(splitString[accountTypeIndex], 10, 0)
	if err != nil {
		return genesis.UserAccount, 0, err
	}
	accType := genesis.Type(accTypeInt64)

	shId, err := strconv.ParseInt(splitString[shardIdIndex], 10, 0)
	if err != nil {
		return genesis.UserAccount, 0, err
	}
	return accType, uint32(shId), nil
}

func (te *trieExport) exportDataTries(
	leavesChannel chan core.KeyValueHolder,
	accType genesis.Type,
	shId uint32,
	identifier string,
) error {
	for leaf := range leavesChannel {
		keyToExport := createAccountKey(accType, shId, leaf.Key())
		err := te.hardforkStorer.Write(identifier, []byte(keyToExport), leaf.Value())
		if err != nil {
			return err
		}
	}

	err := te.hardforkStorer.FinishedIdentifier(identifier)
	if err != nil {
		return err
	}

	return nil
}

func (te *trieExport) exportAccountLeaves(
	leavesChannel chan core.KeyValueHolder,
	accType genesis.Type,
	shId uint32,
	identifier string,
) ([][]byte, error) {
	rootHashes := make([][]byte, 0)
	for leaf := range leavesChannel {
		keyToExport := createAccountKey(accType, shId, leaf.Key())
		err := te.hardforkStorer.Write(identifier, []byte(keyToExport), leaf.Value())
		if err != nil {
			return nil, err
		}

		account := state.NewEmptyUserAccount()
		err = te.marshalizer.Unmarshal(account, leaf.Value())
		if err != nil {
			log.Trace("this must be a leaf with code", "err", err)
			continue
		}

		if len(account.RootHash) > 0 {
			rootHashes = append(rootHashes, account.RootHash)
		}
	}

	err := te.hardforkStorer.FinishedIdentifier(identifier)
	if err != nil {
		return nil, err
	}

	return rootHashes, nil
}

func (te *trieExport) exportNodesSetupJson(validators map[uint32][]*state.ValidatorInfo) error {
	acceptedListsForExport := []core.PeerType{core.EligibleList, core.WaitingList, core.JailedList}
	initialNodes := make([]*sharding.InitialNode, 0)

	for _, validatorsInShard := range validators {
		for _, validator := range validatorsInShard {
			if shouldExportValidator(validator, acceptedListsForExport) {
				initialNodes = append(initialNodes, &sharding.InitialNode{
					PubKey:        te.validatorPubKeyConverter.Encode(validator.GetPublicKey()),
					Address:       te.addressPubKeyConverter.Encode(validator.GetRewardAddress()),
					InitialRating: validator.GetRating(),
				})
			}
		}
	}

	sort.SliceStable(initialNodes, func(i, j int) bool {
		return strings.Compare(initialNodes[i].PubKey, initialNodes[j].PubKey) < 0
	})

	genesisNodesSetupHandler := te.genesisNodesSetupHandler
	nodesSetup := &sharding.NodesSetup{
		StartTime:                   genesisNodesSetupHandler.GetStartTime(),
		RoundDuration:               genesisNodesSetupHandler.GetRoundDuration(),
		ConsensusGroupSize:          genesisNodesSetupHandler.GetShardConsensusGroupSize(),
		MinNodesPerShard:            genesisNodesSetupHandler.MinNumberOfShardNodes(),
		ChainID:                     genesisNodesSetupHandler.GetChainId(),
		MinTransactionVersion:       genesisNodesSetupHandler.GetMinTransactionVersion(),
		MetaChainConsensusGroupSize: genesisNodesSetupHandler.GetMetaConsensusGroupSize(),
		MetaChainMinNodes:           genesisNodesSetupHandler.MinNumberOfMetaNodes(),
		Hysteresis:                  genesisNodesSetupHandler.GetHysteresis(),
		Adaptivity:                  genesisNodesSetupHandler.GetAdaptivity(),
		InitialNodes:                initialNodes,
	}

	nodesSetupBytes, err := json.MarshalIndent(nodesSetup, "", "  ")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filepath.Join(te.exportFolder, core.NodesSetupJsonFileName), nodesSetupBytes, 0664)
}

// TODO: create a structure or use this function also in process/peer/process.go
func getValidatorDataFromLeaves(
	leavesChannel chan core.KeyValueHolder,
	shardCoordinator sharding.Coordinator,
	marshalizer marshal.Marshalizer,
) (map[uint32][]*state.ValidatorInfo, error) {

	validators := make(map[uint32][]*state.ValidatorInfo, shardCoordinator.NumberOfShards()+1)
	for i := uint32(0); i < shardCoordinator.NumberOfShards(); i++ {
		validators[i] = make([]*state.ValidatorInfo, 0)
	}
	validators[core.MetachainShardId] = make([]*state.ValidatorInfo, 0)

	for pa := range leavesChannel {
		peerAccount, err := unmarshalPeer(pa.Value(), marshalizer)
		if err != nil {
			return nil, err
		}

		currentShardId := peerAccount.GetShardId()
		validatorInfoData := peerAccountToValidatorInfo(peerAccount)
		validators[currentShardId] = append(validators[currentShardId], validatorInfoData)
	}

	return validators, nil
}

func unmarshalPeer(pa []byte, marshalizer marshal.Marshalizer) (state.PeerAccountHandler, error) {
	peerAccount := state.NewEmptyPeerAccount()
	err := marshalizer.Unmarshal(peerAccount, pa)
	if err != nil {
		return nil, err
	}
	return peerAccount, nil
}

func peerAccountToValidatorInfo(peerAccount state.PeerAccountHandler) *state.ValidatorInfo {
	return &state.ValidatorInfo{
		PublicKey:                  peerAccount.GetBLSPublicKey(),
		ShardId:                    peerAccount.GetShardId(),
		List:                       getActualList(peerAccount),
		Index:                      peerAccount.GetIndexInList(),
		TempRating:                 peerAccount.GetTempRating(),
		Rating:                     peerAccount.GetRating(),
		RewardAddress:              peerAccount.GetRewardAddress(),
		LeaderSuccess:              peerAccount.GetLeaderSuccessRate().NumSuccess,
		LeaderFailure:              peerAccount.GetLeaderSuccessRate().NumFailure,
		ValidatorSuccess:           peerAccount.GetValidatorSuccessRate().NumSuccess,
		ValidatorFailure:           peerAccount.GetValidatorSuccessRate().NumFailure,
		TotalLeaderSuccess:         peerAccount.GetTotalLeaderSuccessRate().NumSuccess,
		TotalLeaderFailure:         peerAccount.GetTotalLeaderSuccessRate().NumFailure,
		TotalValidatorSuccess:      peerAccount.GetTotalValidatorSuccessRate().NumSuccess,
		TotalValidatorFailure:      peerAccount.GetTotalValidatorSuccessRate().NumFailure,
		NumSelectedInSuccessBlocks: peerAccount.GetNumSelectedInSuccessBlocks(),
		AccumulatedFees:            big.NewInt(0).Set(peerAccount.GetAccumulatedFees()),
	}
}

func getActualList(peerAccount state.PeerAccountHandler) string {
	savedList := peerAccount.GetList()
	if peerAccount.GetUnStakedEpoch() == core.DefaultUnstakedEpoch {
		if savedList == string(core.InactiveList) {
			return string(core.JailedList)
		}
		return savedList
	}
	if savedList == string(core.InactiveList) {
		return savedList
	}

	return string(core.LeavingList)
}

func shouldExportValidator(validator *state.ValidatorInfo, allowedLists []core.PeerType) bool {
	validatorList := validator.GetList()

	for _, list := range allowedLists {
		if validatorList == string(list) {
			return true
		}
	}

	return false
}

// CreateAccountKey creates a key for an account according to its type, shard ID and address
func createAccountKey(accType genesis.Type, shId uint32, address []byte) string {
	key := createTrieIdentifier(shId, accType)
	return key + "@" + hex.EncodeToString(address)
}

// CreateTrieIdentifier creates a trie identifier according to trie type and shard id
func createTrieIdentifier(shID uint32, accountType genesis.Type) string {
	return fmt.Sprint("tr", "@", shID, "@", accountType)
}

// CreateRootHashKey creates a key of type roothash for a given trie identifier
func createRootHashKey(trieIdentifier string) string {
	return "rt" + "@" + hex.EncodeToString([]byte(trieIdentifier))
}
