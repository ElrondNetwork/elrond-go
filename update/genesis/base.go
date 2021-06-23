package genesis

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/rewardTx"
	"github.com/ElrondNetwork/elrond-go/data/smartContractResult"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
	"github.com/ElrondNetwork/elrond-go/update"
)

// EpochStartMetaBlockIdentifier is the constant which defines the export/import identifier for epoch start metaBlock
const EpochStartMetaBlockIdentifier = "epochStartMetaBlock"

// UnFinishedMetaBlocksIdentifier is the constant which defines the export/import identifier for unFinished metaBlocks
const UnFinishedMetaBlocksIdentifier = "unFinishedMetaBlocks"

// TransactionsIdentifier is the constant which defines the export/import identifier for transactions
const TransactionsIdentifier = "transactions"

// MiniBlocksIdentifier is the constant which defines the export/import identifier for miniBlocks
const MiniBlocksIdentifier = "miniBlocks"

// TrieIdentifier is the constant which defines the export/import identifier for tries
const TrieIdentifier = "trie"

// Type identifies the type of the export / import
type Type uint8

const (
	// Unknown is an export/import type which is not known by the system
	Unknown Type = iota
	// Transaction is the export/import type for pending transactions
	Transaction
	// SmartContractResult is the export/import type for pending smart contract results
	SmartContractResult
	// RewardTransaction is the export/import type for pending reward transaction
	RewardTransaction
	// MiniBlock is the export/import type for pending miniBlock
	MiniBlock
	// Header is the export/import type for pending headers
	Header
	// MetaHeader is the export/import type for pending meta headers
	MetaHeader
	// RootHash is the export/import type for byte array which has to be treated as rootHash
	RootHash
	// UserAccount is the export/import type for an account of type user account
	UserAccount
	// ValidatorAccount is the export/import type for peer account
	ValidatorAccount
	// DataTrie identifies the data trie kept under a specific account
	DataTrie
)

// atSep is a separator used for export and import to decipher needed types
const atSep = "@"
const accTypeIDX = 3
const shardIDIDX = 2

// NewObject creates an object according to the given type
func NewObject(objType Type) (interface{}, error) {
	switch objType {
	case Transaction:
		return &transaction.Transaction{}, nil
	case SmartContractResult:
		return &smartContractResult.SmartContractResult{}, nil
	case RewardTransaction:
		return &rewardTx.RewardTx{}, nil
	case MiniBlock:
		return &block.MiniBlock{}, nil
	case Header:
		return &block.Header{}, nil
	case MetaHeader:
		return &block.MetaBlock{}, nil
	case RootHash:
		return make([]byte, 0), nil
	}
	return nil, update.ErrUnknownType
}

// NewEmptyAccount returns a new account according to the given type
func NewEmptyAccount(accType Type, address []byte) (state.AccountHandler, error) {
	switch accType {
	case UserAccount:
		return state.NewUserAccount(address)
	case ValidatorAccount:
		return state.NewPeerAccount(address)
	case DataTrie:
		return nil, nil
	}
	return nil, update.ErrUnknownType
}

// GetTrieTypeAndShId returns the type and shard Id for a given account according to the saved key
func GetTrieTypeAndShId(key string) (Type, uint32, error) {
	splitString := strings.Split(key, atSep)
	if len(splitString) < 3 {
		return UserAccount, 0, update.ErrUnknownType
	}

	accTypeInt64, err := strconv.ParseInt(splitString[accTypeIDX], 10, 0)
	if err != nil {
		return UserAccount, 0, err
	}
	accType := Type(accTypeInt64)

	shId, err := strconv.ParseInt(splitString[shardIDIDX], 10, 0)
	if err != nil {
		return UserAccount, 0, err
	}
	return accType, uint32(shId), nil
}

func getTransactionKeyTypeAndHash(splitString []string) (Type, []byte, error) {
	if len(splitString) < 2 {
		return Unknown, nil, update.ErrUnknownType
	}

	decodedHash, err := hex.DecodeString(splitString[1])
	if err != nil {
		return Unknown, nil, update.ErrUnknownType
	}

	switch splitString[0] {
	case "nrm":
		return Transaction, decodedHash, nil
	case "scr":
		return SmartContractResult, decodedHash, nil
	case "rwd":
		return RewardTransaction, decodedHash, nil
	}

	return Unknown, nil, update.ErrUnknownType
}

func getTrieTypeAndHash(splitString []string) (Type, []byte, error) {
	if len(splitString) < 3 {
		return Unknown, nil, update.ErrUnknownType
	}

	accTypeInt64, err := strconv.ParseInt(splitString[1], 10, 0)
	if err != nil {
		return Unknown, nil, err
	}
	accType := Type(accTypeInt64)

	decodedHash, err := hex.DecodeString(splitString[2])
	if err != nil {
		return Unknown, nil, err
	}

	return accType, decodedHash, nil
}

// GetKeyTypeAndHash returns the type of the key by splitting it up and deciphering it
func GetKeyTypeAndHash(key string) (Type, []byte, error) {
	splitString := strings.Split(key, atSep)

	if len(splitString) < 2 {
		return Unknown, nil, update.ErrUnknownType
	}

	switch splitString[0] {
	case "meta":
		return getHeaderTypeAndHash(splitString)
	case "mb":
		return getMbTypeAndHash(splitString)
	case "tx":
		return getTransactionKeyTypeAndHash(splitString[1:])
	case "tr":
		return getTrieTypeAndHash(splitString[1:])
	case "rt":
		return RootHash, []byte(key), nil
	}

	return Unknown, nil, update.ErrUnknownType
}

func getHeaderTypeAndHash(splitString []string) (Type, []byte, error) {
	if len(splitString) < 3 {
		return Unknown, nil, update.ErrUnknownType
	}

	hash, err := hex.DecodeString(splitString[2])
	if err != nil {
		return Unknown, nil, err
	}

	return MetaHeader, hash, nil
}

func getMbTypeAndHash(splitString []string) (Type, []byte, error) {
	hash, err := hex.DecodeString(splitString[1])
	if err != nil {
		return Unknown, nil, err
	}

	return MiniBlock, hash, nil
}

// CreateVersionKey creates a version key from the given metaBlock
func CreateVersionKey(meta *block.MetaBlock, hash []byte) string {
	return "meta" + atSep + string(meta.ChainID) + atSep + hex.EncodeToString(hash)
}

// CreateTrieIdentifier creates a trie identifier according to trie type and shard id
func CreateTrieIdentifier(shID uint32, accountType Type) string {
	return fmt.Sprint("tr", atSep, shID, atSep, accountType)
}

// AddRootHashToIdentifier adds the roothash to the current identifier
func AddRootHashToIdentifier(identifier string, hash string) string {
	return identifier + atSep + hex.EncodeToString([]byte(hash))
}

// CreateMiniBlockKey returns a miniblock key
func CreateMiniBlockKey(key string) string {
	return "mb" + atSep + hex.EncodeToString([]byte(key))
}

// CreateTransactionKey create a transaction key according to its type
func CreateTransactionKey(key string, tx data.TransactionHandler) string {
	switch tx.(type) {
	case *transaction.Transaction:
		return "tx" + atSep + "nrm" + atSep + hex.EncodeToString([]byte(key))
	case *smartContractResult.SmartContractResult:
		return "tx" + atSep + "scr" + atSep + hex.EncodeToString([]byte(key))
	case *rewardTx.RewardTx:
		return "tx" + atSep + "rwd" + atSep + hex.EncodeToString([]byte(key))
	default:
		return "tx" + atSep + "ukw" + atSep + hex.EncodeToString([]byte(key))
	}
}
