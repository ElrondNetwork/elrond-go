package state

import (
	"encoding/hex"
	"errors"
)

// ErrMissingTrie is an error-compatible struct holding the root hash of the trie that is missing
type ErrMissingTrie struct {
	rootHash []byte
}

//------- ErrMissingTrie

// NewErrMissingTrie  returns a new instantiated struct
func NewErrMissingTrie(rootHash []byte) *ErrMissingTrie {
	return &ErrMissingTrie{rootHash: rootHash}
}

// Error returns the error as string
func (e *ErrMissingTrie) Error() string {
	return "trie was not found for hash " + hex.EncodeToString(e.rootHash)
}

// ErrNilAccountsAdapter defines the error when trying to revert on nil accounts
var ErrNilAccountsAdapter = errors.New("nil AccountsAdapter")

// ErrNilAddress defines the error when trying to work with a nil address
var ErrNilAddress = errors.New("nil address")

// ErrEmptyAddress defines the error when trying to work with an empty address
var ErrEmptyAddress = errors.New("empty Address")

// ErrNilTrie signals that a trie is nil and no operation can be made
var ErrNilTrie = errors.New("trie is nil")

// ErrNilHasher signals that an operation has been attempted to or with a nil hasher implementation
var ErrNilHasher = errors.New("nil hasher")

// ErrNilMarshalizer signals that an operation has been attempted to or with a nil marshalizer implementation
var ErrNilMarshalizer = errors.New("nil marshalizer")

// ErrNegativeValue signals that an operation has been attempted with a negative value
var ErrNegativeValue = errors.New("negative values are not permited")

// ErrNilAccountFactory signals that a nil account factory was provided
var ErrNilAccountFactory = errors.New("account factory is nil")

// ErrNilUpdater signals that a nil updater has been provided
var ErrNilUpdater = errors.New("updater is nil")

// ErrNilAccountHandler signals that a nil account wrapper was provided
var ErrNilAccountHandler = errors.New("account wrapper is nil")

// ErrNilShardCoordinator signals that nil shard coordinator was provided
var ErrNilShardCoordinator = errors.New("shard coordinator is nil")

// ErrWrongTypeAssertion signals that a wrong type assertion occurred
var ErrWrongTypeAssertion = errors.New("wrong type assertion")

// ErrNilTrackableDataTrie signals that a nil trackable data trie has been provided
var ErrNilTrackableDataTrie = errors.New("nil trackable data trie")

// ErrAccNotFound signals that account was not found in state trie
var ErrAccNotFound = errors.New("account was not found")

// ErrUnknownShardId signals that shard id is not valid
var ErrUnknownShardId = errors.New("shard id is not valid")

//ErrBech32ConvertError signals that conversion the 5bit alphabet to 8bit failed
var ErrBech32ConvertError = errors.New("can't convert bech32 string")

// ErrNilBLSPublicKey signals that the provided BLS public key is nil
var ErrNilBLSPublicKey = errors.New("bls public key is nil")

// ErrNilOrEmptyDataTrieUpdates signals that there are no data trie updates
var ErrNilOrEmptyDataTrieUpdates = errors.New("no data trie updates")

// ErrOperationNotPermitted signals that operation is not permitted
var ErrOperationNotPermitted = errors.New("operation in account not permitted")

// ErrInvalidAddressLength signals that address length is invalid
var ErrInvalidAddressLength = errors.New("invalid address length")

// ErrInsufficientFunds signals the funds are insufficient for the move balance operation but the
// transaction fee is covered by the current balance
var ErrInsufficientFunds = errors.New("insufficient funds")

// ErrNilStorageManager signals that nil storage manager has been provided
var ErrNilStorageManager = errors.New("nil storage manager")

// ErrNilRequestHandler signals that nil request handler has been provided
var ErrNilRequestHandler = errors.New("nil request handler")

// ErrNilCacher signals that nil cacher has been provided
var ErrNilCacher = errors.New("nil cacher")

// ErrSnapshotValueOutOfBounds signals that the snapshot value is out of bounds
var ErrSnapshotValueOutOfBounds = errors.New("snapshot value out of bounds")

// ErrWrongSize signals that a wrong size occurred
var ErrWrongSize = errors.New("wrong size")

// ErrInvalidErdAddress signals that the provided address is not an ERD address
var ErrInvalidErdAddress = errors.New("invalid ERD address")

// ErrInvalidPubkeyConverterType signals that the provided pubkey converter type is invalid
var ErrInvalidPubkeyConverterType = errors.New("invalid pubkey converter type")

// ErrNilMapOfHashes signals that the provided map of hashes is nil
var ErrNilMapOfHashes = errors.New("nil map of hashes")

// ErrInvalidRootHash signals that the provided root hash is invalid
var ErrInvalidRootHash = errors.New("invalid root hash")

// ErrInvalidMaxHardCapForMissingNodes signals that the maximum hardcap value for missing nodes is invalid
var ErrInvalidMaxHardCapForMissingNodes = errors.New("invalid max hardcap for missing nodes")

// ErrNilTrieExporter signals that nil trie exporter has been provided
var ErrNilTrieExporter = errors.New("nil trie exporter")
