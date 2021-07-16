package trie

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
)

var log = logger.GetOrCreate("trie")

var _ dataRetriever.TrieDataGetter = (*patriciaMerkleTrie)(nil)

const (
	extension = iota
	leaf
	branch
)

// EmptyTrieHash returns the value with empty trie hash
var EmptyTrieHash = make([]byte, 32)

type patriciaMerkleTrie struct {
	root node

	trieStorage  data.StorageManager
	marshalizer  marshal.Marshalizer
	hasher       hashing.Hasher
	mutOperation sync.RWMutex

	oldHashes            [][]byte
	oldRoot              []byte
	maxTrieLevelInMemory uint
	chanClose            chan struct{}
}

// NewTrie creates a new Patricia Merkle Trie
func NewTrie(
	trieStorage data.StorageManager,
	msh marshal.Marshalizer,
	hsh hashing.Hasher,
	maxTrieLevelInMemory uint,
) (*patriciaMerkleTrie, error) {
	if check.IfNil(trieStorage) {
		return nil, ErrNilTrieStorage
	}
	if check.IfNil(msh) {
		return nil, ErrNilMarshalizer
	}
	if check.IfNil(hsh) {
		return nil, ErrNilHasher
	}
	if maxTrieLevelInMemory == 0 {
		return nil, ErrInvalidLevelValue
	}
	log.Trace("created new trie", "max trie level in memory", maxTrieLevelInMemory)

	return &patriciaMerkleTrie{
		trieStorage:          trieStorage,
		marshalizer:          msh,
		hasher:               hsh,
		oldHashes:            make([][]byte, 0),
		oldRoot:              make([]byte, 0),
		maxTrieLevelInMemory: maxTrieLevelInMemory,
		chanClose:            make(chan struct{}),
	}, nil
}

// Get starts at the root and searches for the given key.
// If the key is present in the tree, it returns the corresponding value
func (tr *patriciaMerkleTrie) Get(key []byte) ([]byte, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	if tr.root == nil {
		return nil, nil
	}
	hexKey := keyBytesToHex(key)

	val, err := tr.root.tryGet(hexKey, tr.trieStorage.Database())
	if err != nil {
		err = fmt.Errorf("trie get error: %w, for key %v", err, hex.EncodeToString(key))
		return nil, err
	}

	return val, nil
}

// Update updates the value at the given key.
// If the key is not in the trie, it will be added.
// If the value is empty, the key will be removed from the trie
func (tr *patriciaMerkleTrie) Update(key, value []byte) error {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	log.Trace("update trie", "key", hex.EncodeToString(key), "val", hex.EncodeToString(value))

	hexKey := keyBytesToHex(key)
	newLn, err := newLeafNode(hexKey, value, tr.marshalizer, tr.hasher)
	if err != nil {
		return err
	}

	var newRoot node
	var oldHashes [][]byte
	if len(value) != 0 {
		if tr.root == nil {
			newRoot, err = newLeafNode(hexKey, value, tr.marshalizer, tr.hasher)
			if err != nil {
				return err
			}

			tr.root = newRoot
			return nil
		}

		if !tr.root.isDirty() {
			tr.oldRoot = tr.root.getHash()
		}

		newRoot, oldHashes, err = tr.root.insert(newLn, tr.trieStorage.Database())
		if err != nil {
			return err
		}

		if check.IfNil(newRoot) {
			return nil
		}

		tr.root = newRoot
		tr.oldHashes = append(tr.oldHashes, oldHashes...)

		logArrayWithTrace("oldHashes after insert", "hash", oldHashes)
	} else {
		if tr.root == nil {
			return nil
		}

		if !tr.root.isDirty() {
			tr.oldRoot = tr.root.getHash()
		}

		_, newRoot, oldHashes, err = tr.root.delete(hexKey, tr.trieStorage.Database())
		if err != nil {
			return err
		}
		tr.root = newRoot
		tr.oldHashes = append(tr.oldHashes, oldHashes...)

		logArrayWithTrace("oldHashes after delete", "hash", oldHashes)
	}

	return nil
}

// Delete removes the node that has the given key from the tree
func (tr *patriciaMerkleTrie) Delete(key []byte) error {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	hexKey := keyBytesToHex(key)
	if tr.root == nil {
		return nil
	}

	if !tr.root.isDirty() {
		tr.oldRoot = tr.root.getHash()
	}

	_, newRoot, oldHashes, err := tr.root.delete(hexKey, tr.trieStorage.Database())
	if err != nil {
		return err
	}
	tr.root = newRoot
	tr.oldHashes = append(tr.oldHashes, oldHashes...)

	return nil
}

// RootHash returns the hash of the root node
func (tr *patriciaMerkleTrie) RootHash() ([]byte, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	return tr.getRootHash()
}

func (tr *patriciaMerkleTrie) getRootHash() ([]byte, error) {
	if tr.root == nil {
		return EmptyTrieHash, nil
	}

	hash := tr.root.getHash()
	if hash != nil {
		return hash, nil
	}
	err := tr.root.setRootHash()
	if err != nil {
		return nil, err
	}
	return tr.root.getHash(), nil
}

// Commit adds all the dirty nodes to the database
func (tr *patriciaMerkleTrie) Commit() error {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	if tr.root == nil {
		return nil
	}
	if !tr.root.isDirty() {
		return nil
	}
	err := tr.root.setRootHash()
	if err != nil {
		return err
	}

	tr.oldRoot = make([]byte, 0)
	tr.oldHashes = make([][]byte, 0)

	if log.GetLevel() == logger.LogTrace {
		log.Trace("started committing trie", "trie", tr.root.getHash())
	}

	err = tr.root.commitDirty(0, tr.maxTrieLevelInMemory, tr.trieStorage.Database(), tr.trieStorage.Database())
	if err != nil {
		return err
	}

	return nil
}

// Recreate returns a new trie that has the given root hash and database
func (tr *patriciaMerkleTrie) Recreate(root []byte) (data.Trie, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	return tr.recreate(root)
}

func (tr *patriciaMerkleTrie) recreate(root []byte) (*patriciaMerkleTrie, error) {
	if emptyTrie(root) {
		return NewTrie(
			tr.trieStorage,
			tr.marshalizer,
			tr.hasher,
			tr.maxTrieLevelInMemory,
		)
	}

	newTr := tr.recreateFromMainDb(root)
	if !check.IfNil(newTr) {
		return newTr, nil
	}

	return tr.recreateFromSnapshotDb(root)
}

func (tr *patriciaMerkleTrie) recreateFromMainDb(rootHash []byte) *patriciaMerkleTrie {
	_, err := tr.trieStorage.Database().Get(rootHash)
	if err != nil {
		return nil
	}

	newTr, _, err := tr.recreateFromDb(rootHash, tr.trieStorage.Database(), tr.trieStorage)
	if err != nil {
		log.Warn("trie recreate error:", "error", err, "root", hex.EncodeToString(rootHash))
		return nil
	}

	return newTr
}

func (tr *patriciaMerkleTrie) recreateFromSnapshotDb(rootHash []byte) (*patriciaMerkleTrie, error) {
	db := tr.trieStorage.GetSnapshotThatContainsHash(rootHash)
	if db == nil {
		return nil, fmt.Errorf("%w for %s", ErrHashNotFound, hex.EncodeToString(rootHash))
	}
	defer db.DecreaseNumReferences()

	newTr, newRoot, err := tr.recreateFromDb(rootHash, db, tr.trieStorage)
	if err != nil {
		err = fmt.Errorf("trie recreate error: %w, for root %v", err, hex.EncodeToString(rootHash))
		return nil, err
	}

	err = newRoot.commitSnapshot(db, tr.trieStorage.Database(), nil)
	if err != nil {
		return nil, err
	}

	return newTr, nil
}

// String outputs a graphical view of the trie. Mainly used in tests/debugging
func (tr *patriciaMerkleTrie) String() string {
	writer := bytes.NewBuffer(make([]byte, 0))

	if tr.root == nil {
		_, _ = fmt.Fprintln(writer, "*** EMPTY TRIE ***")
	} else {
		tr.root.print(writer, 0, tr.trieStorage.Database())
	}

	return writer.String()
}

// IsInterfaceNil returns true if there is no value under the interface
func (tr *patriciaMerkleTrie) IsInterfaceNil() bool {
	return tr == nil
}

func emptyTrie(root []byte) bool {
	if len(root) == 0 {
		return true
	}
	if bytes.Equal(root, EmptyTrieHash) {
		return true
	}
	return false
}

// GetObsoleteHashes resets the oldHashes and oldRoot variables and returns the old hashes
func (tr *patriciaMerkleTrie) GetObsoleteHashes() [][]byte {
	tr.mutOperation.Lock()
	oldHashes := tr.oldHashes
	logArrayWithTrace("old trie hash", "hash", oldHashes)

	tr.mutOperation.Unlock()

	return oldHashes
}

// GetDirtyHashes returns all the dirty hashes from the trie
func (tr *patriciaMerkleTrie) GetDirtyHashes() (data.ModifiedHashes, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	if tr.root == nil {
		return nil, nil
	}

	err := tr.root.setRootHash()
	if err != nil {
		return nil, err
	}

	dirtyHashes := make(data.ModifiedHashes)
	err = tr.root.getDirtyHashes(dirtyHashes)
	if err != nil {
		return nil, err
	}

	logMapWithTrace("new trie hash", "hash", dirtyHashes)

	return dirtyHashes, nil
}

func (tr *patriciaMerkleTrie) recreateFromDb(rootHash []byte, db data.DBWriteCacher, tsm data.StorageManager) (*patriciaMerkleTrie, snapshotNode, error) {
	newTr, err := NewTrie(
		tsm,
		tr.marshalizer,
		tr.hasher,
		tr.maxTrieLevelInMemory,
	)
	if err != nil {
		return nil, nil, err
	}

	newRoot, err := getNodeFromDBAndDecode(rootHash, db, tr.marshalizer, tr.hasher)
	if err != nil {
		return nil, nil, err
	}

	newRoot.setGivenHash(rootHash)
	newTr.root = newRoot

	return newTr, newRoot, nil
}

func getDbThatContainsHash(trieStorage data.StorageManager, rootHash []byte) data.SnapshotDbHandler {
	db := trieStorage.GetSnapshotThatContainsHash(rootHash)
	if db != nil {
		return db
	}

	_, err := trieStorage.Database().Get(rootHash)
	if err != nil {
		return nil
	}

	return &snapshotDb{
		DBWriteCacher: trieStorage.Database(),
	}
}

// GetSerializedNode returns the serialized node (if existing) provided the node's hash
func (tr *patriciaMerkleTrie) GetSerializedNode(hash []byte) ([]byte, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	log.Trace("GetSerializedNode", "hash", hash)

	db := getDbThatContainsHash(tr.trieStorage, hash)
	if db == nil {
		return nil, ErrHashNotFound
	}
	defer db.DecreaseNumReferences()

	return db.Get(hash)
}

// GetSerializedNodes returns a batch of serialized nodes from the trie, starting from the given hash
func (tr *patriciaMerkleTrie) GetSerializedNodes(rootHash []byte, maxBuffToSend uint64) ([][]byte, uint64, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	log.Trace("GetSerializedNodes", "rootHash", rootHash)
	size := uint64(0)

	db := getDbThatContainsHash(tr.trieStorage, rootHash)
	if db == nil {
		return nil, 0, fmt.Errorf("%w for %s", ErrHashNotFound, hex.EncodeToString(rootHash))
	}
	defer db.DecreaseNumReferences()

	newTsm, err := NewTrieStorageManagerWithoutPruning(db)
	if err != nil {
		return nil, 0, err
	}

	newTr, _, err := tr.recreateFromDb(rootHash, db, newTsm)
	if err != nil {
		return nil, 0, err
	}

	it, err := NewIterator(newTr)
	if err != nil {
		return nil, 0, err
	}

	encNode, err := it.MarshalizedNode()
	if err != nil {
		return nil, 0, err
	}

	nodes := make([][]byte, 0)
	nodes = append(nodes, encNode)
	size += uint64(len(encNode))

	for it.HasNext() {
		err = it.Next()
		if err != nil {
			return nil, 0, err
		}

		encNode, err = it.MarshalizedNode()
		if err != nil {
			return nil, 0, err
		}

		if size+uint64(len(encNode)) > maxBuffToSend {
			return nodes, 0, nil
		}
		nodes = append(nodes, encNode)
		size += uint64(len(encNode))
	}

	remainingSpace := maxBuffToSend - size

	return nodes, remainingSpace, nil
}

// GetAllLeavesOnChannel adds all the trie leaves to the given channel
func (tr *patriciaMerkleTrie) GetAllLeavesOnChannel(rootHash []byte) (chan core.KeyValueHolder, error) {
	leavesChannel := make(chan core.KeyValueHolder, 100)

	tr.mutOperation.RLock()
	newTrie, err := tr.recreate(rootHash)
	if err != nil {
		tr.mutOperation.RUnlock()
		close(leavesChannel)
		return nil, err
	}

	if check.IfNil(newTrie) || newTrie.root == nil {
		tr.mutOperation.RUnlock()
		close(leavesChannel)
		return leavesChannel, nil
	}

	tr.trieStorage.EnterPruningBufferingMode()
	tr.mutOperation.RUnlock()

	go func() {
		err = newTrie.root.getAllLeavesOnChannel(
			leavesChannel,
			[]byte{},
			tr.trieStorage.Database(),
			tr.marshalizer,
			tr.chanClose,
		)
		if err != nil {
			log.Error("could not get all trie leaves: ", "error", err)
		}

		tr.mutOperation.Lock()
		tr.trieStorage.ExitPruningBufferingMode()
		tr.mutOperation.Unlock()

		close(leavesChannel)
	}()

	return leavesChannel, nil
}

// GetAllHashes returns all the hashes from the trie
func (tr *patriciaMerkleTrie) GetAllHashes() ([][]byte, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	hashes := make([][]byte, 0)
	if tr.root == nil {
		return hashes, nil
	}

	err := tr.root.setRootHash()
	if err != nil {
		return nil, err
	}

	hashes, err = tr.root.getAllHashes(tr.trieStorage.Database())
	if err != nil {
		return nil, err
	}

	return hashes, nil
}

func logArrayWithTrace(message string, paramName string, hashes [][]byte) {
	if log.GetLevel() == logger.LogTrace {
		for _, hash := range hashes {
			log.Trace(message, paramName, hash)
		}
	}
}

func logMapWithTrace(message string, paramName string, hashes data.ModifiedHashes) {
	if log.GetLevel() == logger.LogTrace {
		for key := range hashes {
			log.Trace(message, paramName, []byte(key))
		}
	}
}

// GetProof computes a Merkle proof for the node that is present at the given key
func (tr *patriciaMerkleTrie) GetProof(key []byte) ([][]byte, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	if tr.root == nil {
		return nil, ErrNilNode
	}

	var proof [][]byte
	hexKey := keyBytesToHex(key)
	currentNode := tr.root

	err := currentNode.setRootHash()
	if err != nil {
		return nil, err
	}

	for {
		encodedNode, errGet := currentNode.getEncodedNode()
		if errGet != nil {
			return nil, errGet
		}
		proof = append(proof, encodedNode)

		currentNode, hexKey, errGet = currentNode.getNext(hexKey, tr.trieStorage.Database())
		if errGet != nil {
			return nil, errGet
		}

		if currentNode == nil {
			return proof, nil
		}
	}
}

// VerifyProof verifies the given Merkle proof
func (tr *patriciaMerkleTrie) VerifyProof(key []byte, proof [][]byte) (bool, error) {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	wantHash, err := tr.getRootHash()
	if err != nil {
		return false, err
	}

	key = keyBytesToHex(key)
	for _, encodedNode := range proof {
		if encodedNode == nil {
			return false, nil
		}

		hash := tr.hasher.Compute(string(encodedNode))
		if !bytes.Equal(wantHash, hash) {
			return false, nil
		}

		n, errDecode := decodeNode(encodedNode, tr.marshalizer, tr.hasher)
		if errDecode != nil {
			return false, errDecode
		}

		var proofVerified bool
		proofVerified, wantHash, key = n.getNextHashAndKey(key)
		if proofVerified {
			return true, nil
		}
	}

	return false, nil
}

// GetNumNodes will return the trie nodes statistics DTO
func (tr *patriciaMerkleTrie) GetNumNodes() data.NumNodesDTO {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	n := tr.root
	if check.IfNil(n) {
		return data.NumNodesDTO{}
	}

	return n.getNumNodes()
}

// GetStorageManager returns the storage manager for the trie
func (tr *patriciaMerkleTrie) GetStorageManager() data.StorageManager {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	return tr.trieStorage
}

// GetOldRoot returns the rootHash of the trie before the latest changes
func (tr *patriciaMerkleTrie) GetOldRoot() []byte {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	return tr.oldRoot
}

// Close stops all the active goroutines started by the trie
func (tr *patriciaMerkleTrie) Close() error {
	tr.mutOperation.Lock()
	defer tr.mutOperation.Unlock()

	if !isChannelClosed(tr.chanClose) {
		close(tr.chanClose)
	}

	return nil
}

func isChannelClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}
