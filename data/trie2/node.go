package trie2

import (
	"bytes"

	"github.com/ElrondNetwork/elrond-go-sandbox/hashing"
	"github.com/ElrondNetwork/elrond-go-sandbox/marshal"
)

const nrOfChildren = 17
const firstByte = 0

type node interface {
	getHash() []byte
	setHash(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error
	getCollapsed() node // a collapsed node is a node that instead of the children holds the children hashes
	isCollapsed() bool
	isDirty() bool
	getEncodedNode(marshal.Marshalizer) ([]byte, error)
	commit(dbw DBWriteCacher, marshalizer marshal.Marshalizer) error
	resolveCollapsed(pos byte, dbw DBWriteCacher, marshalizer marshal.Marshalizer) error
	hashNode(marshalizer marshal.Marshalizer, hasher hashing.Hasher) ([]byte, error)
	hashChildren(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error
	tryGet(key []byte, dbw DBWriteCacher, marshalizer marshal.Marshalizer) ([]byte, error)
	insert(n *leafNode, dbw DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error)
	delete(key []byte, dbw DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error)
	nextChild(previousState *nodeIteratorState, path []byte) (newState *nodeIteratorState, newPath []byte, ok bool)
	reduceNode(pos int) node
}

type branchNode struct {
	EncodedChildren [nrOfChildren][]byte
	children        [nrOfChildren]node
	hash            []byte
	dirty           bool
}

type extensionNode struct {
	Key          []byte
	EncodedChild []byte
	child        node
	hash         []byte
	dirty        bool
}

type leafNode struct {
	Key   []byte
	Value []byte
	hash  []byte
	dirty bool
}

func newExtensionNode(key []byte, child node) *extensionNode {
	return &extensionNode{
		Key:          key,
		EncodedChild: nil,
		child:        child,
		hash:         nil,
		dirty:        true,
	}
}

func newLeafNode(key, value []byte) *leafNode {
	return &leafNode{
		Key:   key,
		Value: value,
		hash:  nil,
		dirty: true,
	}
}

func (bn *branchNode) getHash() []byte {
	return bn.hash
}

func (en *extensionNode) getHash() []byte {
	return en.hash
}

func (ln *leafNode) getHash() []byte {
	return ln.hash
}

func (bn *branchNode) isDirty() bool {
	return bn.dirty
}

func (en *extensionNode) isDirty() bool {
	return en.dirty
}

func (ln *leafNode) isDirty() bool {
	return ln.dirty
}

func (bn *branchNode) getCollapsed() node {
	collapsed := bn.clone()
	for i := range bn.EncodedChildren {
		if bn.children[i] != nil {
			collapsed.EncodedChildren[i] = bn.children[i].getHash()
			collapsed.children[i] = nil
		}
	}
	return collapsed
}

func (en *extensionNode) getCollapsed() node {
	collapsed := en.clone()
	collapsed.EncodedChild = en.child.getHash()
	collapsed.child = nil
	return collapsed
}

func (ln *leafNode) getCollapsed() node {
	return ln
}

func (bn *branchNode) setHash(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error {
	if bn.isCollapsed() && !bn.isDirty() {
		hash, err := encodeNodeAndGetHash(bn, marshalizer, hasher)
		if err != nil {
			return err
		}
		bn.hash = hash
		return nil
	}
	hash, err := hashChildrenAndNode(bn, marshalizer, hasher)
	if err != nil {
		return err
	}
	bn.hash = hash
	return nil
}

func (en *extensionNode) setHash(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error {
	if en.isCollapsed() {
		hash, err := encodeNodeAndGetHash(en, marshalizer, hasher)
		if err != nil {
			return err
		}
		en.hash = hash
		return nil
	}
	hash, err := hashChildrenAndNode(en, marshalizer, hasher)
	if err != nil {
		return err
	}
	en.hash = hash
	return nil
}

func (ln *leafNode) setHash(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error {
	hash, err := hashChildrenAndNode(ln, marshalizer, hasher)
	if err != nil {
		return err
	}
	ln.hash = hash
	return nil
}

func hashChildrenAndNode(n node, marshalizer marshal.Marshalizer, hasher hashing.Hasher) ([]byte, error) {
	err := n.hashChildren(marshalizer, hasher)
	if err != nil {
		return nil, err
	}
	hashed, err := n.hashNode(marshalizer, hasher)
	if err != nil {
		return nil, err
	}
	return hashed, nil
}

func (bn *branchNode) hashChildren(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error {
	for i := 0; i < nrOfChildren; i++ {
		if bn.children[i] != nil {
			err := bn.children[i].setHash(marshalizer, hasher)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (en *extensionNode) hashChildren(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error {
	err := en.child.setHash(marshalizer, hasher)
	if err != nil {
		return err
	}
	return nil
}

func (ln *leafNode) hashChildren(marshalizer marshal.Marshalizer, hasher hashing.Hasher) error {
	return nil
}

func (bn *branchNode) hashNode(marshalizer marshal.Marshalizer, hasher hashing.Hasher) ([]byte, error) {
	for i := range bn.EncodedChildren {
		if bn.children[i] != nil {
			encChild, err := encodeNodeAndGetHash(bn.children[i], marshalizer, hasher)
			if err != nil {
				return nil, err
			}
			bn.EncodedChildren[i] = encChild
		}
	}
	return encodeNodeAndGetHash(bn, marshalizer, hasher)
}

func (en *extensionNode) hashNode(marshalizer marshal.Marshalizer, hasher hashing.Hasher) ([]byte, error) {
	encChild, err := encodeNodeAndGetHash(en.child, marshalizer, hasher)
	if err != nil {
		return nil, err
	}
	en.EncodedChild = encChild
	return encodeNodeAndGetHash(en, marshalizer, hasher)
}

func (ln *leafNode) hashNode(marshalizer marshal.Marshalizer, hasher hashing.Hasher) ([]byte, error) {
	return encodeNodeAndGetHash(ln, marshalizer, hasher)
}

func encodeNodeAndGetHash(n node, marshalizer marshal.Marshalizer, hasher hashing.Hasher) ([]byte, error) {
	encNode, err := n.getEncodedNode(marshalizer)
	if err != nil {
		return nil, err
	}
	hash := hasher.Compute(string(encNode))
	return hash, nil
}

func (bn *branchNode) commit(db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	for i := range bn.children {
		if bn.children[i] != nil {
			err := bn.children[i].commit(db, marshalizer)
			if err != nil {
				return err
			}
		}
	}
	if !bn.dirty {
		return nil
	}
	bn.dirty = false
	return encodeNodeAndCommitToDB(bn, db, marshalizer)
}

func (en *extensionNode) commit(db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	err := en.child.commit(db, marshalizer)
	if err != nil {
		return err
	}
	if !en.dirty {
		return nil
	}
	en.dirty = false
	return encodeNodeAndCommitToDB(en, db, marshalizer)
}

func (ln *leafNode) commit(db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	if !ln.dirty {
		return nil
	}
	ln.dirty = false
	return encodeNodeAndCommitToDB(ln, db, marshalizer)
}

func encodeNodeAndCommitToDB(n node, db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	key := n.getHash()
	n = n.getCollapsed()
	val, err := n.getEncodedNode(marshalizer)
	if err != nil {
		return err
	}
	err = db.Put(key, val)
	return err
}

func (bn *branchNode) getEncodedNode(marshalizer marshal.Marshalizer) ([]byte, error) {
	marshaledNode, err := marshalizer.Marshal(bn)
	if err != nil {
		return nil, err
	}
	marshaledNode = append(marshaledNode, branch)
	return marshaledNode, nil
}

func (en *extensionNode) getEncodedNode(marshalizer marshal.Marshalizer) ([]byte, error) {
	marshaledNode, err := marshalizer.Marshal(en)
	if err != nil {
		return nil, err
	}
	marshaledNode = append(marshaledNode, extension)
	return marshaledNode, nil
}

func (ln *leafNode) getEncodedNode(marshalizer marshal.Marshalizer) ([]byte, error) {
	marshaledNode, err := marshalizer.Marshal(ln)
	if err != nil {
		return nil, err
	}
	marshaledNode = append(marshaledNode, leaf)
	return marshaledNode, nil
}

func (en *extensionNode) resolveCollapsed(pos byte, db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	child, err := getNodeFromDBAndDecode(en.EncodedChild, db, marshalizer)
	if err != nil {
		return err
	}
	en.child = child
	return nil
}

func (bn *branchNode) resolveCollapsed(pos byte, db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	if childPosOutOfRange(pos) {
		return ErrChildPosOutOfRange
	}
	if bn.EncodedChildren[pos] != nil {
		child, err := getNodeFromDBAndDecode(bn.EncodedChildren[pos], db, marshalizer)
		if err != nil {
			return err
		}
		bn.children[pos] = child
	}
	return nil
}

func (ln *leafNode) resolveCollapsed(pos byte, db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	node, err := getNodeFromDBAndDecode(ln.Value, db, marshalizer)
	if err != nil {
		return err
	}
	if node, ok := node.(*leafNode); ok {
		*ln = *node
	}
	return nil
}

func getNodeFromDBAndDecode(n []byte, db DBWriteCacher, marshalizer marshal.Marshalizer) (node, error) {
	encChild, err := db.Get(n)
	if err != nil {
		return nil, err
	}
	node, err := decodeNode(encChild, marshalizer)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (bn *branchNode) isCollapsed() bool {
	for i := range bn.children {
		if bn.children[i] == nil && bn.EncodedChildren[i] != nil {
			return true
		}
	}
	return false
}

func (en *extensionNode) isCollapsed() bool {
	if en.child == nil && en.EncodedChild != nil {
		return true
	}
	return false
}

func (ln *leafNode) isCollapsed() bool {
	return false
}

func (bn *branchNode) tryGet(key []byte, db DBWriteCacher, marshalizer marshal.Marshalizer) (value []byte, err error) {
	childPos := key[firstByte]
	if childPosOutOfRange(childPos) {
		return nil, ErrChildPosOutOfRange
	}
	key, err = removeFirstByte(key)
	if err != nil {
		return nil, err
	}
	err = resolveIfCollapsed(bn, childPos, db, marshalizer)
	if err != nil {
		return nil, err
	}
	if bn.children[childPos] == nil {
		return nil, ErrNodeNotFound
	}
	value, err = bn.children[childPos].tryGet(key, db, marshalizer)
	return value, err
}

func (en *extensionNode) tryGet(key []byte, db DBWriteCacher, marshalizer marshal.Marshalizer) (value []byte, err error) {
	keyTooShort := len(key) < len(en.Key)
	if keyTooShort {
		return nil, ErrNodeNotFound
	}
	keysDontMatch := !bytes.Equal(en.Key, key[:len(en.Key)])
	if keysDontMatch {
		return nil, ErrNodeNotFound
	}
	key = key[len(en.Key):]
	err = resolveIfCollapsed(en, 0, db, marshalizer)
	if err != nil {
		return nil, err
	}
	value, err = en.child.tryGet(key, db, marshalizer)
	return value, err
}

func (ln *leafNode) tryGet(key []byte, db DBWriteCacher, marshalizer marshal.Marshalizer) (value []byte, err error) {
	if bytes.Equal(key, ln.Key) {
		return ln.Value, nil
	}
	return nil, ErrNodeNotFound
}

func resolveIfCollapsed(n node, pos byte, db DBWriteCacher, marshalizer marshal.Marshalizer) error {
	if n.isCollapsed() {
		err := n.resolveCollapsed(pos, db, marshalizer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bn *branchNode) insert(n *leafNode, db DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error) {
	var err error

	childPos := n.Key[firstByte]
	if childPosOutOfRange(childPos) {
		return false, nil, ErrChildPosOutOfRange
	}
	n.Key, err = removeFirstByte(n.Key)
	if err != nil {
		return false, nil, err
	}

	err = resolveIfCollapsed(bn, childPos, db, marshalizer)
	if err != nil {
		return false, nil, err
	}

	if bn.children[childPos] != nil {
		dirty, newNode, err := bn.children[childPos].insert(n, db, marshalizer)
		if !dirty || err != nil {
			return false, bn, err
		}
		bn.children[childPos] = newNode
		bn.dirty = dirty
		return true, bn, nil
	}
	bn.children[childPos] = newLeafNode(n.Key, n.Value)
	return true, bn, nil
}

func (en *extensionNode) insert(n *leafNode, db DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error) {
	err := resolveIfCollapsed(en, 0, db, marshalizer)
	if err != nil {
		return false, nil, err
	}
	keyMatchLen := prefixLen(n.Key, en.Key)

	// If the whole key matches, keep this extension node as is
	// and only update the value.
	if keyMatchLen == len(en.Key) {
		n.Key = n.Key[keyMatchLen:]
		dirty, newNode, err := en.child.insert(n, db, marshalizer)
		if !dirty || err != nil {
			return false, nil, err
		}
		return true, newExtensionNode(en.Key, newNode), nil
	}
	// Otherwise branch out at the index where they differ.
	branch := &branchNode{}
	branch.dirty = true
	oldChildPos := en.Key[keyMatchLen]
	newChildPos := n.Key[keyMatchLen]

	branch.children[oldChildPos] = newExtensionNode(en.Key[keyMatchLen+1:], en.child)
	n.Key = n.Key[keyMatchLen+1:]
	branch.children[newChildPos] = n

	if keyMatchLen == 0 {
		return true, branch, nil
	}
	return true, newExtensionNode(en.Key[:keyMatchLen], branch), nil
}

func (ln *leafNode) insert(n *leafNode, db DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error) {
	if bytes.Equal(n.Key, ln.Key) {
		ln.Value = n.Value
		ln.dirty = true
		return true, ln, nil
	}

	keyMatchLen := prefixLen(n.Key, ln.Key)
	branch := &branchNode{}
	branch.dirty = true
	oldChildPos := ln.Key[keyMatchLen]
	newChildPos := n.Key[keyMatchLen]

	branch.children[oldChildPos] = newLeafNode(ln.Key[keyMatchLen+1:], ln.Value)
	branch.children[newChildPos] = newLeafNode(n.Key[keyMatchLen+1:], n.Value)

	if keyMatchLen == 0 {
		return true, branch, nil
	}
	return true, newExtensionNode(ln.Key[:keyMatchLen], branch), nil
}

func (bn *branchNode) delete(key []byte, db DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error) {
	childPos := key[firstByte]
	key, err := removeFirstByte(key)
	if err != nil {
		return false, nil, err
	}
	err = resolveIfCollapsed(bn, childPos, db, marshalizer)
	if err != nil {
		return false, nil, err
	}
	dirty, newNode, err := bn.children[childPos].delete(key, db, marshalizer)
	if !dirty || err != nil {
		return false, nil, err
	}

	bn.hash = nil
	bn.children[childPos] = newNode
	if newNode == nil {
		bn.EncodedChildren[childPos] = nil
	}

	nrOfChildren, pos := getChildPosition(bn)

	if nrOfChildren == 1 {
		err = bn.resolveCollapsed(byte(pos), db, marshalizer)
		if err != nil {
			return false, nil, err
		}
		if childPos != 16 {
			newNode := bn.children[pos].reduceNode(pos)
			return true, newNode, nil
		}
		child := bn.children[pos]
		if child, ok := child.(*leafNode); ok {
			return true, newLeafNode([]byte{byte(pos)}, child.Value), nil
		}

	}

	bn.dirty = dirty
	return true, bn, nil
}

func (en *extensionNode) delete(key []byte, db DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error) {
	keyMatchLen := prefixLen(key, en.Key)
	if keyMatchLen < len(en.Key) {
		return false, en, nil // don't replace n on mismatch
	}
	err := resolveIfCollapsed(en, 0, db, marshalizer)
	if err != nil {
		return false, nil, err
	}

	dirty, newNode, err := en.child.delete(key[len(en.Key):], db, marshalizer)
	if !dirty || err != nil {
		return false, en, err
	}

	switch newNode := newNode.(type) {
	case *leafNode:
		return true, newLeafNode(concat(en.Key, newNode.Key...), newNode.Value), nil
	case *extensionNode:
		return true, newExtensionNode(concat(en.Key, newNode.Key...), newNode.child), nil
	default:
		return true, newExtensionNode(en.Key, newNode), nil
	}
}

func (ln *leafNode) delete(key []byte, db DBWriteCacher, marshalizer marshal.Marshalizer) (bool, node, error) {
	keyMatchLen := prefixLen(key, ln.Key)
	if keyMatchLen == len(key) {
		return true, nil, nil
	}
	return false, ln, nil
}

func (bn *branchNode) reduceNode(pos int) node {
	return newExtensionNode([]byte{byte(pos)}, bn.children[pos])
}

func (en *extensionNode) reduceNode(pos int) node {
	k := append([]byte{byte(pos)}, en.Key...)
	return newExtensionNode(k, en.child)
}

func (ln *leafNode) reduceNode(pos int) node {
	k := append([]byte{byte(pos)}, ln.Key...)
	return newLeafNode(k, ln.Value)
}

func getChildPosition(n *branchNode) (nrOfChildren int, childPos int) {
	for i := range &n.children {
		if n.children[i] != nil || n.EncodedChildren[i] != nil {
			nrOfChildren++
			childPos = i
		}
	}
	return
}

func (bn *branchNode) nextChild(previousState *nodeIteratorState, path []byte) (newState *nodeIteratorState, newPath []byte, ok bool) {
	for i := previousState.index + 1; i < len(bn.children); i++ {
		child := bn.children[i]
		if child != nil {
			hash := child.getHash()
			state := newIteratorState(hash, child, previousState.hash, len(path))
			newPath := append(path, byte(i))
			if child, ok := child.(*leafNode); ok {
				newPath = append(newPath, child.Key...)
			}
			previousState.index = i - 1
			return state, newPath, true
		}
	}
	return previousState, path, false
}

func (en *extensionNode) nextChild(previousState *nodeIteratorState, path []byte) (newState *nodeIteratorState, newPath []byte, ok bool) {
	if previousState.index < 0 {
		hash := en.child.getHash()
		state := newIteratorState(hash, en.child, previousState.hash, len(path))
		newPath := append(path, en.Key...)
		return state, newPath, true
	}
	return previousState, path, false
}

func (ln *leafNode) nextChild(previousState *nodeIteratorState, path []byte) (newState *nodeIteratorState, newPath []byte, ok bool) {
	return previousState, path, false
}

func (bn *branchNode) clone() *branchNode {
	nodeClone := *bn
	return &nodeClone
}

func (en *extensionNode) clone() *extensionNode {
	nodeClone := *en
	return &nodeClone
}

func removeFirstByte(val []byte) ([]byte, error) {
	if len(val) > 0 {
		return val[1:], nil
	}
	return nil, ErrValueTooShort
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func decodeNode(encNode []byte, marshalizer marshal.Marshalizer) (node, error) {
	if encNode == nil || len(encNode) < 1 {
		return nil, ErrInvalidEncoding
	}
	nodeType := encNode[len(encNode)-1]
	encNode = encNode[:len(encNode)-1]

	node, err := getEmptyNodeOfType(nodeType)
	if err != nil {
		return nil, err
	}
	err = marshalizer.Unmarshal(node, encNode)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func getEmptyNodeOfType(t byte) (node, error) {
	var decNode node
	switch t {
	case extension:
		decNode = &extensionNode{}
	case leaf:
		decNode = &leafNode{}
	case branch:
		decNode = &branchNode{}
	default:
		return nil, ErrInvalidNode
	}
	return decNode, nil
}

func childPosOutOfRange(pos byte) bool {
	if pos < 0 || pos >= nrOfChildren {
		return true
	}
	return false
}
