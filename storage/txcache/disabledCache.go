package txcache

import (
	"github.com/ElrondNetwork/elrond-go/storage"
)

var _ storage.Cacher = (*DisabledCache)(nil)

// DisabledCache represents a disabled cache
type DisabledCache struct {
}

// NewDisabledCache creates a new disabled cache
func NewDisabledCache() *DisabledCache {
	return &DisabledCache{}
}

// AddTx does nothing
func (cache *DisabledCache) AddTx(_ *WrappedTransaction) (ok bool, added bool) {
	return false, false
}

// GetByTxHash returns no transaction
func (cache *DisabledCache) GetByTxHash(_ []byte) (*WrappedTransaction, bool) {
	return nil, false
}

// SelectTransactions returns an empty slice
func (cache *DisabledCache) SelectTransactions(_ int, _ int) []*WrappedTransaction {
	return make([]*WrappedTransaction, 0)
}

// RemoveTxByHash does nothing
func (cache *DisabledCache) RemoveTxByHash(_ []byte) bool {
	return false
}

// RemoveTxBulk does nothing
func (cache *DisabledCache) RemoveTxBulk(_ [][]byte) int {
	return 0
}

// Len returns zero
func (cache *DisabledCache) Len() int {
	return 0
}

// NumBytes returns zero
func (cache *DisabledCache) NumBytes() int {
	return 0
}

// ForEachTransaction does nothing
func (cache *DisabledCache) ForEachTransaction(_ ForEachTransaction) {
}

// Clear does nothing
func (cache *DisabledCache) Clear() {
}

// Put does nothing
func (cache *DisabledCache) Put(_ []byte, _ interface{}, _ int) (evicted bool) {
	return false
}

// Get returns no transaction
func (cache *DisabledCache) Get(_ []byte) (value interface{}, ok bool) {
	return nil, false
}

// Has returns false
func (cache *DisabledCache) Has(_ []byte) bool {
	return false
}

// Peek returns no transaction
func (cache *DisabledCache) Peek(_ []byte) (value interface{}, ok bool) {
	return nil, false
}

// HasOrAdd returns false, does nothing
func (cache *DisabledCache) HasOrAdd(_ []byte, _ interface{}, _ int) (has, added bool) {
	return false, false
}

// Remove does nothing
func (cache *DisabledCache) Remove(_ []byte) {
}

// Keys returns an empty slice
func (cache *DisabledCache) Keys() [][]byte {
	return make([][]byte, 0)
}

// MaxSize returns zero
func (cache *DisabledCache) MaxSize() int {
	return 0
}

// RegisterHandler does nothing
func (cache *DisabledCache) RegisterHandler(func(key []byte, value interface{}), string) {
}

// RegisterHandler does nothing
func (cache *DisabledCache) UnRegisterHandler(string) {
}

// NotifyAccountNonce does nothing
func (cache *DisabledCache) NotifyAccountNonce(_ []byte, _ uint64) {
}

// ImmunizeTxsAgainstEviction does nothing
func (cache *DisabledCache) ImmunizeTxsAgainstEviction(_ [][]byte) {
}

// Diagnose does nothing
func (cache *DisabledCache) Diagnose(_ bool) {
}

// IsInterfaceNil returns true if there is no value under the interface
func (cache *DisabledCache) IsInterfaceNil() bool {
	return cache == nil
}
