package txsimulator

import (
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
)

// readOnlyAccountsDB is a wrapper over an accounts db which works read-only. write operation are disabled
type readOnlyAccountsDB struct {
	originalAccounts state.AccountsAdapter
}

// NewReadOnlyAccountsDB returns a new instance of readOnlyAccountsDB
func NewReadOnlyAccountsDB(accountsDB state.AccountsAdapter) (*readOnlyAccountsDB, error) {
	if check.IfNil(accountsDB) {
		return nil, ErrNilAccountsAdapter
	}

	return &readOnlyAccountsDB{originalAccounts: accountsDB}, nil
}

// GetCode returns the code for the given account
func (w *readOnlyAccountsDB) GetCode(codeHash []byte) []byte {
	return w.originalAccounts.GetCode(codeHash)
}

// GetExistingAccount will call the original accounts' function with the same name
func (w *readOnlyAccountsDB) GetExistingAccount(address []byte) (vmcommon.AccountHandler, error) {
	return w.originalAccounts.GetExistingAccount(address)
}

// LoadAccount will call the original accounts' function with the same name
func (w *readOnlyAccountsDB) LoadAccount(address []byte) (vmcommon.AccountHandler, error) {
	return w.originalAccounts.LoadAccount(address)
}

// SaveAccount won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) SaveAccount(_ vmcommon.AccountHandler) error {
	return nil
}

// RemoveAccount won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) RemoveAccount(_ []byte) error {
	return nil
}

// Commit won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) Commit() ([]byte, error) {
	return nil, nil
}

// JournalLen will call the original accounts' function with the same name
func (w *readOnlyAccountsDB) JournalLen() int {
	return w.originalAccounts.JournalLen()
}

// RevertToSnapshot won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) RevertToSnapshot(_ int) error {
	return nil
}

// GetNumCheckpoints will call the original accounts' function with the same name
func (w *readOnlyAccountsDB) GetNumCheckpoints() uint32 {
	return w.originalAccounts.GetNumCheckpoints()
}

// RootHash will call the original accounts' function with the same name
func (w *readOnlyAccountsDB) RootHash() ([]byte, error) {
	return w.originalAccounts.RootHash()
}

// RecreateTrie won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) RecreateTrie(_ []byte) error {
	return nil
}

// PruneTrie won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) PruneTrie(_ []byte, _ data.TriePruningIdentifier) {
}

// CancelPrune won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) CancelPrune(_ []byte, _ data.TriePruningIdentifier) {
}

// SnapshotState won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) SnapshotState(_ []byte) {
}

// SetStateCheckpoint won't do anything as write operations are disabled on this component
func (w *readOnlyAccountsDB) SetStateCheckpoint(_ []byte) {
}

// IsPruningEnabled will call the original accounts' function with the same name
func (w *readOnlyAccountsDB) IsPruningEnabled() bool {
	return w.originalAccounts.IsPruningEnabled()
}

// GetAllLeaves will call the original accounts' function with the same name
func (w *readOnlyAccountsDB) GetAllLeaves(rootHash []byte) (chan core.KeyValueHolder, error) {
	return w.originalAccounts.GetAllLeaves(rootHash)
}

// RecreateAllTries will return an error which indicates that this operation is not supported
func (w *readOnlyAccountsDB) RecreateAllTries(_ []byte) (map[string]data.Trie, error) {
	return nil, nil
}

// GetTrie will return an error which indicates that this operation is not supported
func (w *readOnlyAccountsDB) GetTrie(_ []byte) (data.Trie, error) {
	return nil, nil
}

// Close will handle the closing of the underlying components
func (w *readOnlyAccountsDB) Close() error {
	return w.originalAccounts.Close()
}

// IsInterfaceNil returns true if there is no value under the interface
func (w *readOnlyAccountsDB) IsInterfaceNil() bool {
	return w == nil
}
