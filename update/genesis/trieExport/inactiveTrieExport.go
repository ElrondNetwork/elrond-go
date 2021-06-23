package trieExport

import (
	"context"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/marshal"
)

type inactiveTrieExport struct {
	marshalizer marshal.Marshalizer
}

// NewInactiveTrieExporter returns a new instance of inactiveTrieExport
func NewInactiveTrieExporter(marshalizer marshal.Marshalizer) (*inactiveTrieExport, error) {
	if check.IfNil(marshalizer) {
		return nil, data.ErrNilMarshalizer
	}

	return &inactiveTrieExport{marshalizer: marshalizer}, nil
}

// ExportValidatorTrie does nothing
func (ite *inactiveTrieExport) ExportValidatorTrie(_ data.Trie, _ context.Context) error {
	return nil
}

// ExportMainTrie exports nothing, but returns the root hashes for the data tries
func (ite *inactiveTrieExport) ExportMainTrie(_ string, trie data.Trie, ctx context.Context) ([][]byte, error) {
	mainRootHash, err := trie.RootHash()
	if err != nil {
		return nil, err
	}

	leavesChannel, err := trie.GetAllLeavesOnChannel(mainRootHash, ctx)
	if err != nil {
		return nil, err
	}

	rootHashes := make([][]byte, 0)
	for leaf := range leavesChannel {
		account := state.NewEmptyUserAccount()
		err = ite.marshalizer.Unmarshal(account, leaf.Value())
		if err != nil {
			log.Trace("this must be a leaf with code", "err", err)
			continue
		}

		if len(account.RootHash) > 0 {
			rootHashes = append(rootHashes, account.RootHash)
		}
	}

	return rootHashes, nil
}

// ExportDataTrie does nothing
func (ite *inactiveTrieExport) ExportDataTrie(_ string, _ data.Trie, _ context.Context) error {
	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (ite *inactiveTrieExport) IsInterfaceNil() bool {
	return ite == nil
}
