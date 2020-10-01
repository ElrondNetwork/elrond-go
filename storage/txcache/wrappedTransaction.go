package txcache

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ElrondNetwork/elrond-go/data"
)

// WrappedTransaction contains a transaction, its hash and extra information
type WrappedTransaction struct {
	Tx              data.TransactionHandler
	TxHash          []byte
	SenderShardID   uint32
	ReceiverShardID uint32
	Size            int64
}

func (wrappedTx *WrappedTransaction) String() string {
	return fmt.Sprintf("%s %d", string(wrappedTx.TxHash), wrappedTx.Tx.GetNonce())
}

func (wrappedTx *WrappedTransaction) sameAs(another *WrappedTransaction) bool {
	return bytes.Equal(wrappedTx.TxHash, another.TxHash)
}

// estimateTxGas returns an approximation for the necessary computation units (gas units)
func estimateTxGas(tx *WrappedTransaction) uint64 {
	gasLimit := tx.Tx.GetGasLimit()
	return gasLimit
}

// estimateTxFee returns an approximation for the cost of a transaction, in nano ERD
// TODO: switch to integer operations (as opposed to float operations).
// TODO: do not assume the order of magnitude of minGasPrice.
func estimateTxFee(tx *WrappedTransaction) uint64 {
	// In order to obtain the result as nano ERD (not as "atomic" 10^-18 ERD), we have to divide by 10^9
	// In order to have better precision, we divide the factors by 10^6, and 10^3 respectively
	gasLimit := float32(tx.Tx.GetGasLimit()) / 1000000
	gasPrice := float32(tx.Tx.GetGasPrice()) / 1000
	feeInNanoERD := gasLimit * gasPrice
	return uint64(feeInNanoERD)
}

// SortTransactionsBySenderAndNonce sorts the provided transactions and hashes simultaneously
func SortTransactionsBySenderAndNonce(transactions []*WrappedTransaction) {
	sorter := func(i, j int) bool {
		txI := transactions[i].Tx
		txJ := transactions[j].Tx

		delta := bytes.Compare(txI.GetSndAddr(), txJ.GetSndAddr())
		if delta == 0 {
			delta = int(txI.GetNonce()) - int(txJ.GetNonce())
		}

		return delta < 0
	}

	sort.Slice(transactions, sorter)
}

// GroupSortedTransactionsBySender sorts in-line by sender, then by nonce, then splits the slice into groups (one group per sender)
func GroupSortedTransactionsBySender(transactions []*WrappedTransaction) []groupOfTxs {
	groups := make([]groupOfTxs, 0)

	if len(transactions) == 0 {
		return groups
	}

	// First, obtain a sorted slice, then split into groups
	SortTransactionsBySenderAndNonce(transactions)

	firstTx := transactions[0]
	groupSender := firstTx.Tx.GetSndAddr()
	groupStart := 0

	for index, tx := range transactions {
		txSender := tx.Tx.GetSndAddr()
		if bytes.Equal(groupSender, txSender) {
			continue
		}

		groups = append(groups, groupOfTxs{groupSender, transactions[groupStart:index]})
		groupSender = txSender
		groupStart = index
	}

	// Handle last group
	groups = append(groups, groupOfTxs{groupSender, transactions[groupStart:]})
	return groups
}

type groupOfTxs struct {
	sender       []byte
	transactions []*WrappedTransaction
}
