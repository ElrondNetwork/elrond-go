package txcache

import (
	"fmt"
	"testing"

	"github.com/ElrondNetwork/elrond-go/data/transaction"
)

func ExampleSortTransactionsBySenderAndNonce() {
	txs := []*WrappedTransaction{
		{Tx: &transaction.Transaction{Nonce: 3, SndAddr: []byte("bbbb")}, TxHash: []byte("w")},
		{Tx: &transaction.Transaction{Nonce: 1, SndAddr: []byte("aaaa")}, TxHash: []byte("x")},
		{Tx: &transaction.Transaction{Nonce: 5, SndAddr: []byte("bbbb")}, TxHash: []byte("y")},
		{Tx: &transaction.Transaction{Nonce: 2, SndAddr: []byte("aaaa")}, TxHash: []byte("z")},
		{Tx: &transaction.Transaction{Nonce: 7, SndAddr: []byte("aabb")}, TxHash: []byte("t")},
		{Tx: &transaction.Transaction{Nonce: 6, SndAddr: []byte("aabb")}, TxHash: []byte("a")},
		{Tx: &transaction.Transaction{Nonce: 3, SndAddr: []byte("ffff")}, TxHash: []byte("b")},
		{Tx: &transaction.Transaction{Nonce: 3, SndAddr: []byte("eeee")}, TxHash: []byte("c")},
	}

	SortTransactionsBySenderAndNonce(txs)

	for _, item := range txs {
		fmt.Println(item.Tx.GetNonce(), string(item.Tx.GetSndAddr()), string(item.TxHash))
	}

	// Output:
	// 1 aaaa x
	// 2 aaaa z
	// 6 aabb a
	// 7 aabb t
	// 3 bbbb w
	// 5 bbbb y
	// 3 eeee c
	// 3 ffff b
}

func BenchmarkSortTransactionsByNonceAndSender_WhenReversedNonces(b *testing.B) {
	numTx := 100000
	txs := make([]*WrappedTransaction, numTx)
	for i := 0; i < numTx; i++ {
		txs[i] = &WrappedTransaction{
			Tx: &transaction.Transaction{
				Nonce:   uint64(numTx - i),
				SndAddr: []byte(fmt.Sprintf("sender-%d", i)),
			},
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SortTransactionsBySenderAndNonce(txs)
	}
}

func ExampleGroupSortedTransactionsBySender() {
	txs := []*WrappedTransaction{
		createTx([]byte("a7"), "alice", 7),
		createTx([]byte("b12"), "bob", 12),
		createTx([]byte("a9"), "alice", 9),
		createTx([]byte("a3"), "alice", 3),
		createTx([]byte("b1"), "bob", 1),
		createTx([]byte("c42"), "carol", 42),
	}

	for _, group := range GroupSortedTransactionsBySender(txs) {
		fmt.Println(string(group.sender), getTxHashesOfTxWrappersAsStrings(group.transactions))
	}

	// Output:
	// alice [a3 a7 a9]
	// bob [b1 b12]
	// carol [c42]
}

func ExampleGroupSortedTransactionsBySender_whenSingleSender() {
	txs := []*WrappedTransaction{
		createTx([]byte("a7"), "alice", 7),
		createTx([]byte("a9"), "alice", 9),
		createTx([]byte("a3"), "alice", 3),
	}

	for _, group := range GroupSortedTransactionsBySender(txs) {
		fmt.Println(string(group.sender), getTxHashesOfTxWrappersAsStrings(group.transactions))
	}

	// Output:
	// alice [a3 a7 a9]
}

func ExampleGroupSortedTransactionsBySender_whenEmpty() {
	txs := []*WrappedTransaction{}

	for _, group := range GroupSortedTransactionsBySender(txs) {
		fmt.Println(string(group.sender), getTxHashesOfTxWrappersAsStrings(group.transactions))
	}

	// Output:
}
