package mock

import (
	"math/big"

	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
)

type Facade struct {
	GenerateTransactionHandler func(sender string, receiver string, value big.Int,
		code string) (*transaction.Transaction, error)
	GetTransactionHandler  func(hash string) (*transaction.Transaction, error)
	SendTransactionHandler func(nonce uint64, sender string, receiver string, value big.Int, code string,
		signature []byte) (*transaction.Transaction, error)
	GenerateAndSendBulkTransactionsHandler func(destination string, value big.Int, nrTransactions uint64) error
}

func (f *Facade) GenerateTransaction(sender string, receiver string, value big.Int,
	code string) (*transaction.Transaction, error) {
	return f.GenerateTransactionHandler(sender, receiver, value, code)
}

func (f *Facade) GetTransaction(hash string) (*transaction.Transaction, error) {
	return f.GetTransactionHandler(hash)
}

// SendTransaction will send a new transaction on the topic channel
func (f *Facade) SendTransaction(nonce uint64, sender string, receiver string, value big.Int, code string, signature []byte) (*transaction.Transaction, error) {
	return f.SendTransactionHandler(nonce, sender, receiver, value, code, signature)
}

func (f *Facade) GenerateAndSendBulkTransactions(destination string, value big.Int, nrTransactions uint64) error {
	return f.GenerateAndSendBulkTransactionsHandler(destination, value, nrTransactions)
}

type WrongFacade struct {
}
