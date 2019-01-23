package sync

import (
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
)

func (boot *Bootstrap) RequestHeader(nonce uint64) {
	boot.requestHeader(nonce)
}

func (boot *Bootstrap) GetHeaderFromPool(nonce uint64) *block.Header {
	return boot.getHeaderFromPoolHavingNonce(nonce)
}

func (boot *Bootstrap) GetTxBodyFromPool(hash []byte) interface{} {
	return boot.getTxBody(hash)
}
