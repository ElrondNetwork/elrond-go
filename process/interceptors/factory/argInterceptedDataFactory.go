package factory

import (
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/ElrondNetwork/elrond-go/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
)

// interceptedDataCoreComponentsHolder holds the core components required by the intercepted data factory
type interceptedDataCoreComponentsHolder interface {
	InternalMarshalizer() marshal.Marshalizer
	TxMarshalizer() marshal.Marshalizer
	TxVersionChecker() process.TxVersionCheckerHandler
	Hasher() hashing.Hasher
	TxSignHasher() hashing.Hasher
	Uint64ByteSliceConverter() typeConverters.Uint64ByteSliceConverter
	AddressPubKeyConverter() core.PubkeyConverter
	ChainID() string
	MinTransactionVersion() uint32
	IsInterfaceNil() bool
	EpochNotifier() process.EpochNotifier
}

// interceptedDataCryptoComponentsHolder holds the crypto components required by the intercepted data factory
type interceptedDataCryptoComponentsHolder interface {
	TxSignKeyGen() crypto.KeyGenerator
	BlockSignKeyGen() crypto.KeyGenerator
	TxSingleSigner() crypto.SingleSigner
	BlockSigner() crypto.SingleSigner
	MultiSigner() crypto.MultiSigner
	PublicKey() crypto.PublicKey
	IsInterfaceNil() bool
}

// ArgInterceptedDataFactory holds all dependencies required by the shard and meta intercepted data factory in order to create
// new instances
type ArgInterceptedDataFactory struct {
	CoreComponents            interceptedDataCoreComponentsHolder
	CryptoComponents          interceptedDataCryptoComponentsHolder
	ShardCoordinator          sharding.Coordinator
	NodesCoordinator          sharding.NodesCoordinator
	FeeHandler                process.FeeHandler
	WhiteListerVerifiedTxs    process.WhiteListHandler
	HeaderSigVerifier         process.InterceptedHeaderSigVerifier
	ValidityAttester          process.ValidityAttester
	HeaderIntegrityVerifier   process.HeaderIntegrityVerifier
	EpochStartTrigger         process.EpochStartTriggerHandler
	ArgsParser                process.ArgumentsParser
	EnableSignTxWithHashEpoch uint32
	PeerSignatureHandler      crypto.PeerSignatureHandler
}
