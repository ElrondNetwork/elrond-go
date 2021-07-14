package factory

import (
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/heartbeat"
)

type interceptedPeerAuthenticationDataFactory struct {
	marshalizer          marshal.Marshalizer
	peerSignatureHandler crypto.PeerSignatureHandler
	hasher               hashing.Hasher
}

// NewInterceptedPeerAuthenticationDataFactory creates an instance of interceptedPeerAuthenticationDataFactory
func NewInterceptedPeerAuthenticationDataFactory(
	argument *ArgInterceptedDataFactory,
) (*interceptedPeerAuthenticationDataFactory, error) {
	if argument == nil {
		return nil, process.ErrNilArgumentStruct
	}
	if check.IfNil(argument.CoreComponents) {
		return nil, process.ErrNilCoreComponentsHolder
	}
	if check.IfNil(argument.CoreComponents.InternalMarshalizer()) {
		return nil, process.ErrNilMarshalizer
	}
	if check.IfNil(argument.CoreComponents.Hasher()) {
		return nil, process.ErrNilHasher
	}
	if check.IfNil(argument.PeerSignatureHandler) {
		return nil, process.ErrNilPeerSignatureHandler
	}

	return &interceptedPeerAuthenticationDataFactory{
		marshalizer:          argument.CoreComponents.InternalMarshalizer(),
		hasher:               argument.CoreComponents.Hasher(),
		peerSignatureHandler: argument.PeerSignatureHandler,
	}, nil
}

// Create creates instances of InterceptedData by unmarshalling provided buffer
func (ipadf *interceptedPeerAuthenticationDataFactory) Create(buff []byte) (process.InterceptedData, error) {
	arg := heartbeat.ArgInterceptedPeerAuthentication{
		DataBuff:             buff,
		Marshalizer:          ipadf.marshalizer,
		PeerSignatureHandler: ipadf.peerSignatureHandler,
		Hasher:               ipadf.hasher,
	}

	return heartbeat.NewInterceptedPeerAuthentication(arg)
}

// IsInterfaceNil returns true if there is no value under the interface
func (ipadf *interceptedPeerAuthenticationDataFactory) IsInterfaceNil() bool {
	return ipadf == nil
}
