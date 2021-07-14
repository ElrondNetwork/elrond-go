package heartbeat

import (
	"fmt"
	"sync"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/ElrondNetwork/elrond-go/data/heartbeat"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
)

const (
	minSizeInBytes          = 1
	maxSizeInBytes          = 128
	interceptedType         = "intercepted peer heartbeat"
	publicKeyProperty       = "public key"
	signatureProperty       = "signature"
	peerIdProperty          = "peer id"
	hardforkPayloadProperty = "payload"
)

// ArgInterceptedPeerAuthentication is the argument used in the intercepted peer authentication constructor
type ArgInterceptedPeerAuthentication struct {
	DataBuff             []byte
	Marshalizer          marshal.Marshalizer
	PeerSignatureHandler crypto.PeerSignatureHandler
	Hasher               hashing.Hasher
}

type interceptedPeerAuthentication struct {
	peerAuthentication   heartbeat.PeerAuthentication
	peerId               core.PeerID
	hash                 []byte
	mutComputedShardID   sync.RWMutex
	computedShardID      uint32
	peerSignatureHandler crypto.PeerSignatureHandler
}

// NewInterceptedPeerAuthentication tries to create a new intercepted peer authentication instance
func NewInterceptedPeerAuthentication(arg ArgInterceptedPeerAuthentication) (*interceptedPeerAuthentication, error) {
	if len(arg.DataBuff) == 0 {
		return nil, process.ErrNilBuffer
	}
	if check.IfNil(arg.Marshalizer) {
		return nil, process.ErrNilMarshalizer
	}
	if check.IfNil(arg.PeerSignatureHandler) {
		return nil, process.ErrNilPeerSignatureHandler
	}
	if check.IfNil(arg.Hasher) {
		return nil, process.ErrNilHasher
	}

	peerAuthentication, err := createPeerAuthentication(arg.Marshalizer, arg.DataBuff)
	if err != nil {
		return nil, err
	}

	intercepted := &interceptedPeerAuthentication{
		peerAuthentication:   *peerAuthentication,
		peerSignatureHandler: arg.PeerSignatureHandler,
	}

	intercepted.processFields(arg.Hasher, arg.DataBuff)

	return intercepted, nil
}

func createPeerAuthentication(marshalizer marshal.Marshalizer, buff []byte) (*heartbeat.PeerAuthentication, error) {
	peerAuthentication := &heartbeat.PeerAuthentication{}
	err := marshalizer.Unmarshal(peerAuthentication, buff)
	if err != nil {
		return nil, err
	}

	return peerAuthentication, nil
}

func (ipa *interceptedPeerAuthentication) processFields(hasher hashing.Hasher, buff []byte) {
	ipa.hash = hasher.Compute(string(buff))
	ipa.peerId = core.PeerID(ipa.peerAuthentication.Pid)
}

// PublicKey returns the public key as byte slice
func (ipa *interceptedPeerAuthentication) PublicKey() []byte {
	return ipa.peerAuthentication.Pubkey
}

// PeerID returns the peer ID
func (ipa *interceptedPeerAuthentication) PeerID() core.PeerID {
	return core.PeerID(ipa.peerAuthentication.Pid)
}

// Signature returns the signature done on the Pid field (concatenated with the hardforkPayload field) that can be
//checked against the provided PubKey field
func (ipa *interceptedPeerAuthentication) Signature() []byte {
	return ipa.peerAuthentication.Signature
}

// HardforkPayload returns the optionally hardfork payload data
func (ipa *interceptedPeerAuthentication) HardforkPayload() []byte {
	return ipa.peerAuthentication.HardforkPayload
}

// SetComputedShardID sets the computed shard ID. Concurrency safe.
func (ipa *interceptedPeerAuthentication) SetComputedShardID(shardId uint32) {
	ipa.mutComputedShardID.Lock()
	ipa.computedShardID = shardId
	ipa.mutComputedShardID.Unlock()
}

// CheckValidity will check the validity of the received peer heartbeat. This call won't trigger the signature validation.
func (ipa *interceptedPeerAuthentication) CheckValidity() error {
	err := VerifyHeartbeatProperyLen(publicKeyProperty, ipa.peerAuthentication.Pubkey)
	if err != nil {
		return err
	}
	err = VerifyHeartbeatProperyLen(signatureProperty, ipa.peerAuthentication.Signature)
	if err != nil {
		return err
	}
	err = VerifyHeartbeatProperyLen(peerIdProperty, ipa.peerId.Bytes())
	if err != nil {
		return err
	}
	err = VerifyHeartbeatProperyLen(hardforkPayloadProperty, ipa.peerAuthentication.HardforkPayload)
	if err != nil {
		return err
	}

	return nil
}

// IsForCurrentShard always returns true
func (ipa *interceptedPeerAuthentication) IsForCurrentShard() bool {
	return true
}

// Hash returns the hash of this intercepted peer heartbeat
func (ipa *interceptedPeerAuthentication) Hash() []byte {
	return ipa.hash
}

// Type returns the type of this intercepted data
func (ipa *interceptedPeerAuthentication) Type() string {
	return interceptedType
}

// Identifiers returns the identifiers used in requests
func (ipa *interceptedPeerAuthentication) Identifiers() [][]byte {
	return [][]byte{ipa.peerAuthentication.Pubkey, ipa.peerAuthentication.Pid}
}

// String returns the transaction's most important fields as string
func (ipa *interceptedPeerAuthentication) String() string {
	ipa.mutComputedShardID.RLock()
	defer ipa.mutComputedShardID.RUnlock()

	return fmt.Sprintf("pk=%s, pid=%s, sig=%s, computed shardID=%d",
		logger.DisplayByteSlice(ipa.peerAuthentication.Pubkey),
		ipa.peerId.Pretty(),
		logger.DisplayByteSlice(ipa.peerAuthentication.Signature),
		ipa.computedShardID,
	)
}

// VerifyHeartbeatProperyLen returns an error if the provided value is longer than accepted by the network
func VerifyHeartbeatProperyLen(property string, value []byte) error {
	if len(value) > maxSizeInBytes {
		return fmt.Errorf("%w for %s", process.ErrPropertyTooLong, property)
	}
	if len(value) < minSizeInBytes {
		return fmt.Errorf("%w for %s", process.ErrPropertyTooShort, property)
	}

	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (ipa *interceptedPeerAuthentication) IsInterfaceNil() bool {
	return ipa == nil
}
