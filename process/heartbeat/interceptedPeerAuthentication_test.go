package heartbeat

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/heartbeat"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createDefaultInterceptedData() *heartbeat.PeerAuthentication {
	return &heartbeat.PeerAuthentication{
		Pubkey:          []byte("public key"),
		Signature:       []byte("signature"),
		Pid:             []byte("peer id"),
		HardforkPayload: []byte("hardfork payload"),
	}
}

func createMockInterceptedPeerAuthenticationArg(interceptedData *heartbeat.PeerAuthentication) ArgInterceptedPeerAuthentication {
	arg := ArgInterceptedPeerAuthentication{
		Marshalizer:          &mock.MarshalizerMock{},
		PeerSignatureHandler: &mock.PeerSignatureHandlerStub{},
		Hasher:               &mock.HasherMock{},
	}
	arg.DataBuff, _ = arg.Marshalizer.Marshal(interceptedData)

	return arg
}

func TestNewInterceptedPeerAuthentication_EmptyBufferShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockInterceptedPeerAuthenticationArg(createDefaultInterceptedData())
	arg.DataBuff = nil
	ipa, err := NewInterceptedPeerAuthentication(arg)

	assert.Equal(t, process.ErrNilBuffer, err)
	assert.True(t, check.IfNil(ipa))
}

func TestNewInterceptedPeerAuthentication_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockInterceptedPeerAuthenticationArg(createDefaultInterceptedData())
	arg.Marshalizer = nil
	ipa, err := NewInterceptedPeerAuthentication(arg)

	assert.Equal(t, process.ErrNilMarshalizer, err)
	assert.True(t, check.IfNil(ipa))
}

func TestNewInterceptedPeerAuthentication_NilPeerSignatureHandlerShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockInterceptedPeerAuthenticationArg(createDefaultInterceptedData())
	arg.PeerSignatureHandler = nil
	ipa, err := NewInterceptedPeerAuthentication(arg)

	assert.Equal(t, process.ErrNilPeerSignatureHandler, err)
	assert.True(t, check.IfNil(ipa))
}

func TestNewInterceptedPeerAuthentication_NilHasherShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockInterceptedPeerAuthenticationArg(createDefaultInterceptedData())
	arg.Hasher = nil
	ipa, err := NewInterceptedPeerAuthentication(arg)

	assert.Equal(t, process.ErrNilHasher, err)
	assert.True(t, check.IfNil(ipa))
}

func TestNewInterceptedPeerAuthentication_NilInvalidDataBuffShouldErr(t *testing.T) {
	t.Parallel()

	arg := createMockInterceptedPeerAuthenticationArg(createDefaultInterceptedData())
	arg.DataBuff = []byte("not a valid buffer")
	ipa, err := NewInterceptedPeerAuthentication(arg)

	assert.NotNil(t, err)
	assert.True(t, check.IfNil(ipa))
}

func TestNewInterceptedPeerAuthentication_ShouldWork(t *testing.T) {
	t.Parallel()

	arg := createMockInterceptedPeerAuthenticationArg(createDefaultInterceptedData())
	ipa, err := NewInterceptedPeerAuthentication(arg)

	assert.Nil(t, err)
	assert.False(t, check.IfNil(ipa))
}

func TestInterceptedPeerAuthentication_GettersAndSetters(t *testing.T) {
	t.Parallel()

	pkBytes := []byte("public key")
	pid := []byte("pid")
	hash := []byte("hash")
	sig := []byte("signature")
	hardforkPayload := []byte("hardfork payload")
	interceptedData := createDefaultInterceptedData()
	interceptedData.Pubkey = pkBytes
	interceptedData.Pid = pid
	interceptedData.Signature = sig
	interceptedData.HardforkPayload = hardforkPayload
	arg := createMockInterceptedPeerAuthenticationArg(interceptedData)
	arg.Hasher = &mock.HasherStub{
		ComputeCalled: func(s string) []byte {
			return hash
		},
	}
	ipa, err := NewInterceptedPeerAuthentication(arg)
	require.Nil(t, err)
	ipa.SetComputedShardID(2)

	assert.Equal(t, pkBytes, ipa.PublicKey())
	assert.True(t, ipa.IsForCurrentShard())
	assert.Equal(t, interceptedType, ipa.Type())
	assert.Equal(t, hash, ipa.Hash())
	assert.Equal(t, [][]byte{pkBytes, pid}, ipa.Identifiers())
	expectedString := "pk=7075626c6963206b6579, pid=ekxT, sig=7369676e6174757265, computed shardID=2"
	assert.Equal(t, expectedString, ipa.String())
	assert.Equal(t, sig, ipa.Signature())
	assert.Equal(t, pid, ipa.PeerID().Bytes())
	assert.Equal(t, hardforkPayload, ipa.HardforkPayload())
}

func TestInterceptedPeerAuthentication_CheckValidity(t *testing.T) {
	t.Parallel()

	interceptedData := createDefaultInterceptedData()
	interceptedData.Pubkey = bytes.Repeat([]byte{1}, maxSizeInBytes+1)
	ipa, _ := NewInterceptedPeerAuthentication(createMockInterceptedPeerAuthenticationArg(interceptedData))
	err := ipa.CheckValidity()
	assert.True(t, errors.Is(err, process.ErrPropertyTooLong) && strings.Contains(err.Error(), publicKeyProperty))

	interceptedData = createDefaultInterceptedData()
	interceptedData.Pubkey = make([]byte, 0)
	ipa, _ = NewInterceptedPeerAuthentication(createMockInterceptedPeerAuthenticationArg(interceptedData))
	err = ipa.CheckValidity()
	assert.True(t, errors.Is(err, process.ErrPropertyTooShort) && strings.Contains(err.Error(), publicKeyProperty))

	interceptedData = createDefaultInterceptedData()
	interceptedData.Signature = bytes.Repeat([]byte{1}, maxSizeInBytes+1)
	ipa, _ = NewInterceptedPeerAuthentication(createMockInterceptedPeerAuthenticationArg(interceptedData))
	err = ipa.CheckValidity()
	assert.True(t, errors.Is(err, process.ErrPropertyTooLong) && strings.Contains(err.Error(), signatureProperty))

	interceptedData = createDefaultInterceptedData()
	interceptedData.Pid = bytes.Repeat([]byte{1}, maxSizeInBytes+1)
	ipa, _ = NewInterceptedPeerAuthentication(createMockInterceptedPeerAuthenticationArg(interceptedData))
	err = ipa.CheckValidity()
	assert.True(t, errors.Is(err, process.ErrPropertyTooLong) && strings.Contains(err.Error(), peerIdProperty))

	interceptedData = createDefaultInterceptedData()
	interceptedData.HardforkPayload = bytes.Repeat([]byte{1}, maxSizeInBytes+1)
	ipa, _ = NewInterceptedPeerAuthentication(createMockInterceptedPeerAuthenticationArg(interceptedData))
	err = ipa.CheckValidity()
	assert.True(t, errors.Is(err, process.ErrPropertyTooLong) && strings.Contains(err.Error(), hardforkPayloadProperty))

	interceptedData = createDefaultInterceptedData()
	ipa, _ = NewInterceptedPeerAuthentication(createMockInterceptedPeerAuthenticationArg(interceptedData))
	err = ipa.CheckValidity()
	assert.Nil(t, err)
}
