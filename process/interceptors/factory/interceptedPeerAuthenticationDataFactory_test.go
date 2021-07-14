package factory

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/heartbeat"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInterceptedPeerAuthenticationDataFactory_NilArgumentShouldErr(t *testing.T) {
	t.Parallel()

	imh, err := NewInterceptedPeerAuthenticationDataFactory(nil)

	assert.True(t, check.IfNil(imh))
	assert.Equal(t, process.ErrNilArgumentStruct, err)
}

func TestNewInterceptedPeerAuthenticationDataFactory_NilCoreComponentsShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, cryptoComponents := createMockComponentHolders()
	arg := createMockArgument(coreComponents, cryptoComponents)
	arg.CoreComponents = nil

	imh, err := NewInterceptedPeerAuthenticationDataFactory(arg)
	assert.True(t, check.IfNil(imh))
	assert.Equal(t, process.ErrNilCoreComponentsHolder, err)
}

func TestNewInterceptedPeerAuthenticationDataFactory_NilMarshalizerShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, cryptoComponents := createMockComponentHolders()
	coreComponents.IntMarsh = nil
	arg := createMockArgument(coreComponents, cryptoComponents)

	imh, err := NewInterceptedPeerAuthenticationDataFactory(arg)
	assert.True(t, check.IfNil(imh))
	assert.Equal(t, process.ErrNilMarshalizer, err)
}

func TestNewInterceptedPeerAuthenticationDataFactory_NilPeerSignatureHandlerShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, cryptoComponents := createMockComponentHolders()
	arg := createMockArgument(coreComponents, cryptoComponents)
	arg.PeerSignatureHandler = nil

	imh, err := NewInterceptedPeerAuthenticationDataFactory(arg)
	assert.True(t, check.IfNil(imh))
	assert.Equal(t, process.ErrNilPeerSignatureHandler, err)
}

func TestNewInterceptedPeerAuthenticationDataFactory_NilHasherShouldErr(t *testing.T) {
	t.Parallel()

	coreComponents, cryptoComponents := createMockComponentHolders()
	coreComponents.Hash = nil
	arg := createMockArgument(coreComponents, cryptoComponents)

	imh, err := NewInterceptedPeerAuthenticationDataFactory(arg)
	assert.True(t, check.IfNil(imh))
	assert.Equal(t, process.ErrNilHasher, err)
}

func TestInterceptedPeerAuthenticationDataFactory_ShouldWorkAndCreate(t *testing.T) {
	t.Parallel()

	coreComponents, cryptoComponents := createMockComponentHolders()
	arg := createMockArgument(coreComponents, cryptoComponents)

	imh, err := NewInterceptedPeerAuthenticationDataFactory(arg)
	assert.False(t, check.IfNil(imh))
	assert.Nil(t, err)

	mockPeerAuthentication := &heartbeat.PeerAuthentication{
		Pubkey:    []byte("pk"),
		Signature: []byte("sig"),
		Pid:       []byte("pid"),
	}
	buff, err := arg.CoreComponents.InternalMarshalizer().Marshal(mockPeerAuthentication)
	require.Nil(t, err)

	interceptedData, err := imh.Create(buff)
	assert.Nil(t, err)

	assert.True(t, strings.Contains(fmt.Sprintf("%T", interceptedData), "*heartbeat.interceptedPeerAuthentication"))
}
