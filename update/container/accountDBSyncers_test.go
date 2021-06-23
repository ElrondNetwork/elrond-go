package containers

import (
	"errors"
	"testing"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/update"
	"github.com/ElrondNetwork/elrond-go/update/mock"
	"github.com/stretchr/testify/require"
)

func TestNewAccountsDBSyncersContainer(t *testing.T) {
	t.Parallel()

	adsc := NewAccountsDBSyncersContainer()
	require.False(t, check.IfNil(adsc))
}

func TestAccountDBSyncers_AddGetShouldWork(t *testing.T) {
	t.Parallel()

	adsc := NewAccountsDBSyncersContainer()
	testKey := "key"
	testVal := &mock.AccountsDBSyncerStub{}
	err := adsc.Add(testKey, testVal)
	require.NoError(t, err)

	res, err := adsc.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, testVal, res)
}

func TestAccountDBSyncers_AddMultipleShouldWork(t *testing.T) {
	t.Parallel()

	adsc := NewAccountsDBSyncersContainer()
	testKey0 := "key0"
	testVal0 := &mock.AccountsDBSyncerStub{}
	testKey1 := "key1"
	testVal1 := &mock.AccountsDBSyncerStub{}

	err := adsc.AddMultiple([]string{testKey0, testKey1}, []update.AccountsDBSyncer{testVal0, testVal1})
	require.NoError(t, err)

	res0, err := adsc.Get(testKey0)
	require.NoError(t, err)
	require.Equal(t, testVal0, res0)

	res1, err := adsc.Get(testKey1)
	require.NoError(t, err)
	require.Equal(t, testVal1, res1)

	require.Equal(t, 2, adsc.Len())
}

func TestAccountDBSyncers_ReplaceShouldWork(t *testing.T) {
	t.Parallel()

	adsc := NewAccountsDBSyncersContainer()
	testKey := "key"
	testVal := &mock.AccountsDBSyncerStub{}
	err := adsc.Add(testKey, testVal)
	require.NoError(t, err)

	res, err := adsc.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, testVal, res)

	// update
	newTestVal := &mock.AccountsDBSyncerStub{
		SyncAccountsCalled: func(_ []byte, _ uint32) error {
			return errors.New("local error")
		},
	}
	err = adsc.Replace(testKey, newTestVal)
	require.NoError(t, err)

	res, err = adsc.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, newTestVal, res)
}

func TestAccountDBSyncers_DeleteShouldWork(t *testing.T) {
	t.Parallel()

	adsc := NewAccountsDBSyncersContainer()
	testKey := "key"
	testVal := &mock.AccountsDBSyncerStub{}
	err := adsc.Add(testKey, testVal)
	require.NoError(t, err)

	res, err := adsc.Get(testKey)
	require.NoError(t, err)
	require.Equal(t, testVal, res)

	adsc.Remove(testKey)

	res, err = adsc.Get(testKey)
	require.Nil(t, res)
	require.Equal(t, update.ErrInvalidContainerKey, err)
}
