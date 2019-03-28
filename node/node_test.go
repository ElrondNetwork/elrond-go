package node_test

import (
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/data"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/ElrondNetwork/elrond-go-sandbox/node"
	"github.com/ElrondNetwork/elrond-go-sandbox/node/mock"
	"github.com/ElrondNetwork/elrond-go-sandbox/process"
	"github.com/ElrondNetwork/elrond-go-sandbox/process/factory"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func logError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestNewNode(t *testing.T) {
	n, err := node.NewNode()
	assert.NotNil(t, n)
	assert.Nil(t, err)
}

func TestNewNode_NotRunning(t *testing.T) {
	n, _ := node.NewNode()
	assert.False(t, n.IsRunning())
}

func TestNewNode_NilOptionShouldError(t *testing.T) {
	_, err := node.NewNode(node.WithAccountsAdapter(nil))
	assert.NotNil(t, err)
}

func TestNewNode_ApplyNilOptionShouldError(t *testing.T) {

	n, _ := node.NewNode()
	err := n.ApplyOptions(node.WithAccountsAdapter(nil))
	assert.NotNil(t, err)
}

func TestStart_NoMessenger(t *testing.T) {
	n, _ := node.NewNode()
	err := n.Start()
	defer func() { _ = n.Stop() }()
	assert.NotNil(t, err)
}

func TestStart_CorrectParams(t *testing.T) {

	messenger := getMessenger()
	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)
	err := n.Start()
	defer func() { _ = n.Stop() }()
	assert.Nil(t, err)
	assert.True(t, n.IsRunning())
}

func TestStart_CorrectParamsApplyingOptions(t *testing.T) {

	n, _ := node.NewNode()
	messenger := getMessenger()
	err := n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)

	logError(err)

	err = n.Start()
	defer func() { _ = n.Stop() }()
	assert.Nil(t, err)
	assert.True(t, n.IsRunning())
}

func TestApplyOptions_NodeStarted(t *testing.T) {

	messenger := getMessenger()
	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
	)
	err := n.Start()
	defer func() { _ = n.Stop() }()
	logError(err)

	assert.True(t, n.IsRunning())
}

func TestStop_NotStartedYet(t *testing.T) {

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
	)

	err := n.Stop()
	assert.Nil(t, err)
	assert.False(t, n.IsRunning())
}

func TestStop_MessengerCloseErrors(t *testing.T) {
	errorString := "messenger close error"
	messenger := getMessenger()
	messenger.CloseCalled = func() error {
		return errors.New(errorString)
	}
	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
	)

	n.Start()

	err := n.Stop()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), errorString)
}

func TestStop(t *testing.T) {

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
	)
	err := n.Start()
	logError(err)

	err = n.Stop()
	assert.Nil(t, err)
	assert.False(t, n.IsRunning())
}

func TestGetBalance_NoAddrConverterShouldError(t *testing.T) {

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
		node.WithPrivateKey(&mock.PrivateKeyStub{}),
	)
	_, err := n.GetBalance("address")
	assert.NotNil(t, err)
	assert.Equal(t, "initialize AccountsAdapter and AddressConverter first", err.Error())
}

func TestGetBalance_NoAccAdapterShouldError(t *testing.T) {

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithPrivateKey(&mock.PrivateKeyStub{}),
	)
	_, err := n.GetBalance("address")
	assert.NotNil(t, err)
	assert.Equal(t, "initialize AccountsAdapter and AddressConverter first", err.Error())
}

func TestGetBalance_CreateAddressFailsShouldError(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.AddressConverterStub{
		CreateAddressFromHexHandler: func(hexAddress string) (state.AddressContainer, error) {
			// Return that will result in a correct run of GenerateTransaction -> will fail test
			/*return mock.AddressContainerStub{
			}, nil*/

			return nil, errors.New("error")
		},
	}
	privateKey := getPrivateKey()
	singleSigner := &mock.SinglesignMock{}
	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)
	_, err := n.GetBalance("address")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid address")
}

func TestGetBalance_GetAccountFailsShouldError(t *testing.T) {

	accAdapter := mock.AccountsAdapterStub{
		GetExistingAccountHandler: func(addrContainer state.AddressContainer) (state.AccountWrapper, error) {
			return nil, errors.New("error")
		},
	}
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
	)
	_, err := n.GetBalance(createDummyHexAddress(64))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "could not fetch sender address from provided param")
}

func createDummyHexAddress(chars int) string {
	if chars < 1 {
		return ""
	}

	var characters = []byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}

	rdm := rand.New(rand.NewSource(time.Now().Unix()))

	buff := make([]byte, chars)
	for i := 0; i < chars; i++ {
		buff[i] = characters[rdm.Int()%16]
	}

	return string(buff)
}

func TestGetBalance_GetAccountReturnsNil(t *testing.T) {

	accAdapter := mock.AccountsAdapterStub{
		GetExistingAccountHandler: func(addrContainer state.AddressContainer) (state.AccountWrapper, error) {
			return nil, nil
		},
	}
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
	)
	balance, err := n.GetBalance(createDummyHexAddress(64))
	assert.Nil(t, err)
	assert.Equal(t, big.NewInt(0), balance)
}

func TestGetBalance(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(100))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
	)
	balance, err := n.GetBalance(createDummyHexAddress(64))
	assert.Nil(t, err)
	assert.Equal(t, big.NewInt(100), balance)
}

//------- GenerateTransaction

func TestGenerateTransaction_NoAddrConverterShouldError(t *testing.T) {

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
		node.WithPrivateKey(&mock.PrivateKeyStub{}),
	)
	_, err := n.GenerateTransaction("sender", "receiver", big.NewInt(10), "code")
	assert.NotNil(t, err)
}

func TestGenerateTransaction_NoAccAdapterShouldError(t *testing.T) {

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithPrivateKey(&mock.PrivateKeyStub{}),
	)
	_, err := n.GenerateTransaction("sender", "receiver", big.NewInt(10), "code")
	assert.NotNil(t, err)
}

func TestGenerateTransaction_NoPrivateKeyShouldError(t *testing.T) {

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)
	_, err := n.GenerateTransaction("sender", "receiver", big.NewInt(10), "code")
	assert.NotNil(t, err)
}

func TestGenerateTransaction_CreateAddressFailsShouldError(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
	)
	_, err := n.GenerateTransaction("sender", "receiver", big.NewInt(10), "code")
	assert.NotNil(t, err)
}

func TestGenerateTransaction_GetAccountFailsShouldError(t *testing.T) {

	accAdapter := mock.AccountsAdapterStub{
		GetExistingAccountHandler: func(addrContainer state.AddressContainer) (state.AccountWrapper, error) {
			return nil, errors.New("error")
		},
	}
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
	)
	_, err := n.GenerateTransaction(createDummyHexAddress(64), createDummyHexAddress(64), big.NewInt(10), "code")
	assert.NotNil(t, err)
}

func TestGenerateTransaction_GetAccountReturnsNilShouldWork(t *testing.T) {

	accAdapter := mock.AccountsAdapterStub{
		GetExistingAccountHandler: func(addrContainer state.AddressContainer) (state.AccountWrapper, error) {
			return nil, nil
		},
	}
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	singleSigner := &mock.SinglesignMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)
	_, err := n.GenerateTransaction(createDummyHexAddress(64), createDummyHexAddress(64), big.NewInt(10), "code")
	assert.Nil(t, err)
}

func TestGenerateTransaction_GetExistingAccountShouldWork(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	singleSigner := &mock.SinglesignMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)
	_, err := n.GenerateTransaction(createDummyHexAddress(64), createDummyHexAddress(64), big.NewInt(10), "code")
	assert.Nil(t, err)
}

func TestGenerateTransaction_MarshalErrorsShouldError(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	singleSigner := &mock.SinglesignMock{}
	marshalizer := mock.MarshalizerMock{
		MarshalHandler: func(obj interface{}) ([]byte, error) {
			return nil, errors.New("error")
		},
	}
	n, _ := node.NewNode(
		node.WithMarshalizer(marshalizer),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)
	_, err := n.GenerateTransaction("sender", "receiver", big.NewInt(10), "code")
	assert.NotNil(t, err)
}

func TestGenerateTransaction_SignTxErrorsShouldError(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := &mock.PrivateKeyStub{}
	singleSigner := &mock.SinglesignFailMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)
	_, err := n.GenerateTransaction(createDummyHexAddress(64), createDummyHexAddress(64), big.NewInt(10), "code")
	assert.NotNil(t, err)
}

func TestGenerateTransaction_ShouldSetCorrectSignature(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	signature := []byte("signed")
	privateKey := &mock.PrivateKeyStub{}
	singleSigner := &mock.SinglesignMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)

	tx, err := n.GenerateTransaction(createDummyHexAddress(64), createDummyHexAddress(64), big.NewInt(10), "code")
	assert.Nil(t, err)
	assert.Equal(t, signature, tx.Signature)
}

func TestGenerateTransaction_ShouldSetCorrectNonce(t *testing.T) {

	nonce := uint64(7)
	accAdapter := mock.AccountsAdapterStub{
		GetExistingAccountHandler: func(addrContainer state.AddressContainer) (state.AccountWrapper, error) {
			return mock.AccountWrapperStub{
				BaseAccountHandler: func() *state.Account {
					return &state.Account{
						Nonce:   nonce,
						Balance: big.NewInt(0),
					}
				},
			}, nil
		},
	}
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	singleSigner := &mock.SinglesignMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)

	tx, err := n.GenerateTransaction(createDummyHexAddress(64), createDummyHexAddress(64), big.NewInt(10), "code")
	assert.Nil(t, err)
	assert.Equal(t, nonce, tx.Nonce)
}

func TestGenerateTransaction_CorrectParamsShouldNotError(t *testing.T) {

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	privateKey := getPrivateKey()
	singleSigner := &mock.SinglesignMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(privateKey),
		node.WithSinglesig(singleSigner),
	)
	_, err := n.GenerateTransaction(createDummyHexAddress(64), createDummyHexAddress(64), big.NewInt(10), "code")
	assert.Nil(t, err)
}

//------- GenerateAndSendBulkTransactions

func TestGenerateAndSendBulkTransactions_ZeroTxShouldErr(t *testing.T) {
	n, _ := node.NewNode()

	err := n.GenerateAndSendBulkTransactions("", big.NewInt(0), 0)
	assert.Equal(t, "can not generate and broadcast 0 transactions", err.Error())
}

func TestGenerateAndSendBulkTransactions_NilAccountAdapterShouldErr(t *testing.T) {
	marshalizer := &mock.MarshalizerFake{}

	addrConverter := mock.NewAddressConverterFake(32, "0x")
	keyGen := &mock.KeyGenMock{}
	sk, pk := keyGen.GeneratePair()
	singleSigner := &mock.SinglesignMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(marshalizer),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithPrivateKey(sk),
		node.WithPublicKey(pk),
		node.WithSinglesig(singleSigner),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
	)

	err := n.GenerateAndSendBulkTransactions(createDummyHexAddress(64), big.NewInt(0), 1)
	assert.Equal(t, node.ErrNilAccountsAdapter, err)
}

func TestGenerateAndSendBulkTransactions_NilAddressConverterShouldErr(t *testing.T) {
	marshalizer := &mock.MarshalizerFake{}
	accAdapter := getAccAdapter(big.NewInt(0))
	keyGen := &mock.KeyGenMock{}
	sk, pk := keyGen.GeneratePair()
	singleSigner := &mock.SinglesignMock{}

	n, _ := node.NewNode(
		node.WithMarshalizer(marshalizer),
		node.WithHasher(mock.HasherMock{}),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(sk),
		node.WithPublicKey(pk),
		node.WithSinglesig(singleSigner),
	)

	err := n.GenerateAndSendBulkTransactions(createDummyHexAddress(64), big.NewInt(0), 1)
	assert.Equal(t, node.ErrNilAddressConverter, err)
}

func TestGenerateAndSendBulkTransactions_NilPrivateKeyShouldErr(t *testing.T) {
	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	keyGen := &mock.KeyGenMock{}
	_, pk := keyGen.GeneratePair()
	singleSigner := &mock.SinglesignMock{}
	n, _ := node.NewNode(
		node.WithAccountsAdapter(accAdapter),
		node.WithAddressConverter(addrConverter),
		node.WithPublicKey(pk),
		node.WithMarshalizer(&mock.MarshalizerFake{}),
		node.WithSinglesig(singleSigner),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
	)

	err := n.GenerateAndSendBulkTransactions(createDummyHexAddress(64), big.NewInt(0), 1)
	assert.True(t, strings.Contains(err.Error(), "trying to set nil private key"))
}

func TestGenerateAndSendBulkTransactions_NilPublicKeyShouldErr(t *testing.T) {
	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	keyGen := &mock.KeyGenMock{}
	sk, _ := keyGen.GeneratePair()
	singleSigner := &mock.SinglesignMock{}
	n, _ := node.NewNode(
		node.WithAccountsAdapter(accAdapter),
		node.WithAddressConverter(addrConverter),
		node.WithPrivateKey(sk),
		node.WithSinglesig(singleSigner),
	)

	err := n.GenerateAndSendBulkTransactions("", big.NewInt(0), 1)
	assert.Equal(t, "trying to set nil public key", err.Error())
}

func TestGenerateAndSendBulkTransactions_InvalidReceiverAddressShouldErr(t *testing.T) {
	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	keyGen := &mock.KeyGenMock{}
	sk, pk := keyGen.GeneratePair()
	singleSigner := &mock.SinglesignMock{}
	n, _ := node.NewNode(
		node.WithAccountsAdapter(accAdapter),
		node.WithAddressConverter(addrConverter),
		node.WithPrivateKey(sk),
		node.WithPublicKey(pk),
		node.WithSinglesig(singleSigner),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
	)

	err := n.GenerateAndSendBulkTransactions("", big.NewInt(0), 1)
	assert.Contains(t, err.Error(), "could not create receiver address from provided param")
}

func TestGenerateAndSendBulkTransactions_CreateAddressFromPublicKeyBytesErrorsShouldErr(t *testing.T) {
	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := &mock.AddressConverterStub{}
	addrConverter.CreateAddressFromPublicKeyBytesHandler = func(pubKey []byte) (container state.AddressContainer, e error) {
		return nil, errors.New("error")
	}
	keyGen := &mock.KeyGenMock{}
	sk, pk := keyGen.GeneratePair()
	singleSigner := &mock.SinglesignMock{}
	n, _ := node.NewNode(
		node.WithAccountsAdapter(accAdapter),
		node.WithAddressConverter(addrConverter),
		node.WithPrivateKey(sk),
		node.WithPublicKey(pk),
		node.WithSinglesig(singleSigner),
	)

	err := n.GenerateAndSendBulkTransactions("", big.NewInt(0), 1)
	assert.Equal(t, "error", err.Error())
}

func TestGenerateAndSendBulkTransactions_MarshalizerErrorsShouldErr(t *testing.T) {
	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	marshalizer := &mock.MarshalizerFake{}
	marshalizer.Fail = true
	keyGen := &mock.KeyGenMock{}
	sk, pk := keyGen.GeneratePair()
	singleSigner := &mock.SinglesignMock{}
	n, _ := node.NewNode(
		node.WithAccountsAdapter(accAdapter),
		node.WithAddressConverter(addrConverter),
		node.WithPrivateKey(sk),
		node.WithPublicKey(pk),
		node.WithMarshalizer(marshalizer),
		node.WithSinglesig(singleSigner),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
	)

	err := n.GenerateAndSendBulkTransactions(createDummyHexAddress(64), big.NewInt(1), 1)
	assert.True(t, strings.Contains(err.Error(), "could not marshal transaction"))
}

func TestGenerateAndSendBulkTransactions_ShouldWork(t *testing.T) {
	marshalizer := &mock.MarshalizerFake{}

	noOfTx := 1000
	mutRecoveredTransactions := &sync.RWMutex{}
	recoveredTransactions := make(map[uint64]*transaction.Transaction)
	signer := &mock.SinglesignMock{}
	shardCoordinator := mock.NewOneShardCoordinatorMock()

	mes := &mock.MessengerStub{
		BroadcastOnChannelCalled: func(pipe string, topic string, buff []byte) {
			identifier := factory.TransactionTopic + shardCoordinator.CommunicationIdentifier(shardCoordinator.SelfId())

			if topic == identifier {
				//handler to capture sent data
				tx := transaction.Transaction{}

				err := marshalizer.Unmarshal(&tx, buff)
				if err != nil {
					assert.Fail(t, err.Error())
				}

				mutRecoveredTransactions.Lock()
				recoveredTransactions[tx.Nonce] = &tx
				mutRecoveredTransactions.Unlock()
			}
		},
	}

	accAdapter := getAccAdapter(big.NewInt(0))
	addrConverter := mock.NewAddressConverterFake(32, "0x")
	keyGen := &mock.KeyGenMock{}
	sk, pk := keyGen.GeneratePair()
	n, _ := node.NewNode(
		node.WithMarshalizer(marshalizer),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(addrConverter),
		node.WithAccountsAdapter(accAdapter),
		node.WithPrivateKey(sk),
		node.WithPublicKey(pk),
		node.WithSinglesig(signer),
		node.WithShardCoordinator(shardCoordinator),
	)

	n.SetMessenger(mes)

	err := n.GenerateAndSendBulkTransactions(createDummyHexAddress(64), big.NewInt(1), uint64(noOfTx))
	assert.Nil(t, err)
	mutRecoveredTransactions.RLock()
	assert.Equal(t, noOfTx, len(recoveredTransactions))
	mutRecoveredTransactions.RUnlock()
}

func getAccAdapter(balance *big.Int) mock.AccountsAdapterStub {
	return mock.AccountsAdapterStub{
		GetExistingAccountHandler: func(addrContainer state.AddressContainer) (state.AccountWrapper, error) {
			return mock.AccountWrapperStub{
				BaseAccountHandler: func() *state.Account {
					return &state.Account{
						Nonce:   1,
						Balance: balance,
					}
				},
			}, nil
		},
	}
}

func getPrivateKey() *mock.PrivateKeyStub {
	return &mock.PrivateKeyStub{}
}

func TestSendTransaction_ShouldWork(t *testing.T) {
	n, _ := node.NewNode(
		node.WithMarshalizer(&mock.MarshalizerFake{}),
		node.WithAddressConverter(mock.NewAddressConverterFake(32, "0x")),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
	)

	txSent := false

	mes := &mock.MessengerStub{
		BroadcastOnChannelCalled: func(pipe string, topic string, buff []byte) {
			txSent = true
		},
	}
	n.SetMessenger(mes)

	nonce := uint64(50)
	value := big.NewInt(567)
	sender := createDummyHexAddress(64)
	receiver := createDummyHexAddress(64)
	txData := "data"
	signature := []byte("signature")

	tx, err := n.SendTransaction(
		nonce,
		sender,
		receiver,
		value,
		txData,
		signature)

	assert.Nil(t, err)
	assert.NotNil(t, tx)
	assert.True(t, txSent)
}

func TestCreateShardedStores_NilShardCoordinatorShouldError(t *testing.T) {
	messenger := getMessenger()
	dataPool := &mock.PoolsHolderStub{}

	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithDataPool(dataPool),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)
	err := n.Start()
	logError(err)
	defer func() { _ = n.Stop() }()
	err = n.CreateShardedStores()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "nil shard coordinator")
}

func TestCreateShardedStores_NilDataPoolShouldError(t *testing.T) {
	messenger := getMessenger()
	shardCoordinator := mock.NewOneShardCoordinatorMock()
	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithShardCoordinator(shardCoordinator),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)
	err := n.Start()
	logError(err)
	defer func() { _ = n.Stop() }()
	err = n.CreateShardedStores()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "nil data pool")
}

func TestCreateShardedStores_NilTransactionDataPoolShouldError(t *testing.T) {
	messenger := getMessenger()
	shardCoordinator := mock.NewOneShardCoordinatorMock()
	dataPool := &mock.PoolsHolderStub{}
	dataPool.TransactionsCalled = func() data.ShardedDataCacherNotifier {
		return nil
	}
	dataPool.HeadersCalled = func() data.ShardedDataCacherNotifier {
		return &mock.ShardedDataStub{}
	}
	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithShardCoordinator(shardCoordinator),
		node.WithDataPool(dataPool),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)
	err := n.Start()
	logError(err)
	defer func() { _ = n.Stop() }()
	err = n.CreateShardedStores()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "nil transaction sharded data store")
}

func TestCreateShardedStores_NilHeaderDataPoolShouldError(t *testing.T) {
	messenger := getMessenger()
	shardCoordinator := mock.NewOneShardCoordinatorMock()
	dataPool := &mock.PoolsHolderStub{}
	dataPool.TransactionsCalled = func() data.ShardedDataCacherNotifier {
		return &mock.ShardedDataStub{}
	}
	dataPool.HeadersCalled = func() data.ShardedDataCacherNotifier {
		return nil
	}
	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithShardCoordinator(shardCoordinator),
		node.WithDataPool(dataPool),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)
	err := n.Start()
	logError(err)
	defer func() { _ = n.Stop() }()
	err = n.CreateShardedStores()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "nil header sharded data store")
}

func TestCreateShardedStores_ReturnsSuccessfully(t *testing.T) {
	messenger := getMessenger()
	shardCoordinator := mock.NewOneShardCoordinatorMock()
	nrOfShards := uint32(2)
	shardCoordinator.SetNoShards(nrOfShards)
	dataPool := &mock.PoolsHolderStub{}

	var txShardedStores []string
	txShardedData := &mock.ShardedDataStub{}
	txShardedData.CreateShardStoreCalled = func(cacherId string) {
		txShardedStores = append(txShardedStores, cacherId)
	}

	var headerShardedStores []string
	headerShardedData := &mock.ShardedDataStub{}
	headerShardedData.CreateShardStoreCalled = func(cacherId string) {
		headerShardedStores = append(headerShardedStores, cacherId)
	}
	dataPool.TransactionsCalled = func() data.ShardedDataCacherNotifier {
		return txShardedData
	}
	dataPool.HeadersCalled = func() data.ShardedDataCacherNotifier {
		return headerShardedData
	}
	n, _ := node.NewNode(
		node.WithMessenger(messenger),
		node.WithShardCoordinator(shardCoordinator),
		node.WithDataPool(dataPool),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithHasher(mock.HasherMock{}),
		node.WithAddressConverter(&mock.AddressConverterStub{}),
		node.WithAccountsAdapter(&mock.AccountsAdapterStub{}),
	)
	err := n.Start()
	logError(err)
	defer func() { _ = n.Stop() }()
	err = n.CreateShardedStores()
	assert.Nil(t, err)

	assert.True(t, containString(process.ShardCacherIdentifier(0, 0), txShardedStores))
	assert.True(t, containString(process.ShardCacherIdentifier(0, 1), txShardedStores))
	assert.True(t, containString(process.ShardCacherIdentifier(1, 0), txShardedStores))

	assert.True(t, containString(process.ShardCacherIdentifier(0, 0), headerShardedStores))
}

func containString(search string, list []string) bool {
	for _, str := range list {
		if str == search {
			return true
		}
	}

	return false
}

func getMessenger() *mock.MessengerStub {
	messenger := &mock.MessengerStub{
		CloseCalled: func() error {
			return nil
		},
		BootstrapCalled: func() error {
			return nil
		},
		BroadcastCalled: func(topic string, buff []byte) {
			return
		},
	}

	return messenger
}

func TestNode_BroadcastBlockShouldFailWhenTxBlockBodyNil(t *testing.T) {
	n, _ := node.NewNode()
	messenger := getMessenger()
	bp := &mock.BlockProcessorStub{MarshalizedDataForCrossShardCalled: func(body data.BodyHandler) (bytes map[uint32][]byte, tx map[uint32][][]byte, e error) {
		return make(map[uint32][]byte, 1), make(map[uint32][][]byte, 1), nil
	}}

	_ = n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithBlockProcessor(bp),
	)

	err := n.BroadcastBlock(nil, &block.Header{})
	assert.Equal(t, node.ErrNilTxBlockBody, err)
}

func TestNode_BroadcastBlockShouldFailWhenMarshalTxBlockBodyErr(t *testing.T) {
	n, _ := node.NewNode()
	messenger := getMessenger()

	marshalizerMock := mock.MarshalizerMock{}
	err := errors.New("error marshal tx vlock body")
	marshalizerMock.MarshalHandler = func(obj interface{}) ([]byte, error) {
		switch obj.(type) {
		case block.Body:
			return nil, err
		}

		return []byte("marshalized ok"), nil
	}
	bp := &mock.BlockProcessorStub{MarshalizedDataForCrossShardCalled: func(body data.BodyHandler) (bytes map[uint32][]byte, tx map[uint32][][]byte, e error) {
		return make(map[uint32][]byte, 1), make(map[uint32][][]byte, 1), nil
	}}

	_ = n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(marshalizerMock),
		node.WithBlockProcessor(bp),
	)

	txHash0 := []byte("txHash0")
	mb0 := block.MiniBlock{
		ReceiverShardID: 0,
		SenderShardID:   0,
		TxHashes:        [][]byte{[]byte(txHash0)},
	}

	body := make(block.Body, 0)
	body = append(body, &mb0)

	err2 := n.BroadcastBlock(body, &block.Header{})
	assert.Equal(t, err, err2)
}

type wrongBody struct {
}

func (wr wrongBody) IntegrityAndValidity() error {
	return nil
}

func TestNode_BroadcastBlockShouldFailWhenBlockIsNotGoodType(t *testing.T) {
	n, _ := node.NewNode()
	messenger := getMessenger()
	bp := &mock.BlockProcessorStub{MarshalizedDataForCrossShardCalled: func(body data.BodyHandler) (bytes map[uint32][]byte, tx map[uint32][][]byte, e error) {
		return nil, nil, process.ErrWrongTypeAssertion
	}}
	_ = n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
		node.WithBlockProcessor(bp),
	)

	wr := wrongBody{}
	err := n.BroadcastBlock(wr, &block.Header{})
	assert.Equal(t, process.ErrWrongTypeAssertion, err)
}

func TestNode_BroadcastBlockShouldFailWhenHeaderNil(t *testing.T) {
	n, _ := node.NewNode()
	messenger := getMessenger()
	bp := &mock.BlockProcessorStub{MarshalizedDataForCrossShardCalled: func(body data.BodyHandler) (bytes map[uint32][]byte, tx map[uint32][][]byte, e error) {
		return make(map[uint32][]byte, 1), make(map[uint32][][]byte, 1), nil
	}}
	_ = n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
		node.WithBlockProcessor(bp),
	)

	txHash0 := []byte("txHash0")
	mb0 := block.MiniBlock{
		ReceiverShardID: 0,
		SenderShardID:   0,
		TxHashes:        [][]byte{[]byte(txHash0)},
	}

	body := make(block.Body, 0)
	body = append(body, &mb0)

	err := n.BroadcastBlock(body, nil)
	assert.Equal(t, node.ErrNilBlockHeader, err)
}

func TestNode_BroadcastBlockShouldFailWhenMarshalHeaderErr(t *testing.T) {
	n, _ := node.NewNode()
	messenger := getMessenger()

	marshalizerMock := mock.MarshalizerMock{}
	err := errors.New("error marshal header")
	marshalizerMock.MarshalHandler = func(obj interface{}) ([]byte, error) {
		switch obj.(type) {
		case *block.Header:
			return nil, err
		}

		return []byte("marshalized ok"), nil
	}

	bp := &mock.BlockProcessorStub{MarshalizedDataForCrossShardCalled: func(body data.BodyHandler) (bytes map[uint32][]byte, tx map[uint32][][]byte, e error) {
		return make(map[uint32][]byte, 1), make(map[uint32][][]byte, 1), nil
	}}

	_ = n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(marshalizerMock),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
		node.WithBlockProcessor(bp),
	)

	txHash0 := []byte("txHash0")
	mb0 := block.MiniBlock{
		ReceiverShardID: 0,
		SenderShardID:   0,
		TxHashes:        [][]byte{[]byte(txHash0)},
	}

	body := make(block.Body, 0)
	body = append(body, &mb0)

	err2 := n.BroadcastBlock(body, &block.Header{})
	assert.Equal(t, err, err2)
}

func TestNode_BroadcastBlockShouldWorkWithOneShard(t *testing.T) {
	n, _ := node.NewNode()
	messenger := getMessenger()
	bp := &mock.BlockProcessorStub{MarshalizedDataForCrossShardCalled: func(body data.BodyHandler) (bytes map[uint32][]byte, tx map[uint32][][]byte, e error) {
		return make(map[uint32][]byte, 1), make(map[uint32][][]byte, 1), nil
	}}
	_ = n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
		node.WithBlockProcessor(bp),
	)

	txHash0 := []byte("txHash0")
	mb0 := block.MiniBlock{
		ReceiverShardID: 0,
		SenderShardID:   0,
		TxHashes:        [][]byte{[]byte(txHash0)},
	}

	body := make(block.Body, 0)
	body = append(body, &mb0)

	err := n.BroadcastBlock(body, &block.Header{})
	assert.Nil(t, err)
}

func TestNode_BroadcastBlockShouldWorkMultiShard(t *testing.T) {
	n, _ := node.NewNode()
	messenger := getMessenger()
	bp := &mock.BlockProcessorStub{MarshalizedDataForCrossShardCalled: func(body data.BodyHandler) (bytes map[uint32][]byte, tx map[uint32][][]byte, e error) {
		return make(map[uint32][]byte, 0), make(map[uint32][][]byte, 1), nil
	}}
	_ = n.ApplyOptions(
		node.WithMessenger(messenger),
		node.WithMarshalizer(mock.MarshalizerMock{}),
		node.WithShardCoordinator(mock.NewOneShardCoordinatorMock()),
		node.WithBlockProcessor(bp),
	)

	txHash0 := []byte("txHash0")
	mb0 := block.MiniBlock{
		ReceiverShardID: 0,
		SenderShardID:   0,
		TxHashes:        [][]byte{[]byte(txHash0)},
	}

	txHash1 := []byte("txHash1")
	mb1 := block.MiniBlock{
		ReceiverShardID: 1,
		SenderShardID:   0,
		TxHashes:        [][]byte{[]byte(txHash1)},
	}

	body := make(block.Body, 0)
	body = append(body, &mb0)
	body = append(body, &mb0)
	body = append(body, &mb0)
	body = append(body, &mb1)
	body = append(body, &mb1)
	body = append(body, &mb1)
	body = append(body, &mb1)
	body = append(body, &mb1)

	err := n.BroadcastBlock(body, &block.Header{})
	assert.Nil(t, err)
}
