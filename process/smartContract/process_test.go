package smartContract

import (
	"bytes"
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/ElrondNetwork/elrond-go-sandbox/process"
	"github.com/ElrondNetwork/elrond-go-sandbox/process/mock"
	"github.com/ElrondNetwork/elrond-vm-common"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func generateRandomByteSlice(size int) []byte {
	buff := make([]byte, size)
	_, _ = rand.Reader.Read(buff)

	return buff
}

func generateEmptyByteSlice(size int) []byte {
	buff := make([]byte, size)

	return buff
}

func createAccounts(tx *transaction.Transaction) (state.AccountHandler, state.AccountHandler) {
	journalizeCalled := 0
	saveAccountCalled := 0
	tracker := &mock.AccountTrackerStub{
		JournalizeCalled: func(entry state.JournalEntry) {
			journalizeCalled++
		},
		SaveAccountCalled: func(accountHandler state.AccountHandler) error {
			saveAccountCalled++
			return nil
		},
	}

	acntSrc, _ := state.NewAccount(mock.NewAddressMock(tx.SndAddr), tracker)
	acntSrc.Balance = acntSrc.Balance.Add(acntSrc.Balance, tx.Value)
	totalFee := big.NewInt(0)
	totalFee = totalFee.Mul(big.NewInt(int64(tx.GasLimit)), big.NewInt(int64(tx.GasPrice)))
	acntSrc.Balance = acntSrc.Balance.Add(acntSrc.Balance, totalFee)

	acntDst, _ := state.NewAccount(mock.NewAddressMock(tx.RcvAddr), tracker)

	return acntSrc, acntDst
}

func TestNewSmartContractProcessorNilVM(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		nil,
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNoVM, err)
}

func TestNewSmartContractProcessorNilArgsParser(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		nil,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNilArgumentParser, err)
}

func TestNewSmartContractProcessorNilHasher(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		nil,
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNilHasher, err)
}

func TestNewSmartContractProcessorNilMarshalizer(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		nil,
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNilMarshalizer, err)
}

func TestNewSmartContractProcessorNilAccountsDB(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		nil,
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNilAccountsAdapter, err)
}

func TestNewSmartContractProcessorNilAdrConv(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		nil,
		mock.NewMultiShardsCoordinatorMock(5))

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNilAddressConverter, err)
}

func TestNewSmartContractProcessorNilShardCoordinator(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		nil)

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNilShardCoordinator, err)
}

func TestNewSmartContractProcessorNilFakeAccountsHandler(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		nil,
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.Nil(t, sc)
	assert.Equal(t, process.ErrNilFakeAccountsHandler, err)
}

func TestNewSmartContractProcessor(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)
}

func TestScProcessor_ComputeTransactionTypeNil(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	_, err = sc.ComputeTransactionType(nil)
	assert.Equal(t, process.ErrNilTransaction, err)
}

func TestScProcessor_ComputeTransactionTypeNilTx(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Value = big.NewInt(45)

	tx = nil
	_, err = sc.ComputeTransactionType(tx)
	assert.Equal(t, process.ErrNilTransaction, err)
}

func TestScProcessor_ComputeTransactionTypeErrWrongTransaction(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = nil
	tx.Value = big.NewInt(45)

	_, err = sc.ComputeTransactionType(tx)
	assert.Equal(t, process.ErrWrongTransaction, err)
}

func TestScProcessor_ComputeTransactionTypeScDeployment(t *testing.T) {
	t.Parallel()

	addressConverter := &mock.AddressConverterMock{}
	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		addressConverter,
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = make([]byte, addressConverter.AddressLen())
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	txType, err := sc.ComputeTransactionType(tx)
	assert.Nil(t, err)
	assert.Equal(t, process.SCDeployment, txType)
}

func TestScProcessor_ComputeTransactionTypeScInvoking(t *testing.T) {
	t.Parallel()

	addrConverter := &mock.AddressConverterMock{}
	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = generateRandomByteSlice(addrConverter.AddressLen())
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	_, acntDst := createAccounts(tx)
	acntDst.SetCode([]byte("code"))

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{GetAccountWithJournalCalled: func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
			return acntDst, nil
		}},
		&mock.FakeAccountsHandlerMock{},
		addrConverter,
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	txType, err := sc.ComputeTransactionType(tx)
	assert.Nil(t, err)
	assert.Equal(t, process.SCInvoking, txType)
}

func TestScProcessor_ComputeTransactionTypeMoveBalance(t *testing.T) {
	t.Parallel()

	addrConverter := &mock.AddressConverterMock{}
	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = generateRandomByteSlice(addrConverter.AddressLen())
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	_, acntDst := createAccounts(tx)

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{GetAccountWithJournalCalled: func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
			return acntDst, nil
		}},
		&mock.FakeAccountsHandlerMock{},
		addrConverter,
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	txType, err := sc.ComputeTransactionType(tx)
	assert.Nil(t, err)
	assert.Equal(t, process.MoveBalance, txType)
}

func TestScProcessor_DeploySmartContractBadParse(t *testing.T) {
	t.Parallel()

	addrConverter := &mock.AddressConverterMock{}
	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		addrConverter,
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = generateEmptyByteSlice(addrConverter.AddressLen())
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, _ := createAccounts(tx)

	tmpError := errors.New("error")
	argParser.ParseDataCalled = func(data []byte) error {
		return tmpError
	}
	err = sc.DeploySmartContract(tx, acntSrc, 10)
	assert.Equal(t, tmpError, err)
}

func TestScProcessor_DeploySmartContractRunError(t *testing.T) {
	t.Parallel()

	addrConverter := &mock.AddressConverterMock{}
	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		addrConverter,
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = generateEmptyByteSlice(addrConverter.AddressLen())
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, _ := createAccounts(tx)

	tmpError := errors.New("error")
	vm.RunSmartContractCreateCalled = func(input *vmcommon.ContractCreateInput) (output *vmcommon.VMOutput, e error) {
		return nil, tmpError
	}
	err = sc.DeploySmartContract(tx, acntSrc, 10)
	assert.Equal(t, tmpError, err)
}

func TestScProcessor_DeploySmartContractWrongTx(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, _ := createAccounts(tx)

	err = sc.DeploySmartContract(tx, acntSrc, 10)
	assert.Equal(t, process.ErrWrongTransaction, err)
}

func TestScProcessor_DeploySmartContract(t *testing.T) {
	t.Parallel()

	addrConverter := &mock.AddressConverterMock{}
	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		addrConverter,
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = generateEmptyByteSlice(addrConverter.AddressLen())
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, _ := createAccounts(tx)

	err = sc.DeploySmartContract(tx, acntSrc, 10)
	assert.Equal(t, nil, err)
}

func TestScProcessor_ExecuteSmartContractTransactionNilTx(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, acntDst := createAccounts(tx)

	err = sc.ExecuteSmartContractTransaction(nil, acntSrc, acntDst, 10)
	assert.Equal(t, process.ErrNilTransaction, err)
}

func TestScProcessor_ExecuteSmartContractTransactionNilAccount(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, acntDst := createAccounts(tx)

	err = sc.ExecuteSmartContractTransaction(tx, acntSrc, nil, 10)
	assert.Equal(t, process.ErrNilSCDestAccount, err)

	acntDst.SetCode(nil)
	err = sc.ExecuteSmartContractTransaction(tx, acntSrc, acntDst, 10)
	assert.Equal(t, process.ErrNilSCDestAccount, err)

	acntDst = nil
	err = sc.ExecuteSmartContractTransaction(tx, acntSrc, acntDst, 10)
	assert.Equal(t, process.ErrNilSCDestAccount, err)
}

func TestScProcessor_ExecuteSmartContractTransactionBadParser(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, acntDst := createAccounts(tx)

	acntDst.SetCode([]byte("code"))
	tmpError := errors.New("error")
	argParser.ParseDataCalled = func(data []byte) error {
		return tmpError
	}
	err = sc.ExecuteSmartContractTransaction(tx, acntSrc, acntDst, 10)
	assert.Equal(t, tmpError, err)
}

func TestScProcessor_ExecuteSmartContractTransactionVMRunError(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, acntDst := createAccounts(tx)

	acntDst.SetCode([]byte("code"))
	tmpError := errors.New("error")
	vm.RunSmartContractCallCalled = func(input *vmcommon.ContractCallInput) (output *vmcommon.VMOutput, e error) {
		return nil, tmpError
	}
	err = sc.ExecuteSmartContractTransaction(tx, acntSrc, acntDst, 10)
	assert.Equal(t, tmpError, err)
}

func TestScProcessor_ExecuteSmartContractTransaction(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)
	acntSrc, acntDst := createAccounts(tx)

	acntDst.SetCode([]byte("code"))
	err = sc.ExecuteSmartContractTransaction(tx, acntSrc, acntDst, 10)
	assert.Nil(t, err)
}

func TestScProcessor_CreateVMCallInputWrongCode(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	tmpError := errors.New("error")
	argParser.GetFunctionCalled = func() (s string, e error) {
		return "", tmpError
	}
	vmInput, err := sc.CreateVMCallInput(tx)
	assert.Nil(t, vmInput)
	assert.Equal(t, tmpError, err)
}

func TestScProcessor_CreateVMCallInput(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	vmInput, err := sc.CreateVMCallInput(tx)
	assert.NotNil(t, vmInput)
	assert.Nil(t, err)
}

func TestScProcessor_CreateVMDeployInputBadFunction(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	tmpError := errors.New("error")
	argParser.GetCodeCalled = func() (code []byte, e error) {
		return nil, tmpError
	}

	vmInput, err := sc.CreateVMDeployInput(tx)
	assert.Nil(t, vmInput)
	assert.Equal(t, tmpError, err)
}

func TestScProcessor_CreateVMDeployInput(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	vmInput, err := sc.CreateVMDeployInput(tx)
	assert.NotNil(t, vmInput)
	assert.Nil(t, err)
}

func TestScProcessor_CreateVMInputWrongArgument(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	tmpError := errors.New("error")
	argParser.GetArgumentsCalled = func() (ints []*big.Int, e error) {
		return nil, tmpError
	}
	vmInput, err := sc.CreateVMInput(tx)
	assert.Nil(t, vmInput)
	assert.Equal(t, tmpError, err)
}

func TestScProcessor_CreateVMInput(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	vmInput, err := sc.CreateVMInput(tx)
	assert.NotNil(t, vmInput)
	assert.Equal(t, nil, err)
}

func createAccountsAndTransaction() (*state.Account, *state.Account, *transaction.Transaction) {
	tx := &transaction.Transaction{}
	tx.Nonce = 0
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")
	tx.Data = []byte("data")
	tx.Value = big.NewInt(45)

	acntSrc, acntDst := createAccounts(tx)

	return acntSrc.(*state.Account), acntDst.(*state.Account), tx
}

func TestScProcessor_processVMOutputNilVMOutput(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acntSrc, _, tx := createAccountsAndTransaction()

	err = sc.processVMOutput(nil, tx, acntSrc, 10)
	assert.Equal(t, process.ErrNilVMOutput, err)
}

func TestScProcessor_processVMOutputNilTx(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acntSrc, _, _ := createAccountsAndTransaction()

	vmOutput := &vmcommon.VMOutput{}
	err = sc.processVMOutput(vmOutput, nil, acntSrc, 10)
	assert.Equal(t, process.ErrNilTransaction, err)
}

func TestScProcessor_processVMOutputNilSndAcc(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	_, _, tx := createAccountsAndTransaction()

	vmOutput := &vmcommon.VMOutput{
		GasRefund:    big.NewInt(0),
		GasRemaining: big.NewInt(0),
	}
	err = sc.processVMOutput(vmOutput, tx, nil, 10)
	assert.Nil(t, err)
}

func TestScProcessor_processVMOutputNilDstAcc(t *testing.T) {
	t.Parallel()

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acntSnd, _, tx := createAccountsAndTransaction()

	vmOutput := &vmcommon.VMOutput{
		GasRefund:    big.NewInt(0),
		GasRemaining: big.NewInt(0),
	}
	err = sc.processVMOutput(vmOutput, tx, acntSnd, 10)
	assert.Nil(t, err)
}

func TestScProcessor_GetAccountFromAddressAccNotFound(t *testing.T) {
	t.Parallel()

	accountsDB := &mock.AccountsStub{}
	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		return nil, state.ErrAccNotFound
	}

	addrConv := &mock.AddressConverterMock{}
	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		return shardCoordinator.SelfId()
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acc, err := sc.GetAccountFromAddress([]byte("SRC"))
	assert.Nil(t, acc)
	assert.Equal(t, state.ErrAccNotFound, err)
}

func TestScProcessor_GetAccountFromAddrFaildAddressConv(t *testing.T) {
	t.Parallel()

	addrConv := &mock.AddressConverterMock{}
	addrConv.Fail = true

	accountsDB := &mock.AccountsStub{}
	getCalled := 0
	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		getCalled++
		return nil, state.ErrAccNotFound
	}

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		return shardCoordinator.SelfId()
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acc, err := sc.GetAccountFromAddress([]byte("DST"))
	assert.Nil(t, acc)
	assert.NotNil(t, err)
	assert.Equal(t, 0, getCalled)
}

func TestScProcessor_GetAccountFromAddrFailedGetExistingAccount(t *testing.T) {
	t.Parallel()

	addrConv := &mock.AddressConverterMock{}

	accountsDB := &mock.AccountsStub{}
	getCalled := 0
	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		getCalled++
		return nil, state.ErrAccNotFound
	}

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		return shardCoordinator.SelfId()
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acc, err := sc.GetAccountFromAddress([]byte("DST"))
	assert.Nil(t, acc)
	assert.Equal(t, state.ErrAccNotFound, err)
	assert.Equal(t, 1, getCalled)
}

func TestScProcessor_GetAccountFromAddrAccNotInShard(t *testing.T) {
	t.Parallel()

	addrConv := &mock.AddressConverterMock{}

	accountsDB := &mock.AccountsStub{}
	getCalled := 0
	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		getCalled++
		return nil, state.ErrAccNotFound
	}

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		return shardCoordinator.SelfId() + 1
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acc, err := sc.GetAccountFromAddress([]byte("DST"))
	assert.Nil(t, acc)
	assert.Nil(t, err)
	assert.Equal(t, 0, getCalled)
}

func TestScProcessor_GetAccountFromAddr(t *testing.T) {
	t.Parallel()

	addrConv := &mock.AddressConverterMock{}

	accountsDB := &mock.AccountsStub{}
	getCalled := 0
	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		getCalled++
		acc, _ := state.NewAccount(addressContainer, &mock.AccountTrackerStub{})
		return acc, nil
	}

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		return shardCoordinator.SelfId()
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	acc, err := sc.GetAccountFromAddress([]byte("DST"))
	assert.NotNil(t, acc)
	assert.Nil(t, err)
	assert.Equal(t, 1, getCalled)
}

func TestScProcessor_DeleteAccountsFailedAtRemove(t *testing.T) {
	t.Parallel()
	addrConv := &mock.AddressConverterMock{}

	accountsDB := &mock.AccountsStub{}
	removeCalled := 0
	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		return nil, state.ErrAccNotFound
	}
	accountsDB.RemoveAccountCalled = func(addressContainer state.AddressContainer) error {
		removeCalled++
		return state.ErrAccNotFound
	}

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		return shardCoordinator.SelfId()
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	deletedAccounts := make([][]byte, 0)
	deletedAccounts = append(deletedAccounts, []byte("acc1"), []byte("acc2"), []byte("acc3"))
	err = sc.DeleteAccounts(deletedAccounts)
	assert.Equal(t, state.ErrAccNotFound, err)
	assert.Equal(t, 0, removeCalled)
}

func TestScProcessor_DeleteAccountsNotInShard(t *testing.T) {
	t.Parallel()
	addrConv := &mock.AddressConverterMock{}

	accountsDB := &mock.AccountsStub{}
	removeCalled := 0
	accountsDB.RemoveAccountCalled = func(addressContainer state.AddressContainer) error {
		removeCalled++
		return state.ErrAccNotFound
	}

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	computeIdCalled := 0
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		computeIdCalled++
		return shardCoordinator.SelfId() + 1
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	deletedAccounts := make([][]byte, 0)
	deletedAccounts = append(deletedAccounts, []byte("acc1"), []byte("acc2"), []byte("acc3"))
	err = sc.DeleteAccounts(deletedAccounts)
	assert.Nil(t, err)
	assert.Equal(t, 0, removeCalled)
	assert.Equal(t, len(deletedAccounts), computeIdCalled)
}

func TestScProcessor_DeleteAccountsInShard(t *testing.T) {
	t.Parallel()

	addrConv := &mock.AddressConverterMock{}
	accountsDB := &mock.AccountsStub{}
	removeCalled := 0
	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		acc, _ := state.NewAccount(addressContainer, &mock.AccountTrackerStub{})
		return acc, nil
	}
	accountsDB.RemoveAccountCalled = func(addressContainer state.AddressContainer) error {
		removeCalled++
		return nil
	}

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	computeIdCalled := 0
	shardCoordinator.ComputeIdCalled = func(address state.AddressContainer) uint32 {
		computeIdCalled++
		return shardCoordinator.SelfId()
	}

	vm := &mock.VMExecutionHandlerStub{}
	argParser := &mock.AtArgumentParserMock{}
	sc, err := NewSmartContractProcessor(
		vm,
		argParser,
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		&mock.FakeAccountsHandlerMock{},
		addrConv,
		shardCoordinator)
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	deletedAccounts := make([][]byte, 0)
	deletedAccounts = append(deletedAccounts, []byte("acc1"), []byte("acc2"), []byte("acc3"))
	err = sc.DeleteAccounts(deletedAccounts)
	assert.Nil(t, err)
	assert.Equal(t, len(deletedAccounts), removeCalled)
	assert.Equal(t, len(deletedAccounts), computeIdCalled)
}

func TestScProcessor_ProcessSCPaymentAccNotInShard(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 1
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")

	tx.Value = big.NewInt(45)
	tx.GasPrice = 10
	tx.GasLimit = 10

	totalPayed, err := sc.ProcessSCPayment(tx, nil)
	assert.Nil(t, err)
	assert.Equal(t, tx.Value.Uint64()+(tx.GasLimit*tx.GasPrice), totalPayed.Uint64())

	acntSnd, _ := createAccounts(tx)
	acntSnd = nil

	totalPayed, err = sc.ProcessSCPayment(tx, acntSnd)
	assert.Nil(t, err)
	assert.Equal(t, tx.Value.Uint64()+(tx.GasLimit*tx.GasPrice), totalPayed.Uint64())
}

func TestScProcessor_ProcessSCPaymentWrongTypeAssertion(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 1
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")

	tx.Value = big.NewInt(45)
	tx.GasPrice = 10
	tx.GasLimit = 10

	acntSrc := &mock.AccountWrapMock{}

	totalPayed, err := sc.ProcessSCPayment(tx, acntSrc)
	assert.Equal(t, process.ErrWrongTypeAssertion, err)
	assert.Nil(t, totalPayed)
}

func TestScProcessor_ProcessSCPaymentNotEnoughBalance(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 1
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")

	tx.Value = big.NewInt(45)
	tx.GasPrice = 10
	tx.GasLimit = 15

	acntSrc, _ := createAccounts(tx)
	stAcc, _ := acntSrc.(*state.Account)
	stAcc.Balance = big.NewInt(45)

	currBalance := acntSrc.(*state.Account).Balance.Uint64()

	totalPayed, err := sc.ProcessSCPayment(tx, acntSrc)
	assert.Equal(t, process.ErrInsufficientFunds, err)
	assert.Nil(t, totalPayed)
	assert.Equal(t, currBalance, acntSrc.(*state.Account).Balance.Uint64())
}

func TestScProcessor_ProcessSCPayment(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 1
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")

	tx.Value = big.NewInt(45)
	tx.GasPrice = 10
	tx.GasLimit = 10

	toPay := tx.Value.Uint64() + tx.GasLimit*tx.GasLimit

	acntSrc, _ := createAccounts(tx)
	currBalance := acntSrc.(*state.Account).Balance.Uint64()
	modifiedBalance := currBalance - tx.Value.Uint64() - tx.GasLimit*tx.GasLimit

	totalPayed, err := sc.ProcessSCPayment(tx, acntSrc)
	assert.Nil(t, err)
	assert.Equal(t, toPay, totalPayed.Uint64())
	assert.Equal(t, modifiedBalance, acntSrc.(*state.Account).Balance.Uint64())
}

func TestScProcessor_RefundGasToSenderNilAndZeroRefund(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 1
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")

	tx.Value = big.NewInt(45)
	tx.GasPrice = 10
	tx.GasLimit = 10

	acntSrc, _ := createAccounts(tx)
	currBalance := acntSrc.(*state.Account).Balance.Uint64()

	err = sc.refundGasToSender(nil, tx, acntSrc)
	assert.Nil(t, err)
	assert.Equal(t, currBalance, acntSrc.(*state.Account).Balance.Uint64())

	err = sc.refundGasToSender(big.NewInt(0), tx, acntSrc)
	assert.Nil(t, err)
	assert.Equal(t, currBalance, acntSrc.(*state.Account).Balance.Uint64())
}

func TestScProcessor_RefundGasToSenderAccNotInShard(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 1
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")

	tx.Value = big.NewInt(45)
	tx.GasPrice = 10
	tx.GasLimit = 10

	acntSrc, _ := createAccounts(tx)

	err = sc.refundGasToSender(big.NewInt(10), tx, nil)
	assert.Nil(t, err)

	acntSrc = nil
	err = sc.refundGasToSender(big.NewInt(10), tx, acntSrc)
	assert.Nil(t, err)

	badAcc := &mock.AccountWrapMock{}
	err = sc.refundGasToSender(big.NewInt(10), tx, badAcc)
	assert.Equal(t, process.ErrWrongTypeAssertion, err)
}

func TestScProcessor_RefundGasToSender(t *testing.T) {
	t.Parallel()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	tx := &transaction.Transaction{}
	tx.Nonce = 1
	tx.SndAddr = []byte("SRC")
	tx.RcvAddr = []byte("DST")

	tx.Value = big.NewInt(45)
	tx.GasPrice = 10
	tx.GasLimit = 15

	acntSrc, _ := createAccounts(tx)
	currBalance := acntSrc.(*state.Account).Balance.Uint64()

	refundGas := big.NewInt(10)
	err = sc.refundGasToSender(refundGas, tx, acntSrc)
	assert.Nil(t, err)

	totalRefund := refundGas.Uint64() * tx.GasPrice
	assert.Equal(t, currBalance+totalRefund, acntSrc.(*state.Account).Balance.Uint64())
}

func TestScProcessor_processVMOutputNilOutput(t *testing.T) {
	t.Parallel()

	round := uint32(10)
	acntSrc, _, tx := createAccountsAndTransaction()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	err = sc.ProcessVMOutput(nil, tx, acntSrc, round)

	assert.Equal(t, process.ErrNilVMOutput, err)
}

func TestScProcessor_processVMOutputNilTransaction(t *testing.T) {
	t.Parallel()

	round := uint32(10)
	acntSrc, _, _ := createAccountsAndTransaction()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	vmOutput := &vmcommon.VMOutput{}
	err = sc.ProcessVMOutput(vmOutput, nil, acntSrc, round)

	assert.Equal(t, process.ErrNilTransaction, err)
}

func TestScProcessor_processVMOutput(t *testing.T) {
	t.Parallel()

	round := uint32(10)
	acntSrc, _, tx := createAccountsAndTransaction()

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		&mock.AccountsStub{},
		&mock.FakeAccountsHandlerMock{},
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	vmOutput := &vmcommon.VMOutput{
		GasRefund:    big.NewInt(0),
		GasRemaining: big.NewInt(0),
	}
	err = sc.ProcessVMOutput(vmOutput, tx, acntSrc, round)
	assert.Nil(t, err)
}

func TestScProcessor_processSCOutputAccounts(t *testing.T) {
	t.Parallel()

	accountsDB := &mock.AccountsStub{}
	fakeAccountsHandler := &mock.FakeAccountsHandlerMock{}
	accTracker := &mock.AccountTrackerStub{}

	sc, err := NewSmartContractProcessor(
		&mock.VMExecutionHandlerStub{},
		&mock.AtArgumentParserMock{},
		&mock.HasherMock{},
		&mock.MarshalizerMock{},
		accountsDB,
		fakeAccountsHandler,
		&mock.AddressConverterMock{},
		mock.NewMultiShardsCoordinatorMock(5))
	assert.NotNil(t, sc)
	assert.Nil(t, err)

	outputAccounts := make([]*vmcommon.OutputAccount, 0)
	err = sc.ProcessSCOutputAccounts(outputAccounts)
	assert.Nil(t, err)

	outaddress := []byte("newsmartcontract")
	outacc1 := &vmcommon.OutputAccount{}
	outacc1.Address = outaddress
	outacc1.Code = []byte("contract-code")
	outacc1.Nonce = big.NewInt(int64(5))
	outacc1.Balance = big.NewInt(int64(5))
	outputAccounts = append(outputAccounts, outacc1)

	testAddr := mock.NewAddressMock(outaddress)
	testAcc, _ := state.NewAccount(testAddr, accTracker)

	accountsDB.GetAccountWithJournalCalled = func(addressContainer state.AddressContainer) (handler state.AccountHandler, e error) {
		if bytes.Equal(addressContainer.Bytes(), testAddr.Bytes()) {
			return testAcc, nil
		}
		return nil, state.ErrAccNotFound
	}
	accountsDB.PutCodeCalled = func(accountHandler state.AccountHandler, code []byte) error {
		return nil
	}

	accTracker.JournalizeCalled = func(entry state.JournalEntry) {
		return
	}
	accTracker.SaveAccountCalled = func(accountHandler state.AccountHandler) error {
		testAcc = accountHandler.(*state.Account)
		return nil
	}

	err = sc.ProcessSCOutputAccounts(outputAccounts)
	assert.Nil(t, err)

	err = sc.ProcessSCOutputAccounts(outputAccounts)
	assert.Nil(t, err)

	outacc1.Balance = nil
	outacc1.Nonce = outacc1.Nonce.Add(outacc1.Nonce, big.NewInt(1))
	err = sc.processSCOutputAccounts(outputAccounts)
	assert.Equal(t, process.ErrNilBalanceFromSC, err)

	outacc1.Nonce = outacc1.Nonce.Add(outacc1.Nonce, big.NewInt(1))
	outacc1.Balance = big.NewInt(int64(10))
	fakeAccountsHandler.GetFakeAccountCalled = func(address []byte) state.AccountHandler {
		fakeAcc, _ := state.NewAccount(mock.NewAddressMock(address), &mock.AccountTrackerStub{})
		fakeAcc.Balance = big.NewInt(int64(5))
		return fakeAcc
	}

	currentBalance := testAcc.Balance.Uint64()
	vmOutBalance := outacc1.Balance.Uint64()
	err = sc.processSCOutputAccounts(outputAccounts)
	assert.Nil(t, err)
	assert.Equal(t, currentBalance+vmOutBalance, testAcc.Balance.Uint64())
}
