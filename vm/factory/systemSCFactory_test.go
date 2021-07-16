package factory

import (
	"errors"
	"fmt"
	"testing"

	arwenConfig "github.com/ElrondNetwork/arwen-wasm-vm/v1_4/config"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/vm"
	"github.com/ElrondNetwork/elrond-go/vm/mock"
	"github.com/ElrondNetwork/elrond-go/vm/systemSmartContracts/defaults"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockNewSystemScFactoryArgs() ArgsNewSystemSCFactory {
	gasMap := arwenConfig.MakeGasMapForTests()
	gasMap = defaults.FillGasMapInternal(gasMap, 1)
	gasSchedule := mock.NewGasScheduleNotifierMock(gasMap)
	return ArgsNewSystemSCFactory{
		SystemEI:            &mock.SystemEIStub{},
		Economics:           &mock.EconomicsHandlerStub{},
		SigVerifier:         &mock.MessageSignVerifierMock{},
		GasSchedule:         gasSchedule,
		NodesConfigProvider: &mock.NodesConfigProviderStub{},
		Marshalizer:         &mock.MarshalizerMock{},
		Hasher:              &mock.HasherMock{},
		SystemSCConfig: &config.SystemSmartContractsConfig{
			ESDTSystemSCConfig: config.ESDTSystemSCConfig{
				BaseIssuingCost: "100000000",
				OwnerAddress:    "aaaaaa",
			},
			GovernanceSystemSCConfig: config.GovernanceSystemSCConfig{
				Active: config.GovernanceSystemSCConfigActive{
					ProposalCost:     "500",
					MinQuorum:        "50",
					MinPassThreshold: "50",
					MinVetoThreshold: "50",
				},
				FirstWhitelistedAddress: "3132333435363738393031323334353637383930313233343536373839303234",
			},
			StakingSystemSCConfig: config.StakingSystemSCConfig{
				GenesisNodePrice:                     "1000",
				UnJailValue:                          "10",
				MinStepValue:                         "10",
				MinStakeValue:                        "1",
				UnBondPeriod:                         1,
				NumRoundsWithoutBleed:                1,
				MaximumPercentageToBleed:             1,
				BleedPercentagePerRound:              1,
				MaxNumberOfNodesForStake:             100,
				ActivateBLSPubKeyMessageVerification: false,
				MinUnstakeTokensValue:                "1",
			},
			DelegationSystemSCConfig: config.DelegationSystemSCConfig{
				MinServiceFee: 0,
				MaxServiceFee: 10000,
			},
			DelegationManagerSystemSCConfig: config.DelegationManagerSystemSCConfig{
				MinCreationDeposit:  "10",
				MinStakeAmount:      "10",
				ConfigChangeAddress: "3132333435363738393031323334353637383930313233343536373839303234",
			},
		},
		EpochNotifier:          &mock.EpochNotifierStub{},
		AddressPubKeyConverter: &mock.PubkeyConverterMock{},
		EpochConfig: &config.EpochConfig{
			EnableEpochs: config.EnableEpochs{
				StakingV2EnableEpoch:               1,
				StakeEnableEpoch:                   0,
				DelegationSmartContractEnableEpoch: 0,
				DelegationManagerEnableEpoch:       0,
			},
		},
		ShardCoordinator: &mock.ShardCoordinatorStub{},
	}
}

func TestNewSystemSCFactory_NilSystemEI(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.SystemEI = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilSystemEnvironmentInterface))
}

func TestNewSystemSCFactory_NilSigVerifier(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.SigVerifier = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilMessageSignVerifier))
}

func TestNewSystemSCFactory_NilNodesConfigProvider(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.NodesConfigProvider = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilNodesConfigProvider))
}

func TestNewSystemSCFactory_NilMarshalizer(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.Marshalizer = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilMarshalizer))
}

func TestNewSystemSCFactory_NilHasher(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.Hasher = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilHasher))
}

func TestNewSystemSCFactory_NilEconomicsData(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.Economics = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilEconomicsData))
}

func TestNewSystemSCFactory_NilSystemScConfig(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.SystemSCConfig = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilSystemSCConfig))
}

func TestNewSystemSCFactory_NilEpochNotifier(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.EpochNotifier = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilEpochNotifier))
}

func TestNewSystemSCFactory_NilPubKeyConverter(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.AddressPubKeyConverter = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, scFactory)
	assert.True(t, errors.Is(err, vm.ErrNilAddressPubKeyConverter))
}

func TestNewSystemSCFactory_NilShardCoordinator(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.ShardCoordinator = nil
	scFactory, err := NewSystemSCFactory(arguments)

	assert.True(t, check.IfNil(scFactory))
	assert.True(t, errors.Is(err, vm.ErrNilShardCoordinator))
}

func TestNewSystemSCFactory_Ok(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	scFactory, err := NewSystemSCFactory(arguments)

	assert.Nil(t, err)
	assert.NotNil(t, scFactory)
}

func TestNewSystemSCFactory_GasScheduleChangeMissingElementsShouldNotPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r != nil {
			assert.Fail(t, fmt.Sprintf("should have not panicked: %v", r))
		}
	}()

	arguments := createMockNewSystemScFactoryArgs()
	scFactory, _ := NewSystemSCFactory(arguments)

	gasSchedule, err := core.LoadGasScheduleConfig("../../cmd/node/config/gasSchedules/gasScheduleV3.toml")
	delete(gasSchedule["MetaChainSystemSCsCost"], "UnstakeTokens")
	require.Nil(t, err)

	scFactory.GasScheduleChange(gasSchedule)

	assert.Equal(t, uint64(1), scFactory.gasCost.MetaChainSystemSCsCost.UnStakeTokens)
}

func TestNewSystemSCFactory_GasScheduleChangeShouldWork(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r != nil {
			assert.Fail(t, fmt.Sprintf("should have not panicked: %v", r))
		}
	}()

	arguments := createMockNewSystemScFactoryArgs()
	scFactory, _ := NewSystemSCFactory(arguments)

	gasSchedule, err := core.LoadGasScheduleConfig("../../cmd/node/config/gasSchedules/gasScheduleV3.toml")
	require.Nil(t, err)

	scFactory.GasScheduleChange(gasSchedule)

	assert.Equal(t, uint64(5000000), scFactory.gasCost.MetaChainSystemSCsCost.UnStakeTokens)
}

func TestSystemSCFactory_CreateWithBadDelegationManagerConfigChangeAddressShouldError(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.SystemSCConfig.DelegationManagerSystemSCConfig.ConfigChangeAddress = "not a hex string"
	scFactory, _ := NewSystemSCFactory(arguments)

	container, err := scFactory.Create()

	assert.True(t, check.IfNil(container))
	assert.True(t, errors.Is(err, vm.ErrInvalidAddress))
}

func TestSystemSCFactory_CreateWithFirstWhiteListAddressShouldError(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	arguments.SystemSCConfig.GovernanceSystemSCConfig.FirstWhitelistedAddress = "not a hex string"
	scFactory, _ := NewSystemSCFactory(arguments)

	container, err := scFactory.Create()

	assert.True(t, check.IfNil(container))
	assert.True(t, errors.Is(err, vm.ErrInvalidAddress))
}

func TestSystemSCFactory_Create(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	scFactory, _ := NewSystemSCFactory(arguments)

	container, err := scFactory.Create()
	assert.Nil(t, err)
	require.NotNil(t, container)
	assert.Equal(t, 6, container.Len())
}

func TestSystemSCFactory_CreateForGenesis(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	scFactory, _ := NewSystemSCFactory(arguments)

	container, err := scFactory.CreateForGenesis()
	assert.Nil(t, err)
	assert.Equal(t, 4, container.Len())
}

func TestSystemSCFactory_IsInterfaceNil(t *testing.T) {
	t.Parallel()

	arguments := createMockNewSystemScFactoryArgs()
	scFactory, _ := NewSystemSCFactory(arguments)
	assert.False(t, scFactory.IsInterfaceNil())

	scFactory = nil
	assert.True(t, check.IfNil(scFactory))
}
