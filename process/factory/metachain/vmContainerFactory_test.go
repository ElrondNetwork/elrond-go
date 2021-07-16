package metachain

import (
	"errors"
	"testing"

	arwenConfig "github.com/ElrondNetwork/arwen-wasm-vm/v1_4/config"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/economics"
	"github.com/ElrondNetwork/elrond-go/process/factory"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/hooks"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/testscommon/economicsmocks"
	"github.com/ElrondNetwork/elrond-go/vm"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	vmcommonBuiltInFunctions "github.com/ElrondNetwork/elrond-vm-common/builtInFunctions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockVMAccountsArguments() hooks.ArgBlockChainHook {
	datapool := testscommon.NewPoolsHolderMock()
	arguments := hooks.ArgBlockChainHook{
		Accounts: &testscommon.AccountsStub{
			GetExistingAccountCalled: func(address []byte) (handler vmcommon.AccountHandler, e error) {
				return &mock.AccountWrapMock{}, nil
			},
		},
		PubkeyConv:         mock.NewPubkeyConverterMock(32),
		StorageService:     &mock.ChainStorerMock{},
		BlockChain:         &mock.BlockChainMock{},
		ShardCoordinator:   mock.NewOneShardCoordinatorMock(),
		Marshalizer:        &mock.MarshalizerMock{},
		Uint64Converter:    &mock.Uint64ByteSliceConverterMock{},
		BuiltInFunctions:   vmcommonBuiltInFunctions.NewBuiltInFunctionContainer(),
		DataPool:           datapool,
		CompiledSCPool:     datapool.SmartContracts(),
		NilCompiledSCStore: true,
	}
	return arguments
}

func createVmContainerMockArgument(gasSchedule core.GasScheduleNotifier) ArgsNewVMContainerFactory {
	return ArgsNewVMContainerFactory{
		ArgBlockChainHook:   createMockVMAccountsArguments(),
		Economics:           &economicsmocks.EconomicsHandlerStub{},
		MessageSignVerifier: &mock.MessageSignVerifierMock{},
		GasSchedule:         gasSchedule,
		NodesConfigProvider: &mock.NodesConfigProviderStub{},
		Hasher:              &mock.HasherMock{},
		Marshalizer:         &mock.MarshalizerMock{},
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
				MaxNumberOfNodesForStake:             1,
				ActivateBLSPubKeyMessageVerification: false,
			},
		},
		ValidatorAccountsDB: &testscommon.AccountsStub{},
		ChanceComputer:      &mock.RaterMock{},
		EpochNotifier:       &mock.EpochNotifierStub{},
		EpochConfig: &config.EpochConfig{
			EnableEpochs: config.EnableEpochs{
				StakingV2EnableEpoch: 10,
				StakeEnableEpoch:     0,
			},
		},
		ShardCoordinator: &mock.ShardCoordinatorStub{},
	}
}

func TestNewVMContainerFactory_NilEconomics(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.Economics = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, process.ErrNilEconomicsData))
}

func TestNewVMContainerFactory_NilMessageSignVerifier(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.MessageSignVerifier = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, process.ErrNilKeyGen))
}

func TestNewVMContainerFactory_NilNodesConfigProvider(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.NodesConfigProvider = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, process.ErrNilNodesConfigProvider))
}

func TestNewVMContainerFactory_NilHasher(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.Hasher = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, process.ErrNilHasher))
}

func TestNewVMContainerFactory_NilMarshalizer(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.Marshalizer = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, process.ErrNilMarshalizer))
}

func TestNewVMContainerFactory_NilSystemConfig(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.SystemSCConfig = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, process.ErrNilSystemSCConfig))
}

func TestNewVMContainerFactory_NilValidatorAccountsDB(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.ValidatorAccountsDB = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, vm.ErrNilValidatorAccountsDB))
}

func TestNewVMContainerFactory_NilChanceComputer(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.ChanceComputer = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, vm.ErrNilChanceComputer))
}

func TestNewVMContainerFactory_NilGasSchedule(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.GasSchedule = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, vm.ErrNilGasSchedule))
}

func TestNewVMContainerFactory_NilPubkeyConverter(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.ArgBlockChainHook.PubkeyConv = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, vm.ErrNilAddressPubKeyConverter))
}

func TestNewVMContainerFactory_NilBlockChainHookFails(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.ArgBlockChainHook.Accounts = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, process.ErrNilAccountsAdapter))
}

func TestNewVMContainerFactory_NilShardCoordinator(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	argsNewVmContainerFactory.ShardCoordinator = nil
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.True(t, check.IfNil(vmf))
	assert.True(t, errors.Is(err, vm.ErrNilShardCoordinator))
}

func TestNewVMContainerFactory_OkValues(t *testing.T) {
	t.Parallel()

	gasSchedule := makeGasSchedule()
	argsNewVmContainerFactory := createVmContainerMockArgument(gasSchedule)
	vmf, err := NewVMContainerFactory(argsNewVmContainerFactory)

	assert.False(t, check.IfNil(vmf))
	assert.Nil(t, err)
}

func TestVmContainerFactory_Create(t *testing.T) {
	t.Parallel()

	argsNewEconomicsData := economics.ArgsNewEconomicsData{
		Economics: &config.EconomicsConfig{
			GlobalSettings: config.GlobalSettings{
				GenesisTotalSupply: "2000000000000000000000",
				MinimumInflation:   0,
				YearSettings: []*config.YearSetting{
					{
						Year:             0,
						MaximumInflation: 0.01,
					},
				},
			},
			RewardsSettings: config.RewardsSettings{
				RewardsConfigByEpoch: []config.EpochRewardSettings{
					{
						LeaderPercentage:                 0.1,
						DeveloperPercentage:              0.1,
						ProtocolSustainabilityPercentage: 0.1,
						ProtocolSustainabilityAddress:    "erd1932eft30w753xyvme8d49qejgkjc09n5e49w4mwdjtm0neld797su0dlxp",
						TopUpGradientPoint:               "300000000000000000000",
						TopUpFactor:                      0.25,
					},
				},
			},
			FeeSettings: config.FeeSettings{
				MaxGasLimitPerBlock:     "10000000000",
				MaxGasLimitPerMetaBlock: "10000000000",
				MinGasPrice:             "10",
				MinGasLimit:             "10",
				GasPerDataByte:          "1",
				GasPriceModifier:        1.0,
			},
		},
		PenalizedTooMuchGasEnableEpoch: 0,
		EpochNotifier:                  &mock.EpochNotifierStub{},
		BuiltInFunctionsCostHandler:    &mock.BuiltInCostHandlerStub{},
	}
	economicsData, _ := economics.NewEconomicsData(argsNewEconomicsData)

	argsNewVMContainerFactory := ArgsNewVMContainerFactory{
		ArgBlockChainHook:   createMockVMAccountsArguments(),
		Economics:           economicsData,
		MessageSignVerifier: &mock.MessageSignVerifierMock{},
		GasSchedule:         makeGasSchedule(),
		NodesConfigProvider: &mock.NodesConfigProviderStub{},
		Hasher:              &mock.HasherMock{},
		Marshalizer:         &mock.MarshalizerMock{},
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
				UnJailValue:                          "100",
				MinStepValue:                         "100",
				MinStakeValue:                        "1",
				UnBondPeriod:                         1,
				NumRoundsWithoutBleed:                1,
				MaximumPercentageToBleed:             1,
				BleedPercentagePerRound:              1,
				MaxNumberOfNodesForStake:             100,
				ActivateBLSPubKeyMessageVerification: false,
				MinUnstakeTokensValue:                "1",
			},
			DelegationManagerSystemSCConfig: config.DelegationManagerSystemSCConfig{
				MinCreationDeposit:  "100",
				MinStakeAmount:      "100",
				ConfigChangeAddress: "3132333435363738393031323334353637383930313233343536373839303234",
			},
			DelegationSystemSCConfig: config.DelegationSystemSCConfig{
				MinServiceFee: 0,
				MaxServiceFee: 100,
			},
		},
		ValidatorAccountsDB: &testscommon.AccountsStub{},
		ChanceComputer:      &mock.RaterMock{},
		EpochNotifier:       &mock.EpochNotifierStub{},
		EpochConfig: &config.EpochConfig{
			EnableEpochs: config.EnableEpochs{
				StakingV2EnableEpoch:               10,
				StakeEnableEpoch:                   1,
				DelegationManagerEnableEpoch:       0,
				DelegationSmartContractEnableEpoch: 0,
			},
		},
		ShardCoordinator: mock.NewMultiShardsCoordinatorMock(1),
	}
	vmf, err := NewVMContainerFactory(argsNewVMContainerFactory)
	assert.NotNil(t, vmf)
	assert.Nil(t, err)

	container, err := vmf.Create()
	require.Nil(t, err)
	require.NotNil(t, container)
	defer func() {
		_ = container.Close()
	}()

	assert.Nil(t, err)
	assert.NotNil(t, container)

	vmInstance, err := container.Get(factory.SystemVirtualMachine)
	assert.Nil(t, err)
	assert.NotNil(t, vmInstance)

	acc := vmf.BlockChainHookImpl()
	assert.NotNil(t, acc)
}

func makeGasSchedule() core.GasScheduleNotifier {
	gasSchedule := arwenConfig.MakeGasMapForTests()
	FillGasMapInternal(gasSchedule, 1)
	return mock.NewGasScheduleNotifierMock(gasSchedule)
}

func FillGasMapInternal(gasMap map[string]map[string]uint64, value uint64) map[string]map[string]uint64 {
	gasMap[core.BaseOperationCost] = FillGasMapBaseOperationCosts(value)
	gasMap[core.MetaChainSystemSCsCost] = FillGasMapMetaChainSystemSCsCosts(value)

	return gasMap
}

func FillGasMapBaseOperationCosts(value uint64) map[string]uint64 {
	gasMap := make(map[string]uint64)
	gasMap["StorePerByte"] = value
	gasMap["DataCopyPerByte"] = value
	gasMap["ReleasePerByte"] = value
	gasMap["PersistPerByte"] = value
	gasMap["CompilePerByte"] = value
	gasMap["AoTPreparePerByte"] = value
	gasMap["GetCode"] = value
	return gasMap
}

func FillGasMapMetaChainSystemSCsCosts(value uint64) map[string]uint64 {
	gasMap := make(map[string]uint64)
	gasMap["Stake"] = value
	gasMap["UnStake"] = value
	gasMap["UnBond"] = value
	gasMap["Claim"] = value
	gasMap["Get"] = value
	gasMap["ChangeRewardAddress"] = value
	gasMap["ChangeValidatorKeys"] = value
	gasMap["UnJail"] = value
	gasMap["ESDTIssue"] = value
	gasMap["ESDTOperations"] = value
	gasMap["Proposal"] = value
	gasMap["Vote"] = value
	gasMap["DelegateVote"] = value
	gasMap["RevokeVote"] = value
	gasMap["CloseProposal"] = value
	gasMap["DelegationOps"] = value
	gasMap["UnStakeTokens"] = value
	gasMap["UnBondTokens"] = value
	gasMap["DelegationMgrOps"] = value
	gasMap["GetAllNodeStates"] = value
	gasMap["ValidatorToDelegation"] = value

	return gasMap
}
