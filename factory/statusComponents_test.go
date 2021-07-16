package factory_test

import (
	"testing"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/errors"
	"github.com/ElrondNetwork/elrond-go/factory"
	"github.com/ElrondNetwork/elrond-go/factory/mock"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var log = logger.GetOrCreate("factory/factory_test")

func TestNewStatusComponentsFactory_NilCoreComponentsShouldErr(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	args, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	args.CoreComponents = nil
	scf, err := factory.NewStatusComponentsFactory(args)
	assert.True(t, check.IfNil(scf))
	assert.Equal(t, errors.ErrNilCoreComponentsHolder, err)
}

func TestNewStatusComponentsFactory_NilNodesCoordinatorShouldErr(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	args, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	args.NodesCoordinator = nil
	scf, err := factory.NewStatusComponentsFactory(args)
	assert.True(t, check.IfNil(scf))
	assert.Equal(t, errors.ErrNilNodesCoordinator, err)
}

func TestNewStatusComponentsFactory_NilEpochStartNotifierShouldErr(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	args, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	args.EpochStartNotifier = nil
	scf, err := factory.NewStatusComponentsFactory(args)
	assert.True(t, check.IfNil(scf))
	assert.Equal(t, errors.ErrNilEpochStartNotifier, err)
}

func TestNewStatusComponentsFactory_NilNetworkComponentsShouldErr(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	args, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	args.NetworkComponents = nil
	scf, err := factory.NewStatusComponentsFactory(args)
	assert.True(t, check.IfNil(scf))
	assert.Equal(t, errors.ErrNilNetworkComponentsHolder, err)
}

func TestNewStatusComponentsFactory_NilShardCoordinatorShouldErr(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	args, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	args.ShardCoordinator = nil
	scf, err := factory.NewStatusComponentsFactory(args)
	assert.True(t, check.IfNil(scf))
	assert.Equal(t, errors.ErrNilShardCoordinator, err)
}

func TestNewStatusComponents_InvalidRoundDurationShouldErr(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	coreArgs := getCoreArgs()
	coreArgs.NodesFilename = "mock/testdata/nodesSetupMockInvalidRound.json"
	coreComponentsFactory, _ := factory.NewCoreComponentsFactory(coreArgs)
	coreComponents, err := factory.NewManagedCoreComponents(coreComponentsFactory)
	require.Nil(t, err)
	require.NotNil(t, coreComponents)
	err = coreComponents.Create()
	require.Nil(t, err)
	networkComponents := getNetworkComponents()
	dataComponents := getDataComponents(coreComponents, shardCoordinator)
	stateComponents := getStateComponents(coreComponents, shardCoordinator)

	statusArgs := factory.StatusComponentsFactoryArgs{
		Config:             testscommon.GetGeneralConfig(),
		ExternalConfig:     config.ExternalConfig{},
		ShardCoordinator:   shardCoordinator,
		NodesCoordinator:   &mock.NodesCoordinatorMock{},
		EpochStartNotifier: &mock.EpochStartNotifierStub{},
		CoreComponents:     coreComponents,
		DataComponents:     dataComponents,
		NetworkComponents:  networkComponents,
		StateComponents:    stateComponents,
		IsInImportMode:     false,
		EconomicsConfig:    config.EconomicsConfig{},
	}
	scf, err := factory.NewStatusComponentsFactory(statusArgs)
	assert.Nil(t, err)
	assert.NotNil(t, scf)

	statusComponents, err := scf.Create()
	assert.Nil(t, statusComponents)
	assert.Equal(t, errors.ErrInvalidRoundDuration, err)
}

func TestNewStatusComponentsFactory_ShouldWork(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	args, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	scf, err := factory.NewStatusComponentsFactory(args)
	require.NoError(t, err)
	require.False(t, check.IfNil(scf))
}

func TestStatusComponentsFactory_Create(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	args, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	scf, err := factory.NewStatusComponentsFactory(args)
	require.Nil(t, err)

	res, err := scf.Create()
	require.NoError(t, err)
	require.NotNil(t, res)
}

// ------------ Test StatusComponents --------------------
func TestStatusComponents_Close_ShouldWork(t *testing.T) {
	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	statusArgs, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	scf, _ := factory.NewStatusComponentsFactory(statusArgs)
	cc, err := scf.Create()
	require.Nil(t, err)

	err = cc.Close()
	require.NoError(t, err)
}

func getStatusComponents(
	coreComponents factory.CoreComponentsHolder,
	networkComponents factory.NetworkComponentsHolder,
	dataComponents factory.DataComponentsHolder,
	stateComponents factory.StateComponentsHolder,
	shardCoordinator sharding.Coordinator,
	nodesCoordinator sharding.NodesCoordinator,
) factory.StatusComponentsHandler {
	indexerURL := "url"
	elasticUsername := "user"
	elasticPassword := "pass"
	statusArgs := factory.StatusComponentsFactoryArgs{
		Config: testscommon.GetGeneralConfig(),
		ExternalConfig: config.ExternalConfig{
			ElasticSearchConnector: config.ElasticSearchConfig{
				Enabled:        false,
				URL:            indexerURL,
				Username:       elasticUsername,
				Password:       elasticPassword,
				EnabledIndexes: []string{"transactions", "blocks"},
			},
		},
		EconomicsConfig:    config.EconomicsConfig{},
		ShardCoordinator:   shardCoordinator,
		NodesCoordinator:   nodesCoordinator,
		EpochStartNotifier: coreComponents.EpochStartNotifierWithConfirm(),
		CoreComponents:     coreComponents,
		DataComponents:     dataComponents,
		NetworkComponents:  networkComponents,
		StateComponents:    stateComponents,
		IsInImportMode:     false,
	}

	statusComponentsFactory, _ := factory.NewStatusComponentsFactory(statusArgs)
	managedStatusComponents, err := factory.NewManagedStatusComponents(statusComponentsFactory)
	if err != nil {
		log.Error("getStatusComponents NewManagedStatusComponents", "error", err.Error())
		return nil
	}
	err = managedStatusComponents.Create()
	if err != nil {
		log.Error("getStatusComponents Create", "error", err.Error())
		return nil
	}
	return managedStatusComponents
}

func getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator sharding.Coordinator) (factory.StatusComponentsFactoryArgs, factory.ProcessComponentsHolder) {
	coreComponents := getCoreComponents()
	networkComponents := getNetworkComponents()
	dataComponents := getDataComponents(coreComponents, shardCoordinator)
	cryptoComponents := getCryptoComponents(coreComponents)
	stateComponents := getStateComponents(coreComponents, shardCoordinator)
	processComponents := getProcessComponents(
		shardCoordinator,
		coreComponents,
		networkComponents,
		dataComponents,
		cryptoComponents,
		stateComponents,
	)

	indexerURL := "url"
	elasticUsername := "user"
	elasticPassword := "pass"
	return factory.StatusComponentsFactoryArgs{
		Config: testscommon.GetGeneralConfig(),
		ExternalConfig: config.ExternalConfig{
			ElasticSearchConnector: config.ElasticSearchConfig{
				Enabled:        false,
				URL:            indexerURL,
				Username:       elasticUsername,
				Password:       elasticPassword,
				EnabledIndexes: []string{"transactions", "blocks"},
			},
		},
		EconomicsConfig:    config.EconomicsConfig{},
		ShardCoordinator:   mock.NewMultiShardsCoordinatorMock(2),
		NodesCoordinator:   &mock.NodesCoordinatorMock{},
		EpochStartNotifier: &mock.EpochStartNotifierStub{},
		CoreComponents:     coreComponents,
		DataComponents:     dataComponents,
		NetworkComponents:  networkComponents,
		StateComponents:    stateComponents,
		IsInImportMode:     false,
	}, processComponents
}

func getNetworkComponents() factory.NetworkComponentsHolder {
	networkArgs := getNetworkArgs()
	networkComponentsFactory, _ := factory.NewNetworkComponentsFactory(networkArgs)
	networkComponents, _ := factory.NewManagedNetworkComponents(networkComponentsFactory)

	_ = networkComponents.Create()

	return networkComponents
}

func getDataComponents(coreComponents factory.CoreComponentsHolder, shardCoordinator sharding.Coordinator) factory.DataComponentsHolder {
	dataArgs := getDataArgs(coreComponents, shardCoordinator)
	dataComponentsFactory, _ := factory.NewDataComponentsFactory(dataArgs)
	dataComponents, _ := factory.NewManagedDataComponents(dataComponentsFactory)
	_ = dataComponents.Create()
	return dataComponents
}

func getCryptoComponents(coreComponents factory.CoreComponentsHolder) factory.CryptoComponentsHolder {
	cryptoArgs := getCryptoArgs(coreComponents)
	cryptoComponentsFactory, _ := factory.NewCryptoComponentsFactory(cryptoArgs)
	cryptoComponents, err := factory.NewManagedCryptoComponents(cryptoComponentsFactory)
	if err != nil {
		log.Error("getCryptoComponents NewManagedCryptoComponents", "error", err.Error())
		return nil
	}

	err = cryptoComponents.Create()
	if err != nil {
		log.Error("getCryptoComponents Create", "error", err.Error())
		return nil
	}
	return cryptoComponents
}

func getStateComponents(coreComponents factory.CoreComponentsHolder, shardCoordinator sharding.Coordinator) factory.StateComponentsHolder {
	stateArgs := getStateArgs(coreComponents, shardCoordinator)
	stateComponentsFactory, err := factory.NewStateComponentsFactory(stateArgs)
	if err != nil {
		log.Error("getStateComponents NewStateComponentsFactory", "error", err.Error())
		return nil
	}

	stateComponents, err := factory.NewManagedStateComponents(stateComponentsFactory)
	if err != nil {
		log.Error("getStateComponents NewManagedStateComponents", "error", err.Error())
		return nil
	}
	err = stateComponents.Create()
	if err != nil {
		log.Error("getStateComponents Create", "error", err.Error())
		return nil
	}
	return stateComponents
}

func getProcessComponents(
	shardCoordinator sharding.Coordinator,
	coreComponents factory.CoreComponentsHolder,
	networkComponents factory.NetworkComponentsHolder,
	dataComponents factory.DataComponentsHolder,
	cryptoComponents factory.CryptoComponentsHolder,
	stateComponents factory.StateComponentsHolder,
) factory.ProcessComponentsHolder {
	processArgs := getProcessArgs(
		shardCoordinator,
		coreComponents,
		dataComponents,
		cryptoComponents,
		stateComponents,
		networkComponents,
	)
	processComponentsFactory, _ := factory.NewProcessComponentsFactory(processArgs)
	managedProcessComponents, err := factory.NewManagedProcessComponents(processComponentsFactory)
	if err != nil {
		log.Error("getProcessComponents NewManagedProcessComponents", "error", err.Error())
		return nil
	}
	err = managedProcessComponents.Create()
	if err != nil {
		log.Error("getProcessComponents Create", "error", err.Error())
		return nil
	}
	return managedProcessComponents
}
