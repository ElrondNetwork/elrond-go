package factory_test

import (
	"testing"

	"github.com/ElrondNetwork/elrond-go/factory"
	"github.com/ElrondNetwork/elrond-go/factory/mock"
	"github.com/stretchr/testify/require"
)

// ------------ Test ManagedStatusComponents --------------------
func TestManagedStatusComponents_CreateWithInvalidArgs_ShouldErr(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	statusArgs, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	coreComponents := getDefaultCoreComponents()
	statusArgs.CoreComponents = coreComponents

	statusComponentsFactory, _ := factory.NewStatusComponentsFactory(statusArgs)
	managedStatusComponents, err := factory.NewManagedStatusComponents(statusComponentsFactory)
	require.NoError(t, err)

	coreComponents.AppStatusHdl = nil
	err = managedStatusComponents.Create()
	require.Error(t, err)
}

func TestManagedStatusComponents_Create_ShouldWork(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	statusArgs, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	statusComponentsFactory, _ := factory.NewStatusComponentsFactory(statusArgs)
	managedStatusComponents, err := factory.NewManagedStatusComponents(statusComponentsFactory)
	require.NoError(t, err)
	require.Nil(t, managedStatusComponents.OutportHandler())
	require.Nil(t, managedStatusComponents.SoftwareVersionChecker())
	require.Nil(t, managedStatusComponents.TpsBenchmark())

	err = managedStatusComponents.Create()
	require.NoError(t, err)
	require.NotNil(t, managedStatusComponents.OutportHandler())
	require.NotNil(t, managedStatusComponents.SoftwareVersionChecker())
	require.NotNil(t, managedStatusComponents.TpsBenchmark())
}

func TestManagedStatusComponents_Close(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	statusArgs, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	statusComponentsFactory, _ := factory.NewStatusComponentsFactory(statusArgs)
	managedStatusComponents, _ := factory.NewManagedStatusComponents(statusComponentsFactory)
	err := managedStatusComponents.Create()
	require.NoError(t, err)

	err = managedStatusComponents.Close()
	require.NoError(t, err)
}

func TestManagedStatusComponents_CheckSubcomponents(t *testing.T) {
	t.Parallel()

	shardCoordinator := mock.NewMultiShardsCoordinatorMock(2)
	statusArgs, _ := getStatusComponentsFactoryArgsAndProcessComponents(shardCoordinator)
	statusComponentsFactory, _ := factory.NewStatusComponentsFactory(statusArgs)
	managedStatusComponents, _ := factory.NewManagedStatusComponents(statusComponentsFactory)
	err := managedStatusComponents.Create()
	require.NoError(t, err)

	err = managedStatusComponents.CheckSubcomponents()
	require.NoError(t, err)
}
