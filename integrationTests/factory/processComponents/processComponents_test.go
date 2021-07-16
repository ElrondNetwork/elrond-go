package processComponents

import (
	"runtime"
	"testing"
	"time"

	"github.com/ElrondNetwork/elrond-go/core/forking"
	"github.com/ElrondNetwork/elrond-go/data/endProcess"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	mainFactory "github.com/ElrondNetwork/elrond-go/factory"
	"github.com/ElrondNetwork/elrond-go/integrationTests/factory"
	"github.com/ElrondNetwork/elrond-go/node"
	"github.com/stretchr/testify/require"
)

// ------------ Test TestProcessComponents --------------------
func TestProcessComponents_Close_ShouldWork(t *testing.T) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	defer factory.CleanupWorkingDir()
	time.Sleep(time.Second * 4)

	nrBefore := runtime.NumGoroutine()
	factory.PrintStack()

	configs := factory.CreateDefaultConfig()
	configs.ExternalConfig.ElasticSearchConnector.Enabled = false

	chanStopNodeProcess := make(chan endProcess.ArgEndProcess)
	nr, err := node.NewNodeRunner(configs)
	require.Nil(t, err)

	managedCoreComponents, err := nr.CreateManagedCoreComponents(chanStopNodeProcess)
	require.Nil(t, err)
	managedCryptoComponents, err := nr.CreateManagedCryptoComponents(managedCoreComponents)
	require.Nil(t, err)
	managedNetworkComponents, err := nr.CreateManagedNetworkComponents(managedCoreComponents)
	require.Nil(t, err)
	managedBootstrapComponents, err := nr.CreateManagedBootstrapComponents(managedCoreComponents, managedCryptoComponents, managedNetworkComponents)
	require.Nil(t, err)
	managedDataComponents, err := nr.CreateManagedDataComponents(managedCoreComponents, managedBootstrapComponents)
	require.Nil(t, err)
	managedStateComponents, err := nr.CreateManagedStateComponents(managedCoreComponents, managedBootstrapComponents)
	require.Nil(t, err)
	nodesShufflerOut, err := mainFactory.CreateNodesShuffleOut(managedCoreComponents.GenesisNodesSetup(), configs.GeneralConfig.EpochStartConfig, managedCoreComponents.ChanStopNodeProcess())
	require.Nil(t, err)
	nodesCoordinator, err := mainFactory.CreateNodesCoordinator(
		nodesShufflerOut,
		managedCoreComponents.GenesisNodesSetup(),
		configs.PreferencesConfig.Preferences,
		managedCoreComponents.EpochStartNotifierWithConfirm(),
		managedCryptoComponents.PublicKey(),
		managedCoreComponents.InternalMarshalizer(),
		managedCoreComponents.Hasher(),
		managedCoreComponents.Rater(),
		managedDataComponents.StorageService().GetStorer(dataRetriever.BootstrapUnit),
		managedCoreComponents.NodesShuffler(),
		managedBootstrapComponents.ShardCoordinator().SelfId(),
		managedBootstrapComponents.EpochBootstrapParams(),
		managedBootstrapComponents.EpochBootstrapParams().Epoch(),
		configs.EpochConfig.EnableEpochs.WaitingListFixEnableEpoch,
		managedCoreComponents.ChanStopNodeProcess(),
		managedCoreComponents.NodeTypeProvider(),
	)
	require.Nil(t, err)
	managedStatusComponents, err := nr.CreateManagedStatusComponents(
		managedCoreComponents,
		managedNetworkComponents,
		managedBootstrapComponents,
		managedDataComponents,
		managedStateComponents,
		nodesCoordinator,
		false,
	)
	require.Nil(t, err)
	argsGasScheduleNotifier := forking.ArgsNewGasScheduleNotifier{
		GasScheduleConfig: configs.EpochConfig.GasSchedule,
		ConfigDir:         configs.ConfigurationPathsHolder.GasScheduleDirectoryName,
		EpochNotifier:     managedCoreComponents.EpochNotifier(),
	}
	gasScheduleNotifier, err := forking.NewGasScheduleNotifier(argsGasScheduleNotifier)
	require.Nil(t, err)
	managedProcessComponents, err := nr.CreateManagedProcessComponents(
		managedCoreComponents,
		managedCryptoComponents,
		managedNetworkComponents,
		managedBootstrapComponents,
		managedStateComponents,
		managedDataComponents,
		managedStatusComponents,
		gasScheduleNotifier,
		nodesCoordinator,
	)
	require.Nil(t, err)
	require.NotNil(t, managedProcessComponents)

	time.Sleep(2 * time.Second)

	managedStatusComponents.SetForkDetector(managedProcessComponents.ForkDetector())
	err = managedStatusComponents.StartPolling()
	require.Nil(t, err)

	time.Sleep(5 * time.Second)

	err = managedProcessComponents.Close()
	require.Nil(t, err)
	err = managedStatusComponents.Close()
	require.Nil(t, err)
	err = managedStateComponents.Close()
	require.Nil(t, err)
	err = managedDataComponents.Close()
	require.Nil(t, err)
	err = managedBootstrapComponents.Close()
	require.Nil(t, err)
	err = managedNetworkComponents.Close()
	require.Nil(t, err)
	err = managedCryptoComponents.Close()
	require.Nil(t, err)
	err = managedCoreComponents.Close()
	require.Nil(t, err)

	time.Sleep(5 * time.Second)

	nrAfter := runtime.NumGoroutine()
	// TODO: find a clean solution
	// On the tests using managed network components, depending on the NAT config, there
	// might be one go routine hanging for up to 3 minutes
	if !(nrBefore == nrAfter || nrBefore == nrAfter-1) {
		factory.PrintStack()
	}

	require.True(t, nrBefore == nrAfter || nrBefore == nrAfter-1)
}
