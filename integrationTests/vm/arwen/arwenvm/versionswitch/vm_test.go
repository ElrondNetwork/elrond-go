// +build !race

// TODO remove build condition above to allow -race -short, after Arwen fix

package versionswitch

import (
	"testing"

	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm"
	"github.com/ElrondNetwork/elrond-go/integrationTests/vm/arwen/arwenvm"
	"github.com/stretchr/testify/require"
)

func TestSCExecutionWithVMVersionSwitching(t *testing.T) {
	if testing.Short() {
		t.Skip("this is not a short test")
	}

	vmConfig := &config.VirtualMachineConfig{
		ArwenVersions: []config.ArwenVersionByEpoch{
			{StartEpoch: 0, Version: "v1.2"},
			{StartEpoch: 1, Version: "v1.2"},
			{StartEpoch: 2, Version: "v1.2"},
			{StartEpoch: 3, Version: "v1.2"},
			{StartEpoch: 4, Version: "v1.2"},
			{StartEpoch: 5, Version: "v1.2"},
			{StartEpoch: 6, Version: "v1.3"},
			{StartEpoch: 7, Version: "v1.2"},
			{StartEpoch: 8, Version: "v1.2"},
			{StartEpoch: 9, Version: "v1.2"},
			{StartEpoch: 10, Version: "v1.3"},
			{StartEpoch: 11, Version: "v1.2"},
			{StartEpoch: 12, Version: "v1.4"},
			{StartEpoch: 13, Version: "v1.3"},
			{StartEpoch: 14, Version: "v1.4"},
			{StartEpoch: 15, Version: "v1.2"},
			{StartEpoch: 16, Version: "v1.4"},
		},
	}

	gasSchedule, _ := core.LoadGasScheduleConfig("../../../../cmd/node/config/gasSchedules/gasScheduleV2.toml")
	testContext, err := vm.CreateTxProcessorArwenWithVMConfig(
		vm.ArgEnableEpoch{},
		vmConfig,
		gasSchedule,
	)
	require.Nil(t, err)
	defer testContext.Close()

	_ = arwenvm.SetupERC20Test(testContext, "../../testdata/erc20-c-03/wrc20_arwen.wasm")

	err = arwenvm.RunERC20TransactionSet(testContext)
	require.Nil(t, err)

	for _, versionConfig := range vmConfig.ArwenVersions {
		testContext.EpochNotifier.CheckEpoch(arwenvm.MakeHeaderHandlerStub(versionConfig.StartEpoch))
		err = arwenvm.RunERC20TransactionSet(testContext)
		require.Nil(t, err)
	}

	err = arwenvm.RunERC20TransactionSet(testContext)
	require.Nil(t, err)
}
