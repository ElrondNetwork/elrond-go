package metachain

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/big"
	"os"
	"strconv"
	"testing"

	arwenConfig "github.com/ElrondNetwork/arwen-wasm-vm/v1_4/config"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/forking"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/blockchain"
	dataMock "github.com/ElrondNetwork/elrond-go/data/mock"
	"github.com/ElrondNetwork/elrond-go/data/rewardTx"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/data/state/factory"
	"github.com/ElrondNetwork/elrond-go/data/state/storagePruningManager"
	"github.com/ElrondNetwork/elrond-go/data/state/storagePruningManager/evictionWaitingList"
	"github.com/ElrondNetwork/elrond-go/data/trie"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/dataRetriever/dataPool"
	"github.com/ElrondNetwork/elrond-go/epochStart"
	"github.com/ElrondNetwork/elrond-go/epochStart/mock"
	"github.com/ElrondNetwork/elrond-go/genesis/process/disabled"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/hashing/sha256"
	"github.com/ElrondNetwork/elrond-go/marshal"
	"github.com/ElrondNetwork/elrond-go/process"
	economicsHandler "github.com/ElrondNetwork/elrond-go/process/economics"
	vmFactory "github.com/ElrondNetwork/elrond-go/process/factory"
	metaProcess "github.com/ElrondNetwork/elrond-go/process/factory/metachain"
	"github.com/ElrondNetwork/elrond-go/process/peer"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/hooks"
	"github.com/ElrondNetwork/elrond-go/sharding"
	"github.com/ElrondNetwork/elrond-go/storage"
	"github.com/ElrondNetwork/elrond-go/storage/storageUnit"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/vm"
	"github.com/ElrondNetwork/elrond-go/vm/systemSmartContracts"
	"github.com/ElrondNetwork/elrond-go/vm/systemSmartContracts/defaults"
	vmcommon "github.com/ElrondNetwork/elrond-vm-common"
	vmcommonBuiltInFunctions "github.com/ElrondNetwork/elrond-vm-common/builtInFunctions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testKeyPair struct {
	walletKey    []byte
	validatorKey []byte
}

func createPhysicalUnit() (storage.Storer, string) {
	cacheConfig := storageUnit.CacheConfig{
		Name:                 "test",
		Type:                 "SizeLRU",
		SizeInBytes:          314572800,
		SizeInBytesPerSender: 0,
		Capacity:             500000,
		SizePerSender:        0,
		Shards:               0,
	}
	dir, _ := ioutil.TempDir("", "")
	persisterConfig := storageUnit.ArgDB{
		Path:              dir,
		DBType:            "LvlDBSerial",
		BatchDelaySeconds: 2,
		MaxBatchSize:      45000,
		MaxOpenFiles:      10,
	}

	cache, _ := storageUnit.NewCache(cacheConfig)
	persist, _ := storageUnit.NewDB(persisterConfig)
	unit, _ := storageUnit.NewStorageUnit(cache, persist)

	return unit, dir
}

func TestNewSystemSCProcessor(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.Marshalizer = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilMarshalizer)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.PeerAccountsDB = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilAccountsDB)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.SystemVM = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilSystemVM)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.UserAccountsDB = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilAccountsDB)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.ValidatorInfoCreator = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilValidatorInfoProcessor)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.EndOfEpochCallerAddress = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilEndOfEpochCallerAddress)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.StakingSCAddress = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilStakingSCAddress)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.ValidatorInfoCreator = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilValidatorInfoProcessor)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.ChanceComputer = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilChanceComputer)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.GenesisNodesConfig = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilGenesisNodesConfig)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.NodesConfigProvider = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilNodesConfigProvider)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.StakingDataProvider = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilStakingDataProvider)

	args, _ = createFullArgumentsForSystemSCProcessing(100, createMemUnit())
	args.EpochNotifier = nil
	checkConstructorWithNilArg(t, args, epochStart.ErrNilEpochStartNotifier)
}

func checkConstructorWithNilArg(t *testing.T, args ArgsNewEpochStartSystemSCProcessing, expectedErr error) {
	_, err := NewSystemSCProcessor(args)
	require.Equal(t, expectedErr, err)
}

func TestSystemSCProcessor_ProcessSystemSmartContract(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(1000, createMemUnit())
	args.ChanceComputer = &mock.ChanceComputerStub{
		GetChanceCalled: func(rating uint32) uint32 {
			if rating == 0 {
				return 10
			}
			return rating
		},
	}
	s, _ := NewSystemSCProcessor(args)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("jailedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		[]byte("rewardAddress"),
		[]byte("rewardAddress"),
	)
	jailedAcc, _ := args.PeerAccountsDB.LoadAccount([]byte("jailedPubKey0"))
	_ = args.PeerAccountsDB.SaveAccount(jailedAcc)

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	vInfo := &state.ValidatorInfo{
		PublicKey:       []byte("jailedPubKey0"),
		ShardId:         0,
		List:            string(core.JailedList),
		TempRating:      1,
		RewardAddress:   []byte("address"),
		AccumulatedFees: big.NewInt(0),
	}
	validatorInfos[0] = append(validatorInfos[0], vInfo)
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	assert.Equal(t, len(validatorInfos[0]), 1)
	newValidatorInfo := validatorInfos[0][0]
	assert.Equal(t, newValidatorInfo.List, string(core.NewList))
}

func TestSystemSCProcessor_JailedNodesShouldNotBeSwappedAllAtOnce(t *testing.T) {
	t.Parallel()

	testSystemSCProcessorJailedNodesShouldNotBeSwappedAllAtOnce(t, 0)
	testSystemSCProcessorJailedNodesShouldNotBeSwappedAllAtOnce(t, 1000)
}

func testSystemSCProcessorJailedNodesShouldNotBeSwappedAllAtOnce(t *testing.T, saveJailedAlwaysEnableEpoch uint32) {
	args, _ := createFullArgumentsForSystemSCProcessing(10000, createMemUnit())
	args.EpochConfig.EnableEpochs.SaveJailedAlwaysEnableEpoch = saveJailedAlwaysEnableEpoch

	args.ChanceComputer = &mock.ChanceComputerStub{
		GetChanceCalled: func(rating uint32) uint32 {
			if rating == 0 {
				return 10
			}
			return rating
		},
	}
	s, _ := NewSystemSCProcessor(args)
	require.NotNil(t, s)

	numEligible := 9
	numWaiting := 5
	numJailed := 8
	stakingScAcc := loadSCAccount(args.UserAccountsDB, vm.StakingSCAddress)
	createEligibleNodes(numEligible, stakingScAcc, args.Marshalizer)
	_ = createWaitingNodes(numWaiting, stakingScAcc, args.UserAccountsDB, args.Marshalizer)
	jailed := createJailedNodes(numJailed, stakingScAcc, args.UserAccountsDB, args.PeerAccountsDB, args.Marshalizer)

	_ = s.userAccountsDB.SaveAccount(stakingScAcc)
	_, _ = s.userAccountsDB.Commit()

	addValidatorData(args.UserAccountsDB, []byte("ownerForAll"), [][]byte{[]byte("stakedPubKey1"), []byte("stakedPubKey2"), []byte("stakedPubKey3")}, big.NewInt(900000), args.Marshalizer)

	validatorsInfo := make(map[uint32][]*state.ValidatorInfo)
	validatorsInfo[0] = append(validatorsInfo[0], jailed...)

	err := s.ProcessSystemSmartContract(validatorsInfo, 0, 0)
	assert.Nil(t, err)
	for i := 0; i < numWaiting; i++ {
		assert.Equal(t, string(core.NewList), validatorsInfo[0][i].List)
	}
	for i := numWaiting; i < numJailed; i++ {
		assert.Equal(t, string(core.JailedList), validatorsInfo[0][i].List)
	}

	newJailedNodes := jailed[numWaiting:numJailed]
	checkNodesStatusInSystemSCDataTrie(t, newJailedNodes, args.UserAccountsDB, args.Marshalizer, saveJailedAlwaysEnableEpoch == 0)
}

func checkNodesStatusInSystemSCDataTrie(t *testing.T, nodes []*state.ValidatorInfo, accounts state.AccountsAdapter, marshalizer marshal.Marshalizer, jailed bool) {
	account, err := accounts.LoadAccount(vm.StakingSCAddress)
	require.Nil(t, err)

	var buff []byte
	systemScAccount, ok := account.(state.UserAccountHandler)
	require.True(t, ok)
	for _, nodeInfo := range nodes {
		buff, err = systemScAccount.DataTrieTracker().RetrieveValue(nodeInfo.PublicKey)
		require.Nil(t, err)
		require.True(t, len(buff) > 0)

		stakingData := &systemSmartContracts.StakedDataV1_1{}
		err = marshalizer.Unmarshal(stakingData, buff)
		require.Nil(t, err)

		assert.Equal(t, jailed, stakingData.Jailed)
	}
}

func TestSystemSCProcessor_NobodyToSwapWithStakingV2(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.ChanceComputer = &mock.ChanceComputerStub{
		GetChanceCalled: func(rating uint32) uint32 {
			if rating == 0 {
				return 10
			}
			return rating
		},
	}
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)
	require.NotNil(t, s)

	owner1 := append([]byte("owner1"), bytes.Repeat([]byte{1}, 26)...)
	blsKeys := [][]byte{
		[]byte("bls key 1"),
		[]byte("bls key 2"),
		[]byte("bls key 3"),
		[]byte("bls key 4"),
	}

	_ = s.initDelegationSystemSC()
	doStake(t, s.systemVM, s.userAccountsDB, owner1, big.NewInt(1000), blsKeys...)
	doUnStake(t, s.systemVM, s.userAccountsDB, owner1, blsKeys[:3]...)
	validatorsInfo := make(map[uint32][]*state.ValidatorInfo)
	jailed := &state.ValidatorInfo{
		PublicKey:       blsKeys[0],
		ShardId:         0,
		List:            string(core.JailedList),
		TempRating:      1,
		RewardAddress:   []byte("owner1"),
		AccumulatedFees: big.NewInt(0),
	}
	validatorsInfo[0] = append(validatorsInfo[0], jailed)

	err := s.ProcessSystemSmartContract(validatorsInfo, 0, 0)
	assert.Nil(t, err)

	for _, vInfo := range validatorsInfo[0] {
		assert.Equal(t, string(core.JailedList), vInfo.List)
	}

	nodesToUnStake, mapOwnersKeys, err := s.stakingDataProvider.ComputeUnQualifiedNodes(validatorsInfo)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(nodesToUnStake))
	assert.Equal(t, 0, len(mapOwnersKeys))
}

func TestSystemSCProcessor_UpdateStakingV2ShouldWork(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(1000, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 1
	args.ChanceComputer = &mock.ChanceComputerStub{
		GetChanceCalled: func(rating uint32) uint32 {
			if rating == 0 {
				return 10
			}
			return rating
		},
	}
	s, _ := NewSystemSCProcessor(args)
	require.NotNil(t, s)

	owner1 := append([]byte("owner1"), bytes.Repeat([]byte{1}, 26)...)
	owner2 := append([]byte("owner2"), bytes.Repeat([]byte{1}, 26)...)

	blsKeys := [][]byte{
		[]byte("bls key 1"),
		[]byte("bls key 2"),
		[]byte("bls key 3"),
		[]byte("bls key 4"),
	}

	doStake(t, s.systemVM, s.userAccountsDB, owner1, big.NewInt(1000), blsKeys[0], blsKeys[1])
	doStake(t, s.systemVM, s.userAccountsDB, owner2, big.NewInt(1000), blsKeys[2], blsKeys[3])

	args.EpochNotifier.CheckEpoch(&testscommon.HeaderHandlerStub{
		EpochField: 1000000,
	})

	for _, blsKey := range blsKeys {
		checkOwnerOfBlsKey(t, s.systemVM, blsKey, []byte(""))
	}

	err := s.updateOwnersForBlsKeys()
	assert.Nil(t, err)

	checkOwnerOfBlsKey(t, s.systemVM, blsKeys[0], owner1)
	checkOwnerOfBlsKey(t, s.systemVM, blsKeys[1], owner1)
	checkOwnerOfBlsKey(t, s.systemVM, blsKeys[2], owner2)
	checkOwnerOfBlsKey(t, s.systemVM, blsKeys[3], owner2)
}

func TestSystemSCProcessor_UpdateStakingV2MoreKeysShouldWork(t *testing.T) {
	t.Parallel()

	db, dir := createPhysicalUnit()
	require.False(t, check.IfNil(db))

	log.Info("using temporary directory", "path", dir)

	sw := core.NewStopWatch()
	sw.Start("complete test")
	defer func() {
		sw.Stop("complete test")
		log.Info("TestSystemSCProcessor_UpdateStakingV2MoreKeysShouldWork time measurements", sw.GetMeasurements()...)
		_ = db.DestroyUnit()
		_ = os.RemoveAll(dir)
	}()

	args, _ := createFullArgumentsForSystemSCProcessing(1000, db)
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 1
	args.ChanceComputer = &mock.ChanceComputerStub{
		GetChanceCalled: func(rating uint32) uint32 {
			if rating == 0 {
				return 10
			}
			return rating
		},
	}
	s, _ := NewSystemSCProcessor(args)
	require.NotNil(t, s)

	numKeys := 5000
	keys := make([]*testKeyPair, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		if i%100 == 0 {
			_, err := args.UserAccountsDB.Commit()
			require.Nil(t, err)
		}

		keys = append(keys, createTestKeyPair())
	}

	sw.Start("do stake")
	for _, tkp := range keys {
		doStake(t, s.systemVM, s.userAccountsDB, tkp.walletKey, big.NewInt(1000), tkp.validatorKey)

	}
	sw.Stop("do stake")

	args.EpochNotifier.CheckEpoch(&testscommon.HeaderHandlerStub{
		EpochField: 1000000,
	})

	sw.Start("initial check")
	for _, tkp := range keys {
		checkOwnerOfBlsKey(t, s.systemVM, tkp.validatorKey, []byte(""))
	}
	sw.Stop("initial check")

	sw.Start("updateOwnersForBlsKeys")
	err := s.updateOwnersForBlsKeys()
	sw.Stop("updateOwnersForBlsKeys")
	assert.Nil(t, err)

	sw.Start("final check")
	for _, tkp := range keys {
		checkOwnerOfBlsKey(t, s.systemVM, tkp.validatorKey, tkp.walletKey)
	}
	sw.Stop("final check")
}

func createTestKeyPair() *testKeyPair {
	tkp := &testKeyPair{
		walletKey:    make([]byte, 32),
		validatorKey: make([]byte, 96),
	}

	_, _ = rand.Read(tkp.walletKey)
	_, _ = rand.Read(tkp.validatorKey)

	return tkp
}

func checkOwnerOfBlsKey(t *testing.T, systemVm vmcommon.VMExecutionHandler, blsKey []byte, expectedOwner []byte) {
	vmInput := &vmcommon.ContractCallInput{
		VMInput: vmcommon.VMInput{
			CallerAddr: vm.ValidatorSCAddress,
			Arguments:  [][]byte{blsKey},
			CallValue:  big.NewInt(0),
		},
		RecipientAddr: vm.StakingSCAddress,
		Function:      "getOwner",
	}

	vmOutput, err := systemVm.RunSmartContractCall(vmInput)
	require.Nil(t, err)
	if len(expectedOwner) == 0 {
		require.Equal(t, vmOutput.ReturnCode, vmcommon.UserError)
		return
	}

	require.Equal(t, vmcommon.Ok, vmOutput.ReturnCode)
	require.Equal(t, 1, len(vmOutput.ReturnData))

	assert.Equal(t, len(expectedOwner), len(vmOutput.ReturnData[0]))
	if len(expectedOwner) > 0 {
		assert.Equal(t, expectedOwner, vmOutput.ReturnData[0])
	}
}

func doStake(t *testing.T, systemVm vmcommon.VMExecutionHandler, accountsDB state.AccountsAdapter, owner []byte, nodePrice *big.Int, blsKeys ...[]byte) {
	numBlsKeys := len(blsKeys)
	args := [][]byte{{0, byte(numBlsKeys)}}
	for _, blsKey := range blsKeys {
		args = append(args, blsKey)
		args = append(args, []byte("sig"))
	}

	vmInput := &vmcommon.ContractCallInput{
		VMInput: vmcommon.VMInput{
			CallerAddr:  owner,
			Arguments:   args,
			CallValue:   big.NewInt(0).Mul(big.NewInt(int64(numBlsKeys)), nodePrice),
			GasProvided: math.MaxUint64,
		},
		RecipientAddr: vm.ValidatorSCAddress,
		Function:      "stake",
	}

	vmOutput, err := systemVm.RunSmartContractCall(vmInput)
	require.Nil(t, err)
	require.Equal(t, vmcommon.Ok, vmOutput.ReturnCode)

	saveOutputAccounts(t, accountsDB, vmOutput)
}

func doUnStake(t *testing.T, systemVm vmcommon.VMExecutionHandler, accountsDB state.AccountsAdapter, owner []byte, blsKeys ...[]byte) {
	vmInput := &vmcommon.ContractCallInput{
		VMInput: vmcommon.VMInput{
			CallerAddr:  owner,
			Arguments:   blsKeys,
			CallValue:   big.NewInt(0),
			GasProvided: math.MaxUint64,
		},
		RecipientAddr: vm.ValidatorSCAddress,
		Function:      "unStake",
	}

	vmOutput, err := systemVm.RunSmartContractCall(vmInput)
	require.Nil(t, err)
	require.Equal(t, vmcommon.Ok, vmOutput.ReturnCode)

	saveOutputAccounts(t, accountsDB, vmOutput)
}

func loadSCAccount(accountsDB state.AccountsAdapter, address []byte) state.UserAccountHandler {
	acc, _ := accountsDB.LoadAccount(address)
	stakingSCAcc := acc.(state.UserAccountHandler)

	return stakingSCAcc
}

func createEligibleNodes(numNodes int, stakingSCAcc state.UserAccountHandler, marshalizer marshal.Marshalizer) {
	for i := 0; i < numNodes; i++ {
		stakedData := &systemSmartContracts.StakedDataV2_0{
			Waiting:       false,
			Staked:        true,
			StakedNonce:   0,
			RewardAddress: []byte(fmt.Sprintf("rewardAddress_w%d", i)),
			OwnerAddress:  []byte("ownerForAll"),
			StakeValue:    big.NewInt(100),
		}
		marshaledData, _ := marshalizer.Marshal(stakedData)
		_ = stakingSCAcc.DataTrieTracker().SaveKeyValue([]byte(fmt.Sprintf("waiting_%d", i)), marshaledData)
	}
}

func createJailedNodes(numNodes int, stakingSCAcc state.UserAccountHandler, userAccounts state.AccountsAdapter, peerAccounts state.AccountsAdapter, marshalizer marshal.Marshalizer) []*state.ValidatorInfo {
	validatorInfos := make([]*state.ValidatorInfo, 0)

	for i := 0; i < numNodes; i++ {
		stakedData := &systemSmartContracts.StakedDataV2_0{
			Staked:        true,
			RewardAddress: []byte(fmt.Sprintf("rewardAddress_j%d", i)),
			StakeValue:    big.NewInt(100),
			OwnerAddress:  []byte("ownerForAll"),
		}
		marshaledData, _ := marshalizer.Marshal(stakedData)
		_ = stakingSCAcc.DataTrieTracker().SaveKeyValue([]byte(fmt.Sprintf("jailed__%d", i)), marshaledData)

		_ = userAccounts.SaveAccount(stakingSCAcc)

		jailedAcc, _ := peerAccounts.LoadAccount([]byte(fmt.Sprintf("jailed__%d", i)))
		_ = peerAccounts.SaveAccount(jailedAcc)

		vInfo := &state.ValidatorInfo{
			PublicKey:       []byte(fmt.Sprintf("jailed__%d", i)),
			ShardId:         0,
			List:            string(core.JailedList),
			TempRating:      1,
			RewardAddress:   []byte("address"),
			AccumulatedFees: big.NewInt(0),
		}

		validatorInfos = append(validatorInfos, vInfo)
	}

	return validatorInfos
}

func addValidatorDataWithUnStakedKey(
	accountsDB state.AccountsAdapter,
	ownerKey []byte,
	registeredKeys [][]byte,
	nodePrice *big.Int,
	marshalizer marshal.Marshalizer,
) {
	stakingAccount := loadSCAccount(accountsDB, vm.StakingSCAddress)
	validatorAccount := loadSCAccount(accountsDB, vm.ValidatorSCAddress)

	validatorData := &systemSmartContracts.ValidatorDataV2{
		RegisterNonce:   0,
		Epoch:           0,
		RewardAddress:   ownerKey,
		TotalStakeValue: big.NewInt(0),
		LockedStake:     big.NewInt(0).Mul(nodePrice, big.NewInt(int64(len(registeredKeys)))),
		TotalUnstaked:   big.NewInt(0).Mul(nodePrice, big.NewInt(int64(len(registeredKeys)))),
		BlsPubKeys:      registeredKeys,
		NumRegistered:   uint32(len(registeredKeys)),
	}

	for _, bls := range registeredKeys {
		validatorData.UnstakedInfo = append(validatorData.UnstakedInfo, &systemSmartContracts.UnstakedValue{
			UnstakedEpoch: 1,
			UnstakedValue: nodePrice,
		})

		stakingData := &systemSmartContracts.StakedDataV2_0{
			RegisterNonce: 0,
			StakedNonce:   0,
			Staked:        false,
			UnStakedNonce: 1,
			UnStakedEpoch: 0,
			RewardAddress: ownerKey,
			StakeValue:    nodePrice,
			JailedRound:   0,
			JailedNonce:   0,
			UnJailedNonce: 0,
			Jailed:        false,
			Waiting:       false,
			NumJailed:     0,
			SlashValue:    big.NewInt(0),
			OwnerAddress:  ownerKey,
		}
		marshaledData, _ := marshalizer.Marshal(stakingData)
		_ = stakingAccount.DataTrieTracker().SaveKeyValue(bls, marshaledData)
	}

	marshaledData, _ := marshalizer.Marshal(validatorData)
	_ = validatorAccount.DataTrieTracker().SaveKeyValue(ownerKey, marshaledData)

	_ = accountsDB.SaveAccount(validatorAccount)
	_ = accountsDB.SaveAccount(stakingAccount)
}

func createWaitingNodes(numNodes int, stakingSCAcc state.UserAccountHandler, userAccounts state.AccountsAdapter, marshalizer marshal.Marshalizer) []*state.ValidatorInfo {
	validatorInfos := make([]*state.ValidatorInfo, 0)
	waitingKeyInList := []byte("waiting")
	for i := 0; i < numNodes; i++ {
		stakedData := &systemSmartContracts.StakedDataV2_0{
			Waiting:       true,
			RewardAddress: []byte(fmt.Sprintf("rewardAddress_w%d", i)),
			OwnerAddress:  []byte("ownerForAll"),
			StakeValue:    big.NewInt(100),
		}
		marshaledData, _ := marshalizer.Marshal(stakedData)
		_ = stakingSCAcc.DataTrieTracker().SaveKeyValue([]byte(fmt.Sprintf("waiting_%d", i)), marshaledData)
		previousKey := string(waitingKeyInList)
		waitingKeyInList = []byte("w_" + fmt.Sprintf("waiting_%d", i))
		waitingListHead := &systemSmartContracts.WaitingList{
			FirstKey: []byte("w_" + fmt.Sprintf("waiting_%d", 0)),
			LastKey:  []byte("w_" + fmt.Sprintf("waiting_%d", numNodes-1)),
			Length:   uint32(numNodes),
		}
		marshaledData, _ = marshalizer.Marshal(waitingListHead)
		_ = stakingSCAcc.DataTrieTracker().SaveKeyValue([]byte("waitingList"), marshaledData)

		waitingListElement := &systemSmartContracts.ElementInList{
			BLSPublicKey: []byte(fmt.Sprintf("waiting_%d", i)),
			PreviousKey:  waitingKeyInList,
			NextKey:      []byte("w_" + fmt.Sprintf("waiting_%d", i+1)),
		}
		if i == numNodes-1 {
			waitingListElement.NextKey = make([]byte, 0)
		}
		if i > 0 {
			waitingListElement.PreviousKey = []byte(previousKey)
		}

		marshaledData, _ = marshalizer.Marshal(waitingListElement)
		_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(waitingKeyInList, marshaledData)

		vInfo := &state.ValidatorInfo{
			PublicKey:       []byte(fmt.Sprintf("waiting_%d", i)),
			ShardId:         0,
			List:            string(core.WaitingList),
			TempRating:      1,
			RewardAddress:   []byte("address"),
			AccumulatedFees: big.NewInt(0),
		}

		validatorInfos = append(validatorInfos, vInfo)
	}

	_ = userAccounts.SaveAccount(stakingSCAcc)

	return validatorInfos
}

func addValidatorData(
	accountsDB state.AccountsAdapter,
	ownerKey []byte,
	registeredKeys [][]byte,
	totalStake *big.Int,
	marshalizer marshal.Marshalizer,
) {
	validatorSC := loadSCAccount(accountsDB, vm.ValidatorSCAddress)
	validatorData := &systemSmartContracts.ValidatorDataV2{
		RegisterNonce:   0,
		Epoch:           0,
		RewardAddress:   ownerKey,
		TotalStakeValue: totalStake,
		LockedStake:     big.NewInt(0),
		TotalUnstaked:   big.NewInt(0),
		BlsPubKeys:      registeredKeys,
		NumRegistered:   uint32(len(registeredKeys)),
	}

	marshaledData, _ := marshalizer.Marshal(validatorData)
	_ = validatorSC.DataTrieTracker().SaveKeyValue(ownerKey, marshaledData)

	_ = accountsDB.SaveAccount(validatorSC)
}

func addStakedData(
	accountsDB state.AccountsAdapter,
	stakedKey []byte,
	ownerKey []byte,
	marshalizer marshal.Marshalizer,
) {
	stakingSCAcc := loadSCAccount(accountsDB, vm.StakingSCAddress)
	stakedData := &systemSmartContracts.StakedDataV2_0{
		Staked:        true,
		RewardAddress: ownerKey,
		OwnerAddress:  ownerKey,
		StakeValue:    big.NewInt(0),
	}
	marshaledData, _ := marshalizer.Marshal(stakedData)
	_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(stakedKey, marshaledData)

	_ = accountsDB.SaveAccount(stakingSCAcc)
}

func prepareStakingContractWithData(
	accountsDB state.AccountsAdapter,
	stakedKey []byte,
	waitingKey []byte,
	marshalizer marshal.Marshalizer,
	rewardAddress []byte,
	ownerAddress []byte,
) {
	stakingSCAcc := loadSCAccount(accountsDB, vm.StakingSCAddress)

	stakedData := &systemSmartContracts.StakedDataV2_0{
		Staked:        true,
		RewardAddress: rewardAddress,
		OwnerAddress:  ownerAddress,
		StakeValue:    big.NewInt(100),
	}
	marshaledData, _ := marshalizer.Marshal(stakedData)
	_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(stakedKey, marshaledData)
	_ = accountsDB.SaveAccount(stakingSCAcc)

	saveOneKeyToWaitingList(accountsDB, waitingKey, marshalizer, rewardAddress, ownerAddress)

	validatorSC := loadSCAccount(accountsDB, vm.ValidatorSCAddress)
	validatorData := &systemSmartContracts.ValidatorDataV2{
		RegisterNonce:   0,
		Epoch:           0,
		RewardAddress:   rewardAddress,
		TotalStakeValue: big.NewInt(10000000000),
		LockedStake:     big.NewInt(10000000000),
		TotalUnstaked:   big.NewInt(0),
		NumRegistered:   2,
		BlsPubKeys:      [][]byte{stakedKey, waitingKey},
	}

	marshaledData, _ = marshalizer.Marshal(validatorData)
	_ = validatorSC.DataTrieTracker().SaveKeyValue(rewardAddress, marshaledData)

	_ = accountsDB.SaveAccount(validatorSC)
	_, err := accountsDB.Commit()
	log.LogIfError(err)
}

func saveOneKeyToWaitingList(
	accountsDB state.AccountsAdapter,
	waitingKey []byte,
	marshalizer marshal.Marshalizer,
	rewardAddress []byte,
	ownerAddress []byte,
) {
	stakingSCAcc := loadSCAccount(accountsDB, vm.StakingSCAddress)
	stakedData := &systemSmartContracts.StakedDataV2_0{
		Waiting:       true,
		RewardAddress: rewardAddress,
		OwnerAddress:  ownerAddress,
		StakeValue:    big.NewInt(100),
	}
	marshaledData, _ := marshalizer.Marshal(stakedData)
	_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(waitingKey, marshaledData)

	waitingKeyInList := []byte("w_" + string(waitingKey))
	waitingListHead := &systemSmartContracts.WaitingList{
		FirstKey: waitingKeyInList,
		LastKey:  waitingKeyInList,
		Length:   1,
	}
	marshaledData, _ = marshalizer.Marshal(waitingListHead)
	_ = stakingSCAcc.DataTrieTracker().SaveKeyValue([]byte("waitingList"), marshaledData)

	waitingListElement := &systemSmartContracts.ElementInList{
		BLSPublicKey: waitingKey,
		PreviousKey:  waitingKeyInList,
		NextKey:      make([]byte, 0),
	}
	marshaledData, _ = marshalizer.Marshal(waitingListElement)
	_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(waitingKeyInList, marshaledData)

	_ = accountsDB.SaveAccount(stakingSCAcc)
}

func addKeysToWaitingList(
	accountsDB state.AccountsAdapter,
	waitingKeys [][]byte,
	marshalizer marshal.Marshalizer,
	rewardAddress []byte,
	ownerAddress []byte,
) {
	stakingSCAcc := loadSCAccount(accountsDB, vm.StakingSCAddress)

	for _, waitingKey := range waitingKeys {
		stakedData := &systemSmartContracts.StakedDataV2_0{
			Waiting:       true,
			RewardAddress: rewardAddress,
			OwnerAddress:  ownerAddress,
			StakeValue:    big.NewInt(100),
		}
		marshaledData, _ := marshalizer.Marshal(stakedData)
		_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(waitingKey, marshaledData)
	}

	marshaledData, _ := stakingSCAcc.DataTrieTracker().RetrieveValue([]byte("waitingList"))
	waitingListHead := &systemSmartContracts.WaitingList{}
	_ = marshalizer.Unmarshal(waitingListHead, marshaledData)
	waitingListHead.Length += uint32(len(waitingKeys))
	lastKeyInList := []byte("w_" + string(waitingKeys[len(waitingKeys)-1]))
	waitingListHead.LastKey = lastKeyInList

	marshaledData, _ = marshalizer.Marshal(waitingListHead)
	_ = stakingSCAcc.DataTrieTracker().SaveKeyValue([]byte("waitingList"), marshaledData)

	numWaitingKeys := len(waitingKeys)
	previousKey := waitingListHead.FirstKey
	for i, waitingKey := range waitingKeys {

		waitingKeyInList := []byte("w_" + string(waitingKey))
		waitingListElement := &systemSmartContracts.ElementInList{
			BLSPublicKey: waitingKey,
			PreviousKey:  previousKey,
			NextKey:      make([]byte, 0),
		}

		if i < numWaitingKeys-1 {
			nextKey := []byte("w_" + string(waitingKeys[i+1]))
			waitingListElement.NextKey = nextKey
		}

		marshaledData, _ = marshalizer.Marshal(waitingListElement)
		_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(waitingKeyInList, marshaledData)

		previousKey = waitingKeyInList
	}

	marshaledData, _ = stakingSCAcc.DataTrieTracker().RetrieveValue(waitingListHead.FirstKey)
	waitingListElement := &systemSmartContracts.ElementInList{}
	_ = marshalizer.Unmarshal(waitingListElement, marshaledData)
	waitingListElement.NextKey = []byte("w_" + string(waitingKeys[0]))
	marshaledData, _ = marshalizer.Marshal(waitingListElement)
	_ = stakingSCAcc.DataTrieTracker().SaveKeyValue(waitingListHead.FirstKey, marshaledData)

	_ = accountsDB.SaveAccount(stakingSCAcc)
}

func createAccountsDB(
	hasher hashing.Hasher,
	marshalizer marshal.Marshalizer,
	accountFactory state.AccountFactory,
	trieStorageManager data.StorageManager,
) *state.AccountsDB {
	tr, _ := trie.NewTrie(trieStorageManager, marshalizer, hasher, 5)
	ewl, _ := evictionWaitingList.NewEvictionWaitingList(10, dataMock.NewMemDbMock(), marshalizer)
	spm, _ := storagePruningManager.NewStoragePruningManager(ewl, 10)
	adb, _ := state.NewAccountsDB(tr, hasher, marshalizer, accountFactory, spm)
	return adb
}

func createFullArgumentsForSystemSCProcessing(stakingV2EnableEpoch uint32, trieStorer storage.Storer) (ArgsNewEpochStartSystemSCProcessing, vm.SystemSCContainer) {
	hasher := sha256.NewSha256()
	marshalizer := &marshal.GogoProtoMarshalizer{}
	trieFactoryManager, _ := trie.NewTrieStorageManagerWithoutPruning(trieStorer)
	userAccountsDB := createAccountsDB(hasher, marshalizer, factory.NewAccountCreator(), trieFactoryManager)
	peerAccountsDB := createAccountsDB(hasher, marshalizer, factory.NewPeerAccountCreator(), trieFactoryManager)
	epochNotifier := forking.NewGenericEpochNotifier()

	argsValidatorsProcessor := peer.ArgValidatorStatisticsProcessor{
		Marshalizer:          marshalizer,
		NodesCoordinator:     &mock.NodesCoordinatorStub{},
		ShardCoordinator:     &mock.ShardCoordinatorStub{},
		DataPool:             &testscommon.PoolsHolderStub{},
		StorageService:       &mock.ChainStorerStub{},
		PubkeyConv:           &mock.PubkeyConverterMock{},
		PeerAdapter:          peerAccountsDB,
		Rater:                &mock.RaterStub{},
		RewardsHandler:       &mock.RewardsHandlerStub{},
		NodesSetup:           &mock.NodesSetupStub{},
		MaxComputableRounds:  1,
		EpochNotifier:        epochNotifier,
		StakingV2EnableEpoch: stakingV2EnableEpoch,
	}
	vCreator, _ := peer.NewValidatorStatisticsProcessor(argsValidatorsProcessor)

	blockChain, _ := blockchain.NewMetaChain(&mock.AppStatusHandlerStub{})
	testDataPool := testscommon.NewPoolsHolderMock()
	argsHook := hooks.ArgBlockChainHook{
		Accounts:           userAccountsDB,
		PubkeyConv:         &mock.PubkeyConverterMock{},
		StorageService:     &mock.ChainStorerStub{},
		BlockChain:         blockChain,
		ShardCoordinator:   &mock.ShardCoordinatorStub{},
		Marshalizer:        marshalizer,
		Uint64Converter:    &mock.Uint64ByteSliceConverterMock{},
		BuiltInFunctions:   vmcommonBuiltInFunctions.NewBuiltInFunctionContainer(),
		DataPool:           testDataPool,
		CompiledSCPool:     testDataPool.SmartContracts(),
		NilCompiledSCStore: true,
	}

	gasSchedule := arwenConfig.MakeGasMapForTests()
	defaults.FillGasMapInternal(gasSchedule, 1)
	signVerifer, _ := disabled.NewMessageSignVerifier(&mock.KeyGenMock{})

	nodesSetup := &mock.NodesSetupStub{}
	argsNewVMContainerFactory := metaProcess.ArgsNewVMContainerFactory{
		ArgBlockChainHook:   argsHook,
		Economics:           createEconomicsData(),
		MessageSignVerifier: signVerifer,
		GasSchedule:         mock.NewGasScheduleNotifierMock(gasSchedule),
		NodesConfigProvider: nodesSetup,
		Hasher:              hasher,
		Marshalizer:         marshalizer,
		SystemSCConfig: &config.SystemSmartContractsConfig{
			ESDTSystemSCConfig: config.ESDTSystemSCConfig{
				BaseIssuingCost: "1000",
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
				MaxNumberOfNodesForStake:             5,
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
		ValidatorAccountsDB: peerAccountsDB,
		ChanceComputer:      &mock.ChanceComputerStub{},
		EpochNotifier:       epochNotifier,
		EpochConfig: &config.EpochConfig{
			EnableEpochs: config.EnableEpochs{
				StakingV2EnableEpoch:               stakingV2EnableEpoch,
				StakeEnableEpoch:                   0,
				DelegationManagerEnableEpoch:       0,
				DelegationSmartContractEnableEpoch: 0,
			},
		},
		ShardCoordinator: &mock.ShardCoordinatorStub{},
	}
	metaVmFactory, _ := metaProcess.NewVMContainerFactory(argsNewVMContainerFactory)

	vmContainer, _ := metaVmFactory.Create()
	systemVM, _ := vmContainer.Get(vmFactory.SystemVirtualMachine)

	stakingSCprovider, _ := NewStakingDataProvider(systemVM, "1000")
	shardCoordinator, _ := sharding.NewMultiShardCoordinator(3, core.MetachainShardId)

	args := ArgsNewEpochStartSystemSCProcessing{
		SystemVM:                systemVM,
		UserAccountsDB:          userAccountsDB,
		PeerAccountsDB:          peerAccountsDB,
		Marshalizer:             marshalizer,
		StartRating:             5,
		ValidatorInfoCreator:    vCreator,
		EndOfEpochCallerAddress: vm.EndOfEpochAddress,
		StakingSCAddress:        vm.StakingSCAddress,
		ChanceComputer:          &mock.ChanceComputerStub{},
		EpochNotifier:           epochNotifier,
		GenesisNodesConfig:      nodesSetup,
		StakingDataProvider:     stakingSCprovider,
		NodesConfigProvider: &mock.NodesCoordinatorStub{
			ConsensusGroupSizeCalled: func(shardID uint32) int {
				if shardID == core.MetachainShardId {
					return 400
				}
				return 63
			},
		},
		ShardCoordinator:      shardCoordinator,
		ESDTOwnerAddressBytes: bytes.Repeat([]byte{1}, 32),
		EpochConfig: config.EpochConfig{
			EnableEpochs: config.EnableEpochs{
				StakingV2EnableEpoch: 1000000,
				ESDTEnableEpoch:      1000000,
			},
		},
	}
	return args, metaVmFactory.SystemSmartContractContainer()
}

func createEconomicsData() process.EconomicsDataHandler {
	maxGasLimitPerBlock := strconv.FormatUint(1500000000, 10)
	minGasPrice := strconv.FormatUint(10, 10)
	minGasLimit := strconv.FormatUint(10, 10)

	argsNewEconomicsData := economicsHandler.ArgsNewEconomicsData{
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
						ProtocolSustainabilityAddress:    "protocol",
						TopUpGradientPoint:               "300000000000000000000",
						TopUpFactor:                      0.25,
					},
				},
			},
			FeeSettings: config.FeeSettings{
				MaxGasLimitPerBlock:     maxGasLimitPerBlock,
				MaxGasLimitPerMetaBlock: maxGasLimitPerBlock,
				MinGasPrice:             minGasPrice,
				MinGasLimit:             minGasLimit,
				GasPerDataByte:          "1",
				GasPriceModifier:        1.0,
			},
		},
		PenalizedTooMuchGasEnableEpoch: 0,
		EpochNotifier:                  &mock.EpochNotifierStub{},
		BuiltInFunctionsCostHandler:    &mock.BuiltInCostHandlerStub{},
	}
	economicsData, _ := economicsHandler.NewEconomicsData(argsNewEconomicsData)
	return economicsData
}

func TestSystemSCProcessor_ProcessSystemSmartContractInitDelegationMgr(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(1000, createMemUnit())
	s, _ := NewSystemSCProcessor(args)

	s.flagDelegationEnabled.Set()
	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	acc, err := s.userAccountsDB.GetExistingAccount(vm.DelegationManagerSCAddress)
	assert.Nil(t, err)

	userAcc, _ := acc.(state.UserAccountHandler)
	assert.Equal(t, userAcc.GetOwnerAddress(), vm.DelegationManagerSCAddress)
	assert.NotNil(t, userAcc.GetCodeMetadata())
}

func TestSystemSCProcessor_ProcessDelegationRewardsNothingToExecute(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(1000, createMemUnit())
	s, _ := NewSystemSCProcessor(args)

	localCache, _ := dataPool.NewCurrentBlockPool()
	miniBlocks := []*block.MiniBlock{
		{
			SenderShardID:   0,
			ReceiverShardID: 0,
			TxHashes:        [][]byte{[]byte("txHash")},
		},
	}

	err := s.ProcessDelegationRewards(miniBlocks, localCache)
	assert.Nil(t, err)
}

func TestSystemSCProcessor_ProcessDelegationRewardsErrors(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(1000, createMemUnit())
	s, _ := NewSystemSCProcessor(args)

	localCache, _ := dataPool.NewCurrentBlockPool()
	miniBlocks := []*block.MiniBlock{
		{
			SenderShardID:   core.MetachainShardId,
			ReceiverShardID: core.MetachainShardId,
			TxHashes:        [][]byte{[]byte("txHash")},
			Type:            block.RewardsBlock,
		},
	}

	err := s.ProcessDelegationRewards(nil, localCache)
	assert.Nil(t, err)

	err = s.ProcessDelegationRewards(miniBlocks, nil)
	assert.Equal(t, err, epochStart.ErrNilLocalTxCache)

	err = s.ProcessDelegationRewards(miniBlocks, localCache)
	assert.Equal(t, err, dataRetriever.ErrTxNotFoundInBlockPool)

	rwdTx := &rewardTx.RewardTx{
		Round:   0,
		Value:   big.NewInt(100),
		RcvAddr: make([]byte, len(vm.StakingSCAddress)),
		Epoch:   0,
	}
	localCache.AddTx([]byte("txHash"), rwdTx)
	copy(rwdTx.RcvAddr, vm.StakingSCAddress)
	err = s.ProcessDelegationRewards(miniBlocks, localCache)
	assert.Equal(t, err, epochStart.ErrSystemDelegationCall)

	rwdTx.RcvAddr[25] = 255
	err = s.ProcessDelegationRewards(miniBlocks, localCache)
	assert.Equal(t, err, vm.ErrUnknownSystemSmartContract)

	rwdTx.RcvAddr = vm.FirstDelegationSCAddress
	err = s.ProcessDelegationRewards(miniBlocks, localCache)
	assert.Equal(t, err, epochStart.ErrSystemDelegationCall)
}

func TestSystemSCProcessor_ProcessDelegationRewards(t *testing.T) {
	t.Parallel()

	args, scContainer := createFullArgumentsForSystemSCProcessing(1000, createMemUnit())
	s, _ := NewSystemSCProcessor(args)

	localCache, _ := dataPool.NewCurrentBlockPool()
	miniBlocks := []*block.MiniBlock{
		{
			SenderShardID:   core.MetachainShardId,
			ReceiverShardID: core.MetachainShardId,
			TxHashes:        [][]byte{[]byte("txHash")},
			Type:            block.RewardsBlock,
		},
	}

	rwdTx := &rewardTx.RewardTx{
		Round:   0,
		Value:   big.NewInt(100),
		RcvAddr: make([]byte, len(vm.FirstDelegationSCAddress)),
		Epoch:   0,
	}
	copy(rwdTx.RcvAddr, vm.FirstDelegationSCAddress)
	rwdTx.RcvAddr[28] = 2
	localCache.AddTx([]byte("txHash"), rwdTx)

	contract, _ := scContainer.Get(vm.FirstDelegationSCAddress)
	_ = scContainer.Add(rwdTx.RcvAddr, contract)

	err := s.ProcessDelegationRewards(miniBlocks, localCache)
	assert.Nil(t, err)

	vmInput := &vmcommon.ContractCallInput{
		VMInput: vmcommon.VMInput{
			CallerAddr:  vm.EndOfEpochAddress,
			Arguments:   [][]byte{big.NewInt(int64(rwdTx.Epoch)).Bytes()},
			CallValue:   big.NewInt(0),
			GasProvided: 1000000,
		},
		RecipientAddr: rwdTx.RcvAddr,
		Function:      "getRewardData",
	}

	vmOutput, err := args.SystemVM.RunSmartContractCall(vmInput)
	assert.Nil(t, err)
	assert.NotNil(t, vmOutput)

	assert.Equal(t, len(vmOutput.ReturnData), 3)
	assert.True(t, bytes.Equal(vmOutput.ReturnData[0], rwdTx.Value.Bytes()))
}

func TestSystemSCProcessor_ProcessSystemSmartContractMaxNodesStakedFromQueue(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.MaxNodesEnableConfig = []config.MaxNodesChangeConfig{{EpochEnable: 0, MaxNumNodes: 10}}
	s, _ := NewSystemSCProcessor(args)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("stakedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		[]byte("rewardAddress"),
		[]byte("rewardAddress"),
	)

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	peerAcc, err := s.getPeerAccount([]byte("waitingPubKey"))
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(peerAcc.GetBLSPublicKey(), []byte("waitingPubKey")))
	assert.Equal(t, peerAcc.GetList(), string(core.NewList))
	numRegistered := getTotalNumberOfRegisteredNodes(t, s)
	assert.Equal(t, 1, numRegistered)
}

func getTotalNumberOfRegisteredNodes(t *testing.T, s *systemSCProcessor) int {
	vmInput := &vmcommon.ContractCallInput{
		VMInput: vmcommon.VMInput{
			CallerAddr: vm.EndOfEpochAddress,
			CallValue:  big.NewInt(0),
			Arguments:  make([][]byte, 0),
		},
		RecipientAddr: vm.StakingSCAddress,
		Function:      "getTotalNumberOfRegisteredNodes",
	}
	vmOutput, errRun := s.systemVM.RunSmartContractCall(vmInput)
	require.Nil(t, errRun)
	require.Equal(t, vmcommon.Ok, vmOutput.ReturnCode)
	require.Equal(t, 1, len(vmOutput.ReturnData))

	value := big.NewInt(0).SetBytes(vmOutput.ReturnData[0])

	return int(value.Int64())
}

func TestSystemSCProcessor_ProcessSystemSmartContractMaxNodesStakedFromQueueOwnerNotSet(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(10, createMemUnit())
	args.MaxNodesEnableConfig = []config.MaxNodesChangeConfig{{EpochEnable: 10, MaxNumNodes: 10}}
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 10
	s, _ := NewSystemSCProcessor(args)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("stakedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		[]byte("rewardAddress"),
		make([]byte, 0),
	)

	args.EpochNotifier.CheckEpoch(&testscommon.HeaderHandlerStub{
		EpochField: 10,
	})
	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 10)
	assert.Nil(t, err)

	peerAcc, err := s.getPeerAccount([]byte("waitingPubKey"))
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(peerAcc.GetBLSPublicKey(), []byte("waitingPubKey")))
	assert.Equal(t, peerAcc.GetList(), string(core.NewList))
}

func TestSystemSCProcessor_ESDTInitShouldWork(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.ESDTEnableEpoch = 1
	args.EpochConfig.EnableEpochs.SwitchJailWaitingEnableEpoch = 1000000
	hdr := &block.MetaBlock{
		Epoch: 1,
	}
	args.EpochNotifier.CheckEpoch(hdr)
	s, _ := NewSystemSCProcessor(args)

	initialContractConfig, err := s.extractConfigFromESDTContract()
	require.Nil(t, err)
	require.Equal(t, 4, len(initialContractConfig))
	require.Equal(t, []byte("aaaaaa"), initialContractConfig[0])

	err = s.ProcessSystemSmartContract(nil, 1, 1)

	require.Nil(t, err)

	updatedContractConfig, err := s.extractConfigFromESDTContract()
	require.Nil(t, err)
	require.Equal(t, 4, len(updatedContractConfig))
	require.Equal(t, args.ESDTOwnerAddressBytes, updatedContractConfig[0])
	//the other config values should be unchanged
	for i := 1; i < len(initialContractConfig); i++ {
		assert.Equal(t, initialContractConfig[i], updatedContractConfig[i])
	}
}

func TestSystemSCProcessor_ProcessSystemSmartContractUnStakeOneNodeStakeOthers(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("stakedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		[]byte("rewardAddress"),
		[]byte("rewardAddress"),
	)

	addStakedData(args.UserAccountsDB, []byte("stakedPubKey1"), []byte("ownerKey"), args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey2"), []byte("ownerKey"), args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey3"), []byte("ownerKey"), args.Marshalizer)
	addValidatorData(args.UserAccountsDB, []byte("ownerKey"), [][]byte{[]byte("stakedPubKey1"), []byte("stakedPubKey2"), []byte("stakedPubKey3")}, big.NewInt(2000), args.Marshalizer)
	_, _ = args.UserAccountsDB.Commit()

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey0"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("rewardAddress"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey1"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("rewardAddress"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey2"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("rewardAddress"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey3"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("rewardAddress"),
		AccumulatedFees: big.NewInt(0),
	})
	for _, vInfo := range validatorInfos[0] {
		jailedAcc, _ := args.PeerAccountsDB.LoadAccount(vInfo.PublicKey)
		_ = args.PeerAccountsDB.SaveAccount(jailedAcc)
	}

	s.flagSetOwnerEnabled.Unset()
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	peerAcc, err := s.getPeerAccount([]byte("waitingPubKey"))
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(peerAcc.GetBLSPublicKey(), []byte("waitingPubKey")))
	assert.Equal(t, peerAcc.GetList(), string(core.NewList))

	peerAcc, _ = s.getPeerAccount([]byte("stakedPubKey1"))
	assert.Equal(t, peerAcc.GetList(), string(core.LeavingList))

	assert.Equal(t, string(core.LeavingList), validatorInfos[0][1].List)

	assert.Equal(t, 5, len(validatorInfos[0]))
	assert.Equal(t, string(core.NewList), validatorInfos[0][4].List)
}

func TestSystemSCProcessor_ProcessSystemSmartContractUnStakeTheOnlyNodeShouldWork(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("stakedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		[]byte("rewardAddress"),
		[]byte("rewardAddress"),
	)

	addStakedData(args.UserAccountsDB, []byte("stakedPubKey1"), []byte("ownerKey"), args.Marshalizer)
	addValidatorDataWithUnStakedKey(args.UserAccountsDB, []byte("ownerKey"), [][]byte{[]byte("stakedPubKey1")}, big.NewInt(1000), args.Marshalizer)
	_, _ = args.UserAccountsDB.Commit()

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey0"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("rewardAddress"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey1"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("rewardAddress"),
		AccumulatedFees: big.NewInt(0),
	})

	s.flagSetOwnerEnabled.Unset()

	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)
}

func addDelegationData(
	accountsDB state.AccountsAdapter,
	delegation []byte,
	stakedKeys [][]byte,
	marshalizer marshal.Marshalizer,
) {
	delegatorSC := loadSCAccount(accountsDB, delegation)
	dStatus := &systemSmartContracts.DelegationContractStatus{
		StakedKeys:    make([]*systemSmartContracts.NodesData, 0),
		NotStakedKeys: make([]*systemSmartContracts.NodesData, 0),
		UnStakedKeys:  make([]*systemSmartContracts.NodesData, 0),
		NumUsers:      0,
	}

	for _, stakedKey := range stakedKeys {
		dStatus.StakedKeys = append(dStatus.StakedKeys, &systemSmartContracts.NodesData{BLSKey: stakedKey, SignedMsg: stakedKey})
	}

	marshaledData, _ := marshalizer.Marshal(dStatus)
	_ = delegatorSC.DataTrieTracker().SaveKeyValue([]byte("delegationStatus"), marshaledData)
	_ = accountsDB.SaveAccount(delegatorSC)
}

func TestSystemSCProcessor_ProcessSystemSmartContractUnStakeFromDelegationContract(t *testing.T) {
	t.Parallel()

	args, scContainer := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	delegationAddr := make([]byte, len(vm.FirstDelegationSCAddress))
	copy(delegationAddr, vm.FirstDelegationSCAddress)
	delegationAddr[28] = 2

	contract, _ := scContainer.Get(vm.FirstDelegationSCAddress)
	_ = scContainer.Add(delegationAddr, contract)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("stakedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		delegationAddr,
		delegationAddr,
	)

	addStakedData(args.UserAccountsDB, []byte("stakedPubKey1"), delegationAddr, args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey2"), delegationAddr, args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey3"), delegationAddr, args.Marshalizer)
	allKeys := [][]byte{[]byte("stakedPubKey0"), []byte("waitingPubKey"), []byte("stakedPubKey1"), []byte("stakedPubKey2"), []byte("stakedPubKey3")}
	addValidatorData(args.UserAccountsDB, delegationAddr, allKeys, big.NewInt(3000), args.Marshalizer)
	addDelegationData(args.UserAccountsDB, delegationAddr, allKeys, args.Marshalizer)
	_, _ = args.UserAccountsDB.Commit()

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey0"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey1"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey2"),
		List:            string(core.WaitingList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey3"),
		List:            string(core.WaitingList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	for _, vInfo := range validatorInfos[0] {
		jailedAcc, _ := args.PeerAccountsDB.LoadAccount(vInfo.PublicKey)
		_ = args.PeerAccountsDB.SaveAccount(jailedAcc)
	}

	s.flagSetOwnerEnabled.Unset()
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	for _, vInfo := range validatorInfos[0] {
		assert.NotEqual(t, string(core.NewList), vInfo.List)
	}

	peerAcc, _ := s.getPeerAccount([]byte("stakedPubKey2"))
	assert.Equal(t, peerAcc.GetList(), string(core.LeavingList))
	assert.Equal(t, 4, len(validatorInfos[0]))

	delegationSC := loadSCAccount(args.UserAccountsDB, delegationAddr)
	marshalledData, err := delegationSC.DataTrie().Get([]byte("delegationStatus"))
	assert.Nil(t, err)
	dStatus := &systemSmartContracts.DelegationContractStatus{
		StakedKeys:    make([]*systemSmartContracts.NodesData, 0),
		NotStakedKeys: make([]*systemSmartContracts.NodesData, 0),
		UnStakedKeys:  make([]*systemSmartContracts.NodesData, 0),
		NumUsers:      0,
	}
	_ = args.Marshalizer.Unmarshal(dStatus, marshalledData)

	assert.Equal(t, 2, len(dStatus.UnStakedKeys))
	assert.Equal(t, 3, len(dStatus.StakedKeys))
	assert.Equal(t, []byte("stakedPubKey2"), dStatus.UnStakedKeys[1].BLSKey)
}

func TestSystemSCProcessor_ProcessSystemSmartContractShouldUnStakeFromAdditionalQueueOnly(t *testing.T) {
	t.Parallel()

	args, scContainer := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	delegationAddr := make([]byte, len(vm.FirstDelegationSCAddress))
	copy(delegationAddr, vm.FirstDelegationSCAddress)
	delegationAddr[28] = 2

	contract, _ := scContainer.Get(vm.FirstDelegationSCAddress)
	_ = scContainer.Add(delegationAddr, contract)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("stakedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		delegationAddr,
		delegationAddr,
	)

	addStakedData(args.UserAccountsDB, []byte("stakedPubKey1"), delegationAddr, args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey2"), delegationAddr, args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey3"), delegationAddr, args.Marshalizer)

	listOfKeysInWaiting := [][]byte{[]byte("waitingPubKe1"), []byte("waitingPubKe2"), []byte("waitingPubKe3"), []byte("waitingPubKe4")}
	allStakedKeys := append(listOfKeysInWaiting, []byte("waitingPubKey"), []byte("stakedPubKey0"), []byte("stakedPubKey1"), []byte("stakedPubKey2"), []byte("stakedPubKey3"))
	addKeysToWaitingList(args.UserAccountsDB, listOfKeysInWaiting, args.Marshalizer, delegationAddr, delegationAddr)
	addValidatorData(args.UserAccountsDB, delegationAddr, allStakedKeys, big.NewInt(4000), args.Marshalizer)
	addDelegationData(args.UserAccountsDB, delegationAddr, allStakedKeys, args.Marshalizer)
	_, _ = args.UserAccountsDB.Commit()

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey0"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey1"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey2"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey3"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	for _, vInfo := range validatorInfos[0] {
		jailedAcc, _ := args.PeerAccountsDB.LoadAccount(vInfo.PublicKey)
		_ = args.PeerAccountsDB.SaveAccount(jailedAcc)
	}

	s.flagSetOwnerEnabled.Unset()
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	for _, vInfo := range validatorInfos[0] {
		assert.Equal(t, string(core.EligibleList), vInfo.List)
	}

	delegationSC := loadSCAccount(args.UserAccountsDB, delegationAddr)
	marshalledData, err := delegationSC.DataTrie().Get([]byte("delegationStatus"))
	assert.Nil(t, err)
	dStatus := &systemSmartContracts.DelegationContractStatus{
		StakedKeys:    make([]*systemSmartContracts.NodesData, 0),
		NotStakedKeys: make([]*systemSmartContracts.NodesData, 0),
		UnStakedKeys:  make([]*systemSmartContracts.NodesData, 0),
		NumUsers:      0,
	}
	_ = args.Marshalizer.Unmarshal(dStatus, marshalledData)

	assert.Equal(t, 5, len(dStatus.UnStakedKeys))
	assert.Equal(t, 4, len(dStatus.StakedKeys))
}

func TestSystemSCProcessor_ProcessSystemSmartContractUnStakeFromAdditionalQueue(t *testing.T) {
	t.Parallel()

	args, scContainer := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	delegationAddr := make([]byte, len(vm.FirstDelegationSCAddress))
	copy(delegationAddr, vm.FirstDelegationSCAddress)
	delegationAddr[28] = 2

	contract, _ := scContainer.Get(vm.FirstDelegationSCAddress)
	_ = scContainer.Add(delegationAddr, contract)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("stakedPubKey0"),
		[]byte("waitingPubKey"),
		args.Marshalizer,
		delegationAddr,
		delegationAddr,
	)

	addStakedData(args.UserAccountsDB, []byte("stakedPubKey1"), delegationAddr, args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey2"), delegationAddr, args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey3"), delegationAddr, args.Marshalizer)
	addValidatorData(args.UserAccountsDB, delegationAddr, [][]byte{[]byte("stakedPubKey1"), []byte("stakedPubKey2"), []byte("stakedPubKey3"), []byte("waitingPubKey")}, big.NewInt(10000), args.Marshalizer)
	addDelegationData(args.UserAccountsDB, delegationAddr, [][]byte{[]byte("stakedPubKey1"), []byte("stakedPubKey2"), []byte("stakedPubKey3"), []byte("waitingPubKey")}, args.Marshalizer)
	_, _ = args.UserAccountsDB.Commit()

	delegationAddr2 := make([]byte, len(vm.FirstDelegationSCAddress))
	copy(delegationAddr2, vm.FirstDelegationSCAddress)
	delegationAddr2[28] = 5
	_ = scContainer.Add(delegationAddr2, contract)

	listOfKeysInWaiting := [][]byte{[]byte("waitingPubKe1"), []byte("waitingPubKe2"), []byte("waitingPubKe3"), []byte("waitingPubKe4")}
	addKeysToWaitingList(args.UserAccountsDB, listOfKeysInWaiting, args.Marshalizer, delegationAddr2, delegationAddr2)
	addValidatorData(args.UserAccountsDB, delegationAddr2, listOfKeysInWaiting, big.NewInt(2000), args.Marshalizer)
	addDelegationData(args.UserAccountsDB, delegationAddr2, listOfKeysInWaiting, args.Marshalizer)
	_, _ = args.UserAccountsDB.Commit()

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey0"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey1"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey2"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey3"),
		List:            string(core.EligibleList),
		RewardAddress:   delegationAddr,
		AccumulatedFees: big.NewInt(0),
	})
	for _, vInfo := range validatorInfos[0] {
		peerAcc, _ := args.PeerAccountsDB.LoadAccount(vInfo.PublicKey)
		_ = args.PeerAccountsDB.SaveAccount(peerAcc)
	}
	s.flagSetOwnerEnabled.Unset()
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	delegationSC := loadSCAccount(args.UserAccountsDB, delegationAddr2)
	marshalledData, err := delegationSC.DataTrie().Get([]byte("delegationStatus"))
	assert.Nil(t, err)
	dStatus := &systemSmartContracts.DelegationContractStatus{
		StakedKeys:    make([]*systemSmartContracts.NodesData, 0),
		NotStakedKeys: make([]*systemSmartContracts.NodesData, 0),
		UnStakedKeys:  make([]*systemSmartContracts.NodesData, 0),
		NumUsers:      0,
	}
	_ = args.Marshalizer.Unmarshal(dStatus, marshalledData)

	assert.Equal(t, 2, len(dStatus.UnStakedKeys))
	assert.Equal(t, 2, len(dStatus.StakedKeys))
	assert.Equal(t, []byte("waitingPubKe4"), dStatus.UnStakedKeys[0].BLSKey)
	assert.Equal(t, []byte("waitingPubKe3"), dStatus.UnStakedKeys[1].BLSKey)

	stakingSCAcc := loadSCAccount(args.UserAccountsDB, vm.StakingSCAddress)
	marshaledData, _ := stakingSCAcc.DataTrieTracker().RetrieveValue([]byte("waitingList"))
	waitingListHead := &systemSmartContracts.WaitingList{}
	_ = args.Marshalizer.Unmarshal(waitingListHead, marshaledData)
	assert.Equal(t, uint32(3), waitingListHead.Length)
}

func TestSystemSCProcessor_ProcessSystemSmartContractWrongValidatorInfoShouldBeCleaned(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	prepareStakingContractWithData(
		args.UserAccountsDB,
		[]byte("oneAddress1"),
		[]byte("oneAddress2"),
		args.Marshalizer,
		[]byte("oneAddress1"),
		[]byte("oneAddress1"),
	)

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey0"),
		List:            "",
		RewardAddress:   []byte("stakedPubKey0"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey1"),
		List:            "",
		RewardAddress:   []byte("stakedPubKey0"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey2"),
		List:            "",
		RewardAddress:   []byte("stakedPubKey0"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey3"),
		List:            "",
		RewardAddress:   []byte("stakedPubKey0"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("oneAddress1"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("oneAddress1"),
		AccumulatedFees: big.NewInt(0),
	})

	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	assert.Equal(t, len(validatorInfos[0]), 1)
}

func TestSystemSCProcessor_TogglePauseUnPause(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	err := s.ToggleUnStakeUnBond(true)
	assert.Nil(t, err)

	validatorSC := loadSCAccount(s.userAccountsDB, vm.ValidatorSCAddress)
	value, _ := validatorSC.DataTrie().Get([]byte("unStakeUnBondPause"))
	assert.True(t, value[0] == 1)

	err = s.ToggleUnStakeUnBond(false)
	assert.Nil(t, err)

	validatorSC = loadSCAccount(s.userAccountsDB, vm.ValidatorSCAddress)
	value, _ = validatorSC.DataTrie().Get([]byte("unStakeUnBondPause"))
	assert.True(t, value[0] == 0)
}

func TestSystemSCProcessor_ResetUnJailListErrors(t *testing.T) {
	t.Parallel()

	localErr := errors.New("local error")
	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	s, _ := NewSystemSCProcessor(args)
	s.systemVM = &mock.VMExecutionHandlerStub{RunSmartContractCallCalled: func(input *vmcommon.ContractCallInput) (*vmcommon.VMOutput, error) {
		return nil, localErr
	}}

	err := s.resetLastUnJailed()
	assert.Equal(t, localErr, err)

	s.systemVM = &mock.VMExecutionHandlerStub{RunSmartContractCallCalled: func(input *vmcommon.ContractCallInput) (*vmcommon.VMOutput, error) {
		return &vmcommon.VMOutput{ReturnCode: vmcommon.UserError}, nil
	}}

	err = s.resetLastUnJailed()
	assert.Equal(t, epochStart.ErrResetLastUnJailedFromQueue, err)
}

func TestSystemSCProcessor_ProcessSystemSmartContractJailAndUnStake(t *testing.T) {
	t.Parallel()

	args, _ := createFullArgumentsForSystemSCProcessing(0, createMemUnit())
	args.EpochConfig.EnableEpochs.StakingV2EnableEpoch = 0
	s, _ := NewSystemSCProcessor(args)

	addStakedData(args.UserAccountsDB, []byte("stakedPubKey0"), []byte("ownerKey"), args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey1"), []byte("ownerKey"), args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey2"), []byte("ownerKey"), args.Marshalizer)
	addStakedData(args.UserAccountsDB, []byte("stakedPubKey3"), []byte("ownerKey"), args.Marshalizer)
	saveOneKeyToWaitingList(args.UserAccountsDB, []byte("waitingPubKey"), args.Marshalizer, []byte("ownerKey"), []byte("ownerKey"))
	addValidatorData(args.UserAccountsDB, []byte("ownerKey"), [][]byte{[]byte("stakedPubKey0"), []byte("stakedPubKey1"), []byte("stakedPubKey2"), []byte("stakedPubKey3"), []byte("waitingPubKey")}, big.NewInt(0), args.Marshalizer)
	_, _ = args.UserAccountsDB.Commit()

	validatorInfos := make(map[uint32][]*state.ValidatorInfo)
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey0"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("ownerKey"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey1"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("ownerKey"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey2"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("ownerKey"),
		AccumulatedFees: big.NewInt(0),
	})
	validatorInfos[0] = append(validatorInfos[0], &state.ValidatorInfo{
		PublicKey:       []byte("stakedPubKey3"),
		List:            string(core.EligibleList),
		RewardAddress:   []byte("ownerKey"),
		AccumulatedFees: big.NewInt(0),
	})

	for _, vInfo := range validatorInfos[0] {
		jailedAcc, _ := args.PeerAccountsDB.LoadAccount(vInfo.PublicKey)
		_ = args.PeerAccountsDB.SaveAccount(jailedAcc)
	}

	s.flagSetOwnerEnabled.Unset()
	err := s.ProcessSystemSmartContract(validatorInfos, 0, 0)
	assert.Nil(t, err)

	_, err = s.peerAccountsDB.GetExistingAccount([]byte("waitingPubKey"))
	assert.NotNil(t, err)

	assert.Equal(t, 4, len(validatorInfos[0]))
	for _, vInfo := range validatorInfos[0] {
		assert.Equal(t, vInfo.List, string(core.LeavingList))
		peerAcc, _ := s.getPeerAccount(vInfo.PublicKey)
		assert.Equal(t, peerAcc.GetList(), string(core.LeavingList))
	}
}
