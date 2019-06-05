package node

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"time"

	"github.com/ElrondNetwork/elrond-go-sandbox/config"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/chronology"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/spos/sposFactory"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/validators"
	"github.com/ElrondNetwork/elrond-go-sandbox/consensus/validators/groupSelectors"
	"github.com/ElrondNetwork/elrond-go-sandbox/core"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/genesis"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/logger"
	"github.com/ElrondNetwork/elrond-go-sandbox/crypto"
	"github.com/ElrondNetwork/elrond-go-sandbox/data"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/state"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/transaction"
	"github.com/ElrondNetwork/elrond-go-sandbox/data/typeConverters"
	"github.com/ElrondNetwork/elrond-go-sandbox/dataRetriever"
	"github.com/ElrondNetwork/elrond-go-sandbox/hashing"
	"github.com/ElrondNetwork/elrond-go-sandbox/marshal"
	"github.com/ElrondNetwork/elrond-go-sandbox/node/heartbeat"
	"github.com/ElrondNetwork/elrond-go-sandbox/ntp"
	"github.com/ElrondNetwork/elrond-go-sandbox/p2p"
	"github.com/ElrondNetwork/elrond-go-sandbox/process"
	"github.com/ElrondNetwork/elrond-go-sandbox/process/factory"
	"github.com/ElrondNetwork/elrond-go-sandbox/process/sync"
	"github.com/ElrondNetwork/elrond-go-sandbox/sharding"
)

// WaitTime defines the time in milliseconds until node waits the requested info from the network
const WaitTime = time.Duration(2000 * time.Millisecond)

// SendTransactionsPipe is the pipe used for sending new transactions
const SendTransactionsPipe = "send transactions pipe"

// HeartbeatTopic is the topic used for heartbeat signaling
const HeartbeatTopic = "heartbeat"

var log = logger.DefaultLogger()

// Option represents a functional configuration parameter that can operate
//  over the None struct.
type Option func(*Node) error

// Node is a structure that passes the configuration parameters and initializes
//  required services as requested
type Node struct {
	marshalizer              marshal.Marshalizer
	ctx                      context.Context
	hasher                   hashing.Hasher
	initialNodesPubkeys      map[uint32][]string
	initialNodesBalances     map[string]*big.Int
	roundDuration            uint64
	consensusGroupSize       int
	messenger                P2PMessenger
	syncTimer                ntp.SyncTimer
	rounder                  consensus.Rounder
	blockProcessor           process.BlockProcessor
	blockTracker             process.BlocksTracker
	genesisTime              time.Time
	accounts                 state.AccountsAdapter
	addrConverter            state.AddressConverter
	uint64ByteSliceConverter typeConverters.Uint64ByteSliceConverter
	interceptorsContainer    process.InterceptorsContainer
	resolversFinder          dataRetriever.ResolversFinder
	heartbeatMonitor         *heartbeat.Monitor
	heartbeatSender          *heartbeat.Sender

	txSignPrivKey  crypto.PrivateKey
	txSignPubKey   crypto.PublicKey
	pubKey         crypto.PublicKey
	privKey        crypto.PrivateKey
	keyGen         crypto.KeyGenerator
	singleSigner   crypto.SingleSigner
	txSingleSigner crypto.SingleSigner
	multiSigner    crypto.MultiSigner
	forkDetector   process.ForkDetector

	blkc             data.ChainHandler
	dataPool         dataRetriever.PoolsHolder
	metaDataPool     dataRetriever.MetaPoolsHolder
	store            dataRetriever.StorageService
	shardCoordinator sharding.Coordinator

	consensusTopic string
	consensusType  string

	isRunning                bool
	isMetachainActive        bool
	txStorageSize            uint32
	currentSendingGoRoutines int32
}

// ApplyOptions can set up different configurable options of a Node instance
func (n *Node) ApplyOptions(opts ...Option) error {
	if n.IsRunning() {
		return errors.New("cannot apply options while node is running")
	}
	for _, opt := range opts {
		err := opt(n)
		if err != nil {
			return errors.New("error applying option: " + err.Error())
		}
	}
	return nil
}

// NewNode creates a new Node instance
func NewNode(opts ...Option) (*Node, error) {
	node := &Node{
		ctx:                      context.Background(),
		isMetachainActive:        true,
		currentSendingGoRoutines: 0,
	}
	for _, opt := range opts {
		err := opt(node)
		if err != nil {
			return nil, errors.New("error applying option: " + err.Error())
		}
	}

	return node, nil
}

// IsRunning will return the current state of the node
func (n *Node) IsRunning() bool {
	return n.isRunning
}

// Start will create a new messenger and and set up the Node state as running
func (n *Node) Start() error {
	err := n.P2PBootstrap()
	if err == nil {
		n.isRunning = true
	}
	return err
}

// Stop closes the messenger and undos everything done in Start
func (n *Node) Stop() error {
	if !n.IsRunning() {
		return nil
	}
	err := n.messenger.Close()
	if err != nil {
		return err
	}

	return nil
}

// P2PBootstrap will try to connect to many peers as possible
func (n *Node) P2PBootstrap() error {
	if n.messenger == nil {
		return ErrNilMessenger
	}

	return n.messenger.Bootstrap()
}

// CreateShardedStores instantiate sharded cachers for Transactions and Headers
func (n *Node) CreateShardedStores() error {
	if n.shardCoordinator == nil {
		return ErrNilShardCoordinator
	}

	if n.dataPool == nil {
		return ErrNilDataPool
	}

	transactionsDataStore := n.dataPool.Transactions()
	headersDataStore := n.dataPool.Headers()

	if transactionsDataStore == nil {
		return errors.New("nil transaction sharded data store")
	}

	if headersDataStore == nil {
		return errors.New("nil header sharded data store")
	}

	shards := n.shardCoordinator.NumberOfShards()
	currentShardId := n.shardCoordinator.SelfId()

	transactionsDataStore.CreateShardStore(process.ShardCacherIdentifier(currentShardId, currentShardId))
	for i := uint32(0); i < shards; i++ {
		if i == n.shardCoordinator.SelfId() {
			continue
		}
		transactionsDataStore.CreateShardStore(process.ShardCacherIdentifier(i, currentShardId))
		transactionsDataStore.CreateShardStore(process.ShardCacherIdentifier(currentShardId, i))
	}

	return nil
}

// StartConsensus will start the consesus service for the current node
func (n *Node) StartConsensus() error {
	isGenesisBlockNotInitialized := n.blkc.GetGenesisHeaderHash() == nil ||
		n.blkc.GetGenesisHeader() == nil
	if isGenesisBlockNotInitialized {
		return ErrGenesisBlockNotInitialized
	}

	chronologyHandler, err := n.createChronologyHandler(n.rounder)
	if err != nil {
		return err
	}

	bootstrapper, err := n.createBootstrapper(n.rounder)
	if err != nil {
		return err
	}

	bootstrapper.StartSync()

	consensusState, err := n.createConsensusState()
	if err != nil {
		return err
	}

	consensusService, err := sposFactory.GetConsensusCoreFactory(n.consensusType)
	if err != nil {
		return err
	}

	broadcastMessenger, err := sposFactory.GetBroadcastMessenger(
		n.marshalizer,
		n.messenger,
		n.shardCoordinator,
		n.privKey,
		n.singleSigner,
		n.syncTimer)

	if err != nil {
		return err
	}

	worker, err := spos.NewWorker(
		consensusService,
		n.blockProcessor,
		n.blockTracker,
		bootstrapper,
		broadcastMessenger,
		consensusState,
		n.forkDetector,
		n.keyGen,
		n.marshalizer,
		n.rounder,
		n.shardCoordinator,
		n.singleSigner,
		n.syncTimer,
	)
	if err != nil {
		return err
	}

	err = n.createConsensusTopic(worker, n.shardCoordinator)
	if err != nil {
		return err
	}

	validatorGroupSelector, err := n.createValidatorGroupSelector()
	if err != nil {
		return err
	}

	consensusDataContainer, err := spos.NewConsensusCore(
		n.blkc,
		n.blockProcessor,
		bootstrapper,
		broadcastMessenger,
		chronologyHandler,
		n.hasher,
		n.marshalizer,
		n.privKey,
		n.singleSigner,
		n.multiSigner,
		n.rounder,
		n.shardCoordinator,
		n.syncTimer,
		validatorGroupSelector)
	if err != nil {
		return err
	}

	fct, err := sposFactory.GetSubroundsFactory(consensusDataContainer, consensusState, worker, n.consensusType)
	if err != nil {
		return err
	}

	err = fct.GenerateSubrounds()
	if err != nil {
		return err
	}

	go chronologyHandler.StartRounds()

	return nil
}

// CreateShardGenesisBlock creates the shard genesis block
func (n *Node) CreateShardGenesisBlock() error {
	header, err := genesis.CreateShardGenesisBlockFromInitialBalances(
		n.accounts,
		n.shardCoordinator,
		n.addrConverter,
		n.initialNodesBalances,
		uint64(n.genesisTime.Unix()),
	)
	if err != nil {
		return err
	}

	marshalizedHeader, err := n.marshalizer.Marshal(header)
	if err != nil {
		return err
	}

	blockHeaderHash := n.hasher.Compute(string(marshalizedHeader))

	return n.setGenesis(header, blockHeaderHash)
}

// CreateMetaGenesisBlock creates the meta genesis block
func (n *Node) CreateMetaGenesisBlock() error {
	header, err := genesis.CreateMetaGenesisBlock(uint64(n.genesisTime.Unix()), n.initialNodesPubkeys)
	marshalizedHeader, err := n.marshalizer.Marshal(header)
	if err != nil {
		return err
	}

	blockHeaderHash := n.hasher.Compute(string(marshalizedHeader))
	err = n.store.Put(dataRetriever.MetaBlockUnit, blockHeaderHash, marshalizedHeader)
	if err != nil {
		return err
	}

	return n.setGenesis(header, blockHeaderHash)
}

func (n *Node) setGenesis(genesisHeader data.HeaderHandler, genesisHeaderHash []byte) error {
	err := n.blkc.SetGenesisHeader(genesisHeader)
	if err != nil {
		return err
	}

	n.blkc.SetGenesisHeaderHash(genesisHeaderHash)
	return nil
}

// GetBalance gets the balance for a specific address
func (n *Node) GetBalance(addressHex string) (*big.Int, error) {
	if n.addrConverter == nil || n.accounts == nil {
		return nil, errors.New("initialize AccountsAdapter and AddressConverter first")
	}

	address, err := n.addrConverter.CreateAddressFromHex(addressHex)
	if err != nil {
		return nil, errors.New("invalid address, could not decode from hex: " + err.Error())
	}
	accWrp, err := n.accounts.GetExistingAccount(address)
	if err != nil {
		return nil, errors.New("could not fetch sender address from provided param: " + err.Error())
	}

	if accWrp == nil {
		return big.NewInt(0), nil
	}

	account, ok := accWrp.(*state.Account)
	if !ok {
		return big.NewInt(0), nil
	}

	return account.Balance, nil
}

// createChronologyHandler method creates a chronology object
func (n *Node) createChronologyHandler(rounder consensus.Rounder) (consensus.ChronologyHandler, error) {
	chr, err := chronology.NewChronology(
		n.genesisTime,
		rounder,
		n.syncTimer)

	if err != nil {
		return nil, err
	}

	return chr, nil
}

//// BroadcastBlock returns a function used for broadcast blocks
//func (n *Node) BroadcastBlock() func(data.BodyHandler, data.HeaderHandler) error {
//	if n.shardCoordinator.SelfId() < n.shardCoordinator.NumberOfShards() {
//		return n.broadcastShardBlock
//	}
//
//	if n.shardCoordinator.SelfId() == sharding.MetachainShardId {
//		return n.broadcastMetaBlock
//	}
//
//	return nil
//}
//
//// BroadcastHeader returns a function used for broadcast headers
//func (n *Node) BroadcastHeader() func(data.HeaderHandler) error {
//	if n.shardCoordinator.SelfId() < n.shardCoordinator.NumberOfShards() {
//		return n.broadcastShardHeader
//	}
//
//	if n.shardCoordinator.SelfId() == sharding.MetachainShardId {
//		return n.broadcastMetaHeader
//	}
//
//	return nil
//}
//
//// BroadcastMiniBlocksAndTransactions returns a function used for broadcast transactions
//func (n *Node) BroadcastMiniBlocksAndTransactions() func(data.BodyHandler, data.HeaderHandler) error {
//	if n.shardCoordinator.SelfId() < n.shardCoordinator.NumberOfShards() {
//		return n.broadcastMiniBlocksAndTransactions
//	}
//
//	if n.shardCoordinator.SelfId() == sharding.MetachainShardId {
//		return n.broadcastMetaShardHeaders
//	}
//
//	return nil
//}
//
//// BroadcastConsensusMessage returns a function used for broadcast consensus messages
//func (n *Node) BroadcastConsensusMessage() func(*consensus.Message) error {
//	return n.broadcastConsensusMessage
//}

func (n *Node) createBootstrapper(rounder consensus.Rounder) (process.Bootstrapper, error) {
	if n.shardCoordinator.SelfId() < n.shardCoordinator.NumberOfShards() {
		return n.createShardBootstrapper(rounder)
	}

	if n.shardCoordinator.SelfId() == sharding.MetachainShardId {
		return n.createMetaChainBootstrapper(rounder)
	}

	return nil, sharding.ErrShardIdOutOfRange
}

func (n *Node) createShardBootstrapper(rounder consensus.Rounder) (process.Bootstrapper, error) {
	bootstrap, err := sync.NewShardBootstrap(
		n.dataPool,
		n.store,
		n.blkc,
		rounder,
		n.blockProcessor,
		WaitTime,
		n.hasher,
		n.marshalizer,
		n.forkDetector,
		n.resolversFinder,
		n.shardCoordinator,
		n.accounts,
	)
	if err != nil {
		return nil, err
	}

	return bootstrap, nil
}

func (n *Node) createMetaChainBootstrapper(rounder consensus.Rounder) (process.Bootstrapper, error) {
	bootstrap, err := sync.NewMetaBootstrap(
		n.metaDataPool,
		n.store,
		n.blkc,
		rounder,
		n.blockProcessor,
		WaitTime,
		n.hasher,
		n.marshalizer,
		n.forkDetector,
		n.resolversFinder,
		n.shardCoordinator,
		n.accounts,
	)

	if err != nil {
		return nil, err
	}

	return bootstrap, nil
}

// createConsensusState method creates a consensusState object
func (n *Node) createConsensusState() (*spos.ConsensusState, error) {
	selfId, err := n.pubKey.ToByteArray()

	if err != nil {
		return nil, err
	}

	roundConsensus := spos.NewRoundConsensus(
		n.initialNodesPubkeys[n.shardCoordinator.SelfId()],
		n.consensusGroupSize,
		string(selfId))

	roundConsensus.ResetRoundState()

	roundThreshold := spos.NewRoundThreshold()

	roundStatus := spos.NewRoundStatus()
	roundStatus.ResetRoundStatus()

	consensusState := spos.NewConsensusState(
		roundConsensus,
		roundThreshold,
		roundStatus)

	return consensusState, nil
}

// createValidatorGroupSelector creates a index hashed group selector object
func (n *Node) createValidatorGroupSelector() (consensus.ValidatorGroupSelector, error) {
	validatorGroupSelector, err := groupSelectors.NewIndexHashedGroupSelector(n.consensusGroupSize, n.hasher)
	if err != nil {
		return nil, err
	}

	validatorsList := make([]consensus.Validator, 0)
	shID := n.shardCoordinator.SelfId()

	if len(n.initialNodesPubkeys[shID]) == 0 {
		return nil, errors.New("could not create validator group as shardID is out of range")
	}

	for i := 0; i < len(n.initialNodesPubkeys[shID]); i++ {
		validator, err := validators.NewValidator(big.NewInt(0), 0, []byte(n.initialNodesPubkeys[shID][i]))
		if err != nil {
			return nil, err
		}

		validatorsList = append(validatorsList, validator)
	}

	err = validatorGroupSelector.LoadEligibleList(validatorsList)
	if err != nil {
		return nil, err
	}

	return validatorGroupSelector, nil
}

// createConsensusTopic creates a consensus topic for node
func (n *Node) createConsensusTopic(messageProcessor p2p.MessageProcessor, shardCoordinator sharding.Coordinator) error {
	if shardCoordinator == nil {
		return ErrNilShardCoordinator
	}
	if messageProcessor == nil {
		return ErrNilMessenger
	}

	n.consensusTopic = core.ConsensusTopic + shardCoordinator.CommunicationIdentifier(shardCoordinator.SelfId())
	if n.messenger.HasTopicValidator(n.consensusTopic) {
		return ErrValidatorAlreadySet
	}

	if !n.messenger.HasTopic(n.consensusTopic) {
		err := n.messenger.CreateTopic(n.consensusTopic, true)
		if err != nil {
			return err
		}
	}

	return n.messenger.RegisterMessageProcessor(n.consensusTopic, messageProcessor)
}

// SendTransaction will send a new transaction on the topic channel
func (n *Node) SendTransaction(
	nonce uint64,
	senderHex string,
	receiverHex string,
	value *big.Int,
	transactionData string,
	signature []byte) (*transaction.Transaction, error) {

	if n.shardCoordinator == nil {
		return nil, ErrNilShardCoordinator
	}

	sender, err := n.addrConverter.CreateAddressFromHex(senderHex)
	if err != nil {
		return nil, err
	}

	receiver, err := n.addrConverter.CreateAddressFromHex(receiverHex)
	if err != nil {
		return nil, err
	}

	senderShardId := n.shardCoordinator.ComputeId(sender)

	tx := transaction.Transaction{
		Nonce:     nonce,
		Value:     value,
		RcvAddr:   receiver.Bytes(),
		SndAddr:   sender.Bytes(),
		Data:      []byte(transactionData),
		Signature: signature,
	}

	txBuff, err := n.marshalizer.Marshal(&tx)
	if err != nil {
		return nil, err
	}

	marshalizedTx, err := n.marshalizer.Marshal([][]byte{txBuff})
	if err != nil {
		return nil, errors.New("could not marshal transaction")
	}

	//the topic identifier is made of the current shard id and sender's shard id
	identifier := factory.TransactionTopic + n.shardCoordinator.CommunicationIdentifier(senderShardId)

	n.messenger.BroadcastOnChannel(
		SendTransactionsPipe,
		identifier,
		marshalizedTx,
	)

	return &tx, nil
}

//GetTransaction gets the transaction
func (n *Node) GetTransaction(hash string) (*transaction.Transaction, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// GetCurrentPublicKey will return the current node's public key
func (n *Node) GetCurrentPublicKey() string {
	if n.txSignPubKey != nil {
		pkey, _ := n.txSignPubKey.ToByteArray()
		return fmt.Sprintf("%x", pkey)
	}
	return ""
}

// GetAccount will return acount details for a given address
func (n *Node) GetAccount(address string) (*state.Account, error) {
	if n.addrConverter == nil || n.accounts == nil {
		return nil, errors.New("initialize AccountsAdapter and AddressConverter first")
	}

	addr, err := n.addrConverter.CreateAddressFromHex(address)
	if err != nil {
		return nil, errors.New("could not create address object from provided string")
	}

	accWrp, err := n.accounts.GetExistingAccount(addr)
	if err != nil {
		return nil, errors.New("could not fetch sender address from provided param")
	}

	account, ok := accWrp.(*state.Account)
	if !ok {
		return nil, errors.New("account is not of type with balance and nonce")
	}

	return account, nil
}

// StartHeartbeat starts the node's heartbeat processing/signaling module
func (n *Node) StartHeartbeat(config config.HeartbeatConfig) error {
	if !config.Enabled {
		return nil
	}

	err := n.checkConfigParams(config)
	if err != nil {
		return err
	}

	if n.messenger.HasTopicValidator(HeartbeatTopic) {
		return ErrValidatorAlreadySet
	}

	if !n.messenger.HasTopic(HeartbeatTopic) {
		err := n.messenger.CreateTopic(HeartbeatTopic, true)
		if err != nil {
			return err
		}
	}

	n.heartbeatSender, err = heartbeat.NewSender(
		n.messenger,
		n.singleSigner,
		n.privKey,
		n.marshalizer,
		HeartbeatTopic,
	)
	if err != nil {
		return err
	}

	allPubKeys := make([]string, 0)
	for _, shardPubKeys := range n.initialNodesPubkeys {
		allPubKeys = append(allPubKeys, shardPubKeys...)
	}

	n.heartbeatMonitor, err = heartbeat.NewMonitor(
		n.messenger,
		n.singleSigner,
		n.keyGen,
		n.marshalizer,
		time.Duration(time.Second*time.Duration(config.DurationInSecToConsiderUnresponsive)),
		allPubKeys,
	)
	if err != nil {
		return err
	}

	err = n.messenger.RegisterMessageProcessor(HeartbeatTopic, n.heartbeatMonitor)
	if err != nil {
		return err
	}

	go n.startSendingHeartbeats(config)

	return nil
}

func (n *Node) checkConfigParams(config config.HeartbeatConfig) error {
	if config.DurationInSecToConsiderUnresponsive < 1 {
		return ErrNegativeDurationInSecToConsiderUnresponsive
	}
	if config.MaxTimeToWaitBetweenBroadcastsInSec < 1 {
		return ErrNegativeMaxTimeToWaitBetweenBroadcastsInSec
	}
	if config.MinTimeToWaitBetweenBroadcastsInSec < 1 {
		return ErrNegativeMinTimeToWaitBetweenBroadcastsInSec
	}
	if config.MaxTimeToWaitBetweenBroadcastsInSec <= config.MinTimeToWaitBetweenBroadcastsInSec {
		return ErrWrongValues
	}
	if config.DurationInSecToConsiderUnresponsive <= config.MaxTimeToWaitBetweenBroadcastsInSec {
		return ErrWrongValues
	}

	return nil
}

func (n *Node) startSendingHeartbeats(config config.HeartbeatConfig) {
	r := rand.New(rand.NewSource(time.Now().Unix()))

	for {
		diffSeconds := config.MaxTimeToWaitBetweenBroadcastsInSec - config.MinTimeToWaitBetweenBroadcastsInSec
		diffNanos := int64(diffSeconds) * time.Second.Nanoseconds()
		randomNanos := r.Int63n(diffNanos)
		timeToWait := time.Second*time.Duration(config.MinTimeToWaitBetweenBroadcastsInSec) + time.Duration(randomNanos)

		time.Sleep(timeToWait)

		err := n.heartbeatSender.SendHeartbeat()
		log.LogIfError(err)
	}
}

// GetHeartbeats returns the heartbeat status for each public key defined in genesis.json
func (n *Node) GetHeartbeats() []heartbeat.PubKeyHeartbeat {
	if n.heartbeatMonitor == nil {
		return nil
	}
	return n.heartbeatMonitor.GetHeartbeats()
}
