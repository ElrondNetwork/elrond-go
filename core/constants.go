package core

import (
	"math"
	"time"
)

// PeerType represents the type of a peer
type PeerType string

// EligibleList represents the list of peers who participate in consensus inside a shard
const EligibleList PeerType = "eligible"

// WaitingList represents the list of peers who don't participate in consensus but will join the next epoch
const WaitingList PeerType = "waiting"

// LeavingList represents the list of peers who were taken out of eligible and waiting because of rating
const LeavingList PeerType = "leaving"

// InactiveList represents the list of peers who were taken out because they were leaving
const InactiveList PeerType = "inactive"

// JailedList represents the list of peers who have stake but are in jail
const JailedList PeerType = "jailed"

// ObserverList represents the list of peers who don't participate in consensus but will join the next epoch
const ObserverList PeerType = "observer"

// NewList -
const NewList PeerType = "new"

// CombinedPeerType - represents the combination of two peerTypes
const CombinedPeerType = "%s (%s)"

// UnVersionedAppString represents the default app version that indicate that the binary wasn't build by setting
// the appVersion flag
const UnVersionedAppString = "undefined"

// NodeType represents the node's role in the network
type NodeType string

// NodeTypeObserver signals that a node is running as observer node
const NodeTypeObserver NodeType = "observer"

// NodeTypeValidator signals that a node is running as validator node
const NodeTypeValidator NodeType = "validator"

// DisabledShardIDAsObserver defines the uint32 identifier which tells that the node hasn't configured any preferred
// shard to start in as observer
const DisabledShardIDAsObserver = uint32(0xFFFFFFFF) - 7

// pkPrefixSize specifies the max numbers of chars to be displayed from one publc key
const pkPrefixSize = 12

// FileModeUserReadWrite represents the permission for a file which allows the user for reading and writing
const FileModeUserReadWrite = 0600

// MaxTxNonceDeltaAllowed specifies the maximum difference between an account's nonce and a received transaction's nonce
// in order to mark the transaction as valid.
const MaxTxNonceDeltaAllowed = 30000

// MaxBulkTransactionSize specifies the maximum size of one bulk with txs which can be send over the network
//TODO convert this const into a var and read it from config when this code moves to another binary
const MaxBulkTransactionSize = 1 << 18 //256KB bulks

// MaxTxsToRequest specifies the maximum number of txs to request
const MaxTxsToRequest = 1000

// NodesSetupJsonFileName specifies the name of the json file which contains the setup of the nodes
const NodesSetupJsonFileName = "nodesSetup.json"

// ConsensusTopic is the topic used in consensus algorithm
const ConsensusTopic = "consensus"

// HeartbeatTopic is the topic used for heartbeat signaling
const HeartbeatTopic = "heartbeat"

// PathShardPlaceholder represents the placeholder for the shard ID in paths
const PathShardPlaceholder = "[S]"

// PathEpochPlaceholder represents the placeholder for the epoch number in paths
const PathEpochPlaceholder = "[E]"

// PathIdentifierPlaceholder represents the placeholder for the identifier in paths
const PathIdentifierPlaceholder = "[I]"

// MetricCurrentRound is the metric for monitoring the current round of a node
const MetricCurrentRound = "erd_current_round"

// MetricNonce is the metric for monitoring the nonce of a node
const MetricNonce = "erd_nonce"

// MetricNonceForTPS is the metric for monitoring the nonce of a node used in TPS benchmarks
const MetricNonceForTPS = "erd_nonce_for_tps"

// MetricProbableHighestNonce is the metric for monitoring the max speculative nonce received by the node by listening on the network
const MetricProbableHighestNonce = "erd_probable_highest_nonce"

// MetricNumConnectedPeers is the metric for monitoring the number of connected peers
const MetricNumConnectedPeers = "erd_num_connected_peers"

// MetricNumConnectedPeersClassification is the metric for monitoring the number of connected peers split on the connection type
const MetricNumConnectedPeersClassification = "erd_num_connected_peers_classification"

// MetricSynchronizedRound is the metric for monitoring the synchronized round of a node
const MetricSynchronizedRound = "erd_synchronized_round"

// MetricIsSyncing is the metric for monitoring if a node is syncing
const MetricIsSyncing = "erd_is_syncing"

// MetricPublicKeyBlockSign is the metric for monitoring public key of a node used in block signing
const MetricPublicKeyBlockSign = "erd_public_key_block_sign"

// MetricShardId is the metric for monitoring shard id of a node
const MetricShardId = "erd_shard_id"

// MetricNumShardsWithoutMetachain is the metric for monitoring the number of shards (excluding meta)
const MetricNumShardsWithoutMetachain = "erd_num_shards_without_meta"

// MetricTxPoolLoad is the metric for monitoring number of transactions from pool of a node
const MetricTxPoolLoad = "erd_tx_pool_load"

// MetricCountLeader is the metric for monitoring number of rounds when a node was leader
const MetricCountLeader = "erd_count_leader"

// MetricCountConsensus is the metric for monitoring number of rounds when a node was in consensus group
const MetricCountConsensus = "erd_count_consensus"

// MetricCountAcceptedBlocks is the metric for monitoring number of blocks that was accepted proposed by a node
const MetricCountAcceptedBlocks = "erd_count_accepted_blocks"

// MetricNodeType is the metric for monitoring the type of the node
const MetricNodeType = "erd_node_type"

// MetricLiveValidatorNodes is the metric for monitoring live validators on the network
const MetricLiveValidatorNodes = "erd_live_validator_nodes"

// MetricConnectedNodes is the metric for monitoring total connected nodes on the network
const MetricConnectedNodes = "erd_connected_nodes"

// MetricCpuLoadPercent is the metric for monitoring CPU load [%]
const MetricCpuLoadPercent = "erd_cpu_load_percent"

// MetricMemLoadPercent is the metric for monitoring memory load [%]
const MetricMemLoadPercent = "erd_mem_load_percent"

// MetricMemTotal is the metric for monitoring total memory bytes
const MetricMemTotal = "erd_mem_total"

// MetricMemUsedGolang is a metric for monitoring the memory ("total")
const MetricMemUsedGolang = "erd_mem_used_golang"

// MetricMemUsedSystem is a metric for monitoring the memory ("sys mem")
const MetricMemUsedSystem = "erd_mem_used_sys"

// MetricMemHeapInUse is a metric for monitoring the memory ("heap in use")
const MetricMemHeapInUse = "erd_mem_heap_inuse"

// MetricMemStackInUse is a metric for monitoring the memory ("stack in use")
const MetricMemStackInUse = "erd_mem_stack_inuse"

// MetricNetworkRecvPercent is the metric for monitoring network receive load [%]
const MetricNetworkRecvPercent = "erd_network_recv_percent"

// MetricNetworkRecvBps is the metric for monitoring network received bytes per second
const MetricNetworkRecvBps = "erd_network_recv_bps"

// MetricNetworkRecvBpsPeak is the metric for monitoring network received peak bytes per second
const MetricNetworkRecvBpsPeak = "erd_network_recv_bps_peak"

// MetricNetworkRecvBytesInCurrentEpochPerHost is the metric for monitoring network received bytes in current epoch per host
const MetricNetworkRecvBytesInCurrentEpochPerHost = "erd_network_recv_bytes_in_epoch_per_host"

// MetricNetworkSendBytesInCurrentEpochPerHost is the metric for monitoring network send bytes in current epoch per host
const MetricNetworkSendBytesInCurrentEpochPerHost = "erd_network_sent_bytes_in_epoch_per_host"

// MetricNetworkSentPercent is the metric for monitoring network sent load [%]
const MetricNetworkSentPercent = "erd_network_sent_percent"

// MetricNetworkSentBps is the metric for monitoring network sent bytes per second
const MetricNetworkSentBps = "erd_network_sent_bps"

// MetricNetworkSentBpsPeak is the metric for monitoring network sent peak bytes per second
const MetricNetworkSentBpsPeak = "erd_network_sent_bps_peak"

// MetricRoundTime is the metric for round time in seconds
const MetricRoundTime = "erd_round_time"

// MetricEpochNumber is the metric for the number of epoch
const MetricEpochNumber = "erd_epoch_number"

// MetricAppVersion is the metric for the current app version
const MetricAppVersion = "erd_app_version"

// MetricNumTxInBlock is the metric for the number of transactions in the proposed block
const MetricNumTxInBlock = "erd_num_tx_block"

// MetricConsensusState is the metric for consensus state of node proposer,participant or not consensus group
const MetricConsensusState = "erd_consensus_state"

// MetricNumMiniBlocks is the metric for number of miniblocks in a block
const MetricNumMiniBlocks = "erd_num_mini_blocks"

// MetricConsensusRoundState is the metric for consensus round state for a block
const MetricConsensusRoundState = "erd_consensus_round_state"

// MetricCrossCheckBlockHeight is the metric that store cross block height
const MetricCrossCheckBlockHeight = "erd_cross_check_block_height"

// MetricNumProcessedTxs is the metric that stores the number of transactions processed
const MetricNumProcessedTxs = "erd_num_transactions_processed"

// MetricNumProcessedTxsTPSBenchmark is the metric that stores the number of transactions processed for tps benchmark
const MetricNumProcessedTxsTPSBenchmark = "erd_num_transactions_processed_tps_benchmark"

// MetricCurrentBlockHash is the metric that stores the current block hash
const MetricCurrentBlockHash = "erd_current_block_hash"

// MetricCurrentRoundTimestamp is the metric that stores current round timestamp
const MetricCurrentRoundTimestamp = "erd_current_round_timestamp"

// MetricHeaderSize is the metric that stores the current block size
const MetricHeaderSize = "erd_current_block_size"

// MetricMiniBlocksSize is the metric that stores the current block size
const MetricMiniBlocksSize = "erd_mini_blocks_size"

// MetricNumShardHeadersFromPool is the metric that stores number of shard header from pool
const MetricNumShardHeadersFromPool = "erd_num_shard_headers_from_pool"

// MetricNumShardHeadersProcessed is the metric that stores number of shard header processed
const MetricNumShardHeadersProcessed = "erd_num_shard_headers_processed"

// MetricNumTimesInForkChoice is the metric that counts how many time a node was in fork choice
const MetricNumTimesInForkChoice = "erd_fork_choice_count"

// MetricHighestFinalBlock is the metric for the nonce of the highest final block
const MetricHighestFinalBlock = "erd_highest_final_nonce"

// MetricLatestTagSoftwareVersion is the metric that stores the latest tag software version
const MetricLatestTagSoftwareVersion = "erd_latest_tag_software_version"

// MetricCountConsensusAcceptedBlocks is the metric for monitoring number of blocks accepted when the node was in consensus group
const MetricCountConsensusAcceptedBlocks = "erd_count_consensus_accepted_blocks"

// MetricRewardsValue is the metric that stores rewards value
const MetricRewardsValue = "erd_rewards_value"

// MetricNodeDisplayName is the metric that stores the name of the node
const MetricNodeDisplayName = "erd_node_display_name"

// MetricConsensusGroupSize is the metric for consensus group size for the current shard/meta
const MetricConsensusGroupSize = "erd_consensus_group_size"

// MetricShardConsensusGroupSize is the metric for the shard consensus group size
const MetricShardConsensusGroupSize = "erd_shard_consensus_group_size"

// MetricMetaConsensusGroupSize is the metric for the metachain consensus group size
const MetricMetaConsensusGroupSize = "erd_meta_consensus_group_size"

// MetricNumNodesPerShard is the metric which holds the number of nodes in a shard
const MetricNumNodesPerShard = "erd_num_nodes_in_shard"

// MetricNumMetachainNodes is the metric which holds the number of nodes in metachain
const MetricNumMetachainNodes = "erd_num_metachain_nodes"

//MetricNumValidators is the metric for the number of validators
const MetricNumValidators = "erd_num_validators"

// MetricPeerType is the metric which tells the peer's type (in eligible list, in waiting list, or observer)
const MetricPeerType = "erd_peer_type"

// MetricPeerSubType is the metric which tells the peer's subtype (regular observer or full history observer)
const MetricPeerSubType = "erd_peer_subtype"

//MetricLeaderPercentage is the metric for leader rewards percentage
const MetricLeaderPercentage = "erd_leader_percentage"

//MetricDenomination is the metric for exposing the denomination
const MetricDenomination = "erd_denomination"

// MetricRoundAtEpochStart is the metric for storing the first round of the current epoch
const MetricRoundAtEpochStart = "erd_round_at_epoch_start"

// MetricNonceAtEpochStart is the metric for storing the first nonce of the current epoch
const MetricNonceAtEpochStart = "erd_nonce_at_epoch_start"

// MetricRoundsPerEpoch is the metric that tells the number of rounds in an epoch
const MetricRoundsPerEpoch = "erd_rounds_per_epoch"

// MetricRoundsPassedInCurrentEpoch is the metric that tells the number of rounds passed in current epoch
const MetricRoundsPassedInCurrentEpoch = "erd_rounds_passed_in_current_epoch"

// MetricNoncesPassedInCurrentEpoch is the metric that tells the number of nonces passed in current epoch
const MetricNoncesPassedInCurrentEpoch = "erd_nonces_passed_in_current_epoch"

//MetricReceivedProposedBlock is the metric that specify the moment in the round when the received block has reached the
//current node. The value is provided in percent (0 meaning it has been received just after the round started and
//100 meaning that the block has been received in the last moment of the round)
const MetricReceivedProposedBlock = "erd_consensus_received_proposed_block"

//MetricCreatedProposedBlock is the metric that specify the percent of the block subround used for header and body
//creation (0 meaning that the block was created in no-time and 100 meaning that the block creation used all the
//subround spare duration)
const MetricCreatedProposedBlock = "erd_consensus_created_proposed_block"

//MetricProcessedProposedBlock is the metric that specify the percent of the block subround used for header and body
//processing (0 meaning that the block was processed in no-time and 100 meaning that the block processing used all the
//subround spare duration)
const MetricProcessedProposedBlock = "erd_consensus_processed_proposed_block"

// MetricMinGasPrice is the metric that specifies min gas price
const MetricMinGasPrice = "erd_min_gas_price"

// MetricMinGasLimit is the metric that specifies the minimum gas limit
const MetricMinGasLimit = "erd_min_gas_limit"

// MetricRewardsTopUpGradientPoint is the metric that specifies the rewards top up gradient point
const MetricRewardsTopUpGradientPoint = "erd_rewards_top_up_gradient_point"

// MetricGasPriceModifier is the metric that specifies the gas price modifier
const MetricGasPriceModifier = "erd_gas_price_modifier"

// MetricTopUpFactor is the metric that specifies the top up factor
const MetricTopUpFactor = "erd_top_up_factor"

// MetricMinTransactionVersion is the metric that specifies the minimum transaction version
const MetricMinTransactionVersion = "erd_min_transaction_version"

// MetricGasPerDataByte is the metric that specifies the required gas for a data byte
const MetricGasPerDataByte = "erd_gas_per_data_byte"

// MetricChainId is the metric that specifies current chain id
const MetricChainId = "erd_chain_id"

// MetricStartTime is the metric that specifies the genesis start time
const MetricStartTime = "erd_start_time"

// MetricRoundDuration is the metric that specifies the round duration in milliseconds
const MetricRoundDuration = "erd_round_duration"

// MetricPeakTPS holds the peak transactions per second
const MetricPeakTPS = "erd_peak_tps"

// MetricLastBlockTxCount holds the number of transactions in the last block
const MetricLastBlockTxCount = "erd_last_block_tx_count"

// MetricAverageBlockTxCount holds the average count of transactions in a block
const MetricAverageBlockTxCount = "erd_average_block_tx_count"

// MetricTotalSupply holds the total supply value for the last epoch
const MetricTotalSupply = "erd_total_supply"

// MetricTotalBaseStakedValue holds the total base staked value
const MetricTotalBaseStakedValue = "erd_total_base_staked_value"

// MetricTopUpValue holds the total top up value
const MetricTopUpValue = "erd_total_top_up_value"

// MetricInflation holds the inflation value for the last epoch
const MetricInflation = "erd_inflation"

// MetricDevRewardsInEpoch holds the developers' rewards value for the last epoch
const MetricDevRewardsInEpoch = "erd_dev_rewards"

// MetricTotalFees holds the total fees value for the last epoch
const MetricTotalFees = "erd_total_fees"

// MetricEpochForEconomicsData holds the epoch for which economics data are computed
const MetricEpochForEconomicsData = "erd_epoch_for_economics_data"

// LastNonceKeyMetricsStorage holds the key used for storing the last nonce for stored metrics
const LastNonceKeyMetricsStorage = "lastNonce"

// MetachainShardId will be used to identify a shard ID as metachain
const MetachainShardId = uint32(0xFFFFFFFF)

// AllShardId will be used to identify that a message is for all shards
const AllShardId = uint32(0xFFFFFFF0)

// MegabyteSize represents the size in bytes of a megabyte
const MegabyteSize = 1024 * 1024

// BaseOperationCost represents the field name for base operation costs
const BaseOperationCost = "BaseOperationCost"

// BuiltInCost represents the field name for built in operation costs
const BuiltInCost = "BuiltInCost"

// MetaChainSystemSCsCost represents the field name for metachain system smart contract operation costs
const MetaChainSystemSCsCost = "MetaChainSystemSCsCost"

// ElrondAPICost represents the field name of the Elrond SC API (EEI) gas costs
const ElrondAPICost = "ElrondAPICost"

// AsyncCallStepField is the field name for the gas cost for any of the two steps required to execute an async call
const AsyncCallStepField = "AsyncCallStep"

// AsyncCallbackGasLockField is the field name for the gas amount to be locked
// before executing the destination async call, to be put aside for the async callback
const AsyncCallbackGasLockField = "AsyncCallbackGasLock"

const (
	// MetricScDeployEnableEpoch represents the epoch when the deployment of smart contracts will be enabled
	MetricScDeployEnableEpoch = "erd_smart_contract_deploy_enable_epoch"

	//MetricBuiltInFunctionsEnableEpoch represents the epoch when the built in functions will be enabled
	MetricBuiltInFunctionsEnableEpoch = "erd_built_in_functions_enable_epoch"

	//MetricRelayedTransactionsEnableEpoch represents the epoch when the relayed transactions will be enabled
	MetricRelayedTransactionsEnableEpoch = "erd_relayed_transactions_enable_epoch"

	//MetricPenalizedTooMuchGasEnableEpoch represents the epoch when the penalization for using too much gas will be enabled
	MetricPenalizedTooMuchGasEnableEpoch = "erd_penalized_too_much_gas_enable_epoch"

	//MetricSwitchJailWaitingEnableEpoch represents the epoch when the system smart contract processing at end of epoch is enabled
	MetricSwitchJailWaitingEnableEpoch = "erd_switch_jail_waiting_enable_epoch"

	//MetricSwitchHysteresisForMinNodesEnableEpoch represents the epoch when the system smart contract changes its config to consider
	//also (minimum) hysteresis nodes for the minimum number of nodes
	MetricSwitchHysteresisForMinNodesEnableEpoch = "erd_switch_hysteresis_for_min_nodes_enable_epoch"

	//MetricBelowSignedThresholdEnableEpoch represents the epoch when the change for computing rating for validators below signed rating is enabled
	MetricBelowSignedThresholdEnableEpoch = "erd_below_signed_threshold_enable_epoch"

	//MetricTransactionSignedWithTxHashEnableEpoch represents the epoch when the node will also accept transactions that are
	//signed with the hash of transaction
	MetricTransactionSignedWithTxHashEnableEpoch = "erd_transaction_signed_with_txhash_enable_epoch"

	//MetricMetaProtectionEnableEpoch represents the epoch when the transactions to the metachain are checked to have enough gas
	MetricMetaProtectionEnableEpoch = "erd_meta_protection_enable_epoch"

	//MetricAheadOfTimeGasUsageEnableEpoch represents the epoch when the cost of smart contract prepare changes from compiler per byte to ahead of time prepare per byte
	MetricAheadOfTimeGasUsageEnableEpoch = "erd_ahead_of_time_gas_usage_enable_epoch"

	//MetricGasPriceModifierEnableEpoch represents the epoch when the gas price modifier in fee computation is enabled
	MetricGasPriceModifierEnableEpoch = "erd_gas_price_modifier_enable_epoch"

	//MetricRepairCallbackEnableEpoch represents the epoch when the callback repair is activated for scrs
	MetricRepairCallbackEnableEpoch = "erd_repair_callback_enable_epoch"

	//MetricBlockGasAndFreeRecheckEnableEpoch represents the epoch when gas and fees used in each created or processed block are re-checked
	MetricBlockGasAndFreeRecheckEnableEpoch = "erd_block_gas_and_fee_recheck_enable_epoch"

	//MetricStakingV2EnableEpoch represents the epoch when staking v2 is enabled
	MetricStakingV2EnableEpoch = "erd_staking_v2_enable_epoch"

	//MetricStakeEnableEpoch represents the epoch when staking is enabled
	MetricStakeEnableEpoch = "erd_stake_enable_epoch"

	//MetricDoubleKeyProtectionEnableEpoch represents the epoch when double key protection is enabled
	MetricDoubleKeyProtectionEnableEpoch = "erd_double_key_protection_enable_epoch"

	//MetricEsdtEnableEpoch represents the epoch when ESDT is enabled
	MetricEsdtEnableEpoch = "erd_esdt_enable_epoch"

	//MetricGovernanceEnableEpoch  represents the epoch when governance is enabled
	MetricGovernanceEnableEpoch = "erd_governance_enable_epoch"

	//MetricDelegationManagerEnableEpoch represents the epoch when the delegation manager is enabled epoch should not be 0
	MetricDelegationManagerEnableEpoch = "erd_delegation_manager_enable_epoch"

	//MetricDelegationSmartContractEnableEpoch represents the epoch when delegation smart contract is enabled epoch should not be 0
	MetricDelegationSmartContractEnableEpoch = "erd_delegation_smart_contract_enable_epoch"

	// MetricIncrementSCRNonceInMultiTransferEnableEpoch represents the epoch when the fix for multi transfer SCR is enabled
	MetricIncrementSCRNonceInMultiTransferEnableEpoch = "erd_increment_scr_nonce_in_multi_transfer_enable_epoch"
)

const (
	// StorerOrder defines the order of storers to be notified of a start of epoch event
	StorerOrder = iota
	// NodesCoordinatorOrder defines the order in which NodesCoordinator is notified of a start of epoch event
	NodesCoordinatorOrder
	// ConsensusOrder defines the order in which Consensus is notified of a start of epoch event
	ConsensusOrder
	// NetworkShardingOrder defines the order in which the network sharding subsystem is notified of a start of epoch event
	NetworkShardingOrder
	// IndexerOrder defines the order in which Indexer is notified of a start of epoch event
	IndexerOrder
	// NetStatisticsOrder defines the order in which netStatistic component is notified of a start of epoch event
	NetStatisticsOrder
	// OldDatabaseCleanOrder defines the order in which oldDatabaseCleaner component is notified of a start of epoch event
	OldDatabaseCleanOrder
)

// NodeState specifies what type of state a node could have
type NodeState int

const (
	// NsSynchronized defines ID of a state of synchronized
	NsSynchronized NodeState = iota
	// NsNotSynchronized defines ID of a state of not synchronized
	NsNotSynchronized
	// NsNotCalculated defines ID of a state which is not calculated
	NsNotCalculated
)

// MetricP2PPeerInfo is the metric for the node's p2p info
const MetricP2PPeerInfo = "erd_p2p_peer_info"

// MetricP2PIntraShardValidators is the metric that outputs the intra-shard connected validators
const MetricP2PIntraShardValidators = "erd_p2p_intra_shard_validators"

// MetricP2PCrossShardValidators is the metric that outputs the cross-shard connected validators
const MetricP2PCrossShardValidators = "erd_p2p_cross_shard_validators"

// MetricP2PIntraShardObservers is the metric that outputs the intra-shard connected observers
const MetricP2PIntraShardObservers = "erd_p2p_intra_shard_observers"

// MetricP2PCrossShardObservers is the metric that outputs the cross-shard connected observers
const MetricP2PCrossShardObservers = "erd_p2p_cross_shard_observers"

// MetricP2PFullHistoryObservers is the metric that outputs the full-history connected observers
const MetricP2PFullHistoryObservers = "erd_p2p_full_history_observers"

// MetricP2PUnknownPeers is the metric that outputs the unknown-shard connected peers
const MetricP2PUnknownPeers = "erd_p2p_unknown_shard_peers"

// MetricP2PNumConnectedPeersClassification is the metric for monitoring the number of connected peers split on the connection type
const MetricP2PNumConnectedPeersClassification = "erd_p2p_num_connected_peers_classification"

// HighestRoundFromBootStorage is the key for the highest round that is saved in storage
const HighestRoundFromBootStorage = "highestRoundFromBootStorage"

// TriggerRegistryKeyPrefix is the key prefix to save epoch start registry to storage
const TriggerRegistryKeyPrefix = "epochStartTrigger_"

// TriggerRegistryInitialKeyPrefix is the key prefix to save initial data to storage
const TriggerRegistryInitialKeyPrefix = "initial_value_epoch_"

// NodesCoordinatorRegistryKeyPrefix is the key prefix to save epoch start registry to storage
const NodesCoordinatorRegistryKeyPrefix = "indexHashed_"

// BuiltInFunctionClaimDeveloperRewards is the key for the claim developer rewards built-in function
const BuiltInFunctionClaimDeveloperRewards = "ClaimDeveloperRewards"

// BuiltInFunctionChangeOwnerAddress is the key for the change owner built in function built-in function
const BuiltInFunctionChangeOwnerAddress = "ChangeOwnerAddress"

// BuiltInFunctionSetUserName is the key for the set user name built-in function
const BuiltInFunctionSetUserName = "SetUserName"

// BuiltInFunctionSaveKeyValue is the key for the save key value built-in function
const BuiltInFunctionSaveKeyValue = "SaveKeyValue"

// BuiltInFunctionESDTTransfer is the key for the elrond standard digital token transfer built-in function
const BuiltInFunctionESDTTransfer = "ESDTTransfer"

// BuiltInFunctionESDTBurn is the key for the elrond standard digital token burn built-in function
const BuiltInFunctionESDTBurn = "ESDTBurn"

// BuiltInFunctionESDTFreeze is the key for the elrond standard digital token freeze built-in function
const BuiltInFunctionESDTFreeze = "ESDTFreeze"

// BuiltInFunctionESDTUnFreeze is the key for the elrond standard digital token unfreeze built-in function
const BuiltInFunctionESDTUnFreeze = "ESDTUnFreeze"

// BuiltInFunctionESDTWipe is the key for the elrond standard digital token wipe built-in function
const BuiltInFunctionESDTWipe = "ESDTWipe"

// BuiltInFunctionESDTPause is the key for the elrond standard digital token pause built-in function
const BuiltInFunctionESDTPause = "ESDTPause"

// BuiltInFunctionESDTUnPause is the key for the elrond standard digital token unpause built-in function
const BuiltInFunctionESDTUnPause = "ESDTUnPause"

// BuiltInFunctionSetESDTRole is the key for the elrond standard digital token set built-in function
const BuiltInFunctionSetESDTRole = "ESDTSetRole"

// BuiltInFunctionUnSetESDTRole is the key for the elrond standard digital token unset built-in function
const BuiltInFunctionUnSetESDTRole = "ESDTUnSetRole"

// BuiltInFunctionESDTLocalMint is the key for the elrond standard digital token local mint built-in function
const BuiltInFunctionESDTLocalMint = "ESDTLocalMint"

// BuiltInFunctionESDTLocalBurn is the key for the elrond standard digital token local burn built-in function
const BuiltInFunctionESDTLocalBurn = "ESDTLocalBurn"

// BuiltInFunctionESDTNFTTransfer is the key for the elrond standard digital token NFT transfer built-in function
const BuiltInFunctionESDTNFTTransfer = "ESDTNFTTransfer"

// BuiltInFunctionESDTNFTCreate is the key for the elrond standard digital token NFT create built-in function
const BuiltInFunctionESDTNFTCreate = "ESDTNFTCreate"

// BuiltInFunctionESDTNFTAddQuantity is the key for the elrond standard digital token NFT add quantity built-in function
const BuiltInFunctionESDTNFTAddQuantity = "ESDTNFTAddQuantity"

// BuiltInFunctionESDTNFTCreateRoleTransfer is the key for the elrond standard digital token create role transfer function
const BuiltInFunctionESDTNFTCreateRoleTransfer = "ESDTNFTCreateRoleTransfer"

// BuiltInFunctionESDTNFTBurn is the key for the elrond standard digital token NFT burn built-in function
const BuiltInFunctionESDTNFTBurn = "ESDTNFTBurn"

// ESDTRoleLocalMint is the constant string for the local role of mint for ESDT tokens
const ESDTRoleLocalMint = "ESDTRoleLocalMint"

// ESDTRoleLocalBurn is the constant string for the local role of burn for ESDT tokens
const ESDTRoleLocalBurn = "ESDTRoleLocalBurn"

// ESDTRoleNFTCreate is the constant string for the local role of create for ESDT NFT tokens
const ESDTRoleNFTCreate = "ESDTRoleNFTCreate"

// ESDTRoleNFTAddQuantity is the constant string for the local role of adding quantity for existing ESDT NFT tokens
const ESDTRoleNFTAddQuantity = "ESDTRoleNFTAddQuantity"

// ESDTRoleNFTBurn is the constant string for the local role of burn for ESDT NFT tokens
const ESDTRoleNFTBurn = "ESDTRoleNFTBurn"

// ESDTType defines the possible types in case of ESDT tokens
type ESDTType uint32

const (
	// Fungible defines the token type for ESDT fungible tokens
	Fungible ESDTType = iota
	// NonFungible defines the token type for ESDT non fungible tokens
	NonFungible
)

// FungibleESDT defines the string for the token type of fungible ESDT
const FungibleESDT = "FungibleESDT"

// NonFungibleESDT defines the string for the token type of non fungible ESDT
const NonFungibleESDT = "NonFungibleESDT"

// SemiFungibleESDT defines the string for the token type of semi fungible ESDT
const SemiFungibleESDT = "SemiFungibleESDT"

// MaxRoyalty defines 100% as uint32
const MaxRoyalty = uint32(10000)

// RelayedTransaction is the key for the elrond meta/gassless/relayed transaction standard
const RelayedTransaction = "relayedTx"

// RelayedTransactionV2 is the key for the optimized elrond meta/gassless/relayed transaction standard
const RelayedTransactionV2 = "relayedTxV2"

// SCDeployInitFunctionName is the key for the function which is called at smart contract deploy time
const SCDeployInitFunctionName = "_init"

// ShuffledOut signals that a restart is pending because the node was shuffled out
const ShuffledOut = "shuffledOut"

// WrongConfiguration signals that the node has a malformed configuration and cannot continue processing
const WrongConfiguration = "wrongConfiguration"

// ImportComplete signals that a node restart will be done because the import did complete
const ImportComplete = "importComplete"

// MaxRetriesToCreateDB represents the maximum number of times to try to create DB if it failed
const MaxRetriesToCreateDB = 10

// SleepTimeBetweenCreateDBRetries represents the number of seconds to sleep between DB creates
const SleepTimeBetweenCreateDBRetries = 5 * time.Second

// ElrondProtectedKeyPrefix is the key prefix which is protected from writing in the trie - only for special builtin functions
const ElrondProtectedKeyPrefix = "ELROND"

// DefaultStatsPath is the default path where the node stats are logged
const DefaultStatsPath = "stats"

// DefaultDBPath is the default path for nodes databases
const DefaultDBPath = "db"

// DefaultEpochString is the default folder root name for node per epoch databases
const DefaultEpochString = "Epoch"

// DefaultStaticDbString is the default name for the static databases (not changing with epoch)
const DefaultStaticDbString = "Static"

// DefaultShardString is the default folder root name for per shard databases
const DefaultShardString = "Shard"

// MetachainShardName is the string identifier of the metachain shard
const MetachainShardName = "metachain"

// TemporaryPath is the default temporary path directory
const TemporaryPath = "temp"

// SecondsToWaitForP2PBootstrap is the wait time for the P2P to bootstrap
const SecondsToWaitForP2PBootstrap = 20

// DelegationSystemSCKey is the key under which there is data in case of system delegation smart contracts
const DelegationSystemSCKey = "delegation"

// ESDTKeyIdentifier is the key prefix for esdt tokens
const ESDTKeyIdentifier = "esdt"

// ESDTRoleIdentifier is the key prefix for esdt role identifier
const ESDTRoleIdentifier = "role"

// ESDTNFTLatestNonceIdentifier is the key prefix for esdt latest nonce identifier
const ESDTNFTLatestNonceIdentifier = "nonce"

// MaxSoftwareVersionLengthInBytes represents the maximum length for the software version to be saved in block header
const MaxSoftwareVersionLengthInBytes = 10

// ExtraDelayForBroadcastBlockInfo represents the number of seconds to wait since a block has been broadcast and the
// moment when its components, like mini blocks and transactions, would be broadcast too
const ExtraDelayForBroadcastBlockInfo = 1 * time.Second

// ExtraDelayBetweenBroadcastMbsAndTxs represents the number of seconds to wait since miniblocks have been broadcast
// and the moment when theirs transactions would be broadcast too
const ExtraDelayBetweenBroadcastMbsAndTxs = 1 * time.Second

// ExtraDelayForRequestBlockInfo represents the number of seconds to wait since a block has been received and the
// moment when its components, like mini blocks and transactions, would be requested too if they are still missing
const ExtraDelayForRequestBlockInfo = ExtraDelayForBroadcastBlockInfo + ExtraDelayBetweenBroadcastMbsAndTxs + time.Second

// CommitMaxTime represents max time accepted for a commit action, after which a warn message is displayed
const CommitMaxTime = 3 * time.Second

// PutInStorerMaxTime represents max time accepted for a put action, after which a warn message is displayed
const PutInStorerMaxTime = time.Second

// DefaultUnstakedEpoch represents the default epoch that is set for a validator that has not unstaked yet
const DefaultUnstakedEpoch = math.MaxUint32

// InvalidMessageBlacklistDuration represents the time to keep a peer in the black list if it sends a message that
// does not follow the protocol: example not useing the same marshaler as the other peers
const InvalidMessageBlacklistDuration = time.Second * 3600

// MaxNumShards represents the maximum number of shards possible in the system
const MaxNumShards = 256

// PublicKeyBlacklistDuration represents the time to keep a public key in the black list if it will degrade its
// rating to a minimum threshold due to improper messages
const PublicKeyBlacklistDuration = time.Second * 7200

// WrongP2PMessageBlacklistDuration represents the time to keep a peer id in the blacklist if it sends a message that
// do not follow this protocol
const WrongP2PMessageBlacklistDuration = time.Second * 7200

// MaxWaitingTimeToReceiveRequestedItem represents the maximum waiting time in seconds needed to receive the requested items
const MaxWaitingTimeToReceiveRequestedItem = 5 * time.Second

// DefaultLogProfileIdentifier represents the default log profile used when the logviewer/termui applications do not
// need to change the current logging profile
const DefaultLogProfileIdentifier = "[default log profile]"

// NotSetDestinationShardID represents the shardIdString when the destinationShardId is not set in the prefs
const NotSetDestinationShardID = "disabled"

// AdditionalScrForEachScCallOrSpecialTx specifies the additional number of smart contract results which should be
// considered by a node, when it includes sc calls or special txs in a miniblock.
// Ex.: normal txs -> aprox. 27000, sc calls or special txs -> aprox. 6250 = 27000 / (AdditionalScrForEachScCallOrSpecialTx + 1),
// considering that constant below is set to 3
const AdditionalScrForEachScCallOrSpecialTx = 3

// MaxRoundsWithoutCommittedStartInEpochBlock defines the maximum rounds to wait for start in epoch block to be committed,
// before a special action to be applied
const MaxRoundsWithoutCommittedStartInEpochBlock = 50

// MinMetaTxExtraGasCost is the constant defined for minimum gas value to be sent in meta transaction
const MinMetaTxExtraGasCost = uint64(1_000_000)

// MaxLeafSize represents maximum amount of data which can be saved under one leaf
const MaxLeafSize = uint64(1 << 26) //64MB

// MaxBufferSizeToSendTrieNodes represents max buffer size to send in bytes used when resolving trie nodes
// Every trie node that has a greater size than this constant is considered a large trie node and should be split in
// smaller chunks
const MaxBufferSizeToSendTrieNodes = 1 << 18 //256KB

// MaxUserNameLength represents the maximum number of bytes a UserName can have
const MaxUserNameLength = 32

// MinLenArgumentsESDTTransfer defines the min length of arguments for the ESDT transfer
const MinLenArgumentsESDTTransfer = 2

// MinLenArgumentsESDTNFTTransfer defines the minimum length for esdt nft transfer
const MinLenArgumentsESDTNFTTransfer = 4

// MaxLenForESDTIssueMint defines the maximum length in bytes for the issued/minted balance
const MaxLenForESDTIssueMint = 100

// DefaultResolversIdentifier represents the identifier that is used in conjunction with regular resolvers
//(that makes the node run properly)
const DefaultResolversIdentifier = "default resolver"

// DefaultInterceptorsIdentifier represents the identifier that is used in conjunction with regular interceptors
//(that makes the node run properly)
const DefaultInterceptorsIdentifier = "default interceptor"

// HardforkInterceptorsIdentifier represents the identifier that is used in the hardfork process
const HardforkInterceptorsIdentifier = "hardfork interceptor"

// HardforkResolversIdentifier represents the resolver that is used in the hardfork process
const HardforkResolversIdentifier = "hardfork resolver"

// EpochStartInterceptorsIdentifier represents the identifier that is used in the start-in-epoch process
const EpochStartInterceptorsIdentifier = "epoch start interceptor"

// GetNodeFromDBErrorString represents the string which is returned when a getting node from DB returns an error
const GetNodeFromDBErrorString = "getNodeFromDB error"

// SCDeployIdentifier is the identifier for a smart contract deploy
const SCDeployIdentifier = "SCDeploy"

// SCUpgradeIdentifier is the identifier for a smart contract upgrade
const SCUpgradeIdentifier = "SCUpgrade"
