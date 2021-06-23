package update

import "errors"

// ErrUnknownType signals that type is unknown
var ErrUnknownType = errors.New("unknown type")

// ErrNilStateSyncer signals that state syncer is nil
var ErrNilStateSyncer = errors.New("nil state syncer")

// ErrInvalidFolderName signals that folder name is nil
var ErrInvalidFolderName = errors.New("invalid folder name")

// ErrNilStorage signals that storage is nil
var ErrNilStorage = errors.New("nil storage")

// ErrNotSynced signals that syncing has not been finished yet
var ErrNotSynced = errors.New("not synced")

// ErrNilTrieSyncers signals that trie syncers container is nil
var ErrNilTrieSyncers = errors.New("nil trie syncers")

// ErrNotEpochStartBlock signals that block is not of type epoch start
var ErrNotEpochStartBlock = errors.New("not epoch start block")

// ErrNilContainerElement signals when trying to add a nil element in the container
var ErrNilContainerElement = errors.New("element cannot be nil")

// ErrInvalidContainerKey signals that an element does not exist in the container's map
var ErrInvalidContainerKey = errors.New("element does not exist in container")

// ErrContainerKeyAlreadyExists signals that an element was already set in the container's map
var ErrContainerKeyAlreadyExists = errors.New("provided key already exists in container")

// ErrLenMismatch signals that 2 or more slices have different lengths
var ErrLenMismatch = errors.New("lengths mismatch")

// ErrWrongTypeInContainer signals that a wrong type of object was found in container
var ErrWrongTypeInContainer = errors.New("wrong type of object inside container")

// ErrNilWhiteListHandler signals that white list handler is nil
var ErrNilWhiteListHandler = errors.New("nil white list handler")

// ErrWrongTypeAssertion signals wrong type assertion
var ErrWrongTypeAssertion = errors.New("wrong type assertion")

// ErrNilHasher signals that an operation has been attempted to or with a nil hasher implementation
var ErrNilHasher = errors.New("nil Hasher")

// ErrNilShardCoordinator signals that an operation has been attempted to or with a nil shard coordinator
var ErrNilShardCoordinator = errors.New("nil shard coordinator")

// ErrNilMarshalizer signals that an operation has been attempted to or with a nil Marshalizer implementation
var ErrNilMarshalizer = errors.New("nil Marshalizer")

// ErrNilHeaderValidator signals that nil header validator has been provided
var ErrNilHeaderValidator = errors.New("nil header validator")

// ErrNilUint64Converter signals that uint64converter is nil
var ErrNilUint64Converter = errors.New("unit64converter is nil")

// ErrNilDataPoolHolder signals that the data pool holder is nil
var ErrNilDataPoolHolder = errors.New("nil data pool holder")

// ErrNilRequestHandler signals that a nil request handler interface was provided
var ErrNilRequestHandler = errors.New("nil request handler")

// ErrNilMessenger signals that a nil Messenger object was provided
var ErrNilMessenger = errors.New("nil Messenger")

// ErrNilInterceptorsContainer signals that a nil interceptors container has been provided
var ErrNilInterceptorsContainer = errors.New("nil interceptors container")

// ErrNilTrieDataGetter signals that a nil trie data getter has been provided
var ErrNilTrieDataGetter = errors.New("nil trie data getter provided")

// ErrNilResolverContainer signals that a nil resolver container was provided
var ErrNilResolverContainer = errors.New("nil resolver container")

// ErrNilCacher signals that nil cacher was provided
var ErrNilCacher = errors.New("nil cacher")

// ErrNilEpochHandler signals that nil epoch handler was provided
var ErrNilEpochHandler = errors.New("nil epoch handler")

// ErrNilHeaderSyncHandler signals that nil header sync handler was provided
var ErrNilHeaderSyncHandler = errors.New("nil header sync handler")

// ErrNilMiniBlocksSyncHandler signals that nil miniblocks sync handler was provided
var ErrNilMiniBlocksSyncHandler = errors.New("nil miniblocks sync handler")

// ErrNilTransactionsSyncHandler signals that nil transactions sync handler was provided
var ErrNilTransactionsSyncHandler = errors.New("nil transaction sync handler")

// ErrWrongUnFinishedMetaHdrsMap signals that wrong unFinished meta headers map was provided
var ErrWrongUnFinishedMetaHdrsMap = errors.New("wrong unFinished meta headers map")

// ErrNilAccounts signals that nil accounts was provided
var ErrNilAccounts = errors.New("nil accounts")

// ErrNilMultiSigner signals that nil multi signer was provided
var ErrNilMultiSigner = errors.New("nil multi signer")

// ErrNilNodesCoordinator signals that nil nodes coordinator was provided
var ErrNilNodesCoordinator = errors.New("nil nodes coordinator")

// ErrNilSingleSigner signals that nil single signer was provided
var ErrNilSingleSigner = errors.New("nil single signer")

// ErrNilPubKeyConverter signals that a nil public key converter was provided
var ErrNilPubKeyConverter = errors.New("nil public key converter")

// ErrNilBlockKeyGen signals that nil block key gen was provided
var ErrNilBlockKeyGen = errors.New("nil block key gen")

// ErrNilKeyGenerator signals that nil key generator was provided
var ErrNilKeyGenerator = errors.New("nil key generator")

// ErrNilBlockSigner signals the nil block signer was provided
var ErrNilBlockSigner = errors.New("nil block signer")

// ErrNilHeaderSigVerifier signals that nil header sig verifier was provided
var ErrNilHeaderSigVerifier = errors.New("nil header sig verifier")

// ErrNilHeaderIntegrityVerifier signals that nil header integrity verifier was provided
var ErrNilHeaderIntegrityVerifier = errors.New("nil header integrity verifier")

// ErrNilValidityAttester signals that nil validity was provided
var ErrNilValidityAttester = errors.New("nil validity attester")

// ErrNilStorageManager signals that nil storage manager has been provided
var ErrNilStorageManager = errors.New("nil trie storage manager")

// ErrNilAccountsDBSyncContainer signals that nil accounts sync container was provided
var ErrNilAccountsDBSyncContainer = errors.New("nil accounts db sync container")

// ErrTimeIsOut signals that time is out
var ErrTimeIsOut = errors.New("time is out")

// ErrTriggerNotEnabled signals that the trigger is not enabled
var ErrTriggerNotEnabled = errors.New("trigger is not enabled")

// ErrNilCloser signals that a nil closer instance was provided
var ErrNilCloser = errors.New("nil closer instance")

// ErrInvalidValue signals that the value provided is invalid
var ErrInvalidValue = errors.New("invalid value")

// ErrTriggerPubKeyMismatch signals that there is a mismatch between the public key received and the one read from the config
var ErrTriggerPubKeyMismatch = errors.New("trigger public key mismatch")

// ErrNilAntiFloodHandler signals that nil anti flood handler has been provided
var ErrNilAntiFloodHandler = errors.New("nil anti flood handler")

// ErrIncorrectHardforkMessage signals that the hardfork message is incorrectly formatted
var ErrIncorrectHardforkMessage = errors.New("incorrect hardfork message")

// ErrNilRwdTxProcessor signals that nil reward transaction processor has been provided
var ErrNilRwdTxProcessor = errors.New("nil reward transaction processor")

// ErrNilSCRProcessor signals that nil smart contract result processor has been provided
var ErrNilSCRProcessor = errors.New("nil smart contract result processor")

// ErrNilTxProcessor signals that nil transaction processor has been provided
var ErrNilTxProcessor = errors.New("nil transaction processor")

// ErrNilImportHandler signals that nil import handler has been provided
var ErrNilImportHandler = errors.New("nil import handler")

// ErrNilTxCoordinator signals that nil tx coordinator has been provided
var ErrNilTxCoordinator = errors.New("nil tx coordinator")

// ErrNilPendingTxProcessor signals that nil pending tx processor has been provided
var ErrNilPendingTxProcessor = errors.New("nil pending tx processor")

// ErrNilHardForkBlockProcessor signals that nil hard fork block processor has been provided
var ErrNilHardForkBlockProcessor = errors.New("nil hard fork block processor")

// ErrNilTrieStorageManagers signals that nil trie storage managers has been provided
var ErrNilTrieStorageManagers = errors.New("nil trie storage managers")

// ErrEmptyChainID signals that empty chain ID was provided
var ErrEmptyChainID = errors.New("empty chain ID")

// ErrNilArgumentParser signals that nil argument parser was provided
var ErrNilArgumentParser = errors.New("nil argument parser")

// ErrNilExportFactoryHandler signals that nil export factory handler has been provided
var ErrNilExportFactoryHandler = errors.New("nil export factory handler")

// ErrNilChanStopNodeProcess signals that nil channel to stop node was provided
var ErrNilChanStopNodeProcess = errors.New("nil channel to stop node")

// ErrNilEpochConfirmedNotifier signals that nil epoch confirmed notifier was provided
var ErrNilEpochConfirmedNotifier = errors.New("nil epoch confirmed notifier")

// ErrTriggerAlreadyInAction signals that the trigger is already in action, can not re-enter
var ErrTriggerAlreadyInAction = errors.New("trigger already in action")

// ErrInvalidTimeToWaitAfterHardfork signals that an invalid time to wait after hardfork was provided
var ErrInvalidTimeToWaitAfterHardfork = errors.New("invalid time to wait after hard fork")

// ErrInvalidEpoch signals that an invalid epoch has been provided
var ErrInvalidEpoch = errors.New("invalid epoch")

// ErrNilImportStartHandler signals that a nil import start handler has been provided
var ErrNilImportStartHandler = errors.New("nil import start handler")

// ErrEmptyVersionString signals that the provided version string is empty
var ErrEmptyVersionString = errors.New("empty version string")

// ErrNilTimeCache signals that a nil time cache was provided
var ErrNilTimeCache = errors.New("nil time cache")

// ErrNilHardforkStorer signals that a nil hardfork storer has been provided
var ErrNilHardforkStorer = errors.New("nil hardfork storer")

// ErrExpectedOneStartOfEpochMetaBlock signals that exactly one start of epoch metaBlock should have been used
var ErrExpectedOneStartOfEpochMetaBlock = errors.New("expected one start of epoch metaBlock")

// ErrImportingData signals that an import error occurred
var ErrImportingData = errors.New("error importing data")

// ErrKeyTypeMismatch signals that key type was mismatch during import
var ErrKeyTypeMismatch = errors.New("key type mismatch while importing")

// ErrNilRoundHandler signals that nil round handler has been provided
var ErrNilRoundHandler = errors.New("nil round handler")

// ErrEmptyExportFolderPath signals that the provided export folder's length is empty
var ErrEmptyExportFolderPath = errors.New("empty export folder path")

// ErrNilGenesisNodesSetupHandler signals that a nil genesis nodes setup handler has been provided
var ErrNilGenesisNodesSetupHandler = errors.New("nil genesis nodes setup handler")

// ErrNilEpochNotifier signals that the provided EpochNotifier is nil
var ErrNilEpochNotifier = errors.New("nil EpochNotifier")

// ErrWrongImportedMiniBlocksMap signals that wrong imported miniBlocks map was provided
var ErrWrongImportedMiniBlocksMap = errors.New("wrong imported miniBlocks map was provided")

// ErrWrongImportedTransactionsMap signals that wrong imported transactions map was provided
var ErrWrongImportedTransactionsMap = errors.New("wrong imported transactions map was provided")

// ErrMiniBlockNotFoundInImportedMap signals that the given miniBlock was not found in imported map
var ErrMiniBlockNotFoundInImportedMap = errors.New("miniBlock was not found in imported map")

// ErrTransactionNotFoundInImportedMap signals that the given transaction was not found in imported map
var ErrTransactionNotFoundInImportedMap = errors.New("transaction was not found in imported map")

// ErrNilEpochStartMetaBlock signals that a nil epoch start metaBlock was provided
var ErrNilEpochStartMetaBlock = errors.New("nil epoch start metaBlock was provided")

// ErrNilUnFinishedMetaBlocksMap signals that a nil unFinished metaBlocks map was provided
var ErrNilUnFinishedMetaBlocksMap = errors.New("nil unFinished metaBlocks map was provided")

// ErrPostProcessTransactionNotFound signals that the given transaction was not found in post process map
var ErrPostProcessTransactionNotFound = errors.New("transaction was not found in post process map")

// ErrNilBlockBody signals that a nil block body has been provided
var ErrNilBlockBody = errors.New("nil block body")

// ErrNilHeaderHandler signals that a nil header handler has been provided
var ErrNilHeaderHandler = errors.New("nil header handler")

// ErrInvalidMiniBlockType signals that an invalid miniBlock type has been provided
var ErrInvalidMiniBlockType = errors.New("invalid miniBlock type")

// ErrInvalidMaxHardCapForMissingNodes signals that the maximum hardcap value for missing nodes is invalid
var ErrInvalidMaxHardCapForMissingNodes = errors.New("invalid max hardcap for missing nodes")

// ErrInvalidNumConcurrentTrieSyncers signals that the number of concurrent trie syncers is invalid
var ErrInvalidNumConcurrentTrieSyncers = errors.New("invalid num concurrent trie syncers")

// ErrNilTrieExporter signals that nil trie exporter has been provided
var ErrNilTrieExporter = errors.New("nil trie exporter")

// ErrInvalidOwnShardId signals that an invalid own shard id has been provided
var ErrInvalidOwnShardId = errors.New("invalid own shard id")
