package dataRetriever

import (
	"errors"
)

// ErrNilMessage signals that a nil message has been received
var ErrNilMessage = errors.New("nil message")

// ErrSendRequest signals that the connected peers list is empty or errors appeared when sending requests
var ErrSendRequest = errors.New("cannot send request: peer list is empty or errors during the sending")

// ErrNilValue signals the value is nil
var ErrNilValue = errors.New("nil value")

// ErrTxNotFoundInBlockPool signals the value is nil
var ErrTxNotFoundInBlockPool = errors.New("cannot find tx in current block pool")

// ErrNilMarshalizer signals that an operation has been attempted to or with a nil Marshalizer implementation
var ErrNilMarshalizer = errors.New("nil Marshalizer")

// ErrNilStore signals that the provided storage service is nil
var ErrNilStore = errors.New("nil data storage service")

// ErrNilMessenger signals that a nil Messenger object was provided
var ErrNilMessenger = errors.New("nil Messenger")

// ErrNilTxDataPool signals that a nil transaction pool has been provided
var ErrNilTxDataPool = errors.New("nil transaction data pool")

// ErrNilUnsignedTransactionPool signals that a nil unsigned transactions pool has been provided
var ErrNilUnsignedTransactionPool = errors.New("nil unsigned transactions data pool")

// ErrNilRewardTransactionPool signals that a nil reward transactions pool has been provided
var ErrNilRewardTransactionPool = errors.New("nil reward transaction data pool")

// ErrNilHeadersDataPool signals that a nil header pool has been provided
var ErrNilHeadersDataPool = errors.New("nil headers data pool")

// ErrNilShardCoordinator signals that an operation has been attempted to or with a nil shard coordinator
var ErrNilShardCoordinator = errors.New("nil shard coordinator")

// ErrNilDataToProcess signals that nil data was provided
var ErrNilDataToProcess = errors.New("nil data to process")

// ErrNilTxStorage signals that a nil transaction storage has been provided
var ErrNilTxStorage = errors.New("nil transaction storage")

// ErrNilHeadersStorage signals that a nil header storage has been provided
var ErrNilHeadersStorage = errors.New("nil headers storage")

// ErrNilHeadersNoncesStorage signals that a nil header-nonce storage has been provided
var ErrNilHeadersNoncesStorage = errors.New("nil headers nonces storage")

// ErrNilResolverSender signals that a nil resolver sender object has been provided
var ErrNilResolverSender = errors.New("nil resolver sender")

// ErrInvalidNonceByteSlice signals that an invalid byte slice has been provided
// and an uint64 can not be decoded from that byte slice
var ErrInvalidNonceByteSlice = errors.New("invalid nonce byte slice")

// ErrResolveTypeUnknown signals that an unknown resolve type was provided
var ErrResolveTypeUnknown = errors.New("unknown resolve type")

// ErrNilMiniblocksPool signals that a nil miniblocks pool has been provided
var ErrNilMiniblocksPool = errors.New("nil miniblocks pool")

// ErrNilMiniblocksStorage signals that a nil miniblocks storage has been provided
var ErrNilMiniblocksStorage = errors.New("nil miniblocks storage")

// ErrNilDataPoolHolder signals that the data pool holder is nil
var ErrNilDataPoolHolder = errors.New("nil data pool holder")

// ErrNilContainerElement signals when trying to add a nil element in the container
var ErrNilContainerElement = errors.New("element cannot be nil")

// ErrInvalidContainerKey signals that an element does not exist in the container's map
var ErrInvalidContainerKey = errors.New("element does not exist in container")

// ErrContainerKeyAlreadyExists signals that an element was already set in the container's map
var ErrContainerKeyAlreadyExists = errors.New("provided key already exists in container")

// ErrNilUint64ByteSliceConverter signals that a nil byte slice converter was provided
var ErrNilUint64ByteSliceConverter = errors.New("nil byte slice converter")

// ErrNilResolverContainer signals that a nil resolver container was provided
var ErrNilResolverContainer = errors.New("nil resolver container")

// ErrWrongTypeInContainer signals that a wrong type of object was found in container
var ErrWrongTypeInContainer = errors.New("wrong type of object inside container")

// ErrLenMismatch signals that 2 or more slices have different lengths
var ErrLenMismatch = errors.New("lengths mismatch")

// ErrNilPeerChangeBlockDataPool signals that a nil peer change pool has been provided
var ErrNilPeerChangeBlockDataPool = errors.New("nil peer change block data pool")

// ErrNilTxBlockDataPool signals that a nil tx block body pool has been provided
var ErrNilTxBlockDataPool = errors.New("nil tx block data pool")

// ErrCacheConfigInvalidSizeInBytes signals that the cache parameter "sizeInBytes" is invalid
var ErrCacheConfigInvalidSizeInBytes = errors.New("cache parameter [sizeInBytes] is not valid, it must be a positive, and large enough number")

// ErrCacheConfigInvalidSize signals that the cache parameter "size" is invalid
var ErrCacheConfigInvalidSize = errors.New("cache parameter [size] is not valid, it must be a positive number")

// ErrCacheConfigInvalidShards signals that the cache parameter "shards" is invalid
var ErrCacheConfigInvalidShards = errors.New("cache parameter [shards] is not valid, it must be a positive number")

// ErrCacheConfigInvalidEconomics signals that an economics parameter required by the cache is invalid
var ErrCacheConfigInvalidEconomics = errors.New("cache-economics parameter is not valid")

// ErrCacheConfigInvalidSharding signals that a sharding parameter required by the cache is invalid
var ErrCacheConfigInvalidSharding = errors.New("cache-sharding parameter is not valid")

// ErrNilTrieNodesPool signals that a nil trie nodes data pool was provided
var ErrNilTrieNodesPool = errors.New("nil trie nodes data pool")

// ErrNilTrieNodesChunksPool signals that a nil trie nodes chunks data pool was provided
var ErrNilTrieNodesChunksPool = errors.New("nil trie nodes chunks data pool")

// ErrNoSuchStorageUnit defines the error for using an invalid storage unit
var ErrNoSuchStorageUnit = errors.New("no such unit type")

// ErrNilRandomizer signals that a nil randomizer has been provided
var ErrNilRandomizer = errors.New("nil randomizer")

// ErrRequestTypeNotImplemented signals that a not implemented type of request has been received
var ErrRequestTypeNotImplemented = errors.New("request type is not implemented")

// ErrNilDataPacker signals that a nil data packer has been provided
var ErrNilDataPacker = errors.New("nil data packer provided")

// ErrNilResolverFinder signals that a nil resolver finder has been provided
var ErrNilResolverFinder = errors.New("nil resolvers finder")

// ErrInvalidMaxTxRequest signals that max tx request is too small
var ErrInvalidMaxTxRequest = errors.New("max tx request number is invalid")

// ErrNilPeerListCreator signals that a nil peer list creator implementation has been provided
var ErrNilPeerListCreator = errors.New("nil peer list creator provided")

// ErrNilTrieDataGetter signals that a nil trie data getter has been provided
var ErrNilTrieDataGetter = errors.New("nil trie data getter provided")

// ErrNilCurrBlockTxs signals that nil current blocks txs holder was provided
var ErrNilCurrBlockTxs = errors.New("nil current block txs holder")

// ErrNilRequestedItemsHandler signals that a nil requested items handler was provided
var ErrNilRequestedItemsHandler = errors.New("nil requested items handler")

// ErrNilEpochHandler signals that epoch handler is nil
var ErrNilEpochHandler = errors.New("nil epoch handler")

// ErrBadRequest signals that the request should not have happened
var ErrBadRequest = errors.New("request should not be done as it doesn't follow the protocol")

// ErrNilAntifloodHandler signals that a nil antiflood handler has been provided
var ErrNilAntifloodHandler = errors.New("nil antiflood handler")

// ErrNilPreferredPeersHolder signals that a nil preferred peers holder handler has been provided
var ErrNilPreferredPeersHolder = errors.New("nil preferred peers holder")

// ErrNilSelfShardIDProvider signals that a nil self shard ID provider has been provided
var ErrNilSelfShardIDProvider = errors.New("nil self shard ID provider")

// ErrNilCurrentNetworkEpochProvider signals that a nil CurrentNetworkEpochProvider handler has been provided
var ErrNilCurrentNetworkEpochProvider = errors.New("nil current network epoch provider")

// ErrSystemBusy signals that the system is busy and can not process more requests
var ErrSystemBusy = errors.New("system busy")

// ErrNilThrottler signals that a nil throttler has been provided
var ErrNilThrottler = errors.New("nil throttler")

// ErrEmptyString signals that an empty string has been provided
var ErrEmptyString = errors.New("empty string")

// ErrInvalidValue signals that an invalid value has been provided
var ErrInvalidValue = errors.New("invalid value")

// ErrNilWhiteListHandler signals that white list handler is nil
var ErrNilWhiteListHandler = errors.New("nil white list handler")

// ErrRequestIntervalTooSmall signals that request interval is too small
var ErrRequestIntervalTooSmall = errors.New("request interval is too small")

// ErrNilResolverDebugHandler signals that a nil resolver debug handler has been provided
var ErrNilResolverDebugHandler = errors.New("nil resolver debug handler")

// ErrMissingData signals that the required data is missing
var ErrMissingData = errors.New("missing data")

// ErrNilConfig signals that a nil config has been provided
var ErrNilConfig = errors.New("nil config provided")

// ErrNilEconomicsData signals that a nil economics data handler has been provided
var ErrNilEconomicsData = errors.New("nil economics data provided")

// ErrNilTxGasHandler signals that a nil tx gas handler was provided
var ErrNilTxGasHandler = errors.New("nil tx gas handler provided")

// ErrNilManualEpochStartNotifier signals that a nil manual epoch start notifier has been provided
var ErrNilManualEpochStartNotifier = errors.New("nil manual epoch start notifier")

// ErrNilGracefullyCloseChannel signals that a nil gracefully close channel has been provided
var ErrNilGracefullyCloseChannel = errors.New("nil gracefully close channel")

// ErrNilSmartContractsPool signals that a nil smart contracts pool has been provided
var ErrNilSmartContractsPool = errors.New("nil smart contracts pool")

// ErrNilHasher signals that a nil hasher has been provided
var ErrNilHasher = errors.New("nil hasher")

// ErrNilTrieStorageManager signals that a nil trie storage manager has been provided
var ErrNilTrieStorageManager = errors.New("nil trie storage manager")

// ErrNilPathManager signals that a nil path manager has been provided
var ErrNilPathManager = errors.New("nil path manager")

// ErrNilPeerAuthenticationPool signals that a nil peer authentication pool has been provided
var ErrNilPeerAuthenticationPool = errors.New("nil peer authentication pool")

// ErrNotFound signals that a data is missing
var ErrNotFound = errors.New("data not found")
