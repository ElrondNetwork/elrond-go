package interceptorscontainer

import (
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/data/state"
	"github.com/ElrondNetwork/elrond-go/dataRetriever"
	"github.com/ElrondNetwork/elrond-go/hashing"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/process/dataValidators"
	"github.com/ElrondNetwork/elrond-go/process/factory"
	"github.com/ElrondNetwork/elrond-go/process/interceptors"
	interceptorFactory "github.com/ElrondNetwork/elrond-go/process/interceptors/factory"
	"github.com/ElrondNetwork/elrond-go/process/interceptors/processor"
	"github.com/ElrondNetwork/elrond-go/process/mock"
	"github.com/ElrondNetwork/elrond-go/sharding"
)

const numGoRoutines = 100
const chunksProcessorRequestInterval = time.Millisecond * 400

type baseInterceptorsContainerFactory struct {
	container              process.InterceptorsContainer
	shardCoordinator       sharding.Coordinator
	accounts               state.AccountsAdapter
	store                  dataRetriever.StorageService
	dataPool               dataRetriever.PoolsHolder
	messenger              process.TopicHandler
	nodesCoordinator       sharding.NodesCoordinator
	blockBlackList         process.TimeCacher
	argInterceptorFactory  *interceptorFactory.ArgInterceptedDataFactory
	globalThrottler        process.InterceptorThrottler
	maxTxNonceDeltaAllowed int
	antifloodHandler       process.P2PAntifloodHandler
	whiteListHandler       process.WhiteListHandler
	whiteListerVerifiedTxs process.WhiteListHandler
	preferredPeersHolder   process.PreferredPeersHolderHandler
	hasher                 hashing.Hasher
	requestHandler         process.RequestHandler
}

func checkBaseParams(
	coreComponents process.CoreComponentsHolder,
	cryptoComponents process.CryptoComponentsHolder,
	shardCoordinator sharding.Coordinator,
	accounts state.AccountsAdapter,
	store dataRetriever.StorageService,
	dataPool dataRetriever.PoolsHolder,
	messenger process.TopicHandler,
	nodesCoordinator sharding.NodesCoordinator,
	blackList process.TimeCacher,
	antifloodHandler process.P2PAntifloodHandler,
	whiteListHandler process.WhiteListHandler,
	whiteListerVerifiedTxs process.WhiteListHandler,
	preferredPeersHolder process.PreferredPeersHolderHandler,
	requestHandler process.RequestHandler,
) error {
	if check.IfNil(coreComponents) {
		return process.ErrNilCoreComponentsHolder
	}
	if check.IfNil(cryptoComponents) {
		return process.ErrNilCryptoComponentsHolder
	}
	if check.IfNil(shardCoordinator) {
		return process.ErrNilShardCoordinator
	}
	if check.IfNil(messenger) {
		return process.ErrNilMessenger
	}
	if check.IfNil(store) {
		return process.ErrNilStore
	}
	if check.IfNil(coreComponents.InternalMarshalizer()) || check.IfNil(coreComponents.TxMarshalizer()) {
		return process.ErrNilMarshalizer
	}
	if check.IfNil(coreComponents.Hasher()) {
		return process.ErrNilHasher
	}
	if check.IfNil(coreComponents.TxSignHasher()) {
		return process.ErrNilHasher
	}
	if check.IfNil(coreComponents.TxVersionChecker()) {
		return process.ErrNilTransactionVersionChecker
	}
	if check.IfNil(coreComponents.EpochNotifier()) {
		return process.ErrNilEpochNotifier
	}
	if len(coreComponents.ChainID()) == 0 {
		return process.ErrInvalidChainID
	}
	if coreComponents.MinTransactionVersion() == 0 {
		return process.ErrInvalidTransactionVersion
	}
	if check.IfNil(cryptoComponents.MultiSigner()) {
		return process.ErrNilMultiSigVerifier
	}
	if check.IfNil(cryptoComponents.BlockSignKeyGen()) {
		return process.ErrNilKeyGen
	}
	if check.IfNil(cryptoComponents.BlockSigner()) {
		return process.ErrNilSingleSigner
	}
	if check.IfNil(cryptoComponents.TxSignKeyGen()) {
		return process.ErrNilKeyGen
	}
	if check.IfNil(cryptoComponents.TxSingleSigner()) {
		return process.ErrNilSingleSigner
	}
	if check.IfNil(dataPool) {
		return process.ErrNilDataPoolHolder
	}
	if check.IfNil(nodesCoordinator) {
		return process.ErrNilNodesCoordinator
	}
	if check.IfNil(accounts) {
		return process.ErrNilAccountsAdapter
	}
	if check.IfNil(blackList) {
		return process.ErrNilBlackListCacher
	}
	if check.IfNil(antifloodHandler) {
		return process.ErrNilAntifloodHandler
	}
	if check.IfNil(whiteListHandler) {
		return process.ErrNilWhiteListHandler
	}
	if check.IfNil(whiteListerVerifiedTxs) {
		return process.ErrNilWhiteListHandler
	}
	if check.IfNil(coreComponents.AddressPubKeyConverter()) {
		return process.ErrNilPubkeyConverter
	}
	if check.IfNil(preferredPeersHolder) {
		return process.ErrNilPreferredPeersHolder
	}
	if check.IfNil(requestHandler) {
		return process.ErrNilRequestHandler
	}

	return nil
}

func (bicf *baseInterceptorsContainerFactory) createTopicAndAssignHandler(
	topic string,
	interceptor process.Interceptor,
	createChannel bool,
) (process.Interceptor, error) {

	err := bicf.messenger.CreateTopic(topic, createChannel)
	if err != nil {
		return nil, err
	}

	return interceptor, bicf.messenger.RegisterMessageProcessor(topic, core.DefaultInterceptorsIdentifier, interceptor)
}

//------- Tx interceptors

func (bicf *baseInterceptorsContainerFactory) generateTxInterceptors() error {
	shardC := bicf.shardCoordinator

	noOfShards := shardC.NumberOfShards()

	keys := make([]string, noOfShards)
	interceptorSlice := make([]process.Interceptor, noOfShards)

	for idx := uint32(0); idx < noOfShards; idx++ {
		identifierTx := factory.TransactionTopic + shardC.CommunicationIdentifier(idx)

		interceptor, err := bicf.createOneTxInterceptor(identifierTx)
		if err != nil {
			return err
		}

		keys[int(idx)] = identifierTx
		interceptorSlice[int(idx)] = interceptor
	}

	//tx interceptor for metachain topic
	identifierTx := factory.TransactionTopic + shardC.CommunicationIdentifier(core.MetachainShardId)

	interceptor, err := bicf.createOneTxInterceptor(identifierTx)
	if err != nil {
		return err
	}

	keys = append(keys, identifierTx)
	interceptorSlice = append(interceptorSlice, interceptor)

	return bicf.container.AddMultiple(keys, interceptorSlice)
}

func (bicf *baseInterceptorsContainerFactory) createOneTxInterceptor(topic string) (process.Interceptor, error) {
	if bicf.argInterceptorFactory == nil {
		return nil, process.ErrNilArgumentStruct
	}
	if check.IfNil(bicf.argInterceptorFactory.CoreComponents) {
		return nil, process.ErrNilCoreComponentsHolder
	}

	addrPubKeyConverter := bicf.argInterceptorFactory.CoreComponents.AddressPubKeyConverter()

	txValidator, err := dataValidators.NewTxValidator(
		bicf.accounts,
		bicf.shardCoordinator,
		bicf.whiteListHandler,
		addrPubKeyConverter,
		bicf.maxTxNonceDeltaAllowed,
	)
	if err != nil {
		return nil, err
	}

	argProcessor := &processor.ArgTxInterceptorProcessor{
		ShardedDataCache: bicf.dataPool.Transactions(),
		TxValidator:      txValidator,
	}
	txProcessor, err := processor.NewTxInterceptorProcessor(argProcessor)
	if err != nil {
		return nil, err
	}

	txFactory, err := interceptorFactory.NewInterceptedTxDataFactory(bicf.argInterceptorFactory)
	if err != nil {
		return nil, err
	}

	internalMarshalizer := bicf.argInterceptorFactory.CoreComponents.InternalMarshalizer()
	interceptor, err := interceptors.NewMultiDataInterceptor(
		interceptors.ArgMultiDataInterceptor{
			ArgSingleDataInterceptor: interceptors.ArgSingleDataInterceptor{
				Topic:                topic,
				DataFactory:          txFactory,
				Processor:            txProcessor,
				Throttler:            bicf.globalThrottler,
				AntifloodHandler:     bicf.antifloodHandler,
				WhiteListRequest:     bicf.whiteListHandler,
				CurrentPeerId:        bicf.messenger.ID(),
				PreferredPeersHolder: bicf.preferredPeersHolder,
			},
			Marshalizer: internalMarshalizer,
		},
	)
	if err != nil {
		return nil, err
	}

	return bicf.createTopicAndAssignHandler(topic, interceptor, true)
}

func (bicf *baseInterceptorsContainerFactory) createOneUnsignedTxInterceptor(topic string) (process.Interceptor, error) {
	if bicf.argInterceptorFactory == nil {
		return nil, process.ErrNilArgumentStruct
	}
	if check.IfNil(bicf.argInterceptorFactory.CoreComponents) {
		return nil, process.ErrNilCoreComponentsHolder
	}
	//TODO replace the nil tx validator with white list validator
	txValidator, err := mock.NewNilTxValidator()
	if err != nil {
		return nil, err
	}

	argProcessor := &processor.ArgTxInterceptorProcessor{
		ShardedDataCache: bicf.dataPool.UnsignedTransactions(),
		TxValidator:      txValidator,
	}
	txProcessor, err := processor.NewTxInterceptorProcessor(argProcessor)
	if err != nil {
		return nil, err
	}

	txFactory, err := interceptorFactory.NewInterceptedUnsignedTxDataFactory(bicf.argInterceptorFactory)
	if err != nil {
		return nil, err
	}

	internalMarshalizer := bicf.argInterceptorFactory.CoreComponents.InternalMarshalizer()
	interceptor, err := interceptors.NewMultiDataInterceptor(
		interceptors.ArgMultiDataInterceptor{
			ArgSingleDataInterceptor: interceptors.ArgSingleDataInterceptor{
				Topic:                topic,
				DataFactory:          txFactory,
				Processor:            txProcessor,
				Throttler:            bicf.globalThrottler,
				AntifloodHandler:     bicf.antifloodHandler,
				WhiteListRequest:     bicf.whiteListHandler,
				CurrentPeerId:        bicf.messenger.ID(),
				PreferredPeersHolder: bicf.preferredPeersHolder,
			},
			Marshalizer: internalMarshalizer,
		},
	)
	if err != nil {
		return nil, err
	}

	return bicf.createTopicAndAssignHandler(topic, interceptor, true)
}

func (bicf *baseInterceptorsContainerFactory) createOneRewardTxInterceptor(topic string) (process.Interceptor, error) {
	if bicf.argInterceptorFactory == nil {
		return nil, process.ErrNilArgumentStruct
	}
	if check.IfNil(bicf.argInterceptorFactory.CoreComponents) {
		return nil, process.ErrNilCoreComponentsHolder
	}
	//TODO replace the nil tx validator with white list validator
	txValidator, err := mock.NewNilTxValidator()
	if err != nil {
		return nil, err
	}

	argProcessor := &processor.ArgTxInterceptorProcessor{
		ShardedDataCache: bicf.dataPool.RewardTransactions(),
		TxValidator:      txValidator,
	}
	txProcessor, err := processor.NewTxInterceptorProcessor(argProcessor)
	if err != nil {
		return nil, err
	}

	txFactory, err := interceptorFactory.NewInterceptedRewardTxDataFactory(bicf.argInterceptorFactory)
	if err != nil {
		return nil, err
	}

	internalMarshalizer := bicf.argInterceptorFactory.CoreComponents.InternalMarshalizer()
	interceptor, err := interceptors.NewMultiDataInterceptor(
		interceptors.ArgMultiDataInterceptor{
			ArgSingleDataInterceptor: interceptors.ArgSingleDataInterceptor{
				Topic:                topic,
				DataFactory:          txFactory,
				Processor:            txProcessor,
				Throttler:            bicf.globalThrottler,
				AntifloodHandler:     bicf.antifloodHandler,
				WhiteListRequest:     bicf.whiteListHandler,
				CurrentPeerId:        bicf.messenger.ID(),
				PreferredPeersHolder: bicf.preferredPeersHolder,
			},
			Marshalizer: internalMarshalizer,
		},
	)
	if err != nil {
		return nil, err
	}

	return bicf.createTopicAndAssignHandler(topic, interceptor, true)
}

//------- Hdr interceptor

func (bicf *baseInterceptorsContainerFactory) generateHeaderInterceptors() error {
	shardC := bicf.shardCoordinator
	//TODO implement other HeaderHandlerProcessValidator that will check the header's nonce
	// against blockchain's latest nonce - k finality
	hdrValidator, err := dataValidators.NewNilHeaderValidator()
	if err != nil {
		return err
	}

	hdrFactory, err := interceptorFactory.NewInterceptedShardHeaderDataFactory(bicf.argInterceptorFactory)
	if err != nil {
		return err
	}

	argProcessor := &processor.ArgHdrInterceptorProcessor{
		Headers:        bicf.dataPool.Headers(),
		HdrValidator:   hdrValidator,
		BlockBlackList: bicf.blockBlackList,
	}
	hdrProcessor, err := processor.NewHdrInterceptorProcessor(argProcessor)
	if err != nil {
		return err
	}

	// compose header shard topic, for example: shardBlocks_0_META
	identifierHdr := factory.ShardBlocksTopic + shardC.CommunicationIdentifier(core.MetachainShardId)

	//only one intrashard header topic
	interceptor, err := interceptors.NewSingleDataInterceptor(
		interceptors.ArgSingleDataInterceptor{
			Topic:                identifierHdr,
			DataFactory:          hdrFactory,
			Processor:            hdrProcessor,
			Throttler:            bicf.globalThrottler,
			AntifloodHandler:     bicf.antifloodHandler,
			WhiteListRequest:     bicf.whiteListHandler,
			CurrentPeerId:        bicf.messenger.ID(),
			PreferredPeersHolder: bicf.preferredPeersHolder,
		},
	)
	if err != nil {
		return err
	}

	_, err = bicf.createTopicAndAssignHandler(identifierHdr, interceptor, true)
	if err != nil {
		return err
	}

	return bicf.container.Add(identifierHdr, interceptor)
}

//------- MiniBlocks interceptors

func (bicf *baseInterceptorsContainerFactory) generateMiniBlocksInterceptors() error {
	shardC := bicf.shardCoordinator
	noOfShards := shardC.NumberOfShards()
	keys := make([]string, noOfShards+2)
	interceptorsSlice := make([]process.Interceptor, noOfShards+2)

	for idx := uint32(0); idx < noOfShards; idx++ {
		identifierMiniBlocks := factory.MiniBlocksTopic + shardC.CommunicationIdentifier(idx)

		interceptor, err := bicf.createOneMiniBlocksInterceptor(identifierMiniBlocks)
		if err != nil {
			return err
		}

		keys[int(idx)] = identifierMiniBlocks
		interceptorsSlice[int(idx)] = interceptor
	}

	identifierMiniBlocks := factory.MiniBlocksTopic + shardC.CommunicationIdentifier(core.MetachainShardId)

	interceptor, err := bicf.createOneMiniBlocksInterceptor(identifierMiniBlocks)
	if err != nil {
		return err
	}

	keys[noOfShards] = identifierMiniBlocks
	interceptorsSlice[noOfShards] = interceptor

	identifierAllShardsMiniBlocks := factory.MiniBlocksTopic + shardC.CommunicationIdentifier(core.AllShardId)

	allShardsMiniBlocksInterceptorinterceptor, err := bicf.createOneMiniBlocksInterceptor(identifierAllShardsMiniBlocks)
	if err != nil {
		return err
	}

	keys[noOfShards+1] = identifierAllShardsMiniBlocks
	interceptorsSlice[noOfShards+1] = allShardsMiniBlocksInterceptorinterceptor

	return bicf.container.AddMultiple(keys, interceptorsSlice)
}

func (bicf *baseInterceptorsContainerFactory) createOneMiniBlocksInterceptor(topic string) (process.Interceptor, error) {
	internalMarshalizer := bicf.argInterceptorFactory.CoreComponents.InternalMarshalizer()
	hasher := bicf.argInterceptorFactory.CoreComponents.Hasher()
	argProcessor := &processor.ArgMiniblockInterceptorProcessor{
		MiniblockCache:   bicf.dataPool.MiniBlocks(),
		Marshalizer:      internalMarshalizer,
		Hasher:           hasher,
		ShardCoordinator: bicf.shardCoordinator,
		WhiteListHandler: bicf.whiteListHandler,
	}
	miniblockProcessor, err := processor.NewMiniblockInterceptorProcessor(argProcessor)
	if err != nil {
		return nil, err
	}

	miniblockFactory, err := interceptorFactory.NewInterceptedMiniblockDataFactory(bicf.argInterceptorFactory)
	if err != nil {
		return nil, err
	}

	interceptor, err := interceptors.NewMultiDataInterceptor(
		interceptors.ArgMultiDataInterceptor{
			ArgSingleDataInterceptor: interceptors.ArgSingleDataInterceptor{
				Topic:                topic,
				DataFactory:          miniblockFactory,
				Processor:            miniblockProcessor,
				Throttler:            bicf.globalThrottler,
				AntifloodHandler:     bicf.antifloodHandler,
				WhiteListRequest:     bicf.whiteListHandler,
				CurrentPeerId:        bicf.messenger.ID(),
				PreferredPeersHolder: bicf.preferredPeersHolder,
			},

			Marshalizer: internalMarshalizer,
		},
	)
	if err != nil {
		return nil, err
	}

	return bicf.createTopicAndAssignHandler(topic, interceptor, true)
}

//------- MetachainHeader interceptors

func (bicf *baseInterceptorsContainerFactory) generateMetachainHeaderInterceptors() error {
	identifierHdr := factory.MetachainBlocksTopic
	//TODO implement other HeaderHandlerProcessValidator that will check the header's nonce
	// against blockchain's latest nonce - k finality
	hdrValidator, err := dataValidators.NewNilHeaderValidator()
	if err != nil {
		return err
	}

	hdrFactory, err := interceptorFactory.NewInterceptedMetaHeaderDataFactory(bicf.argInterceptorFactory)
	if err != nil {
		return err
	}

	argProcessor := &processor.ArgHdrInterceptorProcessor{
		Headers:        bicf.dataPool.Headers(),
		HdrValidator:   hdrValidator,
		BlockBlackList: bicf.blockBlackList,
	}
	hdrProcessor, err := processor.NewHdrInterceptorProcessor(argProcessor)
	if err != nil {
		return err
	}

	//only one metachain header topic
	interceptor, err := interceptors.NewSingleDataInterceptor(
		interceptors.ArgSingleDataInterceptor{
			Topic:                identifierHdr,
			DataFactory:          hdrFactory,
			Processor:            hdrProcessor,
			Throttler:            bicf.globalThrottler,
			AntifloodHandler:     bicf.antifloodHandler,
			WhiteListRequest:     bicf.whiteListHandler,
			CurrentPeerId:        bicf.messenger.ID(),
			PreferredPeersHolder: bicf.preferredPeersHolder,
		},
	)
	if err != nil {
		return err
	}

	_, err = bicf.createTopicAndAssignHandler(identifierHdr, interceptor, true)
	if err != nil {
		return err
	}

	return bicf.container.Add(identifierHdr, interceptor)
}

func (bicf *baseInterceptorsContainerFactory) createOneTrieNodesInterceptor(topic string) (process.Interceptor, error) {
	trieNodesProcessor, err := processor.NewTrieNodesInterceptorProcessor(bicf.dataPool.TrieNodes())
	if err != nil {
		return nil, err
	}

	trieNodesFactory, err := interceptorFactory.NewInterceptedTrieNodeDataFactory(bicf.argInterceptorFactory)
	if err != nil {
		return nil, err
	}

	internalMarshalizer := bicf.argInterceptorFactory.CoreComponents.InternalMarshalizer()
	interceptor, err := interceptors.NewMultiDataInterceptor(
		interceptors.ArgMultiDataInterceptor{
			ArgSingleDataInterceptor: interceptors.ArgSingleDataInterceptor{
				Topic:                topic,
				DataFactory:          trieNodesFactory,
				Processor:            trieNodesProcessor,
				Throttler:            bicf.globalThrottler,
				AntifloodHandler:     bicf.antifloodHandler,
				WhiteListRequest:     bicf.whiteListHandler,
				CurrentPeerId:        bicf.messenger.ID(),
				PreferredPeersHolder: bicf.preferredPeersHolder,
			},
			Marshalizer: internalMarshalizer,
		},
	)
	if err != nil {
		return nil, err
	}

	argChunkProcessor := processor.TrieNodesChunksProcessorArgs{
		Hasher:          bicf.hasher,
		ChunksCacher:    bicf.dataPool.TrieNodesChunks(),
		RequestInterval: chunksProcessorRequestInterval,
		RequestHandler:  bicf.requestHandler,
		Topic:           topic,
	}

	chunkProcessor, err := processor.NewTrieNodeChunksProcessor(argChunkProcessor)
	if err != nil {
		return nil, err
	}

	err = interceptor.SetChunkProcessor(chunkProcessor)
	if err != nil {
		return nil, err
	}

	return bicf.createTopicAndAssignHandler(topic, interceptor, true)
}

func (bicf *baseInterceptorsContainerFactory) generateUnsignedTxsInterceptors() error {
	shardC := bicf.shardCoordinator

	noOfShards := shardC.NumberOfShards()

	keys := make([]string, noOfShards)
	interceptorsSlice := make([]process.Interceptor, noOfShards)

	for idx := uint32(0); idx < noOfShards; idx++ {
		identifierScr := factory.UnsignedTransactionTopic + shardC.CommunicationIdentifier(idx)
		interceptor, err := bicf.createOneUnsignedTxInterceptor(identifierScr)
		if err != nil {
			return err
		}

		keys[int(idx)] = identifierScr
		interceptorsSlice[int(idx)] = interceptor
	}

	return bicf.container.AddMultiple(keys, interceptorsSlice)
}
