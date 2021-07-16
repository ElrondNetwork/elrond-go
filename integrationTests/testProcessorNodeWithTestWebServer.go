package integrationTests

import (
	"net/http"
	"net/http/httptest"
	"sync"

	arwenConfig "github.com/ElrondNetwork/arwen-wasm-vm/v1_4/config"
	"github.com/ElrondNetwork/elrond-go/api"
	"github.com/ElrondNetwork/elrond-go/api/middleware"
	"github.com/ElrondNetwork/elrond-go/config"
	dataTransaction "github.com/ElrondNetwork/elrond-go/data/transaction"
	nodeFacade "github.com/ElrondNetwork/elrond-go/facade"
	"github.com/ElrondNetwork/elrond-go/integrationTests/mock"
	"github.com/ElrondNetwork/elrond-go/node/external"
	"github.com/ElrondNetwork/elrond-go/node/trieIterators"
	"github.com/ElrondNetwork/elrond-go/node/trieIterators/factory"
	"github.com/ElrondNetwork/elrond-go/process/coordinator"
	"github.com/ElrondNetwork/elrond-go/process/smartContract/builtInFunctions"
	"github.com/ElrondNetwork/elrond-go/process/transaction"
	"github.com/ElrondNetwork/elrond-go/process/txsimulator"
	"github.com/ElrondNetwork/elrond-go/testscommon"
	"github.com/ElrondNetwork/elrond-go/vm/systemSmartContracts/defaults"
	"github.com/ElrondNetwork/elrond-vm-common/parsers"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// TestProcessorNodeWithTestWebServer represents a TestProcessorNode with a test web server
type TestProcessorNodeWithTestWebServer struct {
	*TestProcessorNode
	facade Facade
	mutWs  sync.Mutex
	ws     *gin.Engine
}

// NewTestProcessorNodeWithTestWebServer returns a new TestProcessorNodeWithTestWebServer instance with a libp2p messenger
func NewTestProcessorNodeWithTestWebServer(
	maxShards uint32,
	nodeShardId uint32,
	txSignPrivKeyShardId uint32,
) *TestProcessorNodeWithTestWebServer {

	tpn := newBaseTestProcessorNode(maxShards, nodeShardId, txSignPrivKeyShardId)
	tpn.initTestNode()

	argFacade := createFacadeArg(tpn)
	facade, err := nodeFacade.NewNodeFacade(argFacade)
	log.LogIfError(err)

	ws := createGinServer(facade, argFacade.ApiRoutesConfig)

	return &TestProcessorNodeWithTestWebServer{
		TestProcessorNode: tpn,
		facade:            facade,
		ws:                ws,
	}
}

// DoRequest preforms a test request on the web server, returning the response ready to be parsed
func (node *TestProcessorNodeWithTestWebServer) DoRequest(request *http.Request) *httptest.ResponseRecorder {
	// this is a critical section, serialize each request
	node.mutWs.Lock()
	defer node.mutWs.Unlock()

	resp := httptest.NewRecorder()
	node.ws.ServeHTTP(resp, request)

	return resp
}

func createFacadeArg(tpn *TestProcessorNode) nodeFacade.ArgNodeFacade {
	apiResolver, txSimulator := createFacadeComponents(tpn)

	return nodeFacade.ArgNodeFacade{
		Node:                   tpn.Node,
		ApiResolver:            apiResolver,
		TxSimulatorProcessor:   txSimulator,
		RestAPIServerDebugMode: false,
		WsAntifloodConfig: config.WebServerAntifloodConfig{
			SimultaneousRequests:         1000,
			SameSourceRequests:           1000,
			SameSourceResetIntervalInSec: 1,
			EndpointsThrottlers:          []config.EndpointsThrottlersConfig{},
		},
		FacadeConfig:    config.FacadeConfig{},
		ApiRoutesConfig: createTestApiConfig(),
		AccountsState:   tpn.AccntState,
		PeerState:       tpn.PeerState,
		Blockchain:      tpn.BlockChain,
	}
}

func createTestApiConfig() config.ApiRoutesConfig {
	routes := map[string][]string{
		"node":        {"/status", "/metrics", "/heartbeatstatus", "/statistics", "/p2pstatus", "/debug", "/peerinfo"},
		"address":     {"/:address", "/:address/balance", "/:address/username", "/:address/key/:key", "/:address/esdt", "/:address/esdt/:tokenIdentifier"},
		"hardfork":    {"/trigger"},
		"network":     {"/status", "/total-staked", "/economics", "/config"},
		"log":         {"/log"},
		"validator":   {"/statistics"},
		"vm-values":   {"/hex", "/string", "/int", "/query"},
		"transaction": {"/send", "/simulate", "/send-multiple", "/cost", "/:txhash"},
		"block":       {"/by-nonce/:nonce", "/by-hash/:hash"},
	}

	routesConfig := config.ApiRoutesConfig{
		APIPackages: make(map[string]config.APIPackageConfig),
	}

	for name, endpoints := range routes {
		packageConfig := config.APIPackageConfig{}
		for _, routeName := range endpoints {
			route := config.RouteConfig{
				Name: routeName,
				Open: true,
			}
			packageConfig.Routes = append(packageConfig.Routes, route)
		}

		routesConfig.APIPackages[name] = packageConfig
	}

	return routesConfig
}

func createFacadeComponents(tpn *TestProcessorNode) (nodeFacade.ApiResolver, nodeFacade.TransactionSimulatorProcessor) {
	gasMap := arwenConfig.MakeGasMapForTests()
	defaults.FillGasMapInternal(gasMap, 1)
	gasScheduleNotifier := mock.NewGasScheduleNotifierMock(gasMap)
	argsBuiltIn := builtInFunctions.ArgsCreateBuiltInFunctionContainer{
		GasSchedule:      gasScheduleNotifier,
		MapDNSAddresses:  make(map[string]struct{}),
		Marshalizer:      TestMarshalizer,
		Accounts:         tpn.AccntState,
		ShardCoordinator: tpn.ShardCoordinator,
		EpochNotifier:    tpn.EpochNotifier,
	}
	builtInFuncs, err := builtInFunctions.CreateBuiltInFunctionContainer(argsBuiltIn)
	log.LogIfError(err)
	esdtTransferParser, _ := parsers.NewESDTTransferParser(TestMarshalizer)
	argsTxTypeHandler := coordinator.ArgNewTxTypeHandler{
		PubkeyConverter:    TestAddressPubkeyConverter,
		ShardCoordinator:   tpn.ShardCoordinator,
		BuiltInFunctions:   builtInFuncs,
		ArgumentParser:     parsers.NewCallArgsParser(),
		EpochNotifier:      tpn.EpochNotifier,
		ESDTTransferParser: esdtTransferParser,
	}
	txTypeHandler, err := coordinator.NewTxTypeHandler(argsTxTypeHandler)
	log.LogIfError(err)

	txCostHandler, err := transaction.NewTransactionCostEstimator(
		txTypeHandler,
		tpn.EconomicsData,
		&mock.TransactionSimulatorStub{
			ProcessTxCalled: func(tx *dataTransaction.Transaction) (*dataTransaction.SimulationResults, error) {
				return &dataTransaction.SimulationResults{}, nil
			},
		},
		tpn.AccntState,
		tpn.ShardCoordinator,
	)
	log.LogIfError(err)

	accountsWrapper := &trieIterators.AccountsWrapper{
		Mutex:           &sync.Mutex{},
		AccountsAdapter: tpn.AccntState,
	}

	args := trieIterators.ArgTrieIteratorProcessor{
		ShardID:            tpn.ShardCoordinator.SelfId(),
		Accounts:           accountsWrapper,
		QueryService:       tpn.SCQueryService,
		BlockChain:         tpn.BlockChain,
		PublicKeyConverter: TestAddressPubkeyConverter,
	}
	totalStakedValueHandler, err := factory.CreateTotalStakedValueHandler(args)
	log.LogIfError(err)

	directStakedListHandler, err := factory.CreateDirectStakedListHandler(args)
	log.LogIfError(err)

	delegatedListHandler, err := factory.CreateDelegatedListHandler(args)
	log.LogIfError(err)

	argsApiResolver := external.ArgNodeApiResolver{
		SCQueryService:          tpn.SCQueryService,
		StatusMetricsHandler:    &mock.StatusMetricsStub{},
		TxCostHandler:           txCostHandler,
		TotalStakedValueHandler: totalStakedValueHandler,
		DirectStakedListHandler: directStakedListHandler,
		DelegatedListHandler:    delegatedListHandler,
	}

	apiResolver, err := external.NewNodeApiResolver(argsApiResolver)
	log.LogIfError(err)

	argSimulator := txsimulator.ArgsTxSimulator{
		TransactionProcessor:      tpn.TxProcessor,
		IntermediateProcContainer: tpn.InterimProcContainer,
		AddressPubKeyConverter:    TestAddressPubkeyConverter,
		ShardCoordinator:          tpn.ShardCoordinator,
		Marshalizer:               TestMarshalizer,
		Hasher:                    TestHasher,
		VMOutputCacher:            &testscommon.CacherMock{},
	}

	txSimulator, err := txsimulator.NewTransactionSimulator(argSimulator)
	log.LogIfError(err)

	return apiResolver, txSimulator
}

func createGinServer(facade Facade, apiConfig config.ApiRoutesConfig) *gin.Engine {
	ws := gin.New()
	ws.Use(cors.Default())
	ws.Use(middleware.WithFacade(facade))

	api.RegisterRoutes(
		ws,
		apiConfig,
		facade,
	)

	return ws
}
