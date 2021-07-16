package factory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/appStatusPolling"
	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/core/statistics"
	"github.com/ElrondNetwork/elrond-go/core/statistics/machine"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/debug/goroutine"
	"github.com/ElrondNetwork/elrond-go/epochStart/notifier"
	"github.com/ElrondNetwork/elrond-go/errors"
	"github.com/ElrondNetwork/elrond-go/outport"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/process"
	"github.com/ElrondNetwork/elrond-go/sharding"
)

var _ ComponentHandler = (*managedStatusComponents)(nil)
var _ StatusComponentsHolder = (*managedStatusComponents)(nil)
var _ StatusComponentsHandler = (*managedStatusComponents)(nil)

type managedStatusComponents struct {
	*statusComponents
	statusComponentsFactory *statusComponentsFactory
	cancelFunc              func()
	mutStatusComponents     sync.RWMutex
}

// NewManagedStatusComponents returns a new instance of managedStatusComponents
func NewManagedStatusComponents(scf *statusComponentsFactory) (*managedStatusComponents, error) {
	if scf == nil {
		return nil, errors.ErrNilStatusComponentsFactory
	}

	return &managedStatusComponents{
		statusComponents:        nil,
		statusComponentsFactory: scf,
		cancelFunc:              nil,
		mutStatusComponents:     sync.RWMutex{},
	}, nil
}

// Create will create the status components
func (msc *managedStatusComponents) Create() error {
	components, err := msc.statusComponentsFactory.Create()
	if err != nil {
		return fmt.Errorf("%w: %v", errors.ErrStatusComponentsFactoryCreate, err)
	}

	msc.mutStatusComponents.Lock()
	msc.statusComponents = components
	msc.mutStatusComponents.Unlock()

	return nil
}

// Close will close all the underlying components
func (msc *managedStatusComponents) Close() error {
	msc.mutStatusComponents.Lock()
	defer msc.mutStatusComponents.Unlock()

	if msc.statusComponents == nil {
		return nil
	}
	if msc.cancelFunc != nil {
		msc.cancelFunc()
	}

	err := msc.statusComponents.Close()
	if err != nil {
		return err
	}
	msc.statusComponents = nil

	return nil
}

// CheckSubcomponents verifies all subcomponents
func (msc *managedStatusComponents) CheckSubcomponents() error {
	msc.mutStatusComponents.Lock()
	defer msc.mutStatusComponents.Unlock()

	if msc.statusComponents == nil {
		return errors.ErrNilStatusComponents
	}
	if check.IfNil(msc.tpsBenchmark) {
		return errors.ErrNilTpsBenchmark
	}
	if check.IfNil(msc.outportHandler) {
		return errors.ErrNilOutportHandler
	}
	if check.IfNil(msc.softwareVersion) {
		return errors.ErrNilSoftwareVersion
	}
	if check.IfNil(msc.statusHandler) {
		return errors.ErrNilStatusHandler
	}

	return nil
}

// SetForkDetector sets the fork detector
func (msc *managedStatusComponents) SetForkDetector(forkDetector process.ForkDetector) {
	msc.mutStatusComponents.Lock()
	msc.statusComponentsFactory.forkDetector = forkDetector
	msc.mutStatusComponents.Unlock()
}

// StartPolling starts polling for the updated status
func (msc *managedStatusComponents) StartPolling() error {
	var ctx context.Context
	msc.mutStatusComponents.Lock()
	ctx, msc.cancelFunc = context.WithCancel(context.Background())
	msc.mutStatusComponents.Unlock()

	err := msc.startStatusPolling(ctx)
	if err != nil {
		return err
	}

	err = msc.startMachineStatisticsPolling(ctx)
	if err != nil {
		return err
	}

	msc.attachEpochGoRoutineAnalyser()

	return nil
}

// TpsBenchmark returns the tps benchmark handler
func (msc *managedStatusComponents) TpsBenchmark() statistics.TPSBenchmark {
	msc.mutStatusComponents.RLock()
	defer msc.mutStatusComponents.RUnlock()

	if msc.statusComponents == nil {
		return nil
	}

	return msc.statusComponents.tpsBenchmark
}

// OutportHandler returns the outport handler
func (msc *managedStatusComponents) OutportHandler() outport.OutportHandler {
	msc.mutStatusComponents.RLock()
	defer msc.mutStatusComponents.RUnlock()

	if msc.statusComponents == nil {
		return nil
	}

	return msc.statusComponents.outportHandler
}

// SoftwareVersionChecker returns the software version checker handler
func (msc *managedStatusComponents) SoftwareVersionChecker() statistics.SoftwareVersionChecker {
	msc.mutStatusComponents.RLock()
	defer msc.mutStatusComponents.RUnlock()

	if msc.statusComponents == nil {
		return nil
	}

	return msc.statusComponents.softwareVersion
}

// IsInterfaceNil returns true if there is no value under the interface
func (msc *managedStatusComponents) IsInterfaceNil() bool {
	return msc == nil
}

func (msc *managedStatusComponents) startStatusPolling(ctx context.Context) error {
	// TODO: inject the context to the AppStatusPolling
	appStatusPollingHandler, err := appStatusPolling.NewAppStatusPolling(
		msc.statusComponentsFactory.coreComponents.StatusHandler(),
		time.Duration(msc.statusComponentsFactory.config.GeneralSettings.StatusPollingIntervalSec)*time.Second,
	)
	if err != nil {
		return errors.ErrStatusPollingInit
	}

	err = registerPollConnectedPeers(appStatusPollingHandler, msc.statusComponentsFactory.networkComponents)
	if err != nil {
		return err
	}

	err = registerPollProbableHighestNonce(appStatusPollingHandler, msc.statusComponentsFactory.forkDetector)
	if err != nil {
		return err
	}

	err = registerShardsInformation(appStatusPollingHandler, msc.statusComponentsFactory.shardCoordinator)
	if err != nil {
		return err
	}

	appStatusPollingHandler.Poll(ctx)

	return nil
}

func registerPollConnectedPeers(
	appStatusPollingHandler *appStatusPolling.AppStatusPolling,
	networkComponents NetworkComponentsHolder,
) error {

	p2pMetricsHandlerFunc := func(appStatusHandler core.AppStatusHandler) {
		computeNumConnectedPeers(appStatusHandler, networkComponents)
		computeConnectedPeers(appStatusHandler, networkComponents)
	}

	err := appStatusPollingHandler.RegisterPollingFunc(p2pMetricsHandlerFunc)
	if err != nil {
		return errors.ErrPollingFunctionRegistration
	}

	return nil
}

func registerShardsInformation(
	appStatusPollingHandler *appStatusPolling.AppStatusPolling,
	coordinator sharding.Coordinator,
) error {

	computeShardsInfo := func(appStatusHandler core.AppStatusHandler) {
		shardId := uint64(coordinator.SelfId())
		numOfShards := uint64(coordinator.NumberOfShards())

		appStatusHandler.SetUInt64Value(core.MetricShardId, shardId)
		appStatusHandler.SetUInt64Value(core.MetricNumShardsWithoutMetachain, numOfShards)
	}

	err := appStatusPollingHandler.RegisterPollingFunc(computeShardsInfo)
	if err != nil {
		return fmt.Errorf("%w, cannot register handler func for shards information", err)
	}

	return nil
}

func computeNumConnectedPeers(
	appStatusHandler core.AppStatusHandler,
	networkComponents NetworkComponentsHolder,
) {
	numOfConnectedPeers := uint64(len(networkComponents.NetworkMessenger().ConnectedAddresses()))
	appStatusHandler.SetUInt64Value(core.MetricNumConnectedPeers, numOfConnectedPeers)
}

func computeConnectedPeers(
	appStatusHandler core.AppStatusHandler,
	networkComponents NetworkComponentsHolder,
) {
	peersInfo := networkComponents.NetworkMessenger().GetConnectedPeersInfo()

	peerClassification := fmt.Sprintf("intraVal:%d,crossVal:%d,intraObs:%d,crossObs:%d,fullObs:%d,unknown:%d,",
		len(peersInfo.IntraShardValidators),
		len(peersInfo.CrossShardValidators),
		len(peersInfo.IntraShardObservers),
		len(peersInfo.CrossShardObservers),
		len(peersInfo.FullHistoryObservers),
		len(peersInfo.UnknownPeers),
	)
	appStatusHandler.SetStringValue(core.MetricNumConnectedPeersClassification, peerClassification)
	appStatusHandler.SetStringValue(core.MetricP2PNumConnectedPeersClassification, peerClassification)

	setP2pConnectedPeersMetrics(appStatusHandler, peersInfo)
	setCurrentP2pNodeAddresses(appStatusHandler, networkComponents)
}

func setP2pConnectedPeersMetrics(appStatusHandler core.AppStatusHandler, info *p2p.ConnectedPeersInfo) {
	appStatusHandler.SetStringValue(core.MetricP2PUnknownPeers, sliceToString(info.UnknownPeers))
	appStatusHandler.SetStringValue(core.MetricP2PIntraShardValidators, mapToString(info.IntraShardValidators))
	appStatusHandler.SetStringValue(core.MetricP2PIntraShardObservers, mapToString(info.IntraShardObservers))
	appStatusHandler.SetStringValue(core.MetricP2PCrossShardValidators, mapToString(info.CrossShardValidators))
	appStatusHandler.SetStringValue(core.MetricP2PCrossShardObservers, mapToString(info.CrossShardObservers))
	appStatusHandler.SetStringValue(core.MetricP2PFullHistoryObservers, mapToString(info.FullHistoryObservers))
}

func sliceToString(input []string) string {
	return strings.Join(input, ",")
}

func mapToString(input map[uint32][]string) string {
	strs := make([]string, 0, len(input))
	keys := make([]uint32, 0, len(input))
	for shard := range input {
		keys = append(keys, shard)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, key := range keys {
		strs = append(strs, sliceToString(input[key]))
	}

	return strings.Join(strs, ",")
}

func setCurrentP2pNodeAddresses(
	appStatusHandler core.AppStatusHandler,
	networkComponents NetworkComponentsHolder,
) {
	appStatusHandler.SetStringValue(core.MetricP2PPeerInfo, sliceToString(networkComponents.NetworkMessenger().Addresses()))
}

func registerPollProbableHighestNonce(
	appStatusPollingHandler *appStatusPolling.AppStatusPolling,
	forkDetector process.ForkDetector,
) error {

	probableHighestNonceHandlerFunc := func(appStatusHandler core.AppStatusHandler) {
		probableHigherNonce := forkDetector.ProbableHighestNonce()
		appStatusHandler.SetUInt64Value(core.MetricProbableHighestNonce, probableHigherNonce)
	}

	err := appStatusPollingHandler.RegisterPollingFunc(probableHighestNonceHandlerFunc)
	if err != nil {
		return fmt.Errorf("%w, cannot register handler func for forkdetector's probable higher nonce", err)
	}

	return nil
}

func (msc *managedStatusComponents) startMachineStatisticsPolling(ctx context.Context) error {
	appStatusPollingHandler, err := appStatusPolling.NewAppStatusPolling(msc.statusComponentsFactory.coreComponents.StatusHandler(), time.Second)
	if err != nil {
		return fmt.Errorf("%w, cannot init AppStatusPolling", err)
	}

	err = registerCpuStatistics(ctx, appStatusPollingHandler)
	if err != nil {
		return err
	}

	err = registerMemStatistics(ctx, appStatusPollingHandler)
	if err != nil {
		return err
	}

	err = registerNetStatistics(ctx, appStatusPollingHandler, msc.statusComponentsFactory.epochStartNotifier)
	if err != nil {
		return err
	}

	appStatusPollingHandler.Poll(ctx)

	return nil
}

func registerMemStatistics(_ context.Context, appStatusPollingHandler *appStatusPolling.AppStatusPolling) error {
	return appStatusPollingHandler.RegisterPollingFunc(func(appStatusHandler core.AppStatusHandler) {
		mem := machine.AcquireMemStatistics()

		appStatusHandler.SetUInt64Value(core.MetricMemLoadPercent, mem.PercentUsed)
		appStatusHandler.SetUInt64Value(core.MetricMemTotal, mem.Total)
		appStatusHandler.SetUInt64Value(core.MetricMemUsedGolang, mem.UsedByGolang)
		appStatusHandler.SetUInt64Value(core.MetricMemUsedSystem, mem.UsedBySystem)
		appStatusHandler.SetUInt64Value(core.MetricMemHeapInUse, mem.HeapInUse)
		appStatusHandler.SetUInt64Value(core.MetricMemStackInUse, mem.StackInUse)
	})
}

func registerNetStatistics(ctx context.Context, appStatusPollingHandler *appStatusPolling.AppStatusPolling, notifier sharding.EpochStartEventNotifier) error {
	netStats := machine.NewNetStatistics()
	notifier.RegisterHandler(netStats.EpochStartEventHandler())
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug("registerNetStatistics go routine is stopping...")
				return
			default:
			}
			netStats.ComputeStatistics()
		}
	}()

	return appStatusPollingHandler.RegisterPollingFunc(func(appStatusHandler core.AppStatusHandler) {
		appStatusHandler.SetUInt64Value(core.MetricNetworkRecvBps, netStats.BpsRecv())
		appStatusHandler.SetUInt64Value(core.MetricNetworkRecvBpsPeak, netStats.BpsRecvPeak())
		appStatusHandler.SetUInt64Value(core.MetricNetworkRecvPercent, netStats.PercentRecv())

		appStatusHandler.SetUInt64Value(core.MetricNetworkSentBps, netStats.BpsSent())
		appStatusHandler.SetUInt64Value(core.MetricNetworkSentBpsPeak, netStats.BpsSentPeak())
		appStatusHandler.SetUInt64Value(core.MetricNetworkSentPercent, netStats.PercentSent())

		appStatusHandler.SetUInt64Value(core.MetricNetworkRecvBytesInCurrentEpochPerHost, netStats.TotalBytesReceivedInCurrentEpoch())
		appStatusHandler.SetUInt64Value(core.MetricNetworkSendBytesInCurrentEpochPerHost, netStats.TotalBytesSentInCurrentEpoch())
	})
}

func registerCpuStatistics(ctx context.Context, appStatusPollingHandler *appStatusPolling.AppStatusPolling) error {
	cpuStats, err := machine.NewCpuStatistics()
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Debug("registerCpuStatistics go routine is stopping...")
				return
			default:
			}
			cpuStats.ComputeStatistics()
		}
	}()

	return appStatusPollingHandler.RegisterPollingFunc(func(appStatusHandler core.AppStatusHandler) {
		appStatusHandler.SetUInt64Value(core.MetricCpuLoadPercent, cpuStats.CpuPercentUsage())
	})
}

// String returns the name of the component
func (msc *managedStatusComponents) String() string {
	return "managedStatusComponents"
}

func (msc *managedStatusComponents) attachEpochGoRoutineAnalyser() {
	currentConfig := msc.statusComponentsFactory.config
	enabledEpochDebug := currentConfig.Debug.EpochStart.GoRoutineAnalyserEnabled
	log.Debug("attachEpochGoRoutineAnalyser", "GoRoutineAnalyserEnabled", enabledEpochDebug)
	if enabledEpochDebug {
		analyser, err := goroutine.NewGoRoutinesAnalyser(goroutine.NewGoRoutinesProcessor())

		if err != nil {
			log.Debug("attachEpochGoRoutineAnalyser", "error", err)
			return
		}

		subscribeHandler := notifier.NewHandlerForEpochStart(func(hdr data.HeaderHandler) {
			analyser.DumpGoRoutinesToLogWithTypes()
		}, func(_ data.HeaderHandler) {}, core.NetStatisticsOrder)

		msc.statusComponentsFactory.epochStartNotifier.RegisterHandler(subscribeHandler)
	}
}
