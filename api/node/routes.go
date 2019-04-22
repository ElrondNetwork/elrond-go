package node

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/ElrondNetwork/elrond-go-sandbox/api/errors"
	"github.com/ElrondNetwork/elrond-go-sandbox/core/statistics"
	"github.com/gin-gonic/gin"
)

// Handler interface defines methods that can be used from `elrondFacade` context variable
type Handler interface {
	IsNodeRunning() bool
	StartNode() error
	StopNode() error
	GetCurrentPublicKey() string
	TpsBenchmark() *statistics.TpsBenchmark
}

type statisticsResponse struct {
	LiveTPS float32 `json:"liveTPS"`
	PeakTPS float32 `json:"peakTPS"`
	NrOfShards uint32 `json:"nrOfShards"`
	BlockNumber uint64 `json:"blockNumber"`
	RoundTime uint32 `json:"roundTime"`
	AverageBlockTxCount float32 `json:"averageBlockTxCount"`
	LastBlockTxCount uint32 `json:"lastBlockTxCount"`
	TotalProcessedTxCount uint32 `json:"totalProcessedTxCount"`
	ShardStatistics []shardStatisticsResponse `json:"shardStatistics"`
}

type shardStatisticsResponse struct {
	ShardID uint32 `json:"shardID"`
	LiveTPS float32 `json:"liveTPS"`
	AverageTPS float32 `json:"averageTPS"`
	PeakTPS float32 `json:"peakTPS"`
	AverageBlockTxCount uint32 `json:"averageBlockTxCount"`
	CurrentBlockNonce uint64 `json:"currentBlockNonce"`
	LastBlockTxCount uint32 `json:"lastBlockTxCount"`
	TotalProcessedTxCount uint32 `json:"totalProcessedTxCount"`
}

// Routes defines node related routes
func Routes(router *gin.RouterGroup) {
	router.GET("/start", StartNode)
	router.GET("/status", Status)
	router.GET("/stop", StopNode)
	router.GET("/address", Address)
	router.GET("/statistics", Statistics)
}

// Status returns the state of the node e.g. running/stopped
func Status(c *gin.Context) {
	ef, ok := c.MustGet("elrondFacade").(Handler)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errors.ErrInvalidAppContext.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "ok", "running": ef.IsNodeRunning()})
}

// StartNode will start the node instance
func StartNode(c *gin.Context) {
	ef, ok := c.MustGet("elrondFacade").(Handler)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errors.ErrInvalidAppContext.Error()})
		return
	}

	if ef.IsNodeRunning() {
		c.JSON(http.StatusOK, gin.H{"message": errors.ErrNodeAlreadyRunning.Error()})
		return
	}

	err := ef.StartNode()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("%s: %s", errors.ErrBadInitOfNode.Error(), err.Error())})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

// Address returns the information about the address passed as parameter
func Address(c *gin.Context) {
	ef, ok := c.MustGet("elrondFacade").(Handler)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errors.ErrInvalidAppContext.Error()})
		return
	}

	currentAddress := ef.GetCurrentPublicKey()
	address, err := url.Parse(currentAddress)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errors.ErrCouldNotParsePubKey.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"address": address.String()})
}

// StopNode will stop the node instance
func StopNode(c *gin.Context) {
	ef, ok := c.MustGet("elrondFacade").(Handler)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errors.ErrInvalidAppContext.Error()})
		return
	}

	if !ef.IsNodeRunning() {
		c.JSON(http.StatusOK, gin.H{"message": errors.ErrNodeAlreadyStopped.Error()})
		return
	}

	err := ef.StopNode()
	if err != nil && ef.IsNodeRunning() {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("%s: %s", errors.ErrCouldNotStopNode.Error(), err.Error())})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

// Statistics returns the blockchain statistics
func Statistics(c *gin.Context) {
	ef, ok := c.MustGet("elrondFacade").(Handler)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errors.ErrInvalidAppContext.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"statistics": statsFromTpsBenchmark(ef.TpsBenchmark())})
}

func statsFromTpsBenchmark(tpsBenchmark *statistics.TpsBenchmark) statisticsResponse {
	sr := statisticsResponse{}
	sr.LiveTPS = tpsBenchmark.LiveTPS()
	sr.PeakTPS = tpsBenchmark.PeakTPS()
	sr.NrOfShards = tpsBenchmark.NrOfShards()
	sr.RoundTime = tpsBenchmark.RoundTime()
	sr.BlockNumber = tpsBenchmark.BlockNumber()
	sr.AverageBlockTxCount = tpsBenchmark.AverageBlockTxCount()
	sr.LastBlockTxCount = tpsBenchmark.LastBlockTxCount()
	sr.TotalProcessedTxCount = tpsBenchmark.TotalProcessedTxCount()
	sr.ShardStatistics = make([]shardStatisticsResponse, tpsBenchmark.NrOfShards())

	for i := 0; i < int(tpsBenchmark.NrOfShards()); i++ {
		ss := tpsBenchmark.ShardStatistic(uint32(i))
		sr.ShardStatistics[i] = shardStatisticsResponse{
			ShardID: ss.ShardID(),
			LiveTPS: ss.LiveTPS(),
			PeakTPS: ss.PeakTPS(),
			AverageTPS: ss.AverageTPS(),
			AverageBlockTxCount: ss.AverageBlockTxCount(),
			CurrentBlockNonce: ss.CurrentBlockNonce(),
			LastBlockTxCount: ss.LastBlockTxCount(),
			TotalProcessedTxCount: ss.TotalProcessedTxCount(),
		}
	}

	return sr
}