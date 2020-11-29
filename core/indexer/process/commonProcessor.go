package process

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/core/indexer/types"
	"github.com/ElrondNetwork/elrond-go/data"
	"github.com/ElrondNetwork/elrond-go/data/block"
	"github.com/ElrondNetwork/elrond-go/data/receipt"
	"github.com/ElrondNetwork/elrond-go/data/rewardTx"
	"github.com/ElrondNetwork/elrond-go/data/smartContractResult"
	"github.com/ElrondNetwork/elrond-go/data/transaction"
)

type commonProcessor struct {
	esdtProc                 *esdtTransactionProcessor
	addressPubkeyConverter   core.PubkeyConverter
	validatorPubkeyConverter core.PubkeyConverter
	minGasLimit              uint64
	gasPerDataByte           uint64
}

func (cm *commonProcessor) buildTransaction(
	tx *transaction.Transaction,
	txHash []byte,
	mbHash []byte,
	mb *block.MiniBlock,
	header data.HeaderHandler,
	txStatus string,
) *types.Transaction {
	gasUsed := cm.minGasLimit + uint64(len(tx.Data))*cm.gasPerDataByte

	var tokenIdentifier, esdtValue string
	if isESDTTx := cm.esdtProc.isESDTTx(tx); isESDTTx {
		tokenIdentifier, esdtValue = cm.esdtProc.getTokenIdentifierAndValue(tx)
	}

	return &types.Transaction{
		Hash:                hex.EncodeToString(txHash),
		MBHash:              hex.EncodeToString(mbHash),
		Nonce:               tx.Nonce,
		Round:               header.GetRound(),
		Value:               tx.Value.String(),
		Receiver:            cm.addressPubkeyConverter.Encode(tx.RcvAddr),
		Sender:              cm.addressPubkeyConverter.Encode(tx.SndAddr),
		ReceiverShard:       mb.ReceiverShardID,
		SenderShard:         mb.SenderShardID,
		GasPrice:            tx.GasPrice,
		GasLimit:            tx.GasLimit,
		Data:                tx.Data,
		Signature:           hex.EncodeToString(tx.Signature),
		Timestamp:           time.Duration(header.GetTimeStamp()),
		Status:              txStatus,
		GasUsed:             gasUsed,
		EsdtTokenIdentifier: tokenIdentifier,
		EsdtValue:           esdtValue,
	}
}

func (cm *commonProcessor) buildRewardTransaction(
	rTx *rewardTx.RewardTx,
	txHash []byte,
	mbHash []byte,
	mb *block.MiniBlock,
	header data.HeaderHandler,
	txStatus string,
) *types.Transaction {
	return &types.Transaction{
		Hash:          hex.EncodeToString(txHash),
		MBHash:        hex.EncodeToString(mbHash),
		Nonce:         0,
		Round:         rTx.Round,
		Value:         rTx.Value.String(),
		Receiver:      cm.addressPubkeyConverter.Encode(rTx.RcvAddr),
		Sender:        fmt.Sprintf("%d", core.MetachainShardId),
		ReceiverShard: mb.ReceiverShardID,
		SenderShard:   mb.SenderShardID,
		GasPrice:      0,
		GasLimit:      0,
		Data:          make([]byte, 0),
		Signature:     "",
		Timestamp:     time.Duration(header.GetTimeStamp()),
		Status:        txStatus,
	}
}

func (cm *commonProcessor) convertScResultInDatabaseScr(scHash string, sc *smartContractResult.SmartContractResult) *types.ScResult {
	relayerAddr := ""
	if len(sc.RelayerAddr) > 0 {
		relayerAddr = cm.addressPubkeyConverter.Encode(sc.RelayerAddr)
	}

	var tokenIdentifier, esdtValue string
	if isESDTTx := cm.esdtProc.isESDTTx(sc); isESDTTx {
		tokenIdentifier, esdtValue = cm.esdtProc.getTokenIdentifierAndValue(sc)
	}

	return &types.ScResult{
		Hash:                hex.EncodeToString([]byte(scHash)),
		Nonce:               sc.Nonce,
		GasLimit:            sc.GasLimit,
		GasPrice:            sc.GasPrice,
		Value:               sc.Value.String(),
		Sender:              cm.addressPubkeyConverter.Encode(sc.SndAddr),
		Receiver:            cm.addressPubkeyConverter.Encode(sc.RcvAddr),
		RelayerAddr:         relayerAddr,
		RelayedValue:        sc.RelayedValue.String(),
		Code:                string(sc.Code),
		Data:                sc.Data,
		PreTxHash:           hex.EncodeToString(sc.PrevTxHash),
		OriginalTxHash:      hex.EncodeToString(sc.OriginalTxHash),
		CallType:            strconv.Itoa(int(sc.CallType)),
		CodeMetadata:        sc.CodeMetadata,
		ReturnMessage:       string(sc.ReturnMessage),
		EsdtTokenIdentifier: tokenIdentifier,
		EsdtValue:           esdtValue,
	}
}

func (cm *commonProcessor) convertReceiptInDatabaseReceipt(
	recHash string,
	rec *receipt.Receipt,
	header data.HeaderHandler,
) *types.Receipt {
	return &types.Receipt{
		Hash:      hex.EncodeToString([]byte(recHash)),
		Value:     rec.Value.String(),
		Sender:    cm.addressPubkeyConverter.Encode(rec.SndAddr),
		Data:      string(rec.Data),
		TxHash:    hex.EncodeToString(rec.TxHash),
		Timestamp: time.Duration(header.GetTimeStamp()),
	}
}
