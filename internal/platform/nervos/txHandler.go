package nervos

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	tokenTypes "block-crawling/internal/types"
	"block-crawling/internal/utils"
	"fmt"
	"time"

	"github.com/nervosnetwork/ckb-sdk-go/address"
	"github.com/nervosnetwork/ckb-sdk-go/types"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txHandler struct {
	chainName   string
	block       *chain.Block
	txByHash    *chain.Transaction
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords      []*data.CkbTransactionRecord
	txHashIndexMap map[string]int
}

func (h *txHandler) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	return h.onTx(c, tx, block.Hash, int(block.Number), block.Time, biz.SUCCESS)
}

func (h *txHandler) onTx(
	c chain.Clienter,
	tx *chain.Transaction,
	blockHash string,
	blockNumber int,
	txTime int64,
	status string,
) (err error) {
	rawTx := tx.Raw.(*txWrapper)
	client := c.(*Client)
	var fromAddr string
	var totalInput uint64
	var matchedToAddresses map[string]string
	if len(rawTx.inputCells) > 0 { // this transaction must be sent by our wallet.
		fromAddr, totalInput, err = h.parseInput(client, rawTx)
	} else {
		matchedToAddresses, err = h.matchToAddresses(rawTx.toAddresses)
		if err != nil {
			return err
		}

		if len(matchedToAddresses) == 0 { // no user matched
			return nil
		}
		fromAddr, totalInput, err = h.parseInput(client, rawTx)
	}
	if err != nil {
		return err
	}

	hashIndex := 0
	feeAmount := fmt.Sprint(totalInput - rawTx.totalOutput)
	fromUid, matchFrom, err := common.MatchAddress(fromAddr, h.chainName)
	for i, toAddr := range rawTx.toAddresses {
		toUid, matchTo := matchedToAddresses[toAddr]
		if !matchTo {
			toUid, matchTo, err = common.MatchAddress(toAddr, h.chainName)
			if err != nil {
				return err
			}
		}
		if !(matchFrom || matchTo) {
			continue
		}

		var txHash string
		hashIndex++
		if hashIndex == 1 {
			txHash = tx.Hash
		} else {
			txHash = tx.Hash + "#result-" + fmt.Sprintf("%v", hashIndex)
		}
		amount, _ := decimal.NewFromString(rawTx.amounts[i])

		txType := rawTx.txTypes[i]
		contractAddr := rawTx.contractAddresses[i]
		var tokenInfo tokenTypes.TokenInfo
		if txType != chain.TxTypeNative {
			tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddr)
			if err != nil {
				log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("txHash", txHash), zap.Any("error", err))
			}

			tokenInfo.Amount = rawTx.amounts[i]
			tokenInfo.Address = contractAddr
		}
		parseData, _ := utils.JsonEncode(map[string]interface{}{
			"cell":  rawTx.outputCells[i],
			"token": tokenInfo,
		})
		tokenInfoStr, _ := utils.JsonEncode(tokenInfo)

		h.txRecords = append(h.txRecords, &data.CkbTransactionRecord{
			BlockHash:       blockHash,
			BlockNumber:     blockNumber,
			TransactionHash: txHash,
			FromAddress:     fromAddr,
			ToAddress:       toAddr,
			FromUid:         fromUid,
			ToUid:           toUid,
			FeeAmount:       decimal.Zero,
			Amount:          amount,
			Status:          status,
			TxTime:          txTime,
			ContractAddress: rawTx.contractAddresses[i],
			ParseData:       parseData,
			ConfirmCount:    int32(h.chainHeight) - int32(h.curHeight),
			EventLog:        "",
			TransactionType: txType,
			TokenInfo:       tokenInfoStr,
			CreatedAt:       h.now,
			UpdatedAt:       h.now,
		})
	}
	if hashIndex > 0 {
		fee, _ := decimal.NewFromString(feeAmount)
		h.txRecords[len(h.txRecords)-1].FeeAmount = fee
	}

	return nil
}

func (h *txHandler) matchToAddresses(addresses []string) (map[string]string, error) {
	addressesMap := make(map[string]bool)
	for _, addr := range addresses {
		addressesMap[addr] = true
	}
	results := make(map[string]string)
	for addr := range addressesMap {
		uid, ok, err := common.MatchAddress(addr, h.chainName)
		if err != nil {
			return nil, err
		}
		if ok {
			results[addr] = uid
		}
	}
	return results, nil
}

func (h *txHandler) parseInput(client *Client, rawTx *txWrapper) (fromAddr string, totalInput uint64, err error) {
	if err := client.fillMissedInputCells(rawTx); err != nil {
		return "", 0, err
	}
	for _, out := range rawTx.prevOutputs {
		cellKey := client.cellKey(out.TxHash.Hex(), int(out.Index))
		cell, ok := rawTx.inputCells[cellKey]
		if !ok {
			return "", 0, fmt.Errorf("missed input cell %s#%d", out.TxHash.Hex(), out.Index)
		}
		if !client.isTokenTransfer(cell.decodeType()) {
			totalInput += cell.Capacity
		}
		fromAddr, err = address.ConvertScriptToAddress(client.mode, cell.decodeLock())
		if err != nil {
			return "", 0, err
		}
	}
	return fromAddr, totalInput, nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	curHeight := h.curHeight
	// client := c.(*Client)

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *c.(*Client), txRecords)
		} else {
			go HandlePendingRecord(h.chainName, *c.(*Client), txRecords)
		}

		if h.newTxs {
			records := make([]interface{}, 0, len(txRecords))
			for _, r := range txRecords {
				records = append(records, r)
			}
			common.SetResultOfTxs(h.block, records)
		} else {
			common.SetTxResult(h.txByHash, txRecords[0])
		}
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) (err error) {
	rawTx := txByHash.Raw.(*txWrapper)
	status := biz.PENDING
	if rawTx.status == types.TransactionStatusCommitted {
		status = biz.SUCCESS
	}

	if rawTx.header == nil {
		record := txByHash.Record.(*data.CkbTransactionRecord)
		record.Status = biz.NO_STATUS
		h.txRecords = append(h.txRecords, record)
		return nil
	}
	return h.onTx(
		c,
		txByHash,
		rawTx.header.Hash.Hex(),
		int(rawTx.header.Number),
		int64(rawTx.header.Timestamp/1000),
		status,
	)
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	record := tx.Record.(*data.CkbTransactionRecord)
	nowTime := time.Now().Unix()
	if record.CreatedAt+300 > nowTime {
		if record.Status == biz.PENDING {
			record.Status = biz.NO_STATUS
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象无状态",
				zap.String("chainName", h.chainName),
				zap.Any("txHash", record.TransactionHash),
				zap.String("nodeUrl", c.URL()),
				zap.Int64("nowTime", nowTime),
				zap.Int64("createTime", record.CreatedAt),
			)
		}
	} else {
		record.Status = biz.DROPPED
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新 PENDING txhash对象为终态:交易被抛弃",
			zap.String("chainName", h.chainName),
			zap.Any("txHash", record.TransactionHash),
			zap.String("nodeUrl", c.URL()),
			zap.Int64("nowTime", nowTime),
			zap.Int64("createTime", record.CreatedAt),
		)
	}
	return nil
}
