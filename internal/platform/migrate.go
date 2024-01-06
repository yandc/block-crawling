package platform

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/platform/kaspa"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/platform/sui/swap"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func MigrateRecord() {
	source := biz.AppConfig.Source
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}
	var tbTransactionRecords []*data.TBTransactionRecord
	ret := dbSource.Find(&tbTransactionRecords)
	err2 := ret.Error
	if err2 != nil {
		fmt.Printf("init, err: %v\n", err)
		log.Errore("init", err)
	}
	log.Info("同步元数据", zap.Any("size", len(tbTransactionRecords)))
	count := 0
	for i, record := range tbTransactionRecords {

		if record.TxTime == 0 {
			record.TxTime = record.CreatedAt.Unix()
		}

		log.Info("index", zap.Any("同步元表数据", record), zap.Any("index", i))
		fmt.Println("index" + record.TransactionHash)
		if record.ChainName == "Avalanche" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "BSC" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "ETH" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "Fantom" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "HECO" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "Klaytn" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "OEC" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "Optimism" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "Polygon" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}
		if record.ChainName == "Cronos" {
			var txRecords []*data.EvmTransactionRecord
			txRecords = append(txRecords, initEvmModel(record))
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, strings.ToLower(record.ChainName)+biz.TABLE_POSTFIX, txRecords)
			count++
			continue
		}

		bn, _ := strconv.Atoi(record.BlockNumber)
		fu := ""
		tu := ""
		flag, fromUid, _ := biz.UserAddressSwitchRetryAlert(record.ChainName, record.FromObj)
		if flag {
			fu = fromUid
		}
		flag1, toUid, _ := biz.UserAddressSwitchRetryAlert(record.ChainName, record.ToObj)
		if flag1 {
			tu = toUid
		}
		fa, _ := decimal.NewFromString(record.FeeAmount)
		am, _ := decimal.NewFromString(record.Amount)

		if record.ChainName == "STC" {

			feeData := make(map[string]string)

			gaslimit := ""
			gasUsed := ""
			gasPrice := ""
			if jsonErr := json.Unmarshal([]byte(record.FeeData), &feeData); jsonErr == nil {
				gaslimit = feeData["gas_limit"]
				gasUsed = feeData["gas_used"]
				gasPrice = feeData["gas_price"]
			}

			stcRecord := &data.StcTransactionRecord{
				BlockHash:       record.BlockHash,
				BlockNumber:     bn,
				TransactionHash: record.TransactionHash,
				FromAddress:     record.FromObj,
				ToAddress:       record.ToObj,
				FromUid:         fu,
				ToUid:           tu,
				FeeAmount:       fa,
				Amount:          am,
				Status:          record.Status,
				TxTime:          record.TxTime,
				ContractAddress: record.ContractAddress,
				ParseData:       record.ParseData,
				GasLimit:        gaslimit,
				GasUsed:         gasUsed,
				GasPrice:        gasPrice,
				Data:            record.Data,
				EventLog:        record.EventLog,
				TransactionType: record.TransactionType,
				DappData:        record.DappData,
				ClientData:      record.ClientData,
				CreatedAt:       record.CreatedAt.Unix(),
				UpdatedAt:       record.UpdatedAt.Unix(),
			}

			var stc []*data.StcTransactionRecord
			stc = append(stc, stcRecord)
			data.StcTransactionRecordRepoClient.BatchSaveOrUpdate(nil, "stc_transaction_record", stc)
			count++

			continue
		}
		if record.ChainName == "TRX" {

			feeData := make(map[string]interface{})

			trxRecord := &data.TrxTransactionRecord{
				BlockHash:       record.BlockHash,
				BlockNumber:     bn,
				TransactionHash: record.TransactionHash,
				FromAddress:     record.FromObj,
				ToAddress:       record.ToObj,
				FromUid:         fu,
				ToUid:           tu,
				FeeAmount:       fa,
				Amount:          am,
				Status:          record.Status,
				TxTime:          record.TxTime,
				ContractAddress: record.ContractAddress,
				ParseData:       record.ParseData,
				TransactionType: record.TransactionType,
				DappData:        record.DappData,
				ClientData:      record.ClientData,
				CreatedAt:       record.CreatedAt.Unix(),
				UpdatedAt:       record.UpdatedAt.Unix(),
			}

			if jsonErr := json.Unmarshal([]byte(record.FeeData), &feeData); jsonErr == nil {
				fd := feeData["net_usage"]
				if fd != nil {
					netUsage := fd.(float64)
					trxRecord.NetUsage = strconv.FormatFloat(netUsage, 'f', 0, 64)
				}
				fe := feeData["fee_limit"]
				if fe != nil {
					feeLimit := fe.(float64)
					trxRecord.FeeLimit = strconv.FormatFloat(feeLimit, 'f', 0, 64)
				}
				eu := feeData["energy_usage"]
				if eu != nil {
					energyUsage := eu.(float64)
					trxRecord.EnergyUsage = strconv.FormatFloat(energyUsage, 'f', 0, 64)
				}
			}

			var trx []*data.TrxTransactionRecord
			trx = append(trx, trxRecord)
			data.TrxTransactionRecordRepoClient.BatchSaveOrUpdate(nil, "trx_transaction_record", trx)
			count++

			continue
		}
		p1 := decimal.NewFromInt(100000000)
		if record.ChainName == "BTC" || record.ChainName == "LTC" || record.ChainName == "DOGE" {
			btcTransactionRecord := &data.BtcTransactionRecord{
				BlockHash:       record.BlockHash,
				BlockNumber:     bn,
				TransactionHash: record.TransactionHash,
				FromAddress:     record.FromObj,
				ToAddress:       record.ToObj,
				FromUid:         fu,
				ToUid:           tu,
				FeeAmount:       fa.Mul(p1),
				Amount:          am.Mul(p1),
				Status:          record.Status,
				TxTime:          record.TxTime,
				ConfirmCount:    6,
				DappData:        record.DappData,
				ClientData:      record.ClientData,
				CreatedAt:       record.CreatedAt.Unix(),
				UpdatedAt:       record.UpdatedAt.Unix(),
			}

			var btc []*data.BtcTransactionRecord
			btc = append(btc, btcTransactionRecord)
			data.BtcTransactionRecordRepoClient.BatchSaveOrUpdate(nil, biz.GetTableName(record.ChainName), btc)

			count++

			continue
		}

	}

	log.Info("同步数据完成！", zap.Any("共同步数据", count))

}

func initEvmModel(record *data.TBTransactionRecord) *data.EvmTransactionRecord {
	bn, _ := strconv.Atoi(record.BlockNumber)
	nonceStr := ""
	paseJson := make(map[string]interface{})
	if jsonErr := json.Unmarshal([]byte(record.ParseData), &paseJson); jsonErr == nil {
		evmMap := paseJson["evm"]
		if evmMap != nil {
			ret := evmMap.(map[string]interface{})
			ne := ret["nonce"]
			if ne != nil {
				nonceStr = ne.(string)
			}
		}
	}

	feeData := make(map[string]string)

	gaslimit := ""
	gasUsed := ""
	gasPrice := ""
	baseFee := ""
	maxFeePerGas := ""
	maxPriorityFeePerGas := ""
	if jsonErr := json.Unmarshal([]byte(record.FeeData), &feeData); jsonErr == nil {
		gaslimit = feeData["gas_limit"]
		gasUsed = feeData["gas_used"]
		gasPrice = feeData["gas_price"]
		baseFee = feeData["base_fee"]
		maxFeePerGas = feeData["max_fee_per_gas"]
		maxPriorityFeePerGas = feeData["max_priority_fee_per_gas"]

	}

	nc, _ := strconv.Atoi(nonceStr)
	fa, _ := decimal.NewFromString(record.FeeAmount)
	am, _ := decimal.NewFromString(record.Amount)

	fu := ""
	tu := ""
	flag, fromUid, _ := biz.UserAddressSwitchRetryAlert(record.ChainName, record.FromObj)
	if flag {
		fu = fromUid
	}
	flag1, toUid, _ := biz.UserAddressSwitchRetryAlert(record.ChainName, record.ToObj)
	if flag1 {
		tu = toUid
	}

	return &data.EvmTransactionRecord{
		BlockHash:            record.BlockHash,
		BlockNumber:          bn,
		Nonce:                int64(nc),
		TransactionHash:      record.TransactionHash,
		FromAddress:          record.FromObj,
		ToAddress:            record.ToObj,
		FeeAmount:            fa,
		Amount:               am,
		Status:               record.Status,
		TxTime:               record.TxTime,
		ContractAddress:      record.ContractAddress,
		ParseData:            record.ParseData,
		Type:                 "",
		FromUid:              fu,
		ToUid:                tu,
		GasLimit:             gaslimit,
		GasUsed:              gasUsed,
		GasPrice:             gasPrice,
		BaseFee:              baseFee,
		MaxFeePerGas:         maxFeePerGas,
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
		Data:                 record.Data,
		EventLog:             record.EventLog,
		TransactionType:      record.TransactionType,
		DappData:             record.DappData,
		ClientData:           record.ClientData,
		CreatedAt:            record.CreatedAt.Unix(),
		UpdatedAt:            record.UpdatedAt.Unix(),
	}
}
func DappReset() {
	for key, platform := range biz.GetChainPlatInfoMap() {
		switch platform.Type {

		case biz.EVM:
			rs, e := data.EvmTransactionRecordRepoClient.ListByTransactionType(nil, biz.GetTableName(key), "approve")
			if e == nil && len(rs) > 0 {

				biz.DappApproveFilter(key, rs)
			}
		}
	}
}

func CheckNonce() {
	for key, platform := range biz.GetChainPlatInfoMap() {
		switch platform.Type {

		case biz.EVM:
			rs, e := data.EvmTransactionRecordRepoClient.FindFromAddress(nil, biz.GetTableName(key))
			if e == nil && len(rs) > 0 {
				total := 0
				for _, address := range rs {
					total = total + 1
					nonceKey := biz.ADDRESS_DONE_NONCE + key + ":" + address
					nonceStr, _ := data.RedisClient.Get(nonceKey).Result()
					nonce, _ := strconv.Atoi(nonceStr)
					n, _ := data.EvmTransactionRecordRepoClient.FindLastNonceByAddress(nil, biz.GetTableName(key), address)
					if n != int64(nonce) {
						log.Info("noncecheck", zap.Any("地址", address), zap.Any("DBnonce", n), zap.Any("redisnonce", nonce), zap.Any("chainName", key))
					}
					if total%100 == 0 {
						log.Info("noncecheck-1", zap.Any("地址", address), zap.Any("DBnonce", n), zap.Any("redisnonce", nonce))
					}

				}

			}
		}
	}
}

func BtcReset() {
	for key, platform := range biz.GetChainPlatInfoMap() {
		switch platform.Type {

		case biz.BTC:
			rs, e := data.BtcTransactionRecordRepoClient.ListAll(nil, biz.GetTableName(key))
			if e == nil && len(rs) > 0 {
				client := bitcoin.NewClient(platform.RpcURL[0], key)
				bitcoin.UnspentTx(key, client, rs)
			}
		}
	}
}

func DeleteRecordData() {
	log.Info("清除非平台用户的交易记录数据开始")
	source := biz.AppConfig.Target
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}
	limit := data.MAX_PAGE_SIZE

	var total int
	for chainName, chainType := range biz.GetChainNameTypeMap() {
		if !biz.IsTestNet(chainName) && chainType == "EVM" {
			log.Info("清除非平台用户的交易记录数据中", zap.Any("chainName", chainName))
			tableName := biz.GetTableName(chainName)
			contractRecordList, err := getTxRecords(dbSource, tableName, "contract", limit)
			if err != nil {
				log.Error("清除非平台用户的交易记录数据, migrate page query contract txRecord failed", zap.Any("chainName", chainName), zap.Any("error", err))
				return
			}
			if len(contractRecordList) == 0 {
				continue
			}
			eventLogRecordList, err := getTxRecords(dbSource, tableName, "eventLog", limit)
			if err != nil {
				log.Error("清除非平台用户的交易记录数据, migrate page query eventLog txRecord failed", zap.Any("tableName", tableName), zap.Any("error", err))
				return
			}

			var txRecords []*data.EvmTransactionRecord
			if len(eventLogRecordList) == 0 {
				txRecords = contractRecordList
			} else {
				eventLogMap := make(map[string]bool)
				for _, record := range eventLogRecordList {
					txHash := strings.Split(record.TransactionHash, "#")[0]
					eventLogMap[txHash] = true
				}
				for _, record := range contractRecordList {
					txHash := record.TransactionHash
					if !eventLogMap[txHash] {
						txRecords = append(txRecords, record)
					}
				}
			}

			size := len(txRecords)
			total += size
			for i, record := range txRecords {
				transactionRequest := &data.TransactionRequest{
					Nonce:               -1,
					TransactionHashLike: record.TransactionHash,
				}
				count, err := data.EvmTransactionRecordRepoClient.Delete(nil, tableName, transactionRequest)
				if err != nil {
					log.Error("清除非平台用户的交易记录数据, delete txRecord failed", zap.Any("tableName", tableName), zap.Any("affected count", count), zap.Any("error", err))
					return
				}
				if i%50 == 0 {
					time.Sleep(1 * time.Second)
				}
			}
			log.Info("清除非平台用户的交易记录数据完成", zap.Any("chainName", chainName), zap.Any("query contract size", size))
		}
	}
	log.Info("清除非平台用户的交易记录数据结束", zap.Any("query contract size", total))
}

func getTxRecords(dbSource *gorm.DB, tableName, transactionType string, limit int) ([]*data.EvmTransactionRecord, error) {
	var txRecords []*data.EvmTransactionRecord
	var txRecordList []*data.EvmTransactionRecord
	id := 0
	total := limit
	for total == limit {
		var sqlStr string
		if transactionType == "contract" {
			sqlStr = getSqlContract(tableName, id, limit)
		} else if transactionType == "eventLog" {
			sqlStr = getSqlEventLog(tableName, id, limit)
		} else {
			return nil, nil
		}

		ret := dbSource.Raw(sqlStr).Find(&txRecordList)
		err := ret.Error
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			ret = dbSource.Raw(sqlStr).Find(&txRecordList)
			err = ret.Error
		}
		if err != nil {
			log.Error("migrate page query txRecord failed", zap.Any("error", err))
			return nil, err
		}
		total = len(txRecordList)
		if total > 0 {
			txRecord := txRecordList[total-1]
			id = int(txRecord.Id)
			txRecords = append(txRecords, txRecordList...)
		}
	}
	return txRecords, nil
}

func getSqlContract(tableName string, id, limit int) string {
	s := "SELECT id, transaction_hash, parse_data, event_log from " + tableName +
		" where from_uid = '' and to_uid = '' and transaction_type = 'contract' " +
		"and id > " + strconv.Itoa(id) + " order by id asc limit " + strconv.Itoa(limit) + ";"
	return s
}

func getSqlEventLog(tableName string, id, limit int) string {
	s := "SELECT id, transaction_hash, parse_data, event_log from " + tableName +
		" where (from_uid != '' or to_uid != '') and transaction_type = 'eventLog' " +
		"and id > " + strconv.Itoa(id) + " order by id asc limit " + strconv.Itoa(limit) + ";"
	return s
}

type TokenParam struct {
	TokenAddress string `json:"tokenAddress"`
	OldDecimals  int    `json:"oldDecimals"`
	OldSymbol    string `json:"oldSymbol"`
	NewDecimals  int    `json:"newDecimals"`
	NewSymbol    string `json:"newSymbol"`
}

type TxRecord struct {
	Id        int64  `json:"id" form:"id" gorm:"primary_key;AUTO_INCREMENT"`
	ParseData string `json:"parseData" form:"parseData"`
	EventLog  string `json:"eventLog" form:"eventLog"`
}

func HandleTokenInfo() {
	tokenParamMap := make(map[string][]*TokenParam)

	tokenParamMap["BSC"] = []*TokenParam{{
		TokenAddress: "0x7eefb6aeb8bc2c1ba6be1d4273ec0758a1321272",
		OldDecimals:  0,
		OldSymbol:    "ENG",
		NewDecimals:  18,
		NewSymbol:    "ENG",
	}}

	tokenParamMap["Fantom"] = []*TokenParam{{
		TokenAddress: "0xe2d27f06f63d98b8e11b38b5b08a75d0c8dd62b9",
		OldDecimals:  18,
		OldSymbol:    "USTC",
		NewDecimals:  6,
		NewSymbol:    "USTC",
	}, {
		TokenAddress: "0x95dd59343a893637be1c3228060ee6afbf6f0730",
		OldDecimals:  18,
		OldSymbol:    "LUNC",
		NewDecimals:  6,
		NewSymbol:    "LUNC",
	}}

	tokenParamMap["Avalanche"] = []*TokenParam{{
		TokenAddress: "0x44a45a9baeb63c6ea4860ecf9ac5732c330c4d4e",
		OldDecimals:  8,
		OldSymbol:    "MEAD",
		NewDecimals:  9,
		NewSymbol:    "MEAD",
	}, {
		TokenAddress: "0x50c72103940d419fb64448f258f7eabba784f84b",
		OldDecimals:  16,
		OldSymbol:    "DLQ",
		NewDecimals:  18,
		NewSymbol:    "DLQ",
	}}

	tokenParamMap["Polygon"] = []*TokenParam{{
		TokenAddress: "0xabede05598760e399ed7eb24900b30c51788f00f",
		OldDecimals:  8,
		OldSymbol:    "SWP",
		NewDecimals:  18,
		NewSymbol:    "SWP",
	}, {
		TokenAddress: "0x9246a5f10a79a5a939b0c2a75a3ad196aafdb43b",
		OldDecimals:  1,
		OldSymbol:    "BETS",
		NewDecimals:  18,
		NewSymbol:    "BETS",
	}, {
		TokenAddress: "0x91c5a5488c0decde1eacd8a4f10e0942fb925067",
		OldDecimals:  17,
		OldSymbol:    "AUDT",
		NewDecimals:  18,
		NewSymbol:    "AUDT",
	}, {
		TokenAddress: "0xf480f38c366daac4305dc484b2ad7a496ff00cea",
		OldDecimals:  15,
		OldSymbol:    "GTON",
		NewDecimals:  18,
		NewSymbol:    "GTON",
	}, {
		TokenAddress: "0x561f7ae92a00c73dc48282d384e0a4fc1f4bc305",
		OldDecimals:  21,
		OldSymbol:    "REB",
		NewDecimals:  18,
		NewSymbol:    "REB",
	}}

	tokenParamMap["Optimism"] = []*TokenParam{{
		TokenAddress: "0xcdb4bb51801a1f399d4402c61bc098a72c382e65",
		OldDecimals:  0,
		OldSymbol:    "OPX",
		NewDecimals:  18,
		NewSymbol:    "OPX",
	}, {
		TokenAddress: "0x2513486f18eee1498d7b6281f668b955181dd0d9",
		OldDecimals:  0,
		OldSymbol:    "XOPENX",
		NewDecimals:  18,
		NewSymbol:    "XOPENX",
	}, {
		TokenAddress: "0xe4de4b87345815c71aa843ea4841bcdc682637bb",
		OldDecimals:  0,
		OldSymbol:    "BUILD",
		NewDecimals:  18,
		NewSymbol:    "BUILD",
	}, {
		TokenAddress: "0x46f21fda29f1339e0ab543763ff683d399e393ec",
		OldDecimals:  0,
		OldSymbol:    "OPXVEVELO",
		NewDecimals:  18,
		NewSymbol:    "OPXVEVELO",
	}, {
		TokenAddress: "0x3e7ef8f50246f725885102e8238cbba33f276747",
		OldDecimals:  17,
		OldSymbol:    "BOND",
		NewDecimals:  18,
		NewSymbol:    "BOND",
	}, {
		TokenAddress: "0x3e29d3a9316dab217754d13b28646b76607c5f04",
		OldDecimals:  0,
		OldSymbol:    "ALETH",
		NewDecimals:  18,
		NewSymbol:    "ALETH",
	}, {
		TokenAddress: "0x9e841f05dc2b92199f3fce96bdefc36a08dcb9f6",
		OldDecimals:  0,
		OldSymbol:    "OPP",
		NewDecimals:  18,
		NewSymbol:    "OPP",
	}}

	tokenParamMap["TRX"] = []*TokenParam{{
		TokenAddress: "TQCMqRAeSgzZTwB3ivHFgkdyGmLHPKw1WQ",
		OldDecimals:  0,
		OldSymbol:    "GGCM",
		NewDecimals:  18,
		NewSymbol:    "GGCM",
	}, {
		TokenAddress: "TUsmP4tY54MB7HCo2D6WA5NaYrZDUtEUna",
		OldDecimals:  0,
		OldSymbol:    "X7C",
		NewDecimals:  14,
		NewSymbol:    "X7C",
	}, {
		TokenAddress: "TCnctCyuXCBrZXGA8Q9uQtNeVZHxEyTZfw",
		OldDecimals:  0,
		OldSymbol:    "INTD",
		NewDecimals:  18,
		NewSymbol:    "INTD",
	}, {
		TokenAddress: "TD1zWDso8wc3eDhQvwH7wvRrSj8K8pEMPG",
		OldDecimals:  0,
		OldSymbol:    "MSCT",
		NewDecimals:  18,
		NewSymbol:    "MSCT",
	}, {
		TokenAddress: "TDXyBerEqKHrGLEQFH9S3prXHPQiqaDsTA",
		OldDecimals:  0,
		OldSymbol:    "GOLC",
		NewDecimals:  18,
		NewSymbol:    "GOLC",
	}, {
		TokenAddress: "TGEKpC1V52jc3Qs2ptV8PcFZAGKTa5Z9GD",
		OldDecimals:  0,
		OldSymbol:    "VNDT",
		NewDecimals:  8,
		NewSymbol:    "VNDT",
	}, {
		TokenAddress: "TVGiaML3hJE7sv9NEEVjqLbF5DcXJgHSfy",
		OldDecimals:  0,
		OldSymbol:    "SFI",
		NewDecimals:  6,
		NewSymbol:    "SFI",
	}, {
		TokenAddress: "TJr7496gExoz1rSmYaznANXZ8hsj1LSXC6",
		OldDecimals:  0,
		OldSymbol:    "HLM",
		NewDecimals:  3,
		NewSymbol:    "HLM",
	}, {
		TokenAddress: "TBqsNXUtqaLptVK8AYvdPPctpqd8oBYWUC",
		OldDecimals:  0,
		OldSymbol:    "TCNH",
		NewDecimals:  18,
		NewSymbol:    "TCNH",
	}, {
		TokenAddress: "TVxA6SiicW236YAQSELRxth52Tp9ypxkr2",
		OldDecimals:  0,
		OldSymbol:    "ET",
		NewDecimals:  8,
		NewSymbol:    "ET",
	}, {
		TokenAddress: "THafzHvgjj17rTXr3DA2d6VyoUy825jQVx",
		OldDecimals:  0,
		OldSymbol:    "LKT",
		NewDecimals:  6,
		NewSymbol:    "LKT",
	}, {
		TokenAddress: "TR6jDdZEXMU38bBUESFnFHLbaXuAWJaPSR",
		OldDecimals:  0,
		OldSymbol:    "METAF",
		NewDecimals:  6,
		NewSymbol:    "METAF",
	}}

	tokenParamMap["Cronos"] = []*TokenParam{{
		TokenAddress: "0xb888d8dd1733d72681b30c00ee76bde93ae7aa93",
		OldDecimals:  0,
		OldSymbol:    "ATOM",
		NewDecimals:  6,
		NewSymbol:    "ATOM",
	}, {
		TokenAddress: "0x0224010ba2d567ffa014222ed960d1fa43b8c8e1",
		OldDecimals:  0,
		OldSymbol:    "MTD",
		NewDecimals:  18,
		NewSymbol:    "MTD",
	}, {
		TokenAddress: "0x9315054f01bf8c13ee67c8498af09a1933cbf24c",
		OldDecimals:  0,
		OldSymbol:    "XCRX",
		NewDecimals:  18,
		NewSymbol:    "XCRX",
	}, {
		TokenAddress: "0xe6bf0e14e33a469f2b0640b53949a9f90e675135",
		OldDecimals:  0,
		OldSymbol:    "GHC",
		NewDecimals:  18,
		NewSymbol:    "GHC",
	}, {
		TokenAddress: "0x3df064069ba2c8b395592e7834934dbc48bbb955",
		OldDecimals:  0,
		OldSymbol:    "CRONO",
		NewDecimals:  18,
		NewSymbol:    "CRONO",
	}, {
		TokenAddress: "0x765277eebeca2e31912c9946eae1021199b39c61",
		OldDecimals:  0,
		OldSymbol:    "WAVAX",
		NewDecimals:  18,
		NewSymbol:    "WAVAX",
	}, {
		TokenAddress: "0xcc57f84637b441127f2f74905b9d99821b47b20c",
		OldDecimals:  0,
		OldSymbol:    "DNA",
		NewDecimals:  18,
		NewSymbol:    "DNA",
	}, {
		TokenAddress: "0xed34211cdd2cf76c3ccee162761a72d7b6601e2b",
		OldDecimals:  0,
		OldSymbol:    "LOOT",
		NewDecimals:  18,
		NewSymbol:    "LOOT",
	}, {
		TokenAddress: "0x8efbaa6080412d7832025b03b9239d0be1e2aa3b",
		OldDecimals:  0,
		OldSymbol:    "PES",
		NewDecimals:  18,
		NewSymbol:    "PES",
	}, {
		TokenAddress: "0x387c06f6e8d1f65839fcb9171171abb5f49f8b20",
		OldDecimals:  0,
		OldSymbol:    "UNICORN",
		NewDecimals:  18,
		NewSymbol:    "UNICORN",
	}, {
		TokenAddress: "0x245a551ee0f55005e510b239c917fa34b41b3461",
		OldDecimals:  0,
		OldSymbol:    "SWAPP",
		NewDecimals:  18,
		NewSymbol:    "SWAPP",
	}, {
		TokenAddress: "0xb09fe1613fe03e7361319d2a43edc17422f36b09",
		OldDecimals:  0,
		OldSymbol:    "BOG",
		NewDecimals:  18,
		NewSymbol:    "BOG",
	}, {
		TokenAddress: "0x6fa9908527d3ae92f56401212964ff7f2da5d478",
		OldDecimals:  0,
		OldSymbol:    "GALE",
		NewDecimals:  18,
		NewSymbol:    "GALE",
	}, {
		TokenAddress: "0x10f6f2b97f3ab29583d9d38babf2994df7220c21",
		OldDecimals:  0,
		OldSymbol:    "TEDDY",
		NewDecimals:  18,
		NewSymbol:    "TEDDY",
	}, {
		TokenAddress: "0x9278c8693e7328bef49804bacbfb63253565dffd",
		OldDecimals:  0,
		OldSymbol:    "WLUNC",
		NewDecimals:  6,
		NewSymbol:    "WLUNC",
	}, {
		TokenAddress: "0x4e57e27e4166275eb7f4966b42a201d76e481b03",
		OldDecimals:  0,
		OldSymbol:    "CGS",
		NewDecimals:  18,
		NewSymbol:    "CGS",
	}, {
		TokenAddress: "0x97cfbdf107468e88e80929afe085541d4725d4ff",
		OldDecimals:  0,
		OldSymbol:    "LDN",
		NewDecimals:  18,
		NewSymbol:    "LDN",
	}, {
		TokenAddress: "0xf707c00d74f002295a6ffdfeea957a5fd91aff17",
		OldDecimals:  0,
		OldSymbol:    "GRIMACE",
		NewDecimals:  18,
		NewSymbol:    "GRIMACE",
	}, {
		TokenAddress: "0x6467df17771ab26d1825bf0891b3c421d92ebc1d",
		OldDecimals:  0,
		OldSymbol:    "BUILD",
		NewDecimals:  18,
		NewSymbol:    "BUILD",
	}, {
		TokenAddress: "0x3e1bef464f2600ef94de4d965c8991e06570d2e8",
		OldDecimals:  0,
		OldSymbol:    "DEXI",
		NewDecimals:  9,
		NewSymbol:    "DEXI",
	}, {
		TokenAddress: "0x7aba852082b6f763e13010ca33b5d9ea4eee2983",
		OldDecimals:  0,
		OldSymbol:    "FIRA",
		NewDecimals:  18,
		NewSymbol:    "FIRA",
	}, {
		TokenAddress: "0xca558149225fb386b9c26716e8c35a650c74d35e",
		OldDecimals:  0,
		OldSymbol:    "FORT",
		NewDecimals:  9,
		NewSymbol:    "FORT",
	}, {
		TokenAddress: "0x40ff4581cf2d6e4e07b02034105d6435d4f3f84c",
		OldDecimals:  0,
		OldSymbol:    "CGX",
		NewDecimals:  18,
		NewSymbol:    "CGX",
	}, {
		TokenAddress: "0x06c04b0ad236e7ca3b3189b1d049fe80109c7977",
		OldDecimals:  0,
		OldSymbol:    "CANDY",
		NewDecimals:  18,
		NewSymbol:    "CANDY",
	}, {
		TokenAddress: "0x1ba477ca252c0ff21c488d41759795e7e7812ab4",
		OldDecimals:  0,
		OldSymbol:    "CROS",
		NewDecimals:  18,
		NewSymbol:    "CROS",
	}, {
		TokenAddress: "0xd3cecbe5639d05aed446da11f08d495ca6bf359f",
		OldDecimals:  0,
		OldSymbol:    "CROBLANC",
		NewDecimals:  18,
		NewSymbol:    "CROBLANC",
	}, {
		TokenAddress: "0x41080ca7be4b3f0cacbd95164e9a56b582382caa",
		OldDecimals:  0,
		OldSymbol:    "ZOGI",
		NewDecimals:  18,
		NewSymbol:    "ZOGI",
	}, {
		TokenAddress: "0x9fae23a2700feecd5b93e43fdbc03c76aa7c08a6",
		OldDecimals:  0,
		OldSymbol:    "LCRO",
		NewDecimals:  18,
		NewSymbol:    "LCRO",
	}, {
		TokenAddress: "0xf69b774be9aca90e87d2788be7e6b1d492fa7acd",
		OldDecimals:  0,
		OldSymbol:    "ECH",
		NewDecimals:  18,
		NewSymbol:    "ECH",
	}, {
		TokenAddress: "0xcdd6494aeb193c8d5541b5b9c5e72a3809a98fdc",
		OldDecimals:  0,
		OldSymbol:    "$DEWO",
		NewDecimals:  18,
		NewSymbol:    "$DEWO",
	}, {
		TokenAddress: "0xa3089f1462426398eb586e07700ae08aba7324c4",
		OldDecimals:  16,
		OldSymbol:    "HORGI",
		NewDecimals:  18,
		NewSymbol:    "HORGI",
	}, {
		TokenAddress: "0x34deb73e57f7be74d2cca1869d2c721e16c7a32c",
		OldDecimals:  0,
		OldSymbol:    "CRD",
		NewDecimals:  18,
		NewSymbol:    "CRD",
	}, {
		TokenAddress: "0xcbf0adea24fd5f32c6e7f0474f0d1b94ace4e2e7",
		OldDecimals:  0,
		OldSymbol:    "CROID",
		NewDecimals:  18,
		NewSymbol:    "CROID",
	}, {
		TokenAddress: "0xdb7d0a1ec37de1de924f8e8adac6ed338d4404e9",
		OldDecimals:  0,
		OldSymbol:    "VNO",
		NewDecimals:  18,
		NewSymbol:    "VNO",
	}}

	arbitrumTokenParam := []*TokenParam{{TokenAddress: "0x46d06cf8052ea6fdbf71736af33ed23686ea1452", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xb40dbbb7931cfef8be73aeec6c67d3809bd4600b", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x43e4ef7e796a631539f523900da824e73edc3110", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xe547fab4d5ceafd29e2653cb19e6ae8ed9c8589b", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xda51015b73ce11f77a115bb1b8a7049e02ddecf0", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x616279ff3dbf57a55e3d1f2e309e5d704e4e58ae", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x76b44e0cf9bd024dbed09e1785df295d59770138", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xfac38532829fdd744373fdcd4708ab90fa0c4078", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x93c15cd7de26f07265f0272e0b831c5d7fab174f", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x7f90122bf0700f9e7e1f688fe926940e8839f353", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x1fae2a29940015632f2a6ce006dfa7e3225515a7", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "0x5ecc0446e8aa72b9bd74b8935687e1e4ca3478d3", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x65c936f008bc34fe819bce9fa5afd9dc2d49977f", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x31c91d8fb96bff40955dd2dbc909b36e8b104dde", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xfb1f65955e168e0ef500b170eed4a4efeb99ae32", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x872bad41cfc8ba731f811fea8b2d0b9fd6369585", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x458a2df1a5c74c5dc9ed6e01dd1178e6d353243b", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x088cd8f5ef3652623c22d48b1605dcfe860cd704", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xdab8c8776a4041415a60ed6b339d8e667cf2a934", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x3d9907f9a368ad0a51be60f7da3b97cf940982d8", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x954ac1c73e16c77198e83c088ade88f6223f3d44", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xe018c227bc84e44c96391d3067fab5a9a46b7e62", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x500756c7d239aee30f52c7e52af4f4f008d1a98f", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x8616e8ea83f048ab9a5ec513c9412dd2993bce3f", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x8e75dafecf75de7747a05b0891177ba03333a166", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x4dad357726b41bb8932764340ee9108cc5ad33a0", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xfc77b86f3ade71793e1eec1e7944db074922856e", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x4945970efeec98d393b4b979b9be265a3ae28a8b", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x45d55eadf0ed5495b369e040af0717eafae3b731", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x46ca8ed5465cb859bb3c3364078912c25f4d74de", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xb348b87b23d5977e2948e6f36ca07e1ec94d7328", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x1922c36f3bc762ca300b4a46bb2102f84b1684ab", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "0xbcc9c1763d54427bdf5efb6e9eb9494e5a1fbabf", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x319e222de462ac959baf2aec848697aec2bbd770", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xc47d9753f3b32aa9548a7c3f30b6aec3b2d2798c", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x43faaf76e1b8555c274092332bb1683fcffba1f3", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xeaba97bf5668f5022f66942c84a03dd5c86045d3", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "0x02150e97271fdc0d6e3a16d9094a0948266f07dd", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xdd8e557c8804d326c72074e987de02a23ae6ef84", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xfd559867b6d3445f9589074c3ac46418fdfffda4", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "0x369eb8197062093a20402935d3a707b4ae414e9d", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0xc4be0798e5b5b1c15eda36d9b2d8c1a60717fa92", OldDecimals: 0, NewDecimals: 18}}

	solanaTokenParam := []*TokenParam{{TokenAddress: "okaxuoDnpewHyCoYykiNX4pmJwKodhWxVxb2tCuGxMy", OldDecimals: 0, NewDecimals: 5},
		{TokenAddress: "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", OldDecimals: 0, NewDecimals: 5},
		{TokenAddress: "3FHpkMTQ3QyAJoLoXVdBpH4TfHiehnL2kXmv9UXBpYuF", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "8WftAet8HSHskSp8RUVwdPt6xr3CtF76UF5FPmazY7bt", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "CLAsHPfTPpsXmzZzdexdEuKeRzZrWjZFRHQEPu2kSgWM", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "FYNmEg12xqyuNrRn8A1cqkEapcUCh3M7ZARUN1yj1bEs", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "efk1hwJ3QNV9dc5qJaLyaw9fhrRdjzDTsxbtWXBh1Xu", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "9tQhCmFtCh56qqf9szLQ8dNjYcd4TTv6MWPpw6MqLubu", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "7eJCLyW5KkvzdzkVXs1ukA1WfFjCcocXjVit64tYcown", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "AHT1yynTv45s3P3KrRfQCVMHckdHeMVA3fteEg34xt9y", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "ANXqXpSkTEuCnR27YK2AoHNH2CCbiSaKYAKcDQVMi6ar", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "AUT1gfMZw37wMMQqAxk89nfpjZpEEf2XSoBUd8V5ydnS", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "CiAkzbxkQCyY7hFtNeUHMbqiL8CXtbWaRnUJpJz5sBrE", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "GENZexWRRGNS2Ko5rEgGG1snRXpaa3CDDGYnhTSmE3kd", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "9igG1YEgda7M2LjWkCiaKwcwYkFW174VDzrrPvoAT6DJ", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "SAX2cChnuhnKfUDERWVHyd8CoeDNR4NjoxwjuW8uiqa", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "Ez2zVjw85tZan1ycnJ5PywNNxR6Gm4jbXQtZKyQNu3Lv", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "HezGWsxSVMqEZy7HJf7TtXzQRLiDruYsheYWqoUVnWQo", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "D5zHHS5tkf9zfGBRPQbDKpUiRAMVK8VvxGwTHP6tP1B8", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "MarcoPaG4dV4qit3ZPGPFm4qt4KKNBKvAsm2rPGNF72", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "9WMwGcY6TcbSfy9XPpQymY3qNEsvEaYL3wivdwPG2fpp", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "GPyzPHuFFGvN4yWWixt6TYUtDG49gfMdFFi2iniTmCkh", OldDecimals: 0, NewDecimals: 2},
		{TokenAddress: "LMDAmLNduiDmSiMxgae1gW7ubArfEGdAfTpKohqE5gn", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "3Ysmnbdddpxv9xK8FUKXexdhRzEA4yrCz8WaE6Za5sjV", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "H1G6sZ1WDoMmMCFqBKAbg9gkQPCo1sKQtaJWz9dHmqZr", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "FM8ai6JTyf6HWAgPbnsdonqCeWfjomPMFfnBgPSf23W2", OldDecimals: 0, NewDecimals: 5},
		{TokenAddress: "5yxNbU8DgYJZNi3mPD9rs4XLh9ckXrhPjJ5VCujUWg5H", OldDecimals: 0, NewDecimals: 5},
		{TokenAddress: "o1Mw5Y3n68o8TakZFuGKLZMGjm72qv4JeoZvGiCLEvK", OldDecimals: 0, NewDecimals: 2},
		{TokenAddress: "Doggoyb1uHFJGFdHhJf8FKEBUMv58qo98CisWgeD7Ftk", OldDecimals: 0, NewDecimals: 5},
		{TokenAddress: "DfCqzB3mV3LuF2FWZ1GyXer4U5X118g8oUBExeUqzPss", OldDecimals: 0, NewDecimals: 5},
		{TokenAddress: "FfpyoV365c7iR8QQg5NHGCXQfahbqzY67B3wpzXkiLXr", OldDecimals: 0, NewDecimals: 6},
		{TokenAddress: "boooCKXQn9YTK2aqN5pWftQeb9TH7cj7iUKuVCShWQx", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "DeoP2swMNa9d4SGcQkR82j4RYYeNhDjcTCwyzEhKwfAf", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "BWXrrYFhT7bMHmNBFoQFWdsSgA3yXoAnMhDK6Fn1eSEn", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "CRYPTi2V87Tu6aLc9gSwXM1wSLc6rjZh3TGC4GDRCecq", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "E69gzpKjxMF9p7u4aEpsbuHevJK9nzxXFdUDzKE5wK6z", OldDecimals: 0, NewDecimals: 5},
		{TokenAddress: "ENvD2Y49D6LQwKTtcxnKBmEMmSYJPWMxXhNsAo18jxNc", OldDecimals: 0, NewDecimals: 9},
		{TokenAddress: "CvB1ztJvpYQPvdPBePtRzjL4aQidjydtUz61NWgcgQtP", OldDecimals: 0, NewDecimals: 6}}

	ethTokenParam := []*TokenParam{{TokenAddress: "0xf2210f65235c2fb391ab8650520237e6378e5c5a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x87cdc02f0812f08cd50f946793706fad9c265e2d", OldDecimals: 18, NewDecimals: 16},
		{TokenAddress: "0xe83ce6bfb580583bd6a62b4be7b34fc25f02910d", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x9d62526f5ce701950c30f2caca70edf70f9fbf0f", OldDecimals: 19, NewDecimals: 18},
		{TokenAddress: "0xfd020998a1bb316dfe7b136fe59ae4b365d79978", OldDecimals: 8, NewDecimals: 18},
		{TokenAddress: "0x6307b25a665efc992ec1c1bc403c38f3ddd7c661", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0xd60d8e670438615721c8f50db31839f98a124ff7", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xba630d3ba20502ba07975b15c719beecc8e4ebb0", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xe0bcd056b6a8c7fd4983cb56c162799e498e85d3", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x1e2d230c7a7f4c679fb1378f1f51dedeae85cd72", OldDecimals: 6, NewDecimals: 18},
		{TokenAddress: "0xc2702fbd7208f53d1a7fd1dd96b9e685aef12d4e", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xbdea5bb640dbfc4593809deec5cdb8f99b704cd2", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x5245c0249e5eeb2a0838266800471fd32adb1089", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x3e362283b86c1b45097cc3fb02213b79cf6211df", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x5444c30210d8a0a156178cfb8048b4137c0d40d1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xe72834590d7a339ead78e7fbd1d3c7f76f6eb430", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xa392c35ec6900346adec720abe50413f48ee5143", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0x3a4cab3dcfab144fe7eb2b5a3e288cc03dc07659", OldDecimals: 9, NewDecimals: 18},
		{TokenAddress: "0x168e39f96a653ce0a456560687241b0b2936e5ff", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xa0471cdd5c0dc2614535fd7505b17a651a8f0dab", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x616ef40d55c0d2c506f4d6873bda8090b79bf8fc", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x014d9a527fe5d11c178d70248921db2b735d6e41", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x0af55d5ff28a3269d69b98680fd034f115dd53ac", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x4fabf135bcf8111671870d4399af739683198f96", OldDecimals: 4, NewDecimals: 18},
		{TokenAddress: "0x099611578c5673e3d8e831a247ea8f47fb501073", OldDecimals: 6, NewDecimals: 9},
		{TokenAddress: "0x6769d86f9c430f5ac6d9c861a0173613f1c5544c", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xf65b639cd872217a5cf224e34e28320b06ef317f", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x582d872a1b094fc48f5de31d3b73f2d9be47def1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xa7f976c360ebbed4465c2855684d1aae5271efa9", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x448cf521608f9fbac30da7746603cba1b9efc46f", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0xc656b2279b0fdf761e832133b06ce607fbbcbceb", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x62d693fe5c13b5a5b24c9ec3f423e51c35f5624f", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x415acc3c6636211e67e248dc28400b452acefa68", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x960fb2b6d4ae1fc3a5349d5b054ffefbae42fb8a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xd50649aab1d39d68bc965e0f6d1cfe0010e4908b", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xb4dffa52fee44bd493f12d85829d775ec8017691", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xfac917971ce50849502022b40aa8a12843f022c0", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xe49cb97091b5bde1e8b7043e3d5717e64fde825e", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x35b08722aa26be119c1608029ccbc976ac5c1082", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xd5281bb2d1ee94866b03a0fccdd4e900c8cb5091", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x1c23f0f3e06fa0e07c5e661353612a2d63323bc6", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xca7b3ba66556c4da2e2a9afef9c64f909a59430a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x2e92864240819e2286d440b0c477077dd660b340", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x698072ff23377f474d42483203eded5f49f9579c", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xcfad57a67689809cda997f655802a119838c9cec", OldDecimals: 8, NewDecimals: 7},
		{TokenAddress: "0x3acf9680b5d57994b6570c0ea97bc4d8b25e3f5b", OldDecimals: 18, NewDecimals: 10},
		{TokenAddress: "0xb64fde8f199f073f41c132b9ec7ad5b61de0b1b7", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x788b6d2b37aa51d916f2837ae25b05f0e61339d1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xdfc3829b127761a3218bfcee7fc92e1232c9d116", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x9ee45adbb2f2083ab5cd9bc888c77a662dbd55fe", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xebf56b58be8339a827f9e9bed5feae3a6c63a562", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x1e2f15302b90edde696593607b6bd444b64e8f02", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xd953a0308617c79ecad8d5eab88a6561fd8a084d", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x6110c64219621ce5b02fb8e8e57b54c01b83bf85", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x965b85d4674f64422c4898c8f8083187f02b32c0", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x52d04a0a7f3d79880a89cc3ca5de294862f6a0d9", OldDecimals: 9, NewDecimals: 18},
		{TokenAddress: "0x1e4ec900dd162ebaf6cf76cfe8a546f34d7a483d", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xf79f9020560963422ecc9c0c04d3a21190bbf045", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x8b3870df408ff4d7c3a26df852d41034eda11d81", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x4123a133ae3c521fd134d7b13a2dec35b56c2463", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xee1ae38be4ce0074c4a4a8dc821cc784778f378c", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0xe4f356ecce6fbda81ecdea2e38527e59422861c2", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x43acedd39ba4b0bfccd92897fce617fb90a971d8", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x475d02372705c18a10b518d36b2135263e0045dc", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xb2f87efa44de3008d6ba75d5e879422003d6dabb", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x0407b4c4eaed35ce3c5b852bdfa1640b09eeedf4", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0xafbf03181833ab4e8dec24d708a2a24c2baaa4a4", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xcf8f32e032f432b02393636b2092a6bef975fbf9", OldDecimals: 18, NewDecimals: 10},
		{TokenAddress: "0xd38b1159c8aee064af2d869afa1c2c1632da8b97", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x069f967be0ca21c7d793d8c343f71e597d9a49b3", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xc67b12049c2d0cf6e476bc64c7f82fc6c63cffc5", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x9767203e89dcd34851240b3919d4900d3e5069f1", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x45734927fa2f616fbe19e65f42a0ef3d37d1c80a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x832c8b1bf8c89f1f827c2bdee2a951530a4e712f", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x198d14f2ad9ce69e76ea330b374de4957c3f850a", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xc4d586ef7be9ebe80bd5ee4fbd228fe2db5f2c4e", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x1095d4a344a4760900071025d6103a17a361abad", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x79c7ef95ad32dcd5ecadb231568bb03df7824815", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xc5fb36dd2fb59d3b98deff88425a3f425ee469ed", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xbb3c2a170fbb8988cdb41c04344f9863b0f71c20", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x720cd16b011b987da3518fbf38c3071d4f0d1495", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xb3cc3d7e656893f22d2372b0ae57106f6b155cbe", OldDecimals: 9, NewDecimals: 18},
		{TokenAddress: "0xeadd9b69f96140283f9ff75da5fd33bcf54e6296", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x0557e0d15aec0b9026dd17aa874fdf7d182a2ceb", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xee9e5eff401ee921b138490d00ca8d1f13f67a72", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x2cc1be643e0882fb096f7f96d2b6ca079ad5270c", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x05fb86775fd5c16290f1e838f5caaa7342bd9a63", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xcc0a71ba8ba5a638fe30da6b7bf079acd09d6dc8", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xd996035db82cae33ba1f16fdf23b816e5e9faabb", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xe3c408bd53c31c085a1746af401a4042954ff740", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x46d473a0b3eeec9f55fade641bc576d5bc0b2246", OldDecimals: 8, NewDecimals: 18},
		{TokenAddress: "0x282d0ad1fa03dfbdb88243b958e77349c73737d1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xd01409314acb3b245cea9500ece3f6fd4d70ea30", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x594207c791afd06a8d087d84d99d1da53ccbd45f", OldDecimals: 18, NewDecimals: 3},
		{TokenAddress: "0xd8c1232fcd219286e341271385bd70601503b3d7", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x8a65b987d9813f0a97446eda0de918b2573ae406", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x1be56412c9606e7285280f76a105eba56996e491", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xd56736e79093d31be093ba1b5a5fe32e054b9592", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x9b932b4fdd6747c341a71a564c7073fd4d0354d5", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xb4371da53140417cbb3362055374b10d97e420bb", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x217ddead61a42369a266f1fb754eb5d3ebadc88a", OldDecimals: 9, NewDecimals: 18},
		{TokenAddress: "0xf3c6327b4c58e38a7986edb4a8f236031708f280", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0xa2b4c0af19cc16a6cfacce81f192b024d625817d", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xe75097d3ec88701361e670e065b8d5bc4dafbc9d", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x310c8f00b9de3c31ab95ea68feb6c877538f7947", OldDecimals: 14, NewDecimals: 18},
		{TokenAddress: "0xd3c51de3e6dd9b53d7f37699afb3ee3bf9b9b3f4", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x48dd66bb5418a14b1ccca6ea61002b1cdeff2ca1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x10dfde4f40416d7c5f4c2b7c2b0f78ae63d66a82", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x0d31df7dedd78649a14aae62d99ccb23abcc3a5a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x4cce2bb41a2b11d7746732df21d48caadb7ab3ba", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xcca3e26be51b8905f1a01872524f17eb55bd02fb", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x777fd20c983d6658c1d50b3958b3a1733d1cd1e1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x86d49fbd3b6f989d641e700a15599d3b165002ab", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x35609dc59e15d03c5c865507e1348fa5abb319a8", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xed04915c23f00a313a544955524eb7dbd823143d", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x89e93789db5f8271e4c4bbc4c0252c2253ff4920", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x9528cceb678b90daf02ca5ca45622d5cbaf58a30", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xd7efb00d12c2c13131fd319336fdf952525da2af", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0xc80c5e40220172b36adee2c951f26f2a577810c5", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x636484a1c41e88e3fc7c99248ca0b3c3a844ab86", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x5935ffc231e93ac04daa089c0f1b94d0fb2449de", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x916c5de09cf63f6602d1e1793fb41f6437814a62", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x673a2722e5a8f614beaa66a2ba73384d98424d51", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xbd31ea8212119f94a611fa969881cba3ea06fa3d", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x0c74e87c123b9a6c61c66468f8718692c1397a53", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x383518188c0c6d7730d91b2c03a03c837814a899", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xcb46c550539ac3db72dc7af7c89b11c306c727c2", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xb893a8049f250b57efa8c62d51527a22404d7c9a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x646a764b4afca56002956b7157cdcbe98b91bee1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x1e7fde66acc959fe02fbe7f830d6dcf4567cfd40", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xf6537fe0df7f0cc0985cf00792cc98249e73efa0", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x181c94a45ed257baf2211d4ff7e1f49a5964134a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x747c4ce9622ea750ea8048423b38a746b096c8e8", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x4ece5c5cfb9b960a49aae739e15cdb6cfdcc5782", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x0ae8b74cd2d566853715800c9927f879d6b76a37", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x93b2fff814fcaeffb01406e80b4ecd89ca6a021b", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x47d49d010c03b40f88f422502d694ff49fe6c9c8", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x2001f2a0cf801ecfda622f6c28fb6e10d803d969", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x5167f7cdeb771417d8722e654ccc3e1734a01878", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x5b0f6ad5875da96ac224ba797c6f362f4c3a2b3b", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x1cd2528522a17b6be63012fb63ae81f3e3e29d97", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xc08512927d12348f6620a698105e1baac6ecd911", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xa87135285ae208e22068acdbff64b11ec73eaa5a", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0xe692c8d72bd4ac7764090d54842a305546dd1de5", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xa0a9c16856c96d5e9d80a8696eea5e02b2dc3398", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xaf4dce16da2877f8c9e00544c93b62ac40631f16", OldDecimals: 18, NewDecimals: 5},
		{TokenAddress: "0xdf96bde075d59e9143b325c75af38e208c986e6f", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xe2a083397521968eb05585932750634bed4b7d56", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x7bfde33d790411a88d46e9e1be32fc86228891a4", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xd9a6803f41a006cbf389f21e55d7a6079dfe8df3", OldDecimals: 19, NewDecimals: 18},
		{TokenAddress: "0x24ccedebf841544c9e6a62af4e8c2fa6e5a46fde", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x2f02be0c4021022b59e9436f335d69df95e5222a", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x61b5c3aee3a25f6f83531d548a4d2ee58450f5d9", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xb67718b98d52318240c52e71a898335da4a28c42", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x3c2a309d9005433c1bc2c92ef1be06489e5bf258", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x8fc8f8269ebca376d046ce292dc7eac40c8d358a", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x6f6d15e2dabd182c7c0830db1bdff1f920b57ffa", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x0c356b7fd36a5357e5a017ef11887ba100c9ab76", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xe0bdaafd0aab238c55d68ad54e616305d4a21772", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x85f6eb2bd5a062f5f8560be93fb7147e16c81472", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0x5fc251c13c4ef172d87a32ab082897132b49435c", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0xb622400807765e73107b7196f444866d7edf6f62", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xc50ef449171a51fbeafd7c562b064b6471c36caa", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x32879b5c0fdbc2a81597f019717b412b9d755a09", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x0f7e1e6c9b67972a0ab31f47ab3e94b60be37d86", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0xf5555732b3925356964695578fefcffcd31bcbb8", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xac51066d7bec65dc4589368da368b212745d63e8", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x4527a3b4a8a150403090a99b87effc96f2195047", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xa6d5c720a9af5a405dfb6b9f44fc44fab5d4a58d", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x8b61f7afe322372940dc4512be579f0a55367650", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x31c2415c946928e9fd1af83cdfa38d3edbd4326f", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x7f66ef4be2c128f121ca776888e6142ec0f3bd75", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xc56c2b7e71b54d38aab6d52e94a04cbfa8f604fa", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xadfb28426864c0ff169f5dcead2efdd09e0deec9", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x1681bcb589b3cfcf0c0616b0ce9b19b240643dc1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xf0c5831ec3da15f3696b4dad8b21c7ce2f007f28", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x62c723d9debcfe4a39a3a05a4eefb67a740ef495", OldDecimals: 9, NewDecimals: 6},
		{TokenAddress: "0x93cfe232311f49b53d4285cd54d31261980496ba", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x0c7c5b92893a522952eb4c939aa24b65ff910c48", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0x1a87077c4f834884691b8ba4fc808d2ec93a9f30", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xc0dbb6b1bd7e574eb45fc78ecc2afad57d9623fe", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x9724f51e3afb6b2ae0a5d86fd3b88c73283bc38f", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x9ac59862934ebc36072d4d8ada37c62373a13856", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xa2791bdf2d5055cda4d46ec17f9f429568275047", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x140b890bf8e2fe3e26fcd516c75728fb20b31c4f", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0xc92e75431614ecae3847842cc7f1b6bc58326427", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x2b5c21578594f7988c7c80d258d0c927c756a848", OldDecimals: 18, NewDecimals: 10},
		{TokenAddress: "0xb4ba6c2d54c6ab55233c60dcd018d307112ab84a", OldDecimals: 18, NewDecimals: 3},
		{TokenAddress: "0x6641b8df62e4b0e00d3b61f8eca63b2052404fd9", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x5cc56c266143f29a5054b9ae07f3ac3513a7965e", OldDecimals: 18, NewDecimals: 10},
		{TokenAddress: "0x083d41d6dd21ee938f0c055ca4fb12268df0efac", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0x044d078f1c86508e13328842cc75ac021b272958", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xee7527841a932d2912224e20a405e1a1ff747084", OldDecimals: 18, NewDecimals: 7},
		{TokenAddress: "0xf5b1fd29d23e98db2d9ebb8435e1082e3b38fb65", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x4546d782ffb14a465a3bb518eecf1a181da85332", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xabd4dc8fde9848cbc4ff2c0ee81d4a49f4803da4", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x7b392dd9bdef6e17c3d1ba62d1a6c7dcc99d839b", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x4d953cf077c0c95ba090226e59a18fcf97db44ec", OldDecimals: 19, NewDecimals: 18},
		{TokenAddress: "0x0a8b16b27d5219c8c6b57d5442ce31d81573eee4", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x849a226f327b89e3133d9930d927f9eb9346f8c9", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x4a9d8b8fce0b6ec033932b13c4e24d24dc4113cd", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x4b4eb5c44d50bfd44124688c6754633f7e258b01", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x27201232579491ce9b116ac6f37d354cc723a2f3", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xa67e9f021b9d208f7e3365b2a155e3c55b27de71", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xdd2e93924bdd4e20c3cf4a8736e5955224fa450e", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x0f3debf94483beecbfd20167c946a61ea62d000f", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xda1e53e088023fe4d1dc5a418581748f52cbd1b8", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xbe46985ee59830e18c02dfa143000dba7ac967dd", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x5b7533812759b45c2b44c19e320ba2cd2681b542", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x1e950af2f6f8505c09f0ca42c4b38f10979cb22e", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xf5cb350b40726b5bcf170d12e162b6193b291b41", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xb087c2180e3134db396977065817aed91fea6ead", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x122f96d596384885b54bccdddf2125018c421d83", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xf5bb30ebc95dca53e3320eb05d3d1bcab806b9bf", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0xb9f747162ab1e95d07361f9048bcdf6edda9eea7", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x7121d00b4fa18f13da6c2e30d19c04844e6afdc8", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xe831f96a7a1dce1aa2eb760b1e296c6a74caa9d5", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xf442a73d52431eb39f3fca49b4a495d9ae99b1a2", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xb8e2e2101ed11e9138803cd3e06e16dd19910647", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x044727e50ff30db57fad06ff4f5846eab5ea52a2", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x24861414c8845b8115397302e9dcfaab3f239826", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x24b20da7a2fa0d1d5afcd693e1c8afff20507efd", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x299698b4b44bd6d023981a7317798dee12860834", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x29d578cec46b50fa5c88a99c6a4b70184c062953", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x36e43065e977bc72cb86dbd8405fae7057cdc7fd", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x3dd98c8a089dbcff7e8fc8d4f532bd493501ab7f", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x4ea507bf90b2d206bff56999dc76e39e447d2587", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x5552e5a89a70cb2ef5adbbc45a6be442fe7160ec", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x5fa54fddf1870c344dbfabb37dfab8700ec0def1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x614d7f40701132e25fe6fc17801fbd34212d2eda", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x72e5390edb7727e3d4e3436451dadaff675dbcc0", OldDecimals: 18, NewDecimals: 12},
		{TokenAddress: "0x81c159f7abaa9139227aff62959b86b4141f6eb2", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x88536c9b2c4701b8db824e6a16829d5b5eb84440", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x8f081eb884fd47b79536d28e2dd9d4886773f783", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x9f91d9f9070b0478abb5a9918c79b5dd533f672c", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xcd7492db29e2ab436e819b249452ee1bbdf52214", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xad22f63404f7305e4713ccbd4f296f34770513f4", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xbf900809f4c73e5a3476eb183d8b06a27e61f8e5", OldDecimals: 18, NewDecimals: 12},
		{TokenAddress: "0xc631120155621ee625835ec810b9885cdd764cd6", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xe0c05ec44775e4ad62cdc2eecdf337aa7a143363", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0xfb19075d77a0f111796fb259819830f4780f1429", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xf6269e2e0c271fb6af35e7f8a539ebc7155e33bb", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x213ecae6b3cbc0ad976f7d82626546d5b63a71cb", OldDecimals: 16, NewDecimals: 18},
		{TokenAddress: "0xb113c6cf239f60d380359b762e95c13817275277", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x4086e77c5e993fdb90a406285d00111a974f877a", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0x29b3d220f0f1e37b342cf7c48c1164bf5bf79efa", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x2cc71c048a804da930e28e93f3211dc03c702995", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x315dc1b524de57ae8e809a2e97699dbc895b8a21", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x320d31183100280ccdf69366cd56180ea442a3e8", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x4c6e796bbfe5eb37f9e3e0f66c009c8bf2a5f428", OldDecimals: 6, NewDecimals: 8},
		{TokenAddress: "0x396ec402b42066864c406d1ac3bc86b575003ed8", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x39ae6d231d831756079ec23589d2d37a739f2e89", OldDecimals: 18, NewDecimals: 4},
		{TokenAddress: "0x3a1311b8c404629e38f61d566cefefed083b9670", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x3e7804c51a70ba26e904c2e0ab440c5623a8a83f", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x471ea49dd8e60e697f4cac262b5fafcc307506e4", OldDecimals: 18, NewDecimals: 10},
		{TokenAddress: "0x5afff9876c1f98b7d2b53bcb69eb57e92408319f", OldDecimals: 5, NewDecimals: 18},
		{TokenAddress: "0x6cf9464b2c628db187f2bc1ddc0c43fda72efdd5", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x727e8260877f8507f8d61917e9778b6af8491e63", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x75858677e27c930fb622759feaffee2b754af07f", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x7c3ff33c76c919b3f5fddaf7bdddbb20a826dc61", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x84da8e731172827fcb233b911678e2a82e27baf2", OldDecimals: 18, NewDecimals: 12},
		{TokenAddress: "0x87f14e9460cecb789f1b125b2e3e353ff8ed6fcd", OldDecimals: 18, NewDecimals: 3},
		{TokenAddress: "0xbd15c4c8cd28a08e43846e3155c01a1f648d8d42", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xbf825207c74b6c3c01ab807c4f4a4fce26ebdf0f", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xc631be100f6cf9a7012c23de5a6ccb990eafc133", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xc81946c6e0e15163b14abd4b5008f3d900b2a736", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0xcdd2fa4c2b36a1a14edc41da1c9f9b2cb9f981aa", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0xcfcecfe2bd2fed07a9145222e8a7ad9cf1ccd22a", OldDecimals: 18, NewDecimals: 11},
		{TokenAddress: "0xd2e5decc08a80be6538f89f9ab8ff296e2f724df", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xdc349913d53b446485e98b76800b6254f43df695", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xdc59ac4fefa32293a95889dc396682858d52e5db", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xebf2096e01455108badcbaf86ce30b6e5a72aa52", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xefc996ce8341cd36c55412b51df5bbca429a7617", OldDecimals: 14, NewDecimals: 18},
		{TokenAddress: "0xf42965f82f9e3171d1205c5e3058caf324a09432", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xfec82a1b2638826bfe53ae2f87cfd94329cde60d", OldDecimals: 18, NewDecimals: 2},
		{TokenAddress: "0x2d5c73f3597b07f23c2bb3f2422932e67eca4543", OldDecimals: 9, NewDecimals: 18},
		{TokenAddress: "0x936b6659ad0c1b244ba8efe639092acae30dc8d6", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x34d31446a522252270b89b09016296ec4c98e23d", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xde16ce60804a881e9f8c4ebb3824646edecd478d", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x467719ad09025fcc6cf6f8311755809d45a5e5f3", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xbea0000029ad1c77d3d5d23ba2d8893db9d1efab", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xfb1172b050bcc798e37ae8abf620cc528e771162", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x2656f02bc30427ed9d380e20cec5e04f5a7a50fe", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x925206b8a707096ed26ae47c84747fe0bb734f59", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x27d8086cb8a9f82cbf53550c281164b8301cf500", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x2c4f1df9c7de0c59778936c9b145ff56813f3295", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x717376111a6f1572cf14aa2a236ec9e743b4b2e7", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0x0d15009896efe9972f8e086bdd3bcba5c1f74bf3", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xcb3c5438dae9fe30b18ea53da3dab0e7dcaa0e4b", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x9e24415d1e549ebc626a13a482bb117a2b43e9cf", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xa670d7237398238de01267472c6f13e5b8010fd1", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xf2260ed15c59c9437848afed04645044a8d5e270", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x9913db584ed23e53b419eb8bc3b6e4707d3bb7e1", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xed279fdd11ca84beef15af5d39bb4d4bee23f0ca", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x1561fb6d1311ec6fc8ded8bcf07725c4e19deeb3", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x246908bff0b1ba6ecadcf57fb94f6ae2fcd43a77", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x29ced58cd96f523f86f2771d79d64a8f58f55f5e", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x2c8988881fa08dbd283348993bed57ea0c971c94", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0xf33893de6eb6ae9a67442e066ae9abd228f5290c", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0x057acee6df29ecc20e87a77783838d90858c5e83", OldDecimals: 18, NewDecimals: 8},
		{TokenAddress: "0xaaaae5a70cded793033853af6c8354397b2fa2e6", OldDecimals: 18, NewDecimals: 6},
		{TokenAddress: "0xb8c3b7a2a618c552c23b1e4701109a9e756bab67", OldDecimals: 0, NewDecimals: 18},
		{TokenAddress: "0x0a661f6ad63a1500d714ed1eeedb64ec493a54a8", OldDecimals: 18, NewDecimals: 9},
		{TokenAddress: "0x14a40443189338c713d9efb289b3427443114ca9", OldDecimals: 18, NewDecimals: 9}}

	arbitrumData := []map[string]interface{}{
		{
			"chain":    "Arbitrum",
			"name":     "ETHRISE",
			"address":  "0x46d06cf8052ea6fdbf71736af33ed23686ea1452",
			"decimals": 18,
			"symbol":   "$ETHRISE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x46d06cf8052ea6fdbf71736af33ed23686ea1452.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "prePO",
			"address":  "0xb40dbbb7931cfef8be73aeec6c67d3809bd4600b",
			"decimals": 18,
			"symbol":   "PPO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xb40dbbb7931cfef8be73aeec6c67d3809bd4600b.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Pegasus PoW",
			"address":  "0x43e4ef7e796a631539f523900da824e73edc3110",
			"decimals": 18,
			"symbol":   "$PGS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x43e4ef7e796a631539f523900da824e73edc3110.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "UNIA Farms",
			"address":  "0xe547fab4d5ceafd29e2653cb19e6ae8ed9c8589b",
			"decimals": 18,
			"symbol":   "UNIA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xe547fab4d5ceafd29e2653cb19e6ae8ed9c8589b.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Jones GLP",
			"address":  "0x616279ff3dbf57a55e3d1f2e309e5d704e4e58ae",
			"decimals": 18,
			"symbol":   "JGLP",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x616279ff3dbf57a55e3d1f2e309e5d704e4e58ae.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Neutra Finance",
			"address":  "0xda51015b73ce11f77a115bb1b8a7049e02ddecf0",
			"decimals": 18,
			"symbol":   "NEU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xda51015b73ce11f77a115bb1b8a7049e02ddecf0.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Y2K",
			"address":  "0x65c936f008bc34fe819bce9fa5afd9dc2d49977f",
			"decimals": 18,
			"symbol":   "Y2K",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x65c936f008bc34fe819bce9fa5afd9dc2d49977f.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "deUSDC",
			"address":  "0x76b44e0cf9bd024dbed09e1785df295d59770138",
			"decimals": 18,
			"symbol":   "DEUSDC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x76b44e0cf9bd024dbed09e1785df295d59770138.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "tLPT",
			"address":  "0xfac38532829fdd744373fdcd4708ab90fa0c4078",
			"decimals": 18,
			"symbol":   "TLPT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xfac38532829fdd744373fdcd4708ab90fa0c4078.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Liquid Finance",
			"address":  "0x93c15cd7de26f07265f0272e0b831c5d7fab174f",
			"decimals": 18,
			"symbol":   "LIQD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x93c15cd7de26f07265f0272e0b831c5d7fab174f.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Curve.fi USDC/USDT",
			"address":  "0x7f90122bf0700f9e7e1f688fe926940e8839f353",
			"decimals": 18,
			"symbol":   "2CRV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x7f90122bf0700f9e7e1f688fe926940e8839f353.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "NitroFloki",
			"address":  "0x1fae2a29940015632f2a6ce006dfa7e3225515a7",
			"decimals": 9,
			"symbol":   "NIFLOKI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x1fae2a29940015632f2a6ce006dfa7e3225515a7.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Lodestar",
			"address":  "0x5ecc0446e8aa72b9bd74b8935687e1e4ca3478d3",
			"decimals": 18,
			"symbol":   "LODE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x5ecc0446e8aa72b9bd74b8935687e1e4ca3478d3.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Poison Finance",
			"address":  "0x31c91d8fb96bff40955dd2dbc909b36e8b104dde",
			"decimals": 18,
			"symbol":   "POI$ON",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x31c91d8fb96bff40955dd2dbc909b36e8b104dde.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "LiquiCats",
			"address":  "0xfb1f65955e168e0ef500b170eed4a4efeb99ae32",
			"decimals": 18,
			"symbol":   "MEOW",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xfb1f65955e168e0ef500b170eed4a4efeb99ae32.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "BattleFly",
			"address":  "0x872bad41cfc8ba731f811fea8b2d0b9fd6369585",
			"decimals": 18,
			"symbol":   "GFLY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x872bad41cfc8ba731f811fea8b2d0b9fd6369585.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Adv3nture.xyz Gemstone",
			"address":  "0x458a2df1a5c74c5dc9ed6e01dd1178e6d353243b",
			"decimals": 18,
			"symbol":   "GEM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Honor World Token",
			"address":  "0xbcc9c1763d54427bdf5efb6e9eb9494e5a1fbabf",
			"decimals": 18,
			"symbol":   "HWT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xbcc9c1763d54427bdf5efb6e9eb9494e5a1fbabf.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Vela Token",
			"address":  "0x088cd8f5ef3652623c22d48b1605dcfe860cd704",
			"decimals": 18,
			"symbol":   "VELA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x088cd8f5ef3652623c22d48b1605dcfe860cd704.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Perp Inu",
			"address":  "0xdab8c8776a4041415a60ed6b339d8e667cf2a934",
			"decimals": 18,
			"symbol":   "PERPI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xdab8c8776a4041415a60ed6b339d8e667cf2a934.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "OreoSwap",
			"address":  "0x319e222de462ac959baf2aec848697aec2bbd770",
			"decimals": 18,
			"symbol":   "OREO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x319e222de462ac959baf2aec848697aec2bbd770.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "LeverageInu",
			"address":  "0x954ac1c73e16c77198e83c088ade88f6223f3d44",
			"decimals": 18,
			"symbol":   "LEVI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x954ac1c73e16c77198e83c088ade88f6223f3d44.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Camelot Token",
			"address":  "0x3d9907f9a368ad0a51be60f7da3b97cf940982d8",
			"decimals": 18,
			"symbol":   "GRAIL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x3d9907f9a368ad0a51be60f7da3b97cf940982d8.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Chromium Dollar",
			"address":  "0xe018c227bc84e44c96391d3067fab5a9a46b7e62",
			"decimals": 18,
			"symbol":   "CR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xe018c227bc84e44c96391d3067fab5a9a46b7e62.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Petroleum OIL",
			"address":  "0x500756c7d239aee30f52c7e52af4f4f008d1a98f",
			"decimals": 18,
			"symbol":   "OIL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x500756c7d239aee30f52c7e52af4f4f008d1a98f.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "SwapFish",
			"address":  "0xb348b87b23d5977e2948e6f36ca07e1ec94d7328",
			"decimals": 18,
			"symbol":   "FISH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xb348b87b23d5977e2948e6f36ca07e1ec94d7328.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Compounded Marinated UMAMI",
			"address":  "0x1922c36f3bc762ca300b4a46bb2102f84b1684ab",
			"decimals": 9,
			"symbol":   "CMUMAMI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x1922c36f3bc762ca300b4a46bb2102f84b1684ab.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "handleUSD",
			"address":  "0x8616e8ea83f048ab9a5ec513c9412dd2993bce3f",
			"decimals": 18,
			"symbol":   "FXUSD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x8616e8ea83f048ab9a5ec513c9412dd2993bce3f.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "nitroDOGE",
			"address":  "0x8e75dafecf75de7747a05b0891177ba03333a166",
			"decimals": 18,
			"symbol":   "NITRODOGE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x8e75dafecf75de7747a05b0891177ba03333a166.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "NitroShiba",
			"address":  "0x4dad357726b41bb8932764340ee9108cc5ad33a0",
			"decimals": 18,
			"symbol":   "NISHIB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x4dad357726b41bb8932764340ee9108cc5ad33a0.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Mugen Finance",
			"address":  "0xfc77b86f3ade71793e1eec1e7944db074922856e",
			"decimals": 18,
			"symbol":   "MGN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xfc77b86f3ade71793e1eec1e7944db074922856e.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "GMD Protocol",
			"address":  "0x4945970efeec98d393b4b979b9be265a3ae28a8b",
			"decimals": 18,
			"symbol":   "GMD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x4945970efeec98d393b4b979b9be265a3ae28a8b.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "ELLERIUM",
			"address":  "0x45d55eadf0ed5495b369e040af0717eafae3b731",
			"decimals": 18,
			"symbol":   "ELM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x45d55eadf0ed5495b369e040af0717eafae3b731.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Kostren Finance",
			"address":  "0x46ca8ed5465cb859bb3c3364078912c25f4d74de",
			"decimals": 18,
			"symbol":   "KTN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x46ca8ed5465cb859bb3c3364078912c25f4d74de.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Tender.fi",
			"address":  "0xc47d9753f3b32aa9548a7c3f30b6aec3b2d2798c",
			"decimals": 18,
			"symbol":   "TND",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xc47d9753f3b32aa9548a7c3f30b6aec3b2d2798c.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Wednesday",
			"address":  "0x43faaf76e1b8555c274092332bb1683fcffba1f3",
			"decimals": 18,
			"symbol":   "WD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x43faaf76e1b8555c274092332bb1683fcffba1f3.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "RANBASED",
			"address":  "0xeaba97bf5668f5022f66942c84a03dd5c86045d3",
			"decimals": 9,
			"symbol":   "RANB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xeaba97bf5668f5022f66942c84a03dd5c86045d3.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Hamachi Finance",
			"address":  "0x02150e97271fdc0d6e3a16d9094a0948266f07dd",
			"decimals": 18,
			"symbol":   "HAMI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x02150e97271fdc0d6e3a16d9094a0948266f07dd.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "ArbInu",
			"address":  "0xdd8e557c8804d326c72074e987de02a23ae6ef84",
			"decimals": 18,
			"symbol":   "ARBINU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xdd8e557c8804d326c72074e987de02a23ae6ef84.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Stake Goblin",
			"address":  "0xfd559867b6d3445f9589074c3ac46418fdfffda4",
			"decimals": 9,
			"symbol":   "GOBLIN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xfd559867b6d3445f9589074c3ac46418fdfffda4.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "PANA DAO",
			"address":  "0x369eb8197062093a20402935d3a707b4ae414e9d",
			"decimals": 18,
			"symbol":   "PANA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0x369eb8197062093a20402935d3a707b4ae414e9d.png",
		},
		{
			"chain":    "Arbitrum",
			"name":     "Adventurer Gold",
			"address":  "0xc4be0798e5b5b1c15eda36d9b2d8c1a60717fa92",
			"decimals": 18,
			"symbol":   "GOLD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/arbitrum-one/arbitrum-one_0xc4be0798e5b5b1c15eda36d9b2d8c1a60717fa92.png",
		},
	}

	solanaData := []map[string]interface{}{
		{
			"chain":    "Solana",
			"name":     "Okami Lana",
			"address":  "okaxuoDnpewHyCoYykiNX4pmJwKodhWxVxb2tCuGxMy",
			"decimals": 5,
			"symbol":   "OKANA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/",
		},
		{
			"chain":    "Solana",
			"name":     "Bonk",
			"address":  "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
			"decimals": 5,
			"symbol":   "BONK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263.png",
		},
		{
			"chain":    "Solana",
			"name":     "Gobi Labs",
			"address":  "MarcoPaG4dV4qit3ZPGPFm4qt4KKNBKvAsm2rPGNF72",
			"decimals": 6,
			"symbol":   "GOBI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_MarcoPaG4dV4qit3ZPGPFm4qt4KKNBKvAsm2rPGNF72.png",
		},
		{
			"chain":    "Solana",
			"name":     "Boo",
			"address":  "FfpyoV365c7iR8QQg5NHGCXQfahbqzY67B3wpzXkiLXr",
			"decimals": 6,
			"symbol":   "BOO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_FfpyoV365c7iR8QQg5NHGCXQfahbqzY67B3wpzXkiLXr.png",
		},
		{
			"chain":    "Solana",
			"name":     "Style",
			"address":  "3FHpkMTQ3QyAJoLoXVdBpH4TfHiehnL2kXmv9UXBpYuF",
			"decimals": 9,
			"symbol":   "STYLE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/",
		},
		{
			"chain":    "Solana",
			"name":     "Kiwe Markets",
			"address":  "8WftAet8HSHskSp8RUVwdPt6xr3CtF76UF5FPmazY7bt",
			"decimals": 9,
			"symbol":   "KIWE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_8WftAet8HSHskSp8RUVwdPt6xr3CtF76UF5FPmazY7bt.png",
		},
		{
			"chain":    "Solana",
			"name":     "Clash",
			"address":  "CLAsHPfTPpsXmzZzdexdEuKeRzZrWjZFRHQEPu2kSgWM",
			"decimals": 6,
			"symbol":   "CLH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_CLAsHPfTPpsXmzZzdexdEuKeRzZrWjZFRHQEPu2kSgWM.png",
		},
		{
			"chain":    "Solana",
			"name":     "Corni",
			"address":  "FYNmEg12xqyuNrRn8A1cqkEapcUCh3M7ZARUN1yj1bEs",
			"decimals": 6,
			"symbol":   "CORNI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_FYNmEg12xqyuNrRn8A1cqkEapcUCh3M7ZARUN1yj1bEs.png",
		},
		{
			"chain":    "Solana",
			"name":     "EFK Token",
			"address":  "efk1hwJ3QNV9dc5qJaLyaw9fhrRdjzDTsxbtWXBh1Xu",
			"decimals": 9,
			"symbol":   "EFK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_efk1hwJ3QNV9dc5qJaLyaw9fhrRdjzDTsxbtWXBh1Xu.png",
		},
		{
			"chain":    "Solana",
			"name":     "Companion",
			"address":  "9tQhCmFtCh56qqf9szLQ8dNjYcd4TTv6MWPpw6MqLubu",
			"decimals": 6,
			"symbol":   "CMPN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_9tQhCmFtCh56qqf9szLQ8dNjYcd4TTv6MWPpw6MqLubu.png",
		},
		{
			"chain":    "Solana",
			"name":     "SLITE",
			"address":  "7eJCLyW5KkvzdzkVXs1ukA1WfFjCcocXjVit64tYcown",
			"decimals": 9,
			"symbol":   "SLITE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_7eJCLyW5KkvzdzkVXs1ukA1WfFjCcocXjVit64tYcown.png",
		},
		{
			"chain":    "Solana",
			"name":     "Avenue Hamilton Token",
			"address":  "AHT1yynTv45s3P3KrRfQCVMHckdHeMVA3fteEg34xt9y",
			"decimals": 9,
			"symbol":   "AHT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_AHT1yynTv45s3P3KrRfQCVMHckdHeMVA3fteEg34xt9y.png",
		},
		{
			"chain":    "Solana",
			"name":     "MetaToken",
			"address":  "ANXqXpSkTEuCnR27YK2AoHNH2CCbiSaKYAKcDQVMi6ar",
			"decimals": 9,
			"symbol":   "MTK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_ANXqXpSkTEuCnR27YK2AoHNH2CCbiSaKYAKcDQVMi6ar.png",
		},
		{
			"chain":    "Solana",
			"name":     "Avenue University Token",
			"address":  "AUT1gfMZw37wMMQqAxk89nfpjZpEEf2XSoBUd8V5ydnS",
			"decimals": 9,
			"symbol":   "AUT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_AUT1gfMZw37wMMQqAxk89nfpjZpEEf2XSoBUd8V5ydnS.png",
		},
		{
			"chain":    "Solana",
			"name":     "PlutusFi",
			"address":  "CiAkzbxkQCyY7hFtNeUHMbqiL8CXtbWaRnUJpJz5sBrE",
			"decimals": 9,
			"symbol":   "PLUT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_CiAkzbxkQCyY7hFtNeUHMbqiL8CXtbWaRnUJpJz5sBrE.png",
		},
		{
			"chain":    "Solana",
			"name":     "GENZ Token",
			"address":  "GENZexWRRGNS2Ko5rEgGG1snRXpaa3CDDGYnhTSmE3kd",
			"decimals": 9,
			"symbol":   "GENZ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_GENZexWRRGNS2Ko5rEgGG1snRXpaa3CDDGYnhTSmE3kd.png",
		},
		{
			"chain":    "Solana",
			"name":     "Jito Staked SOL",
			"address":  "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
			"decimals": 9,
			"symbol":   "JITOSOL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn.png",
		},
		{
			"chain":    "Solana",
			"name":     "Laine Stake",
			"address":  "LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X",
			"decimals": 9,
			"symbol":   "LAINESOL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X.png",
		},
		{
			"chain":    "Solana",
			"name":     "Fronk",
			"address":  "5yxNbU8DgYJZNi3mPD9rs4XLh9ckXrhPjJ5VCujUWg5H",
			"decimals": 5,
			"symbol":   "FRONK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/",
		},
		{
			"chain":    "Solana",
			"name":     "WeSleep",
			"address":  "9igG1YEgda7M2LjWkCiaKwcwYkFW174VDzrrPvoAT6DJ",
			"decimals": 9,
			"symbol":   "WEZ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_9igG1YEgda7M2LjWkCiaKwcwYkFW174VDzrrPvoAT6DJ.png",
		},
		{
			"chain":    "Solana",
			"name":     "SOLA-X",
			"address":  "SAX2cChnuhnKfUDERWVHyd8CoeDNR4NjoxwjuW8uiqa",
			"decimals": 9,
			"symbol":   "SAX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_SAX2cChnuhnKfUDERWVHyd8CoeDNR4NjoxwjuW8uiqa.png",
		},
		{
			"chain":    "Solana",
			"name":     "Fluid USDC",
			"address":  "Ez2zVjw85tZan1ycnJ5PywNNxR6Gm4jbXQtZKyQNu3Lv",
			"decimals": 6,
			"symbol":   "FUSDC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_Ez2zVjw85tZan1ycnJ5PywNNxR6Gm4jbXQtZKyQNu3Lv.png",
		},
		{
			"chain":    "Solana",
			"name":     "HolyGrails.io",
			"address":  "HezGWsxSVMqEZy7HJf7TtXzQRLiDruYsheYWqoUVnWQo",
			"decimals": 9,
			"symbol":   "HOLY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_HezGWsxSVMqEZy7HJf7TtXzQRLiDruYsheYWqoUVnWQo.png",
		},
		{
			"chain":    "Solana",
			"name":     "HONEY",
			"address":  "4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy",
			"decimals": 9,
			"symbol":   "HONEY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_4vMsoUT2BWatFweudnQM1xedRLfJgJ7hswhcpz4xgBTy.png",
		},
		{
			"chain":    "Solana",
			"name":     "Fluid USDT",
			"address":  "D5zHHS5tkf9zfGBRPQbDKpUiRAMVK8VvxGwTHP6tP1B8",
			"decimals": 6,
			"symbol":   "FUSDT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_D5zHHS5tkf9zfGBRPQbDKpUiRAMVK8VvxGwTHP6tP1B8.png",
		},
		{
			"chain":    "Solana",
			"name":     "Jelly eSports",
			"address":  "9WMwGcY6TcbSfy9XPpQymY3qNEsvEaYL3wivdwPG2fpp",
			"decimals": 6,
			"symbol":   "JELLY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_9WMwGcY6TcbSfy9XPpQymY3qNEsvEaYL3wivdwPG2fpp.png",
		},
		{
			"chain":    "Solana",
			"name":     "CHILI",
			"address":  "GPyzPHuFFGvN4yWWixt6TYUtDG49gfMdFFi2iniTmCkh",
			"decimals": 2,
			"symbol":   "CHILI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_GPyzPHuFFGvN4yWWixt6TYUtDG49gfMdFFi2iniTmCkh.png",
		},
		{
			"chain":    "Solana",
			"name":     "Lambda Markets",
			"address":  "LMDAmLNduiDmSiMxgae1gW7ubArfEGdAfTpKohqE5gn",
			"decimals": 6,
			"symbol":   "LMDA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_LMDAmLNduiDmSiMxgae1gW7ubArfEGdAfTpKohqE5gn.png",
		},
		{
			"chain":    "Solana",
			"name":     "edeXa Service Token",
			"address":  "3Ysmnbdddpxv9xK8FUKXexdhRzEA4yrCz8WaE6Za5sjV",
			"decimals": 9,
			"symbol":   "EDX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_3Ysmnbdddpxv9xK8FUKXexdhRzEA4yrCz8WaE6Za5sjV.png",
		},
		{
			"chain":    "Solana",
			"name":     "SHIBONK",
			"address":  "H1G6sZ1WDoMmMCFqBKAbg9gkQPCo1sKQtaJWz9dHmqZr",
			"decimals": 9,
			"symbol":   "SBONK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_H1G6sZ1WDoMmMCFqBKAbg9gkQPCo1sKQtaJWz9dHmqZr.png",
		},
		{
			"chain":    "Solana",
			"name":     "Solge",
			"address":  "FM8ai6JTyf6HWAgPbnsdonqCeWfjomPMFfnBgPSf23W2",
			"decimals": 5,
			"symbol":   "SOLGE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_FM8ai6JTyf6HWAgPbnsdonqCeWfjomPMFfnBgPSf23W2.png",
		},
		{
			"chain":    "Solana",
			"name":     "Cope Token",
			"address":  "o1Mw5Y3n68o8TakZFuGKLZMGjm72qv4JeoZvGiCLEvK",
			"decimals": 2,
			"symbol":   "COPE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_o1Mw5Y3n68o8TakZFuGKLZMGjm72qv4JeoZvGiCLEvK.png",
		},
		{
			"chain":    "Solana",
			"name":     "DOGGO",
			"address":  "Doggoyb1uHFJGFdHhJf8FKEBUMv58qo98CisWgeD7Ftk",
			"decimals": 5,
			"symbol":   "DOGGO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_Doggoyb1uHFJGFdHhJf8FKEBUMv58qo98CisWgeD7Ftk.png",
		},
		{
			"chain":    "Solana",
			"name":     "Poglana",
			"address":  "DfCqzB3mV3LuF2FWZ1GyXer4U5X118g8oUBExeUqzPss",
			"decimals": 5,
			"symbol":   "POG",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_DfCqzB3mV3LuF2FWZ1GyXer4U5X118g8oUBExeUqzPss.png",
		},
		{
			"chain":    "Solana",
			"name":     "GhostKids",
			"address":  "boooCKXQn9YTK2aqN5pWftQeb9TH7cj7iUKuVCShWQx",
			"decimals": 9,
			"symbol":   "BOO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_boooCKXQn9YTK2aqN5pWftQeb9TH7cj7iUKuVCShWQx.png",
		},
		{
			"chain":    "Solana",
			"name":     "Player 2",
			"address":  "DeoP2swMNa9d4SGcQkR82j4RYYeNhDjcTCwyzEhKwfAf",
			"decimals": 9,
			"symbol":   "DEO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_DeoP2swMNa9d4SGcQkR82j4RYYeNhDjcTCwyzEhKwfAf.png",
		},
		{
			"chain":    "Solana",
			"name":     "Hades",
			"address":  "BWXrrYFhT7bMHmNBFoQFWdsSgA3yXoAnMhDK6Fn1eSEn",
			"decimals": 9,
			"symbol":   "HADES",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_BWXrrYFhT7bMHmNBFoQFWdsSgA3yXoAnMhDK6Fn1eSEn.png",
		},
		{
			"chain":    "Solana",
			"name":     "Secret Skellies Society",
			"address":  "CRYPTi2V87Tu6aLc9gSwXM1wSLc6rjZh3TGC4GDRCecq",
			"decimals": 9,
			"symbol":   "$CRYPT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_CRYPTi2V87Tu6aLc9gSwXM1wSLc6rjZh3TGC4GDRCecq.png",
		},
		{
			"chain":    "Solana",
			"name":     "Bracelet",
			"address":  "E69gzpKjxMF9p7u4aEpsbuHevJK9nzxXFdUDzKE5wK6z",
			"decimals": 5,
			"symbol":   "BRC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_E69gzpKjxMF9p7u4aEpsbuHevJK9nzxXFdUDzKE5wK6z.png",
		},
		{
			"chain":    "Solana",
			"name":     "Sollama Utilities",
			"address":  "ENvD2Y49D6LQwKTtcxnKBmEMmSYJPWMxXhNsAo18jxNc",
			"decimals": 9,
			"symbol":   "SOLLAMA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_ENvD2Y49D6LQwKTtcxnKBmEMmSYJPWMxXhNsAo18jxNc.png",
		},
		{
			"chain":    "Solana",
			"name":     "Epics Token",
			"address":  "CvB1ztJvpYQPvdPBePtRzjL4aQidjydtUz61NWgcgQtP",
			"decimals": 6,
			"symbol":   "EPCT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/solana/solana_CvB1ztJvpYQPvdPBePtRzjL4aQidjydtUz61NWgcgQtP.png",
		},
	}

	ethData := []map[string]interface{}{
		{
			"chain":    "ETH",
			"name":     "Kaiba Defi",
			"address":  "0xf2210f65235c2fb391ab8650520237e6378e5c5a",
			"decimals": 9,
			"symbol":   "KAIBA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf2210f65235c2fb391ab8650520237e6378e5c5a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Storage Area Network Anywhere",
			"address":  "0x87cdc02f0812f08cd50f946793706fad9c265e2d",
			"decimals": 16,
			"symbol":   "SANA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x87cdc02f0812f08cd50f946793706fad9c265e2d.png",
		},
		{
			"chain":    "ETH",
			"name":     "ABBC",
			"address":  "0xe83ce6bfb580583bd6a62b4be7b34fc25f02910d",
			"decimals": 8,
			"symbol":   "ABBC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe83ce6bfb580583bd6a62b4be7b34fc25f02910d.png",
		},
		{
			"chain":    "ETH",
			"name":     "Golden Goal",
			"address":  "0xfd020998a1bb316dfe7b136fe59ae4b365d79978",
			"decimals": 18,
			"symbol":   "GDG",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xfd020998a1bb316dfe7b136fe59ae4b365d79978.png",
		},
		{
			"chain":    "ETH",
			"name":     "Gatsby Inu",
			"address":  "0xd60d8e670438615721c8f50db31839f98a124ff7",
			"decimals": 9,
			"symbol":   "GATSBYINU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd60d8e670438615721c8f50db31839f98a124ff7.png",
		},
		{
			"chain":    "ETH",
			"name":     "ExPRO OLD",
			"address":  "0xba630d3ba20502ba07975b15c719beecc8e4ebb0",
			"decimals": 9,
			"symbol":   "EXPRO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xba630d3ba20502ba07975b15c719beecc8e4ebb0.png",
		},
		{
			"chain":    "ETH",
			"name":     "PYROMATIC",
			"address":  "0x1e2d230c7a7f4c679fb1378f1f51dedeae85cd72",
			"decimals": 18,
			"symbol":   "PYRO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1e2d230c7a7f4c679fb1378f1f51dedeae85cd72.png",
		},
		{
			"chain":    "ETH",
			"name":     "Rango exchange",
			"address":  "0xc2702fbd7208f53d1a7fd1dd96b9e685aef12d4e",
			"decimals": 6,
			"symbol":   "RANGO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc2702fbd7208f53d1a7fd1dd96b9e685aef12d4e.png",
		},
		{
			"chain":    "ETH",
			"name":     "DIREWOLF",
			"address":  "0xbdea5bb640dbfc4593809deec5cdb8f99b704cd2",
			"decimals": 2,
			"symbol":   "DIREWOLF",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbdea5bb640dbfc4593809deec5cdb8f99b704cd2.png",
		},
		{
			"chain":    "ETH",
			"name":     "BEAST",
			"address":  "0xe72834590d7a339ead78e7fbd1d3c7f76f6eb430",
			"decimals": 9,
			"symbol":   "BEAST",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe72834590d7a339ead78e7fbd1d3c7f76f6eb430.png",
		},
		{
			"chain":    "ETH",
			"name":     "WKD",
			"address":  "0x5444c30210d8a0a156178cfb8048b4137c0d40d1",
			"decimals": 9,
			"symbol":   "WKD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5444c30210d8a0a156178cfb8048b4137c0d40d1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Blocktanium",
			"address":  "0x9d62526f5ce701950c30f2caca70edf70f9fbf0f",
			"decimals": 18,
			"symbol":   "BKT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9d62526f5ce701950c30f2caca70edf70f9fbf0f.png",
		},
		{
			"chain":    "ETH",
			"name":     "CatCoin.com",
			"address":  "0x3e362283b86c1b45097cc3fb02213b79cf6211df",
			"decimals": 9,
			"symbol":   "CATCOIN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x3e362283b86c1b45097cc3fb02213b79cf6211df.png",
		},
		{
			"chain":    "ETH",
			"name":     "GENRE",
			"address":  "0xa392c35ec6900346adec720abe50413f48ee5143",
			"decimals": 4,
			"symbol":   "GENRE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa392c35ec6900346adec720abe50413f48ee5143.png",
		},
		{
			"chain":    "ETH",
			"name":     "fantomGO",
			"address":  "0x3a4cab3dcfab144fe7eb2b5a3e288cc03dc07659",
			"decimals": 18,
			"symbol":   "FTG",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x3a4cab3dcfab144fe7eb2b5a3e288cc03dc07659.png",
		},
		{
			"chain":    "ETH",
			"name":     "EasySwap",
			"address":  "0xa0471cdd5c0dc2614535fd7505b17a651a8f0dab",
			"decimals": 8,
			"symbol":   "ESWA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa0471cdd5c0dc2614535fd7505b17a651a8f0dab.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kounotori",
			"address":  "0x616ef40d55c0d2c506f4d6873bda8090b79bf8fc",
			"decimals": 9,
			"symbol":   "KTO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x616ef40d55c0d2c506f4d6873bda8090b79bf8fc.png",
		},
		{
			"chain":    "ETH",
			"name":     "BabelFish",
			"address":  "0x014d9a527fe5d11c178d70248921db2b735d6e41",
			"decimals": 9,
			"symbol":   "BABEL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x014d9a527fe5d11c178d70248921db2b735d6e41.png",
		},
		{
			"chain":    "ETH",
			"name":     "Shibzelda",
			"address":  "0x099611578c5673e3d8e831a247ea8f47fb501073",
			"decimals": 9,
			"symbol":   "SHIBZELDA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x099611578c5673e3d8e831a247ea8f47fb501073.png",
		},
		{
			"chain":    "ETH",
			"name":     "888 INFINITY",
			"address":  "0xf65b639cd872217a5cf224e34e28320b06ef317f",
			"decimals": 9,
			"symbol":   "888",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf65b639cd872217a5cf224e34e28320b06ef317f.png",
		},
		{
			"chain":    "ETH",
			"name":     "TrueFlip",
			"address":  "0xa7f976c360ebbed4465c2855684d1aae5271efa9",
			"decimals": 8,
			"symbol":   "TFL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa7f976c360ebbed4465c2855684d1aae5271efa9.png",
		},
		{
			"chain":    "ETH",
			"name":     "TTX",
			"address":  "0x448cf521608f9fbac30da7746603cba1b9efc46f",
			"decimals": 2,
			"symbol":   "TTX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x448cf521608f9fbac30da7746603cba1b9efc46f.png",
		},
		{
			"chain":    "ETH",
			"name":     "Diyarbekirspor",
			"address":  "0x93cfe232311f49b53d4285cd54d31261980496ba",
			"decimals": 2,
			"symbol":   "DIYAR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x93cfe232311f49b53d4285cd54d31261980496ba.png",
		},
		{
			"chain":    "ETH",
			"name":     "The Open Network",
			"address":  "0x582d872a1b094fc48f5de31d3b73f2d9be47def1",
			"decimals": 9,
			"symbol":   "TON",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x582d872a1b094fc48f5de31d3b73f2d9be47def1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Wrapped FCT",
			"address":  "0x415acc3c6636211e67e248dc28400b452acefa68",
			"decimals": 8,
			"symbol":   "WFCT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x415acc3c6636211e67e248dc28400b452acefa68.png",
		},
		{
			"chain":    "ETH",
			"name":     "HUSH",
			"address":  "0x960fb2b6d4ae1fc3a5349d5b054ffefbae42fb8a",
			"decimals": 9,
			"symbol":   "HUSH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x960fb2b6d4ae1fc3a5349d5b054ffefbae42fb8a.png",
		},
		{
			"chain":    "ETH",
			"name":     "TiOS",
			"address":  "0xd50649aab1d39d68bc965e0f6d1cfe0010e4908b",
			"decimals": 8,
			"symbol":   "TOSC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd50649aab1d39d68bc965e0f6d1cfe0010e4908b.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kripton",
			"address":  "0x2cc71c048a804da930e28e93f3211dc03c702995",
			"decimals": 8,
			"symbol":   "LPK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2cc71c048a804da930e28e93f3211dc03c702995.png",
		},
		{
			"chain":    "ETH",
			"name":     "Congruent DAO",
			"address":  "0xb4dffa52fee44bd493f12d85829d775ec8017691",
			"decimals": 9,
			"symbol":   "GAAS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb4dffa52fee44bd493f12d85829d775ec8017691.png",
		},
		{
			"chain":    "ETH",
			"name":     "Bored Museum",
			"address":  "0xfac917971ce50849502022b40aa8a12843f022c0",
			"decimals": 9,
			"symbol":   "BORED",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xfac917971ce50849502022b40aa8a12843f022c0.png",
		},
		{
			"chain":    "ETH",
			"name":     "KuramaInu",
			"address":  "0xe49cb97091b5bde1e8b7043e3d5717e64fde825e",
			"decimals": 9,
			"symbol":   "KUNU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe49cb97091b5bde1e8b7043e3d5717e64fde825e.png",
		},
		{
			"chain":    "ETH",
			"name":     "Husky",
			"address":  "0xd5281bb2d1ee94866b03a0fccdd4e900c8cb5091",
			"decimals": 9,
			"symbol":   "HUSKY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd5281bb2d1ee94866b03a0fccdd4e900c8cb5091.png",
		},
		{
			"chain":    "ETH",
			"name":     "Eminer",
			"address":  "0x35b08722aa26be119c1608029ccbc976ac5c1082",
			"decimals": 8,
			"symbol":   "EM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x35b08722aa26be119c1608029ccbc976ac5c1082.png",
		},
		{
			"chain":    "ETH",
			"name":     "Mega Shiba Inu",
			"address":  "0x1c23f0f3e06fa0e07c5e661353612a2d63323bc6",
			"decimals": 9,
			"symbol":   "MEGASHIB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1c23f0f3e06fa0e07c5e661353612a2d63323bc6.png",
		},
		{
			"chain":    "ETH",
			"name":     "WOLVERINU",
			"address":  "0xca7b3ba66556c4da2e2a9afef9c64f909a59430a",
			"decimals": 9,
			"symbol":   "WOLVERINU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xca7b3ba66556c4da2e2a9afef9c64f909a59430a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Beer Inu",
			"address":  "0x2e92864240819e2286d440b0c477077dd660b340",
			"decimals": 9,
			"symbol":   "BEER",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2e92864240819e2286d440b0c477077dd660b340.png",
		},
		{
			"chain":    "ETH",
			"name":     "ShibaBurn",
			"address":  "0x698072ff23377f474d42483203eded5f49f9579c",
			"decimals": 9,
			"symbol":   "SHIBURN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x698072ff23377f474d42483203eded5f49f9579c.png",
		},
		{
			"chain":    "ETH",
			"name":     "RAY",
			"address":  "0x5245c0249e5eeb2a0838266800471fd32adb1089",
			"decimals": 6,
			"symbol":   "RAY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5245c0249e5eeb2a0838266800471fd32adb1089.png",
		},
		{
			"chain":    "ETH",
			"name":     "Benscoin",
			"address":  "0xcfad57a67689809cda997f655802a119838c9cec",
			"decimals": 7,
			"symbol":   "BSC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcfad57a67689809cda997f655802a119838c9cec.png",
		},
		{
			"chain":    "ETH",
			"name":     "SafeBitcoin",
			"address":  "0x62d693fe5c13b5a5b24c9ec3f423e51c35f5624f",
			"decimals": 9,
			"symbol":   "SAFEBTC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x62d693fe5c13b5a5b24c9ec3f423e51c35f5624f.png",
		},
		{
			"chain":    "ETH",
			"name":     "Blockkoin",
			"address":  "0x3acf9680b5d57994b6570c0ea97bc4d8b25e3f5b",
			"decimals": 10,
			"symbol":   "BK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x3acf9680b5d57994b6570c0ea97bc4d8b25e3f5b.png",
		},
		{
			"chain":    "ETH",
			"name":     "MVD",
			"address":  "0x788b6d2b37aa51d916f2837ae25b05f0e61339d1",
			"decimals": 9,
			"symbol":   "MVD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x788b6d2b37aa51d916f2837ae25b05f0e61339d1.png",
		},
		{
			"chain":    "ETH",
			"name":     "PRivaCY Coin",
			"address":  "0xdfc3829b127761a3218bfcee7fc92e1232c9d116",
			"decimals": 8,
			"symbol":   "PRCY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xdfc3829b127761a3218bfcee7fc92e1232c9d116.png",
		},
		{
			"chain":    "ETH",
			"name":     "APECOIN",
			"address":  "0x1e7fde66acc959fe02fbe7f830d6dcf4567cfd40",
			"decimals": 8,
			"symbol":   "APECOIN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1e7fde66acc959fe02fbe7f830d6dcf4567cfd40.png",
		},
		{
			"chain":    "ETH",
			"name":     "Filecoin Standard Full Hashrate",
			"address":  "0x965b85d4674f64422c4898c8f8083187f02b32c0",
			"decimals": 8,
			"symbol":   "SFIL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x965b85d4674f64422c4898c8f8083187f02b32c0.png",
		},
		{
			"chain":    "ETH",
			"name":     "NiftyNFT",
			"address":  "0xd953a0308617c79ecad8d5eab88a6561fd8a084d",
			"decimals": 2,
			"symbol":   "NIFTY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd953a0308617c79ecad8d5eab88a6561fd8a084d.png",
		},
		{
			"chain":    "ETH",
			"name":     "Tuzlaspor Token",
			"address":  "0x1e4ec900dd162ebaf6cf76cfe8a546f34d7a483d",
			"decimals": 2,
			"symbol":   "TUZLA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1e4ec900dd162ebaf6cf76cfe8a546f34d7a483d.png",
		},
		{
			"chain":    "ETH",
			"name":     "Olympus",
			"address":  "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5",
			"decimals": 9,
			"symbol":   "OHM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5.png",
		},
		{
			"chain":    "ETH",
			"name":     "Baby Saitama",
			"address":  "0xf79f9020560963422ecc9c0c04d3a21190bbf045",
			"decimals": 9,
			"symbol":   "BABYSAITAMA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf79f9020560963422ecc9c0c04d3a21190bbf045.png",
		},
		{
			"chain":    "ETH",
			"name":     "IOI",
			"address":  "0x8b3870df408ff4d7c3a26df852d41034eda11d81",
			"decimals": 6,
			"symbol":   "IOI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x8b3870df408ff4d7c3a26df852d41034eda11d81.png",
		},
		{
			"chain":    "ETH",
			"name":     "BitStash",
			"address":  "0xe4f356ecce6fbda81ecdea2e38527e59422861c2",
			"decimals": 8,
			"symbol":   "STASH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe4f356ecce6fbda81ecdea2e38527e59422861c2.png",
		},
		{
			"chain":    "ETH",
			"name":     "SafeBank ETH",
			"address":  "0x43acedd39ba4b0bfccd92897fce617fb90a971d8",
			"decimals": 9,
			"symbol":   "SBANK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x43acedd39ba4b0bfccd92897fce617fb90a971d8.png",
		},
		{
			"chain":    "ETH",
			"name":     "VNDT",
			"address":  "0x475d02372705c18a10b518d36b2135263e0045dc",
			"decimals": 8,
			"symbol":   "VNDT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x475d02372705c18a10b518d36b2135263e0045dc.png",
		},
		{
			"chain":    "ETH",
			"name":     "Piccolo Inu",
			"address":  "0x3a1311b8c404629e38f61d566cefefed083b9670",
			"decimals": 9,
			"symbol":   "PINU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x3a1311b8c404629e38f61d566cefefed083b9670.png",
		},
		{
			"chain":    "ETH",
			"name":     "Antalyaspor",
			"address":  "0xb2f87efa44de3008d6ba75d5e879422003d6dabb",
			"decimals": 2,
			"symbol":   "AKREP",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb2f87efa44de3008d6ba75d5e879422003d6dabb.png",
		},
		{
			"chain":    "ETH",
			"name":     "BlockWRK",
			"address":  "0x0407b4c4eaed35ce3c5b852bdfa1640b09eeedf4",
			"decimals": 4,
			"symbol":   "WRK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0407b4c4eaed35ce3c5b852bdfa1640b09eeedf4.png",
		},
		{
			"chain":    "ETH",
			"name":     "Mandox",
			"address":  "0xafbf03181833ab4e8dec24d708a2a24c2baaa4a4",
			"decimals": 9,
			"symbol":   "MANDOX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xafbf03181833ab4e8dec24d708a2a24c2baaa4a4.png",
		},
		{
			"chain":    "ETH",
			"name":     "Globe Derivative Exchange",
			"address":  "0xc67b12049c2d0cf6e476bc64c7f82fc6c63cffc5",
			"decimals": 8,
			"symbol":   "GDT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc67b12049c2d0cf6e476bc64c7f82fc6c63cffc5.png",
		},
		{
			"chain":    "ETH",
			"name":     "MRX",
			"address":  "0xd38b1159c8aee064af2d869afa1c2c1632da8b97",
			"decimals": 8,
			"symbol":   "MRX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd38b1159c8aee064af2d869afa1c2c1632da8b97.png",
		},
		{
			"chain":    "ETH",
			"name":     "HZM Coin",
			"address":  "0x069f967be0ca21c7d793d8c343f71e597d9a49b3",
			"decimals": 8,
			"symbol":   "HZM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x069f967be0ca21c7d793d8c343f71e597d9a49b3.png",
		},
		{
			"chain":    "ETH",
			"name":     "A4 Finance",
			"address":  "0x9767203e89dcd34851240b3919d4900d3e5069f1",
			"decimals": 6,
			"symbol":   "A4",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9767203e89dcd34851240b3919d4900d3e5069f1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Rogue Doge",
			"address":  "0x45734927fa2f616fbe19e65f42a0ef3d37d1c80a",
			"decimals": 9,
			"symbol":   "ROGE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x45734927fa2f616fbe19e65f42a0ef3d37d1c80a.png",
		},
		{
			"chain":    "ETH",
			"name":     "BitCoke",
			"address":  "0x832c8b1bf8c89f1f827c2bdee2a951530a4e712f",
			"decimals": 6,
			"symbol":   "COKE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x832c8b1bf8c89f1f827c2bdee2a951530a4e712f.png",
		},
		{
			"chain":    "ETH",
			"name":     "Papa Shiba",
			"address":  "0xc4d586ef7be9ebe80bd5ee4fbd228fe2db5f2c4e",
			"decimals": 9,
			"symbol":   "PHIBA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc4d586ef7be9ebe80bd5ee4fbd228fe2db5f2c4e.png",
		},
		{
			"chain":    "ETH",
			"name":     "Erzurumspor Token",
			"address":  "0x1095d4a344a4760900071025d6103a17a361abad",
			"decimals": 2,
			"symbol":   "ERZ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1095d4a344a4760900071025d6103a17a361abad.png",
		},
		{
			"chain":    "ETH",
			"name":     "Sakaryaspor",
			"address":  "0xc81946c6e0e15163b14abd4b5008f3d900b2a736",
			"decimals": 2,
			"symbol":   "SKRY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc81946c6e0e15163b14abd4b5008f3d900b2a736.png",
		},
		{
			"chain":    "ETH",
			"name":     "ARV",
			"address":  "0x79c7ef95ad32dcd5ecadb231568bb03df7824815",
			"decimals": 8,
			"symbol":   "ARV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x79c7ef95ad32dcd5ecadb231568bb03df7824815.png",
		},
		{
			"chain":    "ETH",
			"name":     "Adshares",
			"address":  "0xcfcecfe2bd2fed07a9145222e8a7ad9cf1ccd22a",
			"decimals": 11,
			"symbol":   "ADS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcfcecfe2bd2fed07a9145222e8a7ad9cf1ccd22a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Dejitaru Tsuka",
			"address":  "0xc5fb36dd2fb59d3b98deff88425a3f425ee469ed",
			"decimals": 9,
			"symbol":   "TSUKA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc5fb36dd2fb59d3b98deff88425a3f425ee469ed.png",
		},
		{
			"chain":    "ETH",
			"name":     "Sivasspor",
			"address":  "0x6cf9464b2c628db187f2bc1ddc0c43fda72efdd5",
			"decimals": 2,
			"symbol":   "SIV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x6cf9464b2c628db187f2bc1ddc0c43fda72efdd5.png",
		},
		{
			"chain":    "ETH",
			"name":     "c0",
			"address":  "0xbb3c2a170fbb8988cdb41c04344f9863b0f71c20",
			"decimals": 9,
			"symbol":   "C0",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbb3c2a170fbb8988cdb41c04344f9863b0f71c20.png",
		},
		{
			"chain":    "ETH",
			"name":     "FLUX",
			"address":  "0x720cd16b011b987da3518fbf38c3071d4f0d1495",
			"decimals": 8,
			"symbol":   "FLUX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x720cd16b011b987da3518fbf38c3071d4f0d1495.png",
		},
		{
			"chain":    "ETH",
			"name":     "Dogger",
			"address":  "0xb3cc3d7e656893f22d2372b0ae57106f6b155cbe",
			"decimals": 18,
			"symbol":   "DOGGER",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb3cc3d7e656893f22d2372b0ae57106f6b155cbe.png",
		},
		{
			"chain":    "ETH",
			"name":     "CFX Quantum",
			"address":  "0x0557e0d15aec0b9026dd17aa874fdf7d182a2ceb",
			"decimals": 6,
			"symbol":   "CFXQ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0557e0d15aec0b9026dd17aa874fdf7d182a2ceb.png",
		},
		{
			"chain":    "ETH",
			"name":     "Asian Fintech",
			"address":  "0xee9e5eff401ee921b138490d00ca8d1f13f67a72",
			"decimals": 8,
			"symbol":   "AFIN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xee9e5eff401ee921b138490d00ca8d1f13f67a72.png",
		},
		{
			"chain":    "ETH",
			"name":     "Money Plant Token",
			"address":  "0x2cc1be643e0882fb096f7f96d2b6ca079ad5270c",
			"decimals": 8,
			"symbol":   "MPT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2cc1be643e0882fb096f7f96d2b6ca079ad5270c.png",
		},
		{
			"chain":    "ETH",
			"name":     "EURB",
			"address":  "0xcc0a71ba8ba5a638fe30da6b7bf079acd09d6dc8",
			"decimals": 6,
			"symbol":   "EURB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcc0a71ba8ba5a638fe30da6b7bf079acd09d6dc8.png",
		},
		{
			"chain":    "ETH",
			"name":     "BNS",
			"address":  "0xd996035db82cae33ba1f16fdf23b816e5e9faabb",
			"decimals": 8,
			"symbol":   "BNS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd996035db82cae33ba1f16fdf23b816e5e9faabb.png",
		},
		{
			"chain":    "ETH",
			"name":     "STEPN",
			"address":  "0xe3c408bd53c31c085a1746af401a4042954ff740",
			"decimals": 8,
			"symbol":   "GMT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe3c408bd53c31c085a1746af401a4042954ff740.png",
		},
		{
			"chain":    "ETH",
			"name":     "LTO Network",
			"address":  "0xd01409314acb3b245cea9500ece3f6fd4d70ea30",
			"decimals": 8,
			"symbol":   "LTO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd01409314acb3b245cea9500ece3f6fd4d70ea30.png",
		},
		{
			"chain":    "ETH",
			"name":     "Buzzshow",
			"address":  "0x594207c791afd06a8d087d84d99d1da53ccbd45f",
			"decimals": 3,
			"symbol":   "GLDY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x594207c791afd06a8d087d84d99d1da53ccbd45f.png",
		},
		{
			"chain":    "ETH",
			"name":     "Dogira",
			"address":  "0xd8c1232fcd219286e341271385bd70601503b3d7",
			"decimals": 9,
			"symbol":   "DOGIRA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd8c1232fcd219286e341271385bd70601503b3d7.png",
		},
		{
			"chain":    "ETH",
			"name":     "FomoETH",
			"address":  "0x8a65b987d9813f0a97446eda0de918b2573ae406",
			"decimals": 9,
			"symbol":   "FOMOETH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x8a65b987d9813f0a97446eda0de918b2573ae406.png",
		},
		{
			"chain":    "ETH",
			"name":     "Zinja",
			"address":  "0x1be56412c9606e7285280f76a105eba56996e491",
			"decimals": 9,
			"symbol":   "Z",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1be56412c9606e7285280f76a105eba56996e491.png",
		},
		{
			"chain":    "ETH",
			"name":     "Nucleus",
			"address":  "0xd56736e79093d31be093ba1b5a5fe32e054b9592",
			"decimals": 9,
			"symbol":   "NUCLEUS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd56736e79093d31be093ba1b5a5fe32e054b9592.png",
		},
		{
			"chain":    "ETH",
			"name":     "APENFT",
			"address":  "0x198d14f2ad9ce69e76ea330b374de4957c3f850a",
			"decimals": 6,
			"symbol":   "NFT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x198d14f2ad9ce69e76ea330b374de4957c3f850a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Carbon Protocol",
			"address":  "0xb4371da53140417cbb3362055374b10d97e420bb",
			"decimals": 8,
			"symbol":   "SWTH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb4371da53140417cbb3362055374b10d97e420bb.png",
		},
		{
			"chain":    "ETH",
			"name":     "Don-key",
			"address":  "0x217ddead61a42369a266f1fb754eb5d3ebadc88a",
			"decimals": 18,
			"symbol":   "DON",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x217ddead61a42369a266f1fb754eb5d3ebadc88a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Protector Roge",
			"address":  "0x282d0ad1fa03dfbdb88243b958e77349c73737d1",
			"decimals": 9,
			"symbol":   "PROGE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x282d0ad1fa03dfbdb88243b958e77349c73737d1.png",
		},
		{
			"chain":    "ETH",
			"name":     "HYD",
			"address":  "0x9b932b4fdd6747c341a71a564c7073fd4d0354d5",
			"decimals": 8,
			"symbol":   "HYD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9b932b4fdd6747c341a71a564c7073fd4d0354d5.png",
		},
		{
			"chain":    "ETH",
			"name":     "Hatayspor Token",
			"address":  "0xf3c6327b4c58e38a7986edb4a8f236031708f280",
			"decimals": 2,
			"symbol":   "HATAY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf3c6327b4c58e38a7986edb4a8f236031708f280.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kishu Inu",
			"address":  "0xa2b4c0af19cc16a6cfacce81f192b024d625817d",
			"decimals": 9,
			"symbol":   "KISHU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa2b4c0af19cc16a6cfacce81f192b024d625817d.png",
		},
		{
			"chain":    "ETH",
			"name":     "MContent",
			"address":  "0xd3c51de3e6dd9b53d7f37699afb3ee3bf9b9b3f4",
			"decimals": 6,
			"symbol":   "MCONTENT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd3c51de3e6dd9b53d7f37699afb3ee3bf9b9b3f4.png",
		},
		{
			"chain":    "ETH",
			"name":     "TCG2",
			"address":  "0x0d31df7dedd78649a14aae62d99ccb23abcc3a5a",
			"decimals": 9,
			"symbol":   "TCG2",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0d31df7dedd78649a14aae62d99ccb23abcc3a5a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Vanspor Token",
			"address":  "0xe75097d3ec88701361e670e065b8d5bc4dafbc9d",
			"decimals": 2,
			"symbol":   "VAN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe75097d3ec88701361e670e065b8d5bc4dafbc9d.png",
		},
		{
			"chain":    "ETH",
			"name":     "SHKG",
			"address":  "0x48dd66bb5418a14b1ccca6ea61002b1cdeff2ca1",
			"decimals": 9,
			"symbol":   "SHKG",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x48dd66bb5418a14b1ccca6ea61002b1cdeff2ca1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Baby Alucard",
			"address":  "0x10dfde4f40416d7c5f4c2b7c2b0f78ae63d66a82",
			"decimals": 9,
			"symbol":   "ALUCARD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x10dfde4f40416d7c5f4c2b7c2b0f78ae63d66a82.png",
		},
		{
			"chain":    "ETH",
			"name":     "Parabolic",
			"address":  "0xcca3e26be51b8905f1a01872524f17eb55bd02fb",
			"decimals": 9,
			"symbol":   "PARA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcca3e26be51b8905f1a01872524f17eb55bd02fb.png",
		},
		{
			"chain":    "ETH",
			"name":     "PUBLISH",
			"address":  "0x777fd20c983d6658c1d50b3958b3a1733d1cd1e1",
			"decimals": 9,
			"symbol":   "NEWS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x777fd20c983d6658c1d50b3958b3a1733d1cd1e1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Alchemy Pay",
			"address":  "0xed04915c23f00a313a544955524eb7dbd823143d",
			"decimals": 8,
			"symbol":   "ACH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xed04915c23f00a313a544955524eb7dbd823143d.png",
		},
		{
			"chain":    "ETH",
			"name":     "COVIR.IO",
			"address":  "0x89e93789db5f8271e4c4bbc4c0252c2253ff4920",
			"decimals": 9,
			"symbol":   "CVR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x89e93789db5f8271e4c4bbc4c0252c2253ff4920.png",
		},
		{
			"chain":    "ETH",
			"name":     "GCME",
			"address":  "0x9528cceb678b90daf02ca5ca45622d5cbaf58a30",
			"decimals": 9,
			"symbol":   "GCME",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9528cceb678b90daf02ca5ca45622d5cbaf58a30.png",
		},
		{
			"chain":    "ETH",
			"name":     "Proton",
			"address":  "0xd7efb00d12c2c13131fd319336fdf952525da2af",
			"decimals": 4,
			"symbol":   "XPR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd7efb00d12c2c13131fd319336fdf952525da2af.png",
		},
		{
			"chain":    "ETH",
			"name":     "Bankera",
			"address":  "0xc80c5e40220172b36adee2c951f26f2a577810c5",
			"decimals": 8,
			"symbol":   "BNK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc80c5e40220172b36adee2c951f26f2a577810c5.png",
		},
		{
			"chain":    "ETH",
			"name":     "Atlas USV",
			"address":  "0x88536c9b2c4701b8db824e6a16829d5b5eb84440",
			"decimals": 9,
			"symbol":   "USV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x88536c9b2c4701b8db824e6a16829d5b5eb84440.png",
		},
		{
			"chain":    "ETH",
			"name":     "GOGETA",
			"address":  "0x636484a1c41e88e3fc7c99248ca0b3c3a844ab86",
			"decimals": 9,
			"symbol":   "GOGETA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x636484a1c41e88e3fc7c99248ca0b3c3a844ab86.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kanva",
			"address":  "0x5935ffc231e93ac04daa089c0f1b94d0fb2449de",
			"decimals": 8,
			"symbol":   "KNV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5935ffc231e93ac04daa089c0f1b94d0fb2449de.png",
		},
		{
			"chain":    "ETH",
			"name":     "JACY",
			"address":  "0x916c5de09cf63f6602d1e1793fb41f6437814a62",
			"decimals": 9,
			"symbol":   "JACY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x916c5de09cf63f6602d1e1793fb41f6437814a62.png",
		},
		{
			"chain":    "ETH",
			"name":     "Terra Classic (Wormhole)",
			"address":  "0xbd31ea8212119f94a611fa969881cba3ea06fa3d",
			"decimals": 6,
			"symbol":   "LUNC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbd31ea8212119f94a611fa969881cba3ea06fa3d.png",
		},
		{
			"chain":    "ETH",
			"name":     "Multi Strategies Capital",
			"address":  "0x673a2722e5a8f614beaa66a2ba73384d98424d51",
			"decimals": 9,
			"symbol":   "MSC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x673a2722e5a8f614beaa66a2ba73384d98424d51.png",
		},
		{
			"chain":    "ETH",
			"name":     "Olympus v1",
			"address":  "0x383518188c0c6d7730d91b2c03a03c837814a899",
			"decimals": 9,
			"symbol":   "OHM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x383518188c0c6d7730d91b2c03a03c837814a899.png",
		},
		{
			"chain":    "ETH",
			"name":     "ONT",
			"address":  "0xcb46c550539ac3db72dc7af7c89b11c306c727c2",
			"decimals": 9,
			"symbol":   "ONT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcb46c550539ac3db72dc7af7c89b11c306c727c2.png",
		},
		{
			"chain":    "ETH",
			"name":     "Blastoise Inu",
			"address":  "0x5167f7cdeb771417d8722e654ccc3e1734a01878",
			"decimals": 9,
			"symbol":   "BLAST",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5167f7cdeb771417d8722e654ccc3e1734a01878.png",
		},
		{
			"chain":    "ETH",
			"name":     "American Shiba",
			"address":  "0xb893a8049f250b57efa8c62d51527a22404d7c9a",
			"decimals": 9,
			"symbol":   "USHIBA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb893a8049f250b57efa8c62d51527a22404d7c9a.png",
		},
		{
			"chain":    "ETH",
			"name":     "MetaShib",
			"address":  "0x181c94a45ed257baf2211d4ff7e1f49a5964134a",
			"decimals": 9,
			"symbol":   "METASHIB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x181c94a45ed257baf2211d4ff7e1f49a5964134a.png",
		},
		{
			"chain":    "ETH",
			"name":     "AXIS",
			"address":  "0xf0c5831ec3da15f3696b4dad8b21c7ce2f007f28",
			"decimals": 8,
			"symbol":   "AXIS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf0c5831ec3da15f3696b4dad8b21c7ce2f007f28.png",
		},
		{
			"chain":    "ETH",
			"name":     "Grumpy.finance",
			"address":  "0x93b2fff814fcaeffb01406e80b4ecd89ca6a021b",
			"decimals": 9,
			"symbol":   "GRUMPY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x93b2fff814fcaeffb01406e80b4ecd89ca6a021b.png",
		},
		{
			"chain":    "ETH",
			"name":     "Giresunspor Token",
			"address":  "0x47d49d010c03b40f88f422502d694ff49fe6c9c8",
			"decimals": 2,
			"symbol":   "GRS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x47d49d010c03b40f88f422502d694ff49fe6c9c8.png",
		},
		{
			"chain":    "ETH",
			"name":     "CoinLoan",
			"address":  "0x2001f2a0cf801ecfda622f6c28fb6e10d803d969",
			"decimals": 8,
			"symbol":   "CLT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2001f2a0cf801ecfda622f6c28fb6e10d803d969.png",
		},
		{
			"chain":    "ETH",
			"name":     "Nerdy Inu",
			"address":  "0x5b0f6ad5875da96ac224ba797c6f362f4c3a2b3b",
			"decimals": 9,
			"symbol":   "NERDY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5b0f6ad5875da96ac224ba797c6f362f4c3a2b3b.png",
		},
		{
			"chain":    "ETH",
			"name":     "GYEN",
			"address":  "0xc08512927d12348f6620a698105e1baac6ecd911",
			"decimals": 6,
			"symbol":   "GYEN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc08512927d12348f6620a698105e1baac6ecd911.png",
		},
		{
			"chain":    "ETH",
			"name":     "Lunr",
			"address":  "0xa87135285ae208e22068acdbff64b11ec73eaa5a",
			"decimals": 4,
			"symbol":   "LUNR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa87135285ae208e22068acdbff64b11ec73eaa5a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Elon's Marvin",
			"address":  "0x81c159f7abaa9139227aff62959b86b4141f6eb2",
			"decimals": 9,
			"symbol":   "MARVIN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x81c159f7abaa9139227aff62959b86b4141f6eb2.png",
		},
		{
			"chain":    "ETH",
			"name":     "ANY Blocknet",
			"address":  "0xe692c8d72bd4ac7764090d54842a305546dd1de5",
			"decimals": 8,
			"symbol":   "ABLOCK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe692c8d72bd4ac7764090d54842a305546dd1de5.png",
		},
		{
			"chain":    "ETH",
			"name":     "Squeeze",
			"address":  "0xabd4dc8fde9848cbc4ff2c0ee81d4a49f4803da4",
			"decimals": 9,
			"symbol":   "SQUEEZE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xabd4dc8fde9848cbc4ff2c0ee81d4a49f4803da4.png",
		},
		{
			"chain":    "ETH",
			"name":     "KleeKai",
			"address":  "0xa67e9f021b9d208f7e3365b2a155e3c55b27de71",
			"decimals": 9,
			"symbol":   "KLEE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa67e9f021b9d208f7e3365b2a155e3c55b27de71.png",
		},
		{
			"chain":    "ETH",
			"name":     "Cubiex",
			"address":  "0x122f96d596384885b54bccdddf2125018c421d83",
			"decimals": 8,
			"symbol":   "CBIX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x122f96d596384885b54bccdddf2125018c421d83.png",
		},
		{
			"chain":    "ETH",
			"name":     "Nexum",
			"address":  "0xe831f96a7a1dce1aa2eb760b1e296c6a74caa9d5",
			"decimals": 8,
			"symbol":   "NEXM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe831f96a7a1dce1aa2eb760b1e296c6a74caa9d5.png",
		},
		{
			"chain":    "ETH",
			"name":     "Lightcoin",
			"address":  "0x320d31183100280ccdf69366cd56180ea442a3e8",
			"decimals": 8,
			"symbol":   "LHC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x320d31183100280ccdf69366cd56180ea442a3e8.png",
		},
		{
			"chain":    "ETH",
			"name":     "DumpBuster",
			"address":  "0xa0a9c16856c96d5e9d80a8696eea5e02b2dc3398",
			"decimals": 9,
			"symbol":   "GTFO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa0a9c16856c96d5e9d80a8696eea5e02b2dc3398.png",
		},
		{
			"chain":    "ETH",
			"name":     "Monetha",
			"address":  "0xaf4dce16da2877f8c9e00544c93b62ac40631f16",
			"decimals": 5,
			"symbol":   "MTH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xaf4dce16da2877f8c9e00544c93b62ac40631f16.png",
		},
		{
			"chain":    "ETH",
			"name":     "YetiCoin",
			"address":  "0xdf96bde075d59e9143b325c75af38e208c986e6f",
			"decimals": 9,
			"symbol":   "YETIC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xdf96bde075d59e9143b325c75af38e208c986e6f.png",
		},
		{
			"chain":    "ETH",
			"name":     "HODL Token",
			"address":  "0xe2a083397521968eb05585932750634bed4b7d56",
			"decimals": 9,
			"symbol":   "HODL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe2a083397521968eb05585932750634bed4b7d56.png",
		},
		{
			"chain":    "ETH",
			"name":     "Shiba Universe",
			"address":  "0x7bfde33d790411a88d46e9e1be32fc86228891a4",
			"decimals": 9,
			"symbol":   "SHIBU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x7bfde33d790411a88d46e9e1be32fc86228891a4.png",
		},
		{
			"chain":    "ETH",
			"name":     "NovaDeFi",
			"address":  "0xd9a6803f41a006cbf389f21e55d7a6079dfe8df3",
			"decimals": 18,
			"symbol":   "NMT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd9a6803f41a006cbf389f21e55d7a6079dfe8df3.png",
		},
		{
			"chain":    "ETH",
			"name":     "BlueSparrow",
			"address":  "0x24ccedebf841544c9e6a62af4e8c2fa6e5a46fde",
			"decimals": 9,
			"symbol":   "BLUESPARROW",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x24ccedebf841544c9e6a62af4e8c2fa6e5a46fde.png",
		},
		{
			"chain":    "ETH",
			"name":     "Bezoge Earth",
			"address":  "0xdc349913d53b446485e98b76800b6254f43df695",
			"decimals": 9,
			"symbol":   "BEZOGE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xdc349913d53b446485e98b76800b6254f43df695.png",
		},
		{
			"chain":    "ETH",
			"name":     "SuperBrain Capital Dao",
			"address":  "0x2f02be0c4021022b59e9436f335d69df95e5222a",
			"decimals": 9,
			"symbol":   "$SBC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2f02be0c4021022b59e9436f335d69df95e5222a.png",
		},
		{
			"chain":    "ETH",
			"name":     "PN",
			"address":  "0x61b5c3aee3a25f6f83531d548a4d2ee58450f5d9",
			"decimals": 9,
			"symbol":   "PN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x61b5c3aee3a25f6f83531d548a4d2ee58450f5d9.png",
		},
		{
			"chain":    "ETH",
			"name":     "Innovative Bioresearch Coin",
			"address":  "0xb67718b98d52318240c52e71a898335da4a28c42",
			"decimals": 6,
			"symbol":   "INNBC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb67718b98d52318240c52e71a898335da4a28c42.png",
		},
		{
			"chain":    "ETH",
			"name":     "Wrapped Paycoin",
			"address":  "0x3c2a309d9005433c1bc2c92ef1be06489e5bf258",
			"decimals": 8,
			"symbol":   "WPCI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x3c2a309d9005433c1bc2c92ef1be06489e5bf258.png",
		},
		{
			"chain":    "ETH",
			"name":     "ODE",
			"address":  "0x6f6d15e2dabd182c7c0830db1bdff1f920b57ffa",
			"decimals": 2,
			"symbol":   "ODE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x6f6d15e2dabd182c7c0830db1bdff1f920b57ffa.png",
		},
		{
			"chain":    "ETH",
			"name":     "Franklin",
			"address":  "0x85f6eb2bd5a062f5f8560be93fb7147e16c81472",
			"decimals": 4,
			"symbol":   "FLY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x85f6eb2bd5a062f5f8560be93fb7147e16c81472.png",
		},
		{
			"chain":    "ETH",
			"name":     "Refract",
			"address":  "0xe0bdaafd0aab238c55d68ad54e616305d4a21772",
			"decimals": 9,
			"symbol":   "RFR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe0bdaafd0aab238c55d68ad54e616305d4a21772.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kayserispor",
			"address":  "0x5fc251c13c4ef172d87a32ab082897132b49435c",
			"decimals": 2,
			"symbol":   "KYSR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5fc251c13c4ef172d87a32ab082897132b49435c.png",
		},
		{
			"chain":    "ETH",
			"name":     "Heros",
			"address":  "0xb622400807765e73107b7196f444866d7edf6f62",
			"decimals": 9,
			"symbol":   "HEROS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb622400807765e73107b7196f444866d7edf6f62.png",
		},
		{
			"chain":    "ETH",
			"name":     "Hanu Yokia",
			"address":  "0x72e5390edb7727e3d4e3436451dadaff675dbcc0",
			"decimals": 12,
			"symbol":   "HANU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x72e5390edb7727e3d4e3436451dadaff675dbcc0.png",
		},
		{
			"chain":    "ETH",
			"name":     "Charizard Inu",
			"address":  "0x727e8260877f8507f8d61917e9778b6af8491e63",
			"decimals": 9,
			"symbol":   "CHARIZARD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x727e8260877f8507f8d61917e9778b6af8491e63.png",
		},
		{
			"chain":    "ETH",
			"name":     "Zombie Inu",
			"address":  "0xc50ef449171a51fbeafd7c562b064b6471c36caa",
			"decimals": 9,
			"symbol":   "ZINU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc50ef449171a51fbeafd7c562b064b6471c36caa.png",
		},
		{
			"chain":    "ETH",
			"name":     "ParaToken",
			"address":  "0x32879b5c0fdbc2a81597f019717b412b9d755a09",
			"decimals": 9,
			"symbol":   "PARA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x32879b5c0fdbc2a81597f019717b412b9d755a09.png",
		},
		{
			"chain":    "ETH",
			"name":     "HTZ",
			"address":  "0x0f7e1e6c9b67972a0ab31f47ab3e94b60be37d86",
			"decimals": 4,
			"symbol":   "HTZ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0f7e1e6c9b67972a0ab31f47ab3e94b60be37d86.png",
		},
		{
			"chain":    "ETH",
			"name":     "Promodio",
			"address":  "0xf5555732b3925356964695578fefcffcd31bcbb8",
			"decimals": 9,
			"symbol":   "PMD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf5555732b3925356964695578fefcffcd31bcbb8.png",
		},
		{
			"chain":    "ETH",
			"name":     "My Neighbor Alice",
			"address":  "0xac51066d7bec65dc4589368da368b212745d63e8",
			"decimals": 6,
			"symbol":   "ALICE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xac51066d7bec65dc4589368da368b212745d63e8.png",
		},
		{
			"chain":    "ETH",
			"name":     "Ezystayz",
			"address":  "0xa6d5c720a9af5a405dfb6b9f44fc44fab5d4a58d",
			"decimals": 8,
			"symbol":   "EZY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa6d5c720a9af5a405dfb6b9f44fc44fab5d4a58d.png",
		},
		{
			"chain":    "ETH",
			"name":     "Star Crunch",
			"address":  "0x7f66ef4be2c128f121ca776888e6142ec0f3bd75",
			"decimals": 9,
			"symbol":   "STARC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x7f66ef4be2c128f121ca776888e6142ec0f3bd75.png",
		},
		{
			"chain":    "ETH",
			"name":     "PRX",
			"address":  "0xadfb28426864c0ff169f5dcead2efdd09e0deec9",
			"decimals": 8,
			"symbol":   "PRX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xadfb28426864c0ff169f5dcead2efdd09e0deec9.png",
		},
		{
			"chain":    "ETH",
			"name":     "Guccinu",
			"address":  "0x62c723d9debcfe4a39a3a05a4eefb67a740ef495",
			"decimals": 6,
			"symbol":   "GUCCIV2",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x62c723d9debcfe4a39a3a05a4eefb67a740ef495.png",
		},
		{
			"chain":    "ETH",
			"name":     "PORT",
			"address":  "0x0c7c5b92893a522952eb4c939aa24b65ff910c48",
			"decimals": 4,
			"symbol":   "PORT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0c7c5b92893a522952eb4c939aa24b65ff910c48.png",
		},
		{
			"chain":    "ETH",
			"name":     "Crypto Tankz",
			"address":  "0xc0dbb6b1bd7e574eb45fc78ecc2afad57d9623fe",
			"decimals": 9,
			"symbol":   "TANKZ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc0dbb6b1bd7e574eb45fc78ecc2afad57d9623fe.png",
		},
		{
			"chain":    "ETH",
			"name":     "RYOSHI",
			"address":  "0x9ac59862934ebc36072d4d8ada37c62373a13856",
			"decimals": 9,
			"symbol":   "RYOSHI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9ac59862934ebc36072d4d8ada37c62373a13856.png",
		},
		{
			"chain":    "ETH",
			"name":     "BUFFS",
			"address":  "0x140b890bf8e2fe3e26fcd516c75728fb20b31c4f",
			"decimals": 4,
			"symbol":   "BUFFS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x140b890bf8e2fe3e26fcd516c75728fb20b31c4f.png",
		},
		{
			"chain":    "ETH",
			"name":     "KNUCKLES",
			"address":  "0xc92e75431614ecae3847842cc7f1b6bc58326427",
			"decimals": 9,
			"symbol":   "KNUCKLES",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc92e75431614ecae3847842cc7f1b6bc58326427.png",
		},
		{
			"chain":    "ETH",
			"name":     "CryptoNijigen",
			"address":  "0x2b5c21578594f7988c7c80d258d0c927c756a848",
			"decimals": 10,
			"symbol":   "NGN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2b5c21578594f7988c7c80d258d0c927c756a848.png",
		},
		{
			"chain":    "ETH",
			"name":     "FSHN",
			"address":  "0xb4ba6c2d54c6ab55233c60dcd018d307112ab84a",
			"decimals": 3,
			"symbol":   "FSHN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb4ba6c2d54c6ab55233c60dcd018d307112ab84a.png",
		},
		{
			"chain":    "ETH",
			"name":     "Sanliurfaspor Token",
			"address":  "0x6641b8df62e4b0e00d3b61f8eca63b2052404fd9",
			"decimals": 2,
			"symbol":   "URFA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x6641b8df62e4b0e00d3b61f8eca63b2052404fd9.png",
		},
		{
			"chain":    "ETH",
			"name":     "ANKR Reward Earning DOT",
			"address":  "0x5cc56c266143f29a5054b9ae07f3ac3513a7965e",
			"decimals": 10,
			"symbol":   "ADOTB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5cc56c266143f29a5054b9ae07f3ac3513a7965e.png",
		},
		{
			"chain":    "ETH",
			"name":     "GogolCoin",
			"address":  "0x083d41d6dd21ee938f0c055ca4fb12268df0efac",
			"decimals": 4,
			"symbol":   "GOL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x083d41d6dd21ee938f0c055ca4fb12268df0efac.png",
		},
		{
			"chain":    "ETH",
			"name":     "Peercoin",
			"address":  "0x044d078f1c86508e13328842cc75ac021b272958",
			"decimals": 6,
			"symbol":   "PPC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x044d078f1c86508e13328842cc75ac021b272958.png",
		},
		{
			"chain":    "ETH",
			"name":     "SHX",
			"address":  "0xee7527841a932d2912224e20a405e1a1ff747084",
			"decimals": 7,
			"symbol":   "SHX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xee7527841a932d2912224e20a405e1a1ff747084.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kishimoto Inu",
			"address":  "0xf5b1fd29d23e98db2d9ebb8435e1082e3b38fb65",
			"decimals": 9,
			"symbol":   "KISHIMOTO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf5b1fd29d23e98db2d9ebb8435e1082e3b38fb65.png",
		},
		{
			"chain":    "ETH",
			"name":     "ZERO",
			"address":  "0x4546d782ffb14a465a3bb518eecf1a181da85332",
			"decimals": 9,
			"symbol":   "ZERO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4546d782ffb14a465a3bb518eecf1a181da85332.png",
		},
		{
			"chain":    "ETH",
			"name":     "Garfield Token",
			"address":  "0x7b392dd9bdef6e17c3d1ba62d1a6c7dcc99d839b",
			"decimals": 9,
			"symbol":   "GARFIELD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x7b392dd9bdef6e17c3d1ba62d1a6c7dcc99d839b.png",
		},
		{
			"chain":    "ETH",
			"name":     "Mini",
			"address":  "0x4d953cf077c0c95ba090226e59a18fcf97db44ec",
			"decimals": 18,
			"symbol":   "MINI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4d953cf077c0c95ba090226e59a18fcf97db44ec.png",
		},
		{
			"chain":    "ETH",
			"name":     "CGU",
			"address":  "0x849a226f327b89e3133d9930d927f9eb9346f8c9",
			"decimals": 8,
			"symbol":   "CGU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x849a226f327b89e3133d9930d927f9eb9346f8c9.png",
		},
		{
			"chain":    "ETH",
			"name":     "United Doge Finance",
			"address":  "0x4a9d8b8fce0b6ec033932b13c4e24d24dc4113cd",
			"decimals": 9,
			"symbol":   "UDOG",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4a9d8b8fce0b6ec033932b13c4e24d24dc4113cd.png",
		},
		{
			"chain":    "ETH",
			"name":     "ANKR Reward Earning KSM",
			"address":  "0x84da8e731172827fcb233b911678e2a82e27baf2",
			"decimals": 12,
			"symbol":   "AKSMB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x84da8e731172827fcb233b911678e2a82e27baf2.png",
		},
		{
			"chain":    "ETH",
			"name":     "SubGame",
			"address":  "0x4b4eb5c44d50bfd44124688c6754633f7e258b01",
			"decimals": 8,
			"symbol":   "SGB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4b4eb5c44d50bfd44124688c6754633f7e258b01.png",
		},
		{
			"chain":    "ETH",
			"name":     "Mesefa",
			"address":  "0x27201232579491ce9b116ac6f37d354cc723a2f3",
			"decimals": 8,
			"symbol":   "SEFA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x27201232579491ce9b116ac6f37d354cc723a2f3.png",
		},
		{
			"chain":    "ETH",
			"name":     "DaddyBezos",
			"address":  "0xbf825207c74b6c3c01ab807c4f4a4fce26ebdf0f",
			"decimals": 9,
			"symbol":   "DJBZ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbf825207c74b6c3c01ab807c4f4a4fce26ebdf0f.png",
		},
		{
			"chain":    "ETH",
			"name":     "SPKI",
			"address":  "0x0f3debf94483beecbfd20167c946a61ea62d000f",
			"decimals": 9,
			"symbol":   "SPKI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0f3debf94483beecbfd20167c946a61ea62d000f.png",
		},
		{
			"chain":    "ETH",
			"name":     "Aidi Inu",
			"address":  "0xda1e53e088023fe4d1dc5a418581748f52cbd1b8",
			"decimals": 9,
			"symbol":   "AIDI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xda1e53e088023fe4d1dc5a418581748f52cbd1b8.png",
		},
		{
			"chain":    "ETH",
			"name":     "SingularityNET",
			"address":  "0x5b7533812759b45c2b44c19e320ba2cd2681b542",
			"decimals": 8,
			"symbol":   "AGIX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5b7533812759b45c2b44c19e320ba2cd2681b542.png",
		},
		{
			"chain":    "ETH",
			"name":     "BabyPenguin",
			"address":  "0xbe46985ee59830e18c02dfa143000dba7ac967dd",
			"decimals": 9,
			"symbol":   "BPENG",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbe46985ee59830e18c02dfa143000dba7ac967dd.png",
		},
		{
			"chain":    "ETH",
			"name":     "BurnX 2.0",
			"address":  "0x1e950af2f6f8505c09f0ca42c4b38f10979cb22e",
			"decimals": 9,
			"symbol":   "BURNX20",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1e950af2f6f8505c09f0ca42c4b38f10979cb22e.png",
		},
		{
			"chain":    "ETH",
			"name":     "BIS",
			"address":  "0xf5cb350b40726b5bcf170d12e162b6193b291b41",
			"decimals": 8,
			"symbol":   "BIS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf5cb350b40726b5bcf170d12e162b6193b291b41.png",
		},
		{
			"chain":    "ETH",
			"name":     "Star Atlas (Wormhole)",
			"address":  "0xb9f747162ab1e95d07361f9048bcdf6edda9eea7",
			"decimals": 8,
			"symbol":   "ATLAS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb9f747162ab1e95d07361f9048bcdf6edda9eea7.png",
		},
		{
			"chain":    "ETH",
			"name":     "Ultimate Nft",
			"address":  "0xf442a73d52431eb39f3fca49b4a495d9ae99b1a2",
			"decimals": 9,
			"symbol":   "UNFT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf442a73d52431eb39f3fca49b4a495d9ae99b1a2.png",
		},
		{
			"chain":    "ETH",
			"name":     "Bean",
			"address":  "0xbea0000029ad1c77d3d5d23ba2d8893db9d1efab",
			"decimals": 6,
			"symbol":   "BEAN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbea0000029ad1c77d3d5d23ba2d8893db9d1efab.png",
		},
		{
			"chain":    "ETH",
			"name":     "WhiteBIT Token",
			"address":  "0x925206b8a707096ed26ae47c84747fe0bb734f59",
			"decimals": 8,
			"symbol":   "WBT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x925206b8a707096ed26ae47c84747fe0bb734f59.png",
		},
		{
			"chain":    "ETH",
			"name":     "Global Coin Research",
			"address":  "0x6307b25a665efc992ec1c1bc403c38f3ddd7c661",
			"decimals": 4,
			"symbol":   "GCR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x6307b25a665efc992ec1c1bc403c38f3ddd7c661.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kitty Inu",
			"address":  "0x044727e50ff30db57fad06ff4f5846eab5ea52a2",
			"decimals": 9,
			"symbol":   "KITTY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x044727e50ff30db57fad06ff4f5846eab5ea52a2.png",
		},
		{
			"chain":    "ETH",
			"name":     "Shield Protocol Token",
			"address":  "0x24861414c8845b8115397302e9dcfaab3f239826",
			"decimals": 9,
			"symbol":   "SHIELD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x24861414c8845b8115397302e9dcfaab3f239826.png",
		},
		{
			"chain":    "ETH",
			"name":     "Everscale",
			"address":  "0x29d578cec46b50fa5c88a99c6a4b70184c062953",
			"decimals": 9,
			"symbol":   "EVER",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x29d578cec46b50fa5c88a99c6a4b70184c062953.png",
		},
		{
			"chain":    "ETH",
			"name":     "New Frontier Presents",
			"address":  "0x299698b4b44bd6d023981a7317798dee12860834",
			"decimals": 9,
			"symbol":   "NFP",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x299698b4b44bd6d023981a7317798dee12860834.png",
		},
		{
			"chain":    "ETH",
			"name":     "ArchAngel",
			"address":  "0x36e43065e977bc72cb86dbd8405fae7057cdc7fd",
			"decimals": 9,
			"symbol":   "ARCHA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x36e43065e977bc72cb86dbd8405fae7057cdc7fd.png",
		},
		{
			"chain":    "ETH",
			"name":     "Town Star",
			"address":  "0x3dd98c8a089dbcff7e8fc8d4f532bd493501ab7f",
			"decimals": 8,
			"symbol":   "TOWN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x3dd98c8a089dbcff7e8fc8d4f532bd493501ab7f.png",
		},
		{
			"chain":    "ETH",
			"name":     "RAT",
			"address":  "0x4ea507bf90b2d206bff56999dc76e39e447d2587",
			"decimals": 9,
			"symbol":   "RAT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4ea507bf90b2d206bff56999dc76e39e447d2587.png",
		},
		{
			"chain":    "ETH",
			"name":     "Kawakami",
			"address":  "0x5552e5a89a70cb2ef5adbbc45a6be442fe7160ec",
			"decimals": 9,
			"symbol":   "KAWA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5552e5a89a70cb2ef5adbbc45a6be442fe7160ec.png",
		},
		{
			"chain":    "ETH",
			"name":     "FrogeX",
			"address":  "0x5fa54fddf1870c344dbfabb37dfab8700ec0def1",
			"decimals": 9,
			"symbol":   "FROGEX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5fa54fddf1870c344dbfabb37dfab8700ec0def1.png",
		},
		{
			"chain":    "ETH",
			"name":     "bePAY Finance",
			"address":  "0x8f081eb884fd47b79536d28e2dd9d4886773f783",
			"decimals": 6,
			"symbol":   "BECOIN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x8f081eb884fd47b79536d28e2dd9d4886773f783.png",
		},
		{
			"chain":    "ETH",
			"name":     "SafeMoon Inu",
			"address":  "0xcd7492db29e2ab436e819b249452ee1bbdf52214",
			"decimals": 8,
			"symbol":   "SMI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcd7492db29e2ab436e819b249452ee1bbdf52214.png",
		},
		{
			"chain":    "ETH",
			"name":     "Vanilla",
			"address":  "0xbf900809f4c73e5a3476eb183d8b06a27e61f8e5",
			"decimals": 12,
			"symbol":   "VNL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbf900809f4c73e5a3476eb183d8b06a27e61f8e5.png",
		},
		{
			"chain":    "ETH",
			"name":     "Mancium",
			"address":  "0xe0c05ec44775e4ad62cdc2eecdf337aa7a143363",
			"decimals": 2,
			"symbol":   "MANC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe0c05ec44775e4ad62cdc2eecdf337aa7a143363.png",
		},
		{
			"chain":    "ETH",
			"name":     "Fenerbahçe",
			"address":  "0xfb19075d77a0f111796fb259819830f4780f1429",
			"decimals": 6,
			"symbol":   "FB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xfb19075d77a0f111796fb259819830f4780f1429.png",
		},
		{
			"chain":    "ETH",
			"name":     "Hayfever",
			"address":  "0xf6269e2e0c271fb6af35e7f8a539ebc7155e33bb",
			"decimals": 9,
			"symbol":   "HAY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf6269e2e0c271fb6af35e7f8a539ebc7155e33bb.png",
		},
		{
			"chain":    "ETH",
			"name":     "BitMEX Token",
			"address":  "0xb113c6cf239f60d380359b762e95c13817275277",
			"decimals": 6,
			"symbol":   "BMEX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb113c6cf239f60d380359b762e95c13817275277.png",
		},
		{
			"chain":    "ETH",
			"name":     "Blockchain Brawlers",
			"address":  "0x4086e77c5e993fdb90a406285d00111a974f877a",
			"decimals": 4,
			"symbol":   "BRWL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4086e77c5e993fdb90a406285d00111a974f877a.png",
		},
		{
			"chain":    "ETH",
			"name":     "HEC",
			"address":  "0x29b3d220f0f1e37b342cf7c48c1164bf5bf79efa",
			"decimals": 9,
			"symbol":   "HEC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x29b3d220f0f1e37b342cf7c48c1164bf5bf79efa.png",
		},
		{
			"chain":    "ETH",
			"name":     "FC Bitcoin",
			"address":  "0x4c6e796bbfe5eb37f9e3e0f66c009c8bf2a5f428",
			"decimals": 8,
			"symbol":   "FCBTC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4c6e796bbfe5eb37f9e3e0f66c009c8bf2a5f428.png",
		},
		{
			"chain":    "ETH",
			"name":     "Zeptacoin",
			"address":  "0x39ae6d231d831756079ec23589d2d37a739f2e89",
			"decimals": 4,
			"symbol":   "ZPTC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x39ae6d231d831756079ec23589d2d37a739f2e89.png",
		},
		{
			"chain":    "ETH",
			"name":     "GPEX",
			"address":  "0x3e7804c51a70ba26e904c2e0ab440c5623a8a83f",
			"decimals": 8,
			"symbol":   "GPX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x3e7804c51a70ba26e904c2e0ab440c5623a8a83f.png",
		},
		{
			"chain":    "ETH",
			"name":     "metavisa",
			"address":  "0x5afff9876c1f98b7d2b53bcb69eb57e92408319f",
			"decimals": 18,
			"symbol":   "MESA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x5afff9876c1f98b7d2b53bcb69eb57e92408319f.png",
		},
		{
			"chain":    "ETH",
			"name":     "RMRK",
			"address":  "0x471ea49dd8e60e697f4cac262b5fafcc307506e4",
			"decimals": 10,
			"symbol":   "RMRK",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x471ea49dd8e60e697f4cac262b5fafcc307506e4.png",
		},
		{
			"chain":    "ETH",
			"name":     "Froggies",
			"address":  "0x7c3ff33c76c919b3f5fddaf7bdddbb20a826dc61",
			"decimals": 9,
			"symbol":   "FROGGIES",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x7c3ff33c76c919b3f5fddaf7bdddbb20a826dc61.png",
		},
		{
			"chain":    "ETH",
			"name":     "Beach",
			"address":  "0xbd15c4c8cd28a08e43846e3155c01a1f648d8d42",
			"decimals": 9,
			"symbol":   "BEACH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xbd15c4c8cd28a08e43846e3155c01a1f648d8d42.png",
		},
		{
			"chain":    "ETH",
			"name":     "Banana Task Force Ape",
			"address":  "0xc631be100f6cf9a7012c23de5a6ccb990eafc133",
			"decimals": 9,
			"symbol":   "BTFA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc631be100f6cf9a7012c23de5a6ccb990eafc133.png",
		},
		{
			"chain":    "ETH",
			"name":     "Axelar",
			"address":  "0x467719ad09025fcc6cf6f8311755809d45a5e5f3",
			"decimals": 6,
			"symbol":   "AXL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x467719ad09025fcc6cf6f8311755809d45a5e5f3.png",
		},
		{
			"chain":    "ETH",
			"name":     "DeFiChain",
			"address":  "0x8fc8f8269ebca376d046ce292dc7eac40c8d358a",
			"decimals": 8,
			"symbol":   "DFI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x8fc8f8269ebca376d046ce292dc7eac40c8d358a.png",
		},
		{
			"chain":    "ETH",
			"name":     "XIDR",
			"address":  "0xebf2096e01455108badcbaf86ce30b6e5a72aa52",
			"decimals": 6,
			"symbol":   "XIDR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xebf2096e01455108badcbaf86ce30b6e5a72aa52.png",
		},
		{
			"chain":    "ETH",
			"name":     "Metaverse Index Token",
			"address":  "0xefc996ce8341cd36c55412b51df5bbca429a7617",
			"decimals": 18,
			"symbol":   "METAI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xefc996ce8341cd36c55412b51df5bbca429a7617.png",
		},
		{
			"chain":    "ETH",
			"name":     "Imperial Obelisk",
			"address":  "0x2d5c73f3597b07f23c2bb3f2422932e67eca4543",
			"decimals": 18,
			"symbol":   "IMP",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2d5c73f3597b07f23c2bb3f2422932e67eca4543.png",
		},
		{
			"chain":    "ETH",
			"name":     "Corite",
			"address":  "0x936b6659ad0c1b244ba8efe639092acae30dc8d6",
			"decimals": 6,
			"symbol":   "CO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x936b6659ad0c1b244ba8efe639092acae30dc8d6.png",
		},
		{
			"chain":    "ETH",
			"name":     "Saudi Shiba Inu",
			"address":  "0x34d31446a522252270b89b09016296ec4c98e23d",
			"decimals": 8,
			"symbol":   "SAUDISHIB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x34d31446a522252270b89b09016296ec4c98e23d.png",
		},
		{
			"chain":    "ETH",
			"name":     "MagicCraft",
			"address":  "0xde16ce60804a881e9f8c4ebb3824646edecd478d",
			"decimals": 9,
			"symbol":   "MCRT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xde16ce60804a881e9f8c4ebb3824646edecd478d.png",
		},
		{
			"chain":    "ETH",
			"name":     "Duzce",
			"address":  "0xcdd2fa4c2b36a1a14edc41da1c9f9b2cb9f981aa",
			"decimals": 2,
			"symbol":   "DUZCE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcdd2fa4c2b36a1a14edc41da1c9f9b2cb9f981aa.png",
		},
		{
			"chain":    "ETH",
			"name":     "Bean",
			"address":  "0xdc59ac4fefa32293a95889dc396682858d52e5db",
			"decimals": 6,
			"symbol":   "BEAN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xdc59ac4fefa32293a95889dc396682858d52e5db.png",
		},
		{
			"chain":    "ETH",
			"name":     "GoMeat",
			"address":  "0xfb1172b050bcc798e37ae8abf620cc528e771162",
			"decimals": 8,
			"symbol":   "GOMT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xfb1172b050bcc798e37ae8abf620cc528e771162.png",
		},
		{
			"chain":    "ETH",
			"name":     "New Community Luna",
			"address":  "0x27d8086cb8a9f82cbf53550c281164b8301cf500",
			"decimals": 9,
			"symbol":   "$CLUNA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x27d8086cb8a9f82cbf53550c281164b8301cf500.png",
		},
		{
			"chain":    "ETH",
			"name":     "AssetMantle",
			"address":  "0x2c4f1df9c7de0c59778936c9b145ff56813f3295",
			"decimals": 6,
			"symbol":   "MNTL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2c4f1df9c7de0c59778936c9b145ff56813f3295.png",
		},
		{
			"chain":    "ETH",
			"name":     "MinePlex",
			"address":  "0x717376111a6f1572cf14aa2a236ec9e743b4b2e7",
			"decimals": 6,
			"symbol":   "PLEX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x717376111a6f1572cf14aa2a236ec9e743b4b2e7.png",
		},
		{
			"chain":    "ETH",
			"name":     "Goldex",
			"address":  "0xc631120155621ee625835ec810b9885cdd764cd6",
			"decimals": 8,
			"symbol":   "GLDX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc631120155621ee625835ec810b9885cdd764cd6.png",
		},
		{
			"chain":    "ETH",
			"name":     "SonoCoin",
			"address":  "0x0d15009896efe9972f8e086bdd3bcba5c1f74bf3",
			"decimals": 8,
			"symbol":   "SONO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0d15009896efe9972f8e086bdd3bcba5c1f74bf3.png",
		},
		{
			"chain":    "ETH",
			"name":     "Risu",
			"address":  "0xcb3c5438dae9fe30b18ea53da3dab0e7dcaa0e4b",
			"decimals": 9,
			"symbol":   "RISU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcb3c5438dae9fe30b18ea53da3dab0e7dcaa0e4b.png",
		},
		{
			"chain":    "ETH",
			"name":     "Lovely Inu",
			"address":  "0x9e24415d1e549ebc626a13a482bb117a2b43e9cf",
			"decimals": 8,
			"symbol":   "LOVELY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9e24415d1e549ebc626a13a482bb117a2b43e9cf.png",
		},
		{
			"chain":    "ETH",
			"name":     "Sommelier",
			"address":  "0xa670d7237398238de01267472c6f13e5b8010fd1",
			"decimals": 6,
			"symbol":   "SOMM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa670d7237398238de01267472c6f13e5b8010fd1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Fast Access Blockchain",
			"address":  "0xf2260ed15c59c9437848afed04645044a8d5e270",
			"decimals": 8,
			"symbol":   "FAB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf2260ed15c59c9437848afed04645044a8d5e270.png",
		},
		{
			"chain":    "ETH",
			"name":     "Slougi",
			"address":  "0x9913db584ed23e53b419eb8bc3b6e4707d3bb7e1",
			"decimals": 9,
			"symbol":   "SLOUGI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9913db584ed23e53b419eb8bc3b6e4707d3bb7e1.png",
		},
		{
			"chain":    "ETH",
			"name":     "LUSD3CRV-f",
			"address":  "0xed279fdd11ca84beef15af5d39bb4d4bee23f0ca",
			"decimals": 18,
			"symbol":   "LUSD3CRV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xed279fdd11ca84beef15af5d39bb4d4bee23f0ca.png",
		},
		{
			"chain":    "ETH",
			"name":     "Divi",
			"address":  "0x246908bff0b1ba6ecadcf57fb94f6ae2fcd43a77",
			"decimals": 8,
			"symbol":   "DIVI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x246908bff0b1ba6ecadcf57fb94f6ae2fcd43a77.png",
		},
		{
			"chain":    "ETH",
			"name":     "Neorbit",
			"address":  "0x1561fb6d1311ec6fc8ded8bcf07725c4e19deeb3",
			"decimals": 9,
			"symbol":   "NRB",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1561fb6d1311ec6fc8ded8bcf07725c4e19deeb3.png",
		},
		{
			"chain":    "ETH",
			"name":     "SOLA-X",
			"address":  "0x2c8988881fa08dbd283348993bed57ea0c971c94",
			"decimals": 9,
			"symbol":   "SAX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2c8988881fa08dbd283348993bed57ea0c971c94.png",
		},
		{
			"chain":    "ETH",
			"name":     "GRV",
			"address":  "0xf33893de6eb6ae9a67442e066ae9abd228f5290c",
			"decimals": 8,
			"symbol":   "GRV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf33893de6eb6ae9a67442e066ae9abd228f5290c.png",
		},
		{
			"chain":    "ETH",
			"name":     "Pirate Chain",
			"address":  "0x057acee6df29ecc20e87a77783838d90858c5e83",
			"decimals": 8,
			"symbol":   "ARRR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x057acee6df29ecc20e87a77783838d90858c5e83.png",
		},
		{
			"chain":    "ETH",
			"name":     "Vision Metaverse",
			"address":  "0xaaaae5a70cded793033853af6c8354397b2fa2e6",
			"decimals": 6,
			"symbol":   "VS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xaaaae5a70cded793033853af6c8354397b2fa2e6.png",
		},
		{
			"chain":    "ETH",
			"name":     "1INCH yVault",
			"address":  "0xb8c3b7a2a618c552c23b1e4701109a9e756bab67",
			"decimals": 18,
			"symbol":   "YV1INCH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb8c3b7a2a618c552c23b1e4701109a9e756bab67.png",
		},
		{
			"chain":    "ETH",
			"name":     "ZEDXION",
			"address":  "0x0a661f6ad63a1500d714ed1eeedb64ec493a54a8",
			"decimals": 9,
			"symbol":   "USDZ",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0a661f6ad63a1500d714ed1eeedb64ec493a54a8.png",
		},
		{
			"chain":    "ETH",
			"name":     "CloudTx",
			"address":  "0x14a40443189338c713d9efb289b3427443114ca9",
			"decimals": 9,
			"symbol":   "CLOUD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x14a40443189338c713d9efb289b3427443114ca9.png",
		},
		{
			"chain":    "ETH",
			"name":     "Toshinori Inu",
			"address":  "0xe0bcd056b6a8c7fd4983cb56c162799e498e85d3",
			"decimals": 9,
			"symbol":   "TOSHINORI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xe0bcd056b6a8c7fd4983cb56c162799e498e85d3.png",
		},
		{
			"chain":    "ETH",
			"name":     "2Based Finance",
			"address":  "0x168e39f96a653ce0a456560687241b0b2936e5ff",
			"decimals": 9,
			"symbol":   "2BASED",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x168e39f96a653ce0a456560687241b0b2936e5ff.png",
		},
		{
			"chain":    "ETH",
			"name":     "BankSocial",
			"address":  "0x0af55d5ff28a3269d69b98680fd034f115dd53ac",
			"decimals": 8,
			"symbol":   "BSL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0af55d5ff28a3269d69b98680fd034f115dd53ac.png",
		},
		{
			"chain":    "ETH",
			"name":     "KoaKombat",
			"address":  "0x6769d86f9c430f5ac6d9c861a0173613f1c5544c",
			"decimals": 9,
			"symbol":   "KOACOMBAT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x6769d86f9c430f5ac6d9c861a0173613f1c5544c.png",
		},
		{
			"chain":    "ETH",
			"name":     "Asuna Inu",
			"address":  "0xc656b2279b0fdf761e832133b06ce607fbbcbceb",
			"decimals": 9,
			"symbol":   "ASUNAINU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc656b2279b0fdf761e832133b06ce607fbbcbceb.png",
		},
		{
			"chain":    "ETH",
			"name":     "Incognito",
			"address":  "0xb64fde8f199f073f41c132b9ec7ad5b61de0b1b7",
			"decimals": 9,
			"symbol":   "PRV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb64fde8f199f073f41c132b9ec7ad5b61de0b1b7.png",
		},
		{
			"chain":    "ETH",
			"name":     "XTR",
			"address":  "0x9ee45adbb2f2083ab5cd9bc888c77a662dbd55fe",
			"decimals": 8,
			"symbol":   "XTR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9ee45adbb2f2083ab5cd9bc888c77a662dbd55fe.png",
		},
		{
			"chain":    "ETH",
			"name":     "Pikachu Inu",
			"address":  "0xebf56b58be8339a827f9e9bed5feae3a6c63a562",
			"decimals": 9,
			"symbol":   "PIKACHU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xebf56b58be8339a827f9e9bed5feae3a6c63a562.png",
		},
		{
			"chain":    "ETH",
			"name":     "Shiryo",
			"address":  "0x1e2f15302b90edde696593607b6bd444b64e8f02",
			"decimals": 9,
			"symbol":   "SHIRYO-INU",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1e2f15302b90edde696593607b6bd444b64e8f02.png",
		},
		{
			"chain":    "ETH",
			"name":     "Node Squared",
			"address":  "0x6110c64219621ce5b02fb8e8e57b54c01b83bf85",
			"decimals": 9,
			"symbol":   "N2",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x6110c64219621ce5b02fb8e8e57b54c01b83bf85.png",
		},
		{
			"chain":    "ETH",
			"name":     "UEC",
			"address":  "0x52d04a0a7f3d79880a89cc3ca5de294862f6a0d9",
			"decimals": 18,
			"symbol":   "UEC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x52d04a0a7f3d79880a89cc3ca5de294862f6a0d9.png",
		},
		{
			"chain":    "ETH",
			"name":     "Qredo",
			"address":  "0x4123a133ae3c521fd134d7b13a2dec35b56c2463",
			"decimals": 8,
			"symbol":   "QRDO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4123a133ae3c521fd134d7b13a2dec35b56c2463.png",
		},
		{
			"chain":    "ETH",
			"name":     "VYNK Chain",
			"address":  "0xee1ae38be4ce0074c4a4a8dc821cc784778f378c",
			"decimals": 4,
			"symbol":   "VYNC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xee1ae38be4ce0074c4a4a8dc821cc784778f378c.png",
		},
		{
			"chain":    "ETH",
			"name":     "DCASH",
			"address":  "0xcf8f32e032f432b02393636b2092a6bef975fbf9",
			"decimals": 10,
			"symbol":   "DCASH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xcf8f32e032f432b02393636b2092a6bef975fbf9.png",
		},
		{
			"chain":    "ETH",
			"name":     "Crypto Carbon Energy",
			"address":  "0xeadd9b69f96140283f9ff75da5fd33bcf54e6296",
			"decimals": 6,
			"symbol":   "CYCE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xeadd9b69f96140283f9ff75da5fd33bcf54e6296.png",
		},
		{
			"chain":    "ETH",
			"name":     "SurfExUtilityToken",
			"address":  "0x46d473a0b3eeec9f55fade641bc576d5bc0b2246",
			"decimals": 18,
			"symbol":   "SURF",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x46d473a0b3eeec9f55fade641bc576d5bc0b2246.png",
		},
		{
			"chain":    "ETH",
			"name":     "Drip Inu",
			"address":  "0x4cce2bb41a2b11d7746732df21d48caadb7ab3ba",
			"decimals": 9,
			"symbol":   "ICY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4cce2bb41a2b11d7746732df21d48caadb7ab3ba.png",
		},
		{
			"chain":    "ETH",
			"name":     "HUH",
			"address":  "0x86d49fbd3b6f989d641e700a15599d3b165002ab",
			"decimals": 9,
			"symbol":   "HUH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x86d49fbd3b6f989d641e700a15599d3b165002ab.png",
		},
		{
			"chain":    "ETH",
			"name":     "GM",
			"address":  "0x35609dc59e15d03c5c865507e1348fa5abb319a8",
			"decimals": 8,
			"symbol":   "GM",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x35609dc59e15d03c5c865507e1348fa5abb319a8.png",
		},
		{
			"chain":    "ETH",
			"name":     "GIV",
			"address":  "0xf6537fe0df7f0cc0985cf00792cc98249e73efa0",
			"decimals": 8,
			"symbol":   "GIV",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf6537fe0df7f0cc0985cf00792cc98249e73efa0.png",
		},
		{
			"chain":    "ETH",
			"name":     "RUSH DeFi",
			"address":  "0x0c74e87c123b9a6c61c66468f8718692c1397a53",
			"decimals": 9,
			"symbol":   "RUSH",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0c74e87c123b9a6c61c66468f8718692c1397a53.png",
		},
		{
			"chain":    "ETH",
			"name":     "Saja",
			"address":  "0x646a764b4afca56002956b7157cdcbe98b91bee1",
			"decimals": 9,
			"symbol":   "SJA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x646a764b4afca56002956b7157cdcbe98b91bee1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Doont Buy",
			"address":  "0x4ece5c5cfb9b960a49aae739e15cdb6cfdcc5782",
			"decimals": 9,
			"symbol":   "DBUY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4ece5c5cfb9b960a49aae739e15cdb6cfdcc5782.png",
		},
		{
			"chain":    "ETH",
			"name":     "Mind Music",
			"address":  "0x1cd2528522a17b6be63012fb63ae81f3e3e29d97",
			"decimals": 9,
			"symbol":   "MND",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1cd2528522a17b6be63012fb63ae81f3e3e29d97.png",
		},
		{
			"chain":    "ETH",
			"name":     "Momento",
			"address":  "0x0ae8b74cd2d566853715800c9927f879d6b76a37",
			"decimals": 9,
			"symbol":   "MOMENTO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0ae8b74cd2d566853715800c9927f879d6b76a37.png",
		},
		{
			"chain":    "ETH",
			"name":     "KAVA",
			"address":  "0x0c356b7fd36a5357e5a017ef11887ba100c9ab76",
			"decimals": 6,
			"symbol":   "KAVA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0c356b7fd36a5357e5a017ef11887ba100c9ab76.png",
		},
		{
			"chain":    "ETH",
			"name":     "P2P solutions foundation",
			"address":  "0x4527a3b4a8a150403090a99b87effc96f2195047",
			"decimals": 8,
			"symbol":   "P2PS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4527a3b4a8a150403090a99b87effc96f2195047.png",
		},
		{
			"chain":    "ETH",
			"name":     "BlockNoteX",
			"address":  "0x8b61f7afe322372940dc4512be579f0a55367650",
			"decimals": 2,
			"symbol":   "BNOX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x8b61f7afe322372940dc4512be579f0a55367650.png",
		},
		{
			"chain":    "ETH",
			"name":     "MADworld",
			"address":  "0x31c2415c946928e9fd1af83cdfa38d3edbd4326f",
			"decimals": 8,
			"symbol":   "UMAD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x31c2415c946928e9fd1af83cdfa38d3edbd4326f.png",
		},
		{
			"chain":    "ETH",
			"name":     "ZUSD",
			"address":  "0xc56c2b7e71b54d38aab6d52e94a04cbfa8f604fa",
			"decimals": 6,
			"symbol":   "ZUSD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xc56c2b7e71b54d38aab6d52e94a04cbfa8f604fa.png",
		},
		{
			"chain":    "ETH",
			"name":     "ISLE",
			"address":  "0x1681bcb589b3cfcf0c0616b0ce9b19b240643dc1",
			"decimals": 9,
			"symbol":   "ISLE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1681bcb589b3cfcf0c0616b0ce9b19b240643dc1.png",
		},
		{
			"chain":    "ETH",
			"name":     "Winry Inu",
			"address":  "0x1a87077c4f834884691b8ba4fc808d2ec93a9f30",
			"decimals": 9,
			"symbol":   "WINRY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x1a87077c4f834884691b8ba4fc808d2ec93a9f30.png",
		},
		{
			"chain":    "ETH",
			"name":     "WAGMI",
			"address":  "0x9724f51e3afb6b2ae0a5d86fd3b88c73283bc38f",
			"decimals": 9,
			"symbol":   "WAGMI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9724f51e3afb6b2ae0a5d86fd3b88c73283bc38f.png",
		},
		{
			"chain":    "ETH",
			"name":     "NULS",
			"address":  "0xa2791bdf2d5055cda4d46ec17f9f429568275047",
			"decimals": 8,
			"symbol":   "NULS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xa2791bdf2d5055cda4d46ec17f9f429568275047.png",
		},
		{
			"chain":    "ETH",
			"name":     "Hellsing Inu",
			"address":  "0xb087c2180e3134db396977065817aed91fea6ead",
			"decimals": 9,
			"symbol":   "HELLSING",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb087c2180e3134db396977065817aed91fea6ead.png",
		},
		{
			"chain":    "ETH",
			"name":     "Rizespor Token",
			"address":  "0xf5bb30ebc95dca53e3320eb05d3d1bcab806b9bf",
			"decimals": 2,
			"symbol":   "RIZE",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf5bb30ebc95dca53e3320eb05d3d1bcab806b9bf.png",
		},
		{
			"chain":    "ETH",
			"name":     "Luffy",
			"address":  "0x7121d00b4fa18f13da6c2e30d19c04844e6afdc8",
			"decimals": 9,
			"symbol":   "LUFFY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x7121d00b4fa18f13da6c2e30d19c04844e6afdc8.png",
		},
		{
			"chain":    "ETH",
			"name":     "ArdCoin",
			"address":  "0xb8e2e2101ed11e9138803cd3e06e16dd19910647",
			"decimals": 2,
			"symbol":   "ARDX",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xb8e2e2101ed11e9138803cd3e06e16dd19910647.png",
		},
		{
			"chain":    "ETH",
			"name":     "Centurion Inu",
			"address":  "0x9f91d9f9070b0478abb5a9918c79b5dd533f672c",
			"decimals": 9,
			"symbol":   "CENT",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x9f91d9f9070b0478abb5a9918c79b5dd533f672c.png",
		},
		{
			"chain":    "ETH",
			"name":     "Atomic Wallet Coin",
			"address":  "0xad22f63404f7305e4713ccbd4f296f34770513f4",
			"decimals": 8,
			"symbol":   "AWC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xad22f63404f7305e4713ccbd4f296f34770513f4.png",
		},
		{
			"chain":    "ETH",
			"name":     "GCD",
			"address":  "0x213ecae6b3cbc0ad976f7d82626546d5b63a71cb",
			"decimals": 18,
			"symbol":   "GCD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x213ecae6b3cbc0ad976f7d82626546d5b63a71cb.png",
		},
		{
			"chain":    "ETH",
			"name":     "Phantasma",
			"address":  "0x75858677e27c930fb622759feaffee2b754af07f",
			"decimals": 8,
			"symbol":   "SOUL",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x75858677e27c930fb622759feaffee2b754af07f.png",
		},
		{
			"chain":    "ETH",
			"name":     "Xave Coin",
			"address":  "0x4fabf135bcf8111671870d4399af739683198f96",
			"decimals": 18,
			"symbol":   "XVC",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x4fabf135bcf8111671870d4399af739683198f96.png",
		},
		{
			"chain":    "ETH",
			"name":     "STIMA",
			"address":  "0xd2e5decc08a80be6538f89f9ab8ff296e2f724df",
			"decimals": 6,
			"symbol":   "STIMA",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xd2e5decc08a80be6538f89f9ab8ff296e2f724df.png",
		},
		{
			"chain":    "ETH",
			"name":     "Alien Inu",
			"address":  "0xf42965f82f9e3171d1205c5e3058caf324a09432",
			"decimals": 9,
			"symbol":   "ALIEN",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xf42965f82f9e3171d1205c5e3058caf324a09432.png",
		},
		{
			"chain":    "ETH",
			"name":     "Hacken HAI",
			"address":  "0x05fb86775fd5c16290f1e838f5caaa7342bd9a63",
			"decimals": 8,
			"symbol":   "HAI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x05fb86775fd5c16290f1e838f5caaa7342bd9a63.png",
		},
		{
			"chain":    "ETH",
			"name":     "Undead Blocks",
			"address":  "0x310c8f00b9de3c31ab95ea68feb6c877538f7947",
			"decimals": 18,
			"symbol":   "UNDEAD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x310c8f00b9de3c31ab95ea68feb6c877538f7947.png",
		},
		{
			"chain":    "ETH",
			"name":     "Baby Floki Doge",
			"address":  "0x747c4ce9622ea750ea8048423b38a746b096c8e8",
			"decimals": 9,
			"symbol":   "BABYFD",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x747c4ce9622ea750ea8048423b38a746b096c8e8.png",
		},
		{
			"chain":    "ETH",
			"name":     "Pist Trust",
			"address":  "0x315dc1b524de57ae8e809a2e97699dbc895b8a21",
			"decimals": 9,
			"symbol":   "PIST",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x315dc1b524de57ae8e809a2e97699dbc895b8a21.png",
		},
		{
			"chain":    "ETH",
			"name":     "SafeBlast",
			"address":  "0x614d7f40701132e25fe6fc17801fbd34212d2eda",
			"decimals": 9,
			"symbol":   "BLAST",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x614d7f40701132e25fe6fc17801fbd34212d2eda.png",
		},
		{
			"chain":    "ETH",
			"name":     "Slougi",
			"address":  "0x2656f02bc30427ed9d380e20cec5e04f5a7a50fe",
			"decimals": 9,
			"symbol":   "SLOUGI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x2656f02bc30427ed9d380e20cec5e04f5a7a50fe.png",
		},
		{
			"chain":    "ETH",
			"name":     "Buying.com",
			"address":  "0x396ec402b42066864c406d1ac3bc86b575003ed8",
			"decimals": 2,
			"symbol":   "BUY",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x396ec402b42066864c406d1ac3bc86b575003ed8.png",
		},
		{
			"chain":    "ETH",
			"name":     "Wrapped ATROMG8",
			"address":  "0x0a8b16b27d5219c8c6b57d5442ce31d81573eee4",
			"decimals": 8,
			"symbol":   "WAG8",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x0a8b16b27d5219c8c6b57d5442ce31d81573eee4.png",
		},
		{
			"chain":    "ETH",
			"name":     "Adana Demirspor",
			"address":  "0xfec82a1b2638826bfe53ae2f87cfd94329cde60d",
			"decimals": 2,
			"symbol":   "DEMIR",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xfec82a1b2638826bfe53ae2f87cfd94329cde60d.png",
		},
		{
			"chain":    "ETH",
			"name":     "AstroSpaces.io",
			"address":  "0x29ced58cd96f523f86f2771d79d64a8f58f55f5e",
			"decimals": 9,
			"symbol":   "SPACES",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x29ced58cd96f523f86f2771d79d64a8f58f55f5e.png",
		},
		{
			"chain":    "ETH",
			"name":     "Foho Coin",
			"address":  "0xdd2e93924bdd4e20c3cf4a8736e5955224fa450e",
			"decimals": 8,
			"symbol":   "FOHO",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0xdd2e93924bdd4e20c3cf4a8736e5955224fa450e.png",
		},
		{
			"chain":    "ETH",
			"name":     "Medi",
			"address":  "0x24b20da7a2fa0d1d5afcd693e1c8afff20507efd",
			"decimals": 9,
			"symbol":   "MEDI",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x24b20da7a2fa0d1d5afcd693e1c8afff20507efd.png",
		},
		{
			"chain":    "ETH",
			"name":     "Bytus",
			"address":  "0x87f14e9460cecb789f1b125b2e3e353ff8ed6fcd",
			"decimals": 3,
			"symbol":   "BYTS",
			"logoURI":  "https://obstatic.243096.com/download/tokentest/images/ethereum/ethereum_0x87f14e9460cecb789f1b125b2e3e353ff8ed6fcd.png",
		},
	}

	arbitrumDataMap := make(map[string]string)
	for _, data := range arbitrumData {
		address := data["address"].(string)
		arbitrumDataMap[address] = data["symbol"].(string)
	}

	solanaDataMap := make(map[string]string)
	for _, data := range solanaData {
		address := data["address"].(string)
		solanaDataMap[address] = data["symbol"].(string)
	}

	ethDataMap := make(map[string]string)
	for _, data := range ethData {
		address := data["address"].(string)
		ethDataMap[address] = data["symbol"].(string)
	}

	for _, tokenParam := range arbitrumTokenParam {
		symbol := arbitrumDataMap[tokenParam.TokenAddress]
		tokenParam.OldSymbol = symbol
		tokenParam.NewSymbol = symbol
	}

	for _, tokenParam := range solanaTokenParam {
		symbol := solanaDataMap[tokenParam.TokenAddress]
		tokenParam.OldSymbol = symbol
		tokenParam.NewSymbol = symbol
	}

	for _, tokenParam := range ethTokenParam {
		symbol := ethDataMap[tokenParam.TokenAddress]
		tokenParam.OldSymbol = symbol
		tokenParam.NewSymbol = symbol
	}

	tokenParamMap["Arbitrum"] = arbitrumTokenParam
	tokenParamMap["Solana"] = solanaTokenParam
	tokenParamMap["ETH"] = ethTokenParam

	source := biz.AppConfig.Target
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}
	limit := biz.PAGE_SIZE

	for chainName, tokenParams := range tokenParamMap {
		chainType, _ := biz.GetChainNameType(chainName)
		switch chainType {
		case biz.EVM:
			for _, tokenParam := range tokenParams {
				if tokenParam.TokenAddress != "" {
					tokenParam.TokenAddress = types2.HexToAddress(tokenParam.TokenAddress).Hex()
				}
			}
		}
		/*for _, tokenParam := range tokenParams {
			fmt.Printf("('" + chainName + "', '" + tokenParam.TokenAddress + "', %v),\n", tokenParam.OldDecimals)
		}*/
		log.Info("处理交易记录TokenInfo中", zap.Any("chainName", chainName))
		doHandleTokenInfo(dbSource, chainName, tokenParams, limit)
		log.Info("处理交易记录TokenInfo完成", zap.Any("chainName", chainName))
	}
	log.Info("处理交易记录TokenInfo完成", zap.Any("chain size", len(tokenParamMap)))
}

func doHandleTokenInfo(dbSource *gorm.DB, chainName string, tokenParams []*TokenParam, limit int) {
	log.Info("处理交易记录TokenInfo开始", zap.Any("chainName", chainName))
	tokenParamMap := make(map[string]*TokenParam)
	var txRecords []*TxRecord
	for _, tokenParam := range tokenParams {
		tokenParamMap[tokenParam.TokenAddress] = tokenParam
		txRecordList, err := getTxRecord(dbSource, chainName, tokenParam, limit)
		if err != nil {
			log.Errore("migrate page query txRecord failed", err)
			return
		}
		if len(txRecordList) > 0 {
			txRecords = append(txRecords, txRecordList...)
		}
	}
	if len(txRecords) == 0 {
		log.Info("没有查到要处理的交易记录TokenInfo，处理结束")
		return
	}

	for _, txRecord := range txRecords {
		parseDataStr := txRecord.ParseData
		if parseDataStr != "" {
			tokenInfo, err := biz.ParseTokenInfo(parseDataStr)
			if err != nil {
				log.Error("parse TokenInfo failed", zap.Any("parseData", parseDataStr), zap.Any("error", err))
				continue
			}
			address := tokenInfo.Address
			tokenParam, ok := tokenParamMap[address]
			if ok {
				oldParseData := "\"decimals\":" + strconv.Itoa(tokenParam.OldDecimals) + ",\"symbol\":\"" + tokenParam.OldSymbol + "\""
				newParseData := "\"decimals\":" + strconv.Itoa(tokenParam.NewDecimals) + ",\"symbol\":\"" + tokenParam.NewSymbol + "\""
				parseDataStr = strings.ReplaceAll(parseDataStr, oldParseData, newParseData)
				txRecord.ParseData = parseDataStr
			}
		}

		eventLogStr := txRecord.EventLog
		if eventLogStr != "" {
			var eventLogs []*types.EventLog
			err := json.Unmarshal([]byte(eventLogStr), &eventLogs)
			if err != nil {
				log.Error("parse EventLog failed", zap.Any("eventLog", eventLogStr), zap.Any("error", err))
				continue
			}
			for _, eventLog := range eventLogs {
				address := eventLog.Token.Address
				tokenParam, ok := tokenParamMap[address]
				if ok {
					eventLog.Token.Decimals = int64(tokenParam.NewDecimals)
					eventLog.Token.Symbol = tokenParam.NewSymbol
				}
			}
			eventLogJson, _ := utils.JsonEncode(eventLogs)
			txRecord.EventLog = eventLogJson
		}
	}

	count, err := pageBatchUpdateSelectiveById(chainName, txRecords, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = pageBatchUpdateSelectiveById(chainName, txRecords, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("处理交易记录TokenInfo，更新交易记录，将数据插入到数据库中失败", zap.Any("size", len(txRecords)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("处理交易记录TokenInfo结束", zap.Any("query size", len(txRecords)), zap.Any("affected count", count))
}

func getTxRecord(dbSource *gorm.DB, chainName string, tokenParam *TokenParam, limit int) ([]*TxRecord, error) {
	tableName := biz.GetTableName(chainName)
	chainType, _ := biz.GetChainNameType(chainName)
	var sqlStr string

	var txRecords []*TxRecord
	var txRecordList []*TxRecord
	id := 0
	total := limit
	for total == limit {
		if chainType == biz.BTC || chainType == biz.KASPA || chainType == biz.CASPER {
			sqlStr = getSqlNoEventLog(tableName, tokenParam.TokenAddress, tokenParam.OldDecimals, tokenParam.OldSymbol, id, limit)
		} else {
			sqlStr = getSql(tableName, tokenParam.TokenAddress, tokenParam.OldDecimals, tokenParam.OldSymbol, id, limit)
		}
		ret := dbSource.Raw(sqlStr).Find(&txRecordList)
		err := ret.Error
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			ret = dbSource.Raw(sqlStr).Find(&txRecordList)
			err = ret.Error
		}
		if err != nil {
			log.Error("migrate page query txRecord failed", zap.Any("tokenParam", tokenParam), zap.Any("error", err))
			return nil, err
		}
		total = len(txRecordList)
		if total > 0 {
			txRecord := txRecordList[total-1]
			id = int(txRecord.Id)
			txRecords = append(txRecords, txRecordList...)
		}
	}
	return txRecords, nil
}

func getSql(tableName string, tokenAdress string, decimals int, symbol string, id, limit int) string {
	s := "SELECT id, parse_data, event_log from " + tableName +
		" where ((parse_data like '%" + tokenAdress + "%' and parse_data like '%\"decimals\":" + strconv.Itoa(decimals) + ",\"symbol\":\"" + symbol + "\"%') " +
		"or (event_log like '%" + tokenAdress + "%' and event_log like '%\"decimals\":" + strconv.Itoa(decimals) + ",\"symbol\":\"" + symbol + "\"%')) " +
		"and id > " + strconv.Itoa(id) + " order by id asc limit " + strconv.Itoa(limit) + ";"
	return s
}

func getSqlNoEventLog(tableName string, tokenAdress string, decimals int, symbol string, id, limit int) string {
	s := "SELECT id, parse_data from " + tableName +
		" where ((parse_data like '%" + tokenAdress + "%' and parse_data like '%\"decimals\":" + strconv.Itoa(decimals) + ",\"symbol\":\"" + symbol + "\"%')) " +
		"and id > " + strconv.Itoa(id) + " order by id asc limit " + strconv.Itoa(limit) + ";"
	return s
}

func HandleTokenUri() {
	log.Info("处理交易记录TokenInfo中tokenUri开始")
	source := biz.AppConfig.Target
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}
	limit := biz.PAGE_SIZE

	for chainName, _ := range biz.GetChainNameTypeMap() {
		log.Info("处理交易记录TokenInfo中tokenUri中", zap.Any("chainName", chainName))
		doHandleTokenUri(dbSource, chainName, limit)
		log.Info("处理交易记录TokenInfo中tokenUri完成", zap.Any("chainName", chainName))
	}
	log.Info("处理交易记录TokenInfo中tokenUri结束")
}

func doHandleTokenUri(dbSource *gorm.DB, chainName string, limit int) {
	log.Info("处理交易记录TokenInfo中tokenUri开始", zap.Any("chainName", chainName))
	txRecords, err := tokenUriGetTxRecord(dbSource, chainName, limit)
	if err != nil {
		log.Errore("migrate page query txRecord failed", err)
		return
	}
	if len(txRecords) == 0 {
		log.Info("没有查到要处理的交易记录TokenInfo中tokenUri，处理结束")
		return
	}

	for _, txRecord := range txRecords {
		parseDataStr := txRecord.ParseData
		if parseDataStr != "" {
			tokenInfo, err := biz.ParseTokenInfo(parseDataStr)
			if err != nil {
				log.Error("parse TokenInfo failed", zap.Any("parseData", parseDataStr), zap.Any("error", err))
				continue
			}
			tokenAddress := tokenInfo.Address
			if tokenAddress != "" && tokenInfo.TokenType == "" && tokenInfo.TokenUri == "" {
				token, err := biz.GetTokenInfoRetryAlert(nil, chainName, tokenAddress)
				if err != nil {
					log.Error("处理交易记录TokenInfo中tokenUri，从nodeProxy中获取代币精度失败", zap.Any("chainName", chainName), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
					continue
				}
				oldParseData := "\"}}"
				newParseData := "\",\"token_uri\":\"" + token.TokenUri + "\"}}"
				parseDataStr = strings.ReplaceAll(parseDataStr, oldParseData, newParseData)
				txRecord.ParseData = parseDataStr
			}
		}

		eventLogStr := txRecord.EventLog
		if eventLogStr != "" {
			var eventLogs []*types.EventLog
			err := json.Unmarshal([]byte(eventLogStr), &eventLogs)
			if err != nil {
				log.Error("parse EventLog failed", zap.Any("eventLog", eventLogStr), zap.Any("error", err))
				continue
			}
			for _, eventLog := range eventLogs {
				tokenAddress := eventLog.Token.Address
				if tokenAddress != "" && eventLog.Token.TokenType == "" && eventLog.Token.TokenUri == "" {
					token, err := biz.GetTokenInfoRetryAlert(nil, chainName, tokenAddress)
					if err != nil {
						log.Error("处理交易记录TokenInfo中tokenUri，从nodeProxy中获取代币精度失败", zap.Any("chainName", chainName), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						continue
					}
					eventLog.Token.TokenUri = token.TokenUri
				}
			}
			eventLogJson, _ := utils.JsonEncode(eventLogs)
			txRecord.EventLog = eventLogJson
		}
	}

	count, err := pageBatchUpdateSelectiveById(chainName, txRecords, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = pageBatchUpdateSelectiveById(chainName, txRecords, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("处理交易记录TokenInfo中tokenUri，更新交易记录，将数据插入到数据库中失败", zap.Any("size", len(txRecords)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("处理交易记录TokenInfo中tokenUri结束", zap.Any("query size", len(txRecords)), zap.Any("affected count", count))
}

func tokenUriGetTxRecord(dbSource *gorm.DB, chainName string, limit int) ([]*TxRecord, error) {
	tableName := biz.GetTableName(chainName)
	chainType, _ := biz.GetChainNameType(chainName)
	var sqlStr string

	var txRecords []*TxRecord
	var txRecordList []*TxRecord
	id := 0
	total := limit
	for total == limit {
		if chainType == biz.BTC || chainType == biz.KASPA || chainType == biz.CASPER {
			sqlStr = tokenUriGetSqlNoEventLog(tableName, id, limit)
		} else {
			sqlStr = tokenUriGetSql(tableName, id, limit)
		}
		ret := dbSource.Raw(sqlStr).Find(&txRecordList)
		err := ret.Error
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			ret = dbSource.Raw(sqlStr).Find(&txRecordList)
			err = ret.Error
		}
		if err != nil {
			log.Error("migrate page query txRecord failed", zap.Any("chainName", chainName), zap.Any("error", err))
			return nil, err
		}
		total = len(txRecordList)
		if total > 0 {
			txRecord := txRecordList[total-1]
			id = int(txRecord.Id)
			txRecords = append(txRecords, txRecordList...)
		}
	}
	return txRecords, nil
}

func tokenUriGetSql(tableName string, id, limit int) string {
	s := "SELECT id, parse_data, event_log from " + tableName +
		" where transaction_type not in('native', 'directTransferNFTSwitch', 'createAccount', 'closeAccount') " +
		"and id > " + strconv.Itoa(id) + " order by id asc limit " + strconv.Itoa(limit) + ";"
	return s
}

func tokenUriGetSqlNoEventLog(tableName string, id, limit int) string {
	s := "SELECT id, parse_data from " + tableName +
		" where transaction_type not in('native', 'directTransferNFTSwitch', 'createAccount', 'closeAccount') " +
		"and id > " + strconv.Itoa(id) + " order by id asc limit " + strconv.Itoa(limit) + ";"
	return s
}

func pageBatchUpdateSelectiveById(chainName string, txRecords []*TxRecord, pageSize int) (int64, error) {
	tableName := biz.GetTableName(chainName)
	chainType, _ := biz.GetChainNameType(chainName)

	var count int64 = 0
	var err error
	switch chainType {
	case biz.EVM:
		var txRecordList []*data.EvmTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.EvmTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
				EventLog:  record.EventLog,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.STC:
		var txRecordList []*data.StcTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.StcTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.StcTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.TVM:
		var txRecordList []*data.TrxTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.TrxTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.TrxTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.APTOS:
		var txRecordList []*data.AptTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.AptTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
				EventLog:  record.EventLog,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.AptTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.SUI:
		var txRecordList []*data.SuiTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.SuiTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
				EventLog:  record.EventLog,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.SuiTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.SOLANA:
		var txRecordList []*data.SolTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.SolTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
				EventLog:  record.EventLog,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.SolTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.COSMOS:
		var txRecordList []*data.AtomTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.AtomTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
				EventLog:  record.EventLog,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.AtomTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.NERVOS:
		var txRecordList []*data.CkbTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.CkbTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
				EventLog:  record.EventLog,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.CkbTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.POLKADOT:
		var txRecordList []*data.DotTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.DotTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
				EventLog:  record.EventLog,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.DotTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	case biz.CASPER:
		var txRecordList []*data.CsprTransactionRecord
		for _, record := range txRecords {
			txRecord := &data.CsprTransactionRecord{
				Id:        record.Id,
				ParseData: record.ParseData,
			}
			txRecordList = append(txRecordList, txRecord)
		}
		count, err = data.CsprTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecordList, pageSize)
	}
	return count, err
}

func UpdateAsset() {
	assetRequest := &data.AssetRequest{
		TokenAddressList: []string{""},
	}
	list, err := data.UserAssetRepoClient.List(nil, assetRequest)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		list, err = data.UserAssetRepoClient.List(nil, assetRequest)
	}
	if err != nil {
		log.Error("更新用户资产，从数据库中查询用户资产失败", zap.Any("error", err))
		return
	}
	if len(list) == 0 {
		log.Info("更新用户资产，从数据库中查询用户资产为空", zap.Any("size", len(list)))
		return
	}

	for _, userAsset := range list {
		if platInfo, ok := biz.GetChainPlatInfo(userAsset.ChainName); ok {
			userAsset.Decimals = platInfo.Decimal
			userAsset.Symbol = platInfo.NativeCurrency
		}
	}

	count, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, list, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, list, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("更新用户资产，将数据插入到数据库中失败", zap.Any("size", len(list)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("从userAsset中更新用户资产结束")
}

func DeleteAndUpdateAsset() {
	assetRequest := &data.AssetRequest{
		TokenAddressList: []string{"0x0000000000000000000000000000000000000000"},
	}
	list, err := data.UserAssetRepoClient.List(nil, assetRequest)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		list, err = data.UserAssetRepoClient.List(nil, assetRequest)
	}
	if err != nil {
		log.Error("删数据更新用户资产，从数据库中查询用户资产失败", zap.Any("error", err))
		return
	}
	if len(list) == 0 {
		log.Info("删数据更新用户资产，从数据库中查询用户资产为空", zap.Any("size", len(list)))
		return
	}

	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("DeleteAndUpdateAsset error", e)
			} else {
				log.Errore("DeleteAndUpdateAsset panic", errors.New(fmt.Sprintf("%s", err)))
			}

			for _, userAsset := range list {
				userAsset.TokenAddress = "0x0000000000000000000000000000000000000000"
			}
			count, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, list, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, list, biz.PAGE_SIZE)
			}
			log.Error("DeleteAndUpdateAsset 更新用户资产，将数据插入到数据库中失败", zap.Any("size", len(list)), zap.Any("count", count), zap.Any("error", err))
			return
		}
	}()

	var userAssetList []int64
	for _, userAsset := range list {
		userAssetList = append(userAssetList, userAsset.Id)
	}

	if len(userAssetList) > 0 {
		total, err := data.UserAssetRepoClient.DeleteByIDs(nil, userAssetList)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			total, err = data.UserAssetRepoClient.DeleteByIDs(nil, userAssetList)
		}
		if err != nil {
			log.Error("删数据，从数据库中删数用户资产失败", zap.Any("size", len(userAssetList)), zap.Any("total", total), zap.Any("error", err))
		}
	}
	log.Info("删数据结束", zap.Any("size", len(userAssetList)))

	for _, userAsset := range list {
		userAsset.TokenAddress = ""
	}
	userAssets := list

	log.Info("补数据更新用户资产中", zap.Any("size", len(userAssets)))
	times := 0
	for times < 3 {
		userAssets = doHandleAsset(userAssets)
		if len(userAssets) > 0 {
			time.Sleep(time.Duration(300) * time.Second)
		} else {
			break
		}
		times++
	}

	if len(userAssets) > 0 {
		for _, userAsset := range userAssets {
			userAsset.TokenAddress = "0x0000000000000000000000000000000000000000"
		}

		count, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			count, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
		}
		log.Error("更新用户资产，将数据插入到数据库中失败", zap.Any("size", len(list)), zap.Any("count", count), zap.Any("error", err))
	}

	log.Info("从userAsset中补数据更新用户资产结束, times:" + strconv.Itoa(times))
}

func DeleteAsset() {
	list, err := data.UserAssetRepoClient.ListAll(nil)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		list, err = data.UserAssetRepoClient.ListAll(nil)
	}
	if err != nil {
		log.Error("删数据更新用户资产，从数据库中查询用户资产失败", zap.Any("error", err))
		return
	}

	var userAssetList []int64
	for _, userAsset := range list {
		userAddress, _, err := biz.UserAddressSwitchRetryAlert(userAsset.ChainName, userAsset.Address)
		for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			userAddress, _, err = biz.UserAddressSwitchRetryAlert(userAsset.ChainName, userAsset.Address)
		}
		if err == nil {
			if userAddress {
				chainType, _ := biz.GetChainNameType(userAsset.ChainName)
				switch chainType {
				case biz.EVM:
					address := userAsset.Address
					if userAsset.Address != "" {
						address = types2.HexToAddress(userAsset.Address).Hex()
					}
					tokenAddress := userAsset.TokenAddress
					if userAsset.TokenAddress != "" {
						tokenAddress = types2.HexToAddress(userAsset.TokenAddress).Hex()
					}
					if address != userAsset.Address || tokenAddress != userAsset.TokenAddress {
						log.Info("DELETING USER ASSET", zap.String("chain", userAsset.ChainName), zap.String("address", userAsset.Address), zap.String("tokenAddress", userAsset.TokenAddress))
						userAssetList = append(userAssetList, userAsset.Id)
					}
				case biz.APTOS:
					address := userAsset.Address
					if userAsset.Address != "" {
						address = utils.AddressAdd0(address)
					}
					if address != userAsset.Address {
						log.Info("DELETING USER ASSET", zap.String("chain", userAsset.ChainName), zap.String("address", userAsset.Address), zap.String("tokenAddress", userAsset.TokenAddress))
						userAssetList = append(userAssetList, userAsset.Id)
					}
				}
			} else {
				log.Info("DELETING USER ASSET", zap.String("chain", userAsset.ChainName), zap.String("address", userAsset.Address), zap.String("tokenAddress", userAsset.TokenAddress))
				userAssetList = append(userAssetList, userAsset.Id)
			}
		}
	}

	if len(userAssetList) > 0 {
		total, err := data.UserAssetRepoClient.DeleteByIDs(nil, userAssetList)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			total, err = data.UserAssetRepoClient.DeleteByIDs(nil, userAssetList)
		}
		if err != nil {
			log.Error("删数据更新用户资产，从数据库中删数用户资产失败", zap.Any("size", len(userAssetList)), zap.Any("total", total), zap.Any("error", err))
		}
	}
	log.Info("删数据更新用户资产", zap.Any("size", len(userAssetList)))
}

type EvmTxRecord struct {
	ChainName string `json:"chainName" form:"chainName"`
	EventLog  string `json:"eventLog" form:"eventLog"`
}

func HandleAssetByEventLog() {
	log.Info("补数据更新用户资产开始")
	source := biz.AppConfig.Target
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}

	var evmTxRecordList []*EvmTxRecord

	sqlStr := "select chain_name, event_log from (" +
		"    select 'Avalanche' as chain_name, event_log from avalanche_transaction_record " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'BSC' as chain_name, event_log from bsc_transaction_record       " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'Cronos' as chain_name, event_log from cronos_transaction_record    " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'ETC' as chain_name, event_log from etc_transaction_record       " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'ETH' as chain_name, event_log from eth_transaction_record       " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'ETHW' as chain_name, event_log from ethw_transaction_record      " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'Fantom' as chain_name, event_log from fantom_transaction_record    " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'HECO' as chain_name, event_log from heco_transaction_record      " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'Klaytn' as chain_name, event_log from klaytn_transaction_record    " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'OEC' as chain_name, event_log from oec_transaction_record       " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'Optimism' as chain_name, event_log from optimism_transaction_record  " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'Polygon' as chain_name, event_log from polygon_transaction_record   " +
		"    where transaction_type = 'contract'" +
		"    union all" +
		"    select 'xDai' as chain_name, event_log from xdai_transaction_record      " +
		"    where transaction_type = 'contract'" +
		") as t;"

	ret := dbSource.Raw(sqlStr).Find(&evmTxRecordList)
	err = ret.Error
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		ret = dbSource.Raw(sqlStr).Find(&evmTxRecordList)
		err = ret.Error
	}
	if err != nil {
		log.Errore("migrate page query userAsset failed", err)
		return
	}

	var userAssetMap = make(map[string]*data.UserAsset)
	for _, evmTxRecord := range evmTxRecordList {
		eventLogStr := evmTxRecord.EventLog
		if eventLogStr == "" {
			continue
		}
		eventLogList := []types.EventLog{}
		err = json.Unmarshal([]byte(eventLogStr), &eventLogList)
		if err != nil {
			log.Errore("migrate page query userAsset failed", err)
			continue
		}

		for _, eventLog := range eventLogList {
			fromAddress := eventLog.From
			toAddress := eventLog.To
			tokenAddress := eventLog.Token.Address
			if eventLog.From != "" {
				fromAddress = types2.HexToAddress(eventLog.From).Hex()
			}
			if eventLog.To != "" {
				toAddress = types2.HexToAddress(eventLog.To).Hex()
			}
			if eventLog.Token.Address != "" {
				tokenAddress = types2.HexToAddress(eventLog.Token.Address).Hex()
			}

			key := evmTxRecord.ChainName + fromAddress + tokenAddress
			_, ok := userAssetMap[key]
			if !ok {
				userFromAddress, fromUid, err := biz.UserAddressSwitchRetryAlert(evmTxRecord.ChainName, fromAddress)
				for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					userFromAddress, fromUid, err = biz.UserAddressSwitchRetryAlert(evmTxRecord.ChainName, fromAddress)
				}
				if err == nil {
					if userFromAddress {
						userAsset := &data.UserAsset{
							ChainName:    evmTxRecord.ChainName,
							Uid:          fromUid,
							Address:      fromAddress,
							TokenAddress: tokenAddress,
							Decimals:     int32(eventLog.Token.Decimals),
							Symbol:       eventLog.Token.Symbol,
						}
						userAssetMap[key] = userAsset
					}
				}
			}

			key = evmTxRecord.ChainName + toAddress + tokenAddress
			_, ok = userAssetMap[key]
			if !ok {
				userToAddress, toUid, err := biz.UserAddressSwitchRetryAlert(evmTxRecord.ChainName, toAddress)
				for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					userToAddress, toUid, err = biz.UserAddressSwitchRetryAlert(evmTxRecord.ChainName, toAddress)
				}
				if err == nil {
					if userToAddress {
						userAsset := &data.UserAsset{
							ChainName:    evmTxRecord.ChainName,
							Uid:          toUid,
							Address:      toAddress,
							TokenAddress: tokenAddress,
							Decimals:     int32(eventLog.Token.Decimals),
							Symbol:       eventLog.Token.Symbol,
						}
						userAssetMap[key] = userAsset
					}
				}
			}
		}
	}

	var userAssets []*data.UserAsset
	for _, userAsset := range userAssetMap {
		userAssets = append(userAssets, userAsset)
	}

	log.Info("补数据更新用户资产中", zap.Any("size", len(userAssets)))
	times := 0
	for times < 3 {
		userAssets = doHandleAsset(userAssets)
		if len(userAssets) > 0 {
			time.Sleep(time.Duration(300) * time.Second)
		} else {
			break
		}
		times++
	}

	log.Info("从eventLog中补数据更新用户资产结束, times:" + strconv.Itoa(times))
}

func HandleAsset() {
	log.Info("补数据更新用户资产开始")
	source := biz.AppConfig.Target
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}

	var userAssetList []*data.UserAsset

	sqlStr := "with t as(" +
		"    select chain_name, from_uid, from_address, to_uid, to_address, contract_address from (" +
		"    select 'BTC' as chain_name, from_uid, from_address, to_uid, to_address, '' as contract_address from btc_transaction_record       " +
		"    union all" +
		"    select 'DOGE' as chain_name, from_uid, from_address, to_uid, to_address, '' as contract_address from doge_transaction_record      " +
		"    union all" +
		"    select 'LTC' as chain_name, from_uid, from_address, to_uid, to_address, '' as contract_address from ltc_transaction_record       " +
		"    union all" +
		"    select 'Avalanche' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from avalanche_transaction_record " +
		"    union all" +
		"    select 'BSC' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from bsc_transaction_record       " +
		"    union all" +
		"    select 'Cronos' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from cronos_transaction_record    " +
		"    union all" +
		"    select 'ETC' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from etc_transaction_record       " +
		"    union all" +
		"    select 'ETH' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from eth_transaction_record       " +
		"    union all" +
		"    select 'ETHW' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from ethw_transaction_record      " +
		"    union all" +
		"    select 'Fantom' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from fantom_transaction_record    " +
		"    union all" +
		"    select 'HECO' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from heco_transaction_record      " +
		"    union all" +
		"    select 'Klaytn' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from klaytn_transaction_record    " +
		"    union all" +
		"    select 'OEC' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from oec_transaction_record       " +
		"    union all" +
		"    select 'Optimism' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from optimism_transaction_record  " +
		"    union all" +
		"    select 'Polygon' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from polygon_transaction_record   " +
		"    union all" +
		"    select 'STC' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from stc_transaction_record      " +
		"    union all" +
		"    select 'TRX' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from trx_transaction_record       " +
		"    union all" +
		"    select 'xDai' as chain_name, from_uid, from_address, to_uid, to_address, contract_address from xdai_transaction_record      " +
		"    ) as t" +
		"    group by chain_name, from_uid, from_address, to_uid, to_address, contract_address" +
		")" +
		"select chain_name, uid, address, contract_address as token_address from (" +
		"	select chain_name, from_uid as uid, from_address as address, contract_address from t" +
		"	where from_address != '' and from_uid != ''" +
		"	union all" +
		"	select chain_name, to_uid as uid, to_address as address, contract_address from t" +
		"	where to_address != '' and to_uid != ''" +
		") as tt " +
		"group by chain_name, uid, address, contract_address;"

	ret := dbSource.Raw(sqlStr).Find(&userAssetList)
	err = ret.Error
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		ret = dbSource.Raw(sqlStr).Find(&userAssetList)
		err = ret.Error
	}
	if err != nil {
		log.Errore("migrate page query userAsset failed", err)
		return
	}

	var userAssetMap = make(map[string]*data.UserAsset)
	for _, userAsset := range userAssetList {
		chainType, _ := biz.GetChainNameType(userAsset.ChainName)
		switch chainType {
		case biz.EVM:
			if userAsset.Address != "" {
				userAsset.Address = types2.HexToAddress(userAsset.Address).Hex()
			}
			if userAsset.TokenAddress != "" {
				userAsset.TokenAddress = types2.HexToAddress(userAsset.TokenAddress).Hex()
			}
		case biz.STC:
			if userAsset.TokenAddress == starcoin.STC_CODE {
				userAsset.TokenAddress = ""
			}
		case biz.APTOS:
			if userAsset.TokenAddress == aptos.APT_CODE {
				userAsset.TokenAddress = ""
			}
		}

		key := userAsset.ChainName + userAsset.Address + userAsset.TokenAddress
		asset, ok := userAssetMap[key]
		if !ok {
			userAssetMap[key] = userAsset
		} else {
			log.Warn("补数据更新用户资产uid冲突", zap.Any("oldAsset", asset), zap.Any("newAsset", userAsset))
		}
	}

	var userAssets []*data.UserAsset
	for _, userAsset := range userAssetMap {
		userAssets = append(userAssets, userAsset)
	}

	log.Info("补数据更新用户资产中", zap.Any("size", len(userAssets)))
	times := 0
	for times < 3 {
		userAssets = doHandleAsset(userAssets)
		if len(userAssets) > 0 {
			time.Sleep(time.Duration(300) * time.Second)
		} else {
			break
		}
		times++
	}

	log.Info("补数据更新用户资产结束, times:" + strconv.Itoa(times))
}

func doHandleAsset(userAssetList []*data.UserAsset) []*data.UserAsset {
	var successUserAssetList []*data.UserAsset
	var failedUserAssetList []*data.UserAsset

	for i, record := range userAssetList {
		if i%5 == 0 {
			log.Info("补数据更新用户资产中", zap.Any("index", i), zap.Any("size", len(userAssetList)))
		}
		if record.ChainName == "BTC" || record.ChainName == "DOGE" || record.ChainName == "LTC" {
			userAsset, err := GetBtcBalance(record.ChainName, record.Uid, record.Address)
			if err != nil {
				failedUserAssetList = append(failedUserAssetList, record)
				log.Error(record.ChainName+"补数据更新用户资产失败", zap.Any("userAssets", userAsset), zap.Any("error", err))
				continue
			}
			record.Balance = userAsset.Balance
			record.Decimals = userAsset.Decimals
			record.Symbol = userAsset.Symbol
			successUserAssetList = append(successUserAssetList, record)
		} else if record.ChainName == "STC" {
			userAsset, err := GetStcBalance(record.ChainName, record.Uid, record.Address, record.TokenAddress)
			if err != nil {
				failedUserAssetList = append(failedUserAssetList, record)
				log.Error(record.ChainName+"补数据更新用户资产失败", zap.Any("userAssets", userAsset), zap.Any("error", err))
				continue
			}
			record.Balance = userAsset.Balance
			record.Decimals = userAsset.Decimals
			record.Symbol = userAsset.Symbol
			successUserAssetList = append(successUserAssetList, record)
		} else if record.ChainName == "TRX" {
			userAsset, err := GetTrxBalance(record.ChainName, record.Uid, record.Address, record.TokenAddress)
			if err != nil {
				failedUserAssetList = append(failedUserAssetList, record)
				log.Error(record.ChainName+"补数据更新用户资产失败", zap.Any("userAssets", userAsset), zap.Any("error", err))
				continue
			}
			record.Balance = userAsset.Balance
			record.Decimals = userAsset.Decimals
			record.Symbol = userAsset.Symbol
			successUserAssetList = append(successUserAssetList, record)
		} else {
			userAssets, err := GetBalance(record.ChainName, record.Uid, record.Address, record.TokenAddress)
			if err != nil {
				failedUserAssetList = append(failedUserAssetList, record)
				log.Error(record.ChainName+"补数据更新用户资产失败", zap.Any("userAssets", userAssets[0]), zap.Any("error", err))
				continue
			}
			record.Balance = userAssets[0].Balance
			record.Decimals = userAssets[0].Decimals
			record.Symbol = userAssets[0].Symbol
			successUserAssetList = append(successUserAssetList, record)
		}
	}

	count, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, successUserAssetList, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, successUserAssetList, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("补数据更新用户资产，将数据插入到数据库中失败", zap.Any("size", len(userAssetList)), zap.Any("successSize", len(successUserAssetList)), zap.Any("failedSize", len(failedUserAssetList)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("补数据更新用户资产", zap.Any("size", len(userAssetList)), zap.Any("successSize", len(successUserAssetList)), zap.Any("failedSize", len(failedUserAssetList)), zap.Any("count", count), zap.Any("error", err))
	return failedUserAssetList
}

func GetBalance(chainName string, uid string, address string, tokenAddress string) ([]*data.UserAsset, error) {
	nowTime := time.Now().Unix()
	var userAsset []*data.UserAsset
	var err error

	var nodeURL []string
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		nodeURL = platInfo.RpcURL
	} else {
		return nil, errors.New("chain " + chainName + " is not support")
	}
	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		log.Info("补数据更新用户资产创建Client异常", zap.Any("chainName", len(chainName)), zap.Any("uid", uid), zap.Any("address", address), zap.Any("error", err))
		c := ethereum.NewClient(url, chainName)
		clients = append(clients, c)
	}
	spider := chain.NewBlockSpider(ethereum.NewStateStore(chainName), clients...)
	err = spider.WithRetry(func(client chain.Clienter) error {
		c, _ := client.(*ethereum.Client)
		userAsset, err = handleUserAsset(chainName, *c, uid, address, tokenAddress, nowTime)
		if err != nil {
			return common.Retry(err)
		}
		return nil
	})
	return userAsset, err
}

func handleUserAsset(chainName string, client ethereum.Client, uid string, address string,
	tokenAddress string, nowTime int64) ([]*data.UserAsset, error) {
	getTokenInfo, err := biz.GetTokenInfo(nil, chainName, tokenAddress)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		getTokenInfo, err = biz.GetTokenInfo(nil, chainName, tokenAddress)
	}
	if err != nil {
		return nil, err
	}
	decimals := getTokenInfo.Decimals
	symbol := getTokenInfo.Symbol
	if tokenAddress == "" {
		userAsset, err := doHandleUserAsset(chainName, client, uid, address, tokenAddress, int32(decimals), symbol, nowTime)
		return []*data.UserAsset{userAsset}, err
	}

	return doHandleUserTokenAsset(chainName, client, uid, address, map[string]int{tokenAddress: int(decimals)}, map[string]string{tokenAddress: symbol}, nowTime)
}

func doHandleUserAsset(chainName string, client ethereum.Client, uid string, address string,
	tokenAddress string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	balance, err := client.GetBalance(address)
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
		return nil, err
	}

	var userAsset = &data.UserAsset{
		ChainName:    chainName,
		Uid:          uid,
		Address:      address,
		TokenAddress: tokenAddress,
		Balance:      balance,
		Decimals:     decimals,
		Symbol:       symbol,
		CreatedAt:    nowTime,
		UpdatedAt:    nowTime,
	}
	return userAsset, nil
}

func doHandleUserTokenAsset(chainName string, client ethereum.Client, uid string, address string,
	tokenDecimalsMap map[string]int, tokenSymbolMap map[string]string, nowTime int64) ([]*data.UserAsset, error) {

	var userAssets []*data.UserAsset
	balanceList, err := client.BatchTokenBalance(address, tokenDecimalsMap)
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenDecimalsMap), zap.Any("error", err))
		return nil, err
	}
	for tokenAddress, balancei := range balanceList {
		balance := fmt.Sprintf("%v", balancei)
		decimals := int32(tokenDecimalsMap[tokenAddress])
		symbol := tokenSymbolMap[tokenAddress]

		var userAsset = &data.UserAsset{
			ChainName:    chainName,
			Uid:          uid,
			Address:      address,
			TokenAddress: tokenAddress,
			Balance:      balance,
			Decimals:     decimals,
			Symbol:       symbol,
			CreatedAt:    nowTime,
			UpdatedAt:    nowTime,
		}
		userAssets = append(userAssets, userAsset)
	}
	return userAssets, nil
}

func GetStcBalance(chainName string, uid string, address string, tokenAddress string) (*data.UserAsset, error) {
	nowTime := time.Now().Unix()
	var nodeURL []string
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		nodeURL = platInfo.RpcURL
	} else {
		return nil, errors.New("chain " + chainName + " is not support")
	}
	client := starcoin.NewClient(nodeURL[0], chainName)
	userAsset, err := handleStcUserAsset(chainName, client, uid, address, tokenAddress, nowTime)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		userAsset, err = handleStcUserAsset(chainName, client, uid, address, tokenAddress, nowTime)
	}
	return userAsset, err
}

func handleStcUserAsset(chainName string, client starcoin.Client, uid string, address string,
	tokenAddress string, nowTime int64) (*data.UserAsset, error) {
	getTokenInfo, err := biz.GetTokenInfo(nil, chainName, tokenAddress)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		getTokenInfo, err = biz.GetTokenInfo(nil, chainName, tokenAddress)
	}
	if err != nil {
		return nil, err
	}
	decimals := getTokenInfo.Decimals
	symbol := getTokenInfo.Symbol
	if tokenAddress == starcoin.STC_CODE {
		tokenAddress = ""
	}
	userAsset, err := doHandleStcUserAsset(chainName, client, uid, address, tokenAddress, int32(decimals), symbol, nowTime)
	return userAsset, err
}

func doHandleStcUserAsset(chainName string, client starcoin.Client, uid string, address string,
	tokenAddress string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	var balance string
	var err error
	if tokenAddress == starcoin.STC_CODE || tokenAddress == "" {
		balance, err = client.GetBalance(address)
	} else if tokenAddress != starcoin.STC_CODE && tokenAddress != "" {
		balance, err = client.GetTokenBalance(address, tokenAddress, int(decimals))
	}
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
		return nil, err
	}

	var userAsset = &data.UserAsset{
		ChainName:    chainName,
		Uid:          uid,
		Address:      address,
		TokenAddress: tokenAddress,
		Balance:      balance,
		Decimals:     decimals,
		Symbol:       symbol,
		CreatedAt:    nowTime,
		UpdatedAt:    nowTime,
	}
	return userAsset, nil
}

func GetTrxBalance(chainName string, uid string, address string, tokenAddress string) (*data.UserAsset, error) {
	nowTime := time.Now().Unix()
	client := tron.NewClient("https://api.tronstack.io", chainName)
	userAsset, err := handleTrxUserAsset(chainName, client, uid, address, tokenAddress, nowTime)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		userAsset, err = handleTrxUserAsset(chainName, client, uid, address, tokenAddress, nowTime)
	}
	return userAsset, err
}

func handleTrxUserAsset(chainName string, client tron.Client, uid string, address string,
	tokenAddress string, nowTime int64) (*data.UserAsset, error) {
	getTokenInfo, err := biz.GetTokenInfo(nil, chainName, tokenAddress)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		getTokenInfo, err = biz.GetTokenInfo(nil, chainName, tokenAddress)
	}
	if err != nil {
		return nil, err
	}
	decimals := getTokenInfo.Decimals
	symbol := getTokenInfo.Symbol
	userAsset, err := doHandleTrxUserAsset(chainName, client, uid, address, tokenAddress, int32(decimals), symbol, nowTime)
	return userAsset, err
}

func doHandleTrxUserAsset(chainName string, client tron.Client, uid string, address string,
	tokenAddress string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	var balance string
	var err error
	if tokenAddress == "" {
		balance, err = client.GetBalance(address)
	} else if tokenAddress != "" {
		balance, err = client.GetTokenBalance(address, tokenAddress, int(decimals))
	}
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
		return nil, err
	}

	var userAsset = &data.UserAsset{
		ChainName:    chainName,
		Uid:          uid,
		Address:      address,
		TokenAddress: tokenAddress,
		Balance:      balance,
		Decimals:     decimals,
		Symbol:       symbol,
		CreatedAt:    nowTime,
		UpdatedAt:    nowTime,
	}
	return userAsset, nil
}

func GetBtcBalance(chainName string, uid string, address string) (*data.UserAsset, error) {
	nowTime := time.Now().Unix()
	var nodeURL []string
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		nodeURL = platInfo.RpcURL
	} else {
		return nil, errors.New("chain " + chainName + " is not support")
	}
	client := bitcoin.NewClient(nodeURL[0], chainName)
	userAsset, err := handleBtcUserAsset(chainName, client, uid, address, nowTime)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		userAsset, err = handleBtcUserAsset(chainName, client, uid, address, nowTime)
	}
	return userAsset, err
}

func handleBtcUserAsset(chainName string, client bitcoin.Client, uid string, address string,
	nowTime int64) (*data.UserAsset, error) {
	decimals := 8
	symbol := "BTC"
	userAsset, err := doHandleBtcUserAsset(chainName, client, uid, address, int32(decimals), symbol, nowTime)
	return userAsset, err
}

func doHandleBtcUserAsset(chainName string, client bitcoin.Client, uid string, address string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	balance, err := client.GetBalance(address)
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return nil, err
	}

	var userAsset = &data.UserAsset{
		ChainName: chainName,
		Uid:       uid,
		Address:   address,
		Balance:   balance,
		Decimals:  decimals,
		Symbol:    symbol,
		CreatedAt: nowTime,
		UpdatedAt: nowTime,
	}
	return userAsset, nil
}

func HandleOptimismRecordFee() {
	log.Info("修复Optimism链交易记录中的手续费开始")
	source := biz.AppConfig.Target
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}

	var evmTxRecordList []*data.EvmTransactionRecord

	sqlStr := "select id, transaction_hash, gas_used, gas_price from optimism_transaction_record where created_at < 1677923000;"

	ret := dbSource.Raw(sqlStr).Find(&evmTxRecordList)
	err = ret.Error
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		ret = dbSource.Raw(sqlStr).Find(&evmTxRecordList)
		err = ret.Error
	}
	if err != nil {
		log.Errore("migrate page query userAsset failed", err)
		return
	}
	log.Info("修复Optimism链交易记录中的手续费前置处理开始", zap.Any("query size", len(evmTxRecordList)))

	var evmTxRecordMap = make(map[string][]*data.EvmTransactionRecord)
	for _, evmTxRecord := range evmTxRecordList {
		txHash := evmTxRecord.TransactionHash
		txHash = strings.Split(txHash, "#")[0]

		evmTxRecords, ok := evmTxRecordMap[txHash]
		if !ok {
			evmTxRecords = make([]*data.EvmTransactionRecord, 0)
		}
		evmTxRecords = append(evmTxRecords, evmTxRecord)
		evmTxRecordMap[txHash] = evmTxRecords
	}

	var i int
	//var evmTxRecords []*data.EvmTransactionRecord
	for txHash, evmTransactionRecordList := range evmTxRecordMap {
		if i%10 == 0 {
			log.Info("修复Optimism链交易记录中的手续费中", zap.Any("index", i), zap.Any("size", len(evmTxRecordList)))
		}
		i++
		l1Fee, err := GetRecordL1Fee("Optimism", txHash)
		if err != nil {
			log.Error("修复Optimism链交易记录中的手续费，从链上票据中获取f1Fee失败", zap.Any("txHash", txHash), zap.Any("error", err))
			return
		}
		l1FeeInt, err := utils.HexStringToBigInt(l1Fee)
		if err != nil {
			continue
		}
		l1FeeDecimal := decimal.NewFromBigInt(l1FeeInt, 0)
		for _, evmTxRecord := range evmTransactionRecordList {
			gasUsedInt, _ := new(big.Int).SetString(evmTxRecord.GasUsed, 0)
			gasPriceInt, _ := new(big.Int).SetString(evmTxRecord.GasPrice, 0)
			gasFee := new(big.Int).Mul(gasUsedInt, gasPriceInt)
			feeAmount := decimal.NewFromBigInt(gasFee, 0)
			feeAmount = feeAmount.Add(l1FeeDecimal)
			evmTxRecord.FeeAmount = feeAmount
			//evmTxRecords = append(evmTxRecords, evmTxRecord)
		}
	}

	count, err := data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, "optimism_transaction_record", evmTxRecordList, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, "optimism_transaction_record", evmTxRecordList, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("修复Optimism链交易记录中的手续费，更新交易记录，将数据插入到数据库中失败", zap.Any("size", len(evmTxRecordList)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("修复Optimism链交易记录中的手续费结束", zap.Any("query size", len(evmTxRecordList)), zap.Any("affected count", count))
}

func GetRecordL1Fee(chainName string, txHash string) (string, error) {
	var transactionReceipt *rtypes.Receipt
	var err error

	var nodeURL []string
	if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
		nodeURL = platInfo.RpcURL
	} else {
		return "", errors.New("chain " + chainName + " is not support")
	}
	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		c := ethereum.NewClient(url, chainName)
		clients = append(clients, c)
	}
	spider := chain.NewBlockSpider(ethereum.NewStateStore(chainName), clients...)
	err = spider.WithRetry(func(client chain.Clienter) error {
		c, _ := client.(*ethereum.Client)
		transactionReceipt, err = c.GetTransactionReceipt(context.Background(), ecommon.HexToHash(txHash))
		if err != nil {
			return common.Retry(err)
		}
		return nil
	})

	if err != nil {
		return "", err
	}
	l1FeeStr := utils.GetHexString(transactionReceipt.L1Fee)
	return l1FeeStr, nil
}

var addressUid = make(map[string]string)

func FixAddressToUid() {
	log.Info("修改企业钱包地址对应的uid开始")

	var startTime, stopTime int64
	//startTime = 1680192000//2023-04-00 00:00:00
	startTime = 1677513600 //2023-03-00 00:00:00
	stopTime = time.Now().Unix()

	HandleUserAsset(startTime, stopTime)
	HandleUserNftAsset(startTime, stopTime)
	ReplaceDappApproveRecord()
	ReplaceNervosCellRecord()
	ReplaceUtxoUnspentRecord()
	ReplaceNftRecordHistory()
	HandleTransactionRecord(startTime, stopTime)
	log.Info("修改企业钱包地址对应的uid结束")
}

func HandleUserAsset(startTime, stopTime int64) {
	log.Info("修改企业钱包地址对应的uid，处理用户资产表开始")

	var userAssetList []*data.UserAsset
	var request = &data.AssetRequest{
		StartTime: startTime,
		StopTime:  stopTime,
		OrderBy:   "id asc",
		PageNum:   1,
		PageSize:  biz.PAGE_SIZE,
	}

	for {
		list, _, err := data.UserAssetRepoClient.PageList(nil, request)
		if err != nil {
			log.Error("修改企业钱包地址对应的uid，从用户资产表中查询用户资产信息失败", zap.Any("request", request), zap.Any("error", err))
			return
		}

		if len(list) == 0 {
			break
		}
		for _, userAsset := range list {
			var uid string

			address := userAsset.Address
			chainType, _ := biz.GetChainNameType(userAsset.ChainName)
			switch chainType {
			case biz.APTOS, biz.SUI:
				if address != "" {
					address = utils.AddressAdd0(address)
				}
			}

			if address != "" {
				var ok bool
				uid, ok = addressUid[address]
				if !ok {
					_, uid, err = biz.UserAddressSwitchRetryAlert(userAsset.ChainName, address)
					if err != nil {
						log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("userAsset", userAsset), zap.Any("error", err))
						return
					}
					addressUid[address] = uid
				}
			}

			if userAsset.Uid != uid {
				userAssetList = append(userAssetList, &data.UserAsset{
					Id:  userAsset.Id,
					Uid: uid,
				})
			}
		}
		if len(list) < biz.PAGE_SIZE {
			break
		}
		request.PageNum += 1
	}

	count, err := data.UserAssetRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, userAssetList, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = data.UserAssetRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, userAssetList, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("修改企业钱包地址对应的uid，将用户资产信息插入到用户资产表中失败", zap.Any("size", len(userAssetList)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("修改企业钱包地址对应的uid，处理用户资产表结束", zap.Any("query size", len(userAssetList)), zap.Any("affected count", count))
}

func HandleUserNftAsset(startTime, stopTime int64) {
	log.Info("修改企业钱包地址对应的uid，处理用户Nft资产表开始")

	var userNftAssetList []*data.UserNftAsset
	var request = &data.NftAssetRequest{
		StartTime: startTime,
		StopTime:  stopTime,
		OrderBy:   "id asc",
		PageNum:   1,
		PageSize:  biz.PAGE_SIZE,
	}

	for {
		list, _, err := data.UserNftAssetRepoClient.PageList(nil, request)
		if err != nil {
			log.Error("修改企业钱包地址对应的uid，从用户Nft资产表中查询用户Nft资产信息失败", zap.Any("request", request), zap.Any("error", err))
			return
		}

		if len(list) == 0 {
			break
		}
		for _, userNftAsset := range list {
			var uid string

			address := userNftAsset.Address
			chainType, _ := biz.GetChainNameType(userNftAsset.ChainName)
			switch chainType {
			case biz.APTOS, biz.SUI:
				if address != "" {
					address = utils.AddressAdd0(address)
				}
			}

			if address != "" {
				var ok bool
				uid, ok = addressUid[address]
				if !ok {
					_, uid, err = biz.UserAddressSwitchRetryAlert(userNftAsset.ChainName, address)
					if err != nil {
						log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("userNftAsset", userNftAsset), zap.Any("error", err))
						return
					}
					addressUid[address] = uid
				}
			}

			if userNftAsset.Uid != uid {
				userNftAssetList = append(userNftAssetList, &data.UserNftAsset{
					Id:  userNftAsset.Id,
					Uid: uid,
				})
			}
		}
		if len(list) < biz.PAGE_SIZE {
			break
		}
		request.PageNum += 1
	}

	count, err := data.UserNftAssetRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, userNftAssetList, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = data.UserNftAssetRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, userNftAssetList, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("修改企业钱包地址对应的uid，将用户Nft资产信息插入到用户Nft资产表中失败", zap.Any("size", len(userNftAssetList)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("修改企业钱包地址对应的uid，处理用户Nft资产表结束", zap.Any("query size", len(userNftAssetList)), zap.Any("affected count", count))
}

func ReplaceDappApproveRecord() {
	log.Info("start=======修改企业钱包地址对应的uid，DappApproveRecord")

	addresses, err := data.DappApproveRecordRepoClient.FindAddressGroup(nil)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		addresses, err = data.DappApproveRecordRepoClient.FindAddressGroup(nil)
	}
	if err != nil {
		log.Error("修改企业钱包地址对应的uid，信息失败", zap.Any("error", err))
		return
	}

	for _, address := range addresses {
		addressExist, uid, err := biz.UserAddressSwitchRetryAlert("", address)
		if err != nil {
			log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("address", address), zap.Any("error", err))
			continue
		}
		if !addressExist {
			continue
		}
		data.DappApproveRecordRepoClient.UpdateUidByAddress(nil, address, uid)
	}
	log.Info("end=======修改企业钱包地址对应的uid，DappApproveRecord")
}

func ReplaceNervosCellRecord() {
	log.Info("start=======修改企业钱包地址对应的uid，NervosCellRecord")

	addresses, err := data.NervosCellRecordRepoClient.FindAddressGroup(nil)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		addresses, err = data.NervosCellRecordRepoClient.FindAddressGroup(nil)
	}
	if err != nil {
		log.Error("修改企业钱包地址对应的uid，信息失败", zap.Any("error", err))
		return
	}

	for _, address := range addresses {
		addressExist, uid, err := biz.UserAddressSwitchRetryAlert("", address)
		if err != nil {
			log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("address", address), zap.Any("error", err))
			continue
		}
		if !addressExist {
			continue
		}
		data.NervosCellRecordRepoClient.UpdateUidByAddress(nil, address, uid)
	}
	log.Info("end=======修改企业钱包地址对应的uid，NervosCellRecord")
}

func ReplaceNftRecordHistory() {
	log.Info("start=======修改企业钱包地址对应的uid，ReplaceNftRecordHistory")

	ReplaceNftRecordHistoryUid("from_address", "from_uid")
	ReplaceNftRecordHistoryUid("to_address", "to_uid")
	log.Info("end=======修改企业钱包地址对应的uid，ReplaceNftRecordHistory")
}

func ReplaceNftRecordHistoryUid(columnAddress string, columnUrd string) {
	addresses, err := data.NftRecordHistoryRepoClient.FindAddressGroup(nil, columnAddress)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		addresses, err = data.NftRecordHistoryRepoClient.FindAddressGroup(nil, columnAddress)
	}
	if err != nil {
		log.Error("修改企业钱包地址对应的uid，信息失败", zap.Any("error", err))
		return
	}

	for _, address := range addresses {
		addressExist, uid, err := biz.UserAddressSwitchRetryAlert("", address)
		if err != nil {
			log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("address", address), zap.Any("error", err))
			continue
		}
		if !addressExist {
			continue
		}
		data.NftRecordHistoryRepoClient.UpdateUidByAddress(nil, address, columnAddress, uid, columnUrd)
	}
}

func ReplaceUtxoUnspentRecord() {
	log.Info("start=======修改企业钱包地址对应的uid，ReplaceUtxoUnspentRecord")

	addresses, err := data.UtxoUnspentRecordRepoClient.FindAddressGroup(nil)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		addresses, err = data.UtxoUnspentRecordRepoClient.FindAddressGroup(nil)
	}
	if err != nil {
		log.Error("修改企业钱包地址对应的uid，信息失败", zap.Any("error", err))
		return
	}

	for _, address := range addresses {
		addressExist, uid, err := biz.UserAddressSwitchRetryAlert("", address)
		if err != nil {
			log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("address", address), zap.Any("error", err))
			continue
		}
		if !addressExist {
			continue
		}
		data.UtxoUnspentRecordRepoClient.UpdateUidByAddress(nil, address, uid)
	}
	log.Info("end=======修改企业钱包地址对应的uid，ReplaceUtxoUnspentRecord")
}

func HandleTransactionRecord(startTime, stopTime int64) {
	log.Info("修改企业钱包地址对应的uid，处理交易记录表开始")

	for _, platInfo := range biz.GetChainPlatInfoMap() {
		chainName := platInfo.Chain
		chainType := platInfo.Type
		tableName := biz.GetTableName(chainName)

		var request = &data.TransactionRequest{
			Nonce:     -1,
			StartTime: startTime,
			StopTime:  stopTime,
			OrderBy:   "id asc",
			PageSize:  biz.PAGE_SIZE,
		}
		log.Info("修改企业钱包地址对应的uid，处理" + tableName + "表开始")

		switch chainType {
		case biz.BTC:
			var transactionRecordList []*data.BtcTransactionRecord

			err := data.BtcTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.BtcTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.BtcTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.BtcTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.BtcTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.EVM:
			var transactionRecordList []*data.EvmTransactionRecord

			err := data.EvmTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.EvmTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.EvmTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.APTOS:
			var transactionRecordList []*data.AptTransactionRecord

			err := data.AptTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.AptTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						fromAddress = utils.AddressAdd0(fromAddress)
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						toAddress = utils.AddressAdd0(toAddress)
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.AptTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.AptTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.AptTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.SUI:
			var transactionRecordList []*data.SuiTransactionRecord

			err := data.SuiTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.SuiTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						fromAddress = utils.AddressAdd0(fromAddress)
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						toAddress = utils.AddressAdd0(toAddress)
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.SuiTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.SuiTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.SuiTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.COSMOS:
			var transactionRecordList []*data.AtomTransactionRecord

			err := data.AtomTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.AtomTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.AtomTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.AtomTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.AtomTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.NERVOS:
			var transactionRecordList []*data.CkbTransactionRecord

			err := data.CkbTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.CkbTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.CkbTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.CkbTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.CkbTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.CASPER:
			var transactionRecordList []*data.CsprTransactionRecord

			err := data.CsprTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.CsprTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.CsprTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.CsprTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.CsprTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.POLKADOT:
			var transactionRecordList []*data.DotTransactionRecord

			err := data.DotTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.DotTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.DotTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.DotTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.DotTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.SOLANA:
			var transactionRecordList []*data.SolTransactionRecord

			err := data.SolTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.SolTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.SolTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.SolTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.SolTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.STC:
			var transactionRecordList []*data.StcTransactionRecord

			err := data.StcTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.StcTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.StcTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.StcTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.StcTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.TVM:
			var transactionRecordList []*data.TrxTransactionRecord

			err := data.TrxTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.TrxTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.TrxTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.TrxTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.TrxTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		case biz.KASPA:
			var transactionRecordList []*data.KasTransactionRecord

			err := data.KasTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.KasTransactionRecord) error {
				for _, record := range list {
					var fromUid, toUid string
					var err error

					fromAddress := record.FromAddress
					toAddress := record.ToAddress
					if fromAddress != "" {
						var ok bool
						fromUid, ok = addressUid[fromAddress]
						if !ok {
							_, fromUid, err = biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[fromAddress] = fromUid
						}
					}

					if toAddress != "" {
						var ok bool
						toUid, ok = addressUid[toAddress]
						if !ok {
							_, toUid, err = biz.UserAddressSwitchRetryAlert(chainName, toAddress)
							if err != nil {
								log.Error("修改企业钱包地址对应的uid，从redis中获取用户地址失败", zap.Any("record", record), zap.Any("error", err))
								return err
							}
							addressUid[toAddress] = toUid
						}
					}

					if record.FromUid != fromUid || record.ToUid != toUid {
						transactionRecordList = append(transactionRecordList, &data.KasTransactionRecord{
							Id:      record.Id,
							FromUid: fromUid,
							ToUid:   toUid,
						})
					}
				}
				return nil
			})
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
				return
			}

			count, err := data.KasTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.KasTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, transactionRecordList, biz.PAGE_SIZE)
			}
			if err != nil {
				log.Error("修改企业钱包地址对应的uid，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionRecordList)), zap.Any("count", count), zap.Any("error", err))
			}
			log.Info("修改企业钱包地址对应的uid，处理"+tableName+"表结束", zap.Any("query size", len(transactionRecordList)), zap.Any("affected count", count))
		}
	}
	log.Info("修改企业钱包地址对应的uid，处理交易记录表结束")
}

func HandleTransactionRecordCount() {
	log.Info("补交易次数统计数，处理交易记录表开始")

	for _, platInfo := range biz.GetChainPlatInfoMap() {
		chainName := platInfo.Chain
		chainType := platInfo.Type
		tableName := biz.GetTableName(chainName)

		var err error
		tm := time.Now()
		nowTime := tm.Unix()

		var transactionCountMap = make(map[string]*data.TransactionCount)
		var transactionCountList []*data.TransactionCount

		var request = &data.TransactionRequest{
			Nonce:               -1,
			TransactionTypeList: []string{biz.NATIVE, biz.TRANSFER, biz.TRANSFERNFT, biz.CONTRACT, biz.SWAP, biz.MINT},
			StatusList:          []string{biz.SUCCESS},
			OrderBy:             "id asc",
			PageSize:            biz.PAGE_SIZE,
		}
		log.Info("补交易次数统计数，处理" + tableName + "表开始")

		switch chainType {
		case biz.BTC:
			err = data.BtcTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.BtcTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     biz.NATIVE,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.EVM:
			err = data.EvmTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.EvmTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.APTOS:
			err = data.AptTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.AptTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.SUI:
			err = data.SuiTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.SuiTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.COSMOS:
			err = data.AtomTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.AtomTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.NERVOS:
			err = data.CkbTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.CkbTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.CASPER:
			err = data.CsprTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.CsprTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.POLKADOT:
			err = data.DotTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.DotTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.SOLANA:
			err = data.SolTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.SolTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.STC:
			err = data.StcTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.StcTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.TVM:
			err = data.TrxTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.TrxTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress + record.TransactionType
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     record.TransactionType,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		case biz.KASPA:
			err = data.KasTransactionRecordRepoClient.PageListAllCallBack(nil, tableName, request, func(list []*data.KasTransactionRecord) error {
				for _, record := range list {
					key := chainName + record.FromAddress + record.ToAddress
					if statistic, ok := transactionCountMap[key]; ok {
						statistic.TransactionQuantity += 1
					} else {
						var transactionCount = &data.TransactionCount{
							ChainName:           chainName,
							FromAddress:         record.FromAddress,
							ToAddress:           record.ToAddress,
							TransactionType:     biz.NATIVE,
							TransactionQuantity: 1,
							TransactionHash:     record.TransactionHash,
							CreatedAt:           nowTime,
							UpdatedAt:           nowTime,
						}

						transactionCountMap[key] = transactionCount
					}
				}
				return nil
			})
		}
		if err != nil {
			log.Error("补交易次数统计数，从"+tableName+"表中查询交易记录数据失败", zap.Any("request", request), zap.Any("error", err))
			return
		}

		if len(transactionCountMap) == 0 {
			continue
		}
		for _, transactionCount := range transactionCountMap {
			transactionCountList = append(transactionCountList, transactionCount)
		}
		count, err := data.TransactionCountRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionCountList, biz.PAGE_SIZE)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.TransactionCountRepoClient.PageIncrementBatchSaveOrUpdate(nil, transactionCountList, biz.PAGE_SIZE)
		}
		if err != nil {
			log.Error("补交易次数统计数，将用交易记录数据插入到"+tableName+"表中失败", zap.Any("size", len(transactionCountList)), zap.Any("count", count), zap.Any("error", err))
		}
		log.Info("补交易次数统计数，处理"+tableName+"表结束", zap.Any("query size", len(transactionCountList)), zap.Any("affected count", count))
	}
	log.Info("补交易次数统计数，处理交易记录表结束")
}

func SyncChainNames(chainNames []string) {
	for _, cn := range chainNames {
		SyncCoinMarket(cn)
	}
}

// 每分钟 200 请求 调用币价服务那边 api 限频
func SyncCoinMarket(chainName string) int {
	// 	chainNames := []string{"ETH", "BSC", "Polygon", "Arbitrum"}
	//chainName1 := "ETH"
	tableName := biz.GetTableName(chainName)
	records, e := data.EvmTransactionRecordRepoClient.ListAll(nil, tableName)
	//records, e := data.EvmTransactionRecordRepoClient.FindUsdt(nil, tableName)
	log.Info("eth_usdt", zap.Any("1", records))
	if e != nil {
		log.Info(tableName, zap.Error(e))
		return 0
	}

	total := len(records)
	log.Info(tableName, zap.Any("总条数", total))
	pageSize := 190
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}

	for start < stop {
		subTxRecords := records[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}
		for _, record := range subTxRecords {
			if record.Status != biz.SUCCESS {
				continue
			}
			tm := time.Unix(record.TxTime, 0)
			var dt = utils.GetDayTime(&tm)
			fromUid := record.FromUid
			toUid := record.ToUid
			fromAddress := record.FromAddress
			toAddress := record.ToAddress

			if "0xb7B4D65CB5a0c44cCB9019ca74745686188173Db" == fromAddress {
				log.Info("from地址", zap.Any(record.TransactionType, record))
			}
			if "0xb7B4D65CB5a0c44cCB9019ca74745686188173Db" == toAddress {
				log.Info("to地址", zap.Any(record.TransactionType, record))
			}

			switch record.TransactionType {
			case biz.TRANSFER:
				//解析parse_data 拿出 代币.
				//计算主币的余额，因为有手续费的花费
				if fromUid != "" {
					HandlerNativePriceHistory(chainName, fromAddress, fromUid, dt, true, record.FeeAmount, decimal.Zero)
					HandlerTokenPriceHistory(chainName, fromAddress, record.ParseData, fromUid, dt, true)
				}
				if toUid != "" {
					HandlerTokenPriceHistory(chainName, toAddress, record.ParseData, toUid, dt, false)
				}

			case biz.APPROVE, biz.APPROVENFT, biz.CREATECONTRACT, biz.CREATEACCOUNT, biz.TRANSFERNFT, biz.CONTRACT, biz.CLOSEACCOUNT, biz.REGISTERTOKEN, biz.DIRECTTRANSFERNFTSWITCH, biz.SETAPPROVALFORALL, biz.SAFETRANSFERFROM, biz.SAFEBATCHTRANSFERFROM:
				//主币计算
				if fromUid != "" {
					HandlerNativePriceHistory(chainName, fromAddress, fromUid, dt, true, record.FeeAmount, decimal.Zero)
				}
			case biz.EVENTLOG:
				//  解析parse_data 拿出 代币
				if fromUid != "" {
					HandlerTokenPriceHistory(chainName, fromAddress, record.ParseData, fromUid, dt, true)
				}
				if toUid != "" {
					HandlerTokenPriceHistory(chainName, toAddress, record.ParseData, toUid, dt, false)
				}
			case biz.NATIVE:
				// 主币 + 手续费
				if fromUid != "" {
					HandlerNativePriceHistory(chainName, fromAddress, fromUid, dt, true, record.FeeAmount, record.Amount)
				}
				if toUid != "" {
					HandlerNativePriceHistory(chainName, toAddress, fromUid, dt, false, record.FeeAmount, record.Amount)
				}
			}
		}
		log.Info("处理完", zap.Any("处理条数", len(subTxRecords)))
		time.Sleep(time.Duration(2) * time.Minute)
		log.Info("处理完 睡眠2分钟")
	}

	return total
}
func HandlerTokenPriceHistory(chainName, address, parseData, uid string, dt int64, fromFlag bool) {
	now := time.Now().Unix()
	tokenInfo, _ := biz.ParseTokenInfo(parseData)
	tokenSymbolMap := make(map[string]int)

	tokenSymbolMap[tokenInfo.Address] = int(tokenInfo.Decimals)
	if tokenInfo == nil || tokenInfo.Address == "" {
		return
	}
	tma, e := biz.DescribeCoinPriceByTimestamp(tokenInfo.Address, "", chainName, uint32(dt))
	if tma == nil || tma.Price == nil || e != nil {
		log.Error("清洗数据，币价调用失败！", zap.Any("tma", tma), zap.Error(e))
		return
	}
	var cnyTokenPrice, usdTokenPrice string
	cnyTokenPrice = strconv.FormatFloat(tma.Price.Cny, 'f', 2, 64)
	usdTokenPrice = strconv.FormatFloat(tma.Price.Usd, 'f', 2, 64)

	cnyTokenPriceDecimal, _ := decimal.NewFromString(cnyTokenPrice)
	usdTokenPriceDecimal, _ := decimal.NewFromString(usdTokenPrice)

	log.Info("cny 币价服务 ", zap.Any("", cnyTokenPriceDecimal))
	log.Info("usd 币价服务 ", zap.Any("", usdTokenPriceDecimal))

	tokenAmount, _ := decimal.NewFromString(tokenInfo.Amount)
	log.Info("交易金额", zap.Any("", tokenAmount))
	var tas string
	if fromFlag {
		tas = utils.StringDecimals(tokenAmount.Neg().String(), int(tokenInfo.Decimals))
	} else {
		tas = utils.StringDecimals(tokenAmount.String(), int(tokenInfo.Decimals))
	}
	tokenAmountDecimal, _ := decimal.NewFromString(tas)
	cnyTokenAmount := tokenAmountDecimal.Mul(cnyTokenPriceDecimal)
	usdTokenAmount := tokenAmountDecimal.Mul(usdTokenPriceDecimal)
	log.Info("交易法币", zap.Any("", cnyTokenAmount))
	log.Info("交易法币usd", zap.Any("", usdTokenAmount))

	fmcs, _ := data.MarketCoinHistoryRepoClient.ListByCondition(nil, &data.MarketCoinHistory{
		ChainName:    chainName,
		Address:      address,
		TokenAddress: tokenInfo.Address,
	})
	txb := utils.StringDecimals(tokenAmount.String(), int(tokenInfo.Decimals))
	if len(fmcs) == 0 {
		//插入代币 币价
		msh := &data.MarketCoinHistory{
			Uid:                 uid,
			Address:             address,
			ChainName:           chainName,
			TokenAddress:        tokenInfo.Address,
			Symbol:              tokenInfo.Symbol,
			CnyPrice:            cnyTokenPrice, //均价   1： 10  2：15  3：20  (CnyAmount + inputBalance * inputcnyprice)/(balance+inputBalance)
			UsdPrice:            usdTokenPrice, //均价
			TransactionQuantity: 1,
			CnyAmount:           cnyTokenAmount, // CnyAmount + inputBalance * inputcnyprice
			UsdAmount:           usdTokenAmount,
			Dt:                  dt,
			CreatedAt:           now,
			UpdatedAt:           now,
			TransactionBalance:  txb,
			Balance:             tas,
		}
		//改成saveor update
		log.Info("第一次代币存入DB", zap.Any("msh", msh))

		data.MarketCoinHistoryRepoClient.SaveOrUpdate(nil, msh)
	} else if len(fmcs) == 1 {
		marketCoinHistory := fmcs[0]
		log.Info("查询出代币结果", zap.Any("marketCoinHistory", marketCoinHistory))
		oldTransactionBalance, _ := decimal.NewFromString(marketCoinHistory.TransactionBalance)
		newTransactionBalance, _ := decimal.NewFromString(txb)
		marketCoinHistory.TransactionBalance = oldTransactionBalance.Add(newTransactionBalance).String()

		oldBalance, _ := decimal.NewFromString(marketCoinHistory.Balance)
		marketCoinHistory.Balance = oldBalance.Add(tokenAmountDecimal).String()
		if oldBalance.Cmp(decimal.Zero) == 0 {
			marketCoinHistory.CnyPrice = cnyTokenPrice
			marketCoinHistory.UsdPrice = usdTokenPrice
			marketCoinHistory.CnyAmount = cnyTokenAmount
			marketCoinHistory.UsdAmount = usdTokenAmount
		} else {
			marketCoinHistory.CnyAmount = marketCoinHistory.CnyAmount.Add(cnyTokenAmount)
			marketCoinHistory.UsdAmount = marketCoinHistory.UsdAmount.Add(usdTokenAmount)
			if oldBalance.Add(tokenAmountDecimal).Cmp(decimal.Zero) != 0 {
				marketCoinHistory.CnyPrice = marketCoinHistory.CnyAmount.DivRound(oldBalance.Add(tokenAmountDecimal), 2).String()
				marketCoinHistory.UsdPrice = marketCoinHistory.UsdAmount.DivRound(oldBalance.Add(tokenAmountDecimal), 2).String()
			}
		}
		marketCoinHistory.UpdatedAt = now
		if dt == marketCoinHistory.Dt {
			marketCoinHistory.TransactionQuantity = marketCoinHistory.TransactionQuantity + 1
			log.Info("更新代币结果", zap.Any("marketCoinHistory", marketCoinHistory))

			data.MarketCoinHistoryRepoClient.Update(nil, marketCoinHistory)
		} else {
			marketCoinHistory.Dt = dt
			marketCoinHistory.TransactionQuantity = 1
			marketCoinHistory.Id = 0
			log.Info("更新代币结果", zap.Any("marketCoinHistory", marketCoinHistory))
			data.MarketCoinHistoryRepoClient.SaveOrUpdate(nil, marketCoinHistory)
		}
	}
}
func HandlerNativePriceHistory(chainName, address, uid string, dt int64, fromFlag bool, feeAmount, amount decimal.Decimal) {
	now := time.Now().Unix()
	var cnyPrice, usdPrice string
	platInfo, _ := biz.GetChainPlatInfo(chainName)
	decimals := int(platInfo.Decimal)
	symbol := platInfo.NativeCurrency
	getPriceKey := platInfo.GetPriceKey

	tma, e := biz.DescribeCoinPriceByTimestamp("", getPriceKey, chainName, uint32(dt))
	if tma == nil || tma.Price == nil || e != nil {
		log.Error("清洗数据，币价调用失败！", zap.Any("tma", tma), zap.Error(e))
		return
	}
	cnyPrice = strconv.FormatFloat(tma.Price.Cny, 'f', 2, 64)
	usdPrice = strconv.FormatFloat(tma.Price.Usd, 'f', 2, 64)

	cnyPriceDecimal, _ := decimal.NewFromString(cnyPrice)
	usdPriceDecimal, _ := decimal.NewFromString(usdPrice)
	var fs string
	if fromFlag {
		totalAmount := feeAmount.Add(amount)
		fs = utils.StringDecimals(totalAmount.Neg().String(), decimals)
	} else {
		totalAmount := feeAmount.Add(amount)
		fs = utils.StringDecimals(totalAmount.String(), decimals)
	}
	totalNum, _ := decimal.NewFromString(fs)
	log.Info("此次交易金额", zap.Any("totalNum", totalNum))
	cnyFee := totalNum.Mul(cnyPriceDecimal)
	usdFee := totalNum.Mul(usdPriceDecimal)
	log.Info("此次交易金额", zap.Any("cnyFee", cnyFee))
	log.Info("此次交易金额u", zap.Any("usdFee", usdFee))

	//查询 余额
	mcs, _ := data.MarketCoinHistoryRepoClient.ListByCondition(nil, &data.MarketCoinHistory{
		ChainName: chainName,
		Address:   address,
	})
	//计算 ，更新平均值

	if len(mcs) == 0 {
		//插入主币 币价
		msh := &data.MarketCoinHistory{
			Uid:                 uid,
			Address:             address,
			ChainName:           chainName,
			Symbol:              symbol,
			CnyPrice:            cnyPrice, //均价   1： 10  2：15  3：20  (CnyAmount + inputBalance * inputcnyprice)/(balance+inputBalance)
			UsdPrice:            usdPrice, //均价
			TransactionQuantity: 1,
			CnyAmount:           cnyFee, // CnyAmount + inputBalance * inputcnyprice
			UsdAmount:           usdFee,
			Dt:                  dt,
			CreatedAt:           now,
			UpdatedAt:           now,
			TransactionBalance:  totalNum.Abs().String(),
			Balance:             fs,
		}
		log.Info("第一次存入DB", zap.Any("msh", msh))
		data.MarketCoinHistoryRepoClient.SaveOrUpdate(nil, msh)
	} else if len(mcs) == 1 {
		marketCoinHistory := mcs[0]

		log.Info("查询出结果", zap.Any("marketCoinHistory", marketCoinHistory))
		marketCoinHistory.TransactionQuantity = marketCoinHistory.TransactionQuantity + 1

		oldTransactionBalance, _ := decimal.NewFromString(marketCoinHistory.TransactionBalance)
		newTransactionBalance, _ := decimal.NewFromString(totalNum.Abs().String())
		marketCoinHistory.TransactionBalance = oldTransactionBalance.Add(newTransactionBalance).String()
		oldBalance, _ := decimal.NewFromString(marketCoinHistory.Balance)
		marketCoinHistory.Balance = oldBalance.Add(totalNum).String()

		if oldBalance.Cmp(decimal.Zero) == 0 {
			marketCoinHistory.CnyPrice = cnyPrice
			marketCoinHistory.UsdPrice = usdPrice
			marketCoinHistory.CnyAmount = cnyFee
			marketCoinHistory.UsdAmount = usdFee
		} else {
			marketCoinHistory.CnyAmount = marketCoinHistory.CnyAmount.Add(cnyFee)
			marketCoinHistory.UsdAmount = marketCoinHistory.UsdAmount.Add(usdFee)
			if oldBalance.Add(totalNum).Cmp(decimal.Zero) != 0 {
				marketCoinHistory.CnyPrice = marketCoinHistory.CnyAmount.DivRound(oldBalance.Add(totalNum), 2).String()
				marketCoinHistory.UsdPrice = marketCoinHistory.UsdAmount.DivRound(oldBalance.Add(totalNum), 2).String()
			}
		}
		marketCoinHistory.UpdatedAt = now

		log.Info("更新结果", zap.Any("marketCoinHistory", marketCoinHistory))
		if dt == marketCoinHistory.Dt {
			marketCoinHistory.TransactionQuantity = marketCoinHistory.TransactionQuantity + 1
			data.MarketCoinHistoryRepoClient.Update(nil, marketCoinHistory)
		} else {
			marketCoinHistory.Dt = dt
			marketCoinHistory.TransactionQuantity = 1
			marketCoinHistory.Id = 0
			data.MarketCoinHistoryRepoClient.SaveOrUpdate(nil, marketCoinHistory)
		}
	}
}

func UpdateAssetUidType() {
	log.Info("填补uid对应的钱包类型，处理用户资产表开始")

	var request = &data.AssetRequest{
		SelectColumn: "id, uid, address",
		OrderBy:      "id asc",
		PageSize:     data.MAX_PAGE_SIZE,
	}
	uidUidTypeMap := make(map[string]int8)
	var recordGroupList []*data.UserAsset
	recordGroupMap := make(map[string]*data.UserAsset)
	var err error
	var total int64
	err = data.UserAssetRepoClient.PageListAllCallBack(nil, request, func(userAssets []*data.UserAsset) error {
		total += int64(len(userAssets))
		for _, userAsset := range userAssets {
			key := userAsset.Uid + userAsset.Address
			recordGroupMap[key] = userAsset
		}
		return nil
	})
	if err != nil {
		log.Error("填补uid对应的钱包类型，从数据库中查询用户资产失败", zap.Any("total", total), zap.Any("error", err))
		return
	}
	if total == 0 {
		log.Info("填补uid对应的钱包类型，从数据库中查询用户资产为空", zap.Any("total", total))
		return
	}
	for _, userAsset := range recordGroupMap {
		recordGroupList = append(recordGroupList, userAsset)
	}

	recordSize := len(recordGroupList)
	if recordSize == 0 {
		log.Info("填补uid对应的钱包类型，从数据库中查询用户资产为空", zap.Any("size", recordSize), zap.Any("total", total))
		return
	}

	log.Info("填补uid对应的钱包类型，开始执行从Redis中查询钱包类型操作", zap.Any("size", recordSize), zap.Any("total", total))
	var num int
	for _, userAsset := range recordGroupList {
		uidType, ok := uidUidTypeMap[userAsset.Uid]
		if ok {
			continue
		}
		if num++; num%data.MAX_PAGE_SIZE == 0 {
			log.Info("填补uid对应的钱包类型，从Redis中查询钱包类型中", zap.Any("num", num), zap.Any("size", recordSize), zap.Any("total", total))
			time.Sleep(time.Duration(1) * time.Second)
		}
		uidType, _ = biz.GetUidTypeCode(userAsset.Address)
		uidUidTypeMap[userAsset.Uid] = uidType
	}

	log.Info("填补uid对应的钱包类型，开始执行修改数据库操作", zap.Any("size", len(uidUidTypeMap)))
	num = 0
	for uid, uidType := range uidUidTypeMap {
		if num++; num%biz.PAGE_SIZE == 0 {
			log.Info("填补uid对应的钱包类型，修改数据库中", zap.Any("num", num), zap.Any("size", len(uidUidTypeMap)))
			time.Sleep(time.Duration(2) * time.Second)
		}
		var count int64
		count, err = data.UserAssetRepoClient.UpdateUidTypeByUid(nil, uid, uidType)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			count, err = data.UserAssetRepoClient.UpdateUidTypeByUid(nil, uid, uidType)
		}
		if err != nil {
			log.Error("填补uid对应的钱包类型，将数据插入到数据库中失败", zap.Any("size", len(uidUidTypeMap)), zap.Any("count", count), zap.Any("uid", uid), zap.Any("uidType", uidType), zap.Any("error", err))
		}
	}

	log.Info("填补uid对应的钱包类型，处理用户资产表结束")
}

func UpdateAssetUid() {
	log.Info("修改钱包地址对应的uid，处理用户资产表开始")

	var request = &data.AssetRequest{
		SelectColumn: "id, address",
		Uid:          "10",
		OrderBy:      "id asc",
		PageSize:     data.MAX_PAGE_SIZE,
	}
	addressUidTypeMap := make(map[string]string)
	var recordGroupList []*data.UserAsset
	recordGroupMap := make(map[string]*data.UserAsset)
	var err error
	var total int64
	err = data.UserAssetRepoClient.PageListAllCallBack(nil, request, func(userAssets []*data.UserAsset) error {
		total += int64(len(userAssets))
		for _, userAsset := range userAssets {
			key := userAsset.Address
			recordGroupMap[key] = userAsset
		}
		return nil
	})
	if err != nil {
		log.Error("修改钱包地址对应的uid，从数据库中查询用户资产失败", zap.Any("total", total), zap.Any("error", err))
		return
	}
	if total == 0 {
		log.Info("修改钱包地址对应的uid，从数据库中查询用户资产为空", zap.Any("total", total))
		return
	}
	for _, userAsset := range recordGroupMap {
		recordGroupList = append(recordGroupList, userAsset)
	}

	recordSize := len(recordGroupList)
	if recordSize == 0 {
		log.Info("修改钱包地址对应的uid，从数据库中查询用户资产为空", zap.Any("size", recordSize), zap.Any("total", total))
		return
	}

	log.Info("修改钱包地址对应的uid，开始执行从Redis中查询钱包类型操作", zap.Any("size", recordSize), zap.Any("total", total))
	var num int
	for _, userAsset := range recordGroupList {
		uid, ok := addressUidTypeMap[userAsset.Address]
		if ok {
			continue
		}
		if num++; num%data.MAX_PAGE_SIZE == 0 {
			log.Info("修改钱包地址对应的uid，从Redis中查询钱包类型中", zap.Any("num", num), zap.Any("size", recordSize), zap.Any("total", total))
			time.Sleep(time.Duration(1) * time.Second)
		}
		_, uid, _ = biz.UserAddressSwitchRetryAlert(userAsset.ChainName, userAsset.Address)
		addressUidTypeMap[userAsset.Address] = uid
	}

	log.Info("修改钱包地址对应的uid，开始执行修改数据库操作", zap.Any("size", len(addressUidTypeMap)))
	num = 0
	for address, uid := range addressUidTypeMap {
		if num++; num%biz.PAGE_SIZE == 0 {
			log.Info("修改钱包地址对应的uid，修改数据库中", zap.Any("num", num), zap.Any("size", len(addressUidTypeMap)))
			time.Sleep(time.Duration(2) * time.Second)
		}
		var count int64
		count, err = data.UserAssetRepoClient.UpdateUidByAddress(nil, address, uid)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			count, err = data.UserAssetRepoClient.UpdateUidByAddress(nil, address, uid)
		}
		if err != nil {
			log.Error("修改钱包地址对应的uid，将数据插入到数据库中失败", zap.Any("size", len(addressUidTypeMap)), zap.Any("count", count), zap.Any("address", address), zap.Any("uid", uid), zap.Any("error", err))
		}
	}

	log.Info("修改钱包地址对应的uid，处理用户资产表结束")
}

func UpdateAssetTokenUri() {
	log.Info("填补token对应的tokenUri，处理用户资产表开始")

	var request = &data.AssetRequest{
		SelectColumn: "id, chain_name, token_address",
		TokenType:    2,
		OrderBy:      "id asc",
		PageSize:     data.MAX_PAGE_SIZE,
	}
	chainNameTokenAddressTokenInfoMap := make(map[string]map[string]types.TokenInfo)
	var recordGroupList []*data.UserAsset
	recordGroupMap := make(map[string]*data.UserAsset)
	var err error
	var total int64
	err = data.UserAssetRepoClient.PageListAllCallBack(nil, request, func(userAssets []*data.UserAsset) error {
		total += int64(len(userAssets))
		for _, userAsset := range userAssets {
			key := userAsset.ChainName + userAsset.TokenAddress
			recordGroupMap[key] = userAsset
		}
		return nil
	})
	if err != nil {
		log.Error("填补token对应的tokenUri，从数据库中查询用户资产失败", zap.Any("total", total), zap.Any("error", err))
		return
	}
	if total == 0 {
		log.Info("填补token对应的tokenUri，从数据库中查询用户资产为空", zap.Any("total", total))
		return
	}
	for _, userAsset := range recordGroupMap {
		recordGroupList = append(recordGroupList, userAsset)
	}

	recordSize := len(recordGroupList)
	if recordSize == 0 {
		log.Info("填补token对应的tokenUri，从数据库中查询用户资产为空", zap.Any("size", recordSize), zap.Any("total", total))
		return
	}

	log.Info("填补token对应的tokenUri，开始执行从nodeProxy中获取代币信息操作", zap.Any("size", recordSize), zap.Any("total", total))
	var num int
	requestNum := 1
	requestInterval := 100
	var chainNameTokenAddressMap = make(map[string][]string)
	for i, userAsset := range recordGroupList {
		tokenAddressList, ok := chainNameTokenAddressMap[userAsset.ChainName]
		if !ok {
			tokenAddressList = make([]string, 0)
		}
		tokenAddressList = append(tokenAddressList, userAsset.TokenAddress)
		chainNameTokenAddressMap[userAsset.ChainName] = tokenAddressList
		if i++; i%requestNum != 0 && i < recordSize {
			continue
		}

		if num++; num%requestInterval == 0 {
			log.Info("填补token对应的tokenUri，从nodeProxy中获取代币信息中", zap.Any("i", i), zap.Any("num", num), zap.Any("size", recordSize), zap.Any("total", total))
			time.Sleep(time.Duration(1) * time.Second)
		}
		var resultMap map[string]map[string]types.TokenInfo
		resultMap, err = biz.GetTokensInfo(nil, chainNameTokenAddressMap)
		if err != nil {
			log.Error("填补token对应的tokenUri，从nodeProxy中获取代币信息失败", zap.Any("i", i), zap.Any("num", num), zap.Any("size", recordSize), zap.Any("total", total), zap.Any("chainNameTokenAddressMap", chainNameTokenAddressMap), zap.Any("error", err))
			chainNameTokenAddressMap = make(map[string][]string)
			continue
		}
		for chainName, tokenAddressInfoMap := range resultMap {
			oldTokenAddressPriceMap, ok := chainNameTokenAddressTokenInfoMap[chainName]
			if !ok {
				chainNameTokenAddressTokenInfoMap[chainName] = tokenAddressInfoMap
			} else {
				for tokenAddress, tokenInfo := range tokenAddressInfoMap {
					oldTokenAddressPriceMap[tokenAddress] = tokenInfo
				}
			}
		}
		chainNameTokenAddressMap = make(map[string][]string)
	}

	log.Info("填补token对应的tokenUri，开始执行修改数据库操作", zap.Any("size", len(chainNameTokenAddressTokenInfoMap)))
	num = 0
	for chainName, tokenAddressTokenInfoMap := range chainNameTokenAddressTokenInfoMap {
		for tokenAddress, tokenInfo := range tokenAddressTokenInfoMap {
			tokenUri := tokenInfo.TokenUri
			if num++; num%biz.PAGE_SIZE == 0 {
				log.Info("填补token对应的tokenUri，修改数据库中", zap.Any("num", num), zap.Any("size", len(chainNameTokenAddressTokenInfoMap)))
				time.Sleep(time.Duration(2) * time.Second)
			}
			var count int64
			count, err = data.UserAssetRepoClient.UpdateTokenUriByChainTokenAddress(nil, chainName, tokenAddress, tokenUri)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				count, err = data.UserAssetRepoClient.UpdateTokenUriByChainTokenAddress(nil, chainName, tokenAddress, tokenUri)
			}
			if err != nil {
				log.Error("填补token对应的tokenUri，将数据插入到数据库中失败", zap.Any("size", len(chainNameTokenAddressTokenInfoMap)), zap.Any("count", count), zap.Any("chainName", chainName), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenUri", tokenUri), zap.Any("error", err))
			}
		}
	}

	log.Info("填补token对应的tokenUri，处理用户资产表结束")
}

func UpdateSignAddress() {
	log.Info("将签名记录中钱包地址转换为标准格式，处理签名记录表开始")

	var request = &data.SignRequest{
		SelectColumn: "id, chain_name, address",
		OrderBy:      "id asc",
		PageSize:     data.MAX_PAGE_SIZE,
	}
	addressUpdateAddressMap := make(map[string][]string)
	var recordGroupList []*data.UserSendRawHistory
	recordGroupMap := make(map[string]*data.UserSendRawHistory)
	var err error
	var total int64
	err = data.UserSendRawHistoryRepoInst.PageListAllCallBack(nil, request, func(userSendRawHistorys []*data.UserSendRawHistory) error {
		total += int64(len(userSendRawHistorys))
		for _, userSendRawHistory := range userSendRawHistorys {
			key := userSendRawHistory.ChainName + userSendRawHistory.Address
			recordGroupMap[key] = userSendRawHistory
		}
		return nil
	})
	if err != nil {
		log.Error("将签名记录中钱包地址转换为标准格式，从数据库中查询用户资产失败", zap.Any("total", total), zap.Any("error", err))
		return
	}
	if total == 0 {
		log.Info("将签名记录中钱包地址转换为标准格式，从数据库中查询用户资产为空", zap.Any("total", total))
		return
	}
	for _, userSendRawHistory := range recordGroupMap {
		recordGroupList = append(recordGroupList, userSendRawHistory)
	}

	recordSize := len(recordGroupList)
	if recordSize == 0 {
		log.Info("将签名记录中钱包地址转换为标准格式，从数据库中查询用户资产为空", zap.Any("size", recordSize), zap.Any("total", total))
		return
	}

	log.Info("将签名记录中钱包地址转换为标准格式，开始执行从nodeProxy中获取代币信息操作", zap.Any("size", recordSize), zap.Any("total", total))
	for _, userSendRawHistory := range recordGroupList {
		chainType, _ := biz.GetChainNameType(userSendRawHistory.ChainName)
		switch chainType {
		case biz.EVM:
			address := types2.HexToAddress(userSendRawHistory.Address).Hex()
			if userSendRawHistory.Address != address {
				updateAddressList, ok := addressUpdateAddressMap[address]
				if !ok {
					updateAddressList = make([]string, 0)
				}
				updateAddressList = append(updateAddressList, userSendRawHistory.Address)
				addressUpdateAddressMap[address] = updateAddressList
			}
		}
	}

	log.Info("将签名记录中钱包地址转换为标准格式，开始执行修改数据库操作", zap.Any("size", len(addressUpdateAddressMap)))
	var num int
	for address, updateAddressList := range addressUpdateAddressMap {
		if num++; num%biz.PAGE_SIZE == 0 {
			log.Info("将签名记录中钱包地址转换为标准格式，修改数据库中", zap.Any("num", num), zap.Any("size", len(addressUpdateAddressMap)))
			time.Sleep(time.Duration(2) * time.Second)
		}
		var count int64
		count, err = data.UserSendRawHistoryRepoInst.UpdateAddressListByAddress(nil, address, updateAddressList)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			count, err = data.UserSendRawHistoryRepoInst.UpdateAddressListByAddress(nil, address, updateAddressList)
		}
		if err != nil {
			log.Error("将签名记录中钱包地址转换为标准格式，将数据插入到数据库中失败", zap.Any("size", len(addressUpdateAddressMap)), zap.Any("count", count), zap.Any("address", address), zap.Any("updateAddressList", updateAddressList), zap.Any("error", err))
		}
	}

	log.Info("将签名记录中钱包地址转换为标准格式，处理用户资产表结束")
}

func UpdateBTCAmount() {
	var cursor int64
	pricesByDt := make(map[uint32]string)
	for {
		log.Info("HANDLING", zap.Int64("cursor", cursor))
		list, err := data.ChainTypeAssetRepoClient.ListByCursor(context.Background(), &cursor, 100)
		if err != nil {
			log.Error("更新资陈记录 BTC 计价失败", zap.Error(err))
			return
		}
		if len(list) == 0 {
			return
		}
		toUpdated := make([]*data.ChainTypeAsset, 0, len(list))
		for _, item := range list {
			btcUsd, ok := pricesByDt[uint32(item.Dt)]
			if !ok {
				var err error
				btcUsd, err = biz.GetBTCUSDPriceByTimestamp(context.Background(), uint32(item.Dt))
				pricesByDt[uint32(item.Dt)] = btcUsd
				if err != nil {
					log.Warn("根据时间戳拉取 BTC 价格失败", zap.Error(err))
					continue
				}
			}
			if btcUsd == "" {
				continue
			}

			btcUsds, _ := decimal.NewFromString(btcUsd)
			item.BtcAmount = item.UsdAmount.Div(btcUsds)
			toUpdated = append(toUpdated, item)
		}
		if len(toUpdated) > 0 {
			data.ChainTypeAssetRepoClient.BatchSaveOrUpdate(context.Background(), toUpdated)
		}
	}
}

type AddressAndUid struct {
	Address string
	Uid     string
}

var btcUrls = []string{
	"https://zpka_6fcb516767e641788d81729aa4c1e424_126062cf@svc.blockdaemon.com/universal/v1",
	"https://zpka_4b1caefd905344c0b7421d59db313978_056073db@svc.blockdaemon.com/universal/v1",
	"https://zpka_de0aedcf3fdf4f35a619bc25a4a76161_6e3b2032@svc.blockdaemon.com/universal/v1",
	"https://zpka_97260809ad9843d989d69cbab3c7ba3d_16d3e166@svc.blockdaemon.com/universal/v1",
	"https://zpka_31c85f0b836a40cd8f8b64496a39213d_758cb8e7@svc.blockdaemon.com/universal/v1",
	"https://zpka_88e506f3cbd04f598b2d69d7281dad56_649e7352@svc.blockdaemon.com/universal/v1",
	"https://zpka_04775fe772954e40a4b90571ebc41ce2_2c080edb@svc.blockdaemon.com/universal/v1",
	"https://zpka_a92cabf094b44a5d900c1887560416ef_641a1abf@svc.blockdaemon.com/universal/v1",
	"https://zpka_d310f812910c40c8a808d6d4296b81a0_67fdfbf8@svc.blockdaemon.com/universal/v1",
	"https://zpka_febc2cded89d4359b744af2d41eda59f_7a7c7e75@svc.blockdaemon.com/universal/v1",
	"https://zpka_a96686455d374da6a418f65eaca8a0a5_1652e9a7@svc.blockdaemon.com/universal/v1",
	"https://zpka_2991d45e50054722ba547e54e739a7e8_41cff525@svc.blockdaemon.com/universal/v1",
	"https://zpka_630c27c7492847b18b0b21aa346fc0ab_011359a9@svc.blockdaemon.com/universal/v1",
	"https://zpka_8976bc36b6c84ef6b493460b1dc4a8ce_4e56d164@svc.blockdaemon.com/universal/v1",
	"https://zpka_ea8d8727c06349e6b93d939d328e0e04_4085b24f@svc.blockdaemon.com/universal/v1",
}

// UpdateUserUtxo 更新所有用户的UTXO
// 1、查询交易记录表中所有有uid的地址
// 2、遍历所有地址，获取地址的UTXO并更新到数据库
func UpdateUserUtxo() {

	chains := []string{
		"BTC",
		"LTC",
		"DOGE",
		//"Kaspa",
	}
	var tableName string
	for _, chainName := range chains {

		//删除该链所有UTXO
		if err := data.BlockCreawlingDB.Table("utxo_unspent_record").
			Delete(nil, "chain_name = ?", chainName).Error; err != nil {
			log.Error("delete utxo error", zap.String("chain name", chainName))
			continue
		}

		if chainName == "Kaspa" {
			UpdateKaspaUtxo()
			continue
		}

		log.Info("start update utxo", zap.String("chainName", chainName))

		if chainName == "BTC" {
			tableName = "btc_transaction_record"
		} else if chainName == "LTC" {
			tableName = "ltc_transaction_record"
		} else if chainName == "DOGE" {
			tableName = "doge_transaction_record"
		}

		//从交易记录中查询有uid的fromAddress
		var fromAddressAndUids []AddressAndUid
		if err := data.BlockCreawlingDB.Table(tableName).Select("distinct(from_address) as address,from_uid as uid").Where("from_uid != ''").Find(&fromAddressAndUids).Error; err != nil {
			log.Error(err.Error())
		}

		//从交易记录中查询有uid的toAddress
		var toAddressAndUids []AddressAndUid
		if err := data.BlockCreawlingDB.Table(tableName).Select("distinct(to_address) as address,to_uid as uid").Where("to_uid != ''").Find(&toAddressAndUids).Error; err != nil {
			log.Error(err.Error())
		}

		//合并fromAddress和toAddress，遍历更新UTXO
		addressAndUids := append(fromAddressAndUids, toAddressAndUids...)
		//addressAndUids := []AddressAndUid{{
		//	Address: "DHQsfy66JsYSnwjCABFN6NNqW4kHQe63oU",
		//	Uid:     "uid",
		//}}
		platInfo, _ := biz.GetChainPlatInfo(chainName)
		decimals := int(platInfo.Decimal)
		client := bitcoin.NewOklinkClient(chainName, "https://83c5939d-9051-46d9-9e72-ed69d5855209@www.oklink.com")
		for _, addressAndUid := range addressAndUids {
			log.Info("start update user utxo", zap.String("address", addressAndUid.Address), zap.String("uid", addressAndUid.Uid))

			utxoList, err := client.GetUTXO(addressAndUid.Address)
			if err != nil {
				continue
			}

			for _, utxo := range utxoList {
				index, _ := strconv.Atoi(utxo.Index)
				blockTime, _ := strconv.ParseInt(utxo.BlockTime, 10, 64)
				amountDecimal, _ := decimal.NewFromString(utxo.UnspentAmount)
				amount := biz.Pow10(amountDecimal, decimals).BigInt().String()
				var utxoUnspentRecord = &data.UtxoUnspentRecord{
					Uid:       addressAndUid.Uid,
					Hash:      utxo.Txid,
					N:         index,
					ChainName: chainName,
					Address:   addressAndUid.Address,
					Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
					Amount:    amount,
					TxTime:    blockTime,
					UpdatedAt: time.Now().Unix(),
				}

				r, err := data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
				if err != nil {
					log.Error("更新用户UTXO，将数据插入到数据库中", zap.Any("chainName", chainName), zap.Any("address", addressAndUid.Address), zap.Any("插入utxo对象结果", r), zap.Any("error", err))
				}
			}
		}
	}

}

func UpdateKaspaUtxo() {
	chainName := "Kaspa"
	tableName := "kaspa_transaction_record"

	log.Info("start update utxo", zap.String("chainName", chainName))

	//从交易记录中查询有uid的fromAddress
	var fromAddressAndUids []AddressAndUid
	if err := data.BlockCreawlingDB.Table(tableName).Select("distinct(from_address) as address,from_uid as uid").Where("from_uid != ''").Find(&fromAddressAndUids).Error; err != nil {
		log.Error(err.Error())
	}

	//从交易记录中查询有uid的toAddress
	var toAddressAndUids []AddressAndUid
	if err := data.BlockCreawlingDB.Table(tableName).Select("distinct(to_address) as address,to_uid as uid").Where("to_uid != ''").Find(&toAddressAndUids).Error; err != nil {
		log.Error(err.Error())
	}

	//合并fromAddress和toAddress，遍历更新UTXO
	addressAndUids := append(fromAddressAndUids, toAddressAndUids...)
	//addressAndUids := []AddressAndUid{{
	//	Address: "D8ZEVbgf4yPs3MK8dMJJ7PpSyBKsbd66TX",
	//	Uid:     "uid",
	//}}

	client := kaspa.NewClient(chainName, "https://api.kaspa.org")
	for _, addressAndUid := range addressAndUids {
		log.Info("start update user utxo", zap.String("address", addressAndUid.Address), zap.String("uid", addressAndUid.Uid))

		utxoList, err := client.GetUtxo(addressAndUid.Address)
		for i := 0; i < len(btcUrls) && err != nil; i++ {
			utxoList, err = client.GetUtxo(addressAndUid.Address)
		}
		if err != nil {
			continue
		}

		for _, d := range utxoList {
			tx, err := client.GetTransactionByHash(d.Outpoint.TransactionId)
			if err != nil {
				continue
			}

			var utxoUnspentRecord = &data.UtxoUnspentRecord{
				ChainName: chainName,
				Uid:       addressAndUid.Uid,
				Address:   addressAndUid.Address,
				Hash:      d.Outpoint.TransactionId,
				N:         d.Outpoint.Index,
				Script:    d.UtxoEntry.ScriptPublicKey.ScriptPublicKey,
				Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
				Amount:    d.UtxoEntry.Amount,
				TxTime:    tx.BlockTime / 1000,
				UpdatedAt: time.Now().Unix(),
			}

			r, err := data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
			if err != nil {
				log.Error("更新用户UTXO，将数据插入到数据库中", zap.Any("chainName", chainName), zap.Any("address", addressAndUid.Address), zap.Any("插入utxo对象结果", r), zap.Any("error", err))
			}
		}
	}
}

func NormalizeBenfenCoinType() {
	chainName := "BenfenTEST"
	records, err := data.UserAssetRepoClient.ListByChainNames(context.Background(), []string{chainName})
	if err != nil {
		panic(err)
	}
	toUpdate := make([]*data.UserAsset, 0, 4)
	for _, item := range records {
		if len(item.TokenAddress) == 0 {
			continue
		}
		item.TokenAddress = swap.NormalizeBenfenCoinType(chainName, item.TokenAddress)
		toUpdate = append(toUpdate, item)
	}
	if len(toUpdate) > 0 {
		_, err = data.UserAssetRepoClient.BatchSaveOrUpdate(context.Background(), toUpdate)
		if err != nil {
			panic(err)
		}
	}

	txRecords, err := data.SuiTransactionRecordRepoClient.ListAll(context.Background(), biz.GetTableName(chainName))
	if err != nil {
		panic(err)
	}
	toUpdated := make([]*data.SuiTransactionRecord, 0, 4)
	for _, record := range txRecords {
		if strings.Contains(record.ContractAddress, "::") {
			record.ContractAddress = swap.NormalizeBenfenCoinType(chainName, record.ContractAddress)
			toUpdated = append(toUpdated, record)
		}
	}
	if len(toUpdated) > 0 {
		_, err = data.SuiTransactionRecordRepoClient.BatchSaveOrUpdate(context.Background(), biz.GetTableName(chainName), toUpdated)
		if err != nil {
			panic(err)
		}
	}
}

func CleanupBefenAsset() {
	chainName := "BenfenTEST"
	if err := fixupBenfenToken(chainName); err != nil {
		panic(err)
	}
	if err := cleanupBenfenTokenAsset(chainName); err != nil {
		panic(err)
	}
	if err := cleanupBenfenNFTAsset(chainName); err != nil {
		panic(err)
	}
}

func fixupBenfenToken(chainName string) error {
	records, err := data.BFCStationRepoIns.ListAllAccountTokens(context.Background(), chainName)
	if err != nil {
		return err
	}
	for _, r := range records {
		var yes bool
		if strings.HasPrefix(r.TokenCoinType, "0x") {
			r.TokenCoinType = r.TokenCoinType[2:]
			yes = true
		}
		rawBalance, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
			c := client.(*sui.Client)
			return c.GetTokenBalance(r.Address, "0x"+r.TokenCoinType, 0)
		})
		if err != nil {
			rawBalance = "0"
		}
		balance, _ := decimal.NewFromString(rawBalance.(string))
		if balance.String() != r.Balance.String() {
			r.Balance = balance
			println("new balance of address: %s", r.Address, ", token: %s", r.TokenCoinType, ", balance: ", balance.String())
			yes = true
		}
		if yes {
			err = data.BFCStationRepoIns.SaveAccountToken(context.Background(), chainName, &r)
			if err != nil {
				println(err.Error())
			}
		}
	}

	return nil
}

func cleanupBenfenTokenAsset(chainName string) error {
	records, err := data.UserAssetRepoClient.ListByChainNames(context.Background(), []string{chainName})
	if err != nil {
		return fmt.Errorf("[listAssets] %w", err)
	}
	for _, r := range records {
		if r.TokenAddress == "" {
			continue
		}
		rawBalance, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
			c := client.(*sui.Client)
			return c.GetTokenBalance(r.Address, r.TokenAddress, 0)
		})
		var errStr string
		if err != nil {
			errStr = err.Error()
			rawBalance = "0"
		}
		balance := rawBalance.(string)
		if balance == "0" || err != nil {
			println("Cleaning, id: ", r.Id, ", address: ", r.Address, ", tokenAddress: ", r.TokenAddress, ", balance: ", balance, ", err: ", errStr)
			if _, err := data.UserAssetRepoClient.DeleteByID(context.Background(), r.Id); err != nil {
				return fmt.Errorf("[deleteAsset] %w", err)
			}
		}
	}
	return nil
}

func cleanupBenfenNFTAsset(chainName string) error {
	records, err := data.UserNftAssetRepoClient.ListByChainNames(context.Background(), []string{chainName})
	if err != nil {
		return fmt.Errorf("[listNfts] %w", err)
	}
	for _, r := range records {
		rawBalance, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
			c := client.(*sui.Client)
			return c.Erc721BalanceByTokenId(r.Address, r.TokenAddress, r.TokenId)
		})
		var errStr string
		if err != nil {
			errStr = err.Error()
			rawBalance = "0"
		}
		balance := rawBalance.(string)
		if balance == "0" || err != nil {
			println("Cleaning, id: ", r.Id, ", address: ", r.Address, ", tokenAddress: ", r.TokenAddress, ", tokenId: ", r.TokenId, ", balance: ", balance, ", err: ", errStr)
			if _, err := data.UserNftAssetRepoClient.DeleteByID(context.Background(), r.Id); err != nil {
				return fmt.Errorf("[deleteNft] %w", err)
			}
		}
	}
	return nil
}

func InitUserAssetCostPrice() {
	log.Info("InitUserAssetCostPrice start")
	now := time.Now().Unix()

	//获取自定义链配置
	chainNodeInUsedList, _ := biz.GetCustomChainList(nil)
	for _, chainInfo := range chainNodeInUsedList.Data {
		cp := &conf.PlatInfo{
			Chain:          chainInfo.Chain,
			Type:           chainInfo.Type,
			RpcURL:         chainInfo.Urls,
			ChainId:        chainInfo.ChainId,
			Decimal:        int32(chainInfo.Decimals),
			NativeCurrency: chainInfo.CurrencyName,
			Source:         biz.SOURCE_REMOTE,
			Handler:        chainInfo.Chain,
			GetPriceKey:    chainInfo.GetPriceKey,
		}
		if chainInfo.IsTest {
			cp.NetType = biz.TEST_NET_TYPE
		} else {
			cp.NetType = biz.MAIN_NET_TYPE
		}
		biz.SetChainPlatInfo(cp.Chain, cp)
	}

	//分页查询所有uid
	ctx := context.Background()
	var uids []string
	var offset, limit = 0, 50
	var err error

	//分页查询uid
	for {
		uids, err = data.UserAssetRepoClient.FindDistinctUidByOffset(ctx, offset, limit)
		offset += limit
		if err != nil {
			log.Error("FindDistinctUidByOffset error", zap.String("error", err.Error()))
			continue
		}

		if len(uids) == 0 {
			return
		}

		//批量查询用户资产
		userAssets, err := data.UserAssetRepoClient.FindByUids(ctx, uids)
		if err != nil {
			log.Error("FindByUids error", zap.String("error", err.Error()))
			continue
		}

		if len(userAssets) == 0 {
			continue
		}
		//获取所有币价
		tokenPriceMap, err := biz.GetAssetsPrice(userAssets)
		if err != nil {
			continue
		}

		for _, asset := range userAssets {
			//只更新成本价为 0 的资产
			if asset.CostPrice != "0" {
				continue
			}

			//余额为 0 没有成本价
			if asset.Balance == "0" {
				continue
			}
			var key string
			if asset.TokenAddress == "" {
				platInfo, _ := biz.GetChainPlatInfo(asset.ChainName)
				if platInfo == nil {
					continue
				}
				key = platInfo.GetPriceKey
			} else {
				key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
			}
			price := decimal.NewFromFloat(tokenPriceMap[key].Price)
			asset.CostPrice = price.String()
			asset.UpdatedAt = now
		}

		_, err = data.UserAssetRepoClient.BatchSaveOrUpdate(ctx, userAssets)
		if err != nil {
			continue
		}

	}
}
