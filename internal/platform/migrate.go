package platform

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"strconv"
	"strings"
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
		flag, fromUid, _ := biz.UserAddressSwitch(record.FromObj)
		if flag {
			fu = fromUid
		}
		flag1, toUid, _ := biz.UserAddressSwitch(record.ToObj)
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
			data.BtcTransactionRecordRepoClient.BatchSaveOrUpdate(nil, biz.GetTalbeName(record.ChainName), btc)

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
	flag, fromUid, _ := biz.UserAddressSwitch(record.FromObj)
	if flag {
		fu = fromUid
	}
	flag1, toUid, _ := biz.UserAddressSwitch(record.ToObj)
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
	for key, platform := range biz.PlatInfoMap {
		switch platform.Type {

		case biz.EVM:
			rs, e := data.EvmTransactionRecordRepoClient.ListByTransactionType(nil, biz.GetTalbeName(key), "approve")
			if e == nil && len(rs) > 0 {

				biz.DappApproveFilter(key, rs)
			}
		}
	}
}
