package platform

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/types"
	"encoding/json"
	"errors"
	"fmt"
	types2 "github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"time"
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

func BtcReset() {
	for key, platform := range biz.PlatInfoMap {
		switch platform.Type {

		case biz.BTC:
			rs, e := data.BtcTransactionRecordRepoClient.ListAll(nil, biz.GetTalbeName(key))
			if e == nil && len(rs) > 0 {
				client := bitcoin.NewClient(platform.RpcURL[0], key)
				bitcoin.UnspentTx(key, client, rs)
			}
		}
	}
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
		userAddress, _, err := biz.UserAddressSwitch(userAsset.Address)
		for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			userAddress, _, err = biz.UserAddressSwitch(userAsset.Address)
		}
		if err == nil {
			if userAddress {
				chainType := biz.ChainNameType[userAsset.ChainName]
				switch chainType {
				case biz.EVM:
					address := types2.HexToAddress(userAsset.Address).Hex()
					tokenAddress := userAsset.TokenAddress
					if userAsset.TokenAddress != "" {
						tokenAddress = types2.HexToAddress(userAsset.TokenAddress).Hex()
					}
					if address != userAsset.Address || tokenAddress != userAsset.TokenAddress {
						userAssetList = append(userAssetList, userAsset.Id)
					}
				}
			} else {
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
	log.Info("删数据更新用户资产", zap.Any("size", len(userAssetList)), zap.Any("error", err))
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
			fromAddress := types2.HexToAddress(eventLog.From).Hex()
			toAddress := types2.HexToAddress(eventLog.To).Hex()
			tokenAddress := types2.HexToAddress(eventLog.Token.Address).Hex()

			key := evmTxRecord.ChainName + fromAddress + tokenAddress
			_, ok := userAssetMap[key]
			if !ok {
				userFromAddress, fromUid, err := biz.UserAddressSwitch(fromAddress)
				for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					userFromAddress, fromUid, err = biz.UserAddressSwitch(fromAddress)
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
				userToAddress, toUid, err := biz.UserAddressSwitch(toAddress)
				for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					userToAddress, toUid, err = biz.UserAddressSwitch(toAddress)
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

	log.Info("从eventLog中补数据更新用户资产结束")
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
		chainType := biz.ChainNameType[userAsset.ChainName]
		switch chainType {
		case biz.EVM:
			userAsset.Address = types2.HexToAddress(userAsset.Address).Hex()
			userAsset.TokenAddress = types2.HexToAddress(userAsset.TokenAddress).Hex()
		case biz.STC:
			if userAsset.TokenAddress == starcoin.STC_CODE {
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

	log.Info("补数据更新用户资产结束")
}

func doHandleAsset(userAssetList []*data.UserAsset) []*data.UserAsset {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleAsset error, size:"+strconv.Itoa(len(userAssetList)), e)
			} else {
				log.Errore("HandleAsset panic, size:"+strconv.Itoa(len(userAssetList)), errors.New(fmt.Sprintf("%s", err)))
			}
			return
		}
	}()

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
		_, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, successUserAssetList, biz.PAGE_SIZE)
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

	nodeURL := biz.PlatInfoMap[chainName].RpcURL
	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		c, err := ethereum.NewClient(url, chainName)
		if err != nil {
			panic(err)
		}
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
		log.Error(chainName+"query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
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
		log.Error(chainName+"query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenDecimalsMap), zap.Any("error", err))
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
	client := starcoin.NewClient(biz.PlatInfoMap[chainName].RpcURL[0])
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
		log.Error(chainName+"query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
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
		log.Error(chainName+"query balance error", zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
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
	client := bitcoin.NewClient(biz.PlatInfoMap[chainName].RpcURL[0], chainName)
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
		log.Error(chainName+"query balance error", zap.Any("address", address), zap.Any("error", err))
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
