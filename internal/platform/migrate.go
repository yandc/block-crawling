package platform

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/aptos"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/platform/starcoin"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/types"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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

func CheckNonce() {
	for key, platform := range biz.PlatInfoMap {
		switch platform.Type {

		case biz.EVM:
			rs, e := data.EvmTransactionRecordRepoClient.FindFromAddress(nil, biz.GetTalbeName(key))
			if e == nil && len(rs) > 0 {
				total := 0
				for _, address := range rs {
					total = total + 1
					nonceKey := biz.ADDRESS_DONE_NONCE + key + ":" + address
					nonceStr, _ := data.RedisClient.Get(nonceKey).Result()
					nonce, _ := strconv.Atoi(nonceStr)
					n, _ := data.EvmTransactionRecordRepoClient.FindLastNonceByAddress(nil, biz.GetTalbeName(key), address)
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
	chainName := "Arbitrum"
	tokenParams := []TokenParam{{
		TokenAddress: "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
		OldDecimals:  18,
		OldSymbol:    "WBTC",
		NewDecimals:  8,
		NewSymbol:    "WBTC",
	}}

	doHandleTokenInfo(chainName, tokenParams)
}

func doHandleTokenInfo(chainName string, tokenParams []TokenParam) {
	log.Info("处理交易记录TokenInfo开始")
	source := biz.AppConfig.Target
	dbSource, err := gorm.Open(postgres.Open(source), &gorm.Config{})
	if err != nil {
		log.Errore("source DB error", err)
	}

	tableName := biz.GetTalbeName(chainName)

	tokenParamMap := make(map[string]TokenParam)
	var txRecords []*data.EvmTransactionRecord
	for _, tokenParam := range tokenParams {
		tokenParamMap[tokenParam.TokenAddress] = tokenParam
		txRecordList, err := getTxRecord(dbSource, tableName, tokenParam, biz.PAGE_SIZE)
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
			tokenInfo, err := biz.PaseTokenInfo(parseDataStr)
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
			err = json.Unmarshal([]byte(eventLogStr), &eventLogs)
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
			eventLogJson, _ := json.Marshal(eventLogs)
			eventLogStr = string(eventLogJson)
			txRecord.EventLog = eventLogStr
		}
	}

	count, err := data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecords, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		count, err = data.EvmTransactionRecordRepoClient.PageBatchSaveOrUpdateSelectiveById(nil, tableName, txRecords, biz.PAGE_SIZE)
	}
	if err != nil {
		log.Error("更新交易记录，将数据插入到数据库中失败", zap.Any("size", len(txRecords)), zap.Any("count", count), zap.Any("error", err))
	}
	log.Info("处理交易记录TokenInfo结束", zap.Any("query size", len(txRecords)), zap.Any("affected count", count))
}

func getTxRecord(dbSource *gorm.DB, talbeName string, tokenParam TokenParam, limit int) ([]*data.EvmTransactionRecord, error) {
	var txRecords []*data.EvmTransactionRecord
	var txRecordList []*data.EvmTransactionRecord
	id := 0
	total := limit
	for total == limit {
		sqlStr := getSql(talbeName, tokenParam.TokenAddress, tokenParam.OldDecimals, tokenParam.OldSymbol, id, limit)

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

func getSql(talbeName string, tokenAdress string, decimals int, symbol string, id, limit int) string {
	s := "SELECT id, parse_data, event_log from " + talbeName +
		" where ((parse_data like '%" + tokenAdress + "%' and parse_data like '%\"decimals\":" + strconv.Itoa(decimals) + ",\"symbol\":\"" + symbol + "\"%') " +
		"or (event_log like '%" + tokenAdress + "%' and event_log like '%\"decimals\":" + strconv.Itoa(decimals) + ",\"symbol\":\"" + symbol + "\"%')) " +
		"and id > " + strconv.Itoa(id) + " order by id asc limit " + strconv.Itoa(limit) + ";"
	return s
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
		if platInfo, ok := biz.PlatInfoMap[userAsset.ChainName]; ok {
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
					address := userAsset.Address
					if userAsset.Address != "" {
						address = types2.HexToAddress(userAsset.Address).Hex()
					}
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
		chainType := biz.ChainNameType[userAsset.ChainName]
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
	if platInfo, ok := biz.PlatInfoMap[chainName]; ok {
		nodeURL = platInfo.RpcURL
	} else {
		return nil, errors.New("chain " + chainName + " is not support")
	}
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
	var nodeURL []string
	if platInfo, ok := biz.PlatInfoMap[chainName]; ok {
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
	var nodeURL []string
	if platInfo, ok := biz.PlatInfoMap[chainName]; ok {
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
