package ethereum

import (
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"

	"go.uber.org/zap"
)

func HandleRecord(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleRecord error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleRecord panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	go biz.DappApproveFilter(chainName, txRecords)
	go HandleMarketCoinHistory(chainName, txRecords)
	go biz.NftApproveFilter(chainName, txRecords)
	go func() {
		HandleTokenPush(chainName, client, txRecords)
		HandleUserAsset(chainName, client, txRecords)
	}()
	go HandleUserStatistic(chainName, client, txRecords)
	go HandleTransactionCount(chainName, client, txRecords)
	go HandleUserNonce(chainName, client, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go HandleUserNftAsset(false, chainName, client, txRecords)
	go HandleUserStatus(chainName, client, txRecords)
}

func HandlePendingRecord(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandlePendingRecord error, chainName:"+chainName, e)
			} else {
				log.Errore("HandlePendingRecord panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	go func() {
		HandleTokenPush(chainName, client, txRecords)
		HandleUserAsset(chainName, client, txRecords)
	}()
	go HandleTransactionCount(chainName, client, txRecords)
	go HandleNftRecord(chainName, client, txRecords)
	go HandleUserNftAsset(true, chainName, client, txRecords)
	go HandleUserStatus(chainName, client, txRecords)
	go biz.DappApproveFilter(chainName, txRecords)
}

func HandleUserStatus(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserStatus error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserStatus panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户交易状态失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	//1. nonce = 0 时 跨链时，交易已经时终结状态不会查出来。
	//2.用户 根据api 插入的pending交易时，这个nonce 是 -1 不做处理.
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}
		if record.TransactionType == biz.EVENTLOG {
			continue
		}
		if record.FromUid == "" {
			continue
		}
		//查询所有交易记录
		ets, e := data.EvmTransactionRecordRepoClient.UpdateByNonceAndAddressAndStatus(nil, biz.GetTableName(chainName), record.FromAddress, record.Nonce, biz.DROPPED_REPLACED)
		if e != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户交易状态失败, error：%s", chainName, fmt.Sprintf("%s", e))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
		if ets > 0 {
			rs, _ := data.EvmTransactionRecordRepoClient.FindNonceAndAddressAndStatus(nil, biz.GetTableName(chainName), record.FromAddress, record.Nonce)
			for _, r := range rs {
				var ssr []biz.SignStatusRequest
				ssr = append(ssr, biz.SignStatusRequest{
					TransactionHash: r.TransactionHash,
					Status:          r.Status,
					TransactionType: r.TransactionType,
					Nonce:           r.Nonce,
				})
				go biz.SyncStatus(ssr)
			}
		}
	}
}

func HandleUserNonce(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	doneNonce := make(map[string]int)
	doneNonceTotal := make(map[string]int)
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}
		if record.TransactionType == biz.EVENTLOG {
			continue
		}
		nonceKey := biz.ADDRESS_DONE_NONCE + chainName + ":" + record.FromAddress
		dnt := doneNonceTotal[nonceKey]
		doneNonceTotal[nonceKey] = dnt + 1
		bh := doneNonce[nonceKey]
		if bh == 0 {
			doneNonce[nonceKey] = int(record.Nonce)
		} else {
			if bh < int(record.Nonce) {
				doneNonce[nonceKey] = int(record.Nonce)
			}
		}
	}
	//1   2条 2 3  取出 3 -2 == 1
	for k, _ := range doneNonce {
		//判断 链上nonce
		ctx := context.Background()
		rets := strings.Split(k, ":")
		if len(rets) >= 3 {
			result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
				cli, _ := getETHClient(client.url)
				defer cli.Close()
				return cli.NonceAt(ctx, common.HexToAddress(rets[2]), nil)
			})
			nonce := result.(uint64)
			if nonce > 0 {
				doneN := int(nonce) - 1
				if err == nil {
					data.RedisClient.Set(k, strconv.Itoa(doneN), 0)
				}
			}
		}
	}
}

func HandleUserAsset(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserAsset error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserAsset panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	now := time.Now().Unix()
	var userAssets []*data.UserAsset
	userAssetMap := make(map[string]*data.UserAsset)
	addressTokenMap := make(map[string]map[string]int)
	tokenInfoMap := make(map[string]*types.TokenInfo)
	addressUidMap := make(map[string]string)
	var mainDecimals int32
	var mainSymbol string
	for _, record := range txRecords {
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		var tokenAddress = record.ContractAddress
		if strings.HasPrefix(chainName, "Polygon") && tokenAddress == POLYGON_CODE {
			tokenAddress = ""
		}
		if strings.HasPrefix(chainName, "zkSync") && tokenAddress == ZKSYNC_CODE {
			tokenAddress = ""
		}

		if record.TransactionType != biz.CONTRACT && record.TransactionType != biz.SWAP && record.TransactionType != biz.MINT &&
			record.TransactionType != biz.APPROVE && record.TransactionType != biz.APPROVENFT {
			tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
			if err != nil {
				// 更新用户资产出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，解析tokenInfo失败", chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("更新用户资产，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
					zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
				continue
			}
			tokenType := tokenInfo.TokenType
			decimals := tokenInfo.Decimals
			symbol := tokenInfo.Symbol

			if tokenType != biz.ERC721 && tokenType != biz.ERC1155 {
				if record.TransactionType == biz.NATIVE || tokenAddress == "" {
					if record.FromAddress != "" && record.FromUid != "" {
						_, ok := addressUidMap[record.FromAddress]
						if !ok {
							addressUidMap[record.FromAddress] = record.FromUid
						}
					}

					if record.ToAddress != "" && record.ToUid != "" {
						_, ok := addressUidMap[record.ToAddress]
						if !ok {
							addressUidMap[record.ToAddress] = record.ToUid
						}
					}

					if mainSymbol == "" {
						mainDecimals = int32(decimals)
						mainSymbol = symbol
					}
				} else if tokenAddress != "" {
					tokenInfoMap[tokenAddress] = tokenInfo
					if record.FromAddress != "" && record.FromUid != "" {
						fromKey := record.FromUid + "," + record.FromAddress
						tokenDecimalsMap, ok := addressTokenMap[fromKey]
						if !ok {
							tokenDecimalsMap = make(map[string]int)
							addressTokenMap[fromKey] = tokenDecimalsMap
						}
						tokenDecimalsMap[tokenAddress] = int(decimals)
					}

					if record.ToAddress != "" && record.ToUid != "" {
						toKey := record.ToUid + "," + record.ToAddress
						tokenDecimalsMap, ok := addressTokenMap[toKey]
						if !ok {
							tokenDecimalsMap = make(map[string]int)
							addressTokenMap[toKey] = tokenDecimalsMap
						}
						tokenDecimalsMap[tokenAddress] = int(decimals)
					}
				}
			}
		}

		if record.FromAddress != "" && record.FromUid != "" {
			if _, ok := addressUidMap[record.FromAddress]; !ok {
				if platInfo, ok := biz.GetChainPlatInfo(chainName); ok {
					mainDecimals = platInfo.Decimal
					mainSymbol = platInfo.NativeCurrency
				} else {
					continue
				}
				addressUidMap[record.FromAddress] = record.FromUid
			}
		}
	}

	for address, uid := range addressUidMap {
		var err error
		userAssetKey := chainName + address
		if userAsset, ok := userAssetMap[userAssetKey]; !ok {
			err = retryWithAlarm10(chainName, func() error {
				var err error
				userAsset, err = doHandleUserAsset(chainName, uid, address, "", mainDecimals, mainSymbol, now)
				return err

			}, zap.Any("address", address), zap.String("stage", "doHandleUserAsset"))
			if err != nil {
				return
			}
			if userAsset != nil {
				userAssetMap[userAssetKey] = userAsset
			}
		}
	}

	for key, tokenDecimalsMap := range addressTokenMap {
		uidAddress := strings.Split(key, ",")
		var userAssetsList []*data.UserAsset
		err := retryWithAlarm10(
			chainName,
			func() error {
				var err error
				userAssetsList, err = doHandleUserTokenAsset(chainName, uidAddress[0], uidAddress[1], tokenDecimalsMap, tokenInfoMap, now)
				return err
			},
			zap.String("scope", "doHandlerUserTokenAsset"),
			zap.Any("address", uidAddress[1]),
			zap.Any("tokenAddress", tokenDecimalsMap),
		)
		if err != nil {
			return
		}
		for _, userAsset := range userAssetsList {
			if userAsset != nil {
				userAssetKey := userAsset.ChainName + userAsset.Address + userAsset.TokenAddress
				userAssetMap[userAssetKey] = userAsset
			}
		}
	}

	if len(userAssetMap) == 0 {
		return
	}
	for _, userAsset := range userAssetMap {
		uidType, _ := biz.GetUidTypeCode(userAsset.Address)
		userAsset.UidType = uidType
		userAssets = append(userAssets, userAsset)
	}

	//更新用户资产成本价
	_ = biz.UpdateAssetCostPrice(nil, userAssets)

	_, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户资产，将数据插入到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户资产，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserAsset(chainName string, uid string, address string,
	tokenAddress string, decimals int32, symbol string, nowTime int64) (*data.UserAsset, error) {
	if address == "" || uid == "" {
		return nil, nil
	}

	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		return client.GetBalance(address)
	})
	if err != nil {
		log.Error("query balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
		return nil, err
	}
	balance := result.(string)

	var userAsset = &data.UserAsset{
		ChainName:     chainName,
		Uid:           uid,
		Address:       address,
		TokenAddress:  tokenAddress,
		Balance:       balance,
		Decimals:      decimals,
		Symbol:        symbol,
		IsSyncToChain: true,
		SyncToChainTs: nowTime,
		CreatedAt:     nowTime,
		UpdatedAt:     nowTime,
	}
	return userAsset, nil
}

func doHandleUserTokenAsset(chainName string, uid string, address string,
	tokenDecimalsMap map[string]int, tokenInfoMap map[string]*types.TokenInfo, nowTime int64) ([]*data.UserAsset, error) {
	var userAssets []*data.UserAsset

	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		if chainName == "Ronin" || chainName == "Linea" {
			return client.NewBatchTokenBalance(address, tokenDecimalsMap)
		} else {
			return client.BatchTokenBalance(address, tokenDecimalsMap)
		}
	})
	if err != nil {
		log.Error("query token balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenInfo", tokenDecimalsMap), zap.Any("error", err))
		return nil, err
	}
	balanceList := result.(map[string]interface{})

	for tokenAddress, balancei := range balanceList {
		balance := fmt.Sprintf("%v", balancei)
		tokenInfo := tokenInfoMap[tokenAddress]

		var userAsset = &data.UserAsset{
			ChainName:     chainName,
			Uid:           uid,
			Address:       address,
			TokenAddress:  tokenAddress,
			TokenUri:      tokenInfo.TokenUri,
			Balance:       balance,
			Decimals:      int32(tokenInfo.Decimals),
			Symbol:        tokenInfo.Symbol,
			IsSyncToChain: true,
			SyncToChainTs: nowTime,
			CreatedAt:     nowTime,
			UpdatedAt:     nowTime,
		}
		userAssets = append(userAssets, userAsset)
	}
	return userAssets, nil
}

func HandleUserStatistic(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserStatistic error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserStatistic panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易金额失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var userAssetStatisticList []biz.UserAssetStatistic
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		var tokenAddress = record.ContractAddress
		if strings.HasPrefix(chainName, "Polygon") && record.ContractAddress == POLYGON_CODE {
			tokenAddress = ""
		}
		if strings.HasPrefix(chainName, "zkSync") && record.ContractAddress == ZKSYNC_CODE {
			tokenAddress = ""
		}

		decimals, _, err := biz.GetDecimalsSymbolFromTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 统计交易记录出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易金额，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("统计交易金额，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}

		var userAssetStatistic = biz.UserAssetStatistic{
			ChainName:    chainName,
			FromUid:      record.FromUid,
			ToUid:        record.ToUid,
			Amount:       record.Amount,
			TokenAddress: tokenAddress,
			Decimals:     decimals,
		}
		userAssetStatisticList = append(userAssetStatisticList, userAssetStatistic)
	}
	biz.HandleUserAssetStatistic(chainName, userAssetStatisticList)
}

func HandleTransactionCount(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleTransactionCount error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleTransactionCount panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链统计交易次数失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var transactionInfoList []biz.TransactionInfo
	for _, record := range txRecords {
		if record.TransactionType != biz.NATIVE && record.TransactionType != biz.TRANSFER && record.TransactionType != biz.TRANSFERNFT &&
			record.TransactionType != biz.CONTRACT && record.TransactionType != biz.SWAP && record.TransactionType != biz.MINT && record.TransactionType != biz.ADDLIQUIDITY {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}
		if record.FromAddress == "" || record.ToAddress == "" || (record.FromUid == "" && record.ToUid == "") {
			continue
		}

		transactionType := record.TransactionType
		if transactionType == biz.CONTRACT {
			tx, err := data.EvmTransactionRecordRepoClient.SelectColumnByTxHash(nil, biz.GetTableName(chainName), record.TransactionHash, []string{"transaction_type"})
			if err == nil && tx != nil && (tx.TransactionType == biz.MINT || tx.TransactionType == biz.SWAP || tx.TransactionType == biz.ADDLIQUIDITY) {
				transactionType = tx.TransactionType
			}
		}

		var transactionInfo = biz.TransactionInfo{
			ChainName:       chainName,
			FromAddress:     record.FromAddress,
			ToAddress:       record.ToAddress,
			TransactionType: transactionType,
			TransactionHash: record.TransactionHash,
		}
		transactionInfoList = append(transactionInfoList, transactionInfo)
	}
	biz.HandleTransactionCount(chainName, transactionInfoList)
}

func HandleTokenPush(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleTokenPush error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleTokenPush panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链推送token信息失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var userAssetList []biz.UserTokenPush
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链推送token信息，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("推送token信息，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType == biz.ERC721 || tokenType == biz.ERC1155 {
			continue
		}
		decimals := tokenInfo.Decimals
		symbol := tokenInfo.Symbol

		tokenAddress := record.ContractAddress
		address := record.ToAddress
		uid := record.ToUid
		if !(strings.HasPrefix(chainName, "Polygon") && tokenAddress == POLYGON_CODE) &&
			!(strings.HasPrefix(chainName, "zkSync") && tokenAddress == ZKSYNC_CODE) &&
			tokenAddress != "" && address != "" && uid != "" {
			var userAsset = biz.UserTokenPush{
				ChainName:    chainName,
				Uid:          uid,
				Address:      address,
				TokenAddress: tokenAddress,
				Decimals:     int32(decimals),
				Symbol:       symbol,
			}
			userAssetList = append(userAssetList, userAsset)
		}
	}
	biz.HandleTokenPush(chainName, userAssetList)
}

func HandleNftRecord(chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleNftRecord error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleNftRecord panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS && record.Status != biz.FAIL {
			continue
		}

		tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链添加NFT流转记录，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("添加NFT流转记录，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType != biz.ERC721 && tokenType != biz.ERC1155 {
			continue
		}
		if !((record.FromAddress != "" && record.FromUid != "") || (record.ToAddress != "" && record.ToUid != "")) {
			continue
		}

		tokenId := tokenInfo.TokenId
		tokenAddress := tokenInfo.Address
		log.Info("添加NFT流转记录", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
			zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
		//if !GetETHNftHistoryByBlockspan(chainName, tokenAddress, tokenId) {
		//	if !GetETHNftHistoryByNftgo(chainName, tokenAddress, tokenId, client) {
		//		GetETHNftHistoryByOpenSea(chainName, tokenAddress, tokenId)
		//	}
		//}
	}
}

func HandleUserNftAsset(isPending bool, chainName string, client Client, txRecords []*data.EvmTransactionRecord) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleUserNftAsset error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleUserNftAsset panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	now := time.Now().Unix()
	var userAssets []*data.UserNftAsset
	userAssetMap := make(map[string]*data.UserNftAsset)
	addressTokenMap := make(map[string]map[string]map[string]*v1.GetNftReply_NftInfoResp)
	for _, record := range txRecords {
		if record.TransactionType == biz.CONTRACT || record.TransactionType == biz.SWAP || record.TransactionType == biz.MINT ||
			record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
			continue
		}
		if record.Status != biz.SUCCESS {
			continue
		}

		tokenInfo, err := biz.ConvertGetTokenInfo(chainName, record.TokenInfo)
		if err != nil {
			// 更新用户资产出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产，解析tokenInfo失败", chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("更新用户NFT资产，解析tokenInfo失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenInfo", record.TokenInfo), zap.Any("error", err))
			continue
		}
		tokenType := tokenInfo.TokenType
		if tokenType != biz.ERC721 && tokenType != biz.ERC1155 {
			continue
		}
		if !((record.FromAddress != "" && record.FromUid != "") || (record.ToAddress != "" && record.ToUid != "")) {
			continue
		}

		tokenId := tokenInfo.TokenId
		tokenAddress := tokenInfo.Address
		if tokenAddress == "" || tokenId == "" {
			continue
		}
		nftInfo, err := biz.GetRawNftInfoDirectlyRetryAlert(nil, chainName, tokenAddress, tokenId)
		if err != nil {
			log.Error("更新用户NFT资产，从nodeProxy中获取NFT信息失败", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			//continue
		}
		if nftInfo == nil {
			log.Error("更新用户NFT资产，从nodeProxy中获取NFT信息为空", zap.Any("chainName", chainName), zap.Any("blockNumber", record.BlockNumber), zap.Any("txHash", record.TransactionHash),
				zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId))
			//continue
			nftInfo = &v1.GetNftReply_NftInfoResp{
				TokenAddress:   tokenAddress,
				TokenId:        tokenId,
				CollectionName: tokenInfo.CollectionName,
				NftName:        tokenInfo.ItemName,
				TokenType:      tokenType,
			}
		}
		if nftInfo.TokenType == "" {
			nftInfo.TokenType = tokenType
		}

		if !isPending && tokenInfo.TokenType == biz.ERC721 {
			if record.FromAddress != "" && record.FromUid != "" {
				payload, _ := utils.JsonEncode(map[string]interface{}{"collectionDescription": nftInfo.CollectionDescription,
					"description": nftInfo.Description, "rarity": nftInfo.Rarity, "properties": nftInfo.Properties})
				var userAsset = &data.UserNftAsset{
					ChainName:        chainName,
					Uid:              record.FromUid,
					Address:          record.FromAddress,
					TokenAddress:     tokenAddress,
					TokenUri:         nftInfo.CollectionImageURL,
					TokenId:          tokenId,
					Balance:          "0",
					TokenType:        tokenType,
					CollectionName:   nftInfo.CollectionName,
					Symbol:           nftInfo.Symbol,
					Name:             nftInfo.Name,
					ItemName:         nftInfo.NftName,
					ItemUri:          nftInfo.ImageURL,
					ItemOriginalUri:  nftInfo.ImageOriginalURL,
					ItemAnimationUri: nftInfo.AnimationURL,
					Data:             payload,
					CreatedAt:        now,
					UpdatedAt:        now,
				}
				userAssetKey := chainName + record.FromAddress + tokenAddress + tokenId
				userAssetMap[userAssetKey] = userAsset
			}

			if record.ToAddress != "" && record.ToUid != "" {
				payload, _ := utils.JsonEncode(map[string]interface{}{"collectionDescription": nftInfo.CollectionDescription,
					"description": nftInfo.Description, "rarity": nftInfo.Rarity, "properties": nftInfo.Properties})
				var userAsset = &data.UserNftAsset{
					ChainName:        chainName,
					Uid:              record.ToUid,
					Address:          record.ToAddress,
					TokenAddress:     tokenAddress,
					TokenUri:         nftInfo.CollectionImageURL,
					TokenId:          tokenId,
					Balance:          "1",
					TokenType:        tokenType,
					CollectionName:   nftInfo.CollectionName,
					Symbol:           nftInfo.Symbol,
					Name:             nftInfo.Name,
					ItemName:         nftInfo.NftName,
					ItemUri:          nftInfo.ImageURL,
					ItemOriginalUri:  nftInfo.ImageOriginalURL,
					ItemAnimationUri: nftInfo.AnimationURL,
					Data:             payload,
					CreatedAt:        now,
					UpdatedAt:        now,
				}
				userAssetKey := chainName + record.ToAddress + tokenAddress + tokenId
				userAssetMap[userAssetKey] = userAsset
			}
		} else {
			if record.FromAddress != "" && record.FromUid != "" {
				fromKey := record.FromUid + "," + record.FromAddress
				tokenAddressIdMap, ok := addressTokenMap[fromKey]
				if !ok {
					tokenAddressIdMap = make(map[string]map[string]*v1.GetNftReply_NftInfoResp)
					addressTokenMap[fromKey] = tokenAddressIdMap
				}
				tokenIdMap, ok := tokenAddressIdMap[tokenAddress]
				if !ok {
					tokenIdMap = make(map[string]*v1.GetNftReply_NftInfoResp)
					tokenAddressIdMap[tokenAddress] = tokenIdMap
				}
				tokenIdMap[tokenId] = nftInfo
			}

			if record.ToAddress != "" && record.ToUid != "" {
				toKey := record.ToUid + "," + record.ToAddress
				tokenAddressIdMap, ok := addressTokenMap[toKey]
				if !ok {
					tokenAddressIdMap = make(map[string]map[string]*v1.GetNftReply_NftInfoResp)
					addressTokenMap[toKey] = tokenAddressIdMap
				}
				tokenIdMap, ok := tokenAddressIdMap[tokenAddress]
				if !ok {
					tokenIdMap = make(map[string]*v1.GetNftReply_NftInfoResp)
					tokenAddressIdMap[tokenAddress] = tokenIdMap
				}
				tokenIdMap[tokenId] = nftInfo
			}
		}
	}

	for key, tokenAddressIdMap := range addressTokenMap {
		uidAddress := strings.Split(key, ",")
		var userAssetsList []*data.UserNftAsset
		err := retryWithAlarm10(chainName, func() error {
			var err error
			userAssetsList, err = doHandleUserNftAsset(chainName, uidAddress[0], uidAddress[1], tokenAddressIdMap, now)
			return err
		}, zap.Any("address", uidAddress[1]), zap.Any("tokenAddress", tokenAddressIdMap), zap.String("stage", "doHandleUserNftAsset"))
		if err != nil {
			return
		}
		for _, userAsset := range userAssetsList {
			if userAsset != nil {
				userAssetKey := userAsset.ChainName + userAsset.Address + userAsset.TokenAddress + userAsset.TokenId
				userAssetMap[userAssetKey] = userAsset
			}
		}
	}

	if len(userAssetMap) == 0 {
		return
	}
	for _, userAsset := range userAssetMap {
		userAssets = append(userAssets, userAsset)
	}
	_, err := data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(nil, userAssets, biz.PAGE_SIZE)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链更新用户NFT资产，将数据插入到数据库中失败", chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("更新用户NFT资产，将数据插入到数据库中失败", zap.Any("chainName", chainName), zap.Any("blockNumber", txRecords[0].BlockNumber), zap.Any("error", err))
	}
}

func doHandleUserNftAsset(chainName string, uid string, address string,
	tokenAddressIdMap map[string]map[string]*v1.GetNftReply_NftInfoResp, nowTime int64) ([]*data.UserNftAsset, error) {
	var userAssets []*data.UserNftAsset
	for tokenAddress, tokenIdMap := range tokenAddressIdMap {
		for tokenId, nftInfo := range tokenIdMap {
			result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
				var balance string
				var err error
				if nftInfo.TokenType == biz.ERC721 {
					balance, err = client.Erc721Balance(address, tokenAddress, tokenId)
				} else if nftInfo.TokenType == biz.ERC1155 {
					balance, err = client.Erc1155Balance(address, tokenAddress, tokenId)
				} else {
					balance = "0"
					err = errors.New("chain " + chainName + ", tokenType " + nftInfo.TokenType + " is not support")
				}
				if err != nil { //如果报错内容为：execution reverted，一般是参数错误，例如应该调Erc721Balance方法却调用了Erc1155Balance。
					err = errors.New(err.Error() + ", nodeUrl:" + client.URL())
				}
				return balance, err
			})
			if err != nil {
				log.Error("query nft balance error", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("tokenAddress", tokenAddressIdMap), zap.Any("error", err))
				return nil, err
			}
			balance := result.(string)

			payload, _ := utils.JsonEncode(map[string]interface{}{"collectionDescription": nftInfo.CollectionDescription,
				"description": nftInfo.Description, "rarity": nftInfo.Rarity, "properties": nftInfo.Properties})
			var userAsset = &data.UserNftAsset{
				ChainName:        chainName,
				Uid:              uid,
				Address:          address,
				TokenAddress:     tokenAddress,
				TokenUri:         nftInfo.CollectionImageURL,
				TokenId:          tokenId,
				Balance:          balance,
				TokenType:        nftInfo.TokenType,
				CollectionName:   nftInfo.CollectionName,
				Symbol:           nftInfo.Symbol,
				Name:             nftInfo.Name,
				ItemName:         nftInfo.NftName,
				ItemUri:          nftInfo.ImageURL,
				ItemOriginalUri:  nftInfo.ImageOriginalURL,
				ItemAnimationUri: nftInfo.AnimationURL,
				Data:             payload,
				CreatedAt:        nowTime,
				UpdatedAt:        nowTime,
			}
			userAssets = append(userAssets, userAsset)
		}
	}
	return userAssets, nil
}

func ExecuteRetry(chainName string, fc func(client Client) (interface{}, error)) (interface{}, error) {
	result, err := biz.ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		c, _ := client.(*Client)
		return fc(*c)
	})
	return result, err
}

func HandleMarketCoinHistory(chainName string, txRecords []*data.EvmTransactionRecord) {
	//获取 币价
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("HandleMarketCoinHistory error, chainName:"+chainName, e)
			} else {
				log.Errore("HandleMarketCoinHistory panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理币价失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	tm := time.Now()
	var dt = utils.GetDayTime(&tm)

	//拿出精度，还有symbol 还有 getpricekey

	for _, record := range txRecords {
		if record.Status != biz.SUCCESS {
			continue
		}
		fromUid := record.FromUid
		toUid := record.ToUid
		fromAddress := record.FromAddress
		toAddress := record.ToAddress
		switch record.TransactionType {
		case biz.TRANSFER:
			//解析parse_data 拿出 代币.
			//计算主币的余额，因为有手续费的花费
			if fromUid != "" {
				HandlerNativePriceHistory(chainName, fromAddress, fromUid, dt, true, record.FeeAmount, decimal.Zero)
				HandlerTokenPriceHistory(chainName, fromAddress, record.TokenInfo, fromUid, dt, true)
			}
			if toUid != "" {
				HandlerTokenPriceHistory(chainName, toAddress, record.TokenInfo, toUid, dt, false)
			}

		case biz.APPROVE, biz.APPROVENFT, biz.CREATECONTRACT, biz.CREATEACCOUNT, biz.TRANSFERNFT, biz.CONTRACT, biz.CLOSEACCOUNT, biz.REGISTERTOKEN, biz.DIRECTTRANSFERNFTSWITCH, biz.SETAPPROVALFORALL, biz.SAFETRANSFERFROM, biz.SAFEBATCHTRANSFERFROM:
			//主币计算
			if fromUid != "" {
				HandlerNativePriceHistory(chainName, fromAddress, fromUid, dt, true, record.FeeAmount, decimal.Zero)
			}
		case biz.EVENTLOG:
			//  解析parse_data 拿出 代币
			if fromUid != "" {
				HandlerTokenPriceHistory(chainName, fromAddress, record.TokenInfo, fromUid, dt, true)
			}
			if toUid != "" {
				HandlerTokenPriceHistory(chainName, toAddress, record.TokenInfo, toUid, dt, false)
			}
		case biz.NATIVE:
			// 主币 + 手续费
			if fromUid != "" {
				HandlerNativePriceHistory(chainName, fromAddress, fromUid, dt, true, record.FeeAmount, record.Amount)
			}
			if toUid != "" {
				HandlerNativePriceHistory(chainName, toAddress, toUid, dt, false, record.FeeAmount, record.Amount)
			}
		}
	}

	//该交易获取后。处理完需要
	//查询资产时对比 userAssert 里面的资产查询出所有代
}

func HandlerTokenPriceHistory(chainName, address, tokenInfoStr, uid string, dt int64, fromFlag bool) {
	now := time.Now().Unix()
	tokenInfo, _ := biz.ConvertGetTokenInfo(chainName, tokenInfoStr)
	tokenSymbolMap := make(map[string]int)
	tokenSymbolMap[tokenInfo.Address] = int(tokenInfo.Decimals)

	tma, _ := biz.GetPriceFromTokenAddress([]string{tokenInfo.Address})
	if tma == nil {
		return
	}
	var cnyTokenPrice, usdTokenPrice string
	if len(tma.Tokens) == 1 {
		cnyTokenPrice = strconv.FormatFloat(tma.Tokens[0].Price.Cny, 'f', 2, 64)
		usdTokenPrice = strconv.FormatFloat(tma.Tokens[0].Price.Usd, 'f', 2, 64)
	}
	cnyTokenPriceDecimal, _ := decimal.NewFromString(cnyTokenPrice)
	usdTokenPriceDecimal, _ := decimal.NewFromString(usdTokenPrice)

	tokenResult, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		return client.BatchTokenBalance(address, tokenSymbolMap)
	})
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询%s余额失败", chainName, address)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("查询余额失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
	}
	balanceList := tokenResult.(map[string]interface{})
	tokenBalance := fmt.Sprintf("%v", balanceList[tokenInfo.Address])
	tokenBalanceDecimal, _ := decimal.NewFromString(tokenBalance)

	tokenAmount, _ := decimal.NewFromString(tokenInfo.Amount)
	var tas string
	if fromFlag {
		tas = utils.StringDecimals(tokenAmount.Neg().String(), int(tokenInfo.Decimals))
	} else {
		tas = utils.StringDecimals(tokenAmount.String(), int(tokenInfo.Decimals))
	}
	tokenAmountDecimal, _ := decimal.NewFromString(tas)
	cnyTokenAmount := tokenAmountDecimal.Mul(cnyTokenPriceDecimal)
	usdTokenAmount := tokenAmountDecimal.Mul(usdTokenPriceDecimal)

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
			Balance:             tokenBalance, //当前余额 带小数点
			CreatedAt:           now,
			UpdatedAt:           now,
			TransactionBalance:  txb,
		}
		data.MarketCoinHistoryRepoClient.Save(nil, msh)
	} else if len(fmcs) == 1 {
		marketCoinHistory := fmcs[0]
		marketCoinHistory.TransactionQuantity = marketCoinHistory.TransactionQuantity + 1
		oldBalance, _ := decimal.NewFromString(marketCoinHistory.Balance)

		marketCoinHistory.Balance = tokenBalance

		oldTransactionBalance, _ := decimal.NewFromString(marketCoinHistory.TransactionBalance)
		newTransactionBalance, _ := decimal.NewFromString(txb)
		marketCoinHistory.TransactionBalance = oldTransactionBalance.Add(newTransactionBalance).String()

		if oldBalance.Cmp(decimal.Zero) == 0 {
			marketCoinHistory.CnyPrice = cnyTokenPrice
			marketCoinHistory.UsdPrice = usdTokenPrice
			marketCoinHistory.CnyAmount = cnyTokenAmount
			marketCoinHistory.UsdAmount = usdTokenAmount
		} else {
			marketCoinHistory.CnyAmount = marketCoinHistory.CnyAmount.Add(cnyTokenAmount)
			marketCoinHistory.UsdAmount = marketCoinHistory.UsdAmount.Add(usdTokenAmount)
			if tokenBalanceDecimal.Cmp(decimal.Zero) != 0 {
				marketCoinHistory.CnyPrice = marketCoinHistory.CnyAmount.DivRound(tokenBalanceDecimal, 2).String()
				marketCoinHistory.UsdPrice = marketCoinHistory.UsdAmount.DivRound(tokenBalanceDecimal, 2).String()
			}
		}

		marketCoinHistory.UpdatedAt = now
		data.MarketCoinHistoryRepoClient.Update(nil, marketCoinHistory)
	}
}

func HandlerNativePriceHistory(chainName, address, uid string, dt int64, fromFlag bool, feeAmount, amount decimal.Decimal) {
	now := time.Now().Unix()
	var cnyPrice, usdPrice string
	platInfo, _ := biz.GetChainPlatInfo(chainName)
	decimals := int(platInfo.Decimal)
	symbol := platInfo.NativeCurrency
	getPriceKey := platInfo.GetPriceKey

	tokenMarket, _ := biz.GetPriceFromChain([]string{getPriceKey})
	if tokenMarket == nil {
		return
	}
	cs := tokenMarket.Coins
	if len(cs) > 0 {
		for _, c := range cs {
			if c.CoinID == getPriceKey {
				if c.Price != nil {
					cnyPrice = strconv.FormatFloat(c.Price.Cny, 'f', 2, 64)
					usdPrice = strconv.FormatFloat(c.Price.Usd, 'f', 2, 64)
				}
				break
			}
		}
	}

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
	cnyFee := totalNum.Mul(cnyPriceDecimal)
	usdFee := totalNum.Mul(usdPriceDecimal)

	result, err := ExecuteRetry(chainName, func(client Client) (interface{}, error) {
		return client.GetBalance(address)
	})
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询%s余额失败", chainName, address)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("查询余额失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
	}
	//主币余额，处理过精度，结果为带小数点
	balance := result.(string)
	balanceDecimal, _ := decimal.NewFromString(balance)

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
			Balance:             balance, //当前余额 带小数点
			CreatedAt:           now,
			UpdatedAt:           now,
			TransactionBalance:  totalNum.Abs().String(),
		}

		r, e := data.MarketCoinHistoryRepoClient.Save(nil, msh)
		log.Info("what native", zap.Any("chainName", chainName), zap.Any("msh", msh), zap.Any("r", r), zap.Error(e))
	} else if len(mcs) == 1 {
		marketCoinHistory := mcs[0]
		marketCoinHistory.TransactionQuantity = marketCoinHistory.TransactionQuantity + 1
		oldBalance, _ := decimal.NewFromString(marketCoinHistory.Balance)
		marketCoinHistory.Balance = balance

		oldTransactionBalance, _ := decimal.NewFromString(marketCoinHistory.TransactionBalance)
		newTransactionBalance, _ := decimal.NewFromString(totalNum.Abs().String())
		marketCoinHistory.TransactionBalance = oldTransactionBalance.Add(newTransactionBalance).String()

		if oldBalance.Cmp(decimal.Zero) == 0 {
			marketCoinHistory.CnyPrice = cnyPrice
			marketCoinHistory.UsdPrice = usdPrice
			marketCoinHistory.CnyAmount = cnyFee
			marketCoinHistory.UsdAmount = usdFee
		} else {
			marketCoinHistory.CnyAmount = marketCoinHistory.CnyAmount.Add(cnyFee)
			marketCoinHistory.UsdAmount = marketCoinHistory.UsdAmount.Add(usdFee)
			if balanceDecimal.Cmp(decimal.Zero) != 0 {
				marketCoinHistory.CnyPrice = marketCoinHistory.CnyAmount.DivRound(balanceDecimal, 2).String()
				marketCoinHistory.UsdPrice = marketCoinHistory.UsdAmount.DivRound(balanceDecimal, 2).String()
			}
		}
		marketCoinHistory.UpdatedAt = now

		data.MarketCoinHistoryRepoClient.Update(nil, marketCoinHistory)
	}
}

func retryWithAlarm10(chainName string, fn func() error, fields ...zap.Field) error {
	return pcommon.RetryWithAlarm10(chainName, fn, "更新用户资产", fields...)
}
