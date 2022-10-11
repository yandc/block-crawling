package sui

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type Platform struct {
	subhandle.CommPlatform
	client    Client
	CoinIndex uint
}

const SUI_CODE = "0x2::sui::SUI"
const TYPE_PREFIX = "0x2::coin::Coin"
const NATIVE_TYPE = "0x2::coin::Coin<0x2::sui::SUI>"

func Init(handler string, value *conf.PlatInfo, nodeURL []string, height int) *Platform {
	chain := value.Handler
	chainName := value.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		client:    NewClient(nodeURL[0]),
		CommPlatform: subhandle.CommPlatform{
			Height:         height,
			Chain:          chain,
			ChainName:      chainName,
			HeightAlarmThr: int(value.GetMonitorHeightAlarmThr()),
		},
	}
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetTransactions() {
	log.Info("GetTransactions starting, chainName:" + p.ChainName)
	for true {
		p.DoGetTransactions()
		time.Sleep(time.Duration(p.Coin().LiveInterval) * time.Millisecond)
	}
}

func (p *Platform) DoGetTransactions() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactions error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("GetTransactions panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链爬块失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	//获取最新块高
	height, err := p.client.GetTransactionNumber()
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		height, err = p.client.GetTransactionNumber()
	}
	if err != nil || height <= 0 {
		log.Error(p.ChainName+"扫块，从链上获取最新块高失败", zap.Any("new", height), zap.Any("error", err))
		return
	}

	//获取本地当前块高
	oldHeight := -1
	redisHeight, err := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		redisHeight, err = data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	}
	if err != nil {
		if fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
			// 从数据库中查询最新一条数据
			lastRecord, err := data.SuiTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				lastRecord, err = data.SuiTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
			}
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中块高失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(p.ChainName+"扫块，从数据库中获取块高失败", zap.Any("old", oldHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
			if lastRecord == nil {
				oldHeight = height - 1
				log.Error(p.ChainName+"扫块，从数据库中获取的块高为空", zap.Any("old", oldHeight), zap.Any("new", height))
				//return
			} else {
				oldHeight = lastRecord.TransactionVersion
			}
		} else {
			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询redis中块高失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，从redis中获取块高失败", zap.Any("old", oldHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}
	} else {
		if redisHeight != "" {
			oldHeight, err = strconv.Atoi(redisHeight)
			if err != nil {
				log.Error(p.ChainName+"扫块，从redis获取的块高不合法", zap.Any("old", oldHeight), zap.Any("new", height))
				return
			}
		} else {
			log.Error(p.ChainName+"扫块，从redis获取的块高为空", zap.Any("old", oldHeight), zap.Any("new", height))
			return
		}
	}
	log.Info(p.ChainName+"扫块，高度为", zap.Any("old", oldHeight), zap.Any("new", height))
	if oldHeight >= height {
		return
	}

	curHeight := oldHeight
	for curHeight < height {
		curHeight++

		block, err := p.client.GetTransactionByNumber(curHeight)
		for i := 0; i < 10 && err != nil && fmt.Sprintf("%s", err) != "not found"; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetTransactionByNumber(curHeight)
		}
		if err != nil {
			if fmt.Sprintf("%s", err) == "not found" {
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight, 0)
				log.Warn(p.ChainName+"扫块，从链上获取区块信息为空", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				continue
			} else {
				log.Error(p.ChainName+"扫块，从链上获取区块信息失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
		}

		var txRecords []*data.SuiTransactionRecord
		now := time.Now().Unix()

		index := 0
		for _, tx := range block.Certificate.Data.Transactions {
			var status string
			if block.Effects.Status.Status == "success" {
				status = biz.SUCCESS
			} else {
				status = biz.FAIL
			}

			if tx.TransferSui != nil || tx.TransferObject != nil {
				txType := biz.NATIVE
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				if tx.TransferSui != nil {
					fromAddress = block.Certificate.Data.Sender
					toAddress = tx.TransferSui.Recipient
					amount = strconv.Itoa(tx.TransferSui.Amount)
				} else {
					var inEvent *Event
					var eventAmount int
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.TransferObject
						if moveEvent != nil {
							event := &Event{
								FromAddress: moveEvent.Sender,
								ToAddress:   moveEvent.Recipient.AddressOwner,
								ObjectId:    moveEvent.ObjectId,
								Amount:      strconv.Itoa(moveEvent.Amount),
							}
							eventAmount += moveEvent.Amount
							if inEvent == nil {
								inEvent = event
							} else {
								inEvent.Amount = strconv.Itoa(eventAmount)
							}
						}
					}
					if inEvent != nil {
						objectId := inEvent.ObjectId
						object, err := p.client.GetObject(objectId)
						for i := 0; i < 10 && err != nil && fmt.Sprintf("%s", err) != "Deleted"; i++ {
							time.Sleep(time.Duration(i*5) * time.Second)
							object, err = p.client.GetObject(objectId)
						}
						if err != nil {
							if fmt.Sprintf("%s", err) == "Deleted" {
								log.Error(p.ChainName+"扫块，从链上获取object信息被删除", zap.Any("objectId", objectId), zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								//continue
							} else {
								log.Error(p.ChainName+"扫块，从链上获取object信息失败", zap.Any("objectId", objectId), zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								return
							}
						}

						fromAddress = inEvent.FromAddress
						toAddress = inEvent.ToAddress
						amount = inEvent.Amount
						if object != nil {
							dataType := object.Details.Data.Type
							if dataType != NATIVE_TYPE {
								if strings.HasPrefix(dataType, TYPE_PREFIX) {
									contractAddress = dataType[len(TYPE_PREFIX)+1 : len(dataType)-1]
								} else {
									contractAddress = dataType
								}
								txType = biz.TRANSFER
							}
						}
					} else {
						fromAddress = block.Certificate.Data.Sender
						toAddress = tx.TransferObject.Recipient
					}
				}

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
						return
					}
				}

				if fromAddressExist || toAddressExist {
					txHash := block.Certificate.TransactionDigest
					txTime := now
					gasLimit := strconv.Itoa(block.Certificate.Data.GasBudget)
					computationCost := block.Effects.GasUsed.ComputationCost
					storageCost := block.Effects.GasUsed.StorageCost
					storageRebate := block.Effects.GasUsed.StorageRebate
					gasUsedi := computationCost + storageCost - storageRebate
					gasUsed := strconv.Itoa(gasUsedi)
					feeAmount := decimal.NewFromInt(int64(gasUsedi))

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 && txType == biz.TRANSFER {
						var decimals int64 = 0
						getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
						} else if getTokenInfo.Symbol != "" {
							decimals = getTokenInfo.Decimals
						}
						tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
					}
					suiMap := map[string]interface{}{
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(suiMap)
					amountValue, _ := decimal.NewFromString(amount)

					suiTransactionRecord := &data.SuiTransactionRecord{
						TransactionVersion: curHeight,
						TransactionHash:    txHash,
						FromAddress:        fromAddress,
						ToAddress:          toAddress,
						FromUid:            fromUid,
						ToUid:              toUid,
						FeeAmount:          feeAmount,
						Amount:             amountValue,
						Status:             status,
						TxTime:             txTime,
						ContractAddress:    contractAddress,
						ParseData:          parseData,
						GasLimit:           gasLimit,
						GasUsed:            gasUsed,
						Data:               "",
						EventLog:           "",
						TransactionType:    txType,
						DappData:           "",
						ClientData:         "",
						CreatedAt:          now,
						UpdatedAt:          now,
					}
					txRecords = append(txRecords, suiTransactionRecord)
					if tx.TransferObject != nil {
						break
					}
				}
			} else if tx.Call != nil {
				flag := false
				var txTime int64
				var gasLimit string
				var gasUsed string
				var feeAmount decimal.Decimal
				var payload string
				var eventLogs []types.EventLog
				var suiContractRecord *data.SuiTransactionRecord

				txType := biz.CONTRACT
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.Call.Package.ObjectId

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
						return
					}
				}
				if fromAddress == toAddress {
					continue
				}

				if fromAddressExist || toAddressExist {
					index++
					var txHash string
					if index == 1 {
						txHash = block.Certificate.TransactionDigest
					} else {
						txHash = block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)
					}
					txTime = now
					gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
					computationCost := block.Effects.GasUsed.ComputationCost
					storageCost := block.Effects.GasUsed.StorageCost
					storageRebate := block.Effects.GasUsed.StorageRebate
					gasUsedi := computationCost + storageCost - storageRebate
					gasUsed = strconv.Itoa(gasUsedi)
					feeAmount = decimal.NewFromInt(int64(gasUsedi))

					flag = true

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
						var decimals int64 = 0
						getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
						} else if getTokenInfo.Symbol != "" {
							decimals = getTokenInfo.Decimals
						}
						tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
					}
					suiMap := map[string]interface{}{
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(suiMap)
					amountValue, _ := decimal.NewFromString(amount)

					suiContractRecord = &data.SuiTransactionRecord{
						TransactionVersion: curHeight,
						TransactionHash:    txHash,
						FromAddress:        fromAddress,
						ToAddress:          toAddress,
						FromUid:            fromUid,
						ToUid:              toUid,
						FeeAmount:          feeAmount,
						Amount:             amountValue,
						Status:             status,
						TxTime:             txTime,
						ContractAddress:    contractAddress,
						ParseData:          parseData,
						GasLimit:           gasLimit,
						GasUsed:            gasUsed,
						Data:               payload,
						EventLog:           "",
						TransactionType:    txType,
						DappData:           "",
						ClientData:         "",
						CreatedAt:          now,
						UpdatedAt:          now,
					}
					txRecords = append(txRecords, suiContractRecord)
				}

				txType = biz.EVENTLOG
				//index := 0

				var events []Event
				if tx.Call.Function == "swap_x_to_y" {
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.MoveEvent
						if moveEvent != nil {
							inEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: tx.Call.TypeArguments[0],
								Amount:       strconv.Itoa(moveEvent.Fields.InAmount),
							}
							events = append(events, inEvent)

							outEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: tx.Call.TypeArguments[1],
								Amount:       strconv.Itoa(moveEvent.Fields.OutAmount),
							}
							events = append(events, outEvent)
						}
					}
				} else if tx.Call.Function == "add_liquidity" {
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.MoveEvent
						if moveEvent != nil {
							xEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: tx.Call.TypeArguments[0],
								Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
							}
							events = append(events, xEvent)

							yEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: tx.Call.TypeArguments[1],
								Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
							}
							events = append(events, yEvent)

							lspEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
								Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
							}
							events = append(events, lspEvent)
						}
					}
				} else if tx.Call.Function == "remove_liquidity" {
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.MoveEvent
						if moveEvent != nil {
							xEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: tx.Call.TypeArguments[0],
								Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
							}
							events = append(events, xEvent)

							yEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: tx.Call.TypeArguments[1],
								Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
							}
							events = append(events, yEvent)

							lspEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
								Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
							}
							events = append(events, lspEvent)
						}
					}
				}

				for _, event := range events {
					var tokenInfo types.TokenInfo
					var amount, contractAddress string
					var fromAddress, toAddress, fromUid, toUid string
					var fromAddressExist, toAddressExist bool

					amount = event.Amount
					fromAddress = event.FromAddress
					toAddress = event.ToAddress
					contractAddress = event.TokenAddress

					if fromAddress == toAddress {
						continue
					}

					if fromAddress != "" {
						fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
						if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
							return
						}
					}

					if toAddress != "" {
						toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
						if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
							return
						}
					}

					if fromAddressExist || toAddressExist {
						index++
						txHash := block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)

						if !flag {
							txTime = now
							gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
							computationCost := block.Effects.GasUsed.ComputationCost
							storageCost := block.Effects.GasUsed.StorageCost
							storageRebate := block.Effects.GasUsed.StorageRebate
							gasUsedi := computationCost + storageCost - storageRebate
							gasUsed = strconv.Itoa(gasUsedi)
							feeAmount = decimal.NewFromInt(int64(gasUsedi))

							flag = true
						}

						contractAddressSplit := strings.Split(contractAddress, "::")
						if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
							var decimals int64 = 0
							getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
							for i := 0; i < 3 && err != nil; i++ {
								time.Sleep(time.Duration(i*1) * time.Second)
								getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
							}
							if err != nil {
								// nodeProxy出错 接入lark报警
								alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
							} else if getTokenInfo.Symbol != "" {
								decimals = getTokenInfo.Decimals
							}
							tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
						}
						suiMap := map[string]interface{}{
							"token": tokenInfo,
						}
						parseData, _ := utils.JsonEncode(suiMap)
						amountValue, _ := decimal.NewFromString(amount)
						eventLogInfo := types.EventLog{
							From:   fromAddress,
							To:     toAddress,
							Amount: amountValue.BigInt(),
							Token:  tokenInfo,
						}
						eventLogs = append(eventLogs, eventLogInfo)
						eventLog, _ := utils.JsonEncode(eventLogInfo)

						suiTransactionRecord := &data.SuiTransactionRecord{
							TransactionVersion: curHeight,
							TransactionHash:    txHash,
							FromAddress:        fromAddress,
							ToAddress:          toAddress,
							FromUid:            fromUid,
							ToUid:              toUid,
							FeeAmount:          feeAmount,
							Amount:             amountValue,
							Status:             status,
							TxTime:             txTime,
							ContractAddress:    contractAddress,
							ParseData:          parseData,
							GasLimit:           gasLimit,
							GasUsed:            gasUsed,
							Data:               payload,
							EventLog:           eventLog,
							TransactionType:    txType,
							DappData:           "",
							ClientData:         "",
							CreatedAt:          now,
							UpdatedAt:          now,
						}
						txRecords = append(txRecords, suiTransactionRecord)
					}
				}

				if suiContractRecord != nil && eventLogs != nil {
					eventLog, _ := utils.JsonEncode(eventLogs)
					suiContractRecord.EventLog = eventLog
				}
			}
		}

		if txRecords != nil && len(txRecords) > 0 {
			//保存交易数据
			err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(p.ChainName+"扫块，将数据插入到数据库中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
			go HandleRecord(p.ChainName, p.client, txRecords)
		}

		//更新 redis中的块高
		result, err := data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight, 0).Result()
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			result, err = data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight, 0).Result()
		}
		if err != nil {
			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入块高到redis中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，将块高插入到redis中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}
		if result != "OK" {
			log.Error(p.ChainName+"扫块，将块高插入到redis中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("result", result))
		}
	}
}

func (p *Platform) GetTransactionResultByTxhash() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactionsResult error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("GetTransactionsResult panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易结果失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	records, err := data.SuiTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(p.ChainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(p.ChainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info("", zap.Any("records", records))
	var txRecords []*data.SuiTransactionRecord
	now := time.Now().Unix()

	for _, record := range records {
		block, err := p.client.GetTransactionByHash(record.TransactionHash)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetTransactionByHash(record.TransactionHash)
		}
		if err != nil {
			log.Error(p.ChainName+"查询链上数据失败", zap.Any("txHash", record.TransactionHash), zap.Any("error", err))
			continue
		}

		/*transactionVersion, _ := strconv.Atoi(block.Version)
		if transactionInfo.ErrorCode == "transaction_not_found" {
			status := biz.FAIL
			record.Status = status
			record.UpdatedAt = now
			txRecords = append(txRecords, record)
			continue
		} else if transactionInfo.Type == "pending_transaction" {
			continue
		}

		curHeight, _ := strconv.Atoi(block.BlockHeight)*/
		curHeight := 0

		index := 0
		for _, tx := range block.Certificate.Data.Transactions {
			var status string
			if block.Effects.Status.Status == "success" {
				status = biz.SUCCESS
			} else {
				status = biz.FAIL
			}

			if tx.TransferSui != nil || tx.TransferObject != nil {
				txType := biz.NATIVE
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				if tx.TransferSui != nil {
					fromAddress = block.Certificate.Data.Sender
					toAddress = tx.TransferSui.Recipient
					amount = strconv.Itoa(tx.TransferSui.Amount)
				} else {
					var inEvent *Event
					var eventAmount int
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.TransferObject
						if moveEvent != nil {
							event := &Event{
								FromAddress: moveEvent.Sender,
								ToAddress:   moveEvent.Recipient.AddressOwner,
								ObjectId:    moveEvent.ObjectId,
								Amount:      strconv.Itoa(moveEvent.Amount),
							}
							eventAmount += moveEvent.Amount
							if inEvent == nil {
								inEvent = event
							} else {
								inEvent.Amount = strconv.Itoa(eventAmount)
							}
						}
					}
					if inEvent != nil {
						objectId := inEvent.ObjectId
						object, err := p.client.GetObject(objectId)
						for i := 0; i < 10 && err != nil && fmt.Sprintf("%s", err) != "Deleted"; i++ {
							time.Sleep(time.Duration(i*5) * time.Second)
							object, err = p.client.GetObject(objectId)
						}
						if err != nil {
							if fmt.Sprintf("%s", err) == "Deleted" {
								log.Error(p.ChainName+"扫块，从链上获取object信息被删除", zap.Any("objectId", objectId), zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
								//continue
							} else {
								log.Error(p.ChainName+"扫块，从链上获取object信息失败", zap.Any("objectId", objectId), zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
								return
							}
						}

						fromAddress = inEvent.FromAddress
						toAddress = inEvent.ToAddress
						amount = inEvent.Amount
						if object != nil {
							dataType := object.Details.Data.Type
							if dataType != NATIVE_TYPE {
								if strings.HasPrefix(dataType, TYPE_PREFIX) {
									contractAddress = dataType[len(TYPE_PREFIX)+1 : len(dataType)-1]
								} else {
									contractAddress = dataType
								}
								txType = biz.TRANSFER
							}
						}
					} else {
						fromAddress = block.Certificate.Data.Sender
						toAddress = tx.TransferObject.Recipient
					}
				}

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						return
					}
				}

				if fromAddressExist || toAddressExist {
					txHash := block.Certificate.TransactionDigest
					txTime := now
					gasLimit := strconv.Itoa(block.Certificate.Data.GasBudget)
					computationCost := block.Effects.GasUsed.ComputationCost
					storageCost := block.Effects.GasUsed.StorageCost
					storageRebate := block.Effects.GasUsed.StorageRebate
					gasUsedi := computationCost + storageCost - storageRebate
					gasUsed := strconv.Itoa(gasUsedi)
					feeAmount := decimal.NewFromInt(int64(gasUsedi))

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 && txType == biz.TRANSFER {
						var decimals int64 = 0
						getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						} else if getTokenInfo.Symbol != "" {
							decimals = getTokenInfo.Decimals
						}
						tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
					}
					suiMap := map[string]interface{}{
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(suiMap)
					amountValue, _ := decimal.NewFromString(amount)

					suiTransactionRecord := &data.SuiTransactionRecord{
						TransactionVersion: curHeight,
						TransactionHash:    txHash,
						FromAddress:        fromAddress,
						ToAddress:          toAddress,
						FromUid:            fromUid,
						ToUid:              toUid,
						FeeAmount:          feeAmount,
						Amount:             amountValue,
						Status:             status,
						TxTime:             txTime,
						ContractAddress:    contractAddress,
						ParseData:          parseData,
						GasLimit:           gasLimit,
						GasUsed:            gasUsed,
						Data:               "",
						EventLog:           "",
						TransactionType:    txType,
						DappData:           "",
						ClientData:         "",
						CreatedAt:          now,
						UpdatedAt:          now,
					}
					txRecords = append(txRecords, suiTransactionRecord)
					if tx.TransferObject != nil {
						break
					}
				}
			} else if tx.Call != nil {
				flag := false
				var txTime int64
				var gasLimit string
				var gasUsed string
				var feeAmount decimal.Decimal
				var payload string
				var eventLogs []types.EventLog
				var suiContractRecord *data.SuiTransactionRecord

				txType := biz.CONTRACT
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.Call.Package.ObjectId

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						return
					}
				}
				if fromAddress == toAddress {
					continue
				}

				if fromAddressExist || toAddressExist {
					index++
					var txHash string
					if index == 1 {
						txHash = block.Certificate.TransactionDigest
					} else {
						txHash = block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)
					}
					txTime = now
					gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
					computationCost := block.Effects.GasUsed.ComputationCost
					storageCost := block.Effects.GasUsed.StorageCost
					storageRebate := block.Effects.GasUsed.StorageRebate
					gasUsedi := computationCost + storageCost - storageRebate
					gasUsed = strconv.Itoa(gasUsedi)
					feeAmount = decimal.NewFromInt(int64(gasUsedi))

					flag = true

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
						var decimals int64 = 0
						getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						} else if getTokenInfo.Symbol != "" {
							decimals = getTokenInfo.Decimals
						}
						tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
					}
					suiMap := map[string]interface{}{
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(suiMap)
					amountValue, _ := decimal.NewFromString(amount)

					suiContractRecord = &data.SuiTransactionRecord{
						TransactionVersion: curHeight,
						TransactionHash:    txHash,
						FromAddress:        fromAddress,
						ToAddress:          toAddress,
						FromUid:            fromUid,
						ToUid:              toUid,
						FeeAmount:          feeAmount,
						Amount:             amountValue,
						Status:             status,
						TxTime:             txTime,
						ContractAddress:    contractAddress,
						ParseData:          parseData,
						GasLimit:           gasLimit,
						GasUsed:            gasUsed,
						Data:               payload,
						EventLog:           "",
						TransactionType:    txType,
						DappData:           "",
						ClientData:         "",
						CreatedAt:          now,
						UpdatedAt:          now,
					}
					txRecords = append(txRecords, suiContractRecord)
				}

				txType = biz.EVENTLOG
				//index := 0

				var events []Event
				if tx.Call.Function == "swap_x_to_y" {
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.MoveEvent
						if moveEvent != nil {
							inEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: tx.Call.TypeArguments[0],
								Amount:       strconv.Itoa(moveEvent.Fields.InAmount),
							}
							events = append(events, inEvent)

							outEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: tx.Call.TypeArguments[1],
								Amount:       strconv.Itoa(moveEvent.Fields.OutAmount),
							}
							events = append(events, outEvent)
						}
					}
				} else if tx.Call.Function == "add_liquidity" {
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.MoveEvent
						if moveEvent != nil {
							xEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: tx.Call.TypeArguments[0],
								Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
							}
							events = append(events, xEvent)

							yEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: tx.Call.TypeArguments[1],
								Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
							}
							events = append(events, yEvent)

							lspEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
								Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
							}
							events = append(events, lspEvent)
						}
					}
				} else if tx.Call.Function == "remove_liquidity" {
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						moveEvent := txEvent.MoveEvent
						if moveEvent != nil {
							xEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: tx.Call.TypeArguments[0],
								Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
							}
							events = append(events, xEvent)

							yEvent := Event{
								FromAddress:  moveEvent.Fields.PoolId,
								ToAddress:    moveEvent.Sender,
								TokenAddress: tx.Call.TypeArguments[1],
								Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
							}
							events = append(events, yEvent)

							lspEvent := Event{
								FromAddress:  moveEvent.Sender,
								ToAddress:    moveEvent.Fields.PoolId,
								TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
								Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
							}
							events = append(events, lspEvent)
						}
					}
				}

				for _, event := range events {
					var tokenInfo types.TokenInfo
					var amount, contractAddress string
					var fromAddress, toAddress, fromUid, toUid string
					var fromAddressExist, toAddressExist bool

					amount = event.Amount
					fromAddress = event.FromAddress
					toAddress = event.ToAddress
					contractAddress = event.TokenAddress

					if fromAddress == toAddress {
						continue
					}

					if fromAddress != "" {
						fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
						if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
							return
						}
					}

					if toAddress != "" {
						toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
						if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
							return
						}
					}

					if fromAddressExist || toAddressExist {
						index++
						txHash := block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)

						if !flag {
							txTime = now
							gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
							computationCost := block.Effects.GasUsed.ComputationCost
							storageCost := block.Effects.GasUsed.StorageCost
							storageRebate := block.Effects.GasUsed.StorageRebate
							gasUsedi := computationCost + storageCost - storageRebate
							gasUsed = strconv.Itoa(gasUsedi)
							feeAmount = decimal.NewFromInt(int64(gasUsedi))

							flag = true
						}

						contractAddressSplit := strings.Split(contractAddress, "::")
						if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
							var decimals int64 = 0
							getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
							for i := 0; i < 3 && err != nil; i++ {
								time.Sleep(time.Duration(i*1) * time.Second)
								getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
							}
							if err != nil {
								// nodeProxy出错 接入lark报警
								alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
							} else if getTokenInfo.Symbol != "" {
								decimals = getTokenInfo.Decimals
							}
							tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
						}
						suiMap := map[string]interface{}{
							"token": tokenInfo,
						}
						parseData, _ := utils.JsonEncode(suiMap)
						amountValue, _ := decimal.NewFromString(amount)
						eventLogInfo := types.EventLog{
							From:   fromAddress,
							To:     toAddress,
							Amount: amountValue.BigInt(),
							Token:  tokenInfo,
						}
						eventLogs = append(eventLogs, eventLogInfo)
						eventLog, _ := utils.JsonEncode(eventLogInfo)

						suiTransactionRecord := &data.SuiTransactionRecord{
							TransactionVersion: curHeight,
							TransactionHash:    txHash,
							FromAddress:        fromAddress,
							ToAddress:          toAddress,
							FromUid:            fromUid,
							ToUid:              toUid,
							FeeAmount:          feeAmount,
							Amount:             amountValue,
							Status:             status,
							TxTime:             txTime,
							ContractAddress:    contractAddress,
							ParseData:          parseData,
							GasLimit:           gasLimit,
							GasUsed:            gasUsed,
							Data:               payload,
							EventLog:           eventLog,
							TransactionType:    txType,
							DappData:           "",
							ClientData:         "",
							CreatedAt:          now,
							UpdatedAt:          now,
						}
						txRecords = append(txRecords, suiTransactionRecord)
					}
				}

				if suiContractRecord != nil && eventLogs != nil {
					eventLog, _ := utils.JsonEncode(eventLogs)
					suiContractRecord.EventLog = eventLog
				}
			}
		}
	}

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，将数据插入到数据库中失败" /*, zap.Any("current", curHeight), zap.Any("new", height)*/, zap.Any("error", err))
			return
		}
	}
}

func BatchSaveOrUpdate(txRecords []*data.SuiTransactionRecord, tableName string) error {
	total := len(txRecords)
	pageSize := biz.PAGE_SIZE
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTxRecords := txRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		_, err := data.SuiTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.SuiTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
