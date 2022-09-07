package tron

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const ADDRESS_PREFIX = "41"
const TRC10TYPE = "TransferAssetContract"

type Platform struct {
	subhandle.CommPlatform
	client    Client
	CoinIndex uint
}

func Init(handler, chain, chainName string, nodeUrl []string, height int) *Platform {
	return &Platform{
		CoinIndex:    coins.HandleMap[handler],
		client:       NewClient(nodeUrl[0]),
		CommPlatform: subhandle.CommPlatform{Height: height, Chain: chain, ChainName: chainName},
	}
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetTransactions() {
	log.Info("GetTransactions starting, chainName:" + p.ChainName)
	for true {
		h := p.IndexBlock()
		if h {
			time.Sleep(time.Duration(p.Coin().LiveInterval) * time.Millisecond)
		}
	}
}

func (p *Platform) IndexBlock() bool {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("IndexBlock error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("IndexBlock panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链爬块失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	height, err := p.client.GetBlockHeight()
	if err != nil || height <= 0 {
		return true
	}

	curHeight := -1
	preDBBlockHash := make(map[int]string)
	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	if redisHeight != "" {
		curHeight, _ = strconv.Atoi(redisHeight)
	} else {
		lastRecord, err := data.TrxTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			lastRecord, err = data.TrxTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
		}
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中块高失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，从数据库中获取块高失败", zap.Any("curHeight", curHeight), zap.Any("new", height), zap.Any("error", err))
			return true
		}
		if lastRecord == nil {
			curHeight = height
			log.Error(p.ChainName+"扫块，从数据库中获取的块高为空", zap.Any("curHeight", curHeight), zap.Any("new", height))
			//return true
		} else {
			curHeight = lastRecord.BlockNumber + 1
			preDBBlockHash[lastRecord.BlockNumber] = lastRecord.BlockHash
		}
	}
	if curHeight > height {
		return true
	}

	if curHeight <= height {
		block, err := p.client.GetBlockByNum(curHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetBlockByNum(curHeight)
		}
		if err != nil {
			log.Warn("请注意：未查到块高数据", zap.Any("chain", p.ChainName), zap.Any("height", curHeight))
			return true
		}
		if block != nil {
			forked := false
			preHeight := curHeight - 1
			preHash := block.BlockHeader.RawData.ParentHash
			curPreBlockHash, _ := data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()

			if curPreBlockHash == "" {
				bh := preDBBlockHash[preHeight]
				if bh != "" {
					curPreBlockHash = bh
				}
			}
			for curPreBlockHash != "" && curPreBlockHash != preHash {
				forked = true
				pBlock, _ := p.client.GetBlockByNum(preHeight)
				preHash = pBlock.BlockHeader.RawData.ParentHash
				preHeight = preHeight - 1
				curPreBlockHash, _ = data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
			}
			if forked {
				curHeight = preHeight
				rows, _ := data.EvmTransactionRecordRepoClient.DeleteByBlockNumber(nil, biz.GetTalbeName(p.ChainName), preHeight+1)
				log.Info("出现分叉回滚数据", zap.Any("链类型", p.ChainName), zap.Any("共删除数据", rows), zap.Any("回滚到块高", preHeight))
			} else {
				var txRecords []*data.TrxTransactionRecord
				now := time.Now().Unix()
				for _, tx := range block.Transactions {
					if len(tx.RawData.Contract) > 0 {
						if tx.RawData.Contract[0].Type == TRC10TYPE || tx.RawData.Contract[0].Parameter.Value.AssetName != "" {
							continue
						}
						txType := "native"
						status := "pending"
						if len(tx.Ret) > 0 {
							if tx.Ret[0].ContractRet == "SUCCESS" {
								status = "success"
							} else {
								status = "fail"
							}
						}
						value := tx.RawData.Contract[0].Parameter.Value
						fromAddress := value.OwnerAddress
						var toAddress, contractAddress string
						tokenAmount := "0"
						var amount int
						if value.ContractAddress != "" && len(value.Data) >= 136 {
							methodId := value.Data[:8]
							if methodId == "a9059cbb" {
								txType = "transfer"
								contractAddress = value.ContractAddress
								toAddress = utils.TronHexToBase58(ADDRESS_PREFIX + value.Data[32:72])
								banInt, b := new(big.Int).SetString(value.Data[72:], 16)
								if b {
									tokenAmount = banInt.String()
								}
							}
						} else {
							toAddress = value.ToAddress
							amount = value.Amount
						}
						userFromAddress, fromUid, errr := biz.UserAddressSwitch(fromAddress)
						if errr != nil {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							return true
						}
						userToAddress, toUid, errt := biz.UserAddressSwitch(toAddress)
						if errt != nil {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							return true
						}
						if userFromAddress || userToAddress {
							var tokenInfo types.TokenInfo
							txInfo, err := p.client.GetTransactionInfoByHash(tx.TxID)
							for i := 0; i < 10 && err != nil; i++ {
								time.Sleep(time.Duration(i*5) * time.Second)
								txInfo, err = p.client.GetTransactionInfoByHash(tx.TxID)
							}
							if err != nil {
								return true
							}

							if txType == "transfer" {
								tokenInfo, _ = GetTokenInfo(p.ChainName, contractAddress)
								tokenInfo.Amount = tokenAmount
							}
							feeData := map[string]interface{}{
								"net_usage": txInfo.Receipt.NetUsage,
							}
							if contractAddress != "" {
								feeData["fee_limit"] = tx.RawData.FeeLimit
							}
							if txInfo.Receipt.EnergyUsage > 0 {
								feeData["energy_usage"] = txInfo.Receipt.EnergyUsage
							}
							feeAmount := 0
							if txInfo.Fee > 0 {
								feeAmount = txInfo.Fee
							}
							if txInfo.Receipt.NetFee > 0 && feeAmount == 0 {
								feeAmount = txInfo.Receipt.NetFee
							}
							tronMap := map[string]interface{}{
								"tvm":   map[string]string{},
								"token": tokenInfo,
							}
							parseData, _ := json.Marshal(tronMap)
							exTime := tx.RawData.Timestamp / 1000
							if tx.RawData.Timestamp == 0 {
								exTime = block.BlockHeader.RawData.Timestamp / 1000
							}

							trxRecord := &data.TrxTransactionRecord{
								BlockHash:       block.BlockID,
								BlockNumber:     block.BlockHeader.RawData.Number,
								TransactionHash: tx.TxID,
								FromAddress:     fromAddress,
								ToAddress:       toAddress,
								FromUid:         fromUid,
								ToUid:           toUid,
								FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
								Amount:          decimal.NewFromInt(int64(amount)),
								Status:          status,
								TxTime:          exTime,
								ContractAddress: contractAddress,
								ParseData:       string(parseData),
								NetUsage:        strconv.Itoa(txInfo.Receipt.NetUsage),
								FeeLimit:        strconv.Itoa(tx.RawData.FeeLimit),
								EnergyUsage:     strconv.Itoa(txInfo.Receipt.EnergyUsage),
								TransactionType: txType,
								DappData:        "",
								ClientData:      "",
								CreatedAt:       now,
								UpdatedAt:       now,
							}
							txRecords = append(txRecords, trxRecord)
						}
					}
				}

				if txRecords != nil && len(txRecords) > 0 {
					e := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
					if e != nil {
						// postgres出错 接入lark报警
						log.Error("插入数据到数据库库中失败", zap.Any("current", curHeight), zap.Any("chain", p.ChainName))
						alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						return true
					}
					go HandleRecord(p.ChainName, p.client, txRecords)
				}
				//保存对应块高hash
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), block.BlockID, biz.BLOCK_HASH_EXPIRATION_KEY)
			}
		} else {
			return true
		}
	} else {
		return true
	}

	data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight+1, 0)
	return false
}

func GetTokenInfo(chainName, token string) (types.TokenInfo, error) {
	var url string
	if strings.Contains(chainName, "TEST") {
		url = "https://shastapi.tronscan.org/api/contract"
	} else {
		url = "https://apilist.tronscan.org/api/contract"
	}
	params := map[string]string{
		"contract": token,
	}
	out := &types.TronTokenInfo{}
	err := httpclient.HttpsForm(url, http.MethodGet, params, nil, out)
	if err != nil {
		return types.TokenInfo{}, err
	}
	if len(out.Data) == 0 {
		return types.TokenInfo{}, nil
	}
	return types.TokenInfo{
		Decimals: out.Data[0].TokenInfo.TokenDecimal,
		Symbol:   out.Data[0].TokenInfo.TokenName,
		Address:  token,
	}, nil
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

	records, err := data.TrxTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(p.ChainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(p.ChainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info("", zap.Any("records", records))
	var txRecords []*data.TrxTransactionRecord
	now := time.Now().Unix()

	for _, record := range records {
		transactionInfo, err := p.client.GetTransactionInfoByHash(record.TransactionHash)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			transactionInfo, err = p.client.GetTransactionInfoByHash(record.TransactionHash)
		}
		if err != nil {
			log.Error(p.ChainName+"查询链上数据失败", zap.Any("error", err))
			continue
		}

		isPending := len(transactionInfo.ContractResult) == 0
		if isPending {
			continue
		}

		result := transactionInfo.ContractResult[0]
		if result != "" && result != "SUCCESS" {
			record.Status = biz.FAIL
			txRecords = append(txRecords, record)
			continue
		}

		curHeight := transactionInfo.BlockNumber
		block, err := p.client.GetBlockByNum(curHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetBlockByNum(curHeight)
		}
		if err != nil {
			log.Error(p.ChainName+"扫块，从链上获取区块hash失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
			return
		}

		for _, tx := range block.Transactions {
			if len(tx.RawData.Contract) > 0 {
				if tx.TxID != record.TransactionHash {
					continue
				}

				if tx.RawData.Contract[0].Type == TRC10TYPE || tx.RawData.Contract[0].Parameter.Value.AssetName != "" {
					continue
				}
				txType := "native"
				status := "pending"
				if len(tx.Ret) > 0 {
					if tx.Ret[0].ContractRet == "SUCCESS" {
						status = "success"
					} else {
						status = "fail"
					}
				}
				value := tx.RawData.Contract[0].Parameter.Value
				fromAddress := value.OwnerAddress
				var toAddress, contractAddress string
				tokenAmount := "0"
				var amount int
				if value.ContractAddress != "" && len(value.Data) >= 136 {
					methodId := value.Data[:8]
					if methodId == "a9059cbb" {
						txType = "transfer"
						contractAddress = value.ContractAddress
						toAddress = utils.TronHexToBase58(ADDRESS_PREFIX + value.Data[32:72])
						banInt, b := new(big.Int).SetString(value.Data[72:], 16)
						if b {
							tokenAmount = banInt.String()
						}
					}
				} else {
					toAddress = value.ToAddress
					amount = value.Amount
				}
				userFromAddress, fromUid, errr := biz.UserAddressSwitch(fromAddress)
				if errr != nil {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					return
				}
				userToAddress, toUid, errt := biz.UserAddressSwitch(toAddress)
				if errt != nil {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					return
				}
				if userFromAddress || userToAddress {
					var tokenInfo types.TokenInfo
					txInfo, err := p.client.GetTransactionInfoByHash(tx.TxID)
					for i := 0; i < 10 && err != nil; i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						txInfo, err = p.client.GetTransactionInfoByHash(tx.TxID)
					}
					if err != nil {
						return
					}

					if txType == "transfer" {
						tokenInfo, _ = GetTokenInfo(p.ChainName, contractAddress)
						tokenInfo.Amount = tokenAmount
					}
					feeData := map[string]interface{}{
						"net_usage": txInfo.Receipt.NetUsage,
					}
					if contractAddress != "" {
						feeData["fee_limit"] = tx.RawData.FeeLimit
					}
					if txInfo.Receipt.EnergyUsage > 0 {
						feeData["energy_usage"] = txInfo.Receipt.EnergyUsage
					}
					feeAmount := 0
					if txInfo.Fee > 0 {
						feeAmount = txInfo.Fee
					}
					if txInfo.Receipt.NetFee > 0 && feeAmount == 0 {
						feeAmount = txInfo.Receipt.NetFee
					}
					tronMap := map[string]interface{}{
						"tvm":   map[string]string{},
						"token": tokenInfo,
					}
					parseData, _ := json.Marshal(tronMap)
					exTime := tx.RawData.Timestamp / 1000
					if tx.RawData.Timestamp == 0 {
						exTime = block.BlockHeader.RawData.Timestamp / 1000
					}

					trxRecord := &data.TrxTransactionRecord{
						BlockHash:       block.BlockID,
						BlockNumber:     block.BlockHeader.RawData.Number,
						TransactionHash: tx.TxID,
						FromAddress:     fromAddress,
						ToAddress:       toAddress,
						FromUid:         fromUid,
						ToUid:           toUid,
						FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
						Amount:          decimal.NewFromInt(int64(amount)),
						Status:          status,
						TxTime:          exTime,
						ContractAddress: contractAddress,
						ParseData:       string(parseData),
						NetUsage:        strconv.Itoa(txInfo.Receipt.NetUsage),
						FeeLimit:        strconv.Itoa(tx.RawData.FeeLimit),
						EnergyUsage:     strconv.Itoa(txInfo.Receipt.EnergyUsage),
						TransactionType: txType,
						DappData:        "",
						ClientData:      "",
						CreatedAt:       now,
						UpdatedAt:       now,
					}
					txRecords = append(txRecords, trxRecord)
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

func BatchSaveOrUpdate(txRecords []*data.TrxTransactionRecord, table string) error {
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

		_, err := data.TrxTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.TrxTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
