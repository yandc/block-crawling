package biz

import (
	v12 "block-crawling/api/transaction/v1"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"github.com/go-redis/redis"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"time"
)

const (
	WRITE_BENFEN_RECHARGE       = "benfen:recharge:"
	WRITE_RECHARGE_EVM_TYPE     = "benfenRechargeAddress"
	WRITE_RECHARGE_BENFEN_TYPE  = "benfenRechargeTxHash"
	RECHARGE_SOURCE_TX_HASH     = "benfen:recharge:sourceTxHash:"
	RECHARGE_BENFEN_TX_HASH     = "benfen:recharge:benfenTxHash:"
	TOPIC_RECHARGE_ADDRESS_TYPE = "source_charge"
	TOPIC_RECHARGE_SWAP_TYPE    = "bridge_swap"
	TOPIC_RECHARGE_TX_TYPE      = "benfen_charge"
)

type WriteRedisReq struct {
	WriteType       string `json:"write_type"`
	Chain           string `json:"chain"`
	RechargeAddress string `json:"recharge_address"`
	SourceTxHash    string `json:"source_tx_hash"`
	BenfenTxHash    string `json:"benfen_tx_hash"`
}

type CreateRecordFromRechargeReq struct {
	OrderId string `json:"order_id"`
	*v12.TransactionReq
}

type BenfenRechargeAddress struct {
	TxHash    string          `json:"tx_hash"`
	TxTime    int64           `json:"tx_time"`
	ToAddress string          `json:"to_address"`
	TokenInfo string          `json:"tokenInfo"`
	Chain     string          `json:"chain"`
	Amount    decimal.Decimal `json:"amount"`
}

type BenfenSwapSource struct {
	SourceTxHash string          `json:"source_tx_hash"`
	OrderId      string          `json:"order_id"`
	Status       string          `json:"status"`
	TxTime       int64           `json:"tx_time"`
	FeeAmount    decimal.Decimal `json:"fee_amount"`
}

type BenfenRechargeTx struct {
	SourceTxHash string          `json:"source_tx_hash"`
	BenfenTxHash string          `json:"benfen_tx_hash"`
	TxTime       int64           `json:"tx_time"`
	Status       string          `json:"status"`
	FeeAmount    decimal.Decimal `json:"fee_amount"`
	Amount       decimal.Decimal `json:"amount"`
	TokenInfo    string          `json:"tokenInfo"`
}

type BenfenRechargeMq struct {
	RechargeType          string `json:"recharge_type"`
	BenfenRechargeAddress `json:"benfen_recharge_address"`
	BenfenSwapSource      `json:"benfen_swap_source"`
	BenfenRechargeTx      `json:"benfen_recharge_tx"`
}

func BenfenRechargePushMq(rechargeMQ BenfenRechargeMq, chainName string) {
        if AppConfig.Cmq.Endpoint.BenfenTopicURL == "" {
                return
        }
	msg, _ := utils.JsonEncode(rechargeMQ)
	log.Info("BenfenRechargePushMq:", zap.Any("type", rechargeMQ.RechargeType), zap.Any("info", msg))
	PushTopicCMQ(chainName, AppConfig.Cmq.Topic.BenfenCharge.Id, msg, AppConfig.Cmq.Endpoint.BenfenTopicURL)
}

// WriteRedisByType write redis is success
func WriteRedisByType(req *WriteRedisReq) (bool, error) {
	switch req.WriteType {
	case WRITE_RECHARGE_EVM_TYPE:
		if err := data.RedisClient.Set(WRITE_BENFEN_RECHARGE+req.Chain+":"+req.RechargeAddress, req.RechargeAddress, -1).Err(); err != nil {
			log.Error("WriteRedisByType benfenRechargeAddress error:%s", zap.Error(err))
			return false, err
		}
		return true, nil
	case WRITE_RECHARGE_BENFEN_TYPE:
		if req.BenfenTxHash == "" {
			log.Error("WriteRedisByType benfenRechargeTxHash error txhash is nil")
			return false, nil
		}
		if err := data.RedisClient.Set(RECHARGE_BENFEN_TX_HASH+req.BenfenTxHash, req.SourceTxHash, -1).Err(); err != nil {
			log.Error("WriteRedisByType benfenRechargeTxHash error:%s", zap.Error(err))
			return false, err
		}
		if req.Chain == "" {
			req.Chain = "BenfenTEST"
		}
		log.Info("WriteRedisByType get benfen record start")
		//直接获取benfen链交易记录
		record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(context.Background(), GetTableName(req.Chain), req.BenfenTxHash)
		if err != nil {
			log.Error("WriteRedisByType get benfen tx info error:%s", zap.Error(err))
			return false, err
		}
		if record == nil {
			log.Error("WriteRedisByType benfen record is nil")
			return false, nil
		}
		amount := record.Amount
		var tokenInfo string
		if record.TransactionType != "native" && record.EventLog != "" {
			eventRecord, _ := data.SuiTransactionRecordRepoClient.FindByTxHash(context.Background(), GetTableName(req.Chain), req.BenfenTxHash+"#result-1")
			amount = eventRecord.Amount
			tokenInfo = eventRecord.TokenInfo
		}
		log.Info("WriteRedisByType get benfen record end")
		//#result-1
		HandlerRechargeBenfenTx(req.BenfenTxHash, record.TxTime, req.Chain, record.Status, req.SourceTxHash, tokenInfo,
			record.FeeAmount, amount)
		return true, nil
	}
	return true, nil
}

// HandleBenfenRechargeAddress 处理benfen充值地址
func HandleBenfenRechargeAddress(rechargeAddressInfo BenfenRechargeAddress) {
	log.Info("HandleBenfenRechargeAddress start")
	toFlag, toErr := GetBenfenRechargeByAddress(rechargeAddressInfo.Chain, rechargeAddressInfo.ToAddress)
	if toErr != nil {
		return
	}
	if toFlag {
		log.Info("HandleBenfenRechargeAddress start true")
		rechargeMQ := BenfenRechargeMq{
			RechargeType:          TOPIC_RECHARGE_ADDRESS_TYPE,
			BenfenRechargeAddress: rechargeAddressInfo,
		}
		BenfenRechargePushMq(rechargeMQ, rechargeAddressInfo.Chain)
	}
}

//HandlerRechargeSourceTxHash 处理benfen充值跨链交易
func HandlerRechargeSourceTxHash(txHash, status, chainName string, txTime int64, feeAmount decimal.Decimal) {
	log.Info("HandlerRechargeSourceTxHash start ")
	//判断txHash是否是充值订单
	orderId, err := GetOrderIdByTxHash(txHash)
	if err != nil || orderId == "" {
		return
	}
	//处理源交易和订单ID的
	HandleRechargeOrderId(orderId, txHash, status, chainName, txTime, feeAmount)
}

// GetBenfenRechargeByAddress 是否是benfen充值地址
func GetBenfenRechargeByAddress(chainName, address string) (bool, error) {
	if address == "" {
		return false, nil
	}
	key := WRITE_BENFEN_RECHARGE + chainName + ":" + address
	rechargeAddress, err := data.RedisClient.Get(key).Result()
	for i := 0; i < 3 && err != nil && err != redis.Nil; i++ {
		time.Sleep(time.Duration(i) * time.Second)
		rechargeAddress, err = data.RedisClient.Get(key).Result()
	}
	return rechargeAddress != "", err
}

func GetOrderIdByTxHash(txHash string) (string, error) {
	if txHash == "" {
		return "", nil
	}
	redisKey := RECHARGE_SOURCE_TX_HASH + txHash
	orderId, err := data.RedisClient.Get(redisKey).Result()
	for i := 0; i < 3 && err != nil && err != redis.Nil; i++ {
		time.Sleep(time.Duration(i) * time.Second)
		orderId, err = data.RedisClient.Get(redisKey).Result()
	}
	return orderId, err
}

func HandleRechargeOrderId(orderId, sourceTxHash, status, chainName string, txTime int64, feeAmount decimal.Decimal) {
	rechargeOrderSource := BenfenSwapSource{
		OrderId:      orderId,
		SourceTxHash: sourceTxHash,
		Status:       status,
		TxTime:       txTime,
		FeeAmount:    feeAmount,
	}
	log.Info("HandleRechargeOrderId start:", zap.Any("status", status), zap.Any("txHash", sourceTxHash))
	redisKey := RECHARGE_SOURCE_TX_HASH + sourceTxHash
	if status == PENDING {
		//写入redis
		if err := data.RedisClient.Set(RECHARGE_SOURCE_TX_HASH+sourceTxHash, orderId, -1).Err(); err != nil {
			log.Error("HandleRechargeOrderId set error:%s", zap.Error(err))
		}
	} else if status == SUCCESS {
		//已完成，删除记录
		if err := data.RedisClient.Del(redisKey).Err(); err != nil {
			log.Error("HandleRechargeOrderId del error:%s", zap.Error(err))
		}
		log.Info("HandleRechargeOrderId delete:", zap.Any("status", status), zap.Any("txHash", sourceTxHash))
	}
	rechargeMQ := BenfenRechargeMq{
		RechargeType:     TOPIC_RECHARGE_SWAP_TYPE,
		BenfenSwapSource: rechargeOrderSource,
	}
	BenfenRechargePushMq(rechargeMQ, chainName)
}

func GetSourceHashByTxHash(txHash string) (string, error) {
	if txHash == "" {
		return "", nil
	}
	redisKey := RECHARGE_BENFEN_TX_HASH + txHash
	log.Info("GetSourceHashByTxHash redis:", zap.Any("key=", redisKey))
	sourceTxHash, err := data.RedisClient.Get(redisKey).Result()
	for i := 0; i < 3 && err != nil && err != redis.Nil; i++ {
		time.Sleep(time.Duration(i) * time.Second)
		sourceTxHash, err = data.RedisClient.Get(redisKey).Result()
	}
	return sourceTxHash, err
}

// HandlerRechargeBenfenTx 处理benfen链充值benfen交易
func HandlerRechargeBenfenTx(txHash string, txTime int64, chainName, status, sourceTxHash, tokenInfo string, feeAmount, amount decimal.Decimal) {
	log.Info("HandlerRechargeBenfenTx start true:", zap.Any("status", status), zap.Any("txHash=", txHash))
	rechargeBenfenSource := BenfenRechargeTx{
		SourceTxHash: sourceTxHash,
		BenfenTxHash: txHash,
		TxTime:       txTime,
		Status:       status,
		FeeAmount:    feeAmount,
		TokenInfo:    tokenInfo,
		Amount:       amount,
	}
	rechargeMQ := BenfenRechargeMq{
		RechargeType:     TOPIC_RECHARGE_TX_TYPE,
		BenfenRechargeTx: rechargeBenfenSource,
	}
	BenfenRechargePushMq(rechargeMQ, chainName)
	//已完成，删除记录
	if err := data.RedisClient.Del(RECHARGE_BENFEN_TX_HASH + txHash).Err(); err != nil {
		log.Error("HandlerRechargeBenfenTx del error:%s", zap.Error(err))
	}
}
