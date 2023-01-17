package common

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/log"
	"strings"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

// TxMeta meta of transactions.
type TxMeta struct {
	FromAddress     string
	Value           string
	ToAddress       string
	TransactionType string
	User            *UserMeta

	chainName string
}

// UserMeta match users.
type UserMeta struct {
	MatchFrom bool
	FromUid   string

	MatchTo bool
	ToUid   string
}

// AttemptMatchUser attempt to match user of tx.
func AttemptMatchUser(chainName string, tx *chain.Transaction) (*TxMeta, error) {
	meta := &TxMeta{
		FromAddress:     tx.FromAddress,
		Value:           tx.Value,
		ToAddress:       tx.ToAddress,
		TransactionType: string(tx.TxType),

		chainName: chainName,
	}
	user, err := meta.matchUser()
	if err != nil {
		return nil, err
	}
	meta.User = user
	tx.FromAddress = meta.FromAddress
	tx.ToAddress = meta.ToAddress
	return meta, nil
}

// WrapFields wrap log fields.
func (meta *TxMeta) WrapFields(results ...zap.Field) []zap.Field {
	results = append(
		results,
		zap.String("fromAddr", meta.FromAddress),
		zap.String("toAddr", meta.ToAddress),
		zap.String("txType", meta.TransactionType),
	)
	if meta.User != nil {
		results = append(
			results,
			zap.Bool("matchFrom", meta.User.MatchFrom),
			zap.Bool("matchTo", meta.User.MatchTo),
			zap.String("fromUid", meta.User.FromUid),
			zap.String("toUid", meta.User.ToUid),
		)
	}
	return results
}

func (meta *TxMeta) matchUser() (*UserMeta, error) {
	fromAddress := meta.FromAddress
	toAddress := meta.ToAddress
	chainName := meta.chainName

	var userFromAddress, userToAddress bool
	var fromUid, toUid string
	if fromAddress != "" {
		fromAddressList := strings.Split(fromAddress, ",")
		for i, addr := range fromAddressList {
			tmpUserFromAddress, tmpFromUid, err := biz.UserAddressSwitchRetryAlert(chainName, addr)
			if err != nil {
				log.Info("查询redis缓存报错：用户中心获取", zap.Any(chainName, fromAddress), zap.Any("error", err))
				return nil, err
			}
			if i == 0 {
				meta.FromAddress = addr
				fromUid = tmpFromUid
			}
			if tmpUserFromAddress {
				userFromAddress = tmpUserFromAddress
				break
			}
		}
	}
	if toAddress != "" {
		toAddressList := strings.Split(toAddress, ",")
		for i, addr := range toAddressList {
			tmpUserToAddress, tmpToUid, err := biz.UserAddressSwitchRetryAlert(chainName, addr)
			if err != nil {
				log.Info("查询redis缓存报错：用户中心获取", zap.Any(chainName, toAddress), zap.Any("error", err))
				return nil, err
			}
			if i == 0 {
				meta.ToAddress = addr
				toUid = tmpToUid
			}
			if tmpUserToAddress {
				userToAddress = tmpUserToAddress
				break
			}
		}
	}
	return &UserMeta{
		MatchFrom: userFromAddress,
		FromUid:   fromUid,

		MatchTo: userToAddress,
		ToUid:   toUid,
	}, nil
}

func MatchUser(fromAddress, toAddress, chainName string) (*UserMeta, error) {
	userFromAddress, fromUid, err := biz.UserAddressSwitchRetryAlert(chainName, fromAddress)
	if err != nil {
		log.Info("查询redis缓存报错：用户中心获取", zap.Any(chainName, fromAddress), zap.Any("error", err))
		return nil, err
	}
	userToAddress, toUid, err := biz.UserAddressSwitchRetryAlert(chainName, toAddress)
	if err != nil {
		log.Info("查询redis缓存报错：用户中心获取", zap.Any(chainName, toAddress), zap.Any("error", err))
		return nil, err
	}
	return &UserMeta{
		MatchFrom: userFromAddress,
		FromUid:   fromUid,

		MatchTo: userToAddress,
		ToUid:   toUid,
	}, nil
}

func MatchAddress(address, chainName string) (string, bool, error) {
	ok, uid, err := biz.UserAddressSwitchRetryAlert(chainName, address)
	if err != nil {
		log.Info("查询redis缓存报错：用户中心获取", zap.Any(chainName, address), zap.Any("error", err))
		return "", false, err
	}
	return uid, ok, nil
}
