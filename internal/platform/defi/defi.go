package defi

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type ContractAssetFlow struct {
	BlockNumber     int64
	PlatformID      int64
	PlatformEnabled bool
	TxTime          int64
	Address         string
	TransactionHash string
	ReceivedAssets  []Asset
	SentAssets      []Asset

	IdentifiedType         string
	IdentifiedTopicAddress string
}

type Asset struct {
	PeerAddress string
	Amount      decimal.Decimal
	Token       types.TokenInfo
	UsdPrice    decimal.Decimal
}

// AssetKey concatenate all asset keys in sorted order.
func (flow *ContractAssetFlow) AssetKey() string {
	tokenAddresses := make([]string, 0, len(flow.ReceivedAssets)+len(flow.SentAssets))
	for _, item := range flow.ReceivedAssets {
		tokenAddresses = append(tokenAddresses, item.Token.Address)
	}
	for _, item := range flow.SentAssets {
		tokenAddresses = append(tokenAddresses, item.Token.Address)
	}
	return GenerateAssetAddress(tokenAddresses)
}

func GenerateAssetAddress(tokenAddresses []string) string {
	sort.Strings(tokenAddresses)
	return strings.Join(tokenAddresses, "-")
}

func (flow *ContractAssetFlow) IsBorrowing() bool {
	return (len(flow.ReceivedAssets) == 1 && len(flow.SentAssets) == 0) || flow.mayStakedBeBorrowing()
}

func (flow *ContractAssetFlow) mayStakedBeBorrowing() bool {
	if flow.IdentifiedType == data.DeFiAssetTypeStake {
		return false
	}
	// 同时接收和发送了一个代币，但是发送的代币价格价格是 0，这种情况不处理会被当成 Staked
	return (len(flow.ReceivedAssets) == 1 && len(flow.SentAssets) == 1 && flow.SentAssets[0].UsdPrice.IsZero())
}

func (flow *ContractAssetFlow) IsDeposit() bool {
	return (len(flow.ReceivedAssets) == 0 && len(flow.SentAssets) == 1) || flow.mayStakedBeDeposit()
}

func (flow *ContractAssetFlow) mayStakedBeDeposit() bool {
	if flow.IdentifiedType == data.DeFiAssetTypeStake {
		return false
	}
	// 接收了一个代币，但是接收的代币价格价格是 0，这种情况不处理会被当成 Staked
	return (len(flow.ReceivedAssets) == 1 && len(flow.SentAssets) == 1 && flow.ReceivedAssets[0].UsdPrice.IsZero())
}

func (flow *ContractAssetFlow) IsStaked() bool {
	return len(flow.ReceivedAssets) == 1 && len(flow.SentAssets) == 1
}

func (flow *ContractAssetFlow) IsAddingLP() bool {
	return len(flow.ReceivedAssets) == 1 && len(flow.SentAssets) == 2
}

func (flow *ContractAssetFlow) IsRemovingLP() bool {
	return len(flow.ReceivedAssets) == 2 && len(flow.SentAssets) == 1
}

func (flow *ContractAssetFlow) Received() map[string]Asset {
	results := make(map[string]Asset)
	for _, recv := range flow.ReceivedAssets {
		results[recv.Token.Address] = recv
	}
	return results
}

func (flow *ContractAssetFlow) Sent() map[string]Asset {
	results := make(map[string]Asset)
	for _, sent := range flow.SentAssets {
		results[sent.Token.Address] = sent
	}
	return results
}

func (flow *ContractAssetFlow) fullfill() {
	switch flow.IdentifiedType {
	case data.DeFiAssetTypeStake:
		flow.fullfillStaked()
		log.Info("DEFI: FULLFILLED STAKED", zap.Any("flow", flow))
	case data.DeFiAssetTypeLP:
		flow.fullfillLP()
		log.Info("DEFI: FULLFILLED LP", zap.Any("flow", flow))
	}
}

var rewardedTokens = map[string]bool{
	"0xc5102fE9359FD9a28f877a67E36B0F050d81a3CC": true, // HOP
}

func isRewardedToken(tokenAddress string) bool {
	if _, ok := rewardedTokens[tokenAddress]; ok {
		return ok
	}
	return false
}

var placeholderAsset = Asset{
	PeerAddress: "",
	Amount:      decimal.Zero,
	Token: types.TokenInfo{
		Address: placeholderStakedToken,
		Amount:  decimal.Zero.String(),
	},
	UsdPrice: decimal.Zero,
}

func (flow *ContractAssetFlow) fullfillStaked() {
	if len(flow.ReceivedAssets) == 2 {
		// 去掉解质押返回额外的奖励。
		// https://polygonscan.com/tx/0x25f5b7fbed00522c64ead7db038f31a17fc1c180f7f980d1fb33e04ddf4a88d9
		var nonRewardAsset Asset
		var foundRewardAsset bool
		for _, v := range flow.ReceivedAssets {
			if isRewardedToken(v.Token.Address) {
				foundRewardAsset = true
			} else {
				nonRewardAsset = v
			}
		}
		if foundRewardAsset && nonRewardAsset.Amount.GreaterThan(decimal.Zero) {
			flow.ReceivedAssets = []Asset{nonRewardAsset}
		}
	}
	if len(flow.ReceivedAssets) == 0 {
		flow.ReceivedAssets = append(flow.ReceivedAssets, placeholderAsset)
	} else if len(flow.SentAssets) == 0 {
		flow.SentAssets = append(flow.SentAssets, placeholderAsset)
	}
}

func (flow *ContractAssetFlow) fullfillLP() {
	if len(flow.ReceivedAssets) == 2 && len(flow.SentAssets) == 0 {
		flow.SentAssets = append(flow.SentAssets, placeholderAsset)
	}
	if len(flow.SentAssets) == 2 && len(flow.ReceivedAssets) == 0 {
		flow.ReceivedAssets = append(flow.ReceivedAssets, placeholderAsset)
	}
}

const (
	// Some stake protocols like HOP don't give us a staked token while we're staking.
	// So we use a placeholder token as the mocked staked token.
	placeholderStakedToken = "placeholder"
)

func ParseAndSave(chainName string, repo data.DeFiAssetRepo, flow *ContractAssetFlow) error {
	log.Info("DEFI: PROCESS", zap.String("chainName", chainName), zap.Int("received", len(flow.ReceivedAssets)), zap.Int("sent", len(flow.SentAssets)))
	for i := range flow.ReceivedAssets {
		flow.ReceivedAssets[i].UsdPrice = getUsdPrice(chainName, flow.ReceivedAssets[i].Token.Address)
	}
	for i := range flow.SentAssets {
		flow.SentAssets[i].UsdPrice = getUsdPrice(chainName, flow.SentAssets[i].Token.Address)
	}

	flow.fullfill()

	var matched []matchedDeFiAsset
	earlist, err := data.DeFiAssetRepoInst.GetPlatformEarlistOpenedAt(context.Background(), chainName, flow.Address, flow.PlatformID)
	if err != nil {
		return err
	}
	if earlist != nil {
		platHistories, err := data.DeFiAssetRepoInst.GetPlatformHistories(context.Background(), chainName, flow.Address, flow.PlatformID, earlist.OpenedAt)
		if err != nil {
			return err
		}
		set := platformHistorySet(platHistories)
		matched = set.TryMatch(flow.Received(), flow.Sent())
	}

	var historiesByTx map[string][]*data.UserDeFiAssetTxnHistory
	if len(matched) > 0 {
		historiesByTx = make(map[string][]*data.UserDeFiAssetTxnHistory)
		for _, item := range matchedToHistories(chainName, flow, matched) {
			key := fmt.Sprint(item.TransactionHash, item.Type)
			historiesByTx[key] = append(historiesByTx[key], item)
		}
	} else {
		historiesByTx, err = parseFlow(chainName, flow)
		if err != nil {
			return err
		}
	}

	now := time.Now().Unix()
	for _, histories := range historiesByTx {
		for _, h := range histories {
			h.CreatedAt = now
			h.UpdatedAt = now
		}

		if err := repo.SaveTxnHistory(context.Background(), histories); err != nil {
			return fmt.Errorf("[saveHistory] %w", err)
		}

		req := &data.DeFiOpenPostionReq{
			ChainName:    chainName,
			PlatformID:   histories[0].PlatformID,
			Address:      histories[0].Address,
			AssetAddress: histories[0].AssetAddress,
			TxTime:       0,
		}
		assetHistories, err := repo.LoadAssetHistories(context.Background(), req, histories[0].Type)
		if err != nil {
			return fmt.Errorf("[loadHistory] %w", err)
		}
		valueUsd := repo.GetValueUsd(assetHistories)
		return repo.MaintainDeFiTxnAsset(context.Background(), assetHistories, valueUsd)
	}
	return nil
}

func parseFlow(chainName string, flow *ContractAssetFlow) (map[string][]*data.UserDeFiAssetTxnHistory, error) {
	historiesByTx := make(map[string][]*data.UserDeFiAssetTxnHistory)
	var action data.DeFiAction
	if flow.IsBorrowing() {
		action = data.DeFiActionDebtBorrow
	} else if flow.IsDeposit() {
		if flow.SentAssets[0].Token.IsNFT() {
			log.Info("DEFI: IGNORE SENT NFT", zap.String("chainName", chainName))
			return nil, nil
		}

		action = data.DeFiActionDepositSupply
	} else if flow.IsStaked() {
		if flow.SentAssets[0].Token.IsNFT() || flow.ReceivedAssets[0].Token.IsNFT() {
			log.Info("DEFI: IGNORE BOTH NFT", zap.String("chainName", chainName))
			return nil, nil
		}
		action = data.DeFiActionStakedStake
	} else if flow.IsAddingLP() {
		action = data.DeFiActionLPAdd
	} else {
		log.Info("DEFI: IGNORE PATTERN", zap.String("chainName", chainName))
		return nil, nil
	}
	_, uid, err := biz.UserAddressSwitchNew(flow.Address)
	if err != nil {
		return nil, err
	}
	tokenAddresses := make(map[string]bool)

	for _, recv := range flow.ReceivedAssets {
		// Shouldn't exist the same address in the DeFi asset.
		if _, ok := tokenAddresses[recv.Token.Address]; ok {
			log.Info("DEFI: IGNORE MULTIPLE SAME TOKENS", zap.String("chainName", chainName))
			return nil, nil
		}
		tokenAddresses[recv.Token.Address] = true
		historiesByTx[flow.TransactionHash] = append(historiesByTx[flow.TransactionHash], &data.UserDeFiAssetTxnHistory{
			Uid:             uid,
			BlockNumber:     flow.BlockNumber,
			PlatformID:      flow.PlatformID,
			ChainName:       chainName,
			TransactionHash: flow.TransactionHash,
			Address:         flow.Address,
			TokenAddress:    recv.Token.Address,
			Action:          action,
			AssetDirection:  data.DeFiAssetTypeDirRecv,
			Amount:          toAmount(chainName, recv.Amount, recv.Token),
			RawAmount:       recv.Amount,
			TokenUri:        recv.Token.TokenUri,
			Decimals:        int32(recv.Token.Decimals),
			UsdPrice:        recv.UsdPrice,
			Symbol:          recv.Token.Symbol,
			TxTime:          flow.TxTime,
			AssetAddress:    flow.AssetKey(),
			Enabled:         flow.PlatformEnabled,
		})
	}
	for _, send := range flow.SentAssets {
		// Shouldn't exist the same address in the DeFi asset.
		if _, ok := tokenAddresses[send.Token.Address]; ok {
			log.Info("DEFI: IGNORE MULTIPLE SAME TOKENS", zap.String("chainName", chainName))
			return nil, nil
		}
		tokenAddresses[send.Token.Address] = true
		historiesByTx[flow.TransactionHash] = append(historiesByTx[flow.TransactionHash], &data.UserDeFiAssetTxnHistory{
			Uid:             uid,
			ChainName:       chainName,
			BlockNumber:     flow.BlockNumber,
			PlatformID:      flow.PlatformID,
			TransactionHash: flow.TransactionHash,
			Address:         flow.Address,
			Action:          action,
			AssetDirection:  data.DeFiAssetTypeDirSend,
			TokenAddress:    send.Token.Address,
			Amount:          toAmount(chainName, send.Amount, send.Token),
			RawAmount:       send.Amount,
			TokenUri:        send.Token.TokenUri,
			Decimals:        int32(send.Token.Decimals),
			UsdPrice:        send.UsdPrice,
			Symbol:          send.Token.Symbol,
			TxTime:          flow.TxTime,
			AssetAddress:    flow.AssetKey(),
		})
	}
	return historiesByTx, nil
}

func getUsdPrice(chainName string, tokenAddress string) decimal.Decimal {
	rawUsdPrice, err := biz.GetTokenPriceRetryAlert(nil, chainName, biz.USD, tokenAddress)
	if err != nil {
		log.Info("FAILED TO FETCH TOKEN PRICE", zap.String("chainName", chainName), zap.String("tokenAddress", tokenAddress))
		rawUsdPrice = decimal.Zero.String()
	}
	usdPrice, _ := decimal.NewFromString(rawUsdPrice)
	return usdPrice
}

func toAmount(chainName string, rawAmount decimal.Decimal, token types.TokenInfo) string {
	decimals := token.Decimals
	if token.Address == "" {

		platInfo, _ := biz.GetChainPlatInfo(chainName)
		decimals = int64(platInfo.Decimal)
	}
	return utils.StringDecimals(rawAmount.String(), int(decimals))
}
