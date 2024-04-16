package defi

import (
	"block-crawling/internal/data"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type platformHistorySet []*data.UserDeFiAssetTxnHistory

type historyGroup struct {
	tokens map[string]decimal.Decimal
	items  []*data.UserDeFiAssetTxnHistory
}

type matchedDeFiAsset struct {
	Uid          string
	AssetAddress string
	Type         string
	TokenAddress string
	Amount       decimal.Decimal
	Asset        Asset
	Dir          string
}

func (set platformHistorySet) TryMatch(received map[string]Asset, sent map[string]Asset) []matchedDeFiAsset {
	byAssetKey := make(map[string]historyGroup)
	for _, item := range set {
		assetKey := fmt.Sprint(item.Type, item.AssetAddress)
		if _, ok := byAssetKey[assetKey]; !ok {
			byAssetKey[assetKey] = historyGroup{
				tokens: make(map[string]decimal.Decimal),
				items:  make([]*data.UserDeFiAssetTxnHistory, 0, 4),
			}
		}
		group := byAssetKey[assetKey]
		group.items = append(group.items, item)
		if item.AssetDirection == data.DeFiAssetTypeDirSend {
			// 增加在合约中的资产
			group.tokens[item.TokenAddress] = group.tokens[item.TokenAddress].Add(item.RawAmount)
		} else {
			// 减少在合约中的资产
			group.tokens[item.TokenAddress] = group.tokens[item.TokenAddress].Sub(item.RawAmount)
		}
		byAssetKey[assetKey] = group
	}

	results := make(map[string]matchedDeFiAsset)

	// 匹配同类 Token，
	for tokenAddress := range received {
		if received[tokenAddress].Amount.LessThanOrEqual(decimal.Zero) {
			continue
		}
		for assetKey, group := range byAssetKey {
			defiAmount := group.tokens[tokenAddress]
			// 大于 0 是我们的把资产发送到了合约中。
			if defiAmount.GreaterThan(decimal.Zero) {
				if _, ok := results[assetKey]; !ok {
					results[assetKey] = matchedDeFiAsset{
						Uid:          group.items[0].Uid,
						AssetAddress: group.items[0].AssetAddress,
						Type:         group.items[0].Type,
						TokenAddress: tokenAddress,
						Amount:       decimal.Zero,
						Asset:        received[tokenAddress],
						Dir:          data.DeFiAssetTypeDirRecv,
					}
				}
				result := results[assetKey]
				if defiAmount.GreaterThan(received[tokenAddress].Amount) {
					result.Amount = result.Amount.Add(received[tokenAddress].Amount)
					group.tokens[tokenAddress] = group.tokens[tokenAddress].Sub(received[tokenAddress].Amount)
				} else {
					result.Amount = result.Amount.Add(defiAmount)
					group.tokens[tokenAddress] = decimal.Zero
				}
				results[assetKey] = result
				recv := received[tokenAddress]
				recv.Amount = recv.Amount.Sub(defiAmount)
				received[tokenAddress] = recv
				if received[tokenAddress].Amount.LessThanOrEqual(decimal.Zero) {
					goto _NEXT_RECV
				}
			}
		}
	_NEXT_RECV:
	}
	for tokenAddress := range sent {
		if sent[tokenAddress].Amount.LessThanOrEqual(decimal.Zero) {
			continue
		}
		for assetKey, group := range byAssetKey {
			defiAmount := group.tokens[tokenAddress]
			// 小于 0 是我们的资产是从合约中出来的。
			if defiAmount.LessThan(decimal.Zero) {
				if _, ok := results[assetKey]; !ok {
					results[assetKey] = matchedDeFiAsset{
						AssetAddress: group.items[0].AssetAddress,
						Type:         group.items[0].Type,
						TokenAddress: tokenAddress,
						Amount:       decimal.Zero,
						Dir:          data.DeFiAssetTypeDirSend,
						Asset:        sent[tokenAddress],
					}
				}
				result := results[assetKey]
				if defiAmount.Abs().GreaterThan(sent[tokenAddress].Amount) {
					result.Amount = result.Amount.Add(sent[tokenAddress].Amount)
					group.tokens[tokenAddress] = group.tokens[tokenAddress].Add(sent[tokenAddress].Amount)
				} else {
					result.Amount = result.Amount.Add(defiAmount.Abs())
					group.tokens[tokenAddress] = decimal.Zero
				}
				results[assetKey] = result
				s := sent[tokenAddress]
				s.Amount = s.Amount.Sub(defiAmount.Abs())
				sent[tokenAddress] = s
				if sent[tokenAddress].Amount.LessThanOrEqual(decimal.Zero) {
					goto _NEXT_SEND
				}
			}
		}
	_NEXT_SEND:
	}
	// 按价值消除
	for tokenAddress, recv := range received {
		if recv.Amount.LessThanOrEqual(decimal.Zero) || recv.UsdPrice.IsZero() {
			continue
		}
		for assetKey, group := range byAssetKey {
			for ta, defiAmount := range group.tokens {
				// 大于 0 是我们的把资产发送到了合约中。
				if defiAmount.GreaterThan(decimal.Zero) {
					defiAmount = convertToAssetAmount(group.items[0].ChainName, ta, defiAmount, recv)
					if !defiAmount.GreaterThan(decimal.Zero) {
						continue
					}

					if _, ok := results[assetKey]; !ok {
						results[assetKey] = matchedDeFiAsset{
							Uid:          group.items[0].Uid,
							AssetAddress: group.items[0].AssetAddress,
							Type:         group.items[0].Type,
							TokenAddress: tokenAddress,
							Amount:       decimal.Zero,
							Asset:        received[tokenAddress],
							Dir:          data.DeFiAssetTypeDirRecv,
						}
					}
					result := results[assetKey]
					if defiAmount.GreaterThan(received[tokenAddress].Amount) {
						result.Amount = result.Amount.Add(received[tokenAddress].Amount)
						group.tokens[tokenAddress] = group.tokens[tokenAddress].Sub(received[tokenAddress].Amount)
					} else {
						result.Amount = result.Amount.Add(defiAmount)
						group.tokens[tokenAddress] = decimal.Zero
					}
					results[assetKey] = result
					recv := received[tokenAddress]
					recv.Amount = recv.Amount.Sub(defiAmount)
					received[tokenAddress] = recv
					if recv.Amount.LessThanOrEqual(decimal.Zero) {
						goto _NEXT_VAL_RECV
					}
				}
			}
		}
		received[tokenAddress] = recv
	_NEXT_VAL_RECV:
	}
	for tokenAddress, asset := range sent {
		if asset.Amount.LessThanOrEqual(decimal.Zero) || asset.UsdPrice.IsZero() {
			continue
		}
		for assetKey, group := range byAssetKey {
			for ta, defiAmount := range group.tokens {
				// 小于 0 是我们的资产是从合约中出来的。
				if defiAmount.LessThan(decimal.Zero) {
					defiAmount = convertToAssetAmount(group.items[0].ChainName, ta, defiAmount, asset)
					if !defiAmount.GreaterThan(decimal.Zero) {
						continue
					}
					if _, ok := results[assetKey]; !ok {
						results[assetKey] = matchedDeFiAsset{
							AssetAddress: group.items[0].AssetAddress,
							Type:         group.items[0].Type,
							TokenAddress: tokenAddress,
							Amount:       decimal.Zero,
							Dir:          data.DeFiAssetTypeDirSend,
							Asset:        asset,
						}
					}
					result := results[assetKey]
					if defiAmount.Abs().GreaterThan(asset.Amount) {
						result.Amount = result.Amount.Add(asset.Amount)
						group.tokens[tokenAddress] = group.tokens[tokenAddress].Add(asset.Amount)
					} else {
						result.Amount = result.Amount.Add(defiAmount.Abs())
						group.tokens[tokenAddress] = decimal.Zero
					}
					if asset.Amount.LessThanOrEqual(decimal.Zero) {
						goto _NEXT_VAL_SEND
					}
				}
			}
		}
		sent[tokenAddress] = asset
	_NEXT_VAL_SEND:
	}

	matchs := make([]matchedDeFiAsset, 0, len(results))
	for _, item := range results {
		matchs = append(matchs, item)
	}
	return matchs
}

func matchedToHistories(chainName string, flow *ContractAssetFlow, matched []matchedDeFiAsset) []*data.UserDeFiAssetTxnHistory {
	counters := make(map[string]int)
	results := make([]*data.UserDeFiAssetTxnHistory, 0, len(matched))
	for _, item := range matched {
		key := fmt.Sprint(flow.TransactionHash, flow.Address, item.TokenAddress)
		counter := counters[key]
		counters[key]++
		txHash := flow.TransactionHash
		if counter > 0 {
			txHash = fmt.Sprint("%s#split-%d", txHash, counter)
		}
		results = append(results, &data.UserDeFiAssetTxnHistory{
			Uid:             item.Uid,
			BlockNumber:     flow.BlockNumber,
			PlatformID:      flow.PlatformID,
			ChainName:       chainName,
			TransactionHash: txHash,
			Address:         flow.Address,
			TokenAddress:    item.TokenAddress,
			Action:          getElimationAction(item.Type),
			AssetDirection:  item.Dir,
			Type:            item.Type,
			Amount:          toAmount(chainName, item.Amount, item.Asset.Token),
			RawAmount:       item.Amount,
			UsdPrice:        item.Asset.UsdPrice,
			TokenUri:        item.Asset.Token.TokenUri,
			Decimals:        int32(item.Asset.Token.Decimals),
			Symbol:          item.Asset.Token.Symbol,
			TxTime:          flow.TxTime,
			AssetAddress:    item.AssetAddress,
			CreatedAt:       time.Now().Unix(),
			UpdatedAt:       time.Now().Unix(),
			Enabled:         flow.PlatformEnabled,
		})
	}
	return results
}

func getElimationAction(typ string) data.DeFiAction {
	switch typ {
	case data.DeFiAssetTypeDeposits:
		return data.DeFiActionDepositWithdraw
	case data.DeFiAssetTypeDebt:
		return data.DeFiActionDebtRepay
	case data.DeFiAssetTypeStake:
		return data.DeFiActionStakedUnstake
	case data.DeFiAssetTypeLP:
		return data.DeFiActionLPRemove
	}
	panic(fmt.Errorf("unknown type: %s", typ))
}

func convertToAssetAmount(chainName, tokenAddress string, amount decimal.Decimal, asset Asset) decimal.Decimal {
	usdPrice := getUsdPrice(chainName, tokenAddress)
	usdVal := amount.Mul(usdPrice)
	return usdVal.Div(asset.UsdPrice).Round(0)
}
