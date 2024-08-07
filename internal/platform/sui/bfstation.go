package sui

import (
	v1 "block-crawling/api/bfstation/client"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/platform/sui/stypes"
	suiswap "block-crawling/internal/platform/sui/swap"
	"block-crawling/internal/platform/swap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type bfstationHandler struct {
	chainName   string
	repo        data.BFCStationRepo
	blockNumber uint64
	blockHash   string
	suffixers   map[string]common.TxHashSuffixer
}

func (h *bfstationHandler) Handle(txInfo *stypes.TransactionInfo, pairs []*swap.Pair, status string) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("Handle BFStation error, chainName:"+h.chainName, e)
			} else {
				log.Errore("Handle BFStation Swap panic, chainName:"+h.chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s处理 Station 交易失败, error：%s", h.chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithAlarmChannel("kanban")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	if err := h.do(txInfo, pairs, status); err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链 STATION 交易处理失败: %s, txHash: %s。", h.chainName, err, txInfo.Digest)
		alarmOpts := biz.WithMsgLevel("FATAL")
		if biz.LarkClient == nil {
			return
		}
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts, biz.WithAlarmChannel("kanban"))
		log.Error("STATION 维护代币和交易池失败", zap.Any("chainName", h.chainName), zap.Any("error", err), zap.String("chainName", txInfo.Digest))
	}
}

func (h *bfstationHandler) do(txInfo *stypes.TransactionInfo, pairs []*swap.Pair, status string) error {
	gasLimit, gasUsed, feeAmount := getFees(txInfo)
	records := make([]data.BFCStationRecord, 0, len(pairs))
	if feeRecords, err := h.trackCollectFees(status, txInfo); err != nil {
		return fmt.Errorf("[trackCollectFees] %w", err)
	} else if feeRecords != nil {
		records = append(records, feeRecords...)
	}

	for _, item := range pairs {
		if item.Dex != biz.BFStationDexSwap && item.Dex != biz.BFStationStable && item.Dex != biz.BFStationDexLiq {
			continue
		}
		var poolID string
		var vault string
		if item.Dex == biz.BFStationStable {
			vault = item.PairContract
		} else {
			poolID = item.PairContract
		}

		swapType := data.BSTxTypeSwap

		if item.Dex == biz.BFStationStable {
			var event *suiswap.BFStationStableEvent
			if err := json.Unmarshal(item.RawEvent, &event); err != nil {
				log.Error("STATION SWAP STABLE PARSE FAILED", zap.Any("item", item), zap.Error(err))
				continue
			}
			swapType = data.BSTxTypeMint
			if event.Atob {
				swapType = data.BSTxRedeem
			}
		} else if item.Dex == biz.BFStationDexLiq {
			var event *suiswap.BFStationDexLiqEvent
			if err := json.Unmarshal(item.RawEvent, &event); err != nil {
				log.Error("STATION DEX LIQ PARSE FAILED", zap.Any("item", item), zap.Error(err))
				continue
			}
			if event.Action == "add" {
				swapType = data.BSTxTypeAddLiquidity
			} else {
				swapType = data.BSTxTypeRemoveLiquidity
			}
		}

		amountIn, _ := decimal.NewFromString(item.Input.Amount)
		amountOut, _ := decimal.NewFromString(item.Output.Amount)
		_, walletUID, _ := biz.UserAddressSwitchRetryAlert(h.chainName, item.FromAddress)
		if walletUID == "" {
			walletUID = item.FromAddress
		}
		txHash := item.TxHash
		if _, ok := h.suffixers[txHash]; !ok {
			h.suffixers[txHash] = common.NewTxHashSuffixer(txHash)
		}
		record := data.BFCStationRecord{
			BlockHash:       h.blockHash,
			BlockNumber:     item.BlockNumber,
			TransactionHash: h.suffixers[txHash].WithSuffix(),
			TxTime:          int64(item.TxTime),
			WalletAddress:   item.FromAddress,
			WalletUID:       walletUID,
			FeeAmount:       feeAmount,
			GasLimit:        gasLimit,
			GasUsed:         gasUsed,
			Type:            swapType,
			PoolID:          poolID,
			Vault:           vault,
			TokenAmountIn:   amountIn,
			TokenAmountOut:  amountOut,
			CoinTypeIn:      item.Input.Address,
			CoinTypeOut:     item.Output.Address,
			ParsedJson:      string(item.RawEvent),
			CreatedAt:       time.Now().Unix(),
			Status:          status,
			UpdatedAt:       time.Now().Unix(),
		}
		records = append(records, record)
		if status == biz.SUCCESS {
			h.pushEvent(&record, txInfo)
		}
	}
	for i := 0; i < len(records); i++ {
		coinInfoIn, _ := biz.GetTokenInfoRetryAlert(context.Background(), h.chainName, "0x"+records[i].CoinTypeIn)
		coinInfoOut, _ := biz.GetTokenInfoRetryAlert(context.Background(), h.chainName, "0x"+records[i].CoinTypeOut)
		coinInfoInStr, _ := json.Marshal(coinInfoIn)
		coinInfoOutStr, _ := json.Marshal(coinInfoOut)
		records[i].CoinInfoIn = string(coinInfoInStr)
		records[i].CoinInfoOut = string(coinInfoOutStr)
	}
	if len(records) > 0 {
		err := h.repo.BatchSave(context.Background(), h.chainName, records)
		if err != nil {
			return fmt.Errorf("[batchSave] %w", err)
		}

		if status == biz.SUCCESS {
			if err := h.handleBFStationHolders(records, txInfo); err != nil {
				return fmt.Errorf("[handleBFStationHolders] %w", err)
			}
		} else if status == biz.FAIL {
			h.notifyBFStationFailure(records, txInfo.Effects.Status.Error)
		}
	}
	return nil
}

func (h *bfstationHandler) pushEvent(record *data.BFCStationRecord, txInfo *stypes.TransactionInfo) {
	if err := h.doPushEvent(record); err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s推送 STATION 事件失败: %s, txHash: %s。", h.chainName, err, txInfo.Digest)
		alarmOpts := biz.WithMsgLevel("FATAL")
		if biz.LarkClient == nil {
			return
		}
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts, biz.WithAlarmChannel("kanban"))
		log.Error("推送 STATION 事件失败", zap.Any("chainName", h.chainName), zap.Any("error", err), zap.String("chainName", txInfo.Digest))
	}
}

func (h *bfstationHandler) doPushEvent(record *data.BFCStationRecord) error {
	switch record.Type {
	case data.BSTxTypeSwap:
		var raw *suiswap.BFStationDexSwapEvent
		if err := json.Unmarshal([]byte(record.ParsedJson), &raw); err != nil {
			return err
		}
		return biz.PushBFStationSwapEvent(context.Background(), &v1.SyncSwapEventRequest{
			ChainName:       h.chainName,
			PoolID:          raw.Pool,
			A2B:             raw.Atob,
			SenderAddress:   raw.Sender,
			CoinTypeA:       raw.CoinTypeA,
			CoinTypeB:       raw.CoinTypeB,
			AmountIn:        raw.AmountIn,
			AmountOut:       raw.AmountOut,
			FeeAmount:       raw.FeeAmount,
			BeforeSqrtPrice: raw.BeforeSqrtPrice,
			AfterSqrtPrice:  raw.AfterSqrtPrice,
			CreatedAt:       record.CreatedAt,
		})
	case data.BSTxTypeAddLiquidity, data.BSTxTypeRemoveLiquidity:
		var raw *suiswap.BFStationDexLiqEvent
		if err := json.Unmarshal([]byte(record.ParsedJson), &raw); err != nil {
			return err
		}
		return biz.PushBFStationLiquidityEvent(context.Background(), &v1.SyncLiquidityEventRequest{
			ChainName:                h.chainName,
			PoolID:                   raw.Pool,
			PositionID:               raw.Position,
			SenderAddress:            raw.Sender,
			CoinTypeA:                raw.CoinTypeA,
			CoinTypeB:                raw.CoinTypeB,
			DeltaLiquidity:           raw.DeltaLiquidity,
			BeforePositioinLiquidity: raw.BeforePositionLiquidity,
			BeforePoolLiquidity:      raw.BeforePoolLiquidity,
			AfterPositionLiquidity:   raw.AfterPositionLiquidity,
			AfterPoolLiquidity:       raw.AfterPoolLiquidity,
			AmountA:                  raw.AmountA,
			AmountB:                  raw.AmountB,
			Action:                   raw.Action,
			CreatedAt:                record.CreatedAt,
		})
	}
	return nil
}

func (h *bfstationHandler) handleBFStationHolders(records []data.BFCStationRecord, txInfo *stypes.TransactionInfo) error {
	if err := h.trackTokensAndPools(records, txInfo); err != nil {
		return fmt.Errorf("[trackTokensAndPools] %w", err)
	}
	if err := h.trackTokenBalanceChanges(txInfo); err != nil {
		return fmt.Errorf("[trackBalanceChanges] %w", err)
	}
	if err := h.trackPoolObjectChanges(txInfo); err != nil {
		return fmt.Errorf("[trackObjectChanges] %w", err)
	}
	return nil
}

func (h *bfstationHandler) trackTokensAndPools(records []data.BFCStationRecord, txInfo *stypes.TransactionInfo) error {
	var tokens []data.BFStationToken
	now := time.Now().Unix()
	for _, r := range records {
		switch r.Type {
		case data.BSTxTypeMint:
			// mint: Input BFC, Output token
			coinType := r.CoinTypeOut
			tokens = append(tokens, data.BFStationToken{
				CoinType:    coinType,
				MintTxHash:  r.TransactionHash,
				MintAddress: r.WalletAddress,
				MintedAt:    r.TxTime,
				CreatedAt:   now,
				UpdatedAt:   now,
			})
		case data.BSTxRedeem:
			// redeem: Input token, Output BFC
			coinType := r.CoinTypeIn
			tokens = append(tokens, data.BFStationToken{
				CoinType:  coinType,
				CreatedAt: now,
				UpdatedAt: now,
			})
		}
	}
	if len(tokens) > 0 {
		if err := h.repo.BatchSaveTokens(context.Background(), h.chainName, tokens); err != nil {
			return fmt.Errorf("[saveTokens] %w", err)
		}
	}
	return h.trackPoolCreating(txInfo)
}

func (h *bfstationHandler) trackTokenBalanceChanges(txInfo *stypes.TransactionInfo) error {
	balanceChanges := txInfo.BalanceChanges
	txHash := txInfo.Digest

	var tokens []data.BFStationAccountToken
	now := time.Now().Unix()
	for _, balanceChange := range balanceChanges {
		address := getOwnerAddress(balanceChange.Owner)
		if address == "" {
			continue
		}
		var uid string
		if data.RedisClient != nil {
			var err error
			_, uid, err = biz.UserAddressSwitchRetryAlert(h.chainName, address)
			if err != nil {
				return fmt.Errorf("[getAddressUid] %w", err)
			}
		}
		balance, err := h.getTokenBalance(address, balanceChange.CoinType)
		if err != nil {
			return fmt.Errorf("[getTokenBalance] %w", err)
		}
		tokens = append(tokens, data.BFStationAccountToken{
			Uid:               uid,
			Address:           address,
			TokenCoinType:     suiswap.StationizeBenfenCoinType(balanceChange.CoinType),
			FirstTxHash:       txHash,
			LastTxHash:        txHash,
			FirstBlockNumber:  h.blockNumber,
			LastBlockNumber:   h.blockNumber,
			Balance:           balance,
			BalanceChangeHash: txHash,
			CreatedAt:         now,
			UpdatedAt:         now,
		})
	}
	if len(tokens) > 0 {
		if err := h.repo.BatchSaveAccountTokens(context.Background(), h.chainName, tokens); err != nil {
			return fmt.Errorf("[saveAccountTokens] %w", err)
		}
	}
	return nil
}

type collectedToken struct {
	amount decimal.Decimal
	owner  string
}

type BFStationCollectFeeEvent struct {
	AmountA   string `json:"amount_a"`
	AmountB   string `json:"amount_b"`
	CoinTypeA string `json:"coin_type_a"`
	CoinTypeB string `json:"coin_type_b"`
	Pool      string `json:"pool"`
	Sender    string `json:"sender"`
	Position  string `json:"position"`
}

func (h *bfstationHandler) trackCollectFees(status string, txInfo *stypes.TransactionInfo) ([]data.BFCStationRecord, error) {
	collectFeeCall := h.extractCollectFeeMoveCall(txInfo)
	if collectFeeCall == nil {
		return nil, nil
	}
	var collectFees []data.BFStationCollectFee
	var err error
	var event *BFStationCollectFeeEvent
	if status == biz.SUCCESS {
		if err := suiswap.ExtractMoveCallEvent(collectFeeCall, txInfo, &event); err != nil {
			return nil, fmt.Errorf("[extractCollectFeeEvent] %w", err)
		}
		amountA, _ := decimal.NewFromString(event.AmountA)
		amountB, _ := decimal.NewFromString(event.AmountB)
		collectFees = []data.BFStationCollectFee{
			{
				TxHash:    txInfo.Digest,
				PoolID:    event.Pool,
				Address:   event.Sender,
				CoinTypeA: event.CoinTypeA,
				CoinTypeB: event.CoinTypeB,
				AmountA:   amountA,
				AmountB:   amountB,
				TxTime:    txInfo.TxTime(),
				Position:  event.Position,
				CreatedAt: time.Now().Unix(),
				UpdatedAt: time.Now().Unix(),
			},
		}
	} else {
		collectFees, err = h.extractCollectFeesFromMoveCall(txInfo, collectFeeCall)
	}
	if err != nil {
		return nil, fmt.Errorf("[extractCollectFees] %w", err)
	}
	if len(collectFees) == 0 {
		return nil, nil
	}
	var records []data.BFCStationRecord
	for i := 0; i < len(collectFees); i++ {
		var uid string
		if data.RedisClient != nil {
			var err error
			_, uid, err = biz.UserAddressSwitchRetryAlert(h.chainName, collectFees[i].Address)
			if err != nil {
				return nil, fmt.Errorf("[getAddressUid] %w", err)
			}
		}
		parsedJson, _ := json.Marshal(event)
		collectFees[i].Uid = uid
		records = append(records, data.BFCStationRecord{
			BlockNumber:     int(h.blockNumber),
			BlockHash:       h.blockHash,
			TransactionHash: txInfo.Digest,
			TxTime:          txInfo.TxTime(),
			WalletAddress:   collectFees[i].Address,
			WalletUID:       uid,
			FeeAmount:       txInfo.FeeAmount(),
			GasLimit:        txInfo.GasLimit(),
			GasUsed:         txInfo.GasUsed(),
			Type:            data.BSTxTypeCollectFee,
			PoolID:          collectFees[i].PoolID,
			TokenAmountIn:   collectFees[i].AmountA,
			TokenAmountOut:  collectFees[i].AmountB,
			CoinTypeIn:      collectFees[i].CoinTypeA,
			CoinTypeOut:     collectFees[i].CoinTypeB,
			ParsedJson:      string(parsedJson),
			CreatedAt:       collectFees[i].CreatedAt,
			Status:          status,
			UpdatedAt:       collectFees[i].CreatedAt,
		})
	}

	if err := h.repo.BatchSaveCollectFees(context.Background(), h.chainName, collectFees); err != nil {
		return nil, fmt.Errorf("[saveCollectFees] %w", err)
	}
	return records, nil
}

func (h *bfstationHandler) extractCollectFeesFromMoveCall(txInfo *stypes.TransactionInfo, collectFeeCall *stypes.MoveCall) ([]data.BFStationCollectFee, error) {
	collectedTokens := make(map[string]collectedToken)
	for _, balanceChange := range txInfo.BalanceChanges {
		if IsNative(balanceChange.CoinType) {
			continue
		}
		address := getOwnerAddress(balanceChange.Owner)
		if address == "" {
			continue
		}
		// owner is the sender and the amount is positive
		if address == txInfo.Transaction.Data.Sender {
			amount, err := decimal.NewFromString(balanceChange.Amount)
			if err != nil {
				return nil, fmt.Errorf("[collectFeeParseAmount] %w", err)
			}
			if amount.IsPositive() {
				collectedTokens[balanceChange.CoinType] = collectedToken{
					amount: amount,
					owner:  address,
				}
			}
		}
	}

	poolID, err := h.getMoveCallArgObjectID(&txInfo.Transaction.Data.Transaction, collectFeeCall, 1)
	if err != nil {
		return nil, fmt.Errorf("[collectFeePoolId] %w", err)
	}
	positionID, err := h.getMoveCallArgObjectID(&txInfo.Transaction.Data.Transaction, collectFeeCall, 2)
	if err != nil {
		return nil, fmt.Errorf("[collectFeePositionId] %w", err)
	}
	pool, err := data.BFCStationRepoIns.GetPool(context.Background(), h.chainName, poolID)
	if err != nil {
		return nil, fmt.Errorf("[collectFeeLoadPool] %w", err)
	}
	tokenA, ok := collectedTokens[pool.CoinTypeA]
	if !ok {
		return nil, fmt.Errorf("[collectFee no tokenA %s in txn] %w", pool.CoinTypeA, err)
	}
	tokenB, ok := collectedTokens[pool.CoinTypeB]
	if !ok {
		return nil, fmt.Errorf("[collectFee no tokenB %s in txn] %w", pool.CoinTypeB, err)
	}
	return []data.BFStationCollectFee{
		{
			TxHash:    txInfo.Digest,
			PoolID:    poolID,
			Address:   tokenB.owner,
			CoinTypeA: pool.CoinTypeA,
			CoinTypeB: pool.CoinTypeB,
			AmountA:   tokenA.amount,
			AmountB:   tokenB.amount,
			TxTime:    txInfo.TxTime(),
			Position:  positionID,
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		},
	}, nil
}

func (h *bfstationHandler) getMoveCallArgObjectID(txData *stypes.TransactionData, moveCall *stypes.MoveCall, pos int) (string, error) {
	return moveCall.GetArgObjectID(pos, txData)
}

var bfStationCollectFeeMoveCall = &stypes.MoveCall{
	Package:  suiswap.BFStationContract,
	Module:   "pool_script",
	Function: "collect_fee",
}

func (h *bfstationHandler) extractCollectFeeMoveCall(txInfo *stypes.TransactionInfo) *stypes.MoveCall {
	for _, tx := range txInfo.Transaction.Data.Transaction.Transactions() {
		moveCall, err := tx.MoveCall()
		if err != nil {
			continue
		}
		if moveCall == nil {
			continue
		}
		if moveCall.FuncEquals(bfStationCollectFeeMoveCall) {
			return moveCall
		}
	}
	return nil
}

type BFStationCreatePoolEvent struct {
	PoolID           string `json:"pool_id"`
	PoolKey          string `json:"pool_key"`
	Sender           string `json:"sender"`
	TickSpacing      int    `json:"tick_spacing"`
	Liquidity        string `json:"liquidity"`
	Index            string `json:"index"`
	FeeRate          string `json:"fee_rate"`
	CurrentTickIndex struct {
		Bits int `json:"bits"`
	} `json:"current_tick_index"`
	CurrentSqrtPrice string `json:"current_sqrt_price"`
	CoinTypeA        string `json:"coin_type_a"`
	CoinTypeB        string `json:"coin_type_b"`
}

func (h *bfstationHandler) trackPoolCreating(txInfo *stypes.TransactionInfo) error {
	var event *BFStationCreatePoolEvent
	err := suiswap.ExtractMoveCallEvent(
		&stypes.MoveCall{
			Package:  suiswap.BFStationContract,
			Module:   "pool_script",
			Function: "create_pool",
		},
		txInfo,
		&event,
	)
	if err != nil {
		return fmt.Errorf("[parsePoolCreatingEvent] %w", err)
	}
	if event == nil {
		return nil
	}
	return h.repo.BatchSavePools(context.Background(), h.chainName, []data.BFStationPool{
		{
			ObjectID:  event.PoolID,
			TxHash:    txInfo.Digest,
			CoinTypeA: event.CoinTypeA,
			CoinTypeB: event.CoinTypeB,
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		},
	})
}

// https://obc.openblock.vip/txblock/ES5FesWsRKCExm3g6a5SSC72JfqosYeHTAnCvhfyauGF
func (h *bfstationHandler) trackPoolObjectChanges(txInfo *stypes.TransactionInfo) error {
	stationPos := &SuiSimpleStructTag{
		Address: suiswap.BFStationContract,
		Module:  "position",
		Name:    "Position",
	}
	stationPool := &SuiSimpleStructTag{
		Address: suiswap.BFStationContract,
		Module:  "pool",
		Name:    "Pool",
	}
	var poolObjectID string
	for _, oc := range txInfo.ObjectChanges {
		if oc.ObjectType == "" {
			continue
		}
		st, err := ParseSuiSimpleStructTag(h.chainName, oc.ObjectType)
		if err != nil {
			return fmt.Errorf("[parsePoolStructTag] %w", err)
		}
		if st.Equals(stationPool) {
			poolObjectID = oc.ObjectId
			break
		}
	}
	var records []data.BFStationAccountPool
	for _, oc := range txInfo.ObjectChanges {
		if oc.ObjectType == "" {
			continue
		}
		pos, err := ParseSuiSimpleStructTag(h.chainName, oc.ObjectType)
		if err != nil {
			return fmt.Errorf("[parsePostionStructTag] %w", err)
		}
		if !pos.Equals(stationPos) {
			continue
		}
		address := getOwnerAddress(oc.Owner)
		var uid string
		if data.RedisClient != nil {
			_, uid, _ = biz.UserAddressSwitchNew(address)
		}
		records = append(records, data.BFStationAccountPool{
			Uid:               uid,
			Address:           address,
			Position:          oc.ObjectId,
			PoolObjectID:      poolObjectID,
			ObjectChangeType:  oc.Type,
			ObjectChangeHash:  txInfo.Digest,
			ObjectChangeBlock: h.blockNumber,
			CreatedAt:         time.Now().Unix(),
			UpdatedAt:         time.Now().Unix(),
		})

	}
	if len(records) > 0 {
		return h.repo.BatchSaveAccountPools(context.Background(), h.chainName, records)
	}
	return nil
}

func (h *bfstationHandler) getTokenBalance(address string, coinType string) (decimal.Decimal, error) {
	if data.RedisClient == nil {
		return decimal.Zero, nil
	}
	ret, err := biz.ExecuteRetry(h.chainName, func(client chain.Clienter) (interface{}, error) {
		c := client.(*Client)
		r, err := c.GetTokenBalance(address, coinType, 0)
		if err != nil {
			return nil, err
		}
		return decimal.NewFromString(r)
	})
	if err != nil {
		return decimal.Zero, err
	}
	return ret.(decimal.Decimal), nil
}

func (h *bfstationHandler) notifyBFStationFailure(records []data.BFCStationRecord, err string) {
	var msgBody strings.Builder
	for _, r := range records {
		msgBody.WriteString(fmt.Sprintf(`
ID: %d
Type: %s
TxHash: %s
Wallet: %s
PoolId: %s
`,

			r.Id,
			r.Type,
			r.TransactionHash,
			r.WalletAddress,
			r.PoolID))
	}
	alarmMsg := fmt.Sprintf(`%s Station 订单异常。
Error: %s
------
%s`,
		h.chainName,
		err,
		msgBody.String(),
	)
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, biz.WithStationBot())
}
