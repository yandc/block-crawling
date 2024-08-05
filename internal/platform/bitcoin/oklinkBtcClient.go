package bitcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blockcypher/gobcy"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type OklinkBtcClient struct {
	nodeURL    string
	chainName  string
	apiKey     string
	retryAfter time.Time
	// Oklink限频 5次/s，加锁保证每次请求之后休息一秒
	lock sync.Mutex
}

func NewOklinkClient(chainName, rpcUrl string) OklinkBtcClient {
	parsed, err := url.Parse(rpcUrl)
	if err == nil && parsed.User != nil {
		username := parsed.User.Username()
		return OklinkBtcClient{
			nodeURL:    parsed.String(),
			chainName:  chainName,
			apiKey:     username,
			retryAfter: time.Time{},
		}
	}

	return OklinkBtcClient{}
}

func (c OklinkBtcClient) GetBalance(address string) (string, error) {
	c.lock.TryLock()
	defer c.lock.Unlock()
	time.Sleep(time.Second)

	url := c.nodeURL + "/api/v5/explorer/address/address-summary"
	params := map[string]string{"chainShortName": c.chainName, "address": address}
	headers := map[string]string{"OK-ACCESS-KEY": c.apiKey}

	var resp types.OklinkAddressSummaryResp
	timeout := 5 * time.Second
	err := httpclient.HttpsSignGetForm(url, params, headers, &resp, &timeout)
	if err != nil {
		return "", err
	}

	if resp.Code != "0" || len(resp.Data) == 0 {
		return "", nil
	}

	return resp.Data[0].Balance, nil
}

func (c OklinkBtcClient) GetUTXO(address string) ([]types.OklinkUTXO, error) {
	c.lock.TryLock()
	defer c.lock.Unlock()
	time.Sleep(time.Second)

	url := c.nodeURL + "/api/v5/explorer/address/utxo"
	headers := map[string]string{"OK-ACCESS-KEY": c.apiKey}
	params := map[string]string{
		"chainShortName": c.chainName,
		"address":        address,
		"limit":          "100",
	}
	timeout := 5 * time.Second

	var resp types.OklinkUTXOResp
	page := 1
	var utxos []types.OklinkUTXO
	for {
		params["page"] = strconv.Itoa(page)

		err := httpclient.HttpsSignGetForm(url, params, headers, &resp, &timeout)
		if err != nil {
			return nil, err
		}

		if resp.Code != "0" || len(resp.Data) == 0 {
			if resp.Code != "0" {
				return nil, fmt.Errorf("%s(%s)", resp.Msg, resp.Code)
			}
			return nil, nil
		}

		utxos = append(utxos, resp.Data[0].UtxoList...)

		page++
		totalPage, _ := strconv.Atoi(resp.Data[0].TotalPage)
		if page > totalPage {
			break
		}
		time.Sleep(time.Second)
	}

	return utxos, nil
}

func (c OklinkBtcClient) GetBlockNumber() (int, error) {
	height, err := c.GetBlockHeight()
	return int(height), err
}

func (c OklinkBtcClient) GetTransactionByHash(hash string) (tx types.TX, err error) {
	c.lock.TryLock()
	defer c.lock.Unlock()
	time.Sleep(time.Second)

	url := c.nodeURL + "/api/v5/explorer/transaction/transaction-fills"
	params := map[string]string{"chainShortName": c.chainName, "txid": hash}
	headers := map[string]string{"OK-ACCESS-KEY": c.apiKey}

	platInfo, _ := biz.GetChainPlatInfo(c.chainName)
	decimals := int(platInfo.Decimal)

	var resp types.OklinkTransactionDetailResp
	timeout := 5 * time.Second
	err = httpclient.HttpsSignGetForm(url, params, headers, &resp, &timeout)
	if err != nil {
		return tx, err
	}

	if resp.Code != "0" || len(resp.Data) == 0 {
		return tx, nil
	}

	transaction := resp.Data[0]
	height, _ := strconv.ParseUint(transaction.Height, 10, 64)

	var txInputs []gobcy.TXInput
	for i, input := range transaction.InputDetails {

		amountDecimal, _ := decimal.NewFromString(input.Amount)
		amount := biz.Pow10(amountDecimal, decimals).BigInt().Int64()
		txInputs = append(txInputs, gobcy.TXInput{
			OutputIndex: i,
			OutputValue: int(amount),
			Addresses:   []string{input.InputHash},
		})
	}

	var txOutputs []gobcy.TXOutput
	for _, output := range transaction.OutputDetails {
		amountDecimal, _ := decimal.NewFromString(output.Amount)
		amount := biz.Pow10(amountDecimal, decimals).BigInt()
		txOutputs = append(txOutputs, gobcy.TXOutput{
			Value:     *amount,
			Addresses: []string{output.OutputHash},
		})
	}

	feeDecimal, _ := decimal.NewFromString(transaction.Txfee)
	fee := biz.Pow10(feeDecimal, decimals).BigInt()
	timeStr, _ := strconv.ParseInt(transaction.TransactionTime, 10, 64)
	tt := time.UnixMilli(timeStr)

	rowTx := types.TX{
		//BlockHash:   transaction.,
		BlockHeight: int(height),
		Hash:        transaction.Txid,
		Fees:        *fee,
		Confirmed:   tt,
		VinSize:     len(txInputs),
		VoutSize:    len(txOutputs),
		Inputs:      txInputs,
		Outputs:     txOutputs,
	}

	return rowTx, nil
}

func (c OklinkBtcClient) GetBlockHeight() (uint64, error) {
	c.lock.TryLock()
	defer c.lock.Unlock()
	time.Sleep(time.Second)

	url := c.nodeURL + "/api/v5/explorer/blockchain/summary"
	params := map[string]string{"chainShortName": c.chainName}
	headers := map[string]string{"OK-ACCESS-KEY": c.apiKey}

	var resp types.OklinkBlockChainSummaryResp
	timeout := 5 * time.Second
	err := httpclient.HttpsSignGetForm(url, params, headers, &resp, &timeout)
	if err != nil {
		return 0, err
	}

	if resp.Code != "0" || len(resp.Data) == 0 {
		return 0, nil
	}

	lastHeight, err := strconv.Atoi(resp.Data[0].LastHeight)
	if err != nil {
		return 0, err
	}
	if lastHeight == 0 {
		return 0, errors.New("node returned zero height")
	}

	return uint64(lastHeight), nil
}

func (c OklinkBtcClient) URL() string {
	return c.nodeURL
}

func (c OklinkBtcClient) Recover(r interface{}) (err error) {
	if e, ok := r.(error); ok {
		log.Errore("IndexBlock error, chainName:"+c.chainName, e)
		err = e
	} else {
		err = errors.New(fmt.Sprintf("%s", err))
		log.Errore("IndexBlock panic, chainName:"+c.chainName, err)
	}

	// 程序出错 接入lark报警
	alarmMsg := fmt.Sprintf("请注意：%s链服务内部异常, error：%s", c.chainName, fmt.Sprintf("%s", r))
	alarmOpts := biz.WithMsgLevel("FATAL")
	alarmOpts = biz.WithAlarmChainName(c.chainName)
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	return
}

func (c OklinkBtcClient) RetryAfter() time.Time {
	return time.Now().Add(time.Second * 5)
}

func (c OklinkBtcClient) GetBlock(height uint64) (*chain.Block, error) {
	curBlock, err := c.getBlock(height)
	if err != nil {
		return nil, err
	}

	preBlock, err := c.getBlock(height - 1)
	if err != nil {
		return nil, err
	}
	if preBlock == nil || curBlock == nil {
		log.Warn(
			"OKLINK MISSED BLOCK", zap.Uint64("height", height), zap.Any("block", curBlock),
			zap.Any("prev", preBlock),
		)
		return nil, errors.New("no such block")
	}

	txs, err := c.GetTransactionsByHeight(height)
	if err != nil {
		return nil, err
	}

	curBlock.ParentHash = preBlock.Hash
	curBlock.Transactions = txs

	return curBlock, nil

}

func (c OklinkBtcClient) getBlock(height uint64) (*chain.Block, error) {
	c.lock.TryLock()
	defer c.lock.Unlock()
	time.Sleep(time.Second)

	url := c.nodeURL + "/api/v5/explorer/block/block-fills"
	params := map[string]string{"chainShortName": c.chainName, "height": strconv.FormatUint(height, 10)}
	headers := map[string]string{"OK-ACCESS-KEY": c.apiKey}

	var resp types.OklinkBlockFillsResp
	timeout := 50 * time.Second
	err := httpclient.HttpsSignGetForm(url, params, headers, &resp, &timeout)
	if err != nil {
		return nil, err
	}

	if resp.Code != "0" || len(resp.Data) == 0 {
		return nil, nil
	}

	curBlock := resp.Data[0]
	hash := curBlock.Hash
	nonce, _ := hexutil.DecodeUint64(curBlock.Nonce)
	blockTime, _ := strconv.ParseInt(curBlock.BlockTime, 10, 64)

	block := &chain.Block{
		Hash:   hash,
		Number: height,
		Nonce:  nonce,
		Time:   blockTime / 1000,
	}

	return block, nil
}

func (c OklinkBtcClient) GetTransactionsByHeight(height uint64) (txs []*chain.Transaction, err error) {
	c.lock.TryLock()
	defer c.lock.Unlock()
	time.Sleep(time.Second)

	url := c.nodeURL + "/api/v5/explorer/transaction/transaction-list"
	params := map[string]string{
		"chainShortName": c.chainName,
		"height":         strconv.FormatUint(height, 10),
		"limit":          "100",
	}
	headers := map[string]string{"OK-ACCESS-KEY": c.apiKey}

	var resp types.OklinkTransactionListResp
	timeout := 5 * time.Second
	page := 1
	platInfo, _ := biz.GetChainPlatInfo(c.chainName)
	decimals := int(platInfo.Decimal)

	for {
		params["page"] = strconv.Itoa(page)
		err := httpclient.HttpsSignGetForm(url, params, headers, &resp, &timeout)
		time.Sleep(time.Second)
		if err != nil {
			break
		}

		data := resp.Data
		if resp.Code != "0" || len(data) == 0 {
			return nil, err
		}

		for _, transaction := range data[0].TransactionList {
			var txInputs []types.Inputs
			inputs := strings.Split(transaction.Input, ",")
			for _, input := range inputs {
				txInputs = append(txInputs, types.Inputs{
					PrevOut: types.PrevOut{
						Addr: input,
					},
				})
			}

			var txOutputs []types.Out
			outputs := strings.Split(transaction.Output, ",")
			for _, output := range outputs {
				amountDecimal, _ := decimal.NewFromString(transaction.Amount)
				amount := biz.Pow10(amountDecimal, decimals).BigInt().Int64()
				txOutputs = append(txOutputs, types.Out{
					Addr:  output,
					Value: int(amount),
				})
			}

			feeDecimal, _ := decimal.NewFromString(transaction.Txfee)
			fee := biz.Pow10(feeDecimal, decimals).BigInt()
			timeInt, _ := strconv.ParseInt(transaction.TransactionTime, 10, 64)
			tx := types.Tx{
				Hash:        transaction.Txid,
				Fee:         int(fee.Int64()),
				DoubleSpend: false,
				Time:        int(timeInt) / 1000,
				BlockIndex:  int(height),
				BlockHeight: int(height),
				Inputs:      txInputs,
				Out:         txOutputs,
			}

			txs = append(txs, &chain.Transaction{
				Hash:        transaction.Txid,
				BlockNumber: height,
				TxType:      "",
				FromAddress: "",
				ToAddress:   "",
				Value:       "",
				Result:      nil,
				Raw:         tx,
				Record:      nil,
			})
		}

		page += 1
		totalPage, _ := strconv.Atoi(data[0].TotalPage)
		if page > totalPage {
			break
		}

	}

	return txs, nil
}

func (c OklinkBtcClient) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	rowTx, err := c.GetTransactionByHash(txHash)
	if err != nil {
		return nil, err
	}

	if rowTx.Hash == "" {
		return nil, nil
	}

	tx = &chain.Transaction{
		Hash:        rowTx.Hash,
		BlockNumber: uint64(rowTx.BlockHeight),
		Raw:         rowTx,
	}

	return tx, nil
}
