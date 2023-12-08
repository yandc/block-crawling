package btc

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"math/big"
	"time"

	"github.com/blockcypher/gobcy"
	"go.uber.org/zap"
)

type blockdaemonNode struct {
	nodeURL string
	key     string
	baseURL string
}

func newBlockdaemonNode(nodeURL string) *blockdaemonNode {
	key, baseURL := parseKeyFromNodeURL(nodeURL)
	return &blockdaemonNode{
		nodeURL: nodeURL,
		key:     key,
		baseURL: baseURL,
	}
}

func (n *blockdaemonNode) GetBalance(address string) (string, error) {
	timeout := 5 * time.Second
	header := map[string]string{"Authorization": n.key}
	u, err := base.BuildURL("/account/"+address, n.baseURL, nil)
	if err != nil {
		return "", err
	}
	url := u.String()
	var balances []*blockdaemonBalanceItem
	err = httpclient.HttpsSignGetForm(url, nil, header, &balances, &timeout)
	if err != nil {
		log.Error("BTC BALANCE", zap.Error(err), zap.String("url", url))
		return "", err
	}
	for _, item := range balances {
		if item.Currency.AssetPath == "bitcoin/native/btc" {
			return utils.StringDecimals(item.ConfirmedBalance, 8), nil
		}
	}
	return "", errors.New("native currency missed")
}

type blockdaemonBalanceItem struct {
	ConfirmedBalance string                     `json:"confirmed_balance"`
	PendingBalance   string                     `json:"pending_balance"`
	ConfirmedBlock   int                        `json:"confirmed_block"`
	Currency         blockdaemonBalanceCurrency `json:"currency"`
}

type blockdaemonBalanceCurrency struct {
	AssetPath string `json:"asset_path"`
	Symbol    string `json:"symbol"`
	Name      string `json:"name"`
	Decimals  int    `json:"decimals"`
	Type      string `json:"native"`
}

func (n *blockdaemonNode) GetBlockNumber() (int, error) {
	timeout := 5 * time.Second
	var height int
	header := map[string]string{"Authorization": n.key}
	u, err := base.BuildURL("/sync/block_number", n.baseURL, nil)
	if err != nil {
		return 0, err
	}
	url := u.String()
	err = httpclient.HttpsSignGetForm(url, nil, header, &height, &timeout)
	if err != nil {
		log.Error("BTC BLOCK HEIGHT", zap.Error(err), zap.String("url", url))
		return 0, err
	}

	log.Info("BTC BLOCK HEIGHT", zap.String("url", n.baseURL), zap.Any("r", height))
	return height, nil
}

func (n *blockdaemonNode) GetTransactionByHash(hash string) (tx types.TX, err error) {
	utxoTxByDD, e := n.getTransactionsByTXHash(hash)
	if e != nil {
		err = e
		return
	}
	if utxoTxByDD.Detail == "The requested resource has not been found" {
		return tx, errors.New(utxoTxByDD.Detail)
	}
	var inputs []gobcy.TXInput
	var inputAddress []string
	var outs []gobcy.TXOutput
	var outputAddress []string

	var feeAmount int64
	for _, event := range utxoTxByDD.Events {
		if event.Type == FROM_TYPE {
			input := gobcy.TXInput{
				OutputValue: int(event.Amount),
				Addresses:   append(inputAddress, event.Source),
			}
			inputs = append(inputs, input)
		}
		if event.Type == TO_TYPE {
			out := gobcy.TXOutput{
				Value:     *big.NewInt(event.Amount),
				Addresses: append(outputAddress, event.Destination),
			}
			outs = append(outs, out)
		}
		if event.Type == FEE_TYPE {
			feeAmount = event.Amount
		}
	}

	txTime := time.Unix(int64(utxoTxByDD.Date), 0)
	tx = types.TX{
		BlockHash:   utxoTxByDD.BlockId,
		BlockHeight: utxoTxByDD.BlockNumber,
		Hash:        utxoTxByDD.Id,
		Fees:        *big.NewInt(feeAmount),
		Confirmed:   txTime,
		VinSize:     len(inputs),
		VoutSize:    len(outs),
		Inputs:      inputs,
		Outputs:     outs,
		Error:       "",
	}
	return tx, nil
}

func (n *blockdaemonNode) getTransactionsByTXHash(tx string) (types.TxInfo, error) {
	u, err := base.BuildURL("/tx/"+tx, n.baseURL, nil)
	if err != nil {
		return types.TxInfo{}, err
	}
	url := u.String()
	var txInfo types.TxInfo
	timeoutMS := 10_000 * time.Millisecond
	err = httpclient.HttpsSignGetForm(url, nil, map[string]string{"Authorization": n.key}, &txInfo, &timeoutMS)
	return txInfo, err
}
