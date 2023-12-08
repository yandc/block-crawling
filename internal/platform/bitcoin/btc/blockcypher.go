package btc

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/bitcoin/base"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"strconv"
	"time"

	"github.com/blockcypher/gobcy"
	"go.uber.org/zap"
)

type blockcypherNode struct {
	nodeURL string
}

func newBlockcypherNode(nodeURL string) *blockcypherNode {
	return &blockcypherNode{
		nodeURL: nodeURL,
	}
}

func (node *blockcypherNode) GetBalance(address string) (string, error) {
	u, err := base.BuildURLBTC("/addrs/"+address+"/balance", node.nodeURL, nil)
	if err != nil {
		return "", err
	}
	var addr gobcy.Addr
	timeoutMS := 5_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &addr, &timeoutMS)
	if err != nil {
		return "", err
	}
	btcValue := utils.BigIntString(&addr.Balance, 8)
	return btcValue, nil
}

func (node *blockcypherNode) GetBlockNumber() (int, error) {
	timeout := 5 * time.Second
	u, err := base.BuildURL("", node.nodeURL, nil)
	if err != nil {
		return 0, err
	}
	var chain gobcy.Blockchain
	err = httpclient.GetResponse(u.String(), nil, &chain, &timeout)
	if err != nil {
		log.Error("BTC BLOCK HEIGHT", zap.Error(err), zap.String("url", u.String()), zap.Any("r", chain))
		return 0, err
	}
	height := chain.Height
	log.Info("BTC BLOCK HEIGHT", zap.String("url", node.nodeURL), zap.Any("r", height))
	return height, nil
}

func (node *blockcypherNode) GetTransactionByHash(hash string) (tx types.TX, err error) {
	tx, err = node.doGetTransactionByHash(hash + "?instart=0&outstart=0&limit=500")
	if err != nil {
		return
	}
	putsTx := tx
	for (putsTx.NextInputs != "" && len(putsTx.Inputs) > 80) || (putsTx.NextOutputs != "" && len(putsTx.Outputs) > 80) {
		putsTx, err = node.doGetTransactionByHash(hash + "?instart=" + strconv.Itoa(len(putsTx.Inputs)) + "&outstart=" + strconv.Itoa(len(putsTx.Outputs)) + "&limit=500")
		if err != nil {
			return
		}
		for _, input := range putsTx.Inputs {
			tx.Inputs = append(tx.Inputs, input)
		}
		for _, output := range putsTx.Outputs {
			tx.Outputs = append(tx.Outputs, output)
		}
	}
	return
}

func (node *blockcypherNode) doGetTransactionByHash(hash string) (tx types.TX, err error) {
	u, err := base.BuildURL("/txs/"+hash, node.nodeURL, nil)
	if err != nil {
		return
	}
	timeoutMS := 10_000 * time.Millisecond
	err = httpclient.GetResponse(u.String(), nil, &tx, &timeoutMS)
	return
}
