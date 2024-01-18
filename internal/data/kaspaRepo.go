package data

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types"
	"fmt"
	"time"
)

var KaspaRepoClient KaspaRepo

type KaspaRepo struct {
	nodeURL string
}

func NewKaspaRepoClient(rpcUrl string) KaspaRepo {
	repo := KaspaRepo{
		nodeURL: rpcUrl,
	}
	KaspaRepoClient = repo
	return repo
}

func (c *KaspaRepo) GetUtxo(chainName, address string) ([]types.KaspaUtxoResp, error) {
	url := fmt.Sprintf("%s/addresses/%s/utxos", c.nodeURL, address)
	out := []types.KaspaUtxoResp{}
	timeoutMS := 20_000 * time.Millisecond
	err := httpclient.GetResponse(url, nil, &out, &timeoutMS)
	if err != nil {
		return nil, err
	}
	return out, err
}
