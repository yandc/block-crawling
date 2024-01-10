package data

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types"
	"net/url"
	"strconv"
	"sync"
	"time"
)

var OklinkRepoClient OklinkRepo

type OklinkRepo struct {
	nodeURL string
	apiKey  string
	// Oklink限频 5次/s，加锁保证每次请求之后休息一秒
	lock sync.Mutex
}

func NewOklinkRepo(rpcUrl string) OklinkRepo {
	parsed, err := url.Parse(rpcUrl)
	if err == nil && parsed.User != nil {
		username := parsed.User.Username()
		repo := OklinkRepo{
			nodeURL: parsed.String(),
			apiKey:  username,
		}
		OklinkRepoClient = repo
		return repo
	}

	return OklinkRepo{}
}

func (c *OklinkRepo) GetUtxo(chainName, address string) ([]types.OklinkUTXO, error) {

	c.lock.TryLock()
	defer c.lock.Unlock()
	time.Sleep(time.Second)

	url := c.nodeURL + "/api/v5/explorer/address/utxo"
	headers := map[string]string{"OK-ACCESS-KEY": c.apiKey}
	params := map[string]string{
		"chainShortName": chainName,
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
		time.Sleep(time.Second)
		if err != nil {
			return nil, err
		}

		if resp.Code != "0" || len(resp.Data) == 0 {
			return nil, nil
		}

		utxos = append(utxos, resp.Data[0].UtxoList...)

		page++
		totalPage, _ := strconv.Atoi(resp.Data[0].TotalPage)
		if page > totalPage {
			break
		}
	}

	return utxos, nil
}
