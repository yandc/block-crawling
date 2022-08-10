package tron

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types"
	"net/http"
)

type Client struct {
	URL    string
	client *http.Client
}

func NewClient(rawUrl string) Client {
	return Client{URL: rawUrl, client: http.DefaultClient}
}

func (c *Client) GetBlockHeight() (int, error) {
	url := c.URL + "/wallet/getnowblock"
	out := &types.NowBlock{}
	err := httpclient.HttpsForm(url, http.MethodGet, nil, nil, out)
	if err != nil {
		return 0, err
	}
	return out.BlockHeader.RawData.Number, nil
}

func (c *Client) GetBlockByNum(num int) (*types.BlockResponse, error) {
	url := c.URL + "/wallet/getblockbynum"
	out := &types.BlockResponse{}
	reqBody := types.BlockReq{
		Num:     num,
		Visible: true,
	}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	return out, err
}

func (c *Client) GetTransactionInfoByHash(txHash string) (*types.TronTxInfoResponse, error) {
	url := c.URL + "/wallet/gettransactioninfobyid"
	out := &types.TronTxInfoResponse{}
	reqBody := types.TronTxReq{
		Value:   txHash,
		Visible: true,
	}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	return out, err
}
