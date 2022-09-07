package tron

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"math/big"
	"net/http"
)

type Client struct {
	URL    string
	client *http.Client
}

func NewClient(rawUrl string) Client {
	return Client{URL: rawUrl, client: http.DefaultClient}
}

func (c *Client) GetBalance(address string) (string, error) {
	url := c.URL + "/wallet/getaccount"
	reqBody := types.BalanceReq{
		Address: address,
		Visible: true,
	}
	out := &types.TronBalance{}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	if err != nil {
		return "", err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return "", errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return "", err
			} else {
				return "", errors.New(e)
			}
		}
	}

	balance := utils.BigIntString(new(big.Int).SetInt64(out.Balance), 6)
	return balance, nil
}

func (c *Client) GetTokenBalance(ownerAddress string, contractAddress string, decimal int) (string, error) {
	url := c.URL + "/wallet/triggerconstantcontract"
	addrB := Base58ToHex(ownerAddress)
	parameter := "0000000000000000000000000000000000000000000000000000000000000000"[len(addrB):] + addrB
	out := &types.TronTokenBalanceRes{}
	reqBody := types.TronTokenBalanceReq{
		OwnerAddress:     ownerAddress,
		ContractAddress:  contractAddress,
		FunctionSelector: "balanceOf(address)",
		Parameter:        parameter,
		Visible:          true,
	}
	err := httpclient.HttpsForm(url, http.MethodPost, nil, reqBody, out)
	if err != nil {
		return "0", err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return "0", errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return "0", err
			} else {
				return "0", errors.New(e)
			}
		}
	}

	tokenBalance := "0"
	if len(out.ConstantResult) > 0 {
		banInt, b := new(big.Int).SetString(out.ConstantResult[0], 16)
		if b {
			tokenBalance = utils.BigIntString(banInt, decimal)
		}
	}
	return tokenBalance, err
}

func (c *Client) GetBlockHeight() (int, error) {
	url := c.URL + "/wallet/getnowblock"
	out := &types.NowBlock{}
	err := httpclient.HttpsForm(url, http.MethodGet, nil, nil, out)
	if err != nil {
		return 0, err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return 0, errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return 0, err
			} else {
				return 0, errors.New(e)
			}
		}
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
	if err != nil {
		return nil, err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return nil, errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return nil, err
			} else {
				return nil, errors.New(e)
			}
		}
	}

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
	if err != nil {
		return nil, err
	}
	if out.Error != nil {
		if e, ok := out.Error.(string); ok {
			return nil, errors.New(e)
		} else {
			e, err := utils.JsonEncode(out.Error)
			if err != nil {
				return nil, err
			} else {
				return nil, errors.New(e)
			}
		}
	}

	return out, err
}
