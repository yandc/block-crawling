package cosmos

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/utils"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var errNotImplemented = errors.New("Not Implemented")

type Client struct {
	*common.NodeDefaultIn

	url    string
	legacy int32
}

func NewClient(chainName, nodeUrl string, enableProxy bool) Client {
	return Client{
		url: nodeUrl,
		NodeDefaultIn: &common.NodeDefaultIn{
			ChainName:       chainName,
			RoundRobinProxy: enableProxy,
		},
		legacy: 1,
	}
}

type CosmosBadResp struct {
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Details []interface{} `json:"details"`
	Error   interface{}   `json:"error"`
}

type CosmosBalanceResp struct {
	Data struct {
		Coin struct {
			Value string `json:"value"`
		} `json:"coin"`
	} `json:"data"`
	CosmosBadResp
}

func (c *Client) Detect() error {
	log.Info(c.ChainName+"链节点检测", zap.Any("nodeURL", c.url))
	_, err := c.GetBlockNumber()
	return err
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) GetBlockHeight() (uint64, error) {
	height, err := c.GetBlockNumber()
	if err != nil {
		return 0, err
	}
	return uint64(height), nil
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	block, err := c.GetBlockByNumber(int(height))
	if err != nil {
		return nil, err
	}

	txs := make([]*chain.Transaction, 0, len(block.Block.Data.Txs))
	for _, rawTx := range block.Block.Data.Txs {
		textb, _ := base64.StdEncoding.DecodeString(rawTx)
		sha256sum := sha256.Sum256(textb)
		hash := hex.EncodeToString(sha256sum[:])
		hash = strings.ToUpper(hash)

		txs = append(txs, &chain.Transaction{
			Hash:        hash,
			BlockNumber: height,
		})
	}

	return &chain.Block{
		Hash:         block.BlockId.Hash,
		ParentHash:   block.Block.Header.LastBlockId.Hash,
		Number:       height,
		Time:         block.Block.Header.Time.Unix(),
		Raw:          block,
		Transactions: txs,
	}, nil
}

func (c *Client) GetTxByHash(txHash string) (*chain.Transaction, error) {
	rawTx, err := c.GetTransactionByHash(txHash)
	if err != nil {
		log.Error("get transaction by hash error", zap.String("chainName", c.ChainName), zap.String("txHash", txHash), zap.String("nodeUrl", c.URL()), zap.Any("error", err))
		return nil, err
	}
	if rawTx.Message != "" {
		if strings.Contains(rawTx.Message, "not found") || strings.Contains(rawTx.Message, "transaction indexing is disabled") {
			return nil, common.TransactionNotFound // retry on next node
		}
		return nil, errors.New(rawTx.Message)
	}
	if len(rawTx.Tx.AuthInfo.SignerInfos) == 0 {
		return nil, errors.New("SignerInfos is empty, txHash " + txHash)
	}
	height, _ := strconv.Atoi(rawTx.TxResponse.Height)
	nonce, _ := strconv.Atoi(rawTx.Tx.AuthInfo.SignerInfos[0].Sequence)
	return &chain.Transaction{
		BlockNumber: uint64(height),
		Hash:        txHash,
		Nonce:       uint64(nonce),
		TxType:      "",
		FromAddress: "",
		ToAddress:   "",
		Value:       "",
		Raw:         rawTx,
		Record:      nil,
	}, nil
}

type Balance struct {
	Balances []struct {
		Denom  string `json:"denom"`
		Amount string `json:"amount"`
	} `json:"balances"`
	Pagination struct {
		NextKey interface{} `json:"next_key"`
		Total   string      `json:"total"`
	} `json:"pagination"`
}

func (c *Client) GetBalance(address string) (string, error) {
	u, err := c.buildURL("/cosmos/bank/v1beta1/balances/"+address+"?pagination.limit=1000", nil)
	if err != nil {
		return "0", err
	}
	var chain Balance
	err = c.getResponse(u, &chain)
	if err != nil {
		return "0", err
	}
	var decimal int32
	var denom string
	if platInfo, ok := biz.GetChainPlatInfo(c.ChainName); ok {
		decimal = platInfo.Decimal
		denom = strings.ToLower(platInfo.NativeCurrency)
	} else {
		return "0", nil
	}
	for _, balanceInfo := range chain.Balances {
		subTokenDenom := balanceInfo.Denom[1:]
		if subTokenDenom == denom {
			amount := balanceInfo.Amount
			balance := utils.StringDecimals(amount, int(decimal))
			return balance, nil
		}
	}
	return "0", nil
}

func (c *Client) GetTokenBalance(address, tokenAddress string, decimals int) (string, error) {
	u, err := c.buildURL("/cosmos/bank/v1beta1/balances/"+address+"?pagination.offset=0&pagination.limit=1000&pagination.count_total=true", nil)
	if err != nil {
		return "0", err
	}
	var chain Balance
	err = c.getResponse(u, &chain)
	if err != nil {
		return "0", err
	}
	for _, balanceInfo := range chain.Balances {
		if balanceInfo.Denom == tokenAddress {
			amount := balanceInfo.Amount
			balance := utils.StringDecimals(amount, decimals)
			return balance, nil
		}
	}
	return "0", nil
}

type OwnerOf struct {
	Data struct {
		Owner     string        `json:"owner"`
		Approvals []interface{} `json:"approvals"`
	} `json:"data"`
}

func (c *Client) Erc721BalanceByTokenId(address string, tokenAddress string, tokenId string) (string, error) {
	ownerOfMap := map[string]map[string]string{"owner_of": {"token_id": tokenId}}
	ownerOfBase64, err := utils.GetBase64String(ownerOfMap)
	if err != nil {
		return "0", err
	}

	u, err := c.buildURL("/cosmwasm/wasm/v1/contract/"+tokenAddress+"/smart/"+ownerOfBase64, nil)
	if err != nil {
		return "0", err
	}
	var out OwnerOf
	err = c.getResponse(u, &out)
	if err != nil {
		return "0", err
	}
	ownerAddress := out.Data.Owner
	if address == ownerAddress {
		return "1", nil
	}
	return "0", nil
}

type Blockchain struct {
	Height string `json:"height"`
	Result struct {
		NotBondedTokens string `json:"not_bonded_tokens"`
		BondedTokens    string `json:"bonded_tokens"`
	} `json:"result"`
	CosmosBadResp
}

type LatestBlock struct {
	Block struct {
		Header struct {
			Height string `json:"height"`
		} `json:"header"`
	} `json:"block"`
	CosmosBadResp
}

func (c *Client) setNotLegacy() {
	atomic.CompareAndSwapInt32(&c.legacy, 1, 0)
}

func (c *Client) isLegacy() bool {
	return atomic.LoadInt32(&c.legacy) == 1
}

func (c *Client) GetBlockNumber() (int, error) {
	if c.isLegacy() {
		r, err := c.getLegacyBlockNumber()
		if err != errNotImplemented {
			return r, err
		}
		c.setNotLegacy()
	}

	return c.getBlockNumber()
}

func (c *Client) getLegacyBlockNumber() (int, error) {
	u, err := c.buildURL("/staking/pool", nil)
	if err != nil {
		return 0, err
	}
	var chain Blockchain
	err = c.getResponse(u, &chain)
	if err != nil {
		return 0, err
	}
	if chain.Message != "" {
		if chain.Message == errNotImplemented.Error() {
			return 0, errNotImplemented
		}
		return 0, errors.New(chain.Message)
	}
	if chain.Error != nil {
		return 0, errors.New(utils.GetString(chain.Error))
	}
	height, err := strconv.Atoi(chain.Height)
	if err != nil {
		return 0, err
	}
	return height, err
}

func (c *Client) getBlockNumber() (int, error) {
	u, err := c.buildURL("/cosmos/base/tendermint/v1beta1/blocks/latest", nil)
	if err != nil {
		return 0, err
	}

	var chain LatestBlock
	err = c.getResponse(u, &chain)
	if err != nil {
		return 0, err
	}
	if chain.Message != "" {
		return 0, errors.New(chain.Message)
	}
	if chain.Error != nil {
		return 0, errors.New(utils.GetString(chain.Error))
	}
	height, err := strconv.Atoi(chain.Block.Header.Height)
	if err != nil {
		return 0, err
	}
	return height, err
}

type BlockerInfo struct {
	BlockId struct {
		Hash  string `json:"hash"`
		Parts struct {
			Total int    `json:"total"`
			Hash  string `json:"hash"`
		} `json:"parts"`
	} `json:"block_id"`
	Block struct {
		Header struct {
			Version struct {
				Block string `json:"block"`
			} `json:"version"`
			ChainId     string    `json:"chain_id"`
			Height      string    `json:"height"`
			Time        time.Time `json:"time"`
			LastBlockId struct {
				Hash  string `json:"hash"`
				Parts struct {
					Total int    `json:"total"`
					Hash  string `json:"hash"`
				} `json:"parts"`
			} `json:"last_block_id"`
			LastCommitHash     string `json:"last_commit_hash"`
			DataHash           string `json:"data_hash"`
			ValidatorsHash     string `json:"validators_hash"`
			NextValidatorsHash string `json:"next_validators_hash"`
			ConsensusHash      string `json:"consensus_hash"`
			AppHash            string `json:"app_hash"`
			LastResultsHash    string `json:"last_results_hash"`
			EvidenceHash       string `json:"evidence_hash"`
			ProposerAddress    string `json:"proposer_address"`
		} `json:"header"`
		Data struct {
			Txs []string `json:"txs"`
		} `json:"data"`
		/*Evidence struct {
			Evidence []interface{} `json:"evidence"`
		} `json:"evidence"`*/
		// Evidence   interface{} `json:"evidence"`
		/*
			LastCommit struct {
				Height  string `json:"height"`
				Round   int    `json:"round"`
				BlockId struct {
					Hash  string `json:"hash"`
					Parts struct {
						Total int    `json:"total"`
						Hash  string `json:"hash"`
					} `json:"parts"`
				} `json:"block_id"`
					Signatures []struct {
						BlockIdFlag      int       `json:"block_id_flag"`
						ValidatorAddress string    `json:"validator_address"`
						Timestamp        time.Time `json:"timestamp"`
						Signature        string    `json:"signature"`
					} `json:"signatures"`
			} `json:"last_commit"`
		*/
	} `json:"block"`
	CosmosBadResp
}

func (c *Client) GetBlockByNumber(number int) (tx BlockerInfo, err error) {
	if c.isLegacy() {
		tx, err := c.getLegacyBlockByNumber(number)
		if err == nil && tx.Message != "" {
			if tx.Message == errNotImplemented.Error() {
				err = errNotImplemented
			} else {
				err = errors.New(tx.Message)
			}
		}
		if err != errNotImplemented {
			return tx, err
		}
		c.setNotLegacy()
	}
	return c.getBlockByNumber(number)
}

func (c *Client) getLegacyBlockByNumber(number int) (tx BlockerInfo, err error) {
	u, err := c.buildURL("/blocks/"+strconv.Itoa(number), nil)
	if err != nil {
		return
	}
	err = c.getResponse(u, &tx)
	return tx, err
}

func (c *Client) getBlockByNumber(number int) (tx BlockerInfo, err error) {
	u, err := c.buildURL("/cosmos/base/tendermint/v1beta1/blocks/"+strconv.Itoa(number), nil)
	if err != nil {
		return
	}
	err = c.getResponse(u, &tx)
	return tx, err
}

type TransactionInfo struct {
	Tx struct {
		Body struct {
			Messages []interface{} `json:"messages"`
			/*Messages []struct {
				Type        string `json:"@type"`
				FromAddress string `json:"from_address"`
				ToAddress   string `json:"to_address"`
				Amount      []struct {
					Denom  string `json:"denom"`
					Amount string `json:"amount"`
				} `json:"amount"`

				SourcePort    string `json:"source_port"`
				SourceChannel string `json:"source_channel"`
				Token         struct {
					Denom  string `json:"denom"`
					Amount string `json:"amount"`
				} `json:"token"`
				Sender        string `json:"sender"`
				Receiver      string `json:"receiver"`
				TimeoutHeight struct {
					RevisionNumber string `json:"revision_number"`
					RevisionHeight string `json:"revision_height"`
				} `json:"timeout_height"`
				TimeoutTimestamp string `json:"timeout_timestamp"`

				DelegatorAddress    string `json:"delegator_address"`
				ValidatorAddress    string `json:"validator_address"`
				ValidatorSrcAddress string `json:"validator_src_address"`
				ValidatorDstAddress string `json:"validator_dst_address"`

				Granter string `json:"granter"`
				Grantee string `json:"grantee"`
				Grant   struct {
					Authorization struct {
						Type      string      `json:"@type"`
						MaxTokens interface{} `json:"max_tokens"`
						AllowList struct {
							Address []string `json:"address"`
						} `json:"allow_list"`
						AuthorizationType string `json:"authorization_type"`
					} `json:"authorization"`
					Expiration time.Time `json:"expiration"`
				} `json:"grant"`
			} `json:"messages"`*/
			Memo                        string        `json:"memo"`
			TimeoutHeight               string        `json:"timeout_height"`
			ExtensionOptions            []interface{} `json:"extension_options"`
			NonCriticalExtensionOptions []interface{} `json:"non_critical_extension_options"`
		} `json:"body"`
		AuthInfo struct {
			SignerInfos []struct {
				PublicKey struct {
					Type string `json:"@type"`
					Key  string `json:"key"`
				} `json:"public_key"`
				ModeInfo struct {
					Single struct {
						Mode string `json:"mode"`
					} `json:"single"`
				} `json:"mode_info"`
				Sequence string `json:"sequence"`
			} `json:"signer_infos"`
			Fee struct {
				Amount []struct {
					Denom  string `json:"denom"`
					Amount string `json:"amount"`
				} `json:"amount"`
				GasLimit string `json:"gas_limit"`
				Payer    string `json:"payer"`
				Granter  string `json:"granter"`
			} `json:"fee"`
		} `json:"auth_info"`
		Signatures []string `json:"signatures"`
	} `json:"tx"`
	TxResponse struct {
		Height    string `json:"height"`
		Txhash    string `json:"txhash"`
		Codespace string `json:"codespace"`
		Code      int    `json:"code"`
		Data      string `json:"data"`
		RawLog    string `json:"raw_log"`
		Logs      []struct {
			MsgIndex int    `json:"msg_index"`
			Log      string `json:"log"`
			Events   []struct {
				Type       string `json:"type"`
				Attributes []struct {
					Key   string `json:"key"`
					Value string `json:"value"`
				} `json:"attributes"`
			} `json:"events"`
		} `json:"logs"`
		Info      string `json:"info"`
		GasWanted string `json:"gas_wanted"`
		GasUsed   string `json:"gas_used"`
		Tx        struct {
			Type string `json:"@type"`
			Body struct {
				Messages                    []interface{} `json:"messages"`
				Memo                        string        `json:"memo"`
				TimeoutHeight               string        `json:"timeout_height"`
				ExtensionOptions            []interface{} `json:"extension_options"`
				NonCriticalExtensionOptions []interface{} `json:"non_critical_extension_options"`
			} `json:"body"`
			AuthInfo struct {
				SignerInfos []struct {
					PublicKey struct {
						Type string `json:"@type"`
						Key  string `json:"key"`
					} `json:"public_key"`
					ModeInfo struct {
						Single struct {
							Mode string `json:"mode"`
						} `json:"single"`
					} `json:"mode_info"`
					Sequence string `json:"sequence"`
				} `json:"signer_infos"`
				Fee struct {
					Amount []struct {
						Denom  string `json:"denom"`
						Amount string `json:"amount"`
					} `json:"amount"`
					GasLimit string `json:"gas_limit"`
					Payer    string `json:"payer"`
					Granter  string `json:"granter"`
				} `json:"fee"`
			} `json:"auth_info"`
			Signatures []string `json:"signatures"`
		} `json:"tx"`
		Timestamp time.Time `json:"timestamp"`
		Events    []struct {
			Type       string `json:"type"`
			Attributes []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
				Index bool   `json:"index"`
			} `json:"attributes"`
		} `json:"events"`
	} `json:"tx_response"`
	CosmosBadResp
}

func (c *Client) GetTransactionByHash(hash string) (tx TransactionInfo, err error) {
	u, err := c.buildURL("/cosmos/tx/v1beta1/txs/"+hash, nil)
	if err != nil {
		return
	}
	err = c.getResponse(u, &tx)
	if err != nil {
		return
	}
	if tx.Error != nil {
		return tx, errors.New(utils.GetString(tx.Error))
	}
	return tx, err
}

type NftHistories struct {
	Data []struct {
		Id         int `json:"id"`
		Attributes struct {
			CreatedAt    time.Time   `json:"createdAt"`
			Status       int         `json:"status"`
			UserId       int         `json:"user_id"`
			CollectionId int         `json:"collection_id"`
			TokenId      interface{} `json:"token_id"`
			Price        string      `json:"price"`
			Address      string      `json:"address"`
			TnxHash      interface{} `json:"tnx_hash"`
			UpdatedAt    time.Time   `json:"updatedAt"`
			User         struct {
				Data struct {
					Id         int `json:"id"`
					Attributes struct {
						Username          string      `json:"username"`
						Email             string      `json:"email"`
						Provider          interface{} `json:"provider"`
						Confirmed         bool        `json:"confirmed"`
						Blocked           bool        `json:"blocked"`
						CreatedAt         time.Time   `json:"createdAt"`
						UpdatedAt         time.Time   `json:"updatedAt"`
						WalletAddress     string      `json:"wallet_address"`
						Bio               interface{} `json:"bio"`
						TwitterId         interface{} `json:"twitter_id"`
						TwitterScreenName *string     `json:"twitter_screen_name"`
						IsFollowTwitter   bool        `json:"is_follow_twitter"`
						Discord           interface{} `json:"discord"`
						IsJoinDiscord     bool        `json:"is_join_discord"`
						DeviceToken       interface{} `json:"device_token"`
						Avatar            interface{} `json:"avatar"`
						Cover             interface{} `json:"cover"`
						IsVip             bool        `json:"is_vip"`
						IsVipExpired      interface{} `json:"is_vip_expired"`
						AvatarRarity      interface{} `json:"avatar_rarity"`
						AvatarNft         interface{} `json:"avatar_nft"`
						PublicKey         string      `json:"public_key"`
					} `json:"attributes"`
				} `json:"data"`
			} `json:"user"`
		} `json:"attributes"`
	} `json:"data"`
	Meta struct {
		Pagination struct {
			Page      int `json:"page"`
			PageSize  int `json:"pageSize"`
			PageCount int `json:"pageCount"`
			Total     int `json:"total"`
		} `json:"pagination"`
	} `json:"meta"`
}

func (c *Client) GetNftHistories(tokenAddress string, tokenId string, pageNum int, pageSize int) (*NftHistories, error) {
	url := "https://sei-api-testnet.bluemove.net/api/market-item-histories?filters[market_item][collection_address][$eq]=" + tokenAddress + "&filters[market_item][token_id][$eq]=" + tokenId + "&sort[0]=createdAt%3Adesc&pagination[page]=" + strconv.Itoa(pageNum) + "&pagination[pageSize]=" + strconv.Itoa(pageSize) + "&populate[user][populate][0]=%2A"
	out := &NftHistories{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.GetResponse(url, nil, out, &timeoutMS)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// constructs BlockCypher URLs with parameters for requests
func (c *Client) buildURL(u string, params map[string]string) (target *url.URL, err error) {
	target, err = url.Parse(c.url + u)
	if err != nil {
		return
	}
	values := target.Query()
	//Set parameters
	for k, v := range params {
		values.Set(k, v)
	}
	//add token to url, if present

	target.RawQuery = values.Encode()
	return
}

// getResponse is a boilerplate for HTTP GET responses.
func (c *Client) getResponse(target *url.URL, decTarget interface{}) (err error) {
	timeoutMS := 10_000 * time.Millisecond
	if c.ChainName == "Osmosis" {
		err = httpclient.GetUseCloudscraper(target.String(), &decTarget, &timeoutMS)
	} else {
		var statusCode int
		statusCode, err = httpclient.GetStatusCode(target.String(), nil, &decTarget, &timeoutMS, c.ProxyTransport(nil))
		if statusCode == 429 && biz.IsTestNet(c.ChainName) {
			// on test we only sleep for 3 seconds when we meet 429
			c.SetRetryAfter(time.Second * 3)
		}
	}
	return
}
