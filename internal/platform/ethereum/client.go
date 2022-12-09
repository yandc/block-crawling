package ethereum

import (
	"block-crawling/internal/biz"
	icommon "block-crawling/internal/common"
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/hex"
	"errors"
	"github.com/metachris/eth-go-bindings/erc1155"
	"github.com/metachris/eth-go-bindings/erc721"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/metachris/eth-go-bindings/erc20"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type Client struct {
	*ethclient.Client
	c *rpc.Client

	*pcommon.NodeDefaultIn

	url       string
	chainName string
}

func NewClient(rawUrl string, chainName string) (*Client, error) {
	c, err := rpc.DialContext(context.Background(), rawUrl)
	if err != nil {
		return nil, err
	}
	client := ethclient.NewClient(c)

	if err != nil {
		log.Error("new client error:", zap.Error(err), zap.Any("url", rawUrl))
		return &Client{}, err
	}
	return &Client{
		Client: client,
		c:      c,
		NodeDefaultIn: &pcommon.NodeDefaultIn{
			ChainName: chainName,
		},
		url:       rawUrl,
		chainName: chainName,
	}, nil
}

func (c *Client) Detect() error {
	_, err := c.GetBlockNumber(context.Background())
	return err
}

func (c *Client) URL() string {
	return c.url
}

func (c *Client) GetBlockHeight() (uint64, error) {
	start := time.Now()
	height, err := c.GetBlockNumber(context.Background())
	if err != nil {
		return 0, err
	}
	log.Debug(
		"RETRIEVED CHAIN HEIGHT FROM NODE",
		zap.Uint64("height", height),
		zap.String("nodeUrl", c.url),
		zap.String("chainName", c.chainName),
		zap.String("elapsed", time.Now().Sub(start).String()),
	)
	return height, nil
}

func (c *Client) GetBlock(height uint64) (*chain.Block, error) {
	start := time.Now()

	block, err := c.GetBlockByNumber(context.Background(), big.NewInt(int64(height)))
	if err != nil {
		if err == ethereum.NotFound /* && isNonstandardEVM(c.chainName)*/ {
			return nil, errors.New("block not found") // retry on next node
		}

		log.Debug(
			"RETRIEVED BLOCK FROM CHAIN FAILED WITH ERROR",
			zap.String("chainName", c.chainName),
			zap.Uint64("height", height),
			zap.String("nodeUrl", c.url),
			zap.String("elapsed", time.Now().Sub(start).String()),
			zap.Error(err),
		)
		return nil, err
	}
	txs := make([]*chain.Transaction, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		txc := &chain.Transaction{
			Hash:        tx.Hash().String(),
			Nonce:       tx.Nonce(),
			BlockNumber: height,
			Raw:         tx,
		}
		c.parseTxMeta(txc, tx)
		txs = append(txs, txc)
	}
	var baseFee string
	if block.BaseFee() != nil {
		baseFee = block.BaseFee().String()
	}
	return &chain.Block{
		Hash:         block.Hash().Hex(),
		ParentHash:   block.ParentHash().Hex(),
		Number:       block.NumberU64(),
		Nonce:        block.Nonce(),
		BaseFee:      baseFee,
		Time:         int64(block.Time()),
		Transactions: txs,
	}, nil
}

func (c *Client) parseTxMeta(txc *chain.Transaction, tx *Transaction) (err error) {
	var from *common.Address
	var toAddress string
	from = tx.From
	if tx.To() != nil {
		toAddress = tx.To().String()
	}
	fromAddress := from.String()
	value := tx.Value().String()
	transactionType := biz.NATIVE
	data := tx.Data()
	if len(data) >= 68 && tx.To() != nil {
		methodId := hex.EncodeToString(data[:4])
		if methodId == "a9059cbb" || methodId == "095ea7b3" || methodId == "a22cb465" {
			toAddress = common.HexToAddress(hex.EncodeToString(data[4:36])).String()
			amount := new(big.Int).SetBytes(data[36:])
			if methodId == "a9059cbb" { // ERC20
				transactionType = biz.TRANSFER
			} else if methodId == "095ea7b3" { // ERC20 or ERC721
				transactionType = biz.APPROVE
			} else if methodId == "a22cb465" { // ERC721 or ERC1155
				transactionType = biz.SETAPPROVALFORALL
			}
			value = amount.String()
		} else if methodId == "23b872dd" { // ERC20 or ERC721
			//transferFrom(address sender, address recipient, uint256 amount)
			//transferFrom(address from, address to, uint256 tokenId)
			fromAddress = common.HexToAddress(hex.EncodeToString(data[4:36])).String()
			toAddress = common.HexToAddress(hex.EncodeToString(data[36:68])).String()
			var amountOrTokenId *big.Int
			if len(data) > 68 {
				amountOrTokenId = new(big.Int).SetBytes(data[68:])
			} else {
				amountOrTokenId = new(big.Int)
			}
			transactionType = biz.TRANSFERFROM
			value = amountOrTokenId.String()
		} else if methodId == "42842e0e" { // ERC721
			//safeTransferFrom(address from, address to, uint256 tokenId)
			fromAddress = common.HexToAddress(hex.EncodeToString(data[4:36])).String()
			toAddress = common.HexToAddress(hex.EncodeToString(data[36:68])).String()
			var tokenId *big.Int
			if len(data) > 68 {
				tokenId = new(big.Int).SetBytes(data[68:])
			} else {
				tokenId = new(big.Int)
			}
			transactionType = biz.SAFETRANSFERFROM
			value = tokenId.String()
		} else if methodId == "b88d4fde" { // ERC721
			//safeTransferFrom(address from, address to, uint256 tokenId, bytes _data)
			fromAddress = common.HexToAddress(hex.EncodeToString(data[4:36])).String()
			toAddress = common.HexToAddress(hex.EncodeToString(data[36:68])).String()
			var tokenId *big.Int
			if len(data) > 68 {
				if len(data) <= 100 {
					tokenId = new(big.Int).SetBytes(data[68:])
				} else {
					tokenId = new(big.Int).SetBytes(data[68:100])
				}
			} else {
				tokenId = new(big.Int)
			}
			transactionType = biz.SAFETRANSFERFROM
			value = tokenId.String()
		} else if methodId == "f242432a" { // ERC1155
			//safeTransferFrom(address from, address to, uint256 id, uint256 amount, bytes data)
			fromAddress = common.HexToAddress(hex.EncodeToString(data[4:36])).String()
			toAddress = common.HexToAddress(hex.EncodeToString(data[36:68])).String()
			var tokenId *big.Int
			if len(data) > 68 {
				if len(data) <= 100 {
					tokenId = new(big.Int).SetBytes(data[68:])
				} else {
					tokenId = new(big.Int).SetBytes(data[68:100])
				}
			} else {
				tokenId = new(big.Int)
			}
			var amount *big.Int
			if len(data) > 100 {
				if len(data) <= 132 {
					amount = new(big.Int).SetBytes(data[100:])
				} else {
					amount = new(big.Int).SetBytes(data[100:132])
				}
			} else {
				amount = new(big.Int)
			}
			transactionType = biz.SAFETRANSFERFROM
			value = tokenId.String() + "," + amount.String()
		} else if methodId == "2eb2c2d6" { // ERC1155
			//safeBatchTransferFrom(address from, address to, uint256[] ids, uint256[] amounts, bytes data)
			fromAddress = common.HexToAddress(hex.EncodeToString(data[4:36])).String()
			toAddress = common.HexToAddress(hex.EncodeToString(data[36:68])).String()
			transactionType = biz.SAFEBATCHTRANSFERFROM
			// TODO
		} else {
			if methodId == "e7acab24" { // Seaport 1.1 Contract
				transactionType = biz.CONTRACT
				if len(data) >= 324 {
					realFromAddress := common.HexToAddress(hex.EncodeToString(data[296:324])).String()
					fromAddress = fromAddress + "," + realFromAddress
				}
				if len(data) >= 132 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[100:132])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if methodId == "fb0f3ee1" { // Seaport 1.1 Contract
				transactionType = biz.CONTRACT
				if len(data) >= 164 {
					realFromAddress := common.HexToAddress(hex.EncodeToString(data[132:164])).String()
					fromAddress = fromAddress + "," + realFromAddress
				}
			} else if methodId == "357a150b" { // X2Y2: Exchange Contract
				transactionType = biz.CONTRACT
				if len(data) >= 516 {
					realFromAddress := common.HexToAddress(hex.EncodeToString(data[484:516])).String()
					fromAddress = fromAddress + "," + realFromAddress
				}
				if len(data) >= 260 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[228:260])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if methodId == "b4e4b296" { // LooksRare: Exchange Contract
				transactionType = biz.CONTRACT
				if len(data) >= 356 {
					realFromAddress := common.HexToAddress(hex.EncodeToString(data[324:356])).String()
					fromAddress = fromAddress + "," + realFromAddress
				}
				if len(data) >= 132 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[100:132])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if methodId == "0175b1c4" { // Multichain: Router V4 Contract
				transactionType = biz.CONTRACT
				if len(data) >= 68 {
					realFromAddress := common.HexToAddress(hex.EncodeToString(data[36:68])).String()
					fromAddress = fromAddress + "," + realFromAddress
				}
				if len(data) >= 100 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[68:100])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if methodId == "5dea8376" { // NFT Contract
				transactionType = biz.CONTRACT
				if len(data) >= 68 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[36:68])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if methodId == "252f7b01" { // Optimism和BSC链 Contract
				transactionType = biz.CONTRACT
				if strings.HasPrefix(c.chainName, "Optimism") {
					if len(data) >= 68 {
						realFromAddress := common.HexToAddress(hex.EncodeToString(data[36:68])).String()
						fromAddress = fromAddress + "," + realFromAddress
					}
					if len(data) >= 389 {
						realToAddress := common.HexToAddress(hex.EncodeToString(data[357:389])).String()
						toAddress = toAddress + "," + realToAddress
					}
				} else if strings.HasPrefix(c.chainName, "BSC") {
					if len(data) >= 790 {
						realToAddress := common.HexToAddress(hex.EncodeToString(data[758:790])).String()
						toAddress = toAddress + "," + realToAddress
					}
				}
			}
		}
	}
	txc.FromAddress = fromAddress
	txc.ToAddress = toAddress
	txc.TxType = chain.TxType(transactionType)
	txc.Value = value
	return nil
}

// 3
func (c *Client) GetBlockNumber(ctx context.Context) (uint64, error) {
	return c.BlockNumber(ctx)
}

func (c *Client) GetBlockByNumber(ctx context.Context, number *big.Int) (*Block, error) {
	return c.BlockByNumber(ctx, number)
}

// GetTxByHash 没有错误的情况下：
//
// 1. 返回 non-nil tx 表示调用 TxHandler.OnSealedTx
// 2. 返回 nil tx 表示调用 TxHandler.OnDroppedTx（兜底方案）
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	txHash = strings.Split(txHash, "#")[0]
	txByHash, _, err := c.GetTransactionByHash(context.Background(), common.HexToHash(txHash))

	if err != nil && err != ethereum.NotFound {
		log.Error(c.chainName+"查询链上数据失败", zap.Any("txHash", txHash), zap.Any("error", err))
		return nil, err
	}

	if txByHash == nil {
		// 兜底
		return nil, nil
	}
	receipt, err := c.GetTransactionReceipt(context.Background(), txByHash.Hash())

	if err != nil && err != ethereum.NotFound {
		log.Error(c.chainName+"查询链上 Receipt 数据失败", zap.Any("txHash", txHash), zap.Any("error", err))
		return nil, err
	}

	if receipt == nil {
		return nil, nil
	}
	intBlockNumber, _ := utils.HexStringToInt(receipt.BlockNumber)
	tx = &chain.Transaction{
		Hash:        txByHash.Hash().Hex(),
		Nonce:       txByHash.Nonce(),
		BlockNumber: intBlockNumber.Uint64(),

		TxType:      "",
		FromAddress: "",
		ToAddress:   "",
		Value:       "",

		Raw:    receipt,
		Record: nil,
	}
	c.parseTxMeta(tx, txByHash)
	return tx, nil
}

// 1
func (c *Client) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*Transaction, bool, error) {
	return c.TransactionByHash(ctx, txHash)
}

// Receipt represents the results of a transaction.
type Receipt struct {
	// Consensus fields: These fields are defined by the Yellow Paper
	Type              string        `json:"type,omitempty"`
	PostState         string        `json:"root,omitempty"`
	Status            string        `json:"status,omitempty"`
	From              string        `json:"from,omitempty"`
	To                string        `json:"to,omitempty"`
	CumulativeGasUsed string        `json:"cumulativeGasUsed"` //`json:"cumulativeGasUsed" gencodec:"required"`
	EffectiveGasPrice string        `json:"effectiveGasPrice,omitempty"`
	Bloom             string        `json:"logsBloom"` //`json:"logsBloom" gencodec:"required"`
	Logs              []*types2.Log `json:"logs" gencodec:"required"`

	// Implementation fields: These fields are added by geth when processing a transaction.
	// They are stored in the chain database.
	TxHash          string `json:"transactionHash" gencodec:"required"`
	ContractAddress string `json:"contractAddress,omitempty"`
	GasUsed         string `json:"gasUsed" gencodec:"required"`

	// Inclusion information: These fields provide information about the inclusion of the
	// transaction corresponding to this receipt.
	BlockHash        string `json:"blockHash,omitempty"`
	BlockNumber      string `json:"blockNumber,omitempty"`
	TransactionIndex string `json:"transactionIndex,omitempty"`
}

// 2
func (c *Client) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*Receipt, error) {
	//return c.TransactionReceipt(ctx, txHash)
	var r *Receipt
	rpcClient, err := rpc.DialHTTP(c.url)
	err = rpcClient.CallContext(ctx, &r, "eth_getTransactionReceipt", txHash)
	if err == nil {
		if r == nil {
			return nil, ethereum.NotFound
		}
	}
	return r, err
}

// client.TransactionSender(ctx,tx,block.Hash(),r.TransactionIndex)
func (c *Client) GetTransactionSender(ctx context.Context, tx *types2.Transaction, block common.Hash,
	index uint) (common.Address, error) {
	return c.TransactionSender(ctx, tx, block, index)
}

func (c *Client) GetBalance(address string) (string, error) {
	account := common.HexToAddress(address)
	balance, err := c.BalanceAt(context.Background(), account, nil)
	if err != nil {
		return "", err
	}
	ethValue := utils.BigIntString(balance, 18)
	return ethValue, err
}

func (c *Client) BatchTokenBalance(address string, tokenMap map[string]int) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	destAddress := common.HexToAddress(address)
	balanceFun := []byte("balanceOf(address)")
	hash := crypto.NewKeccakState()
	hash.Write(balanceFun)
	methodID := hash.Sum(nil)[:4]
	rpcClient, err := rpc.DialHTTP(c.url)
	if err != nil {
		return result, err
	}
	var tokenAddrs []string
	var be []rpc.BatchElem
	for token, _ := range tokenMap {
		var data []byte
		data = append(data, methodID...)
		tokenAddress := common.HexToAddress(token)
		data = append(data, common.LeftPadBytes(destAddress.Bytes(), 32)...)
		callMsg := map[string]interface{}{
			"from": destAddress,
			"to":   tokenAddress,
			"data": hexutil.Bytes(data),
		}
		be = append(be, rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{callMsg, "latest"},
			Result: new(string),
		})
		tokenAddrs = append(tokenAddrs, token)
	}
	err = rpcClient.BatchCall(be)
	if err != nil {
		return result, err
	}
	for index, b := range be {
		token := tokenAddrs[index]
		hexAmount := b.Result.(*string)
		newHexAmount := *hexAmount
		if len(newHexAmount) > 66 {
			newHexAmount = newHexAmount[0:66]
		}
		bi := new(big.Int).SetBytes(common.FromHex(newHexAmount))
		var balance string
		if tokenMap[token] == 0 {
			balance = bi.String()
		} else {
			balance = utils.BigIntString(bi, tokenMap[token])
		}
		result[token] = balance
	}
	return result, nil
}

func (c *Client) Erc721Balance(address string, tokenAddress string, tokenId string) (string, error) {
	hexTokenAddress := common.HexToAddress(tokenAddress)
	erc721Token, err := erc721.NewErc721(hexTokenAddress, c)
	if err != nil {
		return "", err
	}
	tokenIdBig, ok := new(big.Int).SetString(tokenId, 0)
	if !ok {
		return "", errors.New("tokenId " + tokenId + " is invalid")
	}
	ownerAddress, err := erc721Token.OwnerOf(nil, tokenIdBig)
	if err != nil {
		return "", err
	}
	if address == ownerAddress.String() {
		return "1", nil
	}
	return "0", nil
}

func (c *Client) Erc1155Balance(address string, tokenAddress string, tokenId string) (string, error) {
	hexTokenAddress := common.HexToAddress(tokenAddress)
	erc1155Token, err := erc1155.NewErc1155(hexTokenAddress, c)
	if err != nil {
		return "", err
	}
	tokenIdBig, ok := new(big.Int).SetString(tokenId, 0)
	if !ok {
		return "", errors.New("tokenId " + tokenId + " is invalid")
	}
	hexAddress := common.HexToAddress(address)
	balance, err := erc1155Token.BalanceOf(nil, hexAddress, tokenIdBig)
	if err != nil {
		return "", err
	}
	return balance.String(), nil
}

var EvmTokenInfoMap = make(map[string]types.TokenInfo)
var lock = icommon.NewSyncronized(0)

func (c *Client) GetEvmTokenInfo(chainName string, tokenAddress string) (types.TokenInfo, error) {
	var key = chainName + tokenAddress
	tokenInfo, ok := EvmTokenInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	lock.Lock(key)
	defer lock.Unlock(key)
	tokenInfo, ok = EvmTokenInfoMap[key]
	if ok {
		return tokenInfo, nil
	}

	erc20Token, err := erc20.NewErc20(common.HexToAddress(tokenAddress), c)
	if err != nil {
		return tokenInfo, err
	}
	decimals, err := erc20Token.Decimals(nil)
	if err != nil {
		return tokenInfo, err
	}
	symbol, err := erc20Token.Symbol(nil)
	if err != nil {
		return tokenInfo, err
	}
	tokenInfo = types.TokenInfo{Address: tokenAddress, Decimals: int64(decimals), Symbol: symbol}
	EvmTokenInfoMap[key] = tokenInfo
	return tokenInfo, nil
}

// --- start override ---

// BlockByHash returns the given full block.
//
// Note that loading full blocks requires two requests. Use HeaderByHash
// if you don't need all transactions or uncle headers.
func (c *Client) BlockByHash(ctx context.Context, hash common.Hash) (*Block, error) {
	return c.getBlock(ctx, "eth_getBlockByHash", hash, true)
}

// BlockByNumber returns a block from the current canonical chain. If number is nil, the
// latest known block is returned.
//
// Note that loading full blocks requires two requests. Use HeaderByNumber
// if you don't need all transactions or uncle headers.
func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*Block, error) {
	return c.getBlock(ctx, "eth_getBlockByNumber", toBlockNumArg(number), true)
}
