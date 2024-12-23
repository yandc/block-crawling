package ethereum

import (
	"block-crawling/internal/biz"
	icommon "block-crawling/internal/common"
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/metachris/eth-go-bindings/erc1155"
	"github.com/metachris/eth-go-bindings/erc165"
	"github.com/metachris/eth-go-bindings/erc721"

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
	//*ethclient.Client
	rwLock    *sync.RWMutex
	rpcClient *rpc.Client

	*pcommon.NodeDefaultIn

	url       string
	chainName string
}

func getETHClient(rawUrl string) (*ethclient.Client, error) {
	c, err := rpc.DialContext(context.Background(), rawUrl)
	if err != nil {
		log.Error("new client error:", zap.Any("url", rawUrl), zap.Error(err))
		return nil, err
	}
	cli := ethclient.NewClient(c)
	return cli, nil
}

func NewClient(rawUrl string, chainName string) *Client {
	//client := ethclient.NewClient(c)
	return &Client{
		//Client: client,
		rwLock: &sync.RWMutex{},
		NodeDefaultIn: &pcommon.NodeDefaultIn{
			ChainName: chainName,
		},
		url:       rawUrl,
		chainName: chainName,
	}
}

func (c *Client) callContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	client, err := c.dial()
	if err != nil {
		return err
	}
	return client.CallContext(ctx, result, method, args...)
}

func (c *Client) dial() (*rpc.Client, error) {
	if r := c.getRPCClient(); r != nil {
		return r, nil
	}

	c.rwLock.Lock()
	if c.rpcClient == nil {
		client, err := rpc.DialContext(context.Background(), c.url)
		if err != nil {
			log.Error("new client error:", zap.Any("url", c.url), zap.Error(err))
			return nil, err
		}

		c.rpcClient = client
	}
	c.rwLock.Unlock()

	return c.getRPCClient(), nil
}

func (c *Client) getRPCClient() *rpc.Client {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	return c.rpcClient
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
			return nil, pcommon.BlockNotFound // retry on next node
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

func (c *Client) GetBlockTransaction(height uint64, txHash string) (*chain.Block, error) {
	start := time.Now()

	block, err := c.GetBlockByNumber(context.Background(), big.NewInt(int64(height)))
	if err != nil {
		if err == ethereum.NotFound /* && isNonstandardEVM(c.chainName)*/ {
			return nil, pcommon.BlockNotFound // retry on next node
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
		if tx.Hash().String() == txHash {
			txc := &chain.Transaction{
				Hash:        tx.Hash().String(),
				Nonce:       tx.Nonce(),
				BlockNumber: height,
				Raw:         tx,
			}
			c.parseTxMeta(txc, tx)
			txs = append(txs, txc)
			break
		}
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
	fromAddress := tx.From.String()
	var toAddress string
	if tx.To() != nil {
		toAddress = tx.To().String()
	}
	value := tx.Value().String()
	transactionType := biz.NATIVE
	data := tx.Data()
	var methodId string
	if len(data) >= 4 {
		methodId = hex.EncodeToString(data[:4])
	}
	if len(data) >= 68 && tx.To() != nil {
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
				if len(data) <= 100 {
					amountOrTokenId = new(big.Int).SetBytes(data[68:])
				} else {
					amountOrTokenId = new(big.Int).SetBytes(data[68:100])
				}
			} else {
				amountOrTokenId = new(big.Int)
			}
			transactionType = biz.TRANSFERFROM
			value = amountOrTokenId.String()
		} else if methodId == "42842e0e" || methodId == "b88d4fde" { // ERC721
			//safeTransferFrom(address from, address to, uint256 tokenId)
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
			value = tokenId.String() + ":" + amount.String()
		} else if methodId == "2eb2c2d6" { // ERC1155
			//https://etherscan.io/tx/0x228391a15896826e31d41f2c7627d693ed5d4832aad0429a65063399aaecb621
			//safeBatchTransferFrom(address from, address to, uint256[] ids, uint256[] amounts, bytes data)
			fromAddress = common.HexToAddress(hex.EncodeToString(data[4:36])).String()
			toAddress = common.HexToAddress(hex.EncodeToString(data[36:68])).String()
			transactionType = biz.SAFEBATCHTRANSFERFROM
			value = ""
			if len(data) >= 292 {
				var tokenId string
				var amount *big.Int
				tokenNumIndex := new(big.Int).SetBytes(data[68:100]).Int64()
				tokenNumIndex += 4
				tokenNum := int(new(big.Int).SetBytes(data[tokenNumIndex : tokenNumIndex+32]).Int64())
				amountNumIndex := new(big.Int).SetBytes(data[100:132]).Int64()
				amountNumIndex += 4
				amountNum := int(new(big.Int).SetBytes(data[amountNumIndex : amountNumIndex+32]).Int64())
				if tokenNum == amountNum {
					tokenIdIndex := tokenNumIndex + 32
					amountIndex := amountNumIndex + 32
					for i := 0; i < tokenNum; i++ {
						tokenId = new(big.Int).SetBytes(data[tokenIdIndex : tokenIdIndex+32]).String()
						amount = new(big.Int).SetBytes(data[amountIndex : amountIndex+32])
						amountStr := amount.String()
						if amountStr == "0" {
							continue
						}

						value = value + tokenId + ":" + amountStr + ","
						tokenIdIndex += 32
						amountIndex += 32
					}
					if strings.HasSuffix(value, ",") {
						value = value[:len(value)-1]
					}
				}
			}
		}
	}

	if transactionType == biz.NATIVE && methodId != "" {
		transactionType = biz.CONTRACT
		if methodId == "e7acab24" { // Seaport 1.1 Contract
			if strings.HasPrefix(c.chainName, "ETH") { //BSC链 Galxe: Space Station
				if len(data) >= 324 {
					realFromAddress := common.HexToAddress(hex.EncodeToString(data[296:324])).String()
					fromAddress = fromAddress + "," + realFromAddress
				}
				if len(data) >= 132 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[100:132])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if strings.HasPrefix(c.chainName, "Polygon") { //Polygon链 NFT Contract
				if len(data) >= 324 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[292:324])).String()
					toAddress = toAddress + "," + realToAddress
				}
			}
		} else if methodId == "fb0f3ee1" { // Seaport 1.1 Contract
			if len(data) >= 164 {
				realFromAddress := common.HexToAddress(hex.EncodeToString(data[132:164])).String()
				fromAddress = fromAddress + "," + realFromAddress
			}
		} else if methodId == "357a150b" { // X2Y2: Exchange Contract
			if len(data) >= 516 {
				realFromAddress := common.HexToAddress(hex.EncodeToString(data[484:516])).String()
				fromAddress = fromAddress + "," + realFromAddress
			}
			if len(data) >= 260 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[228:260])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "b4e4b296" { // LooksRare: Exchange Contract
			if len(data) >= 356 {
				realFromAddress := common.HexToAddress(hex.EncodeToString(data[324:356])).String()
				fromAddress = fromAddress + "," + realFromAddress
			}
			if len(data) >= 132 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[100:132])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "0175b1c4" { // Multichain: Router V4 Contract
			if len(data) >= 68 {
				realFromAddress := common.HexToAddress(hex.EncodeToString(data[36:68])).String()
				fromAddress = fromAddress + "," + realFromAddress
			}
			if len(data) >= 100 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[68:100])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "5dea8376" { // NFT Contract
			if len(data) >= 68 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[36:68])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "252f7b01" { // Optimism和BSC链 Contract
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
		} else if methodId == "f2b1251b" { //BSC链 Galxe: Space Station
			if strings.HasPrefix(c.chainName, "BSC") { //BSC链 Galxe: Space Station
				if len(data) >= 196 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[164:196])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if strings.HasPrefix(c.chainName, "Polygon") { //Polygon链 NFT Contract
				if len(data) >= 196 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[164:196])).String()
					toAddress = toAddress + "," + realToAddress
				}
			}
		} else if methodId == "ef6c5996" { //BSC和Polygon链 NFT Contract
			if strings.HasPrefix(c.chainName, "BSC") { //BSC链 Galxe: Space Station
				if len(data) >= 164 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[132:164])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if strings.HasPrefix(c.chainName, "Polygon") { //Polygon链 NFT Contract
				if len(data) >= 164 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[132:164])).String()
					toAddress = toAddress + "," + realToAddress
				}
			}
		} else if methodId == "6a627842" { //BSC链 NFT Contract
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "afd76a0b" { //BSC和Avalanche链 NFT Contract
			if strings.HasPrefix(c.chainName, "BSC") { //BSC链 NFT Contract
				if len(data) >= 36 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if strings.HasPrefix(c.chainName, "Avalanche") { //Avalanche链 NFT Contract
				if len(data) >= 36 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
					toAddress = toAddress + "," + realToAddress
				}
			}
		} else if methodId == "a8809485" { //BSC链 NFT Contract
			if len(data) >= 292 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[260:292])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "eb31403f" || methodId == "74a8f103" || methodId == "d3fc9864" { //BSC链 NFT Contract
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "a5599dfe" { //BSC链 NFT Contract
			if len(data) >= 132 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[100:132])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "32389b71" { //Polygon和Avalanche链 NFT Contract
			if strings.HasPrefix(c.chainName, "Polygon") { //Polygon链 NFT Contract
				if len(data) >= 196 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[164:196])).String()
					toAddress = toAddress + "," + realToAddress
				}
			} else if strings.HasPrefix(c.chainName, "Avalanche") { //Avalanche链 NFT Contract
				if len(data) >= 196 {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[164:196])).String()
					toAddress = toAddress + "," + realToAddress
				}
			}
		} else if methodId == "a8174404" { //Polygon链 NFT Contract
			if len(data) >= 1412 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[1380:1412])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "c4605394" || methodId == "00d26c0c" { //Polygon链 NFT Contract
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "9c2605a5" || methodId == "41706c4e" { //Polygon链 NFT Contract
			if len(data) >= 164 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[132:164])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "53d4c775" { //Optimism链 NFT Contract
			if len(data) >= 68 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[36:68])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "c725e054" { //Optimism链 NFT Contract
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "26fb76c2" || methodId == "50bb4e7f" { //Klaytn链 NFT Contract
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "3cbf4f8a" { //ETH链 X2Y2: ERC721 Delegate
			if len(data) >= 68 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[36:68])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "89d56c84" { //ETH链 Element: Marketplace
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "4c674c2d" { //ETH链 Element: Element Swap 2
			if len(data) >= 452 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[420:452])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "9a1fc3a7" { //ETH链 Blur.io: Marketplace
			if len(data) >= 324 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[292:324])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "8a9c2d65" { //BSC链 NFT Contract
			dl := len(data)
			if dl >= 164 {
				i := 100
				dl = dl/2 + i/2 - 32
				for i < dl {
					realToAddress := common.HexToAddress(hex.EncodeToString(data[i : i+32])).String()
					toAddress = toAddress + "," + realToAddress
					i += 32
				}
			}
		} else if methodId == "00000000" { //Arbitrum链 Seaport 1.4
			if len(data) >= 164 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[132:164])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "8171e632" { //Polygon链 NFT Contract
			if len(data) >= 100 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[68:100])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "439dff06" { //Polygon链 Contract
			if len(data) >= 100 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[68:100])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "23c452cd" { //ETH链 Hop Protocol: Ethereum or MATIC Bridge
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "a4d73041" { //zkSync链 NFT
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "87201b41" { //Polygon链 NFT
			if len(data) >= 196 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[164:196])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "4782f779" { //Arbitrum, Optimism, Fantom, BSC链 Contract
			//https://arbiscan.io/tx/0xf65c3b8a2a31754059a90fcf65ed3ff7a672c46abf84d30d80dd7d09c8a9d3bb
			//https://optimistic.etherscan.io/tx/0x1de553537b19e29619da0112c688ce4ecc5e185c2e289d757084148f6d4c6d6c
			//https://ftmscan.com/tx/0xce25179db51f9ee48fbdc518b96d2cf584af655a34b95bc535544c1a653be9a8
			//https://bscscan.com/tx/0x076501069df7ab50acb5244bcefcfe8940d970095a93a5287b75ae8fb3d9269b
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "4630a0d8" { //openBlock跨链Swap
			//https://polygonscan.com/tx/0xf8c87e264fb54d02a625d8c1f1af4ec0109126127d11daebea046a8210ea71f1
			if len(data) >= 132 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[100:132])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "cc29a306" { //Arbitrum链 Contract
			//https://arbiscan.io/tx/0xed0b45e9dc70fde48288f21fdcef0d6677e84d7387ac10d5cc5130fcc22f317d
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "11290d59" { //手续费代付
			//https://testnet.bscscan.com/tx/0x34a6cc6024154db19c946c03dd2f3c046328e09865980b8290da0cbba102cefd
			if len(data) >= 100 {
				realFromAddress := common.HexToAddress(hex.EncodeToString(data[68:100])).String()
				fromAddress = fromAddress + "," + realFromAddress
			}
		} else if methodId == "ca350aa6" { //ETH链 Coinbase: Deposit
			//https://cn.etherscan.com/tx/0xad7dc826dfb58dcf31cd550f24180c16746621ad5844731bcf7e4441ae65230f
			dl := len(data)
			if dl >= 196 {
				start := 100
				stop := start + 96
				num := int(new(big.Int).SetBytes(data[68:100]).Int64())
				for i := 0; i < num; i++ {
					if stop > dl {
						break
					}
					d := data[start:stop]
					addressData := d[32:64]
					realToAddress := common.HexToAddress(hex.EncodeToString(addressData)).String()
					toAddress = toAddress + "," + realToAddress
					start = stop
					stop += 96
				}
			}
		} else if methodId == "0ddedd84" { //evm1101(Polygon zkEVM)
			//https://zkevm.polygonscan.com/tx/0xd2b8469b94f2795cb52e486c440f1215f02b0dd5e5720e37880085a1795e9699
			dl := len(data)
			if dl >= 324 {
				start := 196
				stop := start + 32
				num := int(new(big.Int).SetBytes(data[164:196]).Int64())
				for i := 0; i < num; i++ {
					if stop > dl {
						break
					}
					addressData := data[start:stop]
					realToAddress := common.HexToAddress(hex.EncodeToString(addressData)).String()
					toAddress = toAddress + "," + realToAddress
					start = stop
					stop += 32
				}
			}
		} else if methodId == "1e9d2490" { //用户将代币授权给手续费代付合约操作无主币付手续费时，平台会给用户转一些主币垫资手续费
			//https://goerli.etherscan.io/tx/0x33972476564c3f78c410ffee0678bb1bd3955cd53f1fcad4fa9b59d43e431bc7
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "dafe477c" { //ETH链 Contract
			//https://etherscan.io/tx/0xb8a1940235654bc6b11d1f4e725b5bdcaaf8e362917a6fe50115241ab93bf6b1
			if len(data) >= 36 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[4:36])).String()
				toAddress = toAddress + "," + realToAddress
			}
		} else if methodId == "8ef1332e" { //Scroll链 Contract
			//https://blockscout.scroll.io/tx/0xf1191f68d880e190bc0948dcea397ad99392a3bec33fff216b573ff764a4ec9f
			if len(data) >= 232 {
				realToAddress := common.HexToAddress(hex.EncodeToString(data[200:232])).String()
				toAddress = toAddress + "," + realToAddress
			}
		}
	}
	if strings.HasPrefix(c.chainName, "Polygon") && fromAddress == "0x0000000000000000000000000000000000000000" && toAddress == "0x0000000000000000000000000000000000000000" {
		transactionType = biz.CONTRACT
	}
	txc.FromAddress = fromAddress
	txc.ToAddress = toAddress
	txc.TxType = chain.TxType(transactionType)
	txc.Value = value
	return nil
}

// 3
func (c *Client) GetBlockNumber(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return 0, err
	}
	defer cli.Close()
	return cli.BlockNumber(ctx)
}

func (c *Client) GetBlockByNumber(ctx context.Context, number *big.Int) (*Block, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.BlockByNumber(ctx, number)
}

// GetTxByHash 没有错误的情况下：
//
// 1. 返回 non-nil tx 表示调用 TxHandler.OnSealedTx
// 2. 返回 nil tx 表示调用 TxHandler.OnDroppedTx（兜底方案）
func (c *Client) GetTxByHash(txHash string) (tx *chain.Transaction, err error) {
	txHash = strings.Split(txHash, "#")[0]
	receipt, err := c.GetTransactionReceipt(context.Background(), common.HexToHash(txHash))
	if err != nil {
		if err == ethereum.NotFound {
			_, pending, txByHashErr := c.GetTransactionByHash(context.Background(), common.HexToHash(txHash))
			if txByHashErr == ethereum.NotFound {
				return nil, pcommon.TransactionNotFound
			}
			if pending {
				return nil, pcommon.TransactionStillPending
			}
			err = txByHashErr
		}
		log.Error("get transaction receipt by hash error", zap.String("chainName", c.ChainName), zap.String("txHash", txHash), zap.String("nodeUrl", c.URL()), zap.Any("error", err))
		return nil, err
	}

	intBlockNumber, _ := utils.HexStringToUint64(receipt.BlockNumber)
	tx = &chain.Transaction{
		Hash:        txHash,
		BlockNumber: intBlockNumber,
		TxType:      "",
		FromAddress: receipt.From,
		ToAddress:   receipt.To,
		Value:       "",
		Raw:         receipt,
		Record:      nil,
	}
	return tx, nil
}

// 1
func (c *Client) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*Transaction, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.TransactionByHash(ctx, txHash)
}

// 2
func (c *Client) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*rtypes.Receipt, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.TransactionReceipt(ctx, txHash)
}

// client.TransactionSender(ctx,tx,block.Hash(),r.TransactionIndex)
func (c *Client) GetTransactionSender(ctx context.Context, tx *types2.Transaction, block common.Hash,
	index uint) (common.Address, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return [20]byte{}, err
	}
	defer cli.Close()
	return cli.TransactionSender(ctx, tx, block, index)
}

func (c *Client) GetBalance(address string) (string, error) {
	var err error
	var balance *big.Int
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return "", err
	}
	defer cli.Close()
	account := common.HexToAddress(address)
	if c.ChainName == "evm15" {
		var result string
		err = c.callContext(ctx, &result, "eth_getBalance", account, toBlockNumArg(nil))
		if err != nil {
			return "", err
		}
		balance, err = utils.HexStringToBigInt(result)
	} else {
		balance, err = cli.BalanceAt(ctx, account, nil)

	}
	if err != nil {
		return "", err
	}
	ethValue := utils.BigIntString(balance, 18)
	return ethValue, err
}

func (c *Client) BatchTokenBalance(address string, tokenMap map[string]int) (map[string]interface{}, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
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
	err = rpcClient.BatchCallContext(ctx, be)
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

func (c *Client) NewBatchTokenBalance(address string, tokenMap map[string]int) (map[string]interface{}, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
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
	for token, _ := range tokenMap {
		var data []byte
		var b string
		data = append(data, methodID...)
		tokenAddress := common.HexToAddress(token)
		data = append(data, common.LeftPadBytes(destAddress.Bytes(), 32)...)
		callMsg := map[string]interface{}{
			"from": destAddress,
			"to":   tokenAddress,
			"data": hexutil.Bytes(data),
		}
		err = rpcClient.CallContext(ctx, &b, "eth_call", callMsg, "latest")
		if err != nil {
			return result, err
		}
		newHexAmount := b
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

func (c *Client) GetDexPairTokens(contract string) (string, string, error) {
	token0, err := c.GetDexPairToken(contract, "token0")
	if err != nil {
		return "", "", err
	}
	token1, err := c.GetDexPairToken(contract, "token1")
	if err != nil {
		return "", "", err
	}
	return token0, token1, nil
}

func (c *Client) GetDexDodoPairTokens(contract string) (string, string, error) {
	token0, err := c.GetDexPairToken(contract, "_BASE_TOKEN_")
	if err != nil {
		return "", "", err
	}
	token1, err := c.GetDexPairToken(contract, "_QUOTE_TOKEN_")
	if err != nil {
		return "", "", err
	}
	return token0, token1, nil
}

func (c *Client) GetDexPairToken(contract string, method string) (string, error) {
	stringTy, _ := abi.NewType("address", "", nil)
	addressRt := abi.Arguments{
		abi.Argument{
			Name: "",
			Type: stringTy,
		},
	}
	viewMethod := abi.NewMethod(method, method, abi.Function, "view", true, false, nil, addressRt)
	callMsg := map[string]interface{}{
		"from": common.HexToAddress(contract),
		"to":   common.HexToAddress(contract),
		"data": hexutil.Bytes(viewMethod.ID),
	}
	rpcClient, err := rpc.DialHTTP(c.url)
	if err != nil {
		return "", err
	}
	var address string
	err = rpcClient.CallContext(context.Background(), &address, "eth_call", callMsg, "latest")
	if err != nil {
		return "", err
	}
	addr := common.HexToAddress(address).Hex()
	if addr == "0x0000000000000000000000000000000000000000" {
		return "", errors.New("empty result")
	}
	return addr, nil
}

func (c *Client) Erc721Balance(address string, tokenAddress string, tokenId string) (string, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return "", err
	}
	defer cli.Close()
	opts := &bind.CallOpts{
		Context: ctx,
	}
	hexTokenAddress := common.HexToAddress(tokenAddress)
	erc721Token, err := erc721.NewErc721(hexTokenAddress, cli)
	if err != nil {
		return "", err
	}
	tokenIdBig, ok := new(big.Int).SetString(tokenId, 0)
	if !ok {
		return "", errors.New("tokenId " + tokenId + " is invalid")
	}
	ownerAddress, err := erc721Token.OwnerOf(opts, tokenIdBig)
	if err != nil {
		if strings.HasSuffix(err.Error(), "ERC721: owner query for nonexistent token") {
			if address == "0x0000000000000000000000000000000000000000" {
				return "1", nil
			}
			return "0", nil
		} else if strings.HasSuffix(err.Error(), "ERC721: invalid token ID") {
			if address == "0x0000000000000000000000000000000000000000" {
				return "1", nil
			}
			return "0", nil
		}
		if strings.Contains(err.Error(), "execution reverted") && c.manySuspiciousNFTs() {
			return "0", nil
		}

		return "", err
	}
	if address == ownerAddress.String() {
		return "1", nil
	}
	return "0", nil
}

func (c *Client) Erc1155Balance(address string, tokenAddress string, tokenId string) (string, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return "", err
	}
	defer cli.Close()
	opts := &bind.CallOpts{
		Context: ctx,
	}
	hexTokenAddress := common.HexToAddress(tokenAddress)
	erc1155Token, err := erc1155.NewErc1155(hexTokenAddress, cli)
	if err != nil {
		return "", err
	}
	tokenIdBig, ok := new(big.Int).SetString(tokenId, 0)
	if !ok {
		return "", errors.New("tokenId " + tokenId + " is invalid")
	}
	hexAddress := common.HexToAddress(address)
	balance, err := erc1155Token.BalanceOf(opts, hexAddress, tokenIdBig)
	if err != nil {
		if strings.Contains(err.Error(), "execution reverted") && c.manySuspiciousNFTs() {
			return "0", nil
		}

		return "", err
	}
	return balance.String(), nil
}

func (c *Client) manySuspiciousNFTs() bool {
	// Chains that may receive a lot suspicious NFTs
	return (c.chainName == "Polygon" || c.chainName == "Scroll")
}

func (c *Client) IsErc721Contract(tokenAddress string) (bool, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return false, err
	}
	defer cli.Close()
	opts := &bind.CallOpts{
		Context: ctx,
	}
	hexTokenAddress := common.HexToAddress(tokenAddress)
	erc721Token, err := erc721.NewErc721(hexTokenAddress, cli)
	if err != nil {
		return false, err
	}

	result, err := erc721Token.SupportsInterface(opts, erc165.InterfaceIdErc721)
	if err != nil {
		return false, err
	}
	return result, nil
}

func (c *Client) IsErc1155Contract(tokenAddress string) (bool, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return false, err
	}
	defer cli.Close()
	opts := &bind.CallOpts{
		Context: ctx,
	}
	hexTokenAddress := common.HexToAddress(tokenAddress)
	erc1155Token, err := erc1155.NewErc1155(hexTokenAddress, cli)
	if err != nil {
		return false, err
	}

	result, err := erc1155Token.SupportsInterface(opts, erc165.InterfaceIdErc1155)
	if err != nil {
		return false, err
	}
	return result, nil
}

var EvmTokenInfoMap = make(map[string]types.TokenInfo)
var lock = icommon.NewSyncronized(0)
var mutex = new(sync.Mutex)

func (c *Client) GetEvmTokenInfo(chainName string, tokenAddress string) (types.TokenInfo, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	opts := &bind.CallOpts{
		Context: ctx,
	}
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
	cli, err := getETHClient(c.url)
	if err != nil {
		return tokenInfo, err
	}
	defer cli.Close()
	erc20Token, err := erc20.NewErc20(common.HexToAddress(tokenAddress), cli)
	if err != nil {
		return tokenInfo, err
	}
	decimals, err := erc20Token.Decimals(opts)
	if err != nil {
		return tokenInfo, err
	}
	symbol, err := erc20Token.Symbol(opts)
	if err != nil {
		return tokenInfo, err
	}
	tokenInfo = types.TokenInfo{Address: tokenAddress, Decimals: int64(decimals), Symbol: symbol}
	mutex.Lock()
	EvmTokenInfoMap[key] = tokenInfo
	mutex.Unlock()
	return tokenInfo, nil
}

func (c *Client) GetAllowance(contract, owner, spender string) (*big.Int, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cli, err := getETHClient(c.url)
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	opts := &bind.CallOpts{
		Context: ctx,
	}
	hexTokenAddress := common.HexToAddress(contract)
	erc20Token, err := erc20.NewErc20(hexTokenAddress, cli)
	if err != nil {
		return nil, err
	}

	return erc20Token.Allowance(opts, common.HexToAddress(owner), common.HexToAddress(spender))
}
