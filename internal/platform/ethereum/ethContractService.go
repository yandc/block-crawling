package ethereum

import (
	"block-crawling/internal/biz"
	icommon "block-crawling/internal/common"
	"block-crawling/internal/platform/ethereum/etl"
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"strings"
	"sync"
)

var EvmTokenTypeMap = make(map[string]string)
var tokenTypeLock = icommon.NewSyncronized(0)
var tokenTypeMutex = new(sync.Mutex)

func GetTokenType(client *Client, chainName, tokenAddress string, code []byte) (string, error) {
	var key = chainName + tokenAddress
	tokenType, ok := EvmTokenTypeMap[key]
	if ok {
		return tokenType, nil
	}

	tokenTypeLock.Lock(key)
	defer tokenTypeLock.Unlock(key)
	tokenType, ok = EvmTokenTypeMap[key]
	if ok {
		return tokenType, nil
	}

	var err error
	if len(code) == 0 {
		code, err = client.CodeAt(context.Background(), common.HexToAddress(tokenAddress), nil)
		if err != nil {
			return "", err
		}
	}
	evmContract := &etl.EvmContract{Bytecode: code}
	evmContract.SimpleInitFunctionSighashes()
	isErc20Contract := evmContract.IsErc20Contract()
	if isErc20Contract {
		tokenType = biz.ERC20
		tokenTypeMutex.Lock()
		EvmTokenTypeMap[key] = tokenType
		tokenTypeMutex.Unlock()
		return tokenType, nil
	}
	isErc721Contract := evmContract.IsErc721Contract()
	if isErc721Contract {
		tokenType = biz.ERC721
		tokenTypeMutex.Lock()
		EvmTokenTypeMap[key] = tokenType
		tokenTypeMutex.Unlock()
		return tokenType, nil
	}
	isErc1155Contract := evmContract.IsErc1155Contract()
	if isErc1155Contract {
		tokenType = biz.ERC1155
		tokenTypeMutex.Lock()
		EvmTokenTypeMap[key] = tokenType
		tokenTypeMutex.Unlock()
		return tokenType, nil
	}

	tokenInfo, err := client.GetEvmTokenInfo(chainName, tokenAddress)
	if err != nil && !strings.HasPrefix(fmt.Sprintf("%s", err), "execution reverted") {
		return "", err
	}
	if tokenInfo.Decimals != 0 || tokenInfo.Symbol != "" {
		tokenType = biz.ERC20
		tokenTypeMutex.Lock()
		EvmTokenTypeMap[key] = tokenType
		tokenTypeMutex.Unlock()
		return tokenType, nil
	}
	isErc721Contract, err = client.IsErc721Contract(tokenAddress)
	if err != nil && err.Error() != "execution reverted" {
		return "", err
	}
	if isErc721Contract {
		tokenType = biz.ERC721
		tokenTypeMutex.Lock()
		EvmTokenTypeMap[key] = tokenType
		tokenTypeMutex.Unlock()
		return tokenType, nil
	}
	isErc1155Contract, err = client.IsErc1155Contract(tokenAddress)
	if err != nil && err.Error() != "execution reverted" {
		return "", err
	}
	if isErc1155Contract {
		tokenType = biz.ERC1155
		tokenTypeMutex.Lock()
		EvmTokenTypeMap[key] = tokenType
		tokenTypeMutex.Unlock()
		return tokenType, nil
	}
	tokenType = ""
	tokenTypeMutex.Lock()
	EvmTokenTypeMap[key] = tokenType
	tokenTypeMutex.Unlock()
	return tokenType, nil
}
