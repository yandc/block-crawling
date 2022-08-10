package ethereum

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/metachris/eth-go-bindings/erc20"
)

func GetTokenDecimals(contract string, client Client) (int64, string, error) {
	erc20Token, err := erc20.NewErc20(common.HexToAddress(contract), client)
	if err != nil {
		return 0, "", err
	}
	decimals, err := erc20Token.Decimals(nil)
	symbol, err := erc20Token.Symbol(nil)
	return int64(decimals), symbol, nil
}
