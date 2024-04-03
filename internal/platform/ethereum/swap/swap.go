package swap

import (
	"block-crawling/internal/data"
	"errors"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"gitlab.bixin.com/mili/node-driver/chain"
)

const (
	transferEvent = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

type defaultIn struct {
	name      string
	contracts map[string]bool
}

// Is implements SwapContract
func (u *defaultIn) Is(chainName string, tx *chain.Transaction) (bool, error) {
	if _, ok := u.contracts[strings.ToLower(tx.ToAddress)]; ok {
		return ok, nil
	}
	return data.SwapContractRepoClient.Is(chainName, tx.ToAddress), nil
}

// Name returns the name of the swap contract.
func (u *defaultIn) Name() string {
	return u.name
}

func newDefaultIn(name string, contracts map[string]bool) defaultIn {
	return defaultIn{
		name:      name,
		contracts: lowerContract(contracts),
	}
}

func eventTopicToAddress(v common.Hash) string {
	return common.HexToAddress(v.String()).String()
}

func EventTopicToAddress(v common.Hash) string {
	return eventTopicToAddress(v)
}

func bytes2Amounts(byts []byte) ([]*big.Int, error) {
	if len(byts)%32 != 0 {
		return nil, errors.New("invalid amounts")
	}
	amounts := make([]*big.Int, 0, len(byts)/32)
	for i := 0; i < len(byts); i += 32 {
		v, err := bytes2Amount(byts[i : i+32])
		if err != nil {
			return nil, err
		}
		amounts = append(amounts, v)
	}
	return amounts, nil
}

func Bytes2Amount(byts []byte) (*big.Int, error) {
	return bytes2Amount(byts)
}

func bytes2Amount(byts []byte) (*big.Int, error) {
	if len(byts) != 32 {
		return nil, errors.New("invalid length")
	}

	h := common.Bytes2Hex(byts)
	v, ok := ParseBig256("0x" + h)
	if !ok {
		return nil, errors.New("invalid amount")
	}
	return v, nil
}

func lowerContract(contracts map[string]bool) map[string]bool {
	for k, v := range contracts {
		contracts[strings.ToLower(k)] = v
	}
	return contracts
}
