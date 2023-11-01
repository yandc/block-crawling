package swap

import (
	"block-crawling/internal/platform/sui/stypes"
	"encoding/json"
	"errors"

	"gitlab.bixin.com/mili/node-driver/chain"
)

type defaultIn struct {
	name      string
	contracts map[string]bool

	swapModule   string
	swapFunction string
}

// Is implements swap.SwapContract
func (d *defaultIn) Is(chainName string, tx *chain.Transaction) (bool, error) {
	transactionInfo := tx.Raw.(*stypes.TransactionInfo)
	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions {
		if tx.MoveCall == nil {
			continue
		}
		moveCall := tx.MoveCall.(map[string]interface{})
		if d.isSwapMoveCall(moveCall) {
			return true, nil
		}
	}
	return false, nil
}

func (d *defaultIn) isSwapMoveCall(moveCall map[string]interface{}) bool {
	_, ok := d.contracts[getStr(moveCall, "package")]
	if !ok {
		return false
	}
	module := getStr(moveCall, "module")
	function := getStr(moveCall, "function")
	return module == d.swapModule && function == d.swapFunction
}

func getStr(obj map[string]interface{}, key string) string {
	rawVal, ok := obj[key]
	if !ok {
		return ""
	}
	val, ok := rawVal.(string)
	if !ok {
		return ""
	}
	return val
}

// Name implements swap.SwapContract
func (d *defaultIn) Name() string {
	return d.name
}

func extractInputAndOutput(moveCall map[string]interface{}) (string, string, error) {
	rawTypeArgs, ok := moveCall["type_arguments"]
	if !ok {
		return "", "", errors.New("missed type_arguments")
	}
	typeArgs, ok := rawTypeArgs.([]interface{})
	if !ok {
		return "", "", errors.New("type mismatch of type_arguments")
	}
	if len(typeArgs) != 2 {
		return "", "", errors.New("the length of type_arguments must be 2")
	}
	return typeArgs[0].(string), typeArgs[1].(string), nil
}

func extractEvents(module string, events []stypes.Event, out interface{}) (json.RawMessage, error) {
	for _, ev := range events {
		if ev.TransactionModule != module {
			continue
		}
		if err := ev.ParseJson(&out); err != nil {
			return nil, err
		}
		if out == nil {
			continue
		}
		return ev.RawParsedJson, nil
	}
	return nil, errors.New("missed swap event")
}
