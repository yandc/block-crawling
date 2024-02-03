package swap

import (
	"block-crawling/internal/platform/sui/stypes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
	for _, tx := range transactionInfo.Transaction.Data.Transaction.Transactions() {
		moveCall, err := tx.MoveCall()
		if err != nil {
			return false, err
		}

		if moveCall == nil {
			continue
		}
		if d.isSwapMoveCall(moveCall) {
			return true, nil
		}
	}
	return false, nil
}

func (d *defaultIn) isSwapMoveCall(moveCall *stypes.MoveCall) bool {
	_, ok := d.contracts[moveCall.Package]
	if !ok {
		return false
	}
	return isMoveFnCalled(moveCall, moveCall.Package, d.swapModule, d.swapFunction)
}

// isMoveFnCalled Returns true if specific function called.
//
// Example:
//
//	isMoveFnCalled(tx.MoveCall, "BFCa5e3d1ae22b6fa1b238a8552752eb6a6d39835889fc4abed82cc287f55b84a8885c7", "pool_script", "create_pool")
func isMoveFnCalled(moveCall *stypes.MoveCall, pkg, mod, fn string) bool {
	return moveCall.Package == pkg && moveCall.Module == mod && moveCall.Function == fn
}

// Name implements swap.SwapContract
func (d *defaultIn) Name() string {
	return d.name
}

func extractInputAndOutput(moveCall *stypes.MoveCall) (string, string, error) {
	typeArgs := moveCall.TypeArguments
	if len(typeArgs) != 2 {
		return "", "", errors.New("the length of type_arguments must be 2")
	}
	return typeArgs[0].(string), typeArgs[1].(string), nil
}

func extractEvents(module string, events []stypes.Event, out interface{}, eventTypes ...string) (json.RawMessage, error) {
	for _, ev := range events {
		if ev.TransactionModule != module {
			continue
		}
		matchedEvent := len(eventTypes) == 0
		for _, suffix := range eventTypes {
			if strings.HasSuffix(ev.Type, suffix) {
				matchedEvent = true
				break
			}
		}
		if !matchedEvent {
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

// ExtractMoveCallEvent Parse Event from special move calls.
func ExtractMoveCallEvent(expectedMoveCall *stypes.MoveCall, txInfo *stypes.TransactionInfo, out interface{}) error {
	for _, tx := range txInfo.Transaction.Data.Transaction.Transactions() {
		moveCall, err := tx.MoveCall()
		if err != nil {
			return fmt.Errorf("[parseMoveCall] %w", err)
		}

		if moveCall == nil {
			continue
		}
		if moveCall.Package != expectedMoveCall.Package {
			continue
		}
		if moveCall.Module != expectedMoveCall.Module {
			continue
		}
		if moveCall.Function != expectedMoveCall.Function {
			continue
		}
		events, err := txInfo.Events()
		if err != nil {
			return fmt.Errorf("[parseEvents] %w", err)
		}
		_, err = extractEvents(expectedMoveCall.Module, events, out)
		return err
	}
	return nil
}
