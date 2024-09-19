package stypes

import (
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type SuiObjectChanges struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Data []struct {
			Digest      string `json:"digest"`
			Transaction struct {
				Data struct {
					MessageVersion string `json:"messageVersion"`
					Transaction    struct {
						Kind   string `json:"kind"`
						Inputs []struct {
							Type                 string `json:"type"`
							ObjectType           string `json:"objectType,omitempty"`
							ObjectId             string `json:"objectId,omitempty"`
							Version              string `json:"version,omitempty"`
							Digest               string `json:"digest,omitempty"`
							ValueType            string `json:"valueType,omitempty"`
							InitialSharedVersion string `json:"initialSharedVersion,omitempty"`
							Mutable              bool   `json:"mutable,omitempty"`

							Value json.RawMessage `json:"value,omitempty"`
						} `json:"inputs"`
						Transactions []struct {
							TransferObjects []interface{} `json:"TransferObjects,omitempty"`
							SplitCoins      []interface{} `json:"SplitCoins,omitempty"`
							MoveCall        struct {
								Package   string `json:"package"`
								Module    string `json:"module"`
								Function  string `json:"function"`
								Arguments []struct {
									Input        int   `json:"Input,omitempty"`
									NestedResult []int `json:"NestedResult,omitempty"`
								} `json:"arguments"`
							} `json:"MoveCall,omitempty"`
						} `json:"transactions"`
					} `json:"transaction"`
					Sender  string `json:"sender"`
					GasData struct {
						Payment []struct {
							ObjectId string `json:"objectId"`
							Version  int    `json:"version"`
							Digest   string `json:"digest"`
						} `json:"payment"`
						Owner  string `json:"owner"`
						Price  string `json:"price"`
						Budget string `json:"budget"`
					} `json:"gasData"`
				} `json:"data"`
				TxSignatures []string `json:"txSignatures"`
			} `json:"transaction"`
			Effects struct {
				MessageVersion string `json:"messageVersion"`
				Status         struct {
					Status string `json:"status"`
				} `json:"status"`
				ExecutedEpoch string `json:"executedEpoch"`
				GasUsed       struct {
					ComputationCost         string `json:"computationCost"`
					StorageCost             string `json:"storageCost"`
					StorageRebate           string `json:"storageRebate"`
					NonRefundableStorageFee string `json:"nonRefundableStorageFee"`
				} `json:"gasUsed"`
				ModifiedAtVersions []struct {
					ObjectId       string `json:"objectId"`
					SequenceNumber string `json:"sequenceNumber"`
				} `json:"modifiedAtVersions"`
				TransactionDigest string `json:"transactionDigest"`
				Mutated           []struct {
					Owner struct {
						AddressOwner string `json:"AddressOwner,omitempty"`
						Shared       struct {
							InitialSharedVersion int `json:"initial_shared_version"`
						} `json:"Shared,omitempty"`
					} `json:"owner"`
					Reference struct {
						ObjectId string `json:"objectId"`
						Version  int    `json:"version"`
						Digest   string `json:"digest"`
					} `json:"reference"`
				} `json:"mutated"`
				GasObject struct {
					Owner struct {
						AddressOwner string `json:"AddressOwner"`
					} `json:"owner"`
					Reference struct {
						ObjectId string `json:"objectId"`
						Version  int    `json:"version"`
						Digest   string `json:"digest"`
					} `json:"reference"`
				} `json:"gasObject"`
				Dependencies  []string `json:"dependencies"`
				SharedObjects []struct {
					ObjectId string `json:"objectId"`
					Version  int    `json:"version"`
					Digest   string `json:"digest"`
				} `json:"sharedObjects,omitempty"`
				Created []struct {
					Owner struct {
						AddressOwner string `json:"AddressOwner,omitempty"`
						ObjectOwner  string `json:"ObjectOwner,omitempty"`
					} `json:"owner"`
					Reference struct {
						ObjectId string `json:"objectId"`
						Version  int    `json:"version"`
						Digest   string `json:"digest"`
					} `json:"reference"`
				} `json:"created,omitempty"`
				Deleted []struct {
					ObjectId string `json:"objectId"`
					Version  int    `json:"version"`
					Digest   string `json:"digest"`
				} `json:"deleted,omitempty"`
				EventsDigest string `json:"eventsDigest,omitempty"`
			} `json:"effects"`
			ObjectChanges []struct {
				Type   string `json:"type"`
				Sender string `json:"sender"`
				Owner  struct {
					AddressOwner string `json:"AddressOwner,omitempty"`
					Shared       struct {
						InitialSharedVersion int `json:"initial_shared_version"`
					} `json:"Shared,omitempty"`
					ObjectOwner string `json:"ObjectOwner,omitempty"`
				} `json:"owner"`
				ObjectType      string `json:"objectType"`
				ObjectId        string `json:"objectId"`
				Version         string `json:"version"`
				PreviousVersion string `json:"previousVersion,omitempty"`
				Digest          string `json:"digest"`
			} `json:"objectChanges"`
			BalanceChanges []struct {
				Owner struct {
					AddressOwner string `json:"AddressOwner"`
				} `json:"owner"`
				CoinType string `json:"coinType"`
				Amount   string `json:"amount"`
			} `json:"balanceChanges"`
			TimestampMs string `json:"timestampMs"`
			Checkpoint  string `json:"checkpoint"`
		} `json:"data"`
		NextCursor  string `json:"nextCursor"`
		HasNextPage bool   `json:"hasNextPage"`
	} `json:"result"`
	Id string `json:"id"`
}

type TransactionInfo struct {
	Digest      string      `json:"digest"`
	Transaction Transaction `json:"transaction"`
	Effects     struct {
		MessageVersion string `json:"messageVersion"`
		Status         struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"status"`
		ExecutedEpoch string `json:"executedEpoch"`
		GasUsed       struct {
			ComputationCost         string `json:"computationCost"`
			StorageCost             string `json:"storageCost"`
			StorageRebate           string `json:"storageRebate"`
			NonRefundableStorageFee string `json:"nonRefundableStorageFee"`
		} `json:"gasUsed"`
		/*ModifiedAtVersions []struct {
			ObjectId       string `json:"objectId"`
			SequenceNumber string `json:"sequenceNumber"`
		} `json:"modifiedAtVersions"`
		TransactionDigest string `json:"transactionDigest"`
		Created           []struct {
			Owner     interface{} `json:"owner"`
			Reference struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"reference"`
		} `json:"created"`
		Mutated []struct {
			Owner     interface{} `json:"owner"`
			Reference struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"reference"`
		} `json:"mutated"`
		Deleted []struct {
			ObjectId string `json:"objectId"`
			Version  int    `json:"version"`
			Digest   string `json:"digest"`
		} `json:"deleted"`*/
		GasObject struct {
			Owner struct {
				AddressOwner string `json:"AddressOwner"`
			} `json:"owner"`
			Reference struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"reference"`
		} `json:"gasObject"`
		EventsDigest string `json:"eventsDigest"`
		// Dependencies []string `json:"dependencies"`
	} `json:"effects"`
	RawEvents     json.RawMessage `json:"events"`
	ObjectChanges []struct {
		Type            string      `json:"type"`
		Sender          string      `json:"sender"`
		Owner           interface{} `json:"owner"`
		ObjectType      string      `json:"objectType"`
		ObjectId        string      `json:"objectId"`
		Version         string      `json:"version"`
		PreviousVersion string      `json:"previousVersion,omitempty"`
		Digest          string      `json:"digest"`
	} `json:"objectChanges"`
	BalanceChanges []BalanceChange    `json:"balanceChanges"`
	TimestampMs    string             `json:"timestampMs"`
	Checkpoint     string             `json:"checkpoint"`
	Errors         []string           `json:"errors,omitempty"`
	Error          *types.ErrorObject `json:"error,omitempty"`
}

func (transactionInfo *TransactionInfo) TxTime() int64 {
	txTime, _ := strconv.ParseInt(transactionInfo.TimestampMs, 10, 64)
	return txTime / 1000
}

func (transactionInfo *TransactionInfo) GasLimit() string {
	return transactionInfo.Transaction.Data.GasData.Budget
}

func (transactionInfo *TransactionInfo) GasObjectID() string {
	return transactionInfo.Effects.GasObject.Reference.ObjectId
}

func (transactionInfo *TransactionInfo) GasUsedInt() int {
	computationCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.ComputationCost)
	storageCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageCost)
	storageRebate, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageRebate)
	return computationCost + storageCost - storageRebate
}

func (transactionInfo *TransactionInfo) GasUsed() string {
	return strconv.Itoa(transactionInfo.GasUsedInt())
}

func (transactionInfo *TransactionInfo) FeeAmount() decimal.Decimal {
	return decimal.NewFromInt(int64(transactionInfo.GasUsedInt()))
}

type BalanceChange struct {
	/*Owner struct {
		AddressOwner string `json:"AddressOwner"`
	} `json:"owner"`*/
	Owner    interface{} `json:"owner"`
	CoinType string      `json:"coinType"`
	Amount   string      `json:"amount"`
}

func (ti *TransactionInfo) Events() ([]Event, error) {
	var events []Event
	err := json.Unmarshal(ti.RawEvents, &events)
	return events, err
}

type Transaction struct {
	Data struct {
		MessageVersion string          `json:"messageVersion"`
		Transaction    TransactionData `json:"transaction"`
		Sender         string          `json:"sender"`
		GasData        struct {
			/*Payment []struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"payment"`*/
			Owner  string `json:"owner"`
			Price  string `json:"price"`
			Budget string `json:"budget"`
		} `json:"gasData"`
	} `json:"data"`
	//TxSignatures []string `json:"txSignatures"`
}

type TransactionData struct {
	Kind            string             `json:"kind"`
	Inputs          []TransactionInput `json:"inputs"`
	RawTransactions []json.RawMessage  `json:"transactions"`

	transactions []TransactionEnum
}

func (td *TransactionData) Transactions() []TransactionEnum {
	if len(td.transactions) == 0 && len(td.RawTransactions) > 0 {
		for _, raw := range td.RawTransactions {
			var te TransactionEnum
			if err := json.Unmarshal(raw, &te); err != nil {
				// String txn in https://suiexplorer.com/txblock/APJatVeEnGGnWWsngoQTFQoXsMbanETrvP1invWSECbq
				log.Warn("FAILED TO PARSE ENUM OF TX", zap.Error(err), zap.ByteString("raw", raw))
			}
			td.transactions = append(td.transactions, te)
		}
	}
	return td.transactions
}

type TransactionInput struct {
	Type                 string      `json:"type"`
	ValueType            string      `json:"valueType,omitempty"`
	Value                interface{} `json:"value,omitempty"`
	ObjectType           string      `json:"objectType,omitempty"`
	ObjectId             string      `json:"objectId,omitempty"`
	InitialSharedVersion string      `json:"initialSharedVersion,omitempty"`
	Mutable              bool        `json:"mutable,omitempty"`
}

type TransactionEnum struct {
	MergeCoins      []interface{}   `json:"MergeCoins,omitempty"`
	SplitCoins      []interface{}   `json:"SplitCoins,omitempty"`
	TransferObjects []interface{}   `json:"TransferObjects,omitempty"`
	RawMoveCall     json.RawMessage `json:"MoveCall,omitempty"`

	innerMoveCall *MoveCall
}

func (ti *TransactionEnum) MoveCall() (*MoveCall, error) {
	if ti.innerMoveCall != nil {
		return ti.innerMoveCall, nil
	}
	if ti.RawMoveCall == nil {
		return nil, nil
	}
	err := json.Unmarshal(ti.RawMoveCall, &ti.innerMoveCall)
	return ti.innerMoveCall, err
}

type MoveCall struct {
	Package  string `json:"package"`
	Module   string `json:"module"`
	Function string `json:"function"`

	TypeArguments []interface{}     `json:"type_arguments"`
	Args          []json.RawMessage `json:"arguments"`
}

// FuncEquals returns true if the two are the same regardless of type arguments and arguments
func (m *MoveCall) FuncEquals(other *MoveCall) bool {
	if m.Package != other.Package {
		return false
	}
	if m.Module != other.Module {
		return false
	}
	if m.Function != other.Function {
		return false
	}
	return true
}

func (m *MoveCall) GetArgInput(pos int, txData *TransactionData) (*TransactionInput, error) {
	if pos >= len(m.Args) {
		return nil, errors.New("arguments position overflow")
	}
	rawArg := m.Args[pos]
	if string(rawArg) == `"GasCoin"` {
		return nil, errors.New("TODO")
	}
	var arg MoveCallArg
	err := json.Unmarshal(rawArg, &arg)
	if err != nil {
		return nil, err
	}
	if arg.Input >= len(txData.Inputs) {
		return nil, errors.New("input position overflow")
	}
	return &txData.Inputs[arg.Input], nil

}

func (m *MoveCall) GetArgObjectID(pos int, txData *TransactionData) (string, error) {
	input, err := m.GetArgInput(pos, txData)
	if err != nil {
		return "", err
	}
	if input.Type != "object" {
		return "", errors.New("argument is not a object")
	}
	return input.ObjectId, nil
}

type MoveCallArg struct {
	Input int `json:"Input"`
}

type Event struct {
	Id struct {
		TxDigest string `json:"txDigest"`
		EventSeq string `json:"eventSeq"`
	} `json:"id"`
	PackageId         string          `json:"packageId"`
	TransactionModule string          `json:"transactionModule"`
	Sender            string          `json:"sender"`
	Type              string          `json:"type"`
	RawParsedJson     json.RawMessage `json:"parsedJson"`
	Bcs               string          `json:"bcs"`
}

type FundsParseJson struct {
	CardUuid        string `json:"card_uuid"`
	AvailableAmount string `json:"available_amount"`
	DepositAmount   string `json:"deposit_amount"`
	WithdrawAmount  string `json:"withdraw_amount"`
}

type PayEventParseJson struct {
	Id         string `json:"id"`
	PaymentId  string `json:"payment_id"`
	//PayAt      string `json:"pay_at"`
	//Old        uint64 `json:"old"` //修改事件
	//New        uint64 `json:"new"`
	//Name       string `json:"name"`
	//Status     uint8  `json:"status"` //删除事件
	//Arbitrator string `json:"arbitrator"`  //退款事件
	//Operator   string `json:"operator"`
}

func (ev *Event) ParseJson(out interface{}) error {
	return json.Unmarshal(ev.RawParsedJson, out)
}
