package stypes

import (
	"block-crawling/internal/types"
	"encoding/json"
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
							Value                string `json:"value,omitempty"`
							InitialSharedVersion string `json:"initialSharedVersion,omitempty"`
							Mutable              bool   `json:"mutable,omitempty"`
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
		} `json:"deleted"`
		GasObject struct {
			Owner struct {
				AddressOwner string `json:"AddressOwner"`
			} `json:"owner"`
			Reference struct {
				ObjectId string `json:"objectId"`
				Version  int    `json:"version"`
				Digest   string `json:"digest"`
			} `json:"reference"`
		} `json:"gasObject"`*/
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
	BalanceChanges []struct {
		/*Owner struct {
			AddressOwner string `json:"AddressOwner"`
		} `json:"owner"`*/
		Owner    interface{} `json:"owner"`
		CoinType string      `json:"coinType"`
		Amount   string      `json:"amount"`
	} `json:"balanceChanges"`
	TimestampMs string             `json:"timestampMs"`
	Checkpoint  string             `json:"checkpoint"`
	Errors      []string           `json:"errors,omitempty"`
	Error       *types.ErrorObject `json:"error,omitempty"`
}

func (ti *TransactionInfo) Events() ([]Event, error) {
	var events []Event
	err := json.Unmarshal(ti.RawEvents, &events)
	return events, err
}

type Transaction struct {
	Data struct {
		MessageVersion string `json:"messageVersion"`
		Transaction    struct {
			Kind   string `json:"kind"`
			Inputs []struct {
				Type                 string      `json:"type"`
				ValueType            string      `json:"valueType,omitempty"`
				Value                interface{} `json:"value,omitempty"`
				ObjectType           string      `json:"objectType,omitempty"`
				ObjectId             string      `json:"objectId,omitempty"`
				InitialSharedVersion string      `json:"initialSharedVersion,omitempty"`
				Mutable              bool        `json:"mutable,omitempty"`
			} `json:"inputs"`
			Transactions []struct {
				MergeCoins      []interface{} `json:"MergeCoins,omitempty"`
				SplitCoins      []interface{} `json:"SplitCoins,omitempty"`
				TransferObjects []interface{} `json:"TransferObjects,omitempty"`
				MoveCall        interface{}   `json:"MoveCall,omitempty"`
			} `json:"transactions"`
		} `json:"transaction"`
		Sender  string `json:"sender"`
		GasData struct {
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

func (ev *Event) ParseJson(out interface{}) error {
	return json.Unmarshal(ev.RawParsedJson, out)
}
