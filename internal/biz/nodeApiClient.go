package biz

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"errors"
	"fmt"
	"golang.org/x/exp/slices"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	types2 "github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

var pageSize = 50
var timeout = 10_000 * time.Millisecond

func GetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("GetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	switch chainName {
	case "Cosmos", "Osmosis", "Sei", "SeiTEST", "CelestiaMochaTEST":
		err = CosmosGetTxByAddress(chainName, address, urls)
	case "Solana":
		err = SolanaGetTxByAddress(chainName, address, urls)
	case "Arbitrum", "Avalanche", "BSC", "Cronos", "ETH", "Fantom", "HECO", "Optimism", "ETC", "Polygon", "Conflux", "Linea", "Scroll":
		err = EvmNormalAndInternalGetTxByAddress(chainName, address, urls)
	case "zkSync":
		err = ZkSyncGetTxByAddress(chainName, address, urls)
	case "Klaytn":
		err = KlaytnGetTxByAddress(chainName, address, urls)
	case "Ronin":
		err = RoninGetTxByAddress(chainName, address, urls)
	case "Casper":
		err = CasperGetTxByAddress(chainName, address, urls)
	case "Aptos":
		err = AptosGetTxByAddress(chainName, address, urls)
	case "STC":
		err = StarcoinGetTxByAddress(chainName, address, urls)
	case "DOGE":
		err = DogeGetTxByAddress(chainName, address, urls)
		err = UpdateUtxoByAddress(chainName, address)
	case "Polkadot":
		err = DotGetTxByAddress(chainName, address, urls)
	case "BTC":
		err = BTCGetTxByAddress(chainName, address, urls)
		err = UpdateUtxoByAddress(chainName, address)
	case "LTC":
		err = LtcGetTxByAddress(chainName, address, urls)
		err = UpdateUtxoByAddress(chainName, address)
	case "TRX":
		err = TrxGetTxByAddress(chainName, address, urls)
	case "Nervos":
		err = NervosGetTxByAddress(chainName, address, urls)
	case "SUI", "SUITEST":
		err = SuiGetTxByAddress(chainName, address, urls)
	case "Kaspa":
		err = KaspaGetTxByAddress(chainName, address, urls)
	}

	return
}

func EvmNormalAndInternalGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("EvmNormalAndInternalGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("EvmNormalAndInternalGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.EvmTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var result []EvmApiRecord
	var intxResult []EvmApiRecord
	for _, url := range urls {
		//Get a list of 'Normal' Transactions By Address
		result, err = GetApiTx(chainName, url, "txlist", address, dbLastRecordBlockNumber, dbLastRecordHash)

		if err == nil && len(result) == 0 {
			//Get a list of 'ERC20 - Token Transfer Events' by Address
			result, err = GetApiTx(chainName, url, "tokentx", address, dbLastRecordBlockNumber, dbLastRecordHash)
		}

		if chainName != "Scroll" && err == nil && len(result) == 0 {
			//Get a list of 'ERC721 - Token Transfer Events' by Address
			result, err = GetApiTx(chainName, url, "tokennfttx", address, dbLastRecordBlockNumber, dbLastRecordHash)
		}

		if (chainName == "ETH" || chainName == "Polygon") && err == nil && len(result) == 0 {
			//Get a list of 'ERC1155 - Token Transfer Events' by Address
			result, err = GetApiTx(chainName, url, "token1155tx", address, dbLastRecordBlockNumber, dbLastRecordHash)
		}

		if err == nil && len(result) == 0 {
			//Get a list of 'Internal' Transactions by Address
			intxResult, err = GetApiTx(chainName, url, "txlistinternal", address, dbLastRecordBlockNumber, dbLastRecordHash)
		}

		if err == nil {
			break
		}
	}
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", urls), zap.Any("error", err))
		return err
	}

	var evmTransactionRecordList []*data.EvmTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()

	for _, record := range result {
		txHash := record.Hash
		blockNumber, _ := strconv.Atoi(record.BlockNumber)

		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		nonce, _ := strconv.Atoi(record.Nonce)
		evmRecord := &data.EvmTransactionRecord{
			TransactionHash: txHash,
			BlockHash:       record.BlockHash,
			BlockNumber:     blockNumber,
			Nonce:           int64(nonce),
			FromAddress:     types2.HexToAddress(record.From).Hex(),
			ToAddress:       types2.HexToAddress(record.To).Hex(),
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		evmTransactionRecordList = append(evmTransactionRecordList, evmRecord)
	}

	for _, intxRecord := range intxResult {
		txHash := intxRecord.Hash
		blockNumber, _ := strconv.Atoi(intxRecord.BlockNumber)
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		nonce, _ := strconv.Atoi(intxRecord.Nonce)
		txTime, _ := strconv.Atoi(intxRecord.TimeStamp)
		am, _ := decimal.NewFromString(intxRecord.Value)
		fa := types2.HexToAddress(intxRecord.From).Hex()
		ta := types2.HexToAddress(intxRecord.To).Hex()
		_, fromUid, err1 := UserAddressSwitchRetryAlert(chainName, fa)
		if err1 != nil {
			log.Error("通过用户资产变更爬取交易记录，从redis中获取用户地址失败", zap.Any("chainName", chainName), zap.Any("address", fa), zap.Any("error", err1))
			return
		}
		_, toUid, err2 := UserAddressSwitchRetryAlert(chainName, ta)
		if err2 != nil {
			log.Error("通过用户资产变更爬取交易记录，从redis中获取用户地址失败", zap.Any("chainName", chainName), zap.Any("address", fa), zap.Any("error", err2))
			return
		}
		parseData := ""
		transactionType := NATIVE
		if intxRecord.ContractAddress != "" {
			transactionType = TRANSFER
			tokenInfo, e := GetTokenInfoRetryAlert(context.Background(), chainName, intxRecord.ContractAddress)
			if e == nil {
				tokenInfo.Amount = intxRecord.Value
				tokenInfo.Address = intxRecord.ContractAddress
				evmMap := map[string]interface{}{
					"token": tokenInfo,
				}
				parseData, _ = utils.JsonEncode(evmMap)
			}
		}

		evmRecord := &data.EvmTransactionRecord{
			TransactionHash: txHash,
			BlockHash:       intxRecord.BlockHash,
			BlockNumber:     blockNumber,
			Nonce:           int64(nonce),
			FromAddress:     fa,
			FromUid:         fromUid,
			ToAddress:       ta,
			ToUid:           toUid,
			Status:          SUCCESS,
			TxTime:          int64(txTime),
			ContractAddress: intxRecord.ContractAddress,
			TransactionType: transactionType,
			Amount:          am,
			ParseData:       parseData,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		evmTransactionRecordList = append(evmTransactionRecordList, evmRecord)
	}

	if len(evmTransactionRecordList) > 0 {
		_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), evmTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func GetApiTx(chainName, url, action, address string, dbLastRecordBlockNumber int, dbLastRecordHash string) ([]EvmApiRecord, error) {
	var result []EvmApiRecord
	pageNum := 1
	startBlock := strconv.Itoa(dbLastRecordBlockNumber)
	if strings.Contains(url, "?") {
		url = url + "&"
	} else {
		url = url + "?"
	}
	url = url + "module=account&action=" + action + "&address=" + address + "&startblock=" + startBlock + "&start_block=" + startBlock + "&sort=desc&offset=" + strconv.Itoa(pageSize)
	var err error
chainFlag:
	for {
		reqUrl := url + "&page=" + strconv.Itoa(pageNum)
		var out EvmApiModel
		//查询
		if chainName == "ETH" || chainName == "Optimism" || chainName == "ETC" || chainName == "xDai" {
			err = httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				err = httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
			}
		} else {
			_, err = httpclient.HttpGet(reqUrl, nil, &out, &timeout, nil)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				_, err = httpclient.HttpGet(reqUrl, nil, &out, &timeout, nil)
			}
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s, error：%s", chainName, address, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return nil, err
		}

		status := utils.GetString(out.Status)
		if status == API_SUCCEESS {
			var evmInterface = out.Result.([]interface{})
			dataLen := len(evmInterface)
			if dataLen == 0 {
				break
			}

			for _, evm := range evmInterface {
				var evmApiRecord EvmApiRecord
				temp := evm.(map[string]interface{})
				utils.CopyProperties(temp, &evmApiRecord)
				txHash := evmApiRecord.Hash
				txBlockNumber, _ := strconv.Atoi(evmApiRecord.BlockNumber)
				if txBlockNumber < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
					break chainFlag
				}
				result = append(result, evmApiRecord)
			}
			if dataLen < pageSize {
				break
			}
			pageNum++
		} else {
			if out.Message == "No transactions found" || out.Message == "No token transfers found" || out.Message == "No internal transactions found" {
				return result, nil
			}
			msg := out.Message
			if out.Result != nil {
				if intValue, ok := out.Result.(string); ok {
					msg = intValue
				}
			}
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s, error：%s", chainName, address, msg)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("msg", msg))
			return nil, errors.New(msg)
		}
	}
	return result, nil
}

type CosmosBrowserInfo struct {
	Header struct {
		Id        int       `json:"id"`
		ChainId   string    `json:"chain_id"`
		BlockId   int       `json:"block_id"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"header"`
	Data struct {
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
	} `json:"data"`
}

type OsmosisBrowserInfo struct {
	Txs []struct {
		Body struct {
			Messages []struct {
				Type        string `json:"@type"`
				FromAddress string `json:"from_address"`
				ToAddress   string `json:"to_address"`
				Amount      []struct {
					Denom  string `json:"denom"`
					Amount string `json:"amount"`
				} `json:"amount"`
			} `json:"messages"`
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
	} `json:"txs"`
	TxResponses []struct {
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
				Messages []struct {
					Type        string `json:"@type"`
					FromAddress string `json:"from_address"`
					ToAddress   string `json:"to_address"`
					Amount      []struct {
						Denom  string `json:"denom"`
						Amount string `json:"amount"`
					} `json:"amount"`
				} `json:"messages"`
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
	} `json:"tx_responses"`
	Pagination struct {
		NextKey interface{} `json:"next_key"`
		Total   string      `json:"total"`
	} `json:"pagination"`
}

type SeiErrors struct {
	Errors []struct {
		Extensions struct {
			Code string `json:"code"`
			Path string `json:"path"`
		} `json:"extensions"`
		Message string `json:"message"`
	} `json:"errors"`
}

type SeiAccountId struct {
	Data struct {
		AccountsByPk struct {
			Id int `json:"id"`
		} `json:"accounts_by_pk"`
	} `json:"data"`
	SeiErrors
}

type SeiAccountTransaction struct {
	Block struct {
		Height    int    `json:"height"`
		Timestamp string `json:"timestamp"`
	} `json:"block"`
	Transaction struct {
		/*Account struct {
			Address string `json:"address"`
		} `json:"account"`*/
		Hash string `json:"hash"`
		/*Success  bool   `json:"success"`
		Messages []struct {
			Detail struct {
				Contract string `json:"contract"`
				Funds    []struct {
					Denom  string `json:"denom"`
					Amount string `json:"amount"`
				} `json:"funds"`
				Msg struct {
					ProvideLiquidity struct {
						Assets []struct {
							Amount string `json:"amount"`
							Info   struct {
								NativeToken struct {
									Denom string `json:"denom"`
								} `json:"native_token,omitempty"`
								Token struct {
									ContractAddr string `json:"contract_addr"`
								} `json:"token,omitempty"`
							} `json:"info"`
						} `json:"assets"`
					} `json:"provide_liquidity,omitempty"`
					Send struct {
						Amount   string `json:"amount"`
						Contract string `json:"contract"`
						Msg      string `json:"msg"`
					} `json:"send,omitempty"`
					ExecuteSwapOperations struct {
						MaxSpread      string `json:"max_spread"`
						MinimumReceive string `json:"minimum_receive"`
						Operations     []struct {
							AstroSwap struct {
								AskAssetInfo struct {
									Token struct {
										ContractAddr string `json:"contract_addr"`
									} `json:"token"`
								} `json:"ask_asset_info"`
								OfferAssetInfo struct {
									NativeToken struct {
										Denom string `json:"denom"`
									} `json:"native_token"`
								} `json:"offer_asset_info"`
							} `json:"astro_swap"`
						} `json:"operations"`
					} `json:"execute_swap_operations,omitempty"`
					Faucet struct {
					} `json:"faucet,omitempty"`
				} `json:"msg"`
				MsgJson string `json:"msg_json"`
				Sender  string `json:"sender"`
			} `json:"detail"`
			Type string `json:"type"`
		} `json:"messages"`
		IsClearAdmin  bool `json:"is_clear_admin"`
		IsExecute     bool `json:"is_execute"`
		IsIbc         bool `json:"is_ibc"`
		IsInstantiate bool `json:"is_instantiate"`
		IsMigrate     bool `json:"is_migrate"`
		IsSend        bool `json:"is_send"`
		IsStoreCode   bool `json:"is_store_code"`
		IsUpdateAdmin bool `json:"is_update_admin"`*/
	} `json:"transaction"`
	//IsSigner bool `json:"is_signer"`
}

type SeiAccountTransactions struct {
	Data struct {
		AccountTransactions []*SeiAccountTransaction `json:"account_transactions"`
	} `json:"data"`
	SeiErrors
}

type SeiBrowserInfo struct {
	Hash      string    `json:"hash"`
	Messages  string    `json:"messages"`
	Status    int       `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Height    int       `json:"height"`
	Fee       struct {
		Denom  string `json:"denom"`
		Amount string `json:"amount"`
	} `json:"fee"`
}

type CelestiaBrowserResponse struct {
	Data       []*CelestiaBrowserInfo `json:"data"`
	NextCursor string                 `json:"next_cursor"`
}

type CelestiaBrowserInfo struct {
	Height    int       `json:"height"`
	Hash      string    `json:"hash"`
	Timestamp time.Time `json:"timestamp"`
	Code      int       `json:"code"`
	Messages  []struct {
		Type   string `json:"type"`
		Sender bool   `json:"sender,omitempty"`
		Tokens struct {
			Amount int64  `json:"amount"`
			Denom  string `json:"denom"`
		} `json:"tokens"`
		Recipient bool `json:"recipient,omitempty"`
	} `json:"messages"`
	Fee struct {
		Amount int    `json:"amount"`
		Denom  string `json:"denom"`
	} `json:"fee"`
}

func CosmosGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("CosmosGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("CosmosGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.AtomTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	for _, url := range urls {
		if strings.HasPrefix(url, "https://front.api.mintscan.io/v1") {
			atomTransactionRecordList, err = getRecordByCosmosScan(chainName, url, address, dbLastRecordBlockNumber, dbLastRecordHash)
		} else if strings.HasPrefix(url, "https://proxy.atomscan.com") {
			atomTransactionRecordList, err = getRecordByAtomScan(chainName, url, address, dbLastRecordBlockNumber, dbLastRecordHash)
		} else if url == "https://pacific-1-graphql.alleslabs.dev/v1/graphql" || url == "https://atlantic-2-graphql.alleslabs.dev/v1/graphql" {
			atomTransactionRecordList, err = getSeiRecordByGraphql(chainName, url, address, dbLastRecordBlockNumber, dbLastRecordHash)
		} else if url == "https://sei.api.explorers.guru" {
			atomTransactionRecordList, err = getSeiRecordByExplorers(chainName, url, address, dbLastRecordBlockNumber, dbLastRecordHash)
		} else if url == "https://celestia.api.explorers.guru" {
			atomTransactionRecordList, err = getCelestiaRecordByExplorers(chainName, url, address, dbLastRecordBlockNumber, dbLastRecordHash)
		}

		if err == nil {
			break
		}
	}
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", urls), zap.Any("error", err))
		return err
	}

	if len(atomTransactionRecordList) > 0 {
		_, err = data.AtomTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), atomTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func getRecordByCosmosScan(chainName, url, address string, dbLastRecordBlockNumber int, dbLastRecordHash string) ([]*data.AtomTransactionRecord, error) {
	var starIndex = 0
	url = url + address + "/txs?"

	var chainRecords []*CosmosBrowserInfo
chainFlag:
	for {
		var out []*CosmosBrowserInfo
		reqUrl := url + "limit=" + strconv.Itoa(pageSize) + "&from=" + strconv.Itoa(starIndex)

		err := httpclient.GetResponsePostman(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponsePostman(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		dataLen := len(out)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out {
			txHash := browserInfo.Data.Txhash
			txHeight, err := strconv.Atoi(browserInfo.Data.Height)
			if err != nil {
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常，address:%s", chainName, address)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("blockNumber", browserInfo.Data.Height), zap.Any("txHash", txHash), zap.Any("error", err))
				break chainFlag
			}
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		starIndex = starIndex + dataLen
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Data.Txhash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.AtomTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		atomTransactionRecordList = append(atomTransactionRecordList, atomRecord)
	}

	return atomTransactionRecordList, nil
}

func getRecordByAtomScan(chainName, url, address string, dbLastRecordBlockNumber int, dbLastRecordHash string) ([]*data.AtomTransactionRecord, error) {
	var starIndex = 0
	url = url + "?orderBy=ORDER_BY_DESC&events=transfer.recipient='" + address + "'&"
	var chainRecords []string
chainFlag:
	for {
		var out *OsmosisBrowserInfo
		reqUrl := url + "pagination.limit=" + strconv.Itoa(pageSize) + "&pagination.offset=" + strconv.Itoa(starIndex)

		err := httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		dataLen := len(out.TxResponses)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.TxResponses {
			txHash := browserInfo.Txhash
			txHeight, err := strconv.Atoi(browserInfo.Height)
			if err != nil {
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常，address:%s", chainName, address)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("blockNumber", browserInfo.Height), zap.Any("txHash", txHash), zap.Any("error", err))
				break chainFlag
			}
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, txHash)
		}
		if dataLen < pageSize {
			break
		}
		starIndex = starIndex + dataLen
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, txHash := range chainRecords {
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.AtomTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		atomTransactionRecordList = append(atomTransactionRecordList, atomRecord)
	}

	return atomTransactionRecordList, nil
}

func getAccountIdByAddressQueryDocument(url, address string) (int, error) {
	tokenRequest := map[string]interface{}{
		"operationName": "getAccountIdByAddressQueryDocument",
		"variables": map[string]interface{}{
			"address": address,
		},
		"query": "query getAccountIdByAddressQueryDocument($address: String!) {\n  accounts_by_pk(address: $address) {\n    id\n  }\n}",
	}
	out := &SeiAccountId{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.HttpPostJson(url, tokenRequest, out, &timeoutMS)
	if err != nil {
		return 0, err
	}
	if out.Errors != nil {
		errorsStr, err := utils.JsonEncode(out.Errors)
		if err != nil {
			return 0, err
		}
		return 0, errors.New(errorsStr)
	}

	accountsId := out.Data.AccountsByPk.Id
	return accountsId, nil
}

func getTxsByAddressPagination(url string, accountsId, offset, limit int) (*SeiAccountTransactions, error) {
	tokenRequest := map[string]interface{}{
		"operationName": "getTxsByAddressPagination",
		"variables": map[string]interface{}{
			"expression": map[string]interface{}{
				"account_id": map[string]interface{}{
					"_eq": accountsId,
				},
			},
			"offset":   offset,
			"pageSize": limit,
		},
		"query": "query getTxsByAddressPagination($expression: account_transactions_bool_exp, $offset: Int!, $pageSize: Int!) {\n  account_transactions(\n    where: $expression\n    order_by: {block_height: desc}\n    offset: $offset\n    limit: $pageSize\n  ) {\n    block {\n      height\n      timestamp\n    }\n    transaction {\n      account {\n        address\n      }\n      hash\n      success\n      messages\n      is_clear_admin\n      is_execute\n      is_ibc\n      is_instantiate\n      is_migrate\n      is_send\n      is_store_code\n      is_update_admin\n    }\n    is_signer\n  }\n}",
	}
	out := &SeiAccountTransactions{}
	timeoutMS := 3_000 * time.Millisecond
	err := httpclient.HttpPostJson(url, tokenRequest, out, &timeoutMS)
	if err != nil {
		return nil, err
	}
	if out.Errors != nil {
		errorsStr, err := utils.JsonEncode(out.Errors)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorsStr)
	}

	return out, nil
}

func getSeiRecordByGraphql(chainName, url, address string, dbLastRecordBlockNumber int, dbLastRecordHash string) ([]*data.AtomTransactionRecord, error) {
	accountsId, err := getAccountIdByAddressQueryDocument(url, address)
	if err != nil {
		return nil, err
	}
	dbLastRecordHash = strings.ToLower(dbLastRecordHash)

	offset := 0
	var chainRecords []*SeiAccountTransaction
chainFlag:
	for {
		out, err := getTxsByAddressPagination(url, accountsId, offset, pageSize)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			out, err = getTxsByAddressPagination(url, accountsId, offset, pageSize)
		}
		if err != nil {
			return nil, err
			/*alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break*/
		}

		dataLen := len(out.Data.AccountTransactions)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.Data.AccountTransactions {
			txHash := strings.TrimLeft(browserInfo.Transaction.Hash, "\\x")
			browserInfo.Transaction.Hash = txHash
			txHeight := browserInfo.Block.Height
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		offset = offset + dataLen
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Transaction.Hash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.AtomTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		atomTransactionRecordList = append(atomTransactionRecordList, atomRecord)
	}

	return atomTransactionRecordList, nil
}

func getSeiRecordByExplorers(chainName, url, address string, dbLastRecordBlockNumber int, dbLastRecordHash string) ([]*data.AtomTransactionRecord, error) {
	dbLastRecordHash = strings.ToUpper(dbLastRecordHash)
	url = url + "/api/transactions/by/" + address

	var chainRecords []*SeiBrowserInfo
chainFlag:
	for {
		var out []*SeiBrowserInfo
		reqUrl := url

		err := httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			return nil, err
			/*alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break*/
		}

		dataLen := len(out)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out {
			txHash := browserInfo.Hash
			txHeight := browserInfo.Height
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		break
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Hash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.AtomTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		atomTransactionRecordList = append(atomTransactionRecordList, atomRecord)
	}

	return atomTransactionRecordList, nil
}

func getCelestiaRecordByExplorers(chainName, url, address string, dbLastRecordBlockNumber int, dbLastRecordHash string) ([]*data.AtomTransactionRecord, error) {
	dbLastRecordHash = strings.ToUpper(dbLastRecordHash)
	url = url + "/api/v1/accounts/" + address + "/txs?category=all&limit=" + strconv.Itoa(pageSize) + "&cursor="

	var nextCursor string
	var chainRecords []*CelestiaBrowserInfo
chainFlag:
	for {
		var out CelestiaBrowserResponse
		reqUrl := url + nextCursor

		err := httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			return nil, err
			/*alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break*/
		}

		dataLen := len(out.Data)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.Data {
			txHash := browserInfo.Hash
			txHeight := browserInfo.Height
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		nextCursor = out.NextCursor
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Hash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.AtomTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		atomTransactionRecordList = append(atomTransactionRecordList, atomRecord)
	}

	return atomTransactionRecordList, nil
}

type SolanaBrowserResponse struct {
	Succcess bool                 `json:"succcess"`
	Data     []*SolanaBrowserInfo `json:"data"`
}
type SolanaBrowserInfo struct {
	BlockTime         int      `json:"blockTime"`
	Slot              int      `json:"slot"`
	TxHash            string   `json:"txHash"`
	Fee               int      `json:"fee"`
	Status            string   `json:"status"`
	Lamport           int      `json:"lamport"`
	Signer            []string `json:"signer"`
	ParsedInstruction []struct {
		ProgramId string `json:"programId"`
		Type      string `json:"type"`
	} `json:"parsedInstruction"`
}

type SolanaBrowserTxResponse struct {
	Succcess bool `json:"succcess"`
	Data     struct {
		Tx struct {
			Transactions []*SolanaBrowserTxInfo `json:"transactions"`
			HasNext      bool                   `json:"hasNext"`
			Total        int                    `json:"total"`
		} `json:"tx"`
		Begin int `json:"begin"`
	} `json:"data"`
}
type SolanaBrowserTxInfo struct {
	BlockTime int    `json:"blockTime"`
	Slot      int    `json:"slot"`
	TxHash    string `json:"txHash"`
	Fee       int    `json:"fee"`
	Status    string `json:"status"`
}

type SolanaBeachBrowserInfo struct {
	TransactionHash string `json:"transactionHash"`
	BlockNumber     int    `json:"blockNumber"`
	Blocktime       struct {
		Absolute int `json:"absolute"`
		Relative int `json:"relative"`
	} `json:"blocktime"`
}

type SolanaResponse struct {
	Jsonrpc string        `json:"jsonrpc"`
	Result  []*SolanaInfo `json:"result"`
	Id      int           `json:"id"`
}
type SolanaInfo struct {
	BlockTime          int         `json:"blockTime"`
	ConfirmationStatus string      `json:"confirmationStatus"`
	Err                interface{} `json:"err"`
	Memo               interface{} `json:"memo"`
	Signature          string      `json:"signature"`
	Slot               int         `json:"slot"`
}

func SolanaGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("SolanaGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("SolanaGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	log.Info("QUERY TXNS OF ADDRESS", zap.String("chainName", chainName), zap.String("address", address), zap.Strings("urls", urls))
	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.SolTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordSlotNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordSlotNumber = dbLastRecords[0].SlotNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var solTransactionRecordList []*data.SolTransactionRecord
	for _, url := range urls {
		if url == "https://public-api.solanabeach.io" {
			solTransactionRecordList, err = getRecordBySolanaBeach(chainName, url, address, dbLastRecordSlotNumber, dbLastRecordHash)
		} else if url == "https://api.solscan.io" {
			solTransactionRecordList, err = getRecordBySolscan(chainName, url, address, dbLastRecordSlotNumber, dbLastRecordHash)

			// 一些 SPL 和 SOL 转账的交易记录不会出现在 Transactions 里
			// 调用https://api.solscan.io节点查询SOL Transfers
			if records, err := getTxRecordBySolscan(chainName, "https://api.solscan.io/account/soltransfer", address, dbLastRecordSlotNumber, dbLastRecordHash); err == nil {
				solTransactionRecordList = append(solTransactionRecordList, records...)
			} else {
				log.Error("QUERY TXNS OF ADDRESS SOLSCAN ERROR", zap.String("chainName", chainName), zap.String("address", address), zap.String("stage", "soltransfer"), zap.Error(err))
			}
			// 调用https://api.solscan.io节点查询SPL Transfers
			if records, err := getTxRecordBySolscan(chainName, "https://api.solscan.io/account/token", address, dbLastRecordSlotNumber, dbLastRecordHash); err == nil {
				solTransactionRecordList = append(solTransactionRecordList, records...)
			} else {
				log.Error("QUERY TXNS OF ADDRESS SOLSCAN ERROR", zap.String("chainName", chainName), zap.String("address", address), zap.String("stage", "token"), zap.Error(err))
			}
		} else {
			solTransactionRecordList, err = getRecordByRpcNode(chainName, url, address, dbLastRecordSlotNumber, dbLastRecordHash)
		}

		if err == nil {
			break
		}
	}
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", urls), zap.Any("error", err))
		return err
	}

	if len(solTransactionRecordList) > 0 {
		_, err = data.SolTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), solTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func batchSaveChaindataTxns(chainName string, txns []string) error {
	chainType, _ := GetChainNameType(chainName)
	switch chainType {
	case SOLANA:
		return batchSaveSolTxns(chainName, txns)
	case COSMOS:
		return batchSaveCosmosTxns(chainName, txns)
	}
	return nil
}

func batchSaveSolTxns(chainName string, txns []string) error {
	if len(txns) == 0 {
		return nil
	}
	now := time.Now().Unix()
	solTransactionRecordList := make([]*data.SolTransactionRecord, 0, len(txns))
	for _, txHash := range txns {
		solTransactionRecordList = append(solTransactionRecordList, &data.SolTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		})
	}

	_, err := data.SolTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), solTransactionRecordList)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过客户端查询交易记录，插入链上交易记录数据到数据库中失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("kanban")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过客户端查询交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("error", err))
		return err
	}
	return nil
}

func batchSaveCosmosTxns(chainName string, txns []string) error {
	now := time.Now().Unix()
	atomTransactionRecordList := make([]*data.AtomTransactionRecord, 0, len(txns))
	for _, txHash := range txns {
		atomTransactionRecordList = append(atomTransactionRecordList, &data.AtomTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		})
	}

	_, err := data.AtomTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), atomTransactionRecordList)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过客户端查询交易记录，插入链上交易记录数据到数据库中失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("kanban")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过客户端查询交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("error", err))
		return err
	}
	return nil
}

func getRecordBySolanaBeach(chainName, url, address string, dbLastRecordSlotNumber int, dbLastRecordHash string) ([]*data.SolTransactionRecord, error) {
	var cursor int
	var beforeTxHash string
	url = url + "/v1/account/" + address + "/transactions?"
	var chainRecords []*SolanaBeachBrowserInfo
chainFlag:
	for {
		var out []*SolanaBeachBrowserInfo
		reqUrl := url
		if beforeTxHash != "" {
			reqUrl = url + "cursor=" + strconv.Itoa(cursor) + "%2C0&before=" + beforeTxHash
		}

		err := httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			return nil, err
			/*alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break*/
		}

		dataLen := len(out)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out {
			txHash := strings.TrimRight(browserInfo.TransactionHash, " ")
			browserInfo.TransactionHash = txHash
			txHeight := browserInfo.BlockNumber
			if txHeight < dbLastRecordSlotNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		cursor = out[dataLen-1].BlockNumber
		beforeTxHash = out[dataLen-1].TransactionHash
	}

	var solTransactionRecordList []*data.SolTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TransactionHash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		solRecord := &data.SolTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		solTransactionRecordList = append(solTransactionRecordList, solRecord)
	}

	return solTransactionRecordList, nil
}

func getRecordBySolscan(chainName, url, address string, dbLastRecordSlotNumber int, dbLastRecordHash string) ([]*data.SolTransactionRecord, error) {
	var beforeTxHash string
	url = url + "/account/transaction?address=" + address

	var chainRecords []*SolanaBrowserInfo
chainFlag:
	for {
		var out SolanaBrowserResponse
		reqUrl := url + "&before=" + beforeTxHash

		err := httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		}
		if err != nil {
			return nil, err
			/*alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break*/
		}

		dataLen := len(out.Data)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.Data {
			txHash := browserInfo.TxHash
			txSlot := browserInfo.Slot
			if txSlot < dbLastRecordSlotNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		beforeTxHash = out.Data[dataLen-1].TxHash
	}

	var solTransactionRecordList []*data.SolTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TxHash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		solRecord := &data.SolTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		solTransactionRecordList = append(solTransactionRecordList, solRecord)
	}
	return solTransactionRecordList, nil
}

func getTxRecordBySolscan(chainName, url, address string, dbLastRecordSlotNumber int, dbLastRecordHash string) ([]*data.SolTransactionRecord, error) {
	offset := 0
	url = url + "/txs?address=" + address + "&limit=" + strconv.Itoa(pageSize) + "&offset="

	var chainRecords []*SolanaBrowserTxInfo
chainFlag:
	for {
		var out SolanaBrowserTxResponse
		reqUrl := url + strconv.Itoa(offset)

		err := httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		}
		if err != nil {
			return nil, err
			/*alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break*/
		}

		dataLen := len(out.Data.Tx.Transactions)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.Data.Tx.Transactions {
			txHash := browserInfo.TxHash
			txSlot := browserInfo.Slot
			if txSlot < dbLastRecordSlotNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		offset = offset + dataLen
	}

	var solTransactionRecordList []*data.SolTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TxHash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		solRecord := &data.SolTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		solTransactionRecordList = append(solTransactionRecordList, solRecord)
	}
	return solTransactionRecordList, nil
}

func getRecordByRpcNode(chainName, url, address string, dbLastRecordSlotNumber int, dbLastRecordHash string) ([]*data.SolTransactionRecord, error) {
	var beforeTxHash string
	method := "getSignaturesForAddress"

	var chainRecords []*SolanaInfo
chainFlag:
	for {
		var out []*SolanaInfo
		var params []interface{}
		if beforeTxHash == "" {
			params = []interface{}{address, map[string]interface{}{"limit": pageSize}}
		} else {
			params = []interface{}{address, map[string]interface{}{"before": beforeTxHash, "limit": pageSize}}
		}
		_, err := httpclient.JsonrpcCall(url, ID, JSONRPC, method, &out, params, &timeout)
		for i := 0; i < 1 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			_, err = httpclient.JsonrpcCall(url, ID, JSONRPC, method, &out, params, &timeout)
		}
		if err != nil {
			return nil, err
			/*alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break*/
		}

		dataLen := len(out)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out {
			txHash := browserInfo.Signature
			txSlot := browserInfo.Slot
			if txSlot < dbLastRecordSlotNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		beforeTxHash = out[dataLen-1].Signature
	}

	var solTransactionRecordList []*data.SolTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Signature
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		solRecord := &data.SolTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		solTransactionRecordList = append(solTransactionRecordList, solRecord)
	}
	return solTransactionRecordList, nil
}

type TokenRequest struct {
	OperationName string    `json:"operationName"`
	Variables     Variables `json:"variables"`
	Query         string    `json:"query"`
}
type Variables struct {
	Address string `json:"address"`
	Offset  int    `json:"offset"`
	Limit   int    `json:"limit"`
}
type AptosBrowserResponse struct {
	Data struct {
		MoveResources []*AptosBrowserInfo `json:"move_resources"`
	} `json:"data"`
}
type AptosBrowserInfo struct {
	TransactionVersion int    `json:"transaction_version"`
	Typename           string `json:"__typename"`
}

func AptosGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("AptosGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("AptosGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.AptTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordVersion int
	if len(dbLastRecords) > 0 {
		dbLastRecordVersion = dbLastRecords[0].TransactionVersion
	}

	var starIndex = 0
	url := urls[0]

	var chainRecords []*AptosBrowserInfo
chainFlag:
	for {
		var out AptosBrowserResponse

		tokenRequest := TokenRequest{
			OperationName: "AccountTransactionsData",
			Variables: Variables{
				Address: address,
				Offset:  starIndex,
				Limit:   pageSize,
			},
			Query: "query AccountTransactionsData($address: String, $limit: Int, $offset: Int) {\n  move_resources(\n    where: {address: {_eq: $address}}\n    order_by: {transaction_version: desc}\n    distinct_on: transaction_version\n    limit: $limit\n    offset: $offset\n  ) {\n    transaction_version\n    __typename\n  }\n}",
		}
		err = httpclient.HttpPostJson(url, tokenRequest, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.HttpPostJson(url, tokenRequest, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", url), zap.Any("error", err))
			break
		}

		dataLen := len(out.Data.MoveResources)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.Data.MoveResources {
			txVersion := browserInfo.TransactionVersion
			if txVersion <= dbLastRecordVersion {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		starIndex += pageSize
	}

	var aptTransactionRecordList []*data.AptTransactionRecord
	transactionRecordMap := make(map[int]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txVersion := record.TransactionVersion
		if _, ok := transactionRecordMap[txVersion]; !ok {
			transactionRecordMap[txVersion] = ""
		} else {
			continue
		}
		aptRecord := &data.AptTransactionRecord{
			TransactionVersion: txVersion,
			Status:             PENDING,
			CreatedAt:          now,
			UpdatedAt:          now,
		}
		aptTransactionRecordList = append(aptTransactionRecordList, aptRecord)
	}

	if len(aptTransactionRecordList) > 0 {
		_, err = data.AptTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), aptTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

type StarcoinBrowserResponse struct {
	Contents []*StarcoinBrowserInfo `json:"contents"`
	Total    int                    `json:"total"`
}

type StarcoinBrowserInfo struct {
	Id                     string `json:"_id"`
	BlockHash              string `json:"block_hash"`
	BlockNumber            string `json:"block_number"`
	EventRootHash          string `json:"event_root_hash"`
	GasUsed                string `json:"gas_used"`
	StateRootHash          string `json:"state_root_hash"`
	Status                 string `json:"status"`
	Timestamp              int64  `json:"timestamp"`
	TransactionGlobalIndex int    `json:"transaction_global_index"`
	TransactionHash        string `json:"transaction_hash"`
	TransactionIndex       int    `json:"transaction_index"`
	TransactionType        string `json:"transaction_type"`
	UserTransaction        struct {
		Authenticator struct {
			Ed25519 struct {
				PublicKey string `json:"public_key"`
				Signature string `json:"signature"`
			} `json:"Ed25519"`
		} `json:"authenticator"`
		RawTxn struct {
			ChainId                 int    `json:"chain_id"`
			ExpirationTimestampSecs string `json:"expiration_timestamp_secs"`
			GasTokenCode            string `json:"gas_token_code"`
			GasUnitPrice            string `json:"gas_unit_price"`
			MaxGasAmount            string `json:"max_gas_amount"`
			Payload                 string `json:"payload"`
			Sender                  string `json:"sender"`
			SequenceNumber          string `json:"sequence_number"`
			TransactionHash         string `json:"transaction_hash"`
		} `json:"raw_txn"`
		TransactionHash string `json:"transaction_hash"`
	} `json:"user_transaction"`
}

func StarcoinGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("StarcoinGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("StarcoinGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.StcTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var pageNum = 1
	url := urls[0]
	url = url + "/transaction/address/main/" + address + "/page/"

	var chainRecords []*StarcoinBrowserInfo
chainFlag:
	for {
		var out StarcoinBrowserResponse
		reqUrl := url + strconv.Itoa(pageNum)

		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		dataLen := len(out.Contents)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.Contents {
			txHash := browserInfo.TransactionHash
			txHeight, err := strconv.Atoi(browserInfo.BlockNumber)
			if err != nil {
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常，address:%s", chainName, address)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("blockNumber", browserInfo.BlockNumber), zap.Any("txHash", txHash), zap.Any("error", err))
				break chainFlag
			}
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		pageNum++
	}

	var stcTransactionRecordList []*data.StcTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TransactionHash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.StcTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		stcTransactionRecordList = append(stcTransactionRecordList, atomRecord)
	}

	if len(stcTransactionRecordList) > 0 {
		_, err = data.StcTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), stcTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func KlaytnGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("KlaytnGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("KlaytnGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	pageNum := 1
	url := urls[0]
	//klaytn 浏览器只支持 25 的limit
	url = url + address + "/txs?limit=25&page="

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.EvmTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var klayRecords []KlaytnRecord

chainFlag:
	for {
		var out KlaytnApiModel
		reqUrl := url + strconv.Itoa(pageNum)
		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}
		if out.Success {
			if out.Total == 0 {
				break
			}
			//处理交易记录
			for _, browserInfo := range out.Result {
				txHash := browserInfo.TxHash
				txHeight := browserInfo.BlockNumber
				if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
					break chainFlag
				}
				klayRecords = append(klayRecords, browserInfo)
			}
			pageNum++
		} else {
			log.Warn(chainName+"调用浏览器接口失败", zap.Any("result", out))
		}
	}

	if len(klayRecords) == 0 {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更api查询, 未查出结果，但是资产有变动", chainName)
		log.Warn(alarmMsg, zap.Any("chainName", chainName), zap.Any("lastTx", klayRecords))
	} else {
		var evmTransactionRecordList []*data.EvmTransactionRecord
		transactionRecordMap := make(map[string]string)
		now := time.Now().Unix()
		for _, record := range klayRecords {
			txHash := record.TxHash
			if _, ok := transactionRecordMap[txHash]; !ok {
				transactionRecordMap[txHash] = ""
			} else {
				continue
			}
			evmRecord := &data.EvmTransactionRecord{
				TransactionHash: txHash,
				BlockNumber:     record.BlockNumber,
				FromAddress:     types2.HexToAddress(record.FromAddress).Hex(),
				ToAddress:       types2.HexToAddress(record.ToAddress).Hex(),
				Status:          PENDING,
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			evmTransactionRecordList = append(evmTransactionRecordList, evmRecord)
		}

		if len(evmTransactionRecordList) > 0 {
			_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), evmTransactionRecordList)
			if err != nil {
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
				return err
			}
		}
	}
	return
}

func ZkSyncGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("ZkSyncGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("ZkSyncGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	url := urls[0]
	//https://zksync2-mainnet-explorer.zksync.io/transactions?limit=50&direction=older&accountAddress=0xb7B4D65CB5a0c44cCB9019ca74745686188173Db
	//只支持最新五十条
	//https://block-explorer-api.mainnet.zksync.io/transactions?address=0xb68Ad129c20d60B18c11735E16dCFd53294960BF&pageSize=10&page=1

	reqUrl := url + "?pageSize=10&page=1&address=" + address

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.EvmTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var out ZksyncApiModel
	err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
	}
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
	}
	var zkTransactionRecordList []*data.EvmTransactionRecord
	if len(out.Items) > 0 {
		transactionRecordMap := make(map[string]string)
		now := time.Now().Unix()
		for _, zkRecord := range out.Items {
			txHash := zkRecord.Hash
			if _, ok := transactionRecordMap[txHash]; !ok {
				transactionRecordMap[txHash] = ""
			} else {
				continue
			}
			if zkRecord.BlockNumber < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break
			}
			zkEvmRecord := &data.EvmTransactionRecord{
				TransactionHash: txHash,
				FromAddress:     zkRecord.From,
				ToAddress:       zkRecord.To,
				Status:          PENDING,
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			zkTransactionRecordList = append(zkTransactionRecordList, zkEvmRecord)
		}
	}

	if len(zkTransactionRecordList) > 0 {
		_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), zkTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}
	return
}

func RoninGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("ZkSyncGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("ZkSyncGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	pageNum := 0
	url := urls[0]
	//- https://explorerv3-api.roninchain.com/txs/
	//- https://explorerv3-api.roninchain.com/txs/0xb7B4D65CB5a0c44cCB9019ca74745686188173Db?from=0&size=100
	url = url + address + "?size=100&from="

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.EvmTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}
	var chainRecords []*RoninApiRecord
chainFlag:
	for {
		var out RoninApiModel
		reqUrl := url + strconv.Itoa(pageNum)

		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		if out.Total == 0 {
			break
		}
		for _, browserInfo := range out.Results {
			txHash := browserInfo.Hash
			txHeight := browserInfo.BlockNumber
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		pageNum++
	}

	var evmTransactionRecordList []*data.EvmTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Hash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		evmRecord := &data.EvmTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			FromAddress:     types2.HexToAddress(record.From).Hex(),
			ToAddress:       types2.HexToAddress(record.To).Hex(),
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		evmTransactionRecordList = append(evmTransactionRecordList, evmRecord)
	}

	if len(evmTransactionRecordList) > 0 {
		_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), evmTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func CasperGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("ZkSyncGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("ZkSyncGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	var chainRecords []*CasperApiRecord

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "tx_time desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.CsprTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordHash string
	var txTime int64
	if len(dbLastRecords) > 0 {
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
		txTime = dbLastRecords[0].TxTime

	}

	result, err := ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		if c, ok := client.(RPCAccountHash); ok {
			return c.GetAccountHashByAddress(address)
		}
		return "", errors.New("not supported")
	})

	if err != nil {
		log.Warn("casper 节点均不可用！")
		return err
	}
	addressHash := result.(string)
	ahs := strings.Split(addressHash, "-")
	if len(ahs) >= 3 {
		addressHash = ahs[len(ahs)-1]
	}

	url := urls[0]
	inTxUrl := url + addressHash + "/transfers?page=1&limit=100&with_extended_info=1&with_amounts_in_currency_id=1"
	//txUrl := url + address + "/extended-deploys?page=1&limit=100&fields=entry_point,contract_package&with_amounts_in_currency_id=1"
	//cr := CasperTransferAndextendedDeploys(txUrl,address,chainName,dbLastRecordHash)
	cir := CasperTransferAndextendedDeploys(inTxUrl, address, chainName, dbLastRecordHash, txTime)
	//chainRecords = append(chainRecords,cr...)
	chainRecords = append(chainRecords, cir...)

	var csprTransactionRecordList []*data.CsprTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.DeployHash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		csprRecord := &data.CsprTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		csprTransactionRecordList = append(csprTransactionRecordList, csprRecord)
	}

	if len(csprTransactionRecordList) > 0 {
		_, err = data.CsprTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), csprTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}
	return
}

// 支持最近一百笔交易
func CasperTransferAndextendedDeploys(url string, address string, chainName string, dbLastRecordHash string, txTime int64) []*CasperApiRecord {
	var out CasperApiModel
	var chainRecords []*CasperApiRecord

	err := httpclient.GetResponse(url, nil, &out, &timeout)
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		err = httpclient.GetResponse(url, nil, &out, &timeout)
	}
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", url), zap.Any("error", err))
	}
	if out.ItemCount > 0 {
		for _, browserInfo := range out.Data {
			txHash := browserInfo.DeployHash
			tt := browserInfo.Timestamp.Unix()
			if tt < txTime || txHash == dbLastRecordHash {
				break
			}
			chainRecords = append(chainRecords, browserInfo)
		}
	}
	return chainRecords
}

func DogeGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("DogeGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("DogeGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	pageNum := 1
	url := urls[0]
	// https://dogechain.info/api/v1/address/transactions/D5Mn3Xkmre74v6Z1xG6cpzHyTv1EATY5Ee/1
	url = url + address + "/"

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "tx_time desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.BtcTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordHash string
	var txTime int64
	if len(dbLastRecords) > 0 {
		dbLastRecordHash = dbLastRecords[0].TransactionHash
		txTime = dbLastRecords[0].TxTime
	}

	var chainRecords []*DogeApiRecord
chainFlag:
	for {
		var out DogeApiModel
		reqUrl := url + strconv.Itoa(pageNum)
		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		if strconv.Itoa(out.Success) == API_SUCCEESS {
			if len(out.Transactions) == 0 {
				break
			}
			for _, browserInfo := range out.Transactions {
				txHash := browserInfo.Hash
				tt := int64(browserInfo.Time)
				if tt < txTime || txHash == dbLastRecordHash {
					break chainFlag
				}
				chainRecords = append(chainRecords, browserInfo)
			}
			pageNum++
		} else {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常, error：%s", chainName, out.Error)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}
	}
	var btcTransactionRecordList []*data.BtcTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Hash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		btcRecord := &data.BtcTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		btcTransactionRecordList = append(btcTransactionRecordList, btcRecord)
	}

	if len(btcTransactionRecordList) > 0 {
		_, err = data.BtcTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), btcTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func LtcGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("LtcGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("LtcGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	url := urls[0]
	url = url + address + "/txs"

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "tx_time desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.BtcTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordHash = dbLastRecords[0].TransactionHash
	}
	var out []LtcApiRecord
	err = httpclient.HttpsGetForm(url, nil, &out, &timeout)
	var btcTransactionRecordList []*data.BtcTransactionRecord
	transactionRecordMap := make(map[string]string)
	if len(out) > 0 {
		for _, arg := range out {
			txHash := arg.Txid
			if txHash == dbLastRecordHash {
				break
			}
			now := time.Now().Unix()

			if _, ok := transactionRecordMap[txHash]; !ok {
				transactionRecordMap[txHash] = ""

				btcRecord := &data.BtcTransactionRecord{
					TransactionHash: txHash,
					Status:          PENDING,
					CreatedAt:       now,
					UpdatedAt:       now,
				}
				btcTransactionRecordList = append(btcTransactionRecordList, btcRecord)
			}
		}
	}

	if len(btcTransactionRecordList) > 0 {
		_, err = data.BtcTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), btcTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func DotGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("DotGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("DotGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	pageNum := 0
	url := urls[0]

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "tx_time desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.DotTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordHash string
	var txTime int64
	if len(dbLastRecords) > 0 {
		dbLastRecordHash = dbLastRecords[0].TransactionHash
		txTime = dbLastRecords[0].TxTime
	}

	var chainRecords []*PolkadotApiRecord
chainFlag:
	for {
		var out PolkadotApiModel
		key, baseURL := parseKeyFromNodeURL(url)
		param := PolkadotApiReq{
			Address:   address,
			Direction: "all",
			Page:      pageNum,
			Row:       50,
		}

		err = httpclient.HttpRequest(baseURL, http.MethodPost, map[string]string{"Authorization": key}, nil, param, &out, &timeout, nil)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.HttpRequest(baseURL, http.MethodPost, map[string]string{"Authorization": key}, nil, param, &out, &timeout, nil)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", baseURL), zap.Any("error", err))
			break
		}

		//成功
		if out.Code == 0 {
			if out.Data.Transfers == nil || len(out.Data.Transfers) == 0 {
				break
			}
			for _, browserInfo := range out.Data.Transfers {
				txHash := browserInfo.Hash
				tt := int64(browserInfo.BlockTimestamp)
				if tt < txTime || txHash == dbLastRecordHash {
					break chainFlag
				}
				chainRecords = append(chainRecords, browserInfo)
			}
			pageNum++
		} else {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常，address:%s, error：%s", chainName, address, out.Message)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", url), zap.Any("error", err))
			break
		}
	}
	var dotTransactionRecordList []*data.DotTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Hash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		dotRecord := &data.DotTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			FromAddress:     record.From,
			ToAddress:       record.To,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		dotTransactionRecordList = append(dotTransactionRecordList, dotRecord)
	}

	if len(dotTransactionRecordList) > 0 {
		_, err = data.DotTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), dotTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func parseKeyFromNodeURL(nodeURL string) (key, restURL string) {
	parsed, err := url.Parse(nodeURL)
	if err != nil {
		return "", nodeURL
	}
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		key = fmt.Sprintf("%s %s", parsed.User.Username(), password)
		parsed.User = nil
		restURL = parsed.String()
		// log.Debug("DOGE PARSED KEY FROM URL", zap.String("key", key), zap.String("url", restURL))
		return
	}
	return "", nodeURL
}

func BTCGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("BTCGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("BTCGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	//角标
	offset := 0
	limit := 100
	url := urls[0]
	url = url + address + "/transactions?limit=" + strconv.Itoa(limit) + "&offset="

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.BtcTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = dbLastRecords[0].TransactionHash
	}

	var chainRecords []*BTCApiModel
chainFlag:
	for {
		var out []*BTCApiModel
		reqUrl := url + strconv.Itoa(offset)

		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		dataLen := len(out)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out {
			txHash := browserInfo.Txid
			txHeight := browserInfo.Block.Height
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < limit {
			break
		} else {
			offset = offset + dataLen
		}
	}

	var btcTransactionRecordList []*data.BtcTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Txid
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		btcRecord := &data.BtcTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			BlockNumber:     record.Block.Height,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		btcTransactionRecordList = append(btcTransactionRecordList, btcRecord)
	}

	if len(btcTransactionRecordList) > 0 {
		_, err = data.BtcTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), btcTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}
	return
}

func UpdateUtxoByAddress(chainName string, address string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("UpdateUtxoByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("UpdateUtxoByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过余额更新utxo失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	err = RefreshUserUTXO(chainName, address)

	return
}

func RefreshUserUTXO(chainName, address string) (err error) {
	userAsset, err := data.UserAssetRepoClient.GetByChainNameAndAddress(nil, chainName, address, "")
	if err != nil {
		return err
	}

	platInfo, _ := GetChainPlatInfo(chainName)
	decimals := int(platInfo.Decimal)

	//从 oklink 获取 utxo
	utxos, err := data.OklinkRepoClient.GetUtxo(chainName, address)

	//如果查出来没有，则不更新
	if len(utxos) == 0 {
		return nil
	}

	_, _ = data.UtxoUnspentRecordRepoClient.DeleteByAddressWithNotPending(nil, chainName, address)

	for _, utxo := range utxos {
		index, _ := strconv.Atoi(utxo.Index)
		amountDecimal, _ := decimal.NewFromString(utxo.UnspentAmount)
		amount := Pow10(amountDecimal, decimals).BigInt().String()
		txTime, _ := strconv.ParseInt(utxo.BlockTime, 10, 64)
		var utxoUnspentRecord = &data.UtxoUnspentRecord{
			Uid:       userAsset.Uid,
			Hash:      utxo.Txid,
			N:         index,
			ChainName: chainName,
			Address:   address,
			Unspent:   data.UtxoStatusUnSpend, //1 未花费 2 已花费 联合索引
			Amount:    amount,
			TxTime:    txTime,
			UpdatedAt: time.Now().Unix(),
		}
		//插入所有未花费的UTXO
		r, err := data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxoUnspentRecord)
		if err != nil {
			log.Error("更新用户UTXO，将数据插入到数据库中", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("插入utxo对象结果", r), zap.Any("error", err))
		}
	}

	//查询pending的UTXO，与已有的UTXO取差值，差值为已花费的UTXO
	userUTXOs := make([]string, len(utxos))
	for i, utxo := range utxos {
		userUTXOs[i] = fmt.Sprintf("%s#%s", utxo.Txid, utxo.Index)
	}

	pendingUTXOs, err := data.UtxoUnspentRecordRepoClient.FindByCondition(nil, &v1.UnspentReq{IsUnspent: strconv.Itoa(data.UtxoStatusPending), Address: address})
	for _, utxo := range pendingUTXOs {
		u := fmt.Sprintf("%s#%d", utxo.Hash, utxo.N)
		if !slices.Contains(userUTXOs, u) {
			utxo.Unspent = data.UtxoStatusSpent
			data.UtxoUnspentRecordRepoClient.SaveOrUpdate(nil, utxo)
		}
	}
	return nil
}

func TrxGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("TrxGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("TrxGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	url := urls[0]

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.TrxTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var txTime int64
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		txTime = dbLastRecords[0].TxTime
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}
	//start 是角标 limit 最大是 50
	normalTx := GetTronApiTx(url, "transaction", address, chainName, txTime, dbLastRecordHash)
	internalTx := GetTronApiTx(url, "internal-transaction", address, chainName, txTime, dbLastRecordHash)
	var trxTransactionRecordList []*data.TrxTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	if normalTx != nil {
		for _, record := range normalTx {
			txHash := record.Hash
			if _, ok := transactionRecordMap[txHash]; !ok {
				transactionRecordMap[txHash] = ""
			} else {
				continue
			}
			trxRecord := &data.TrxTransactionRecord{
				TransactionHash: txHash,
				Status:          PENDING,
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			trxTransactionRecordList = append(trxTransactionRecordList, trxRecord)
		}
	}

	if internalTx != nil {
		for _, inRecord := range internalTx {
			txHash := inRecord.Hash
			if _, ok := transactionRecordMap[txHash]; !ok {
				transactionRecordMap[txHash] = ""
			} else {
				continue
			}
			trxInRecord := &data.TrxTransactionRecord{
				TransactionHash: txHash,
				Status:          PENDING,
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			trxTransactionRecordList = append(trxTransactionRecordList, trxInRecord)
		}
	}

	if len(trxTransactionRecordList) > 0 {
		_, err = data.TrxTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), trxTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}
	return
}

// tron offset 为 角标
func GetTronApiTx(url string, actionTx string, address string, chainName string, txTime int64, dbLastRecordHash string) []*TornApiRecord {
	offset := 0
	limit := 50
	var err error
	url = url + actionTx + "?sort=-timestamp&count=true&limit=" + strconv.Itoa(limit) + "&address=" + address
	var chainRecords []*TornApiRecord
chainFlag:
	for {
		reqUrl := url + "&start=" + strconv.Itoa(offset)
		var out TornApiModel
		//查询
		_, err = httpclient.HttpGet(reqUrl, nil, &out, &timeout, nil)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			_, err = httpclient.HttpGet(reqUrl, nil, &out, &timeout, nil)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更api查询失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return nil
		}
		if out.Message != "" || out.Total == 0 {
			log.Error(out.Message)
			return nil
		}

		for _, browserInfo := range out.Data {
			txHash := browserInfo.Hash
			tt := browserInfo.Timestamp / 1000
			if tt < txTime || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if out.Total == len(chainRecords) {
			break
		}
		if len(out.Data) == limit {
			offset = offset + limit
		}

	}
	return chainRecords
}

type NervosBrowserResponse struct {
	Data []*NervosBrowserInfo `json:"data"`
	Meta struct {
		Total    int `json:"total"`
		PageSize int `json:"page_size"`
	} `json:"meta"`
}

type NervosBrowserInfo struct {
	Id         string `json:"id"`
	Type       string `json:"type"`
	Attributes struct {
		IsCellbase          bool   `json:"is_cellbase"`
		TransactionHash     string `json:"transaction_hash"`
		BlockNumber         string `json:"block_number"`
		BlockTimestamp      string `json:"block_timestamp"`
		DisplayInputsCount  int    `json:"display_inputs_count"`
		DisplayOutputsCount int    `json:"display_outputs_count"`
		DisplayInputs       []struct {
			Id              string `json:"id"`
			FromCellbase    bool   `json:"from_cellbase"`
			Capacity        string `json:"capacity"`
			AddressHash     string `json:"address_hash"`
			GeneratedTxHash string `json:"generated_tx_hash"`
			CellIndex       string `json:"cell_index"`
			CellType        string `json:"cell_type"`
			Since           struct {
				Raw             string `json:"raw"`
				MedianTimestamp string `json:"median_timestamp"`
			} `json:"since"`
		} `json:"display_inputs"`
		DisplayOutputs []struct {
			Id             string `json:"id"`
			Capacity       string `json:"capacity"`
			AddressHash    string `json:"address_hash"`
			Status         string `json:"status"`
			ConsumedTxHash string `json:"consumed_tx_hash"`
			CellType       string `json:"cell_type"`
		} `json:"display_outputs"`
		Income string `json:"income"`
	} `json:"attributes"`
}

func NervosGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("NervosGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("NervosGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.CkbTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var pageNum = 1
	url := urls[0]
	url = url + "/address_transactions/" + address + "?page_size=" + strconv.Itoa(pageSize) + "&page="

	var chainRecords []*NervosBrowserInfo
chainFlag:
	for {
		var out NervosBrowserResponse
		reqUrl := url + strconv.Itoa(pageNum)

		err = httpclient.GetResponseApiJson(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponseApiJson(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		dataLen := len(out.Data)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out.Data {
			txHash := browserInfo.Attributes.TransactionHash
			txHeight, err := strconv.Atoi(browserInfo.Attributes.BlockNumber)
			if err != nil {
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常，address:%s", chainName, address)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("blockNumber", browserInfo.Attributes.BlockNumber), zap.Any("txHash", txHash), zap.Any("error", err))
				break chainFlag
			}
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		pageNum++
	}

	var ckbTransactionRecordList []*data.CkbTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Attributes.TransactionHash
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.CkbTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		ckbTransactionRecordList = append(ckbTransactionRecordList, atomRecord)
	}

	if len(ckbTransactionRecordList) > 0 {
		_, err = data.CkbTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), ckbTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

func SuiGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("SuiGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("SuiGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "block_number desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.SuiTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	url := urls[0]

	var chainRecords []*SuiBrowserInfo
	chainRecordMap := make(map[string]*SuiBrowserInfo)
	addressKey := "FromAddress"
	var startAddress interface{}
chainFlag:
	for {
		out, err := SuiGetTransactionByHash(url, addressKey, address, startAddress, pageSize, &timeout)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", url), zap.Any("error", err))
			break
		}

		dataLen := len(out.Data)
		if dataLen == 0 {
			if addressKey == "FromAddress" {
				addressKey = "ToAddress"
				startAddress = nil
				continue
			} else {
				break
			}
		}
		for _, browserInfo := range out.Data {
			var txHeight int
			txHash := browserInfo.Digest
			if browserInfo.Checkpoint != "" {
				txHeight, err = strconv.Atoi(browserInfo.Checkpoint)
			}
			if err != nil {
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常，address:%s", chainName, address)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", url), zap.Any("blockNumber", browserInfo.Checkpoint), zap.Any("txHash", txHash), zap.Any("error", err))
				break chainFlag
			}
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				if addressKey == "FromAddress" {
					addressKey = "ToAddress"
					startAddress = nil
					continue chainFlag
				} else {
					break chainFlag
				}
			}
			chainRecordMap[browserInfo.Digest] = browserInfo
		}

		if !out.HasNextPage {
			if addressKey == "FromAddress" {
				addressKey = "ToAddress"
				startAddress = nil
				continue
			} else {
				break
			}
		}
		startAddress = out.NextCursor
	}

	for _, browserInfo := range chainRecordMap {
		chainRecords = append(chainRecords, browserInfo)
	}

	var suiTransactionRecordList []*data.SuiTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Digest
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		atomRecord := &data.SuiTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		suiTransactionRecordList = append(suiTransactionRecordList, atomRecord)
	}

	if len(suiTransactionRecordList) > 0 {
		_, err = data.SuiTransactionRecordRepoClient.BatchSaveOrIgnore(nil, GetTableName(chainName), suiTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

type SuiBrowserResponse struct {
	Data        []*SuiBrowserInfo `json:"data"`
	NextCursor  string            `json:"nextCursor"`
	HasNextPage bool              `json:"hasNextPage"`
}

type SuiBrowserInfo struct {
	Digest         string `json:"digest"`
	RawTransaction string `json:"rawTransaction"`
	TimestampMs    string `json:"timestampMs"`
	Checkpoint     string `json:"checkpoint"`
}

func SuiGetTransactionByHash(url, addressKey, address string, startAddress interface{}, pageSize int, timeout *time.Duration) (*SuiBrowserResponse, error) {
	method := "suix_queryTransactionBlocks"
	var out *SuiBrowserResponse
	params := []interface{}{
		map[string]interface{}{
			"filter": map[string]string{
				addressKey: address,
			},
			"options": map[string]bool{
				"showInput":          false,
				"showRawInput":       true,
				"showEffects":        false,
				"showEvents":         false,
				"showObjectChanges":  false,
				"showBalanceChanges": false,
			},
		},
		startAddress,
		pageSize,
		true,
	}
	_, err := httpclient.JsonrpcCall(url, ID, JSONRPC, method, &out, params, timeout)
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		_, err = httpclient.JsonrpcCall(url, ID, JSONRPC, method, &out, params, timeout)
	}
	return out, err
}

func KaspaGetTxByAddress(chainName string, address string, urls []string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("KaspaGetTxByAddress error, chainName:"+chainName+", address:"+address, e)
			} else {
				log.Errore("KaspaGetTxByAddress panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	req := &data.TransactionRequest{
		Nonce:       -1,
		FromAddress: address,
		OrderBy:     "tx_time desc",
		PageNum:     1,
		PageSize:    1,
	}
	dbLastRecords, _, err := data.KasTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("通过用户资产变更爬取交易记录，查询数据库交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbTxTime int64
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbTxTime = dbLastRecords[0].TxTime
		dbLastRecordHash = dbLastRecords[0].TransactionHash
	}

	offset := 0
	url := urls[0]
	url = url + "/addresses/" + address + "/full-transactions?limit=" + strconv.Itoa(pageSize) + "&offset="

	var chainRecords []*types.KaspaTransactionInfo
chainFlag:
	for {
		var out []*types.KaspaTransactionInfo
		reqUrl := url + strconv.Itoa(offset)

		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		}
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败，address:%s", chainName, address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
		}

		dataLen := len(out)
		if dataLen == 0 {
			break
		}
		for _, browserInfo := range out {
			if !browserInfo.IsAccepted {
				continue
			}
			txHash := browserInfo.TransactionId
			txTime := browserInfo.BlockTime / 1000
			if txTime < dbTxTime || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		if dataLen < pageSize {
			break
		}
		offset += pageSize
	}

	var kasTransactionRecordList []*data.KasTransactionRecord
	transactionRecordMap := make(map[string]string)
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TransactionId
		if _, ok := transactionRecordMap[txHash]; !ok {
			transactionRecordMap[txHash] = ""
		} else {
			continue
		}
		blockHash := record.BlockHash
		btcRecord := &data.KasTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			BlockHash:       blockHash[0],
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		kasTransactionRecordList = append(kasTransactionRecordList, btcRecord)
	}

	if len(kasTransactionRecordList) > 0 {
		_, err = data.KasTransactionRecordRepoClient.BatchSave(nil, GetTableName(chainName), kasTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("chainName", chainName), zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}
