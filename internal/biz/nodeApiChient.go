package biz

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
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
	case "Cosmos":
		err = CosmosGetTxByAddress(chainName, address, urls)
	case "Osmosis":
		err = CosmosGetTxByAddress(chainName, address, urls)
	case "Solana":
		err = SolanaGetTxByAddress(chainName, address, urls)
	case "Aptos":
		err = AptosGetTxByAddress(chainName, address, urls)
	case "STC":
		err = StarcoinGetTxByAddress(chainName, address, urls)
	}

	return
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

	var starIndex = 0
	url := urls[0]
	url = url + address + "/txs?"

	req := &pb.PageListRequest{
		Address:  address,
		OrderBy:  "block_number desc",
		PageNum:  1,
		PageSize: 1,
	}
	dbLastRecords, _, err := data.AtomTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"通过用户资产变更爬取交易记录，链查询数据库交易记录失败", zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var chainRecords []CosmosBrowserInfo
chainFlag:
	for {
		var out []CosmosBrowserInfo
		reqUrl := url + "limit=" + strconv.Itoa(pageSize) + "&from=" + strconv.Itoa(starIndex)

		err = httpclient.GetUseCloudscraper(reqUrl, &out, &timeout)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
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
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常", chainName)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"链通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("blockNumber", browserInfo.Data.Height), zap.Any("txHash", txHash), zap.Any("error", err))
				break chainFlag
			}
			if txHeight < dbLastRecordBlockNumber || txHash == dbLastRecordHash {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		starIndex = out[dataLen-1].Header.Id
	}

	var atomTransactionRecordList []*data.AtomTransactionRecord
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.Data.Txhash
		atomRecord := &data.AtomTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			DappData:        "",
			ClientData:      "",
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		atomTransactionRecordList = append(atomTransactionRecordList, atomRecord)
	}

	if len(atomTransactionRecordList) > 0 {
		_, err = data.AtomTransactionRecordRepoClient.BatchSave(nil, GetTableName(chainName), atomTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

type SolanaBrowserResponse struct {
	Succcess bool                `json:"succcess"`
	Data     []SolanaBrowserInfo `json:"data"`
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

	var beforeTxHash string
	url := urls[0]
	url = url + "/account/transaction?address=" + address

	req := &pb.PageListRequest{
		Address:  address,
		OrderBy:  "block_number desc",
		PageNum:  1,
		PageSize: 1,
	}
	dbLastRecords, _, err := data.SolTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"通过用户资产变更爬取交易记录，链查询数据库交易记录失败", zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordSlotNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordSlotNumber = dbLastRecords[0].SlotNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var chainRecords []SolanaBrowserInfo
chainFlag:
	for {
		var out SolanaBrowserResponse
		reqUrl := url + "&before=" + beforeTxHash

		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
			break
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
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TxHash
		solRecord := &data.SolTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			DappData:        "",
			ClientData:      "",
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		solTransactionRecordList = append(solTransactionRecordList, solRecord)
	}

	if len(solTransactionRecordList) > 0 {
		_, err = data.SolTransactionRecordRepoClient.BatchSave(nil, GetTableName(chainName), solTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
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
		MoveResources []AptosBrowserInfo `json:"move_resources"`
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

	var starIndex = 0
	url := urls[0]

	req := &pb.PageListRequest{
		Address:  address,
		OrderBy:  "block_number desc",
		PageNum:  1,
		PageSize: 1,
	}
	dbLastRecords, _, err := data.AptTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"通过用户资产变更爬取交易记录，链查询数据库交易记录失败", zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordVersion int
	if len(dbLastRecords) > 0 {
		dbLastRecordVersion = dbLastRecords[0].TransactionVersion
	}

	var chainRecords []AptosBrowserInfo
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
		timeoutMS := 5_000 * time.Millisecond
		err = httpclient.HttpPostJson(url, tokenRequest, &out, &timeoutMS)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("address", address), zap.Any("requestUrl", url), zap.Any("error", err))
			break
		}

		if len(out.Data.MoveResources) == 0 {
			break
		}
		for _, browserInfo := range out.Data.MoveResources {
			txVersion := browserInfo.TransactionVersion
			if txVersion <= dbLastRecordVersion {
				break chainFlag
			}
			chainRecords = append(chainRecords, browserInfo)
		}
		starIndex += pageSize
	}

	var aptTransactionRecordList []*data.AptTransactionRecord
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txVersion := record.TransactionVersion
		aptRecord := &data.AptTransactionRecord{
			TransactionVersion: txVersion,
			Status:             PENDING,
			DappData:           "",
			ClientData:         "",
			CreatedAt:          now,
			UpdatedAt:          now,
		}
		aptTransactionRecordList = append(aptTransactionRecordList, aptRecord)
	}

	if len(aptTransactionRecordList) > 0 {
		_, err = data.AptTransactionRecordRepoClient.BatchSave(nil, GetTableName(chainName), aptTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}

type StarcoinBrowserResponse struct {
	Contents []StarcoinBrowserInfo `json:"contents"`
	Total    int                   `json:"total"`
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

	var pageNum = 1
	url := urls[0]
	url = url + "/transaction/address/main/" + address + "/page/"

	req := &pb.PageListRequest{
		Address:  address,
		OrderBy:  "block_number desc",
		PageNum:  1,
		PageSize: 1,
	}
	dbLastRecords, _, err := data.StcTransactionRecordRepoClient.PageList(nil, GetTableName(chainName), req)
	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询数据库交易记录失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(chainName+"通过用户资产变更爬取交易记录，链查询数据库交易记录失败", zap.Any("address", address), zap.Any("error", err))
		return err
	}
	var dbLastRecordBlockNumber int
	var dbLastRecordHash string
	if len(dbLastRecords) > 0 {
		dbLastRecordBlockNumber = dbLastRecords[0].BlockNumber
		dbLastRecordHash = strings.Split(dbLastRecords[0].TransactionHash, "#")[0]
	}

	var chainRecords []StarcoinBrowserInfo
chainFlag:
	for {
		var out StarcoinBrowserResponse
		reqUrl := url + strconv.Itoa(pageNum)

		err = httpclient.GetResponse(reqUrl, nil, &out, &timeout)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，查询链上交易记录失败", zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("error", err))
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
				alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，查询链上交易记录异常", chainName)
				alarmOpts := WithMsgLevel("FATAL")
				LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(chainName+"链通过用户资产变更爬取交易记录，查询链上交易记录异常", zap.Any("address", address), zap.Any("requestUrl", reqUrl), zap.Any("blockNumber", browserInfo.BlockNumber), zap.Any("txHash", txHash), zap.Any("error", err))
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
	now := time.Now().Unix()
	for _, record := range chainRecords {
		txHash := record.TransactionHash
		atomRecord := &data.StcTransactionRecord{
			TransactionHash: txHash,
			Status:          PENDING,
			DappData:        "",
			ClientData:      "",
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		stcTransactionRecordList = append(stcTransactionRecordList, atomRecord)
	}

	if len(stcTransactionRecordList) > 0 {
		_, err = data.StcTransactionRecordRepoClient.BatchSave(nil, GetTableName(chainName), stcTransactionRecordList)
		if err != nil {
			alarmMsg := fmt.Sprintf("请注意：%s链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", chainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(chainName+"链通过用户资产变更爬取交易记录，插入链上交易记录数据到数据库中失败", zap.Any("address", address), zap.Any("error", err))
			return err
		}
	}

	return
}
