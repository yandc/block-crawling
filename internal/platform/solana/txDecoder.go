package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/json"
	"fmt"
	"gorm.io/datatypes"
	"math/big"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txDecoder struct {
	ChainName   string
	block       *chain.Block
	txByHash    *chain.Transaction
	chainHeight uint64
	curHeight   uint64
	now         time.Time
	newTxs      bool

	txRecords []*data.SolTransactionRecord
}

type TokenBalance struct {
	AccountIndex  int         `json:"accountIndex"`
	Account       string      `json:"account"`
	Mint          string      `json:"mint"`
	Owner         string      `json:"owner"`
	ProgramId     string      `json:"programId"`
	UiTokenAmount TokenAmount `json:"uiTokenAmount"`
}

type TokenAmount struct {
	Amount         string   `json:"amount"`
	Decimals       int      `json:"decimals"`
	UiAmount       float64  `json:"uiAmount"`
	UiAmountString string   `json:"uiAmountString"`
	TransferAmount *big.Int `json:"transferAmount"`
}

type AccountKey struct {
	Index          int      `json:"index"`
	Pubkey         string   `json:"pubkey"`
	Signer         bool     `json:"signer"`
	Source         string   `json:"source"`
	Writable       bool     `json:"writable"`
	Amount         *big.Int `json:"amount"`
	TransferAmount *big.Int `json:"transferAmount"`
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) (err error) {
	transactionHash := tx.Hash
	curSlot := block.Number
	curHeight, _ := block.Raw.(int)
	transactionInfo := tx.Raw.(*TransactionInfo)

	txTime := block.Time

	meta := transactionInfo.Meta
	feeAmount := decimal.NewFromInt(meta.Fee)
	var payload string
	var status string
	if meta.Err == nil {
		status = biz.SUCCESS
	} else {
		status = biz.FAIL
	}
	preBalances := meta.PreBalances
	preBalancesLen := len(preBalances)
	postBalances := meta.PostBalances
	postBalancesLen := len(postBalances)
	preTokenBalances := meta.PreTokenBalances
	preTokenBalancesLen := len(preTokenBalances)
	postTokenBalances := meta.PostTokenBalances

	transaction := transactionInfo.Transaction
	accountKeys := transaction.Message.AccountKeys
	accountKeyMap := make(map[string]AccountKey)
	var signerAddressList []string
	for i, accountKey := range accountKeys {
		postBalancesAmount := new(big.Int)
		preBalancesAmount := new(big.Int)
		if i < postBalancesLen {
			postBalancesAmount = postBalances[i]
		}
		if i < preBalancesLen {
			preBalancesAmount = preBalances[i]
		}
		ak := AccountKey{
			Index:          i,
			Pubkey:         accountKey.Pubkey,
			Signer:         accountKey.Signer,
			Source:         accountKey.Source,
			Writable:       accountKey.Writable,
			Amount:         postBalancesAmount,
			TransferAmount: new(big.Int).Sub(postBalancesAmount, preBalancesAmount),
		}
		accountKeyMap[ak.Pubkey] = ak
		if accountKey.Signer {
			signerAddressList = append(signerAddressList, accountKey.Pubkey)
		}
	}

	tokenBalanceMap := make(map[string]TokenBalance)
	tokenBalanceOwnerMap := make(map[string]TokenBalance)
	for i, tokenBalance := range postTokenBalances {
		postAmount, _ := new(big.Int).SetString(tokenBalance.UiTokenAmount.Amount, 0)

		preAmount := new(big.Int)
		if i < preTokenBalancesLen {
			preAmount, _ = new(big.Int).SetString(preTokenBalances[i].UiTokenAmount.Amount, 0)
		}
		tb := TokenBalance{
			AccountIndex: tokenBalance.AccountIndex,
			Account:      accountKeys[tokenBalance.AccountIndex].Pubkey,
			Mint:         tokenBalance.Mint,
			Owner:        tokenBalance.Owner,
			ProgramId:    tokenBalance.ProgramId,
			UiTokenAmount: TokenAmount{
				Amount:         tokenBalance.UiTokenAmount.Amount,
				Decimals:       tokenBalance.UiTokenAmount.Decimals,
				UiAmount:       tokenBalance.UiTokenAmount.UiAmount,
				UiAmountString: tokenBalance.UiTokenAmount.UiAmountString,
				TransferAmount: new(big.Int).Sub(postAmount, preAmount),
			},
		}
		tokenBalanceMap[tb.Account] = tb
		if tb.Owner != "" {
			tokenBalanceOwnerMap[tb.Owner] = tb
		}
	}

	var instructionList []Instruction
	var innerInstructionList []Instruction
	var innerTotal int
	instructions := transaction.Message.Instructions
	innerInstructions := transactionInfo.Meta.InnerInstructions
	for _, instruction := range innerInstructions {
		inInstructions := instruction.Instructions
		innerInstructionList = append(innerInstructionList, inInstructions...)
	}

	// 合并交易
	//https://solscan.io/tx/51hTc8xEUAB53kCDh4uPbvbc7wCdherg5kpMNFKZ8vyroEPBiDEPTqrXJt4gUwSoZVLe7oLM9w736U6kmpDwKrSB
	instructionList, tokenBalanceOwnerMap, _ = mergeInstructions(instructions, tokenBalanceMap, tokenBalanceOwnerMap)
	innerInstructionList, tokenBalanceOwnerMap, innerTotal = mergeInstructions(innerInstructionList, tokenBalanceMap, tokenBalanceOwnerMap)

	payload, _ = utils.JsonEncode(map[string]interface{}{"accountKey": accountKeyMap, "tokenBalance": tokenBalanceOwnerMap})
	isContract := false
	if innerTotal > 0 || len(instructionList) > 1 {
		isContract = true
	}

	createIndex := 0
	index := 0
	if !isContract {
		for _, instruction := range instructionList {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			parsed, ok := instruction.Parsed.(map[string]interface{})
			if !ok {
				//Unknown类型
				//https://solscan.io/tx/SAfgWytZmTEpzQbkCLy2GgzdnVrSbeyGiFD1SHiRM5Gf7XzRAQEeU5gkUn5ic8ZG8UsF1w1MB72om9SoShHF9si
				continue
			}
			programId := instruction.ProgramId
			instructionType := parsed["type"]
			instructionInfo := parsed["info"].(map[string]interface{})
			if instructionType == "createAccount" {
				txType = biz.CREATEACCOUNT
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["newAccount"].(string)
				amount = utils.GetString(instructionInfo["lamports"])
			} else if instructionType == "closeAccount" {
				txType = biz.CLOSEACCOUNT
				fromAddress = instructionInfo["account"].(string)
				toAddress = instructionInfo["destination"].(string)
				transferAmount := accountKeyMap[fromAddress].TransferAmount
				bigAmount := new(big.Int).Abs(transferAmount)
				amount = bigAmount.String()
			} else if instructionType == "create" || instructionType == "createIdempotent" {
				txType = biz.REGISTERTOKEN
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["account"].(string)
				contractAddress = instructionInfo["mint"].(string)
				for ; createIndex < len(innerInstructionList); createIndex++ {
					innerInstruction := innerInstructionList[createIndex]
					innerParsed, innerOk := innerInstruction.Parsed.(map[string]interface{})
					if !innerOk {
						createIndex++
						break
					}
					innerInstructionType := innerParsed["type"]
					innerInstructionInfo := innerParsed["info"].(map[string]interface{})
					if innerInstructionType == "createAccount" {
						amount = utils.GetString(innerInstructionInfo["lamports"])
						createIndex++
						break
					}
				}
			} else if instructionType == "transfer" {
				if programId == SOL_CODE {
					txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					txType = biz.TRANSFER
					fromAddress, ok = instructionInfo["authority"].(string)
					if !ok {
						fromAddress = instructionInfo["multisigAuthority"].(string)
					}
					destination := instructionInfo["destination"].(string)
					toAddress = tokenBalanceMap[destination].Owner
					if toAddress == "" {
						toAddress = destination
					}
					amount = instructionInfo["amount"].(string)
					contractAddress = tokenBalanceMap[destination].Mint
				}
			} else if instructionType == "transferChecked" {
				if programId == SOL_CODE {
					txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					txType = biz.TRANSFER
					fromAddress, ok = instructionInfo["authority"].(string)
					if !ok {
						fromAddress = instructionInfo["multisigAuthority"].(string)
					}
					destination := instructionInfo["destination"].(string)
					toAddress = tokenBalanceMap[destination].Owner
					if toAddress == "" {
						toAddress = destination
					}
					tokenAmount := instructionInfo["tokenAmount"].(map[string]interface{})
					amount = tokenAmount["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)
				}
			} else if instructionType == "mintTo" {
				if programId == SOL_CODE {
					txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					txType = biz.TRANSFER
					toAddress, ok = instructionInfo["mintAuthority"].(string)
					if !ok {
						toAddress = instructionInfo["multisigMintAuthority"].(string)
					}
					amount = instructionInfo["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)
				}
			} else {
				continue
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, fromAddress)
				if err != nil {
					log.Error(h.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, toAddress)
				if err != nil {
					log.Error(h.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}
			if !fromAddressExist && !toAddressExist {
				continue
			}

			index++
			var txHash string
			if index == 1 {
				txHash = transactionHash
			} else {
				txHash = transactionHash + "#result-" + fmt.Sprintf("%v", index)
			}

			if contractAddress != "" {
				tokenInfo, err = biz.GetTokenNftInfoRetryAlert(nil, h.ChainName, contractAddress, programId)
				if err != nil {
					log.Error(h.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", txHash), zap.Any("error", err))
				}
				if txType != biz.REGISTERTOKEN {
					tokenInfo.Amount = amount
				}
				tokenInfo.Address = contractAddress
			}
			if tokenInfo.TokenType != "" && tokenInfo.TokenType != biz.ERC20 && txType == biz.TRANSFER {
				txType = biz.TRANSFERNFT
			}
			solMap := map[string]interface{}{
				"token": tokenInfo,
			}
			parseData, _ := utils.JsonEncode(solMap)
			amountValue, _ := decimal.NewFromString(amount)

			solTransactionRecord := &data.SolTransactionRecord{
				SlotNumber:      int(block.Number),
				BlockHash:       block.Hash,
				BlockNumber:     curHeight,
				TransactionHash: txHash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,
				FeeAmount:       feeAmount,
				Amount:          amountValue,
				Status:          status,
				TxTime:          txTime,
				ContractAddress: contractAddress,
				ParseData:       parseData,
				Data:            payload,
				EventLog:        "",
				TransactionType: txType,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now.Unix(),
				UpdatedAt:       h.now.Unix(),
			}
			h.txRecords = append(h.txRecords, solTransactionRecord)
		}

		if len(h.txRecords) == 0 {
			solTransactionRecord := &data.SolTransactionRecord{
				SlotNumber:      int(block.Number),
				BlockHash:       block.Hash,
				BlockNumber:     curHeight,
				TransactionHash: transactionHash,
				/*FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,*/
				FeeAmount: feeAmount,
				//Amount:          amountValue,
				Status: status,
				TxTime: txTime,
				/*ContractAddress: contractAddress,
				ParseData:       parseData,*/
				Data:            payload,
				EventLog:        "",
				TransactionType: biz.CONTRACT,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now.Unix(),
				UpdatedAt:       h.now.Unix(),
			}
			h.txRecords = append(h.txRecords, solTransactionRecord)
		}
	} else {
		instructionList = append(instructionList, innerInstructionList...)
		var eventLogs []*types.EventLog
		var solTransactionRecords []*data.SolTransactionRecord

		txType := biz.CONTRACT
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		for _, fromAddress = range signerAddressList {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, fromAddress)
			if err != nil {
				log.Error(h.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
			if fromAddressExist {
				break
			}
		}

		for _, instruction := range instructionList {
			toAddress = instruction.ProgramId
			if toAddress != SOL_CODE {
				break
			}
		}
		if fromAddress == toAddress {
			return nil
		}

		solMap := map[string]interface{}{
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(solMap)
		amountValue, _ := decimal.NewFromString(amount)

		solContractRecord := &data.SolTransactionRecord{
			SlotNumber:      int(block.Number),
			BlockHash:       block.Hash,
			BlockNumber:     curHeight,
			TransactionHash: transactionHash,
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			FromUid:         fromUid,
			ToUid:           toUid,
			FeeAmount:       feeAmount,
			Amount:          amountValue,
			Status:          status,
			TxTime:          txTime,
			ContractAddress: contractAddress,
			ParseData:       parseData,
			Data:            payload,
			EventLog:        "",
			TransactionType: txType,
			DappData:        "",
			ClientData:      "",
			CreatedAt:       h.now.Unix(),
			UpdatedAt:       h.now.Unix(),
		}

		txType = biz.EVENTLOG
		for _, instruction := range instructionList {
			//txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			parsed, ok := instruction.Parsed.(map[string]interface{})
			if !ok {
				continue
			}
			programId := instruction.ProgramId
			instructionType := parsed["type"]
			instructionInfo := parsed["info"].(map[string]interface{})
			if instructionType == "createAccount" {
				//txType = biz.CREATEACCOUNT
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["newAccount"].(string)
				amount = utils.GetString(instructionInfo["lamports"])
			} else if instructionType == "closeAccount" {
				//txType = biz.CLOSEACCOUNT
				fromAddress = instructionInfo["account"].(string)
				toAddress = instructionInfo["destination"].(string)
				transferAmount := accountKeyMap[fromAddress].TransferAmount
				bigAmount := new(big.Int).Abs(transferAmount)
				amount = bigAmount.String()
				/*} else if instructionType == "create" || instructionType == "createIdempotent" {
				//txType = biz.REGISTERTOKEN
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["account"].(string)
				//不能保存被激活的代币信息，否则就会变成发送改代币了
				//contractAddress = instructionInfo["mint"].(string)
				for ; createIndex < len(innerInstructionList); createIndex++ {
					innerInstruction := innerInstructionList[createIndex]
					innerParsed, innerOk := innerInstruction.Parsed.(map[string]interface{})
					if !innerOk {
						createIndex++
						break
					}
					innerInstructionType := innerParsed["type"]
					innerInstructionInfo := innerParsed["info"].(map[string]interface{})
					if innerInstructionType == "createAccount" {
						amount = utils.GetString(innerInstructionInfo["lamports"])
						createIndex++
						break
					}
				}*/
			} else if instructionType == "transfer" {
				if programId == SOL_CODE {
					//txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					//txType = biz.TRANSFER
					fromAddress, ok = instructionInfo["authority"].(string)
					if !ok {
						fromAddress = instructionInfo["multisigAuthority"].(string)
					}
					destination := instructionInfo["destination"].(string)
					toAddress = tokenBalanceMap[destination].Owner
					if toAddress == "" {
						toAddress = destination
					}
					amount = instructionInfo["amount"].(string)
					contractAddress = tokenBalanceMap[destination].Mint
				}
			} else if instructionType == "transferChecked" {
				if programId == SOL_CODE {
					//txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					//txType = biz.TRANSFER
					fromAddress, ok = instructionInfo["authority"].(string)
					if !ok {
						fromAddress = instructionInfo["multisigAuthority"].(string)
					}
					destination := instructionInfo["destination"].(string)
					toAddress = tokenBalanceMap[destination].Owner
					if toAddress == "" {
						toAddress = destination
					}
					tokenAmount := instructionInfo["tokenAmount"].(map[string]interface{})
					amount = tokenAmount["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)
				}
			} else if instructionType == "mintTo" {
				if programId == SOL_CODE {
					//txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					//txType = biz.TRANSFER
					toAddress, ok = instructionInfo["mintAuthority"].(string)
					if !ok {
						toAddress = instructionInfo["multisigMintAuthority"].(string)
					}
					amount = instructionInfo["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)
				}
			} else {
				continue
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, fromAddress)
				if err != nil {
					log.Error(h.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, toAddress)
				if err != nil {
					log.Error(h.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}
			if !fromAddressExist && !toAddressExist {
				continue
			}

			index++
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index)

			if contractAddress != "" {
				tokenInfo, err = biz.GetTokenNftInfoRetryAlert(nil, h.ChainName, contractAddress, programId)
				if err != nil {
					log.Error(h.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", txHash), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
			}
			solMap := map[string]interface{}{
				"token": tokenInfo,
			}
			parseData, _ := utils.JsonEncode(solMap)
			amountValue, _ := decimal.NewFromString(amount)
			eventLogInfo := &types.EventLog{
				From:   fromAddress,
				To:     toAddress,
				Amount: amountValue.BigInt(),
				Token:  tokenInfo,
			}

			var isContinue bool
			for i, eventLog := range eventLogs {
				if eventLog == nil {
					continue
				}
				if eventLog.From == eventLogInfo.To && eventLog.To == eventLogInfo.From && eventLog.Token.Address == eventLogInfo.Token.Address &&
					eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
					cmp := eventLog.Amount.Cmp(eventLogInfo.Amount)
					if cmp == 1 {
						isContinue = true
						subAmount := new(big.Int).Sub(eventLog.Amount, eventLogInfo.Amount)
						eventLogs[i].Amount = subAmount
						solTransactionRecords[i].Amount = decimal.NewFromBigInt(subAmount, 0)
					} else if cmp == 0 {
						isContinue = true
						eventLogs[i] = nil
						solTransactionRecords[i] = nil
					} else if cmp == -1 {
						eventLogs[i] = nil
						solTransactionRecords[i] = nil
					}
					break
				} else if eventLog.From == eventLogInfo.From && eventLog.To == eventLogInfo.To && eventLog.Token.Address == eventLogInfo.Token.Address &&
					eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
					isContinue = true
					addAmount := new(big.Int).Add(eventLog.Amount, eventLogInfo.Amount)
					eventLogs[i].Amount = addAmount
					solTransactionRecords[i].Amount = decimal.NewFromBigInt(addAmount, 0)
					break
				}
			}
			if isContinue {
				continue
			}
			eventLogs = append(eventLogs, eventLogInfo)

			solTransactionRecord := &data.SolTransactionRecord{
				SlotNumber:      int(block.Number),
				BlockHash:       block.Hash,
				BlockNumber:     curHeight,
				TransactionHash: txHash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,
				FeeAmount:       feeAmount,
				Amount:          amountValue,
				Status:          status,
				TxTime:          txTime,
				ContractAddress: contractAddress,
				ParseData:       parseData,
				Data:            payload,
				EventLog:        "",
				TransactionType: txType,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now.Unix(),
				UpdatedAt:       h.now.Unix(),
			}
			solTransactionRecords = append(solTransactionRecords, solTransactionRecord)
		}

		if fromAddressExist || toAddressExist || len(eventLogs) > 0 {
			h.txRecords = append(h.txRecords, solContractRecord)
		}
		if len(eventLogs) > 0 {
			for _, solTransactionRecord := range solTransactionRecords {
				if solTransactionRecord != nil {
					h.txRecords = append(h.txRecords, solTransactionRecord)
				}
			}

			var eventLogList []*types.EventLog
			for _, eventLog := range eventLogs {
				if eventLog != nil {
					eventLogList = append(eventLogList, eventLog)
				}
			}
			if len(eventLogList) > 0 {
				eventLog, _ := utils.JsonEncode(eventLogList)
				solContractRecord.EventLog = eventLog

				var logAddress datatypes.JSON
				var logFromAddress []string
				var logToAddress []string
				for _, log := range eventLogList {
					logFromAddress = append(logFromAddress, log.From)
					logToAddress = append(logToAddress, log.To)
				}
				logAddressList := [][]string{logFromAddress, logToAddress}
				logAddress, _ = json.Marshal(logAddressList)
				solContractRecord.LogAddress = logAddress
			}
		}
	}
	return nil
}

func (h *txDecoder) Save(client chain.Clienter) error {
	txRecords := h.txRecords
	if txRecords != nil && len(txRecords) > 0 {
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", h.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.ChainName+"扫块，将数据插入到数据库中失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.ChainName, *(client.(*Client)), txRecords)
		} else {
			go HandlePendingRecord(h.ChainName, *(client.(*Client)), txRecords)
		}

		if h.newTxs {
			records := make([]interface{}, 0, len(txRecords))
			for _, r := range txRecords {
				records = append(records, r)
			}
			common.SetResultOfTxs(h.block, records)
		} else {
			common.SetTxResult(h.txByHash, txRecords[0])
		}
	}
	return nil
}

func (h *txDecoder) OnSealedTx(c chain.Clienter, tx *chain.Transaction) error {
	var err error
	client := c.(*Client)
	var block *chain.Block

	block, err = client.GetBlock(tx.BlockNumber)
	if err != nil {
		return err
	}

	err = h.OnNewTx(c, block, tx)

	return err
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	// client := c.(*Client)
	record := tx.Record.(*data.SolTransactionRecord)

	log.Info(
		"PENDING TX COULD NOT FOUND ON THE CHAIN",
		zap.String("chainName", h.ChainName),
		zap.Uint64("height", tx.BlockNumber),
		zap.String("nodeUrl", c.URL()),
		zap.String("txHash", tx.Hash),
		zap.String("fromUid", record.FromUid),
		zap.String("toUid", record.ToUid),
		zap.String("fromAddress", record.FromAddress),
		zap.String("toAddress", record.ToAddress),
	)

	nowTime := time.Now().Unix()
	if record.CreatedAt+300 > nowTime {
		record.Status = biz.NO_STATUS
		h.txRecords = append(h.txRecords, record)
	} else {
		record.Status = biz.FAIL
		h.txRecords = append(h.txRecords, record)
	}

	return nil
}

func mergeInstructions(instructions []Instruction, tokenBalanceMap map[string]TokenBalance, tokenBalanceOwnerMap map[string]TokenBalance) ([]Instruction, map[string]TokenBalance, int) {
	var instructionList []Instruction
	var instructionMap = make(map[string]Instruction)
	var createInstructionMap = make(map[string]Instruction)
	var closeInstructionMap = make(map[string]Instruction)
	var innerTotal int
	for _, instruction := range instructions {
		var amount, newAmount, contractAddress string
		var fromAddress, toAddress string

		parsed, ok := instruction.Parsed.(map[string]interface{})
		if !ok {
			instructionList = append(instructionList, instruction)
			continue
		}
		programId := instruction.ProgramId
		instructionType := parsed["type"]
		instructionInfo := parsed["info"].(map[string]interface{})

		if instructionType == "transfer" {
			innerTotal++
			if programId == SOL_CODE {
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = instruction
					instructionMap[key] = newInstruction
				} else {
					amount = utils.GetString(instructionInfo["lamports"])

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					newAmount = utils.GetString(newInstructionInfo["lamports"])
					amountInt, _ := new(big.Int).SetString(amount, 0)
					newAmountInt, _ := new(big.Int).SetString(newAmount, 0)
					sumAmount := new(big.Int).Add(amountInt, newAmountInt).String()
					newInstructionInfo["lamports"] = sumAmount
				}
			} else {
				fromAddress, ok = instructionInfo["authority"].(string)
				if !ok {
					fromAddress = instructionInfo["multisigAuthority"].(string)
				}
				source := instructionInfo["source"].(string)
				destination := instructionInfo["destination"].(string)
				sourceTokenBalance := tokenBalanceMap[source]
				destinationTokenBalance := tokenBalanceMap[destination]
				toAddress = destinationTokenBalance.Owner
				if toAddress == "" {
					toAddress = destination
				}
				if sourceTokenBalance.Owner == "" {
					sourceTokenBalance.Owner = fromAddress
					tokenBalanceOwnerMap[sourceTokenBalance.Owner] = sourceTokenBalance
				}
				if sourceTokenBalance.Mint != "" {
					contractAddress = sourceTokenBalance.Mint
				} else if destinationTokenBalance.Mint != "" {
					contractAddress = destinationTokenBalance.Mint
				}

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = instruction
					instructionMap[key] = newInstruction
				} else {
					amount = instructionInfo["amount"].(string)

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					newAmount = newInstructionInfo["amount"].(string)
					amountInt, _ := new(big.Int).SetString(amount, 0)
					newAmountInt, _ := new(big.Int).SetString(newAmount, 0)
					sumAmount := new(big.Int).Add(amountInt, newAmountInt).String()
					newInstructionInfo["amount"] = sumAmount
				}
			}
		} else if instructionType == "transferChecked" {
			innerTotal++
			if programId == SOL_CODE {
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = instruction
					instructionMap[key] = newInstruction
				} else {
					amount = utils.GetString(instructionInfo["lamports"])

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					newAmount = utils.GetString(newInstructionInfo["lamports"])
					amountInt, _ := new(big.Int).SetString(amount, 0)
					newAmountInt, _ := new(big.Int).SetString(newAmount, 0)
					sumAmount := new(big.Int).Add(amountInt, newAmountInt).String()
					newInstructionInfo["lamports"] = sumAmount
				}
			} else {
				fromAddress, ok = instructionInfo["authority"].(string)
				if !ok {
					fromAddress = instructionInfo["multisigAuthority"].(string)
				}
				source := instructionInfo["source"].(string)
				destination := instructionInfo["destination"].(string)
				sourceTokenBalance := tokenBalanceMap[source]
				destinationTokenBalance := tokenBalanceMap[destination]
				toAddress = destinationTokenBalance.Owner
				if toAddress == "" {
					toAddress = destination
				}
				if sourceTokenBalance.Owner == "" {
					sourceTokenBalance.Owner = fromAddress
					tokenBalanceOwnerMap[sourceTokenBalance.Owner] = sourceTokenBalance
				}
				contractAddress = instructionInfo["mint"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = instruction
					instructionMap[key] = newInstruction
				} else {
					tokenAmount := instructionInfo["tokenAmount"].(map[string]interface{})
					amount = tokenAmount["amount"].(string)

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					newTokenAmount := newInstructionInfo["tokenAmount"].(map[string]interface{})
					newAmount = newTokenAmount["amount"].(string)
					amountInt, _ := new(big.Int).SetString(amount, 0)
					newAmountInt, _ := new(big.Int).SetString(newAmount, 0)
					sumAmount := new(big.Int).Add(amountInt, newAmountInt).String()
					newTokenAmount["amount"] = sumAmount
				}
			}
		} else if instructionType == "mintTo" {
			innerTotal++
			if programId == SOL_CODE {
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = instruction
					instructionMap[key] = newInstruction
				} else {
					amount = utils.GetString(instructionInfo["lamports"])

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					newAmount = utils.GetString(newInstructionInfo["lamports"])
					amountInt, _ := new(big.Int).SetString(amount, 0)
					newAmountInt, _ := new(big.Int).SetString(newAmount, 0)
					sumAmount := new(big.Int).Add(amountInt, newAmountInt).String()
					newInstructionInfo["lamports"] = sumAmount
				}
			} else {
				toAddress, ok = instructionInfo["mintAuthority"].(string)
				if !ok {
					toAddress = instructionInfo["multisigMintAuthority"].(string)
				}
				contractAddress = instructionInfo["mint"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = instruction
					instructionMap[key] = newInstruction
				} else {
					amount = instructionInfo["amount"].(string)

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					newAmount = newInstructionInfo["amount"].(string)
					amountInt, _ := new(big.Int).SetString(amount, 0)
					newAmountInt, _ := new(big.Int).SetString(newAmount, 0)
					sumAmount := new(big.Int).Add(amountInt, newAmountInt).String()
					newInstructionInfo["amount"] = sumAmount
				}
			}
		} else if instructionType == "createAccount" {
			fromAddress = instructionInfo["source"].(string)
			toAddress = instructionInfo["newAccount"].(string)
			key := fromAddress + toAddress
			if _, closeOk := closeInstructionMap[key]; closeOk {
				delete(closeInstructionMap, key)
			} else {
				createInstructionMap[key] = instruction
			}
		} else if instructionType == "closeAccount" {
			fromAddress = instructionInfo["account"].(string)
			toAddress = instructionInfo["destination"].(string)
			key := toAddress + fromAddress
			if _, createOk := createInstructionMap[key]; createOk {
				delete(createInstructionMap, key)
			} else {
				closeInstructionMap[key] = instruction
			}
		} else {
			instructionList = append(instructionList, instruction)
		}
	}
	for _, instruction := range instructionMap {
		instructionList = append(instructionList, instruction)
	}
	for _, instruction := range createInstructionMap {
		instructionList = append(instructionList, instruction)
	}
	for _, instruction := range closeInstructionMap {
		instructionList = append(instructionList, instruction)
	}

	return instructionList, tokenBalanceOwnerMap, innerTotal
}
