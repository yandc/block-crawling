package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"fmt"
	"math/big"
	"strconv"
	"strings"
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

type TransferTokenBalance struct {
	*TokenBalance
	Pubkey         string   `json:"pubkey"`
	TransferAmount *big.Int `json:"transferAmount"`
}

type TransferAccountKey struct {
	*AccountKey
	Index          int      `json:"index"`
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
	postTokenBalances := meta.PostTokenBalances

	transaction := transactionInfo.Transaction
	accountKeys := transaction.Message.AccountKeys
	accountKeyMap := make(map[string]*TransferAccountKey)
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
		ak := &TransferAccountKey{
			AccountKey:     accountKey,
			Index:          i,
			Amount:         postBalancesAmount,
			TransferAmount: new(big.Int).Sub(postBalancesAmount, preBalancesAmount),
		}
		accountKeyMap[ak.Pubkey] = ak
		if accountKey.Signer {
			signerAddressList = append(signerAddressList, accountKey.Pubkey)
		}
	}

	preTokenBalanceMap := make(map[string]*TokenBalance)
	for _, tokenBalance := range preTokenBalances {
		key := strconv.Itoa(tokenBalance.AccountIndex) + tokenBalance.Mint
		preTokenBalanceMap[key] = tokenBalance
	}
	tokenAccountTokenBalanceMap := make(map[string]*TransferTokenBalance)
	ownerAccountTokenBalanceMap := make(map[string]map[string]*TransferTokenBalance)
	for _, postTokenBalance := range postTokenBalances {
		postAmount, _ := new(big.Int).SetString(postTokenBalance.UiTokenAmount.Amount, 0)
		preAmount := new(big.Int)
		key := strconv.Itoa(postTokenBalance.AccountIndex) + postTokenBalance.Mint
		if preTokenBalance, ok := preTokenBalanceMap[key]; ok {
			preAmount, _ = new(big.Int).SetString(preTokenBalance.UiTokenAmount.Amount, 0)
		}
		tokenBalance := &TransferTokenBalance{
			TokenBalance:   postTokenBalance,
			Pubkey:         accountKeys[postTokenBalance.AccountIndex].Pubkey,
			TransferAmount: new(big.Int).Sub(postAmount, preAmount),
		}
		tokenAccountTokenBalanceMap[tokenBalance.Pubkey] = tokenBalance
		if tokenBalance.Owner != "" {
			accountTokenBalanceMap, ok := ownerAccountTokenBalanceMap[tokenBalance.Owner]
			if !ok {
				accountTokenBalanceMap = make(map[string]*TransferTokenBalance)
				ownerAccountTokenBalanceMap[tokenBalance.Owner] = accountTokenBalanceMap
			}
			accountTokenBalanceMap[tokenBalance.Pubkey] = tokenBalance
		}
	}

	//https://solscan.io/tx/5RBRcH7L1CDHqHKkwibCTSfWax3zJcJabeeQw1xmUHjZz1tATygF2w5HyYnY3pB9kTfbCeLjNta8ZngtecyHPzQe
	for _, preTokenBalance := range preTokenBalances {
		pubkey := accountKeys[preTokenBalance.AccountIndex].Pubkey
		if _, ok := tokenAccountTokenBalanceMap[pubkey]; !ok {
			preAmount, _ := new(big.Int).SetString(preTokenBalance.UiTokenAmount.Amount, 0)
			tokenBalance := &TransferTokenBalance{
				TokenBalance:   preTokenBalance,
				Pubkey:         pubkey,
				TransferAmount: new(big.Int).Neg(preAmount),
			}
			tokenAccountTokenBalanceMap[tokenBalance.Pubkey] = tokenBalance
			if tokenBalance.Owner != "" {
				accountTokenBalanceMap, ok := ownerAccountTokenBalanceMap[tokenBalance.Owner]
				if !ok {
					accountTokenBalanceMap = make(map[string]*TransferTokenBalance)
					ownerAccountTokenBalanceMap[tokenBalance.Owner] = accountTokenBalanceMap
				}
				accountTokenBalanceMap[tokenBalance.Pubkey] = tokenBalance
			}
		}
	}

	var instructionList []*Instruction
	var innerInstructionList []*Instruction
	var transferTotal, accountTotal, innerTransferTotal, innerAccountTotal int
	instructions := transaction.Message.Instructions
	innerInstructions := transactionInfo.Meta.InnerInstructions
	for _, instruction := range innerInstructions {
		inInstructions := instruction.Instructions
		innerInstructionList = append(innerInstructionList, inInstructions...)
	}

	//补全TokenBalance信息
	plugTokenBalance(instructions, accountKeyMap, tokenAccountTokenBalanceMap, ownerAccountTokenBalanceMap)
	plugTokenBalance(innerInstructionList, accountKeyMap, tokenAccountTokenBalanceMap, ownerAccountTokenBalanceMap)

	ownerMintTokenBalanceMap := make(map[string]map[string]*TransferTokenBalance)
	for owner, accountTokenBalanceMap := range ownerAccountTokenBalanceMap {
		mintTokenBalanceMap := make(map[string]*TransferTokenBalance)
		for _, tokenBalance := range accountTokenBalanceMap {
			mintTokenBalanceMap[tokenBalance.Mint] = tokenBalance
		}
		ownerMintTokenBalanceMap[owner] = mintTokenBalanceMap
	}

	// 合并交易
	//https://solscan.io/tx/51hTc8xEUAB53kCDh4uPbvbc7wCdherg5kpMNFKZ8vyroEPBiDEPTqrXJt4gUwSoZVLe7oLM9w736U6kmpDwKrSB
	tokenTransferAccounts := getTokenTransferDests(instructions)
	instructionList, transferTotal, accountTotal = mergeInstructions(instructions, tokenAccountTokenBalanceMap, tokenTransferAccounts)
	innerInstructionList, innerTransferTotal, innerAccountTotal = mergeInstructions(innerInstructionList, tokenAccountTokenBalanceMap, tokenTransferAccounts)

	payload, _ = utils.JsonEncode(map[string]interface{}{"accountKey": accountKeyMap, "tokenBalance": ownerMintTokenBalanceMap})
	isContract := false
	if innerTransferTotal > 0 || innerAccountTotal > 1 || len(instructionList) > 1 {
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
					amount = instructionInfo["amount"].(string)

					destinationTokenBalance := tokenAccountTokenBalanceMap[destination]
					if destinationTokenBalance != nil {
						toAddress = destinationTokenBalance.Owner
						contractAddress = destinationTokenBalance.Mint
					}
					if toAddress == "" {
						toAddress = destination
					}
					if contractAddress == "" {
						contractAddress = "UnknownToken"
					}
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
					tokenAmount := instructionInfo["tokenAmount"].(map[string]interface{})
					amount = tokenAmount["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)

					destinationTokenBalance := tokenAccountTokenBalanceMap[destination]
					if destinationTokenBalance != nil {
						toAddress = destinationTokenBalance.Owner
					}
					if toAddress == "" {
						toAddress = destination
					}
				}
			} else if instructionType == "mintTo" {
				if programId == SOL_CODE {
					txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					txType = biz.TRANSFER
					account := instructionInfo["account"].(string)
					amount = instructionInfo["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)

					accountTokenBalance := tokenAccountTokenBalanceMap[account]
					if accountTokenBalance != nil {
						toAddress = accountTokenBalance.Owner
					}
					if toAddress == "" {
						toAddress, _ = instructionInfo["multisigAuthority"].(string)
					}
				}
			} else if instructionType == "burn" {
				if programId == SOL_CODE {
					txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					txType = biz.TRANSFER
					account := instructionInfo["account"].(string)
					amount = instructionInfo["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)

					accountTokenBalance := tokenAccountTokenBalanceMap[account]
					if accountTokenBalance != nil {
						fromAddress = accountTokenBalance.Owner
					}
					if fromAddress == "" {
						fromAddress, _ = instructionInfo["multisigAuthority"].(string)
					}
				}
			} else {
				continue
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, fromAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, toAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", txHash), zap.Any("tokenAddress", contractAddress), zap.Any("error", err))
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
			tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
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
				TokenInfo:       tokenInfoStr,
				CreatedAt:       h.now.Unix(),
				UpdatedAt:       h.now.Unix(),
			}
			h.txRecords = append(h.txRecords, solTransactionRecord)
		}

		if len(h.txRecords) == 0 && transferTotal == 0 && accountTotal == 0 {
			var fromAddress, fromUid string
			var fromAddressExist bool

			for key, _ := range accountKeyMap {
				fromAddress = key
				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, fromAddress)
					if err != nil {
						log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
						return
					}
				}
				if fromAddressExist {
					break
				}
			}
			if !fromAddressExist {
				return
			}

			solTransactionRecord := &data.SolTransactionRecord{
				SlotNumber:      int(block.Number),
				BlockHash:       block.Hash,
				BlockNumber:     curHeight,
				TransactionHash: transactionHash,
				FromAddress:     fromAddress,
				//ToAddress:       toAddress,
				FromUid: fromUid,
				//ToUid:           toUid,
				FeeAmount: feeAmount,
				//Amount:          amountValue,
				Status: status,
				TxTime: txTime,
				/*ContractAddress: contractAddress,
				ParseData:       parseData,*/
				Data:            payload,
				EventLog:        "",
				TransactionType: biz.CONTRACT,
				CreatedAt:       h.now.Unix(),
				UpdatedAt:       h.now.Unix(),
			}
			h.txRecords = append(h.txRecords, solTransactionRecord)
		}
	} else {
		txType := biz.CONTRACT

		instructionList = append(instructionList, innerInstructionList...)
		//如果集合中同时包含createAccount和closeAccount，需要将这两笔抵消掉
		instructionList = reduceInstructions(instructionList)
		var eventLogs []*types.EventLogUid

		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		for _, fromAddress = range signerAddressList {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, fromAddress)
			if err != nil {
				log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.ChainName), zap.Any("current", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
		tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
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
			TokenInfo:       tokenInfoStr,
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
					amount = instructionInfo["amount"].(string)

					destinationTokenBalance := tokenAccountTokenBalanceMap[destination]
					if destinationTokenBalance != nil {
						toAddress = destinationTokenBalance.Owner
						contractAddress = destinationTokenBalance.Mint
					}
					if toAddress == "" {
						toAddress = destination
					}
					if contractAddress == "" {
						contractAddress = "UnknownToken"
					}
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
					tokenAmount := instructionInfo["tokenAmount"].(map[string]interface{})
					amount = tokenAmount["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)

					destinationTokenBalance := tokenAccountTokenBalanceMap[destination]
					if destinationTokenBalance != nil {
						toAddress = destinationTokenBalance.Owner
					}
					if toAddress == "" {
						toAddress = destination
					}
				}
			} else if instructionType == "mintTo" {
				if programId == SOL_CODE {
					//txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					//https://solscan.io/tx/4UDSnh9WKDMNm31a2Znm6jirw3guLce7QTqfFZM7hqppGUK14LEXQsSUEwJ6dXpTAXTooVYL4Q6g2UDLx5jpUCev
					//https://solscan.io/tx/5RBRcH7L1CDHqHKkwibCTSfWax3zJcJabeeQw1xmUHjZz1tATygF2w5HyYnY3pB9kTfbCeLjNta8ZngtecyHPzQe
					//txType = biz.TRANSFER
					account := instructionInfo["account"].(string)
					amount = instructionInfo["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)

					accountTokenBalance := tokenAccountTokenBalanceMap[account]
					if accountTokenBalance != nil {
						toAddress = accountTokenBalance.Owner
					}
					if toAddress == "" {
						toAddress, _ = instructionInfo["multisigAuthority"].(string)
					}
				}
			} else if instructionType == "burn" {
				if programId == SOL_CODE {
					//txType = biz.NATIVE
					fromAddress = instructionInfo["source"].(string)
					toAddress = instructionInfo["destination"].(string)
					amount = utils.GetString(instructionInfo["lamports"])
				} else {
					//https://solscan.io/tx/5RBRcH7L1CDHqHKkwibCTSfWax3zJcJabeeQw1xmUHjZz1tATygF2w5HyYnY3pB9kTfbCeLjNta8ZngtecyHPzQe
					//txType = biz.TRANSFER
					account := instructionInfo["account"].(string)
					amount = instructionInfo["amount"].(string)
					contractAddress = instructionInfo["mint"].(string)

					accountTokenBalance := tokenAccountTokenBalanceMap[account]
					if accountTokenBalance != nil {
						fromAddress = accountTokenBalance.Owner
					}
					if fromAddress == "" {
						fromAddress, _ = instructionInfo["multisigAuthority"].(string)
					}
				}
			} else {
				continue
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, fromAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.ChainName, toAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}
			if !fromAddressExist && !toAddressExist {
				continue
			}

			if contractAddress != "" {
				tokenInfo, err = biz.GetTokenNftInfoRetryAlert(nil, h.ChainName, contractAddress, programId)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
				if instructionType == "mintTo" && tokenInfo.TokenType == biz.SOLANANFT {
					solContractRecord.TransactionType = biz.MINT
				}
			}
			amountValue, _ := decimal.NewFromString(amount)
			eventLogInfo := &types.EventLogUid{
				EventLog: types.EventLog{
					From:   fromAddress,
					To:     toAddress,
					Amount: amountValue.BigInt(),
					Token:  tokenInfo,
				},
				FromUid: fromUid,
				ToUid:   toUid,
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
					} else if cmp == 0 {
						isContinue = true
						eventLogs[i] = nil
					} else if cmp == -1 {
						eventLogs[i] = nil
					}
					break
				} else if eventLog.From == eventLogInfo.From && eventLog.To == eventLogInfo.To && eventLog.Token.Address == eventLogInfo.Token.Address &&
					eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
					isContinue = true
					addAmount := new(big.Int).Add(eventLog.Amount, eventLogInfo.Amount)
					eventLogs[i].Amount = addAmount
					break
				}
			}
			if isContinue {
				continue
			}
			eventLogs = append(eventLogs, eventLogInfo)
		}

		//发送方主币实际变更的金额可能不等于eventLog中发送方主币转账的金额之和，需要给发送方再补一条主币转账的记录
		//https://solscan.io/tx/2UcUrEe1aggiN2yW591zJZTRAqyDrZNuSAnLWCuYNsnu4L2xsCqAmGZHKHR1Rr47PE6RadE7t4f6LLf7VKqcgoft
		//https://solscan.io/tx/5AdmKbJTcHe8bF2e9hNCYQdw4vX24zrLpctThkfoHTGMoLZmuoYHCaeozSd8SoU62yUcYCesnqc9mQgoF4ye37ND
		//https://solscan.io/tx/9A2LAznreAF5Jyr2Ro4c492gg1BRMmf81f2k4xvh6fgZ8HRpGiEAWXWnXeBHSve4qEtZjP14p4numJegCzfsGSs
		//https://solscan.io/tx/4r7pqxvNKntia3xaMJUCw97rwVjUyiyAAfcoPz6rdksofo7nsqdJ7Bn7yZQgrUtRaa6u5LeVa3QufLJvHJzQyj1D
		if len(eventLogs) > 0 {
			var eventLogFromAddress, eventLogToAddress, eventLogFromUid, eventLogToUid string
			eventLogTransferAmount := new(big.Int)
			for _, eventLog := range eventLogs {
				if eventLog == nil {
					continue
				}
				if eventLog.From != eventLog.To && eventLog.Token.Address == "" &&
					(fromAddress == eventLog.From || fromAddress == eventLog.To) {
					if fromAddress == eventLog.From {
						eventLogTransferAmount = eventLogTransferAmount.Sub(eventLogTransferAmount, eventLog.Amount)
					} else if fromAddress == eventLog.To {
						eventLogTransferAmount = eventLogTransferAmount.Add(eventLogTransferAmount, eventLog.Amount)
					}
				}
			}

			transferAmount := accountKeyMap[fromAddress].TransferAmount
			transferAmount = new(big.Int).Add(transferAmount, feeAmount.BigInt())
			transferAmount = transferAmount.Sub(transferAmount, eventLogTransferAmount)
			cmp := transferAmount.Cmp(new(big.Int))
			if cmp != 0 {
				if cmp < 0 {
					eventLogFromAddress = solContractRecord.FromAddress
					eventLogFromUid = solContractRecord.FromUid
					eventLogToAddress = ""
					eventLogToUid = ""
				} else if cmp > 0 {
					eventLogFromAddress = ""
					eventLogFromUid = ""
					eventLogToAddress = solContractRecord.FromAddress
					eventLogToUid = solContractRecord.FromUid
				}

				transferAmount = transferAmount.Abs(transferAmount)
				eventLogInfo := &types.EventLogUid{
					EventLog: types.EventLog{
						From:   eventLogFromAddress,
						To:     eventLogToAddress,
						Amount: transferAmount,
						Token:  tokenInfo,
					},
					FromUid: eventLogFromUid,
					ToUid:   eventLogToUid,
				}
				eventLogs = append(eventLogs, eventLogInfo)
			}
		}

		//发送方代币实际变更的金额可能不等于eventLog中发送方代币转账的金额之和，需要给发送方再补一或多条代币转账的记录
		//https://solscan.io/tx/Mqoxh4XT2zYKMxeNt1NAaYZbq4Z9jv1iY4r7HjWPPph1KdwXKKEuURSbncy7juX9HapySeSApGUNBw6GBEHfrqv
		if len(eventLogs) > 0 {
			var eventLogFromAddress, eventLogToAddress, eventLogFromUid, eventLogToUid string
			tokenAddressAmountMap := make(map[string]*big.Int)
			for _, eventLog := range eventLogs {
				if eventLog == nil {
					continue
				}
				if eventLog.From != eventLog.To && eventLog.Token.Address != "" &&
					(fromAddress == eventLog.From || fromAddress == eventLog.To) {
					eventLogTransferAmount, ok := tokenAddressAmountMap[eventLog.Token.Address]
					if !ok {
						eventLogTransferAmount = new(big.Int)
					}
					if fromAddress == eventLog.From {
						eventLogTransferAmount = eventLogTransferAmount.Sub(eventLogTransferAmount, eventLog.Amount)
					} else if fromAddress == eventLog.To {
						eventLogTransferAmount = eventLogTransferAmount.Add(eventLogTransferAmount, eventLog.Amount)
					}
					tokenAddressAmountMap[eventLog.Token.Address] = eventLogTransferAmount
				}
			}

			if mintTokenBalanceMap, ook := ownerMintTokenBalanceMap[fromAddress]; ook {
				for mint, tokenBalance := range mintTokenBalanceMap {
					transferAmount := tokenBalance.TransferAmount
					if transferAmount.String() == "0" {
						continue
					}
					eventLogTransferAmount, ok := tokenAddressAmountMap[mint]
					if !ok {
						eventLogTransferAmount = new(big.Int)
					}
					transferAmount = new(big.Int).Sub(transferAmount, eventLogTransferAmount)
					cmp := transferAmount.Cmp(new(big.Int))
					if cmp != 0 {
						if cmp < 0 {
							eventLogFromAddress = solContractRecord.FromAddress
							eventLogFromUid = solContractRecord.FromUid
							eventLogToAddress = ""
							eventLogToUid = ""
						} else if cmp > 0 {
							eventLogFromAddress = ""
							eventLogFromUid = ""
							eventLogToAddress = solContractRecord.FromAddress
							eventLogToUid = solContractRecord.FromUid
						}

						transferAmount = transferAmount.Abs(transferAmount)
						if mint != "" {
							tokenInfo, err = biz.GetTokenNftInfoRetryAlert(nil, h.ChainName, mint, tokenBalance.ProgramId)
							if err != nil {
								log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.ChainName), zap.Any("curHeight", curHeight), zap.Any("new", h.chainHeight), zap.Any("curSlot", curSlot), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", mint), zap.Any("error", err))
							}
							tokenInfo.Amount = transferAmount.String()
							tokenInfo.Address = mint
						}

						eventLogInfo := &types.EventLogUid{
							EventLog: types.EventLog{
								From:   eventLogFromAddress,
								To:     eventLogToAddress,
								Amount: transferAmount,
								Token:  tokenInfo,
							},
							FromUid: eventLogFromUid,
							ToUid:   eventLogToUid,
						}
						eventLogs = append(eventLogs, eventLogInfo)
					}
				}
			}
		}

		//删除重复主币转账的记录
		if len(eventLogs) > 0 {
			var mainEventLog *types.EventLogUid
			var mainToAddress, mainAmount string
			for i, eventLog := range eventLogs {
				if eventLog == nil {
					continue
				}
				if solContractRecord.FromAddress == eventLog.From {
					if eventLog.Token.Address == "So11111111111111111111111111111111111111112" {
						if mintTokenBalanceMap, ook := ownerMintTokenBalanceMap[eventLog.From]; ook {
							tokenBalance, ok := mintTokenBalanceMap[eventLog.Token.Address]
							if !ok || tokenBalance.TransferAmount.String() == "0" {
								//https://solscan.io/tx/5fY1Vc9jX84Cgk78eUUpCH64AMjzM6sEuPEzLNkqtpFWPHNz7AaCfqV2t4PicnZWFpurzGQVBC77GT21eQ9XAQdU
								//https://solscan.io/tx/2UiAvd2BhToCovpyxRcM2RL5Kry7exjsz5ckBmETYpu1FTCsiLiKAw7e5s49mqWoAScWEMwYNHD9kuoD3e3LH33a
								//https://solscan.io/tx/2UcUrEe1aggiN2yW591zJZTRAqyDrZNuSAnLWCuYNsnu4L2xsCqAmGZHKHR1Rr47PE6RadE7t4f6LLf7VKqcgoft
								//https://solscan.io/tx/5AdmKbJTcHe8bF2e9hNCYQdw4vX24zrLpctThkfoHTGMoLZmuoYHCaeozSd8SoU62yUcYCesnqc9mQgoF4ye37ND
								if eventLog.From == eventLog.To {
									//https://solscan.io/tx/9A2LAznreAF5Jyr2Ro4c492gg1BRMmf81f2k4xvh6fgZ8HRpGiEAWXWnXeBHSve4qEtZjP14p4numJegCzfsGSs
									continue
								}
								eventLogs[i] = nil
								mainToAddress = eventLog.To
								mainAmount = eventLog.Token.Amount
							}
						}
					} else if eventLog.Token.Address == "" && eventLog.To == "" {
						mainEventLog = eventLog
					}
				}
				if mainEventLog != nil && mainAmount != "" {
					if mainEventLog.To == "" && mainEventLog.Amount.String() == mainAmount {
						//https://solscan.io/tx/2UcUrEe1aggiN2yW591zJZTRAqyDrZNuSAnLWCuYNsnu4L2xsCqAmGZHKHR1Rr47PE6RadE7t4f6LLf7VKqcgoft
						mainEventLog.To = mainToAddress
					}
					break
				}
			}
		}

		if len(eventLogs) > 0 {
			var eventLogList []*types.EventLogUid
			for _, eventLog := range eventLogs {
				if eventLog != nil {
					eventLogList = append(eventLogList, eventLog)
				}
			}
			eventLogs = eventLogList
		}

		if fromAddressExist || toAddressExist || len(eventLogs) > 0 {
			h.txRecords = append(h.txRecords, solContractRecord)
		}

		if eventLogs != nil {
			eventLog, _ := utils.JsonEncode(eventLogs)
			solContractRecord.EventLog = eventLog
		}

		if len(eventLogs) > 0 {
			logAddress := biz.GetLogAddressFromEventLogUid(eventLogs)
			// database btree index maximum is 2704
			logAddressLen := len(logAddress)
			if logAddressLen > 2704 {
				log.Error("扫块，logAddress长度超过最大限制", zap.Any("chainName", h.ChainName), zap.Any("txHash", transactionHash), zap.Any("logAddressLen", logAddressLen))
				logAddress = nil
			}
			solContractRecord.LogAddress = logAddress
		}

		for index, eventLog := range eventLogs {
			eventMap := map[string]interface{}{
				"token": eventLog.Token,
			}
			eventParseData, _ := utils.JsonEncode(eventMap)
			eventTokenInfoStr, _ := utils.JsonEncode(eventLog.Token)
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index+1)
			txType := biz.EVENTLOG
			contractAddress := eventLog.Token.Address
			amountValue := decimal.NewFromBigInt(eventLog.Amount, 0)

			solTransactionRecord := &data.SolTransactionRecord{
				SlotNumber:      int(block.Number),
				BlockHash:       block.Hash,
				BlockNumber:     curHeight,
				TransactionHash: txHash,
				FromAddress:     eventLog.From,
				ToAddress:       eventLog.To,
				FromUid:         eventLog.FromUid,
				ToUid:           eventLog.ToUid,
				FeeAmount:       feeAmount,
				Amount:          amountValue,
				Status:          status,
				TxTime:          txTime,
				ContractAddress: contractAddress,
				ParseData:       eventParseData,
				Data:            payload,
				EventLog:        "",
				TransactionType: txType,
				TokenInfo:       eventTokenInfoStr,
				CreatedAt:       h.now.Unix(),
				UpdatedAt:       h.now.Unix(),
			}
			h.txRecords = append(h.txRecords, solTransactionRecord)
		}
		eventLogLen := len(eventLogs)

		if eventLogLen == 2 && ((solContractRecord.FromAddress == eventLogs[0].From && solContractRecord.FromAddress == eventLogs[1].To) ||
			(solContractRecord.FromAddress == eventLogs[0].To && solContractRecord.FromAddress == eventLogs[1].From)) {
			solContractRecord.TransactionType = biz.SWAP
		} else if eventLogLen > 2 {
			logMessages := meta.LogMessages
			for _, logMessage := range logMessages {
				if strings.Contains(logMessage, "Swap") || strings.Contains(logMessage, "_swap") || strings.Contains(logMessage, " swap") {
					solContractRecord.TransactionType = biz.SWAP
					break
				}
			}
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

func (h *txDecoder) Save(client chain.Clienter) error {
	txRecords := h.txRecords
	if txRecords != nil && len(txRecords) > 0 {
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.ChainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
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

func plugTokenBalance(instructions []*Instruction, accountKeyMap map[string]*TransferAccountKey, tokenAccountTokenBalanceMap map[string]*TransferTokenBalance, ownerAccountTokenBalanceMap map[string]map[string]*TransferTokenBalance) {
	var instructionList []*Instruction
	for _, instruction := range instructions {
		var contractAddress string
		var fromAddress, source, destination string
		var isPlug bool

		parsed, ok := instruction.Parsed.(map[string]interface{})
		if !ok {
			instructionList = append(instructionList, instruction)
			continue
		}
		programId := instruction.ProgramId
		instructionType := parsed["type"]
		instructionInfo := parsed["info"].(map[string]interface{})

		if instructionType == "transfer" {
			if programId != SOL_CODE {
				fromAddress, ok = instructionInfo["authority"].(string)
				if !ok {
					fromAddress = instructionInfo["multisigAuthority"].(string)
				}
				source = instructionInfo["source"].(string)
				destination = instructionInfo["destination"].(string)

				isPlug = true
			}
		} else if instructionType == "transferChecked" {
			if programId != SOL_CODE {
				fromAddress, ok = instructionInfo["authority"].(string)
				if !ok {
					fromAddress = instructionInfo["multisigAuthority"].(string)
				}
				source = instructionInfo["source"].(string)
				destination = instructionInfo["destination"].(string)
				contractAddress = instructionInfo["mint"].(string)

				isPlug = true
			}
		}

		if !isPlug {
			continue
		}

		if tokenBalance, tok := tokenAccountTokenBalanceMap[source]; tok {
			if tokenBalance.Owner == "" {
				//补全缺失的owner字段
				//https://solscan.io/tx/51hTc8xEUAB53kCDh4uPbvbc7wCdherg5kpMNFKZ8vyroEPBiDEPTqrXJt4gUwSoZVLe7oLM9w736U6kmpDwKrSB
				tokenBalance.Owner = fromAddress

				accountTokenBalanceMap, ook := ownerAccountTokenBalanceMap[tokenBalance.Owner]
				if !ook {
					accountTokenBalanceMap = make(map[string]*TransferTokenBalance)
					ownerAccountTokenBalanceMap[tokenBalance.Owner] = accountTokenBalanceMap
				}
				accountTokenBalanceMap[source] = tokenBalance
			}

			if _, dtok := tokenAccountTokenBalanceMap[destination]; !dtok {
				//补全缺失的中间态的TokenBalance数据
				//https://solscan.io/tx/2UcUrEe1aggiN2yW591zJZTRAqyDrZNuSAnLWCuYNsnu4L2xsCqAmGZHKHR1Rr47PE6RadE7t4f6LLf7VKqcgoft destination:FNoXKiLX4v618Rc8TMaf35pUicVhbKzymeWpa24AzXFq
				destinationTokenBalance := &TransferTokenBalance{
					TokenBalance: &TokenBalance{
						AccountIndex: accountKeyMap[destination].Index,
						Mint:         tokenBalance.Mint,
						Owner:        "",
						ProgramId:    tokenBalance.ProgramId,
					},
					Pubkey:         destination,
					TransferAmount: new(big.Int),
				}
				tokenAccountTokenBalanceMap[destination] = destinationTokenBalance
			}
		} else if tokenBalance, tok = tokenAccountTokenBalanceMap[destination]; tok {
			//补全缺失的中间态的TokenBalance数据
			//https://solscan.io/tx/2UiAvd2BhToCovpyxRcM2RL5Kry7exjsz5ckBmETYpu1FTCsiLiKAw7e5s49mqWoAScWEMwYNHD9kuoD3e3LH33a destination:7x4VcEX8aLd3kFsNWULTp1qFgVtDwyWSxpTGQkoMM6XX
			sourceTokenBalance := &TransferTokenBalance{
				TokenBalance: &TokenBalance{
					AccountIndex: accountKeyMap[destination].Index,
					Mint:         tokenBalance.Mint,
					Owner:        fromAddress,
					ProgramId:    tokenBalance.ProgramId,
				},
				Pubkey:         source,
				TransferAmount: new(big.Int),
			}
			tokenAccountTokenBalanceMap[source] = sourceTokenBalance

			accountTokenBalanceMap, ook := ownerAccountTokenBalanceMap[sourceTokenBalance.Owner]
			if !ook {
				accountTokenBalanceMap = make(map[string]*TransferTokenBalance)
				ownerAccountTokenBalanceMap[sourceTokenBalance.Owner] = accountTokenBalanceMap
			}
			accountTokenBalanceMap[source] = sourceTokenBalance
		} else if accountTokenBalanceMap, ook := ownerAccountTokenBalanceMap[fromAddress]; ook {
			//补全缺失的中间态的TokenBalance数据
			if tokenBalance, tok = accountTokenBalanceMap[source]; tok {
				if tokenBalance.Owner == "" {
					tokenBalance.Owner = fromAddress
				}
				tokenAccountTokenBalanceMap[source] = tokenBalance
			}
		} else if contractAddress != "" {
			//补全缺失的中间态的TokenBalance数据
			transferAmount := new(big.Int)
			destinationTokenBalance := &TransferTokenBalance{
				TokenBalance: &TokenBalance{
					AccountIndex: accountKeyMap[destination].Index,
					Mint:         contractAddress,
					Owner:        "",
				},
				Pubkey:         destination,
				TransferAmount: transferAmount,
			}
			tokenAccountTokenBalanceMap[destination] = destinationTokenBalance

			sourceTokenBalance := &TransferTokenBalance{
				TokenBalance: &TokenBalance{
					AccountIndex: accountKeyMap[destination].Index,
					Mint:         contractAddress,
					Owner:        fromAddress,
				},
				Pubkey:         source,
				TransferAmount: transferAmount,
			}
			tokenAccountTokenBalanceMap[source] = sourceTokenBalance

			accountTokenBalanceMap, ook = ownerAccountTokenBalanceMap[sourceTokenBalance.Owner]
			if !ook {
				accountTokenBalanceMap = make(map[string]*TransferTokenBalance)
				ownerAccountTokenBalanceMap[sourceTokenBalance.Owner] = accountTokenBalanceMap
			}
			accountTokenBalanceMap[source] = sourceTokenBalance
		}
	}
}

func getTokenTransferDests(instructions []*Instruction) map[string]bool {
	// Create a map for erasing ATA creation that for transfering tokens
	tokenTransfers := make(map[string]bool)
	for _, instruction := range instructions {
		parsed, ok := instruction.Parsed.(map[string]interface{})
		if !ok {
			continue
		}
		instructionType := parsed["type"]
		instructionInfo := parsed["info"].(map[string]interface{})
		if instructionType == "transferChecked" {
			tokenTransfers[instructionInfo["destination"].(string)] = true
		}
	}
	return tokenTransfers
}

func mergeInstructions(
	instructions []*Instruction,
	tokenAccountTokenBalanceMap map[string]*TransferTokenBalance,
	tokenTransferAccounts map[string]bool,
) ([]*Instruction, int, int) {
	var instructionList []*Instruction
	var instructionMap = make(map[string]*Instruction)
	var createInstructionMap = make(map[string]*Instruction)
	var closeInstructionMap = make(map[string]*Instruction)
	var transferTotal, accountTotal int

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
			transferTotal++
			if programId == SOL_CODE {
				//https://solscan.io/tx/EeuunP7yQfF45M8Jaeab9ibdP6a9vJPJicNwdBrFYtgHiRye8JqTciMkM68TKNPpsnmK4s6vFGoHDyx2XFHGZsR
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + ":" + toAddress + ":" + contractAddress
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
				//source := instructionInfo["source"].(string)
				destination := instructionInfo["destination"].(string)

				destinationTokenBalance := tokenAccountTokenBalanceMap[destination]
				//https://solscan.io/tx/2zXBRUVkGwv1u3U6kujiJg9B6jNT1p3PEhX8AywSkY1wy5CA3tM6xjDigi24hmK5aNjhw1u1HL4vknmijmWNpPU3
				if destinationTokenBalance != nil {
					toAddress = destinationTokenBalance.Owner
					contractAddress = destinationTokenBalance.Mint
				}
				if toAddress == "" {
					toAddress = destination
				}
				if contractAddress == "" {
					//https://solscan.io/tx/3QH3FQF9EUoU19xKPXWtFYgeuxpmUUC6UjSNVh3ZUc9qVP3UEyx6NDbdnpr9gCh1qdwEex37cGEHui3gGQhfKRdE
					contractAddress = "UnknownToken"
				}

				key := fromAddress + ":" + toAddress + ":" + contractAddress
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
			transferTotal++
			if programId == SOL_CODE {
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + ":" + toAddress + ":" + contractAddress
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
				//source := instructionInfo["source"].(string)
				destination := instructionInfo["destination"].(string)
				tokenAmount := instructionInfo["tokenAmount"].(map[string]interface{})
				contractAddress = instructionInfo["mint"].(string)

				destinationTokenBalance := tokenAccountTokenBalanceMap[destination]
				if destinationTokenBalance != nil {
					toAddress = destinationTokenBalance.Owner
				}
				if toAddress == "" {
					toAddress = destination
				}

				key := fromAddress + ":" + toAddress + ":" + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = instruction
					instructionMap[key] = newInstruction
				} else {
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
			transferTotal++
			if programId == SOL_CODE {
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + ":" + toAddress + ":" + contractAddress
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
				account := instructionInfo["account"].(string)
				contractAddress = instructionInfo["mint"].(string)

				accountTokenBalance := tokenAccountTokenBalanceMap[account]
				if accountTokenBalance != nil {
					toAddress = accountTokenBalance.Owner
				}
				if toAddress == "" {
					toAddress, _ = instructionInfo["multisigAuthority"].(string)
				}

				//https://solscan.io/tx/RAEVNRjq2mTr8KGTkV6kcYdkjnpaNMpVisqQ24GoHw84uFVvTUpJYwhpmLYaX8DKGgj7qek4ggcHRYmnK8iAyXA
				key := fromAddress + ":" + toAddress + ":" + contractAddress
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
		} else if instructionType == "burn" {
			transferTotal++
			if programId == SOL_CODE {
				fromAddress = instructionInfo["source"].(string)

				key := fromAddress + ":" + toAddress + ":" + contractAddress
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
				account := instructionInfo["account"].(string)
				contractAddress = instructionInfo["mint"].(string)

				accountTokenBalance := tokenAccountTokenBalanceMap[account]
				//https://solscan.io/tx/3ziUnrLZoC5wiGGZyh8TaTRjJ6LDQ9wwjzpDWKHL752bUQdUPuYEuauNCf1o6sxtj4z6w7CTmJkDWFFLjograaB2
				if accountTokenBalance != nil {
					fromAddress = accountTokenBalance.Owner
				}
				if fromAddress == "" {
					fromAddress, _ = instructionInfo["multisigAuthority"].(string)
				}

				key := fromAddress + ":" + toAddress + ":" + contractAddress
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
			// Ignore ATA creatation for transfering token.
			if newAccount, ok := instructionInfo["newAccount"]; ok {
				if _, ok := tokenTransferAccounts[newAccount.(string)]; ok {
					continue
				}
			}

			accountTotal++
			fromAddress = instructionInfo["source"].(string)
			toAddress = instructionInfo["newAccount"].(string)
			key := fromAddress + ":" + toAddress
			if _, closeOk := closeInstructionMap[key]; closeOk {
				delete(closeInstructionMap, key)
			} else {
				createInstructionMap[key] = instruction
			}
		} else if instructionType == "closeAccount" {
			accountTotal++
			fromAddress = instructionInfo["account"].(string)
			toAddress = instructionInfo["destination"].(string)
			key := toAddress + ":" + fromAddress
			if _, createOk := createInstructionMap[key]; createOk {
				delete(createInstructionMap, key)
			} else {
				closeInstructionMap[key] = instruction
			}
		} else {
			if instructionType == "create" {
				// Ignore ATA creatation for transfering token.
				if account, ok := instructionInfo["account"]; ok {
					if _, ok := tokenTransferAccounts[account.(string)]; ok && instruction.Program == "spl-associated-token-account" {
						continue
					}
				}
			}

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

	return instructionList, transferTotal, accountTotal
}

func reduceInstructions(instructions []*Instruction) []*Instruction {
	var instructionList []*Instruction
	var createInstructionMap = make(map[string]*Instruction)
	var closeInstructionMap = make(map[string]*Instruction)
	for _, instruction := range instructions {
		var fromAddress, toAddress string

		parsed, ok := instruction.Parsed.(map[string]interface{})
		if !ok {
			instructionList = append(instructionList, instruction)
			continue
		}
		instructionType := parsed["type"]
		instructionInfo := parsed["info"].(map[string]interface{})

		if instructionType == "createAccount" {
			fromAddress = instructionInfo["source"].(string)
			toAddress = instructionInfo["newAccount"].(string)
			key := fromAddress + ":" + toAddress
			if _, closeOk := closeInstructionMap[key]; closeOk {
				delete(closeInstructionMap, key)
			} else {
				createInstructionMap[key] = instruction
			}
		} else if instructionType == "closeAccount" {
			fromAddress = instructionInfo["account"].(string)
			toAddress = instructionInfo["destination"].(string)
			key := toAddress + ":" + fromAddress
			if _, createOk := createInstructionMap[key]; createOk {
				delete(createInstructionMap, key)
			} else {
				closeInstructionMap[key] = instruction
			}
		} else {
			instructionList = append(instructionList, instruction)
		}
	}
	for _, instruction := range createInstructionMap {
		instructionList = append(instructionList, instruction)
	}
	for _, instruction := range closeInstructionMap {
		instructionList = append(instructionList, instruction)
	}

	return instructionList
}
