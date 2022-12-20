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
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txDecoder struct {
	ChainName string
	block     *chain.Block
	txByHash  *chain.Transaction
	now       time.Time
	newTxs    bool

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

type Instructions struct {
	/*Parsed *struct {
		Info map[string]interface{} `json:"info"`
		Type string                 `json:"type"`
	} `json:"parsed,omitempty"`*/
	Parsed    interface{} `json:"parsed"`
	Program   string      `json:"program"`
	ProgramId string      `json:"programId"`

	Accounts []string `json:"accounts,omitempty"`
	Data     string   `json:"data"`
	//ProgramId string `json:"programId"`
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	curSlot := block.Number
	curHeight := block.Raw.(int)
	transactionInfo := tx.Raw.(*TransactionInfo)

	txTime := block.Time

	meta := transactionInfo.Meta
	fee := meta.Fee
	feeAmount := decimal.NewFromInt(meta.Fee)
	var payload string
	err := meta.Err
	var status string
	if err == nil {
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

	instructions := transaction.Message.Instructions

	// 合并交易，例如txHash:51hTc8xEUAB53kCDh4uPbvbc7wCdherg5kpMNFKZ8vyroEPBiDEPTqrXJt4gUwSoZVLe7oLM9w736U6kmpDwKrSB
	var instructionList []Instructions
	var instructionMap = make(map[string]Instructions)
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
			if programId == "11111111111111111111111111111111" {
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = Instructions{
						Parsed:    instruction.Parsed,
						Program:   instruction.Program,
						ProgramId: instruction.ProgramId,
						Accounts:  instruction.Accounts,
						Data:      instruction.Data,
					}
					instructionMap[key] = newInstruction
				} else {
					if value, ok := instructionInfo["lamports"].(float64); ok {
						amount = fmt.Sprintf("%.f", value)
					} else {
						amount = fmt.Sprintf("%v", instructionInfo["lamports"])
					}

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					if value, ok := newInstructionInfo["lamports"].(float64); ok {
						newAmount = fmt.Sprintf("%.f", value)
					} else {
						newAmount = fmt.Sprintf("%v", newInstructionInfo["lamports"])
					}
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
					newInstruction = Instructions{
						Parsed:    instruction.Parsed,
						Program:   instruction.Program,
						ProgramId: instruction.ProgramId,
						Accounts:  instruction.Accounts,
						Data:      instruction.Data,
					}
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
			if programId == "11111111111111111111111111111111" {
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = Instructions{
						Parsed:    instruction.Parsed,
						Program:   instruction.Program,
						ProgramId: instruction.ProgramId,
						Accounts:  instruction.Accounts,
						Data:      instruction.Data,
					}
					instructionMap[key] = newInstruction
				} else {
					if value, ok := instructionInfo["lamports"].(float64); ok {
						amount = fmt.Sprintf("%.f", value)
					} else {
						amount = fmt.Sprintf("%v", instructionInfo["lamports"])
					}

					newParsed := newInstruction.Parsed.(map[string]interface{})
					newInstructionInfo := newParsed["info"].(map[string]interface{})
					if value, ok := newInstructionInfo["lamports"].(float64); ok {
						newAmount = fmt.Sprintf("%.f", value)
					} else {
						newAmount = fmt.Sprintf("%v", newInstructionInfo["lamports"])
					}
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
				if sourceTokenBalance.Owner == "" {
					sourceTokenBalance.Owner = fromAddress
					tokenBalanceOwnerMap[sourceTokenBalance.Owner] = sourceTokenBalance
				}
				contractAddress = instructionInfo["mint"].(string)

				key := fromAddress + toAddress + contractAddress
				newInstruction, ok := instructionMap[key]
				if !ok {
					newInstruction = Instructions{
						Parsed:    instruction.Parsed,
						Program:   instruction.Program,
						ProgramId: instruction.ProgramId,
						Accounts:  instruction.Accounts,
						Data:      instruction.Data,
					}
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
		} else {
			instructionList = append(instructionList, instruction)
		}
	}
	for _, instruction := range instructionMap {
		instructionList = append(instructionList, instruction)
	}

	payload, _ = utils.JsonEncode(map[string]interface{}{"accountKey": accountKeyMap, "tokenBalance": tokenBalanceOwnerMap})
	index := 0
	for _, instruction := range instructionList {
		txType := biz.NATIVE
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress string

		parsed, ok := instruction.Parsed.(map[string]interface{})
		if !ok {
			continue
		}
		programId := instruction.ProgramId
		instructionType := parsed["type"]
		instructionInfo := parsed["info"].(map[string]interface{})
		if instructionType == "create" {
			txType = biz.REGISTERTOKEN
			fromAddress = instructionInfo["source"].(string)
			toAddress = instructionInfo["account"].(string)
			index := accountKeyMap[toAddress].Index
			bigAmount := new(big.Int).Sub(postBalances[index], preBalances[index])
			if len(instructions) == 1 {
				amount = bigAmount.String()
			} else {
				fee = fee + bigAmount.Int64()
				feeAmount = decimal.NewFromInt(fee)
				continue
			}
		} else if instructionType == "transfer" {
			if programId == "11111111111111111111111111111111" {
				txType = biz.NATIVE
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)
				if value, ok := instructionInfo["lamports"].(float64); ok {
					amount = fmt.Sprintf("%.f", value)
				} else {
					amount = fmt.Sprintf("%v", instructionInfo["lamports"])
				}
			} else {
				txType = biz.TRANSFER
				fromAddress, ok = instructionInfo["authority"].(string)
				if !ok {
					fromAddress = instructionInfo["multisigAuthority"].(string)
				}
				destination := instructionInfo["destination"].(string)
				toAddress = tokenBalanceMap[destination].Owner
				amount = instructionInfo["amount"].(string)
				contractAddress = tokenBalanceMap[destination].Mint
			}
		} else if instructionType == "transferChecked" {
			if programId == "11111111111111111111111111111111" {
				txType = biz.NATIVE
				fromAddress = instructionInfo["source"].(string)
				toAddress = instructionInfo["destination"].(string)
				if value, ok := instructionInfo["lamports"].(float64); ok {
					amount = fmt.Sprintf("%.f", value)
				} else {
					amount = fmt.Sprintf("%v", instructionInfo["lamports"])
				}
			} else {
				txType = biz.TRANSFER
				fromAddress, ok = instructionInfo["authority"].(string)
				if !ok {
					fromAddress = instructionInfo["multisigAuthority"].(string)
				}
				destination := instructionInfo["destination"].(string)
				toAddress = tokenBalanceMap[destination].Owner
				tokenAmount := instructionInfo["tokenAmount"].(map[string]interface{})
				amount = tokenAmount["amount"].(string)
				contractAddress = instructionInfo["mint"].(string)
			}
		} else {
			continue
		}

		user, err := h.matchUser(fromAddress, toAddress)
		if err != nil {
			return err
		}
		if !(user.matchFrom || user.matchTo) {
			return nil
		}

		index++
		var txHash string
		if index == 1 {
			txHash = tx.Hash
		} else {
			txHash = tx.Hash + "#result-" + fmt.Sprintf("%v", index)
		}

		if contractAddress != "" {
			var symbol string
			var decimals = int64(tokenBalanceOwnerMap[fromAddress].UiTokenAmount.Decimals)
			getTokenInfo, err := biz.GetTokenInfo(nil, h.ChainName, contractAddress)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				getTokenInfo, err = biz.GetTokenInfo(nil, h.ChainName, contractAddress)
			}
			if err != nil {
				// nodeProxy出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("curHeight", curHeight), zap.Any("curSlot", curSlot), zap.Any("error", err))
			} else if getTokenInfo.Symbol != "" {
				symbol = getTokenInfo.Symbol
				decimals = getTokenInfo.Decimals
			}
			tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: symbol, Decimals: decimals, Amount: amount}
		}
		stcMap := map[string]interface{}{
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(stcMap)
		amountValue, _ := decimal.NewFromString(amount)

		solTransactionRecord := &data.SolTransactionRecord{
			SlotNumber:      int(block.Number),
			BlockHash:       block.Hash,
			BlockNumber:     curHeight,
			TransactionHash: txHash,
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			FromUid:         user.fromUid,
			ToUid:           user.toUid,
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
	return nil
}

type userMeta struct {
	matchFrom bool
	fromUid   string
	matchTo   bool
	toUid     string
}

func (h *txDecoder) matchUser(fromAddress, toAddress string) (*userMeta, error) {
	userFromAddress, fromUid, err := biz.UserAddressSwitch(fromAddress)
	if err != nil {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", h.ChainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Info("查询redis缓存报错：用户中心获取", zap.Any(h.ChainName, fromAddress), zap.Any("error", err))
		return nil, err
	}
	userToAddress, toUid, err := biz.UserAddressSwitch(toAddress)
	if err != nil {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", h.ChainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Info("查询redis缓存报错：用户中心获取", zap.Any(h.ChainName, toAddress), zap.Any("error", err))
		return nil, err
	}
	return &userMeta{
		matchFrom: userFromAddress,
		fromUid:   fromUid,

		matchTo: userToAddress,
		toUid:   toUid,
	}, nil
}

func (h *txDecoder) Save(client chain.Clienter) error {
	txRecords := h.txRecords
	if txRecords != nil && len(txRecords) > 0 {
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(h.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			log.Error("插入数据到数据库库中失败", zap.Any("current", h.block.Number), zap.Any("chain", h.ChainName))
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", h.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Info("插入数据库报错：", zap.Any(h.ChainName, err))
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
