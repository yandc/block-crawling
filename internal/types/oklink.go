package types

type OklinkBlockChainSummaryResp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		ChainFullName               string `json:"chainFullName"`
		ChainShortName              string `json:"chainShortName"`
		Symbol                      string `json:"symbol"`
		LastHeight                  string `json:"lastHeight"`
		LastBlockTime               string `json:"lastBlockTime"`
		CirculatingSupply           string `json:"circulatingSupply"`
		CirculatingSupplyProportion string `json:"circulatingSupplyProportion"`
		Transactions                string `json:"transactions"`
	} `json:"data"`
}

type OklinkBlockFillsResp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		ChainFullName  string `json:"chainFullName"`
		ChainShortName string `json:"chainShortName"`
		Hash           string `json:"hash"`
		Height         string `json:"height"`
		Validator      string `json:"validator"`
		BlockTime      string `json:"blockTime"`
		TxnCount       string `json:"txnCount"`
		Amount         string `json:"amount"`
		BlockSize      string `json:"blockSize"`
		MineReward     string `json:"mineReward"`
		TotalFee       string `json:"totalFee"`
		FeeSymbol      string `json:"feeSymbol"`
		OmmerBlock     string `json:"ommerBlock"`
		MerkleRootHash string `json:"merkleRootHash"`
		GasUsed        string `json:"gasUsed"`
		GasLimit       string `json:"gasLimit"`
		GasAvgPrice    string `json:"gasAvgPrice"`
		State          string `json:"state"`
		Burnt          string `json:"burnt"`
		NetWork        string `json:"netWork"`
		TxnInternal    string `json:"txnInternal"`
		Miner          string `json:"miner"`
		Difficuity     string `json:"difficuity"`
		Nonce          string `json:"nonce"`
		Tips           string `json:"tips"`
		Confirm        string `json:"confirm"`
		BaseFeePerGas  string `json:"baseFeePerGas"`
	} `json:"data"`
}

type OklinkTransactionListResp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		Page            string `json:"page"`
		Limit           string `json:"limit"`
		TotalPage       string `json:"totalPage"`
		ChainFullName   string `json:"chainFullName"`
		ChainShortName  string `json:"chainShortName"`
		TransactionList []struct {
			Txid              string `json:"txid"`
			BlockHash         string `json:"blockHash"`
			Height            string `json:"height"`
			TransactionTime   string `json:"transactionTime"`
			Input             string `json:"input"`
			Output            string `json:"output"`
			IsInputContract   bool   `json:"isInputContract"`
			IsOutputContract  bool   `json:"isOutputContract"`
			Amount            string `json:"amount"`
			TransactionSymbol string `json:"transactionSymbol"`
			Txfee             string `json:"txfee"`
			MethodId          string `json:"methodId"`
			TransactionType   string `json:"transactionType"`
			State             string `json:"state"`
		} `json:"transactionList"`
	} `json:"data"`
}

type OklinkTransactionDetailResp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		ChainFullName     string `json:"chainFullName"`
		ChainShortName    string `json:"chainShortName"`
		Txid              string `json:"txid"`
		Height            string `json:"height"`
		TransactionTime   string `json:"transactionTime"`
		Amount            string `json:"amount"`
		TransactionSymbol string `json:"transactionSymbol"`
		Txfee             string `json:"txfee"`
		Index             string `json:"index"`
		Confirm           string `json:"confirm"`
		InputDetails      []struct {
			InputHash  string `json:"inputHash"`
			IsContract bool   `json:"isContract"`
			Amount     string `json:"amount"`
		} `json:"inputDetails"`
		OutputDetails []struct {
			OutputHash string `json:"outputHash"`
			IsContract bool   `json:"isContract"`
			Amount     string `json:"amount"`
		} `json:"outputDetails"`
		State                string        `json:"state"`
		GasLimit             string        `json:"gasLimit"`
		GasUsed              string        `json:"gasUsed"`
		GasPrice             string        `json:"gasPrice"`
		TotalTransactionSize string        `json:"totalTransactionSize"`
		VirtualSize          string        `json:"virtualSize"`
		Weight               string        `json:"weight"`
		Nonce                string        `json:"nonce"`
		TransactionType      string        `json:"transactionType"`
		MethodId             string        `json:"methodId"`
		IsAaTransaction      bool          `json:"isAaTransaction"`
		TokenTransferDetails []interface{} `json:"tokenTransferDetails"`
		ContractDetails      []interface{} `json:"contractDetails"`
	} `json:"data"`
}

type OklinkAddressSummaryResp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		ChainFullName                 string `json:"chainFullName"`
		ChainShortName                string `json:"chainShortName"`
		Address                       string `json:"address"`
		ContractAddress               string `json:"contractAddress"`
		IsProducerAddress             bool   `json:"isProducerAddress"`
		Balance                       string `json:"balance"`
		BalanceSymbol                 string `json:"balanceSymbol"`
		TransactionCount              string `json:"transactionCount"`
		Verifying                     string `json:"verifying"`
		SendAmount                    string `json:"sendAmount"`
		ReceiveAmount                 string `json:"receiveAmount"`
		TokenAmount                   string `json:"tokenAmount"`
		TotalTokenValue               string `json:"totalTokenValue"`
		CreateContractAddress         string `json:"createContractAddress"`
		CreateContractTransactionHash string `json:"createContractTransactionHash"`
		FirstTransactionTime          string `json:"firstTransactionTime"`
		LastTransactionTime           string `json:"lastTransactionTime"`
		Token                         string `json:"token"`
		Bandwidth                     string `json:"bandwidth"`
		Energy                        string `json:"energy"`
		VotingRights                  string `json:"votingRights"`
		UnclaimedVotingRewards        string `json:"unclaimedVotingRewards"`
		IsAaAddress                   bool   `json:"isAaAddress"`
	} `json:"data"`
}

type OklinkUTXOResp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		Page      string       `json:"page"`
		Limit     string       `json:"limit"`
		TotalPage string       `json:"totalPage"`
		UtxoList  []OklinkUTXO `json:"utxoList"`
	} `json:"data"`
}

type OklinkUTXO struct {
	Txid          string `json:"txid"`
	Height        string `json:"height"`
	BlockTime     string `json:"blockTime"`
	Address       string `json:"address"`
	UnspentAmount string `json:"unspentAmount"`
	Index         string `json:"index"`
}
