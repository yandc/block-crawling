package types

import "math/big"

type TronBalance struct {
	Balance int64       `json:"balance"`
	Error   interface{} `json:"error"`
}

type TronAccountInfo struct {
	Address string      `json:"address"`
	Balance int         `json:"balance"`
	Type    string      `json:"type"`
	Error   interface{} `json:"error"`
}

type BalanceReq struct {
	Address string `json:"address"`
	Visible bool   `json:"visible"`
}

type NowBlock struct {
	BlockHeader struct {
		RawData struct {
			Number         int    `json:"number"`
			TxTrieRoot     string `json:"txTrieRoot"`
			WitnessAddress string `json:"witness_address"`
			ParentHash     string `json:"parent_hash"`
			Version        int    `json:"version"`
			Timestamp      uint64 `json:"timestamp"`
		} `json:"raw_data"`
	} `json:"block_header"`
	Error interface{} `json:"error"`
}

type BlockReq struct {
	Num     int  `json:"num"`
	Visible bool `json:"visible"`
}

type BlockResponse struct {
	BlockID     string `json:"blockID"`
	BlockHeader struct {
		RawData struct {
			Number         int    `json:"number"`
			TxTrieRoot     string `json:"txTrieRoot"`
			WitnessAddress string `json:"witness_address"`
			ParentHash     string `json:"parentHash"`
			Version        int    `json:"version"`
			Timestamp      int64  `json:"timestamp"`
		} `json:"raw_data"`
		WitnessSignature string `json:"witness_signature"`
	} `json:"block_header"`
	Transactions []BlockTx   `json:"transactions"`
	Error        interface{} `json:"error"`
}
type BlockTx struct {
	Ret []struct {
		ContractRet string `json:"contractRet"`
	} `json:"ret"`
	Signature  []string `json:"signature"`
	TxID       string   `json:"txID"`
	RawDataHex string   `json:"raw_data_hex"`
	RawData    struct {
		Contract []struct {
			Parameter struct {
				Value struct {
					Amount          int    `json:"amount"`
					ToAddress       string `json:"to_address"`
					AssetName       string `json:"asset_name"`
					Data            string `json:"data"`
					Resource        string `json:"resource"`
					FrozenBalance   int    `json:"frozen_balance"`
					UnfreezeBalance int    `json:"unfreeze_balance"`
					OwnerAddress    string `json:"owner_address"`
					ContractAddress string `json:"contract_address"`
				} `json:"value"`
				TypeURL string `json:"type_url"`
			} `json:"parameter"`
			Type string `json:"type"`
		} `json:"contract"`
		RefBlockBytes string `json:"ref_block_bytes"`
		RefBlockHash  string `json:"ref_block_hash"`
		Expiration    int64  `json:"expiration"`
		FeeLimit      int    `json:"fee_limit"`
		Timestamp     int64  `json:"timestamp"`
	} `json:"raw_data"`
}

type TronTxReq struct {
	Value   string `json:"value"`
	Visible bool   `json:"visible"`
}

type TronTxInfoResponse struct {
	ID              string   `json:"id"`
	Fee             int      `json:"fee"`
	BlockNumber     int      `json:"blockNumber"`
	BlockTimeStamp  int64    `json:"blockTimeStamp"`
	ContractResult  []string `json:"contractResult"`
	ContractAddress string   `json:"contract_address"`
	Receipt         struct {
		EnergyUsage      int    `json:"energy_usage"`
		EnergyUsageTotal int    `json:"energy_usage_total"`
		NetUsage         int    `json:"net_usage"`
		NetFee           int    `json:"net_fee"`
		Result           string `json:"result"`
	} `json:"receipt"`
	Log []struct {
		Address string   `json:"address"`
		Topics  []string `json:"topics"`
		Data    string   `json:"data"`
	} `json:"log"`
	Error interface{} `json:"error"`
}

type TronTokenInfo struct {
	Data []struct {
		TokenInfo struct {
			TokenID      string `json:"tokenId"`
			TokenAbbr    string `json:"tokenAbbr"`
			TokenName    string `json:"tokenName"`
			TokenDecimal int64  `json:"tokenDecimal"`
			TokenCanShow int    `json:"tokenCanShow"`
			TokenType    string `json:"tokenType"`
		} `json:"tokenInfo"`
	} `json:"data"`
}

type TronTokenBalanceRes struct {
	ConstantResult []string    `json:"constant_result"`
	Error          interface{} `json:"error"`
}

type TronTokenBalanceReq struct {
	OwnerAddress     string `json:"owner_address"`
	ContractAddress  string `json:"contract_address"`
	FunctionSelector string `json:"function_selector"`
	Parameter        string `json:"parameter"`
	Visible          bool   `json:"visible"`
}
type TronContractInfo struct {
	Ret []struct {
		ContractRet string `json:"contractRet"`
	} `json:"ret"`
	Signature []string `json:"signature"`
	TxID      string   `json:"txID"`
	RawData   struct {
		Contract []struct {
			Parameter struct {
				Value struct {
					Data            string   `json:"data"`
					OwnerAddress    string   `json:"owner_address"`
					ContractAddress string   `json:"contract_address"`
					CallValue       *big.Int `json:"call_value"`
				} `json:"value"`
				TypeUrl string `json:"type_url"`
			} `json:"parameter"`
			Type string `json:"type"`
		} `json:"contract"`
		RefBlockBytes string `json:"ref_block_bytes"`
		RefBlockHash  string `json:"ref_block_hash"`
		Expiration    int64  `json:"expiration"`
		FeeLimit      int    `json:"fee_limit"`
		Timestamp     int64  `json:"timestamp"`
	} `json:"raw_data"`
	RawDataHex string `json:"raw_data_hex"`
}
