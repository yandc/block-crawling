package types

type PolkadotBlockInfo struct {
	Error  string `json:"error"`
	Header struct {
		ParentHash     string `json:"parentHash"`
		Number         int    `json:"number"`
		StateRoot      string `json:"stateRoot"`
		ExtrinsicsRoot string `json:"extrinsicsRoot"`
		Digest         struct {
			Logs []struct {
				PreRuntime []string `json:"preRuntime,omitempty"`
				Seal       []string `json:"seal,omitempty"`
			} `json:"logs"`
		} `json:"digest"`
	} `json:"header"`
	Extrinsics                []ExtrinsicInfo `json:"extrinsics"`
	Number                    int             `json:"number"`
	Hash                      string          `json:"hash"`
	BlockTS                   int             `json:"blockTS"`
	Finalized                 bool            `json:"finalized"`
	Author                    string          `json:"author"`
	XcmMeta                   []interface{}   `json:"xcmMeta"`
	RelayBN                   int             `json:"relayBN"`
	AuthorAddress             string          `json:"authorAddress"`
	SpecVersion               int             `json:"specVersion"`
	NumXCMTransfersIn         int             `json:"numXCMTransfersIn"`
	NumXCMMessagesIn          int             `json:"numXCMMessagesIn"`
	NumXCMTransfersOut        int             `json:"numXCMTransfersOut"`
	NumXCMMessagesOut         int             `json:"numXCMMessagesOut"`
	ValXCMTransferIncomingUSD int             `json:"valXCMTransferIncomingUSD"`
	ValXCMTransferOutgoingUSD int             `json:"valXCMTransferOutgoingUSD"`
}

type NodeBlockInfo struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Block struct {
			Header struct {
				ParentHash     string `json:"parentHash"`
				Number         string `json:"number"`
				StateRoot      string `json:"stateRoot"`
				ExtrinsicsRoot string `json:"extrinsicsRoot"`
				Digest         struct {
					Logs []string `json:"logs"`
				} `json:"digest"`
			} `json:"header"`
			Extrinsics []string `json:"extrinsics"`
		} `json:"block"`
		Justifications interface{} `json:"justifications"`
	} `json:"result"`
	Id string `json:"id"`
}

type PolkadotTxInfo struct {
	Error         string `json:"err"`
	ChainID       int    `json:"chainID"`
	Id            string `json:"id"`
	ChainName     string `json:"chainName"`
	ExtrinsicHash string `json:"extrinsicHash"`
	ExtrinsicID   string `json:"extrinsicID"`
	BlockNumber   int    `json:"blockNumber"`
	Ts            int    `json:"ts"`
	BlockHash     string `json:"blockHash"`
	Signer        string `json:"signer"`
	FromAddress   string `json:"fromAddress"`
	Signature     struct {
		Ed25519 string `json:"ed25519"`
	} `json:"signature"`
	Lifetime struct {
		IsImmortal int `json:"isImmortal"`
		Birth      int `json:"birth"`
		Death      int `json:"death"`
	} `json:"lifetime"`
	Nonce     int     `json:"nonce"`
	Tip       int     `json:"tip"`
	Fee       float64 `json:"fee"`
	Weight    int     `json:"weight"`
	Transfers []struct {
		EventID     string  `json:"eventID"`
		Section     string  `json:"section"`
		Method      string  `json:"method"`
		From        string  `json:"from"`
		To          string  `json:"to"`
		FromAddress string  `json:"fromAddress"`
		ToAddress   string  `json:"toAddress"`
		Amount      float64 `json:"amount"`
		RawAmount   int64   `json:"rawAmount"`
		Asset       string  `json:"asset"`
		PriceUSD    float64 `json:"priceUSD"`
		AmountUSD   float64 `json:"amountUSD"`
		RawAsset    string  `json:"rawAsset"`
		Symbol      string  `json:"symbol"`
		Decimals    int     `json:"decimals"`
	} `json:"transfers"`
	ChainSymbol     string  `json:"chainSymbol"`
	PriceUSD        float64 `json:"priceUSD"`
	FeeUSD          float64 `json:"feeUSD"`
	TipUSD          int     `json:"tipUSD"`
	PriceUSDCurrent float64 `json:"priceUSDCurrent"`
	Result          int     `json:"result"`
	Status          string  `json:"status"`
	Section         string  `json:"section"`
	Method          string  `json:"method"`
	Params          struct {
		Dest struct {
			Id        string `json:"id"`
			IdAddress string `json:"idAddress"`
		} `json:"dest"`
		Value int64 `json:"value"`
	} `json:"params"`
	Events []struct {
		EventID  string        `json:"eventID"`
		Docs     string        `json:"docs"`
		Section  string        `json:"section"`
		Method   string        `json:"method"`
		Data     []interface{} `json:"data"`
		DataType []struct {
			TypeDef string `json:"typeDef"`
			Name    string `json:"name"`
		} `json:"dataType"`
		DecodedData []struct {
			Data            interface{} `json:"data"`
			TypeDef         string      `json:"typeDef"`
			Name            string      `json:"name"`
			Address         string      `json:"address,omitempty"`
			Symbol          string      `json:"symbol,omitempty"`
			DataRaw         float64     `json:"dataRaw,omitempty"`
			DataUSD         float64     `json:"dataUSD,omitempty"`
			PriceUSD        float64     `json:"priceUSD,omitempty"`
			PriceUSDCurrent float64     `json:"priceUSDCurrent,omitempty"`
			AddressNickname string      `json:"address_nickname,omitempty"`
		} `json:"decodedData"`
	} `json:"events"`
	SpecVersion int `json:"specVersion"`
}

type PolkadotAccountInfo struct {
	Error      string `json:"error"`
	AssetChain string `json:"assetChain"`
	AssetInfo  struct {
		AssetType             string      `json:"assetType"`
		AssetName             string      `json:"assetName"`
		NumHolders            int         `json:"numHolders"`
		TotalSupply           string      `json:"totalSupply"`
		Asset                 string      `json:"asset"`
		Symbol                string      `json:"symbol"`
		Decimals              int         `json:"decimals"`
		ChainID               int         `json:"chainID"`
		CreateTS              interface{} `json:"createTS"`
		Id                    string      `json:"id"`
		ChainName             string      `json:"chainName"`
		AssetChain            string      `json:"assetChain"`
		IsUSD                 int         `json:"isUSD"`
		PriceUSD              float64     `json:"priceUSD"`
		PriceUSDPercentChange float64     `json:"priceUSDPercentChange"`
		PriceUSDpaths         bool        `json:"priceUSDpaths"`
		NativeAssetChain      interface{} `json:"nativeAssetChain"`
		IsXCAsset             bool        `json:"isXCAsset"`
		XcmInteriorKey        string      `json:"xcmInteriorKey"`
		XcContractAddress     interface{} `json:"xcContractAddress"`
		RelayChain            string      `json:"relayChain"`
	} `json:"assetInfo"`
	State struct {
		Free            float64 `json:"free"`
		Reserved        int     `json:"reserved"`
		MiscFrozen      int     `json:"miscFrozen"`
		FeeFrozen       int     `json:"feeFrozen"`
		Frozen          int     `json:"frozen"`
		Ts              int     `json:"ts"`
		Bn              int     `json:"bn"`
		Source          string  `json:"source"`
		GenTS           int     `json:"genTS"`
		FreeUSD         float64 `json:"freeUSD"`
		Rate            float64 `json:"rate"`
		Transferable    float64 `json:"transferable"`
		TransferableUSD float64 `json:"transferableUSD"`
		Balance         float64 `json:"balance"`
		BalanceUSD      float64 `json:"balanceUSD"`
	} `json:"state"`
}

type ExtrinsicInfo struct {
	ChainID       int    `json:"chainID"`
	Id            string `json:"id"`
	ChainName     string `json:"chainName"`
	ExtrinsicHash string `json:"extrinsicHash"`
	ExtrinsicID   string `json:"extrinsicID"`
	BlockNumber   int    `json:"blockNumber"`
	Ts            int    `json:"ts"`
	BlockHash     string `json:"blockHash"`
	Lifetime      struct {
		IsImmortal int `json:"isImmortal"`
		Birth      int `json:"birth,omitempty"`
		Death      int `json:"death,omitempty"`
	} `json:"lifetime"`
	Nonce     int     `json:"nonce"`
	Fee       float64 `json:"fee"`
	Weight    int64   `json:"weight"`
	Transfers []struct {
		EventID     string  `json:"eventID"`
		Section     string  `json:"section"`
		Method      string  `json:"method"`
		From        string  `json:"from"`
		To          string  `json:"to"`
		FromAddress string  `json:"fromAddress"`
		ToAddress   string  `json:"toAddress"`
		Amount      float64 `json:"amount"`
		RawAmount   int64   `json:"rawAmount"`
		Asset       string  `json:"asset"`
		PriceUSD    float64 `json:"priceUSD"`
		AmountUSD   float64 `json:"amountUSD"`
		RawAsset    string  `json:"rawAsset"`
		Symbol      string  `json:"symbol"`
		Decimals    int     `json:"decimals"`
	} `json:"transfers"`
	Result  int    `json:"result"`
	Err     string `json:"err"`
	Section string `json:"section"`
	Method  string `json:"method"`
	Events  []struct {
		EventID  string        `json:"eventID"`
		Docs     string        `json:"docs"`
		Section  string        `json:"section"`
		Method   string        `json:"method"`
		Data     []interface{} `json:"data"`
		DataType []struct {
			TypeDef string `json:"typeDef"`
			Name    string `json:"name"`
		} `json:"dataType"`
	} `json:"events"`
	Signer      string `json:"signer,omitempty"`
	FromAddress string `json:"fromAddress,omitempty"`
	Signature   struct {
		Ed25519 string `json:"ed25519"`
	} `json:"signature,omitempty"`
	ChainSymbol string `json:"chainSymbol,omitempty"`
}
