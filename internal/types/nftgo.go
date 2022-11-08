package types

type NFtgoEvents struct {
	Transactions []struct {
		Event      string `json:"event"`
		Blockchain string `json:"blockchain"`
		TxHash     string `json:"tx_hash"`
		Sender     struct {
			Address string      `json:"address"`
			Ens     interface{} `json:"ens"`
		} `json:"sender"`
		Nft struct {
			Blockchain            string      `json:"blockchain"`
			CollectionName        string      `json:"collection_name"`
			CollectionSlug        string      `json:"collection_slug"`
			CollectionOpenseaSlug string      `json:"collection_opensea_slug"`
			ContractAddress       string      `json:"contract_address"`
			TokenId               string      `json:"token_id"`
			Name                  string      `json:"name"`
			Description           string      `json:"description"`
			Image                 string      `json:"image"`
			AnimationUrl          interface{} `json:"animation_url"`
			OwnerAddresses        []string    `json:"owner_addresses"`
			Traits                []struct {
				Type       string      `json:"type"`
				Value      string      `json:"value"`
				Percentage interface{} `json:"percentage"`
			} `json:"traits"`
			Rarity interface{} `json:"rarity"`
		} `json:"nft"`
		Quantity int `json:"quantity"`
		GasFee   struct {
			Quantity   float64 `json:"quantity"`
			Value      float64 `json:"value"`
			CryptoUnit string  `json:"crypto_unit"`
			Usd        float64 `json:"usd"`
		} `json:"gas_fee"`
		Price *struct {
			Quantity   float64 `json:"quantity"`
			Value      float64 `json:"value"`
			CryptoUnit string  `json:"crypto_unit"`
			Usd        float64 `json:"usd"`
		} `json:"price"`
		Time     int64 `json:"time"`
		Receiver struct {
			Address string      `json:"address"`
			Ens     interface{} `json:"ens"`
		} `json:"receiver"`
	} `json:"transactions"`
}