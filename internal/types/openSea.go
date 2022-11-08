package types

import "time"

type OpenSeaEvents struct {
	Next        interface{} `json:"next"`
	Previous    interface{} `json:"previous"`
	AssetEvents []struct {
		Asset struct {
			Id                   int         `json:"id"`
			NumSales             int         `json:"num_sales"`
			BackgroundColor      interface{} `json:"background_color"`
			ImageUrl             string      `json:"image_url"`
			ImagePreviewUrl      string      `json:"image_preview_url"`
			ImageThumbnailUrl    string      `json:"image_thumbnail_url"`
			ImageOriginalUrl     string      `json:"image_original_url"`
			AnimationUrl         interface{} `json:"animation_url"`
			AnimationOriginalUrl interface{} `json:"animation_original_url"`
			Name                 string      `json:"name"`
			Description          string      `json:"description"`
			ExternalLink         interface{} `json:"external_link"`
			AssetContract        struct {
				Address                     string      `json:"address"`
				AssetContractType           string      `json:"asset_contract_type"`
				CreatedDate                 string      `json:"created_date"`
				Name                        string      `json:"name"`
				NftVersion                  interface{} `json:"nft_version"`
				OpenseaVersion              interface{} `json:"opensea_version"`
				Owner                       int         `json:"owner"`
				SchemaName                  string      `json:"schema_name"`
				Symbol                      string      `json:"symbol"`
				TotalSupply                 string      `json:"total_supply"`
				Description                 string      `json:"description"`
				ExternalLink                string      `json:"external_link"`
				ImageUrl                    string      `json:"image_url"`
				DefaultToFiat               bool        `json:"default_to_fiat"`
				DevBuyerFeeBasisPoints      int         `json:"dev_buyer_fee_basis_points"`
				DevSellerFeeBasisPoints     int         `json:"dev_seller_fee_basis_points"`
				OnlyProxiedTransfers        bool        `json:"only_proxied_transfers"`
				OpenseaBuyerFeeBasisPoints  int         `json:"opensea_buyer_fee_basis_points"`
				OpenseaSellerFeeBasisPoints int         `json:"opensea_seller_fee_basis_points"`
				BuyerFeeBasisPoints         int         `json:"buyer_fee_basis_points"`
				SellerFeeBasisPoints        int         `json:"seller_fee_basis_points"`
				PayoutAddress               string      `json:"payout_address"`
			} `json:"asset_contract"`
			Permalink  string `json:"permalink"`
			Collection struct {
				BannerImageUrl          string      `json:"banner_image_url"`
				ChatUrl                 interface{} `json:"chat_url"`
				CreatedDate             time.Time   `json:"created_date"`
				DefaultToFiat           bool        `json:"default_to_fiat"`
				Description             string      `json:"description"`
				DevBuyerFeeBasisPoints  string      `json:"dev_buyer_fee_basis_points"`
				DevSellerFeeBasisPoints string      `json:"dev_seller_fee_basis_points"`
				DiscordUrl              interface{} `json:"discord_url"`
				DisplayData             struct {
					CardDisplayStyle string `json:"card_display_style"`
				} `json:"display_data"`
				ExternalUrl                 string      `json:"external_url"`
				Featured                    bool        `json:"featured"`
				FeaturedImageUrl            string      `json:"featured_image_url"`
				Hidden                      bool        `json:"hidden"`
				SafelistRequestStatus       string      `json:"safelist_request_status"`
				ImageUrl                    string      `json:"image_url"`
				IsSubjectToWhitelist        bool        `json:"is_subject_to_whitelist"`
				LargeImageUrl               string      `json:"large_image_url"`
				MediumUsername              interface{} `json:"medium_username"`
				Name                        string      `json:"name"`
				OnlyProxiedTransfers        bool        `json:"only_proxied_transfers"`
				OpenseaBuyerFeeBasisPoints  string      `json:"opensea_buyer_fee_basis_points"`
				OpenseaSellerFeeBasisPoints string      `json:"opensea_seller_fee_basis_points"`
				PayoutAddress               string      `json:"payout_address"`
				RequireEmail                bool        `json:"require_email"`
				ShortDescription            interface{} `json:"short_description"`
				Slug                        string      `json:"slug"`
				TelegramUrl                 interface{} `json:"telegram_url"`
				TwitterUsername             string      `json:"twitter_username"`
				InstagramUsername           interface{} `json:"instagram_username"`
				WikiUrl                     interface{} `json:"wiki_url"`
				IsNsfw                      bool        `json:"is_nsfw"`
				Fees                        struct {
					SellerFees struct {
						X02Bb8Cfafe1666742A42A348Cad8A4A5Df838055 int `json:"0x02bb8cfafe1666742a42a348cad8a4a5df838055"`
						X01Eef68380E0854B21B0Cc36F4Df338Ed80Bbc99 int `json:"0x01eef68380e0854b21b0cc36f4df338ed80bbc99"`
					} `json:"seller_fees"`
					OpenseaFees struct {
						X0000A26B00C1F0Df003000390027140000Faa719 int `json:"0x0000a26b00c1f0df003000390027140000faa719"`
					} `json:"opensea_fees"`
				} `json:"fees"`
				IsRarityEnabled bool `json:"is_rarity_enabled"`
			} `json:"collection"`
			Decimals      interface{} `json:"decimals"`
			TokenMetadata string      `json:"token_metadata"`
			IsNsfw        bool        `json:"is_nsfw"`
			Owner         struct {
				User          interface{} `json:"user"`
				ProfileImgUrl string      `json:"profile_img_url"`
				Address       string      `json:"address"`
				Config        string      `json:"config"`
			} `json:"owner"`
			TokenId string `json:"token_id"`
		} `json:"asset"`
		AssetBundle    interface{} `json:"asset_bundle"`
		EventType      string      `json:"event_type"`
		EventTimestamp string      `json:"event_timestamp"`
		AuctionType    interface{} `json:"auction_type"`
		TotalPrice     *string     `json:"total_price"`
		PaymentToken   *struct {
			Symbol   string `json:"symbol"`
			Address  string `json:"address"`
			ImageUrl string `json:"image_url"`
			Name     string `json:"name"`
			Decimals int    `json:"decimals"`
			EthPrice string `json:"eth_price"`
			UsdPrice string `json:"usd_price"`
		} `json:"payment_token"`
		Transaction *struct {
			BlockHash   string `json:"block_hash"`
			BlockNumber string `json:"block_number"`
			FromAccount struct {
				User *struct {
					Username string `json:"username"`
				} `json:"user"`
				ProfileImgUrl string `json:"profile_img_url"`
				Address       string `json:"address"`
				Config        string `json:"config"`
			} `json:"from_account"`
			Id        int    `json:"id"`
			Timestamp string `json:"timestamp"`
			ToAccount struct {
				User          interface{} `json:"user"`
				ProfileImgUrl string      `json:"profile_img_url"`
				Address       string      `json:"address"`
				Config        string      `json:"config"`
			} `json:"to_account"`
			TransactionHash  string `json:"transaction_hash"`
			TransactionIndex string `json:"transaction_index"`
		} `json:"transaction"`
		CreatedDate             string      `json:"created_date"`
		Quantity                string      `json:"quantity"`
		ApprovedAccount         interface{} `json:"approved_account"`
		BidAmount               interface{} `json:"bid_amount"`
		CollectionSlug          string      `json:"collection_slug"`
		ContractAddress         string      `json:"contract_address"`
		CustomEventName         interface{} `json:"custom_event_name"`
		DevFeePaymentEvent      interface{} `json:"dev_fee_payment_event"`
		DevSellerFeeBasisPoints *int        `json:"dev_seller_fee_basis_points"`
		Duration                *string     `json:"duration"`
		EndingPrice             *string     `json:"ending_price"`
		FromAccount             *struct {
			User *struct {
				Username *string `json:"username"`
			} `json:"user"`
			ProfileImgUrl string `json:"profile_img_url"`
			Address       string `json:"address"`
			Config        string `json:"config"`
		} `json:"from_account"`
		Id           int64       `json:"id"`
		IsPrivate    *bool       `json:"is_private"`
		OwnerAccount interface{} `json:"owner_account"`
		Seller       *struct {
			User struct {
				Username *string `json:"username"`
			} `json:"user"`
			ProfileImgUrl string `json:"profile_img_url"`
			Address       string `json:"address"`
			Config        string `json:"config"`
		} `json:"seller"`
		StartingPrice *string `json:"starting_price"`
		ToAccount     *struct {
			User *struct {
				Username *string `json:"username"`
			} `json:"user"`
			ProfileImgUrl string `json:"profile_img_url"`
			Address       string `json:"address"`
			Config        string `json:"config"`
		} `json:"to_account"`
		WinnerAccount *struct {
			User struct {
				Username string `json:"username"`
			} `json:"user"`
			ProfileImgUrl string `json:"profile_img_url"`
			Address       string `json:"address"`
			Config        string `json:"config"`
		} `json:"winner_account"`
		ListingTime *string `json:"listing_time"`
	} `json:"asset_events"`
}