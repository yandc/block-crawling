package data

import "github.com/go-redis/redis"

type Bundle struct {
	ATM                    AtomTransactionRecordRepo
	BTC                    BtcTransactionRecordRepo
	DOT                    DotTransactionRecordRepo
	EVM                    EvmTransactionRecordRepo
	STC                    StcTransactionRecordRepo
	TRX                    TrxTransactionRecordRepo
	APT                    AptTransactionRecordRepo
	SUI                    SuiTransactionRecordRepo
	SOL                    SolTransactionRecordRepo
	CKB                    CkbTransactionRecordRepo
	CSPR                   CsprTransactionRecordRepo
	KAS                    KasTransactionRecordRepo
	TON                    TonTransactionRecordRepo
	UserNFT                UserNftAssetRepo
	NFTHistory             NftRecordHistoryRepo
	Stat                   TransactionStatisticRepo
	Count                  TransactionCountRepo
	UTXO                   UtxoUnspentRecordRepo
	NervosCell             NervosCellRecordRepo
	UserRecord             UserRecordRepo
	UserAsset              UserAssetRepo
	UserAssetHistory       UserAssetHistoryRepo
	UserWalletAssetHistory UserWalletAssetHistoryRepo
	ChainTypeAsset         ChainTypeAssetRepo
	ChainTypeAddressAmount ChainTypeAddressAmountRepo
	DAPP                   DappApproveRecordRepo
	Redis                  *redis.Client
	History                UserSendRawHistoryRepo
	Market                 MarketCoinHistoryRepo
	Swap                   SwapContractRepo
	DeFi                   DeFiAssetRepo
}

// NewDataBundle Bundle repositories to let them initialize.
func NewBundle(
	atm AtomTransactionRecordRepo,
	btc BtcTransactionRecordRepo,
	dot DotTransactionRecordRepo,
	evm EvmTransactionRecordRepo,
	stc StcTransactionRecordRepo,
	trx TrxTransactionRecordRepo,
	apt AptTransactionRecordRepo,
	sui SuiTransactionRecordRepo,
	sol SolTransactionRecordRepo,
	ckb CkbTransactionRecordRepo,
	cspr CsprTransactionRecordRepo,
	kas KasTransactionRecordRepo,
	ton TonTransactionRecordRepo,
	userNFT UserNftAssetRepo,
	nftHistory NftRecordHistoryRepo,
	stat TransactionStatisticRepo,
	count TransactionCountRepo,
	nervosCell NervosCellRecordRepo,
	utxo UtxoUnspentRecordRepo,
	userRecord UserRecordRepo,
	userAsset UserAssetRepo,
	userAssetHistory UserAssetHistoryRepo,
	chainTypeAsset ChainTypeAssetRepo,
	chainTypeAddressAmount ChainTypeAddressAmountRepo,
	dapp DappApproveRecordRepo,
	redisClient *redis.Client,
	history UserSendRawHistoryRepo,
	market MarketCoinHistoryRepo,
	swap SwapContractRepo,
	bfs BFCStationRepo,
	userWalletAssetHistoryRepo UserWalletAssetHistoryRepo,
	defi DeFiAssetRepo,
) *Bundle {
	return &Bundle{
		ATM:  atm,
		BTC:  btc,
		DOT:  dot,
		EVM:  evm,
		STC:  stc,
		TRX:  trx,
		APT:  apt,
		SUI:  sui,
		SOL:  sol,
		CKB:  ckb,
		CSPR: cspr,
		KAS:  kas,
		TON:  ton,

		UserNFT:                userNFT,
		NFTHistory:             nftHistory,
		Stat:                   stat,
		Count:                  count,
		NervosCell:             nervosCell,
		UTXO:                   utxo,
		UserRecord:             userRecord,
		UserAsset:              userAsset,
		UserAssetHistory:       userAssetHistory,
		ChainTypeAsset:         chainTypeAsset,
		ChainTypeAddressAmount: chainTypeAddressAmount,
		DAPP:                   dapp,
		Redis:                  redisClient,
		History:                history,
		Market:                 market,
		Swap:                   swap,
		UserWalletAssetHistory: userWalletAssetHistoryRepo,
		DeFi:                   defi,
	}
}
