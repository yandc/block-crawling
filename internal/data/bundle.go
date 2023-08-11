package data

import "github.com/go-redis/redis"

type Bundle struct {
	ATM              AtomTransactionRecordRepo
	BTC              BtcTransactionRecordRepo
	DOT              DotTransactionRecordRepo
	EVM              EvmTransactionRecordRepo
	STC              StcTransactionRecordRepo
	TRX              TrxTransactionRecordRepo
	APT              AptTransactionRecordRepo
	SUI              SuiTransactionRecordRepo
	SOL              SolTransactionRecordRepo
	CKB              CkbTransactionRecordRepo
	CSPR             CsprTransactionRecordRepo
	KAS              KasTransactionRecordRepo
	UserNFT          UserNftAssetRepo
	NFTHistory       NftRecordHistoryRepo
	Stat             TransactionStatisticRepo
	UTXO             UtxoUnspentRecordRepo
	NervosCell       NervosCellRecordRepo
	UserRecord       UserRecordRepo
	UserAsset        UserAssetRepo
	UserAssetHistory UserAssetHistoryRepo
	DAPP             DappApproveRecordRepo
	Redis            *redis.Client
	History          UserSendRawHistoryRepo
	Market           MarketCoinHistoryRepo
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
	userNFT UserNftAssetRepo,
	nftHistory NftRecordHistoryRepo,
	stat TransactionStatisticRepo,
	nervosCell NervosCellRecordRepo,
	utxo UtxoUnspentRecordRepo,
	userRecord UserRecordRepo,
	userAsset UserAssetRepo,
	userAssetHistory UserAssetHistoryRepo,
	dapp DappApproveRecordRepo,
	redisClient *redis.Client,
	history UserSendRawHistoryRepo,
	market MarketCoinHistoryRepo,
) *Bundle {
	return &Bundle{
		ATM:              atm,
		BTC:              btc,
		DOT:              dot,
		EVM:              evm,
		STC:              stc,
		TRX:              trx,
		APT:              apt,
		SUI:              sui,
		SOL:              sol,
		CKB:              ckb,
		CSPR:             cspr,
		KAS:              kas,
		UserNFT:          userNFT,
		NFTHistory:       nftHistory,
		Stat:             stat,
		NervosCell:       nervosCell,
		UTXO:             utxo,
		UserRecord:       userRecord,
		UserAsset:        userAsset,
		UserAssetHistory: userAssetHistory,
		DAPP:             dapp,
		Redis:            redisClient,
		History:          history,
		Market:           market,
	}
}
