package kanban

type Bundle struct {
	EVM      EvmTransactionRecordRepo
	Wallet   WalletRepo
	Trending TrendingRepo
}

func NewBundle(evm EvmTransactionRecordRepo, wallet WalletRepo, trending TrendingRepo) *Bundle {
	return &Bundle{
		EVM:      evm,
		Wallet:   wallet,
		Trending: trending,
	}
}
