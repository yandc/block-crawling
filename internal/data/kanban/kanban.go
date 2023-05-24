package kanban

import (
	"github.com/google/wire"
)

var ConnProviderSet = wire.NewSet(NewGormDB)
var ProviderSet = wire.NewSet(NewEvmTransactionRecordRepo, NewBundle, NewWalletRepo, NewTrendingRepo)
