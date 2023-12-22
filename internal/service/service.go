package service

import (
	"block-crawling/internal/platform"

	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(platform.NewPlatform, platform.NewInnerNodeList, NewTransactionService, platform.NewCustomConfigProvider, NewBFStationService, NewUserWalletAssetService)
