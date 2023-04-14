//go:build !linux
// +build !linux

package ptesting

import (
	v1 "block-crawling/internal/client"
	"block-crawling/internal/types"

	"github.com/agiledragon/gomonkey/v2"
)

// StubBackgrounds use gomonkey to patch background tasks during index block.
func StubBackgrounds() []*gomonkey.Patches {
	return nil
}

// StubNodeProxyFuncs use gomonkey to patch node-proxy.
func StubNodeProxyFuncs(chainName string, tokens []types.TokenInfo, nfts []*v1.GetNftReply_NftInfoResp, prices []TokenPrice) []*gomonkey.Patches {
	return nil
}

// StubTokenPushQueue use gomoney to hijack queue name of token pushing.
// To avoid different tests run in parallel cause conflicts.
func StubTokenPushQueue(topic string) func() {
	return func() {
	}
}
