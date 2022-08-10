package ethereum

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetPreferentialUrl(t *testing.T) {
	UrlList := []string{"https://rpc.ankr.com/polygon_mumbai", "https://matic-testnet-archive-rpc.bwarelabs.com"}
	url_list := GetPreferentialUrl(UrlList, "PolygonTEST")
	fmt.Printf("%v\n", url_list)
	assert.Equal(t, len(UrlList), len(url_list))
}
