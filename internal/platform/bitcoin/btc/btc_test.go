package btc

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	nodeURLCypher  = "https://api.blockcypher.com/v1/btc/main"
	nodeURLDaemon  = "https://Bearer:bd1bd2JBVNTa8XTPQOI7ytO8mK5AZpSpQ14sOwZn2CqD0Cd@ubiquity.api.blockdaemon.com/v1/bitcoin/mainnet/"
	nodeURLStream  = "https://blockstream.info"
	nodeURLScan    = "https://btcscan.org"
	nodeURLMempool = "https://mempool.space"
)

const baseNodeURL = nodeURLCypher

var restNodeURLs = []string{
	nodeURLDaemon,
	nodeURLStream,
	nodeURLScan,
	nodeURLMempool,
}

func TestMain(m *testing.M) {
	log.BootstrapLogger(&conf.Logger{
		DEBUG:    false,
		FileName: "stderr",
		Level:    "debug",
	})
	os.Exit(m.Run())
}

func TestBlockNumber(t *testing.T) {
	c := NewClient(baseNodeURL)
	base, err := GetBlockNumber(&c)
	assert.NoError(t, err)
	for _, nodeURL := range restNodeURLs {
		c = NewClient(nodeURL)
		n, err := GetBlockNumber(&c)
		assert.NoError(t, err, nodeURL)
		assert.Equal(t, base, n, nodeURL)
	}
}

func TestGetBalance(t *testing.T) {
	address := "1KnUvBqybbSFCpG897SgQqcZbDmboBnEmq"
	c := NewClient(baseNodeURL)
	base, err := GetBalance(address, &c)
	assert.NoError(t, err)
	for _, nodeURL := range restNodeURLs {
		c = NewClient(nodeURL)
		n, err := GetBalance(address, &c)
		assert.NoError(t, err, nodeURL)
		assert.Equal(t, base, n, nodeURL)
	}
}

func TestGetTxByHash(t *testing.T) {
	txHash := "0ad1d555779d4b16d77034d2254f0e30d839e1188709e6deb2dfe83f6b40479a"
	c := NewClient(baseNodeURL)
	base, err := GetTransactionByHash(txHash, &c)
	assert.NoError(t, err)
	for _, nodeURL := range restNodeURLs {
		c = NewClient(nodeURL)
		n, err := GetTransactionByHash(txHash, &c)
		assert.NoError(t, err, nodeURL)
		assert.Equal(t, base.BlockHash, n.BlockHash, nodeURL)
		assert.Equal(t, base.BlockHeight, n.BlockHeight, nodeURL)
		assert.Equal(t, txHash, n.Hash, nodeURL)
		assert.Equal(t, base.Fees.String(), n.Fees.String(), nodeURL)
		assert.Equal(t, base.Confirmed.Unix(), n.Confirmed.Unix(), nodeURL)
		assert.Equal(t, base.VinSize, n.VinSize, nodeURL)
		assert.Equal(t, base.VoutSize, n.VoutSize, nodeURL)
		assert.Equal(t, base.Inputs[0].OutputValue, n.Inputs[0].OutputValue, nodeURL)
		assert.Equal(t, base.Inputs[0].Addresses, n.Inputs[0].Addresses, nodeURL)
		assert.Equal(t, base.Outputs[0].Value, n.Outputs[0].Value, nodeURL)
		assert.Equal(t, base.Outputs[0].Addresses, n.Outputs[0].Addresses, nodeURL)
	}
}
