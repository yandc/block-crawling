package starcoin

import (
	"testing"
	"time"
)

func TestOnNewTx(t *testing.T) {
	client := NewClient("https://main-seed.starcoin.org", "STC")
	block, err := client.GetBlock(8534709)
	if err != nil {
		t.Fatalf("get block failed with error %v", err)
	}
	h := &txHandler{
		chainName:   "STC",
		block:       block,
		chainHeight: 8534709,
		curHeight:   85347089,
		now:         time.Now().Unix(),
		newTxs:      true,
	}
	if len(block.Transactions) == 0 {
		t.Fatal("empty tx")
	}
	for _, tx := range block.Transactions {
		h.OnNewTx(&client, block, tx)
	}
	if h.txRecords[0].FromAddress != "0xa85ed6bc8876fda3d8bafb2667745bf9" {
		t.Fatalf("from addr is wrong: %s", h.txRecords[0].FromAddress)
	}
	if h.txRecords[0].ToAddress != "0xf4da257b8c1c64598f54d50036742811" {
		t.Fatalf("to addr is wrong: %s", h.txRecords[0].ToAddress)
	}
	if h.txRecords[0].Amount.BigInt().Int64() != 100_000 {
		t.Fatalf("amount is wrong: %d", h.txRecords[0].Amount.BigInt().Int64())
	}
	t.Logf("record: %v", h.txRecords[0])
}
