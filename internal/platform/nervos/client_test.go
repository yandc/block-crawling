package nervos

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nervosnetwork/ckb-sdk-go/address"
)

func testMainnetGetBlockTokenTransfer(t *testing.T) {
	client, err := NewClient("Nervos", "https://rpc.ankr.com/nervos_ckb")
	if err != nil {
		t.Fatalf("failed create client %s", err)
	}
	// Token transfer: https://explorer.nervos.org/transaction/0xfaee1611485bdf31d5957e6802fe6eee33e4c5f6b52a7ed9e9584c458e7acefc
	block, err := client.GetBlock(8_339_560)
	if err != nil {
		t.Fatalf("failed retrieve block %s", err)
	}
	if block.Hash != "0xf2d5215a63be99942b7e19fddbd51135bc9bf6a3b738873e68eb704f06e10097" {
		t.Fatal("block hash mismatch")
	}
	if block.Number != 8_339_560 {
		t.Fatal("block number mismatch")
	}
	if block.ParentHash != "0xd5e43c84b933f02ea577b82324820639ab47777c7c3af1ca8460bea86406f689" {
		t.Fatal("parent hash")
	}

	for _, tx := range block.Transactions {
		fmt.Printf("---- %s ----\n", tx.Hash)
		rawTx := tx.Raw.(*txWrapper)
		fmt.Printf("\ttxType: %v\n", rawTx.txTypes)
		fmt.Printf("\tfromAddr: %v\n", rawTx.prevOutputs)
		fmt.Printf("\ttoAddr: %v\n", rawTx.toAddresses)
		fmt.Printf("\tamount: %v\n", rawTx.amounts)
		fmt.Printf("\tcontractAddr: %v\n", rawTx.contractAddresses)
		fmt.Printf("\tfee: %d\n", rawTx.totalOutput)

	}
}

func testMainnetGetBlockNativeTransfer(t *testing.T) {
	blockNum := 8_248_989

	client, err := NewClient("Nervos", "https://rpc.ankr.com/nervos_ckb")
	if err != nil {
		t.Fatalf("failed create client %s", err)
	}
	block, err := client.GetBlock(uint64(blockNum))
	if err != nil {
		t.Fatalf("failed retrieve block %s", err)
	}
	if block.Hash != "0xe1dc244e25d0628dfcebcf569fd9df22e5f53206b7f3eb493f69c3026dd0a8f1" {
		t.Fatal("block hash mismatch")
	}
	if block.Number != uint64(blockNum) {
		t.Fatal("block number mismatch")
	}
	if block.ParentHash != "0x458eff215dd4337db0401cc60745a54b576675e98e8470603633f02f790ced87" {
		t.Fatal("parent hash")
	}

	for _, tx := range block.Transactions {
		fmt.Printf("---- %s ----\n", tx.Hash)
		rawTx := tx.Raw.(*txWrapper)
		fmt.Printf("\ttxType: %v\n", rawTx.txTypes)
		fmt.Printf("\tfromAddr: %v\n", rawTx.prevOutputs)
		fmt.Printf("\ttoAddr: %v\n", rawTx.toAddresses)
		fmt.Printf("\tamount: %v\n", rawTx.amounts)
		fmt.Printf("\tcontractAddr: %v\n", rawTx.contractAddresses)
		fmt.Printf("\tfee: %d\n", rawTx.totalOutput)
		cell, _ := json.Marshal(rawTx.outputCells)
		fmt.Printf("\tcell: %s\n", cell)

	}
}

func TestDecode(t *testing.T) {
	lockCodeHash := "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8"
	lockCodeType := "type"
	lockArgs := "0x0000000000000000000000006abc4be14ebba8be5a0abff907ec80513fa23e8d"
	s := &script{
		CodeHash: lockCodeHash,
		HashType: lockCodeType,
		Args:     lockArgs,
	}
	toAddr, err := address.ConvertScriptToAddress(address.Testnet, s.decode())
	if err != nil {
		t.Fatal(err.Error())
	}
	if toAddr != "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqt2h397zn4m4zl95z4llyr7eqz3873rarg2apapy" {
		t.Fatal(toAddr)
	}

}

func TestU128TokenAmount(t *testing.T) {
	amount := parseHexUint128TokenAmount("0x00003029881a56431000000000000000")
	if amount != "300000000000000000000" {
		t.Fatal(amount)
	}
}
