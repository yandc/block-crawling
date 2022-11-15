package sui

import (
	"testing"

	"gitlab.bixin.com/mili/node-driver/chain"
)

var nodeUrl = "https://fullnode.devnet.sui.io/"
var chainName = "suiTest"

func TestTransferObj(t *testing.T) {
	client := NewClient(nodeUrl, chainName)
	tx, err := client.GetTransactionByHash("RdEHVnmBLc4dwQarmi5LuptPP8a08lwVttQdD+ok9ZQ=")
	if err != nil {
		t.Fatalf("retrieve tx failed with error %v", err)
	}
	h := &txHandler{
		chainName: chainName,
		block: &chain.Block{
			Hash: tx.Certificate.TransactionDigest,
			Raw:  tx,
			Transactions: []*chain.Transaction{
				{
					Raw: tx,
				},
			},
		},
		chainHeight: 0,
		curHeight:   0,
		now:         0,
		newTxs:      true,
	}
	err = h.OnNewTx(&client, h.block, h.block.Transactions[0])
	if err != nil {
		t.Fatalf("handle tx failed with error %v", err)
	}

	t.Logf("records[0].Amount %s", h.txRecords[0].Amount)
}

func TestTransferSui(t *testing.T) {
	client := NewClient(nodeUrl, chainName)
	tx, err := client.GetTransactionByHash("zgu50ATAjNMIxRKP807BfKY49JdCDCEEOSxOZM0thVM=")
	if err != nil {
		t.Fatalf("retrieve tx failed with error %v", err)
	}
	h := &txHandler{
		chainName: chainName,
		block: &chain.Block{
			Hash: tx.Certificate.TransactionDigest,
			Raw:  tx,
			Transactions: []*chain.Transaction{
				{
					Raw: tx,
				},
			},
		},
		chainHeight: 0,
		curHeight:   0,
		now:         0,
		newTxs:      true,
	}
	err = h.OnNewTx(&client, h.block, h.block.Transactions[0])
	if err != nil {
		t.Fatalf("handle tx failed with error %v", err)
	}

	t.Logf("records[0].Amount %s", h.txRecords[0].Amount)
}
