package dogecoin

//import (
//	"block-crawling/internal/biz"
//	httpclient2 "block-crawling/internal/httpclient"
//	"block-crawling/internal/types"
//	"fmt"
//)
//
//type Client struct {
//	URL string
//}
//
//func NewClient(nodeUrl string) Client {
//	return Client{nodeUrl}
//}
//
//func (c *Client) GetBlockNumber(index int) (int, error) {
//	url := c.URL + "sync/block_number"
//	var height int
//	key := biz.AppConfig.GetDogeKey()[index]
//	err := httpclient2.HttpsSignGetForm(url, nil, key, &height)
//	return height, err
//}
//
//func (c *Client) GetBlockByNumber(number int, index int) (types.Dogecoin, error) {
//	url := c.URL + "block/" + fmt.Sprintf("%d", number)
//	key := biz.AppConfig.GetDogeKey()[index]
//	var block types.Dogecoin
//	err := httpclient2.HttpsSignGetForm(url, nil, key, &block)
//	return block, err
//}
//
//func (c *Client) GetBalance(address string) (string, error) {
//	url := c.URL + "account/" + address
//	key := biz.AppConfig.GetDogeKey()[4]
//	var balances []types.Balances
//	err := httpclient2.HttpsSignGetForm(url, nil, key, &balances)
//	if err == nil {
//		return balances[0].ConfirmedBalance , nil
//	}else {
//		return "",nil
//	}
//}
//func (c *Client) GetTransactionsByTXHash(tx string)(types.TxInfo,error){
//	url := c.URL +"tx/" + tx
//	key := biz.AppConfig.GetDogeKey()[4]
//	var txInfo types.TxInfo
//	err := httpclient2.HttpsSignGetForm(url, nil, key, &txInfo)
//	return txInfo ,err
//}
