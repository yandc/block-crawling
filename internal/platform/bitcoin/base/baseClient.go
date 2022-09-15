package base

import (
	"block-crawling/internal/model"
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

type Client struct {
	URL       string
	StreamURL string
	ChainName string
}

func (c *Client) BuildURL(u string, params map[string]string) (target *url.URL, err error) {
	target, err = url.Parse(c.URL + u)
	if err != nil {
		return
	}
	values := target.Query()
	//Set parameters
	for k, v := range params {
		values.Set(k, v)
	}
	//add token to url, if present

	target.RawQuery = values.Encode()
	return
}

//当前块高
func (c *Client) GetBlockCount()  (count model.BTCCount, err error) {
	countParam := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getblockcount",
	}
	var pa = make([]interface{}, 0, 0)
	countParam.Params = pa
	err = postResponse(c.StreamURL, countParam, &count)
	return
}


//func (c *Client) GetBlock()  (count model.UTXOBlock, err error) {
//
//}











func postResponse(target string, encTarget interface{}, decTarget interface{}) (err error) {
	var data bytes.Buffer
	enc := json.NewEncoder(&data)
	if err = enc.Encode(encTarget); err != nil {
		return
	}
	resp, err := http.Post(target, "application/json", &data)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, decTarget)
	if err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
	}
	return
}