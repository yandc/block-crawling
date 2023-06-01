package httpclient

import (
	"block-crawling/internal/types"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	//"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Danny-Dasilva/CycleTLS/cycletls"
	"github.com/RomainMichau/cloudscraper_go/cloudscraper"
)

var globalTransport *http.Transport
var pool = &sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func init() {
	//rpc 代理
	//uu, _ := url.Parse("http://127.0.0.1:7890")
	globalTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		//Proxy:            http.ProxyURL(uu),

	}
}

func HttpsGetForm(url string, urlParams map[string]string, out interface{}, timeout *time.Duration) (err error) {
	err = HttpRequest(url, http.MethodGet, nil, urlParams, nil, out, timeout, globalTransport)
	return
}

func HttpsSignGetForm(url string, urlParams, headerParams map[string]string, out interface{}, timeout *time.Duration) (err error) {
	err = HttpRequest(url, http.MethodGet, headerParams, urlParams, nil, out, timeout, globalTransport)
	return
}

func HttpPostJson(url string, reqBody, out interface{}, timeout *time.Duration) (err error) {
	err = HttpRequest(url, http.MethodPost, nil, nil, reqBody, out, timeout, globalTransport)
	return
}

func GetResponse(url string, urlParams map[string]string, out interface{}, timeout *time.Duration) (err error) {
	err = HttpRequest(url, http.MethodGet, nil, urlParams, nil, out, timeout, nil)
	return err
}

func GetResponseApiJson(url string, urlParams map[string]string, out interface{}, timeout *time.Duration) (err error) {
	headerParams := map[string]string{
		"Accept":       "application/vnd.api+json",
		"Content-Type": "application/vnd.api+json",
	}
	err = HttpRequest(url, http.MethodGet, headerParams, urlParams, nil, out, timeout, globalTransport)
	return err
}

func PostResponse(url string, reqBody, out interface{}, timeout *time.Duration) (err error) {
	err = HttpRequest(url, http.MethodPost, nil, nil, reqBody, out, timeout, nil)
	return
}

func GetStatusCode(url string, urlParams map[string]string, out interface{}, timeout *time.Duration, transport *http.Transport) (statusCode int, err error) {
	statusCode, err = HttpGet(url, urlParams, out, timeout, transport)
	return
}

func JsonrpcCall(url string, id int, jsonrpc, method string, out interface{}, params interface{}, timeout *time.Duration) (header http.Header, err error) {
	header, err = JsonrpcRequest(url, id, jsonrpc, method, out, params, timeout, nil)
	return
}

func JsonrpcRequest(url string, id int, jsonrpc, method string, out interface{}, params interface{}, timeout *time.Duration, transport *http.Transport) (header http.Header, err error) {
	var resp types.Response
	request := types.Request{
		Id:      id,
		Jsonrpc: jsonrpc,
		Method:  method,
		Params:  params,
	}
	header, err = HttpPost(url, request, &resp, timeout, transport)
	if err != nil {
		return
	}
	if resp.Error != nil {
		return header, resp.Error
	}
	err = json.Unmarshal(resp.Result, out)
	return
}

func HttpGet(url string, urlParams map[string]string, out interface{}, timeout *time.Duration, transport *http.Transport) (statusCode int, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if urlParams != nil {
		q := req.URL.Query()
		for k, v := range urlParams {
			q.Add(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}
	var client *http.Client
	if transport == nil {
		client = http.DefaultClient
	} else {
		client = &http.Client{Transport: transport}
	}
	if timeout != nil {
		client.Timeout = *timeout
	}
	resp, err := client.Do(req)
	if resp != nil {
		statusCode = resp.StatusCode
	}
	err = handleResponse(resp, err, out)
	return
}

func HttpPost(url string, reqBody, out interface{}, timeout *time.Duration, transport *http.Transport) (header http.Header, err error) {
	byteArr, err := json.Marshal(reqBody)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(byteArr)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	var client *http.Client
	if transport == nil {
		client = http.DefaultClient
	} else {
		client = &http.Client{Transport: transport}
	}
	if timeout != nil {
		client.Timeout = *timeout
	}
	resp, err := client.Do(req)
	if resp != nil {
		header = resp.Header
	}
	err = handleResponse(resp, err, out)
	return
}

func GetUseCloudscraper(url string, out interface{}, timeout *time.Duration) (err error) {
	var timeoutInt int
	if timeout != nil {
		timeoutInt = int(timeout.Seconds())
	}
	client, err := cloudscraper.Init(false, false)
	if err != nil {
		return
	}
	options := cycletls.Options{
		Headers: map[string]string{"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
			"Accept":       "application/json",
			"Content-Type": "application/json",
		},

		//Proxy:           "http://127.0.0.1:7890",
		Timeout:         timeoutInt,
		DisableRedirect: true,
	}
	resp, err := client.Do(url, options, "GET")
	statusCode := resp.Status
	body := resp.Body
	if err != nil {
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + body + "\n" + fmt.Sprintf("%s", err))
		return
	}
	err = json.Unmarshal([]byte(body), out)
	if err != nil {
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + body + "\n" + fmt.Sprintf("%s", err))
	}
	return
}

func HttpsGetFormString(url string, params map[string]string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	q := req.URL.Query()
	for k, v := range params {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()
	client := &http.Client{Transport: globalTransport}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func HttpsParamsPost(url string, params interface{}) (string, error) {
	var bodyReader string
	if value, ok := params.(string); ok {
		bodyReader = value
	} else {
		bytes, err := json.Marshal(params)
		if err != nil {
			return "", err
		}
		bodyReader = string(bytes)
	}

	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(bodyReader))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func HttpRequest(url, method string, headerParams, urlParams map[string]string, reqBody, out interface{}, timeout *time.Duration, transport *http.Transport) (err error) {
	var body io.Reader
	if reqBody != nil {
		var byteArr []byte
		byteArr, err = json.Marshal(reqBody)
		if err != nil {
			return
		}
		body = strings.NewReader(string(byteArr))
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if headerParams != nil {
		for k, v := range headerParams {
			req.Header.Set(k, v)
		}
	}
	if urlParams != nil {
		q := req.URL.Query()
		for k, v := range urlParams {
			q.Add(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}
	var client *http.Client
	if transport == nil {
		client = http.DefaultClient
	} else {
		client = &http.Client{Transport: transport}
	}
	if timeout != nil {
		client.Timeout = *timeout
	}
	resp, err := client.Do(req)
	err = handleResponse(resp, err, out)
	return
}

func handleResponse(resp *http.Response, e error, out interface{}) (err error) {
	err = e
	if err != nil {
		if resp != nil {
			statusCode := resp.StatusCode
			status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
			err = errors.New(status + "\n" + fmt.Sprintf("%s", err))
		}
		return
	}
	buf := pool.Get().(*bytes.Buffer)
	defer pool.Put(buf)
	buf.Reset()
	defer resp.Body.Close()

	_, err = io.Copy(buf, resp.Body)
	body := buf.String()
	if err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + body + "\n" + fmt.Sprintf("%s", err))
		return
	}
	err = json.Unmarshal(buf.Bytes(), out)
	if err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + body + "\n" + fmt.Sprintf("%s", err))
	}
	return
}
