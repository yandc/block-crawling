package httpclient

import (
	"block-crawling/internal/types"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var globalTransport *http.Transport

func init() {
	globalTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
}

func HttpsGetForm(url string, params map[string]string, out interface{}) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
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
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &out); err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
		return err
	}
	return nil
}

func HttpsSignGetForm(url string, params map[string]string, authorization string, out interface{}) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authorization)
	q := req.URL.Query()
	for k, v := range params {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()
	client := &http.Client{Transport: globalTransport}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &out); err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
		return err
	}
	return nil

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

func HttpPostForm(url string, v interface{}, out interface{}) error {
	str, err := json.Marshal(v)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(str)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := http.Client{Transport: globalTransport}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &out); err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
		return err
	}
	return nil
}

func HttpsForm(url, method string, params map[string]string, reqBody, out interface{}) error {
	bytes, _ := json.Marshal(reqBody)
	req, err := http.NewRequest(method, url, strings.NewReader(string(bytes)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if params != nil {
		q := req.URL.Query()
		for k, v := range params {
			q.Add(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &out); err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
		return err
	}
	return nil
}

func HttpsPost(url string, id int, method, jsonrpc string, out interface{}, params []interface{}, args ...interface{}) (http.Header, error) {
	request := types.Request{
		ID:      id,
		Jsonrpc: jsonrpc,
		Method:  method,
		Params:  params,
	}
	str, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(str)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := http.DefaultClient
	if len(args) > 0 {
		client.Timeout = time.Duration(args[0].(int)) * time.Millisecond
	}
	resp, err := client.Do(req)
	if err != nil {
		if resp != nil && resp.Header != nil {
			return resp.Header, err
		}
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.Header, err
	}
	//fmt.Println(string(body))
	if err = json.Unmarshal(body, out); err != nil {
		statusCode := resp.StatusCode
		status := "HTTP " + strconv.Itoa(statusCode) + " " + http.StatusText(statusCode)
		err = errors.New(status + "\n" + string(body))
		return resp.Header, err
	}
	return resp.Header, nil
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
