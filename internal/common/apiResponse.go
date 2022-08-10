package common

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type ApiResponse struct {
	Code   int32       `json:"code,omitempty"`
	Status bool        `json:"status"`
	Msg    string      `json:"msg,omitempty"`
	Data   interface{} `json:"data,omitempty"`
}

func (e *ApiResponse) Error() string {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	err := encoder.Encode(e)
	if err != nil {
		return fmt.Sprintf("encode apiResponse error, %s", err)
	}
	json := string(buffer.Bytes())
	return json
}

func NewApiResponse(code int32, status bool, msg string, data interface{}) *ApiResponse {
	return &ApiResponse{
		Code:   code,
		Status: status,
		Msg:    msg,
		Data:   data,
	}
}
