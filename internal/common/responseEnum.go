package common

var SUCCESS = ResponsePair{Code: 200, Msg: "ok"}
var SYSTEM_ERROR = ResponsePair{Code: 500, Msg: "system error"}

type ResponsePair struct {
	Code int32
	Msg  string
}
