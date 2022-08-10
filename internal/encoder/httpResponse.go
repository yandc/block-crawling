package encoder

import "block-crawling/internal/common"

func FromResponse(res interface{}) *common.ApiResponse {
	if res == nil {
		return nil
	}
	if re, ok := res.(*common.ApiResponse); ok {
		return re
	}
	if re, ok := res.(common.ApiResponse); ok {
		return &re
	}
	return common.NewApiResponse(common.SUCCESS.Code, true, common.SUCCESS.Msg, res)
}
