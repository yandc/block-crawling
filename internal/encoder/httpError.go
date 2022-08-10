package encoder

import (
	"block-crawling/internal/common"
	"fmt"
	"github.com/go-kratos/kratos/v2/errors"
)

func FromError(err error) *common.ApiResponse {
	if err == nil {
		return nil
	}
	if se := new(common.ApiResponse); errors.As(err, &se) {
		return se
	}
	if se := new(errors.Error); errors.As(err, &se) {
		return common.NewApiResponse(se.Code, false, se.Message, se.Metadata)
	}
	return common.NewApiResponse(common.SYSTEM_ERROR.Code, false, common.SYSTEM_ERROR.Msg, fmt.Sprintf("%s", err))
}
