package common

import (
	"block-crawling/internal/log"
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
)

func HandlerFunction(ctx context.Context, req, err interface{}) error {
	if err != nil {
		if e, ok := err.(error); ok {
			log.Error("error, req:", zap.Any("req", req), zap.Any("error", e))
			return e
		} else {
			e = errors.New(fmt.Sprintf("%s", err))
			log.Error("panic, req:", zap.Any("req", req), zap.Any("error", e))
			return e
		}
	}

	return nil
}
