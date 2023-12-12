package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEVMAddressToBFC(t *testing.T) {
	assert.Equal(t, "BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999", UnifyBFCAddress("BenfenTEST", "0x4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af9"))
	assert.Equal(t, "0x4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af9", UnifyBFCAddress("SUI", "0x4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af9"))

	assert.Equal(t, "BFC91a8926de4335d2e83d755c6b42cc4bdb6e48063705982dcef5859bda0aa63fa9061", UnifyBFCAddress("BenfenTEST", "bfc91a8926de4335d2e83d755c6b42cc4bdb6e48063705982dcef5859bda0aa63fa9061"))
}

func TestDecimals(t *testing.T) {
	assert.Equal(t, "0", StringDecimals("0", 0))
}
