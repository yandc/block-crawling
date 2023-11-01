package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEVMAddressToBFC(t *testing.T) {
	assert.Equal(t, "BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999", EVMAddressToBFC("BenfenTEST", "0x4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af9"))
	assert.Equal(t, "0x4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af9", EVMAddressToBFC("SUI", "0x4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af9"))

}
