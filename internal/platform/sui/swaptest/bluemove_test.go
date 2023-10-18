package swaptest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBluemove(t *testing.T) {
	pairs := doExtractPairs(t, "WiMhvB1XcQKHqjvvpyawNjTrWcWYrUa1XNobuzyzraz")
	assert.Equal(t, 1, len(pairs), "extract 1 pairs")
	assert.Equal(t, "0x41aecbafec6dddc032778f195fffe5cfbc8f8ba11e722902dc3448eee85c6059", pairs[0].PairContract, "[0].pairContract")
	assert.Equal(t, "0xd9f9b0b4f35276eecd1eea6985bfabe2a2bbd5575f9adb9162ccbdb4ddebde7f::smove::SMOVE", pairs[0].Input.Address, "[0].inputToken")
	assert.Equal(t, "460000000000", pairs[0].Input.Amount, "[0].inputAmount")
	assert.Equal(t, "0x2::sui::SUI", pairs[0].Output.Address, "[0].outputToken")
	assert.Equal(t, "34056099971", pairs[0].Output.Amount, "[0].outputAmount")
}
