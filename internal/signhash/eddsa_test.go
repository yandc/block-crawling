package signhash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBenfen(t *testing.T) {
	r, err := Hash("SUI", &SignMessageRequest{
		SessionId:   "",
		Address:     "BFC2a7d819aac0ae48af3e37de29fa7f72541108fdcc6685b7c0f23fff15e6224b61011",
		ChainName:   "BenfenTest",
		RawChainId:  []byte{},
		Application: "",
		Method:      "",
		Message:     []byte(`"42464332613764383139616163306165343861663365333764653239666137663732353431313038666463633636383562376330663233666666313565363232346236313031313a64724446783045352d637a716e374a56366168496c4438324a70574a5250625254453530594b63556742383461384152714344414a336a4c617a545064586249"`),
	})
	assert.NoError(t, err)
	assert.Equal(t, "a331f7c4742e0c0b82883108b47f43bfb7aa270e28ce9e226a99f49cd61d5949", r)
}
