package ethereum

import (
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestPairToken(t *testing.T) {
	c := NewClient("https://rpc.ankr.com/bsc", "BSC")
	token0, err := c.GetDexPairToken("0x172fcD41E0913e95784454622d1c3724f546f849", "token0")
	if err != nil {
		t.Fatalf("Got err: %s", err.Error())
	}
	t.Logf("token0: %s", token0)

	token1, err := c.GetDexPairToken("0x172fcD41E0913e95784454622d1c3724f546f849", "token1")
	if err != nil {
		t.Fatalf("Got err: %s", err.Error())
	}
	t.Logf("token1: %s", token1)
}

func TestDodoPairToken(t *testing.T) {
	c := NewClient("https://rpc.ankr.com/polygon", "Polygon")
	token0, token1, err := c.GetDexDodoPairTokens("0x813FddecCD0401c4Fa73B092b074802440544E52")
	if err != nil {
		t.Fatalf("Got err: %s", err.Error())
	}
	t.Logf("token0: %s", token0)
	t.Logf("token1: %s", token1)
}

func TestZapField(t *testing.T) {
	f := zap.Any("xx", true)
	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{})
	buf, _ := enc.EncodeEntry(
		zapcore.Entry{
			Level:      zap.ErrorLevel,
			Time:       time.Now(),
			LoggerName: "",
			Message:    "",
			Caller:     zapcore.EntryCaller{},
			Stack:      "",
		},
		[]zapcore.Field{f},
	)
	t.Logf("%s", buf.String())
}
