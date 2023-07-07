package biz

import "testing"

func TestParseDevice(t *testing.T) {
	const ua = "openblock-mac/2023070413 (Channel/openblock; UUID/fefe07d0b8104fd1848b320b415aa998; Version/1.0; Device/Firefox 114.0; System/OS X 10.15;)"
	v := ParseDevice(ua)
	if v.Id != "fefe07d0b8104fd1848b320b415aa998" {
		t.Fatalf("got id %s", v.Id)
	}
	if v.UserAgent != ua {
		t.Fail()
	}

	v1 := ParseDevice("xxx")
	if v1.Id != "" {
		t.Fatalf("got id %s", v.Id)
	}
	if v1.UserAgent != "xxx" {
		t.Fail()
	}
}
