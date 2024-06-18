package tonutils

// Copied from https://github.com/tonkeeper/tongo/blob/9637e64eb8cad80cd456dbc69d5dca96a2177dc4/ton/account.go
import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/snksoft/crc"
)

type AccountID struct {
	Workchain int32
	Address   [32]byte
}

func NewAccountID(id int32, addr [32]byte) *AccountID {
	return &AccountID{Workchain: id, Address: addr}
}

func (id AccountID) String() string {
	return id.ToRaw()
}

func (id AccountID) IsZero() bool {
	for i := range id.Address {
		if id.Address[i] != 0 {
			return false
		}
	}
	return true
}

func (id AccountID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.ToRaw())
}

func (id *AccountID) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	a, err := ParseAccountID(s)
	if err != nil {
		return err
	}
	id.Workchain = a.Workchain
	id.Address = a.Address
	return nil
}

func (id AccountID) ToRaw() string {
	return fmt.Sprintf("%v:%x", id.Workchain, id.Address)
}

func (id AccountID) ToHuman(bounce, testnet bool) string {
	prefix := byte(0b00010001)
	if testnet {
		prefix |= 0b10000000
	}
	if !bounce {
		prefix |= 0b01000000
	}
	buf := make([]byte, 36)
	buf[0] = prefix
	buf[1] = byte(id.Workchain)
	copy(buf[2:34], id.Address[:])
	binary.BigEndian.PutUint16(buf[34:36], Crc16(buf[:34]))
	return base64.URLEncoding.EncodeToString(buf)
}

func (id AccountID) MarshalTL() ([]byte, error) {
	payload := make([]byte, 36)
	binary.LittleEndian.PutUint32(payload[:4], uint32(id.Workchain))
	copy(payload[4:36], id.Address[:])
	return payload, nil
}

func (id *AccountID) UnmarshalTL(r io.Reader) error {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return err
	}
	id.Workchain = int32(binary.LittleEndian.Uint32(b[:]))
	_, err = io.ReadFull(r, id.Address[:])
	return err
}

/*
	func (id *AccountID) ToMsgAddress() tlb.MsgAddress {
		if id == nil {
			return tlb.MsgAddress{
				SumType: "AddrNone",
			}
		}
		return tlb.MsgAddress{
			SumType: "AddrStd",
			AddrStd: struct {
				Anycast     tlb.Maybe[tlb.Anycast]
				WorkchainId int8
				Address     tlb.Bits256
			}{
				WorkchainId: int8(id.Workchain),
				Address:     id.Address,
			},
		}
	  }
*/
func AccountIDFromBase64Url(s string) (AccountID, error) {
	var aa AccountID
	s = strings.Map(func(r rune) rune {
		switch r {
		case '+':
			return '-'
		case '/':
			return '_'
		default:
			return r
		}
	}, s)
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return AccountID{}, err
	}
	if len(b) != 36 {
		return AccountID{}, fmt.Errorf("invalid account 'user friendly' form length: %v", s)
	}
	checksum := uint64(binary.BigEndian.Uint16(b[34:36]))
	if checksum != crc.CalculateCRC(crc.XMODEM, b[0:34]) {
		return AccountID{}, fmt.Errorf("invalid checksum")
	}
	aa.Workchain = int32(int8(b[1]))
	copy(aa.Address[:], b[2:34])
	return aa, nil
}

func AccountIDFromRaw(s string) (AccountID, error) {
	var aa AccountID

	colon := strings.IndexByte(s, ':')
	if colon == -1 {
		return AccountID{}, fmt.Errorf("invalid account id format")
	}
	if len(s)-colon-1 < 64 {
		s = s[:colon] + ":" + strings.Repeat("0", 64-len(s)+colon+1) + s[colon+1:]
	}
	w, err := strconv.ParseInt(s[:colon], 10, 32)
	if err != nil {
		return AccountID{}, err
	}
	address, err := hex.DecodeString(s[colon+1:])
	if err != nil {
		return AccountID{}, err
	}
	if len(address) != 32 {
		return AccountID{}, fmt.Errorf("address len must be 32 bytes")
	}
	aa.Workchain = int32(w)
	copy(aa.Address[:], address)
	return aa, nil
}

func ParseAccountID(s string) (AccountID, error) {
	aa, err := AccountIDFromRaw(s)
	if err != nil {
		aa, err = AccountIDFromBase64Url(s)
		if err != nil {
			return AccountID{}, err
		}
	}
	return aa, nil
}
