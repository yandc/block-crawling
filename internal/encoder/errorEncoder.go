package encoder

import (
	"github.com/go-kratos/kratos/v2/transport/http"
	nethttp "net/http"
)

func HttpErrorEncoder(w nethttp.ResponseWriter, req *nethttp.Request, err error) {
	se := FromError(err)
	codec, _ := http.CodecForRequest(req, "Accept")
	body, err := codec.Marshal(se)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.Header().Set("Content-Type", "application/"+codec.Name())
	if se.Code > 0 && se.Code <= 600 {
		w.WriteHeader(int(se.Code))
	} else {
		w.WriteHeader(500)
	}

	_, _ = w.Write(body)
}
