package encoder

import (
	"github.com/go-kratos/kratos/v2/transport/http"
	nethttp "net/http"
)

func HttpResponseEncoder(w nethttp.ResponseWriter, req *nethttp.Request, res interface{}) error {
	se := FromResponse(res)
	codec, _ := http.CodecForRequest(req, "Accept")
	body, err := codec.Marshal(se)
	if err != nil {
		w.WriteHeader(500)
		return err
	}
	w.Header().Set("Content-Type", "application/"+codec.Name())
	if se.Code > 0 && se.Code <= 600 {
		w.WriteHeader(int(se.Code))
	} else {
		w.WriteHeader(500)
	}

	_, err = w.Write(body)
	return err
}
