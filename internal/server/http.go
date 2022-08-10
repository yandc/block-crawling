package server

import (
	v1 "block-crawling/api/helloworld/v1"
	"block-crawling/internal/conf"
	"block-crawling/internal/encoder"
	"block-crawling/internal/service"
	"context"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/selector"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/gorilla/handlers"
)

func NewSkipRoutersMatcher() selector.MatchFunc {
	skipRouters := map[string]struct{}{
		"/block-crawling.v1.Greeter/GetUser": {},
		"/block-crawling.v1.Greeter/AddUser": {},
	}

	return func(ctx context.Context, operation string) bool {
		if _, ok := skipRouters[operation]; ok {
			return false
		}
		return true
	}
}

// NewHTTPServer new a HTTP server.
func NewHTTPServer(c *conf.Server, greeter *service.GreeterService, logger log.Logger) *http.Server {
	var opts = []http.ServerOption{
		http.ResponseEncoder(encoder.HttpResponseEncoder),
		http.ErrorEncoder(encoder.HttpErrorEncoder),

		http.Middleware(
			recovery.Recovery(),
			//selector.Server(auth.JWTAuth(jwtc.Secret)).Match(NewSkipRoutersMatcher()).Build(),
			logging.Server(logger),
		),
		http.Filter(
			handlers.CORS(
				handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}),
				handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS", "DELETE"}),
				handlers.AllowedOrigins([]string{"*"}),
			),
		),
	}
	/*var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
		),
	}*/
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)
	v1.RegisterGreeterHTTPServer(srv, greeter)
	return srv
}
