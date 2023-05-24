package kanban

import "context"

type Options struct {
	AggerateTime  int64
	AggerateRange []int64
	AggerateOnly  string
	ChainName     string
	Context       context.Context
	Cancel        func()
}
