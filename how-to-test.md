
## 如何编写测试用例

编写测试用例涉及一个结构体和一个入口启动函数： `Preparation` 和 `RunTest` 。我们主要涉及构造 `Preparation` 然后传递给 `RunTest` 去运行。
这里我们着重介绍一下如果构造 `Preparation`。


### 测试爬块

如果要测试爬块，我们需要指定要爬的块的列表，通过指定 `Preparation.IndexBlockNumbers` 字段。

我们以 Solana 为例，创建 `internal/ptesting/solana/solana_test.go`，填充：

``` go
package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/ptesting"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName: "Solana",
		Configs:   "../configs",
		Users: map[string]string{
			"CicfACV8ucYy2nQhHcJn6mW4GjBuhMPDmRkN76G1MeEy": "123",
		},
		IndexBlockNumbers: []uint64{
			175757542,
			175757543,
		},
		Assert: func() {
			record, err := data.SolTransactionRecordRepoClient.FindLast(context.Background(), biz.GetTableName("Solana"))
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.ToAddress, "CicfACV8ucYy2nQhHcJn6mW4GjBuhMPDmRkN76G1MeEy")
			assert.Equal(t, record.TransactionHash, "2yXp5Gwdg2xx6Ekev5jjYkcr4nK9T1uhre7ciNvNvBGDYrh4BKLgeS57ECE9CD2PYzJ9EiCKr9awjFm7NhLequGc")
			assert.Equal(t, record.ToUid, "123")
			assert.Equal(t, record.TransactionType, biz.TRANSFER)
			assert.Equal(t, record.BlockNumber, 159381368)
		},
	})
}
```

上面代码主要通过 `Preparation` 控制 `RunTest` 的行为：

1. 通过 `ChainName` 指定要测试的链；
2. 通过 `Configs` 字段指定配置文件目录，我们这里使用当前文件父级目录下的 `configs` 目录，也就是 `internal/ptesting/configs/` 目录；
3. 通过 `Users` 字段指定涉及的用户匹配，也就是哪个地址能匹配到哪个用户，爬块前 `RunTest` 会把对应的记录写入到 Redis 中；
4. 通过 `IndexBlockNumbers` 指定要爬的块；
5. 然后通过 `Assert` 字段指定一个回调函数在爬块完成后执行断言检测，检测爬块结果。

### 测试交易兜底

测试兜底，我们需要通过 `Preparation.PendingTxs` 指定兜底的交易列表。

我们在 `internal/ptesting/solana/solana_test.go` 中追加:

``` go
import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/ptesting"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPendingTx(t *testing.T) {
	txHash := "645vEGLMcbAdm7YuRpeDRpqYMEGQpZG9BSf3B4vaQLieJpUELQ8XjCHtdBM5yejAr7ejjvS2ktenda77rysBaCJN"
	tableName := biz.GetTableName("Solana")

	ptesting.RunTest(ptesting.Preparation{
		Configs:           "../configs",
		ChainName:         "Solana",
		IndexBlockNumbers: []uint64{},
		Users: map[string]string{
			"FSmS8G3UL1XvxCXrVRpnGiUv1xDD8nPfSEGBXnZCsuzL": "123",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "123",
				ChainName:       "Solana",
				TransactionHash: txHash,
				Status:          "pending",
				FromAddress:     "FSmS8G3UL1XvxCXrVRpnGiUv1xDD8nPfSEGBXnZCsuzL",
				ToAddress:       "7E1QHT8haaDRvDF4XxFGWtSJW2i5sDfna8SmmyzQVxv6",
				Amount:          "0.04078303",
				FeeAmount:       "0.00005",
				TransactionType: "native",
			},
		},
		AfterPrepare: func() {
			record, err := data.SolTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, "pending", record.Status)
		},
		Assert: func() {
			record, err := data.SolTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, "success", record.Status)
		},
	})
}
```

这里有两个变化：

1. 填充了 `PendingTxs` 字段， `RunTest` 会将这些交易作为 pending 状态的交易插入到数据库；
2. 使用 `AfterPrepare` 回调函数，用来做前置数据检查，这里用来检查兜底之前交易状态为 `pending`。

### 数据本地保存

数据本地保存防止链上节点清理数据（如 Solana）避免后期无法从链上数据测试失效的问题，同时也能加速测试再运行。

数据本地保存需要将 `Preparation.Prefetch` 置为 `true` 同时提供 `RawBlockType` 和 `RawTxType` 。

接下来运行 `RunTest` 会首先将 `Preparation.IndexBlockNumbers` 中的块数据和
`Preparation.PendingTxs` 中的交易数据进行预获取并保存到测试目录下 `.prefetched` 目录下，如果对应数据已存在则直接读取对应目录下的本地数据，并根据 `RawBlockType` 和 `RawTxType` 进行类型转换。

``` go
package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/ptesting"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName: "Solana",
		Configs:   "../configs",
		Prefetch:  true,
		RawBlockType: reflect.TypeOf(1),
		RawTxType:    reflect.TypeOf(new(solana.TransactionInfo)),
		Users: map[string]string{
			"CicfACV8ucYy2nQhHcJn6mW4GjBuhMPDmRkN76G1MeEy": "123",
		},
		IndexBlockNumbers: []uint64{
			175757542,
			175757543,
		},
		Assert: func() {
			record, err := data.SolTransactionRecordRepoClient.FindLast(context.Background(), biz.GetTableName("Solana"))
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.ToAddress, "CicfACV8ucYy2nQhHcJn6mW4GjBuhMPDmRkN76G1MeEy")
			assert.Equal(t, record.TransactionHash, "2yXp5Gwdg2xx6Ekev5jjYkcr4nK9T1uhre7ciNvNvBGDYrh4BKLgeS57ECE9CD2PYzJ9EiCKr9awjFm7NhLequGc")
			assert.Equal(t, record.ToUid, "123")
			assert.Equal(t, record.TransactionType, biz.TRANSFER)
			assert.Equal(t, record.BlockNumber, 159381368)
		},
	})
}
```

主要变更：

1. 增加 `Prefetch` 字段并设置为 `true`；
2. 增加 `RawBlockType` & `RawTxType` 标记 `Raw` 字段类型，用来将存储到本地的数据解析补全类型信息。

**NOTE**:

1. 如果兜底需要依赖爬块，则可以指定 `IndexBlockNumbers` 对块数据进行本地保存；
2. 同时此机制也可以手动调整本地数据，来模拟一些特殊场景。

## 运行测试

### 安装配置 Docker

1. 安装 [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. [配置镜像站点加速 Docker 镜像拉取](https://yeasy.gitbook.io/docker_practice/install/mirror#macos)

### 运行测试

```shell
make test
```

## 答疑

实现过程中碰到了一些限制，通过尝试规避了这些限制，但是同时也导致了不符合直觉的结果。

### 为什么使用 Docker？

测试引入了 Docker 这个外部组件，增加了组件依赖很不方面，为什么没有通过 [sqlmock](https://pkg.go.dev/github.com/DATA-DOG/go-sqlmock) 或 [redismock](https://github.com/go-redis/redismock)，
甚至是 SQLite 来替代数据库呢？

首先是尝试了 sqlmock 和 redismock 的，其需要提前写好预期的结果然后运行单元测试来验证这些结果，和我们想要集成测试的想法冲突，而且很不方便。

使用 SQLite 面临我们在 GORM 中硬编码的很多 PostgresQL 类型不兼容的问题，同时可能会面临测试通过但是真正放到连接 PostgresQL 结果异常的情况。

综上，因而选择了使用 Docker 运行真实的环境，减少这些问题。


### 为什么测试要写在单独的包里？

因为启动需要依赖 `platform` 包，如果在把测试写在 `platform` 的子包下再去依赖 `platform` 包就会造成循环依赖。
为了规避这个问题，所以需要将所有的测试用例放在 `ptesting` 这个包里。

### 为什么引入 `Preparation.BeforePrepare` 、`Preparation.AfterPrepare` 和 `Preparation.Assert` ？

为什么不在 `RunTest` 调用前后直接进行初始化和后续的断言？因为 `RunTest` 中通过 `wire` 进行了初始化，并在函数返回后进行了清理。
这也就意味着，`RunTest` 运行之前数据库连接还未打开，同时 `RunTest` 之后数据库连接已经被清理。

所以就加入了这些回调，在特殊的节点进行调用，保证数据库连接状态正常，可以执行正常的测试前和测试后断言和校验。

### 为什么数据本地化需要 `RawBlockType` 和 `RawTxType` ?

因为通过 JSON 序列化到本地后，类型信息就会丢失，从而导致后续逻辑错误，所以需要提供这两个字段用于转换。
