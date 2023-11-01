package ptesting

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform"
	"block-crawling/internal/types"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/google/wire"
	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var RunnerProviderSet = wire.NewSet(NewRunner)

// TokenPrice price of token
type TokenPrice struct {
	TokenAddress string
	Currency     string
	Price        string
}

// Cancellation to cancel & wait.
type Cancellation struct {
	Context context.Context
	cancel  func()
}

func NewCancellation() *Cancellation {
	ctx, cancel := context.WithCancel(context.Background())
	return &Cancellation{
		Context: ctx,
		cancel:  cancel,
	}
}

func (c *Cancellation) Cancel(preparation *Preparation) {
	for _, p := range preparation.patches {
		p.Reset()
	}
	c.cancel()
}

// Preparation preparation to run test
type Preparation struct {
	Configs           string                        // Path of configs.
	ChainName         string                        // Chain to run.
	IndexBlockNumbers []uint64                      // Blocks that need to index.
	PendingTxs        []*pb.TransactionReq          // Pending transaction hashs that need to seal.
	Users             map[string]string             // Users for match.
	Tokens            []types.TokenInfo             // Tokens fetched from node-proxy.
	NFTs              []*v1.GetNftReply_NftInfoResp // NFTs fetched from node-proxy.
	TokenPrices       []TokenPrice                  // Token prices fetched from node-proxy.
	Prefetch          bool                          // Prefetch blocks & txs.
	RawBlockType      reflect.Type
	RawTxType         reflect.Type
	SourceFromChain   bool                      // 是否从 链上获取
	BeforePrepare     func()                    // Will be called before preparation.
	AfterPrepare      func()                    // Will be called before indexing blocks & sealing txs.
	Test              func(*platform.Bootstrap) // Will be called before preparation.
	Assert            func()                    // Will be called after indexing blocks & sealing txs.
	AssertErrs        func([]error)             // Will be called after indexing blocks & sealing txs.

	prefetchedBlocks      []*chain.Block       // Prefetched blocks that don't need to request node.
	prefetchedTransctions []*chain.Transaction // Prefetched transactionts that don't need to request node.
	patches               []*gomonkey.Patches
}

func (p *Preparation) prepare(usecase *biz.TransactionUsecase, plat *platform.Bootstrap) (indexing bool) {
	for address, uid := range p.Users {
		data.RedisClient.Set(biz.USERCENTER_ADDRESS_KEY+address, fmt.Sprintf("{\"uid\": \"%s\"}", uid), 0)
	}
	p.patches = p.makeStub()

	if p.BeforePrepare != nil {
		p.BeforePrepare()
	}
	if p.Test != nil {
		p.Test(plat)
		return false
	}
	if p.Prefetch {
		p.prefetch(plat)
	}
	sort.Slice(p.IndexBlockNumbers, func(i, j int) bool {
		return p.IndexBlockNumbers[i] < p.IndexBlockNumbers[j]
	})

	if len(p.IndexBlockNumbers) == 0 && len(p.prefetchedBlocks) > 0 {
		for _, blk := range p.prefetchedBlocks {
			p.IndexBlockNumbers = append(p.IndexBlockNumbers, blk.Number)
		}
	}
	if len(p.PendingTxs) == 0 && len(p.prefetchedTransctions) > 0 {
		for _, tx := range p.prefetchedTransctions {
			p.PendingTxs = append(p.PendingTxs, &pb.TransactionReq{
				ChainName:       p.ChainName,
				TransactionHash: tx.Hash,
				Status:          biz.PENDING,
				FromAddress:     tx.FromAddress,
				ToAddress:       tx.ToAddress,
				Amount:          tx.Value,
			})
		}
	}

	for _, tx := range p.PendingTxs {
		_, err := usecase.CreateRecordFromWallet(context.TODO(), tx)
		if err != nil {
			panic(err)
		}
	}
	if p.AfterPrepare != nil {
		p.AfterPrepare()
	}
	return true
}

func (p *Preparation) makeStub() []*gomonkey.Patches {
	patches := StubNodeProxyFuncs(p.ChainName, p.Tokens, p.NFTs, p.TokenPrices)
	if len(p.IndexBlockNumbers) > 0 || len(p.PendingTxs) > 0 {
		patches = append(patches, StubBackgrounds()...)
	}
	return patches
}

// Prefetch prefetch txs & blocks.
func (p *Preparation) prefetch(bootstrap *platform.Bootstrap) (err error) {
	errs := make([]error, 0, len(p.IndexBlockNumbers)+len(p.PendingTxs))
	wg := &sync.WaitGroup{}
	var lock sync.Mutex

	for _, n := range p.IndexBlockNumbers {
		var block *chain.Block
		if !p.SourceFromChain {
			if err := p.loadLocal(n, &block); err == nil {
				if p.RawBlockType != nil {
					block.Raw = p.convertType(block.Raw, p.RawBlockType)
				}
				for _, chainTx := range block.Transactions {
					chainTx.Raw = p.convertType(chainTx.Raw, p.RawTxType)
				}
				p.prefetchedBlocks = append(p.prefetchedBlocks, block)
				continue
			}
		}
		wg.Add(1)
		go func(height uint64) {
			defer wg.Done()
			var block *chain.Block
			var err error
			err = bootstrap.Spider.WithRetry(func(client chain.Clienter) error {
				block, err = client.GetBlock(height)
				return common.Retry(err)
			})
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}

			if err := p.storeLocal(height, block); err != nil {
				log.Info("STORE LOCAL FAILED", zap.Error(err))
			}
			p.prefetchedBlocks = append(p.prefetchedBlocks, block)
		}(n)
	}

	for _, tx := range p.PendingTxs {
		var chainTX *chain.Transaction
		if err := p.loadLocal(tx.TransactionHash, &tx); err == nil {
			chainTX.Raw = p.convertType(chainTX.Raw, p.RawTxType)
			p.prefetchedTransctions = append(p.prefetchedTransctions, chainTX)
			continue
		}
		wg.Add(1)
		go func(txHash string) {
			defer wg.Done()
			var tx *chain.Transaction
			var err error
			err = bootstrap.Spider.WithRetry(func(client chain.Clienter) error {
				tx, err = client.GetTxByHash(txHash)
				return common.Retry(err)
			})
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			p.prefetchedTransctions = append(p.prefetchedTransctions, tx)
			if err := p.storeLocal(tx.Hash, tx); err != nil {
				log.Info("STORE LOCAL FAILED", zap.Error(err))
			}
		}(tx.TransactionHash)
	}
	wg.Wait()
	return nil
}

func (p *Preparation) storeLocal(k, v interface{}) error {
	storePath := p.storePath(k)
	log.Info("WRITING PREFETCHED CONTENT", zap.String("storePath", storePath))
	f, err := os.Create(storePath)
	if err != nil {
		return err
	}
	byts, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = f.Write(byts)
	return err
}

func (p *Preparation) loadLocal(k, v interface{}) error {
	byts, err := os.ReadFile(p.storePath(k))
	if err != nil {
		return err
	}
	return json.Unmarshal(byts, v)
}

func (p *Preparation) storePath(k interface{}) string {
	dir, _ := os.Getwd()
	dir = path.Join(dir, ".prefetched")
	os.Mkdir(dir, 0755)
	var kind string
	switch k.(type) {
	case uint64:
		kind = "block"
	case string:
		kind = "tx"
	}
	name := fmt.Sprintf("%s_%s_%v.json", p.ChainName, kind, k)
	return path.Join(dir, name)
}

func (p *Preparation) convertType(v interface{}, tp reflect.Type) interface{} {
	dst := reflect.New(tp)
	byts, err := json.Marshal(v)
	if err != nil {
		log.Error("CONVERT FAILED", zap.Error(err))
		return nil
	}
	err = json.Unmarshal(byts, dst.Interface())
	if err != nil {
		log.Error("CONVERT FAILED", zap.Error(err))
		return nil
	}
	return dst.Elem().Interface()
}

type Runner struct {
	usecase      *biz.TransactionUsecase
	preparation  Preparation
	bootstrap    *platform.Bootstrap
	cancellation *Cancellation
	mr           data.MigrationRepo
	db           *gorm.DB
}

func NewRunner(bs platform.Server, preparation Preparation, usecase *biz.TransactionUsecase, cancellation *Cancellation, mr data.MigrationRepo, db *gorm.DB) *Runner {
	bootstrap := bs.Find(preparation.ChainName)
	if bootstrap == nil {
		fmt.Printf("Please check if %s has been configured", preparation.ChainName)
	}
	return &Runner{
		preparation:  preparation,
		bootstrap:    bootstrap,
		usecase:      usecase,
		cancellation: cancellation,
		mr:           mr,
		db:           db,
	}
}

func (r *Runner) Start(ctx context.Context) error {
	if r.bootstrap != nil {
		biz.DynamicCreateTable(r.mr, r.db, biz.GetTableName(r.bootstrap.ChainName), r.bootstrap.Conf.Type)
	}

	if !r.preparation.prepare(r.usecase, r.bootstrap) {
		r.cancellation.Cancel(&r.preparation)
	}

	if r.bootstrap == nil {
		return nil
	}

	r.bootstrap.Spider.SetPrefetched(r.preparation.prefetchedBlocks, r.preparation.prefetchedTransctions)

	blockHandler := r.bootstrap.Platform.CreateBlockHandler(time.Minute)
	r.bootstrap.Spider.SetStateStore(&stateStore{
		preparation: r.preparation,
		inner:       r.bootstrap.Platform.CreateStateStore(),
	})
	handler := &blockHandlerWrapper{
		inner:       blockHandler,
		preparation: r.preparation,
		cancel: func() {
			r.cancellation.Cancel(&r.preparation)
		},
	}

	if len(r.preparation.PendingTxs) > 0 {
		r.bootstrap.Spider.SealPendingTransactions(handler)
	}

	r.bootstrap.Spider.StartIndexBlockWithContext(
		r.cancellation.Context,
		handler,
		int(r.bootstrap.Conf.GetSafelyConcurrentBlockDelta()),
		// int(r.bootstrap.Conf.GetMaxConcurrency()),
		1, // disable concurrency.
	)
	if r.preparation.AssertErrs != nil {
		errs := make([]error, 0, len(handler.errs))
		for _, e := range handler.errs {
			if errors.Is(e, chain.ErrSlowBlockHandling) {
				continue
			}
			errs = append(errs, e)
		}
		r.preparation.AssertErrs(errs)
	}

	if r.preparation.Assert != nil {
		r.preparation.Assert()
	}
	return nil
}

func (r *Runner) Stop(ctx context.Context) error {
	r.cancellation.Cancel(&r.preparation)

	for _, p := range r.preparation.patches {
		p.Reset()
	}
	return nil
}

type blockHandlerWrapper struct {
	inner         chain.BlockHandler
	preparation   Preparation
	indexedBlocks []uint64
	cancel        func()
	errs          []error
}

// BlockInterval implements chain.BlockHandler
func (w *blockHandlerWrapper) BlockInterval() time.Duration {
	return w.inner.BlockInterval()
}

// BlockMayFork implements chain.BlockHandler
func (w *blockHandlerWrapper) BlockMayFork() bool {
	return w.inner.BlockMayFork()
}

// CreateTxHandler implements chain.BlockHandler
func (w *blockHandlerWrapper) CreateTxHandler(client chain.Clienter, tx *chain.Transaction) (chain.TxHandler, error) {
	return w.inner.CreateTxHandler(client, tx)
}

// OnError implements chain.BlockHandler
func (w *blockHandlerWrapper) OnError(err error, optHeight ...chain.HeightInfo) (incrHeight bool) {
	defer w.cancel()
	w.errs = append(w.errs, err)
	return w.inner.OnError(err, optHeight...)
}

// OnForkedBlock implements chain.BlockHandler
func (w *blockHandlerWrapper) OnForkedBlock(client chain.Clienter, block *chain.Block) error {
	return w.inner.OnForkedBlock(client, block)
}

// OnNewBlock implements chain.BlockHandler
func (w *blockHandlerWrapper) OnNewBlock(client chain.Clienter, chainHeight uint64, block *chain.Block) (chain.TxHandler, error) {
	w.onBlockDone(block.Number)
	return w.inner.OnNewBlock(client, chainHeight, block)
}

func (w *blockHandlerWrapper) onBlockDone(height uint64) {
	for _, b := range w.preparation.IndexBlockNumbers {
		if b == height {
			w.indexedBlocks = append(w.indexedBlocks, b)
		}
	}
	if len(w.indexedBlocks) == len(w.preparation.IndexBlockNumbers) {
		log.Info("CANCELING")
		w.cancel()
	}
}

// WrapsError implements chain.BlockHandler
func (w *blockHandlerWrapper) WrapsError(client chain.Clienter, err error) error {
	return w.inner.WrapsError(client, err)
}

func (w *blockHandlerWrapper) IsDroppedTx(txByHash *chain.Transaction, err error) bool {
	return w.inner.IsDroppedTx(txByHash, err)
}

type stateStore struct {
	preparation Preparation
	inner       chain.StateStore
	blockIdx    int
}

// LoadBlockHash implements chain.StateStore
func (s *stateStore) LoadBlockHash(height uint64) (string, error) {
	return s.inner.LoadBlockHash(height)
}

// LoadHeight implements chain.StateStore
func (s *stateStore) LoadHeight() (height uint64, err error) {
	if s.blockIdx >= len(s.preparation.IndexBlockNumbers) {
		return 0, nil
	}
	return s.preparation.IndexBlockNumbers[s.blockIdx], nil
}

// LoadPendingTxs implements chain.StateStore
func (s *stateStore) LoadPendingTxs() (txs []*chain.Transaction, err error) {
	return s.inner.LoadPendingTxs()
}

// StoreBlockHash implements chain.StateStore
func (s *stateStore) StoreBlockHash(height uint64, blockHash string) error {
	return s.inner.StoreBlockHash(height, blockHash)
}

// StoreHeight implements chain.StateStore
func (s *stateStore) StoreHeight(height uint64) error {
	s.blockIdx++
	return nil
}

// StoreNodeHeight implements chain.StateStore
func (s *stateStore) StoreNodeHeight(height uint64) error {
	return s.inner.StoreNodeHeight(height)
}
