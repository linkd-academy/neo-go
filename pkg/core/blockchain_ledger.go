package core

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/mempool"
	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/statesync"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"go.uber.org/zap"
)

// Various errors that could be returns upon header verification.
var (
	ErrHdrHashMismatch     = errors.New("previous header hash doesn't match")
	ErrHdrIndexMismatch    = errors.New("previous header index doesn't match")
	ErrHdrInvalidTimestamp = errors.New("block is not newer than the previous one")
	ErrHdrStateRootSetting = errors.New("state root setting mismatch")
	ErrHdrInvalidStateRoot = errors.New("state root for previous block is invalid")
)

func (bc *Blockchain) storeBlock(block *block.Block, txpool *mempool.Pool) error {
	var (
		cache          = bc.dao.GetPrivate()
		aerCache       = bc.dao.GetPrivate()
		appExecResults = make([]*state.AppExecResult, 0, 2+len(block.Transactions))
		aerchan        = make(chan *state.AppExecResult, len(block.Transactions)/8) // Tested 8 and 4 with no practical difference, but feel free to test more and tune.
		aerdone        = make(chan error)
	)
	go func() {
		var (
			kvcache      = aerCache
			err          error
			txCnt        int
			baer1, baer2 *state.AppExecResult
			transCache   = make(map[util.Uint160]transferData)
		)
		kvcache.StoreAsCurrentBlock(block)
		if bc.config.Ledger.RemoveUntraceableBlocks {
			var start, stop uint32
			if bc.config.P2PStateExchangeExtensions {
				// remove batch of old blocks starting from P2-MaxTraceableBlocks-StateSyncInterval up to P2-MaxTraceableBlocks
				if block.Index >= 2*uint32(bc.config.StateSyncInterval) &&
					block.Index >= uint32(bc.config.StateSyncInterval)+bc.config.MaxTraceableBlocks && // check this in case if MaxTraceableBlocks>StateSyncInterval
					int(block.Index)%bc.config.StateSyncInterval == 0 {
					stop = block.Index - uint32(bc.config.StateSyncInterval) - bc.config.MaxTraceableBlocks
					start = stop - min(stop, uint32(bc.config.StateSyncInterval))
				}
			} else if block.Index > bc.config.MaxTraceableBlocks {
				start = block.Index - bc.config.MaxTraceableBlocks // is at least 1
				stop = start + 1
			}
			for index := start; index < stop; index++ {
				err := kvcache.DeleteBlock(bc.GetHeaderHash(index))
				if err != nil {
					bc.log.Warn("error while removing old block",
						zap.Uint32("index", index),
						zap.Error(err))
				}
			}
		}
		for aer := range aerchan {
			if aer.Container == block.Hash() {
				if baer1 == nil {
					baer1 = aer
				} else {
					baer2 = aer
				}
			} else {
				err = kvcache.StoreAsTransaction(block.Transactions[txCnt], block.Index, aer)
				txCnt++
			}
			if err != nil {
				err = fmt.Errorf("failed to store exec result: %w", err)
				break
			}
			if aer.Execution.VMState == vmstate.Halt {
				for j := range aer.Execution.Events {
					bc.handleNotification(&aer.Execution.Events[j], kvcache, transCache, block, aer.Container)
				}
			}
		}
		if err != nil {
			aerdone <- err
			return
		}
		if err := kvcache.StoreAsBlock(block, baer1, baer2); err != nil {
			aerdone <- err
			return
		}
		for acc, trData := range transCache {
			err = kvcache.PutTokenTransferInfo(acc, &trData.Info)
			if err != nil {
				aerdone <- err
				return
			}
			if !trData.Info.NewNEP11Batch {
				kvcache.PutTokenTransferLog(acc, trData.Info.NextNEP11NewestTimestamp, trData.Info.NextNEP11Batch, true, &trData.Log11)
			}
			if !trData.Info.NewNEP17Batch {
				kvcache.PutTokenTransferLog(acc, trData.Info.NextNEP17NewestTimestamp, trData.Info.NextNEP17Batch, false, &trData.Log17)
			}
		}
		close(aerdone)
	}()
	_ = cache.GetItemCtx() // Prime serialization context cache (it'll be reused by upper layer DAOs).
	aer, v, err := bc.runPersist(bc.contracts.GetPersistScript(), block, cache, trigger.OnPersist, nil)
	if err != nil {
		// Release goroutines, don't care about errors, we already have one.
		close(aerchan)
		<-aerdone
		return fmt.Errorf("onPersist failed: %w", err)
	}
	appExecResults = append(appExecResults, aer)
	aerchan <- aer

	for _, tx := range block.Transactions {
		systemInterop := bc.newInteropContext(trigger.Application, cache, block, tx)
		systemInterop.ReuseVM(v)
		v.LoadScriptWithFlags(tx.Script, callflag.All)
		v.GasLimit = tx.SystemFee

		err := systemInterop.Exec()
		var faultException string
		if !v.HasFailed() {
			_, err := systemInterop.DAO.Persist()
			if err != nil {
				// Release goroutines, don't care about errors, we already have one.
				close(aerchan)
				<-aerdone
				return fmt.Errorf("failed to persist invocation results: %w", err)
			}
		} else {
			bc.log.Warn("contract invocation failed",
				zap.String("tx", tx.Hash().StringLE()),
				zap.Uint32("block", block.Index),
				zap.Error(err))
			faultException = err.Error()
		}
		aer := &state.AppExecResult{
			Container: tx.Hash(),
			Execution: state.Execution{
				Trigger:        trigger.Application,
				VMState:        v.State(),
				GasConsumed:    v.GasConsumed(),
				Stack:          v.Estack().ToArray(),
				Events:         systemInterop.Notifications,
				FaultException: faultException,
			},
		}
		appExecResults = append(appExecResults, aer)
		aerchan <- aer
	}

	aer, _, err = bc.runPersist(bc.contracts.GetPostPersistScript(), block, cache, trigger.PostPersist, v)
	if err != nil {
		// Release goroutines, don't care about errors, we already have one.
		close(aerchan)
		<-aerdone
		return fmt.Errorf("postPersist failed: %w", err)
	}
	appExecResults = append(appExecResults, aer)
	aerchan <- aer
	close(aerchan)
	b := mpt.MapToMPTBatch(cache.Store.GetStorageChanges())
	mpt, sr, err := bc.stateRoot.AddMPTBatch(block.Index, b, cache.Store)
	if err != nil {
		// Release goroutines, don't care about errors, we already have one.
		<-aerdone
		// Here MPT can be left in a half-applied state.
		// However if this error occurs, this is a bug somewhere in code
		// because changes applied are the ones from HALTed transactions.
		return fmt.Errorf("error while trying to apply MPT changes: %w", err)
	}
	if bc.config.StateRootInHeader && bc.HeaderHeight() > sr.Index {
		h, err := bc.GetHeader(bc.GetHeaderHash(sr.Index + 1))
		if err != nil {
			err = fmt.Errorf("failed to get next header: %w", err)
		} else if h.PrevStateRoot != sr.Root {
			err = fmt.Errorf("local stateroot and next header's PrevStateRoot mismatch: %s vs %s", sr.Root.StringBE(), h.PrevStateRoot.StringBE())
		}
		if err != nil {
			// Release goroutines, don't care about errors, we already have one.
			<-aerdone
			return err
		}
	}

	if bc.config.Ledger.SaveStorageBatch {
		bc.lastBatch = cache.GetBatch()
	}
	// Every persist cycle we also compact our in-memory MPT. It's flushed
	// already in AddMPTBatch, so collapsing it is safe.
	persistedHeight := atomic.LoadUint32(&bc.persistedHeight)
	if persistedHeight == block.Index-1 {
		// 10 is good and roughly estimated to fit remaining trie into 1M of memory.
		mpt.Collapse(10)
	}

	aererr := <-aerdone
	if aererr != nil {
		return aererr
	}

	bc.lock.Lock()
	_, err = aerCache.Persist()
	if err != nil {
		bc.lock.Unlock()
		return err
	}
	_, err = cache.Persist()
	if err != nil {
		bc.lock.Unlock()
		return err
	}

	mpt.Store = bc.dao.Store
	bc.stateRoot.UpdateCurrentLocal(mpt, sr)
	bc.topBlock.Store(block)
	atomic.StoreUint32(&bc.blockHeight, block.Index)
	bc.memPool.RemoveStale(func(tx *transaction.Transaction) bool { return bc.IsTxStillRelevant(tx, txpool, false) }, bc)
	for _, f := range bc.postBlock {
		f(bc.IsTxStillRelevant, txpool, block)
	}
	if err := bc.updateExtensibleWhitelist(block.Index); err != nil {
		bc.lock.Unlock()
		return err
	}
	bc.lock.Unlock()

	updateBlockHeightMetric(block.Index)
	// Genesis block is stored when Blockchain is not yet running, so there
	// is no one to read this event. And it doesn't make much sense as event
	// anyway.
	if block.Index != 0 {
		bc.events <- bcEvent{block, appExecResults}
	}
	return nil
}


// AddBlock accepts successive block for the Blockchain, verifies it and
// stores internally. Eventually it will be persisted to the backing storage.
func (bc *Blockchain) AddBlock(block *block.Block) error {
	bc.addLock.Lock()
	defer bc.addLock.Unlock()

	var mp *mempool.Pool
	expectedHeight := bc.BlockHeight() + 1
	if expectedHeight != block.Index {
		return fmt.Errorf("expected %d, got %d: %w", expectedHeight, block.Index, ErrInvalidBlockIndex)
	}
	if bc.config.StateRootInHeader != block.StateRootEnabled {
		return fmt.Errorf("%w: %v != %v",
			ErrHdrStateRootSetting, bc.config.StateRootInHeader, block.StateRootEnabled)
	}

	if block.Index == bc.HeaderHeight()+1 {
		err := bc.addHeaders(!bc.config.SkipBlockVerification, &block.Header)
		if err != nil {
			return err
		}
	}
	if !bc.config.SkipBlockVerification {
		merkle := block.ComputeMerkleRoot()
		if !block.MerkleRoot.Equals(merkle) {
			return errors.New("invalid block: MerkleRoot mismatch")
		}
		mp = mempool.New(len(block.Transactions), 0, false, nil)
		for _, tx := range block.Transactions {
			var err error
			// Transactions are verified before adding them
			// into the pool, so there is no point in doing
			// it again even if we're verifying in-block transactions.
			if bc.memPool.ContainsKey(tx.Hash()) {
				err = mp.Add(tx, bc)
				if err == nil {
					continue
				}
			} else {
				err = bc.verifyAndPoolTx(tx, mp, bc)
			}
			if err != nil {
				if bc.config.VerifyTransactions {
					return fmt.Errorf("transaction %s failed to verify: %w", tx.Hash().StringLE(), err)
				}
				bc.log.Warn(fmt.Sprintf("transaction %s failed to verify: %s", tx.Hash().StringLE(), err))
			}
		}
	}
	return bc.storeBlock(block, mp)
}

// AddHeaders processes the given headers and add them to the
// HeaderHashList. It expects headers to be sorted by index.
func (bc *Blockchain) AddHeaders(headers ...*block.Header) error {
	return bc.addHeaders(!bc.config.SkipBlockVerification, headers...)
}

// addHeaders is an internal implementation of AddHeaders (`verify` parameter
// tells it to verify or not verify given headers).
func (bc *Blockchain) addHeaders(verify bool, headers ...*block.Header) error {
	var (
		start = time.Now()
		err   error
	)

	if len(headers) > 0 {
		var i int
		curHeight := bc.HeaderHeight()
		for i = range headers {
			if headers[i].Index > curHeight {
				break
			}
		}
		headers = headers[i:]
	}

	if len(headers) == 0 {
		return nil
	} else if verify {
		// Verify that the chain of the headers is consistent.
		var lastHeader *block.Header
		if lastHeader, err = bc.GetHeader(headers[0].PrevHash); err != nil {
			return fmt.Errorf("previous header was not found: %w", err)
		}
		for _, h := range headers {
			if err = bc.verifyHeader(h, lastHeader); err != nil {
				return err
			}
			lastHeader = h
		}
	}
	res := bc.HeaderHashes.addHeaders(headers...)
	if res == nil {
		bc.log.Debug("done processing headers",
			zap.Uint32("headerIndex", bc.HeaderHeight()),
			zap.Uint32("blockHeight", bc.BlockHeight()),
			zap.Duration("took", time.Since(start)))
	}
	return res
}

// GetStateRoot returns state root for the given height.
func (bc *Blockchain) GetStateRoot(height uint32) (*state.MPTRoot, error) {
	return bc.stateRoot.GetStateRoot(height)
}

// GetStateModule returns state root service instance.
func (bc *Blockchain) GetStateModule() StateRoot {
	return bc.stateRoot
}

// GetStateSyncModule returns new state sync service instance.
func (bc *Blockchain) GetStateSyncModule() *statesync.Module {
	return statesync.NewModule(bc, bc.stateRoot, bc.log, bc.dao, bc.jumpToState)
}

// storeBlock performs chain update using the block given, it executes all
// transactions with all appropriate side-effects and updates Blockchain state.
// This is the only way to change Blockchain state.
func (bc *Blockchain) updateExtensibleWhitelist(height uint32) error {
	updateCommittee := bc.config.ShouldUpdateCommitteeAt(height)
	stateVals, sh, err := bc.contracts.Designate.GetDesignatedByRole(bc.dao, noderoles.StateValidator, height)
	if err != nil {
		return err
	}

	if bc.extensible.Load() != nil && !updateCommittee && sh != height {
		return nil
	}

	newList := []util.Uint160{bc.contracts.NEO.GetCommitteeAddress(bc.dao)}
	nextVals := bc.contracts.NEO.GetNextBlockValidatorsInternal(bc.dao)
	script, err := smartcontract.CreateDefaultMultiSigRedeemScript(nextVals)
	if err != nil {
		return err
	}
	newList = append(newList, hash.Hash160(script))
	bc.updateExtensibleList(&newList, nextVals)

	if len(stateVals) > 0 {
		h, err := bc.contracts.Designate.GetLastDesignatedHash(bc.dao, noderoles.StateValidator)
		if err != nil {
			return err
		}
		newList = append(newList, h)
		bc.updateExtensibleList(&newList, stateVals)
	}

	slices.SortFunc(newList, util.Uint160.Compare)
	bc.extensible.Store(newList)
	return nil
}

func (bc *Blockchain) updateExtensibleList(s *[]util.Uint160, pubs keys.PublicKeys) {
	for _, pub := range pubs {
		*s = append(*s, pub.GetScriptHash())
	}
}

// IsExtensibleAllowed determines if script hash is allowed to send extensible payloads.
func (bc *Blockchain) IsExtensibleAllowed(u util.Uint160) bool {
	us := bc.extensible.Load().([]util.Uint160)
	_, ok := slices.BinarySearchFunc(us, u, util.Uint160.Compare)
	return ok
}

// GetStorageItem returns an item from storage.
func (bc *Blockchain) GetStorageItem(id int32, key []byte) state.StorageItem {
	return bc.dao.GetStorageItem(id, key)
}

func (bc *Blockchain) runPersist(script []byte, block *block.Block, cache *dao.Simple, trig trigger.Type, v *vm.VM) (*state.AppExecResult, *vm.VM, error) {
	systemInterop := bc.newInteropContext(trig, cache, block, nil)
	if v == nil {
		v = systemInterop.SpawnVM()
	} else {
		systemInterop.ReuseVM(v)
	}
	v.LoadScriptWithFlags(script, callflag.All)
	if err := systemInterop.Exec(); err != nil {
		return nil, v, fmt.Errorf("VM has failed: %w", err)
	} else if _, err := systemInterop.DAO.Persist(); err != nil {
		return nil, v, fmt.Errorf("can't save changes: %w", err)
	}
	return &state.AppExecResult{
		Container: block.Hash(), // application logs can be retrieved by block hash
		Execution: state.Execution{
			Trigger:     trig,
			VMState:     v.State(),
			GasConsumed: v.GasConsumed(),
			Stack:       v.Estack().ToArray(),
			Events:      systemInterop.Notifications,
		},
	}, v, nil
}

// GetTransaction returns a TX and its height by the given hash. The height is MaxUint32 if tx is in the mempool.
func (bc *Blockchain) GetTransaction(hash util.Uint256) (*transaction.Transaction, uint32, error) {
	if tx, ok := bc.memPool.TryGetValue(hash); ok {
		return tx, math.MaxUint32, nil // the height is not actually defined for memPool transaction.
	}
	return bc.dao.GetTransaction(hash)
}

// GetBlock returns a Block by the given hash.
func (bc *Blockchain) GetBlock(hash util.Uint256) (*block.Block, error) {
	topBlock := bc.topBlock.Load()
	if topBlock != nil {
		tb := topBlock.(*block.Block)
		if tb.Hash().Equals(hash) {
			return tb, nil
		}
	}

	block, err := bc.dao.GetBlock(hash)
	if err != nil {
		return nil, err
	}
	if !block.MerkleRoot.Equals(util.Uint256{}) && len(block.Transactions) == 0 {
		return nil, errors.New("only header is found")
	}
	for _, tx := range block.Transactions {
		stx, _, err := bc.dao.GetTransaction(tx.Hash())
		if err != nil {
			return nil, err
		}
		*tx = *stx
	}
	return block, nil
}

// GetHeader returns data block header identified with the given hash value.
func (bc *Blockchain) GetHeader(hash util.Uint256) (*block.Header, error) {
	topBlock := bc.topBlock.Load()
	if topBlock != nil {
		tb := topBlock.(*block.Block)
		if tb.Hash().Equals(hash) {
			return &tb.Header, nil
		}
	}
	block, err := bc.dao.GetBlock(hash)
	if err != nil {
		return nil, err
	}
	return &block.Header, nil
}

// HasBlock returns true if the blockchain contains the given
// block hash.
func (bc *Blockchain) HasBlock(hash util.Uint256) bool {
	if bc.HeaderHashes.haveRecentHash(hash, bc.BlockHeight()) {
		return true
	}

	if header, err := bc.GetHeader(hash); err == nil {
		return header.Index <= bc.BlockHeight()
	}
	return false
}

// CurrentBlockHash returns the highest processed block hash.
func (bc *Blockchain) CurrentBlockHash() util.Uint256 {
	topBlock := bc.topBlock.Load()
	if topBlock != nil {
		tb := topBlock.(*block.Block)
		return tb.Hash()
	}
	return bc.GetHeaderHash(bc.BlockHeight())
}

// BlockHeight returns the height/index of the highest block.
func (bc *Blockchain) BlockHeight() uint32 {
	return atomic.LoadUint32(&bc.blockHeight)
}

// GetDesignatedByRole returns a set of designated public keys for the given role
// relevant for the next block.
func (bc *Blockchain) GetDesignatedByRole(r noderoles.Role) (keys.PublicKeys, uint32, error) {
	// Retrieve designated nodes starting from the next block, because the current
	// block is already stored, thus, dependant services can't use PostPersist callback
	// to fetch relevant information at their start.
	res, h, err := bc.contracts.Designate.GetDesignatedByRole(bc.dao, r, bc.BlockHeight()+1)
	return res, h, err
}

// getCurrentHF returns the latest currently enabled hardfork. In case if no hardforks are enabled, the
// default config.Hardfork(0) value is returned.
func (bc *Blockchain) getCurrentHF() config.Hardfork {
	var (
		height  = bc.BlockHeight()
		current config.Hardfork
	)
	// Rely on the fact that hardforks list is continuous.
	for _, hf := range config.Hardforks {
		enableHeight, ok := bc.config.Hardforks[hf.String()]
		if !ok || height < enableHeight {
			break
		}
		current = hf
	}
	return current
}

// PoolTx verifies and tries to add given transaction into the mempool. If not
// given, the default mempool is used. Passing multiple pools is not supported.
func (bc *Blockchain) PoolTx(t *transaction.Transaction, pools ...*mempool.Pool) error {
	var pool = bc.memPool

	bc.lock.RLock()
	defer bc.lock.RUnlock()
	// Programmer error.
	if len(pools) > 1 {
		panic("too many pools given")
	}
	if len(pools) == 1 {
		pool = pools[0]
	}
	return bc.verifyAndPoolTx(t, pool, bc)
}

// PoolTxWithData verifies and tries to add given transaction with additional data into the mempool.
func (bc *Blockchain) PoolTxWithData(t *transaction.Transaction, data any, mp *mempool.Pool, feer mempool.Feer, verificationFunction func(tx *transaction.Transaction, data any) error) error {
	bc.lock.RLock()
	defer bc.lock.RUnlock()

	if verificationFunction != nil {
		err := verificationFunction(t, data)
		if err != nil {
			return err
		}
	}
	return bc.verifyAndPoolTx(t, mp, feer, data)
}


// persist flushes current in-memory Store contents to the persistent storage.
func (bc *Blockchain) persist(isSync bool) (time.Duration, error) {
	var (
		start     = time.Now()
		duration  time.Duration
		persisted int
		err       error
	)

	if isSync {
		persisted, err = bc.dao.PersistSync()
	} else {
		persisted, err = bc.dao.Persist()
	}
	if err != nil {
		return 0, err
	}
	if persisted > 0 {
		bHeight, err := bc.persistent.GetCurrentBlockHeight()
		if err != nil {
			return 0, err
		}
		oldHeight := atomic.SwapUint32(&bc.persistedHeight, bHeight)
		diff := bHeight - oldHeight

		storedHeaderHeight, _, err := bc.persistent.GetCurrentHeaderHeight()
		if err != nil {
			return 0, err
		}
		duration = time.Since(start)
		bc.log.Info("persisted to disk",
			zap.Uint32("blocks", diff),
			zap.Int("keys", persisted),
			zap.Uint32("headerHeight", storedHeaderHeight),
			zap.Uint32("blockHeight", bHeight),
			zap.Duration("took", duration))

		// update monitoring metrics.
		updatePersistedHeightMetric(bHeight)
	}

	return duration, nil
}


// LastBatch returns last persisted storage batch.
func (bc *Blockchain) LastBatch() *storage.MemBatch {
	return bc.lastBatch
}

// GetAppExecResults returns application execution results with the specified trigger by the given
// tx hash or block hash.
func (bc *Blockchain) GetAppExecResults(hash util.Uint256, trig trigger.Type) ([]state.AppExecResult, error) {
	return bc.dao.GetAppExecResults(hash, trig)
}

// SeekStorage performs seek operation over contract storage. Prefix is trimmed in the resulting pair's key.
func (bc *Blockchain) SeekStorage(id int32, prefix []byte, cont func(k, v []byte) bool) {
	bc.dao.Seek(id, storage.SeekRange{Prefix: prefix}, cont)
}

// GetConfig returns the config stored in the blockchain.
func (bc *Blockchain) GetConfig() config.Blockchain {
	return bc.config
}
