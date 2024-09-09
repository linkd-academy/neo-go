package core

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/mempool"
	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/statesync"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"go.uber.org/zap"
)

// jumpToState is an atomic operation that changes Blockchain state to the one
// specified by the state sync point p. All the data needed for the jump must be
// collected by the state sync module.
func (bc *Blockchain) jumpToState(p uint32) error {
	bc.addLock.Lock()
	bc.lock.Lock()
	defer bc.lock.Unlock()
	defer bc.addLock.Unlock()

	return bc.jumpToStateInternal(p, none)
}

// jumpToStateInternal is an internal representation of jumpToState callback that
// changes Blockchain state to the one specified by state sync point p and state
// jump stage. All the data needed for the jump must be in the DB, otherwise an
// error is returned. It is not protected by mutex.
func (bc *Blockchain) jumpToStateInternal(p uint32, stage stateChangeStage) error {
	if p >= bc.HeaderHeight() {
		return fmt.Errorf("invalid state sync point %d: headerHeignt is %d", p, bc.HeaderHeight())
	}

	bc.log.Info("jumping to state sync point", zap.Uint32("state sync point", p))

	jumpStageKey := []byte{byte(storage.SYSStateChangeStage)}
	switch stage {
	case none:
		bc.dao.Store.Put(jumpStageKey, []byte{byte(stateJumpStarted)})
		fallthrough
	case stateJumpStarted:
		newPrefix := statesync.TemporaryPrefix(bc.dao.Version.StoragePrefix)
		v, err := bc.dao.GetVersion()
		if err != nil {
			return fmt.Errorf("failed to get dao.Version: %w", err)
		}
		v.StoragePrefix = newPrefix
		bc.dao.PutVersion(v)
		bc.persistent.Version = v

		bc.dao.Store.Put(jumpStageKey, []byte{byte(newStorageItemsAdded)})

		fallthrough
	case newStorageItemsAdded:
		cache := bc.dao.GetPrivate()
		prefix := statesync.TemporaryPrefix(bc.dao.Version.StoragePrefix)
		bc.dao.Store.Seek(storage.SeekRange{Prefix: []byte{byte(prefix)}}, func(k, _ []byte) bool {
			// #1468, but don't need to copy here, because it is done by Store.
			cache.Store.Delete(k)
			return true
		})

		// After current state is updated, we need to remove outdated state-related data if so.
		// The only outdated data we might have is genesis-related data, so check it.
		if p-bc.config.MaxTraceableBlocks > 0 {
			err := cache.DeleteBlock(bc.GetHeaderHash(0))
			if err != nil {
				return fmt.Errorf("failed to remove outdated state data for the genesis block: %w", err)
			}
			prefixes := []byte{byte(storage.STNEP11Transfers), byte(storage.STNEP17Transfers), byte(storage.STTokenTransferInfo)}
			for i := range prefixes {
				cache.Store.Seek(storage.SeekRange{Prefix: prefixes[i : i+1]}, func(k, v []byte) bool {
					cache.Store.Delete(k)
					return true
				})
			}
		}
		// Update SYS-prefixed info.
		block, err := bc.dao.GetBlock(bc.GetHeaderHash(p))
		if err != nil {
			return fmt.Errorf("failed to get current block: %w", err)
		}
		cache.StoreAsCurrentBlock(block)
		cache.Store.Put(jumpStageKey, []byte{byte(staleBlocksRemoved)})
		_, err = cache.Persist()
		if err != nil {
			return fmt.Errorf("failed to persist old items removal: %w", err)
		}
	case staleBlocksRemoved:
		// there's nothing to do after that, so just continue with common operations
		// and remove state jump stage in the end.
	default:
		return fmt.Errorf("unknown state jump stage: %d", stage)
	}
	block, err := bc.dao.GetBlock(bc.GetHeaderHash(p + 1))
	if err != nil {
		return fmt.Errorf("failed to get block to init MPT: %w", err)
	}
	bc.stateRoot.JumpToState(&state.MPTRoot{
		Index: p,
		Root:  block.PrevStateRoot,
	})

	bc.dao.Store.Delete(jumpStageKey)

	err = bc.resetRAMState(p, false)
	if err != nil {
		return fmt.Errorf("failed to update in-memory blockchain data: %w", err)
	}
	return nil
}

// resetRAMState resets in-memory cached info.
func (bc *Blockchain) resetRAMState(height uint32, resetHeaders bool) error {
	if resetHeaders {
		err := bc.HeaderHashes.init(bc.dao)
		if err != nil {
			return err
		}
	}
	block, err := bc.dao.GetBlock(bc.GetHeaderHash(height))
	if err != nil {
		return fmt.Errorf("failed to get current block: %w", err)
	}
	bc.topBlock.Store(block)
	atomic.StoreUint32(&bc.blockHeight, height)
	atomic.StoreUint32(&bc.persistedHeight, height)

	err = bc.initializeNativeCache(block.Index, bc.dao)
	if err != nil {
		return fmt.Errorf("failed to initialize natives cache: %w", err)
	}

	if err := bc.updateExtensibleWhitelist(height); err != nil {
		return fmt.Errorf("failed to update extensible whitelist: %w", err)
	}

	updateBlockHeightMetric(height)
	updatePersistedHeightMetric(height)
	updateHeaderHeightMetric(bc.HeaderHeight())
	return nil
}

// Reset resets chain state to the specified height if possible. This method
// performs direct DB changes and can be called on non-running Blockchain only.
func (bc *Blockchain) Reset(height uint32) error {
	if bc.isRunning.Load().(bool) {
		return errors.New("can't reset state of the running blockchain")
	}
	bc.dao.PutStateSyncPoint(height)
	return bc.resetStateInternal(height, none)
}

func (bc *Blockchain) resetStateInternal(height uint32, stage stateChangeStage) error {
	// Cache isn't yet initialized, so retrieve block height right from DAO.
	currHeight, err := bc.dao.GetCurrentBlockHeight()
	if err != nil {
		return fmt.Errorf("failed to retrieve current block height: %w", err)
	}
	// Headers are already initialized by this moment, thus may use chain's API.
	hHeight := bc.HeaderHeight()
	// State reset may already be started by this moment, so perform these checks only if it wasn't.
	if stage == none {
		if height > currHeight {
			return fmt.Errorf("current block height is %d, can't reset state to height %d", currHeight, height)
		}
		if height == currHeight && hHeight == currHeight {
			bc.log.Info("chain is at the proper state", zap.Uint32("height", height))
			return nil
		}
		if bc.config.Ledger.KeepOnlyLatestState {
			return fmt.Errorf("KeepOnlyLatestState is enabled, state for height %d is outdated and removed from the storage", height)
		}
		if bc.config.Ledger.RemoveUntraceableBlocks && currHeight >= bc.config.MaxTraceableBlocks {
			return fmt.Errorf("RemoveUntraceableBlocks is enabled, a necessary batch of traceable blocks has already been removed")
		}
	}

	// Retrieve necessary state before the DB modification.
	b, err := bc.GetBlock(bc.GetHeaderHash(height))
	if err != nil {
		return fmt.Errorf("failed to retrieve block %d: %w", height, err)
	}
	sr, err := bc.stateRoot.GetStateRoot(height)
	if err != nil {
		return fmt.Errorf("failed to retrieve stateroot for height %d: %w", height, err)
	}
	v := bc.dao.Version
	// dao is MemCachedStore over DB, we use dao directly to persist cached changes
	// right to the underlying DB.
	cache := bc.dao
	// upperCache is a private MemCachedStore over cache. During each of the state
	// sync stages we put the data inside the upperCache; in the end of each stage
	// we persist changes from upperCache to cache. Changes from cache are persisted
	// directly to the underlying persistent storage (boltDB, levelDB, etc.).
	// upperCache/cache segregation is needed to keep the DB state clean and to
	// persist data from different stages separately.
	upperCache := cache.GetPrivate()

	bc.log.Info("initializing state reset", zap.Uint32("target height", height))
	start := time.Now()
	p := start

	// Start batch persisting routine, it will be used for blocks/txs/AERs/storage items batches persist.
	type postPersist func(persistedKeys int, err error) error
	var (
		persistCh       = make(chan postPersist)
		persistToExitCh = make(chan struct{})
	)
	go func() {
		for {
			f, ok := <-persistCh
			if !ok {
				break
			}
			persistErr := f(cache.Persist())
			if persistErr != nil {
				bc.log.Fatal("persist failed", zap.Error(persistErr))
				panic(persistErr)
			}
		}
		close(persistToExitCh)
	}()
	defer func() {
		close(persistCh)
		<-persistToExitCh
		bc.log.Info("reset finished successfully", zap.Duration("took", time.Since(start)))
	}()

	resetStageKey := []byte{byte(storage.SYSStateChangeStage)}
	switch stage {
	case none:
		upperCache.Store.Put(resetStageKey, []byte{stateResetBit | byte(stateJumpStarted)})
		// Technically, there's no difference between Persist() and PersistSync() for the private
		// MemCached storage, but we'd better use the sync version in case of some further code changes.
		_, uerr := upperCache.PersistSync()
		if uerr != nil {
			panic(uerr)
		}
		upperCache = cache.GetPrivate()
		persistCh <- func(persistedKeys int, err error) error {
			if err != nil {
				return fmt.Errorf("failed to persist state reset start marker to the DB: %w", err)
			}
			return nil
		}
		fallthrough
	case stateJumpStarted:
		bc.log.Debug("trying to reset blocks, transactions and AERs")
		// Remove blocks/transactions/aers from currHeight down to height (not including height itself).
		// Keep headers for now, they'll be removed later. It's hard to handle the whole set of changes in
		// one stage, so persist periodically.
		const persistBatchSize = 100 * headerBatchCount // count blocks only, should be enough to avoid OOM killer even for large blocks
		var (
			pBlocksStart        = p
			blocksCnt, batchCnt int
			keysCnt             = new(int)
		)
		for i := height + 1; i <= currHeight; i++ {
			err := upperCache.DeleteBlock(bc.GetHeaderHash(i))
			if err != nil {
				return fmt.Errorf("error while removing block %d: %w", i, err)
			}
			blocksCnt++
			if blocksCnt == persistBatchSize {
				blocksCnt = 0
				batchCnt++
				bc.log.Info("intermediate batch of removed blocks, transactions and AERs is collected",
					zap.Int("batch", batchCnt),
					zap.Duration("took", time.Since(p)))

				persistStart := time.Now()
				persistBatch := batchCnt
				_, uerr := upperCache.PersistSync()
				if uerr != nil {
					panic(uerr)
				}
				upperCache = cache.GetPrivate()
				persistCh <- func(persistedKeys int, err error) error {
					if err != nil {
						return fmt.Errorf("failed to persist intermediate batch of removed blocks, transactions and AERs: %w", err)
					}
					*keysCnt += persistedKeys
					bc.log.Debug("intermediate batch of removed blocks, transactions and AERs is persisted",
						zap.Int("batch", persistBatch),
						zap.Duration("took", time.Since(persistStart)),
						zap.Int("keys", persistedKeys))
					return nil
				}
				p = time.Now()
			}
		}
		upperCache.Store.Put(resetStageKey, []byte{stateResetBit | byte(staleBlocksRemoved)})
		batchCnt++
		bc.log.Info("last batch of removed blocks, transactions and AERs is collected",
			zap.Int("batch", batchCnt),
			zap.Duration("took", time.Since(p)))
		bc.log.Info("blocks, transactions ans AERs are reset", zap.Duration("took", time.Since(pBlocksStart)))

		persistStart := time.Now()
		persistBatch := batchCnt
		_, uerr := upperCache.PersistSync()
		if uerr != nil {
			panic(uerr)
		}
		upperCache = cache.GetPrivate()
		persistCh <- func(persistedKeys int, err error) error {
			if err != nil {
				return fmt.Errorf("failed to persist last batch of removed blocks, transactions ans AERs: %w", err)
			}
			*keysCnt += persistedKeys
			bc.log.Debug("last batch of removed blocks, transactions and AERs is persisted",
				zap.Int("batch", persistBatch),
				zap.Duration("took", time.Since(persistStart)),
				zap.Int("keys", persistedKeys))
			return nil
		}
		p = time.Now()
		fallthrough
	case staleBlocksRemoved:
		// Completely remove contract IDs to update them later.
		bc.log.Debug("trying to reset contract storage items")
		pStorageStart := p

		p = time.Now()
		var mode = mpt.ModeAll
		if bc.config.Ledger.RemoveUntraceableBlocks {
			mode |= mpt.ModeGCFlag
		}
		trieStore := mpt.NewTrieStore(sr.Root, mode, upperCache.Store)
		oldStoragePrefix := v.StoragePrefix
		newStoragePrefix := statesync.TemporaryPrefix(oldStoragePrefix)

		const persistBatchSize = 200000
		var cnt, storageItmsCnt, batchCnt int
		trieStore.Seek(storage.SeekRange{Prefix: []byte{byte(oldStoragePrefix)}}, func(k, v []byte) bool {
			if cnt >= persistBatchSize {
				cnt = 0
				batchCnt++
				bc.log.Info("intermediate batch of contract storage items and IDs is collected",
					zap.Int("batch", batchCnt),
					zap.Duration("took", time.Since(p)))

				persistStart := time.Now()
				persistBatch := batchCnt
				_, uerr := upperCache.PersistSync()
				if uerr != nil {
					panic(uerr)
				}
				upperCache = cache.GetPrivate()
				persistCh <- func(persistedKeys int, err error) error {
					if err != nil {
						return fmt.Errorf("failed to persist intermediate batch of contract storage items: %w", err)
					}
					bc.log.Debug("intermediate batch of contract storage items is persisted",
						zap.Int("batch", persistBatch),
						zap.Duration("took", time.Since(persistStart)),
						zap.Int("keys", persistedKeys))
					return nil
				}
				p = time.Now()
			}
			// May safely omit KV copying.
			k[0] = byte(newStoragePrefix)
			upperCache.Store.Put(k, v)
			cnt++
			storageItmsCnt++

			return true
		})
		trieStore.Close()

		upperCache.Store.Put(resetStageKey, []byte{stateResetBit | byte(newStorageItemsAdded)})
		batchCnt++
		persistBatch := batchCnt
		bc.log.Info("last batch of contract storage items is collected", zap.Int("batch", batchCnt), zap.Duration("took", time.Since(p)))
		bc.log.Info("contract storage items are reset", zap.Duration("took", time.Since(pStorageStart)),
			zap.Int("keys", storageItmsCnt))

		lastStart := time.Now()
		_, uerr := upperCache.PersistSync()
		if uerr != nil {
			panic(uerr)
		}
		upperCache = cache.GetPrivate()
		persistCh <- func(persistedKeys int, err error) error {
			if err != nil {
				return fmt.Errorf("failed to persist contract storage items and IDs changes to the DB: %w", err)
			}
			bc.log.Debug("last batch of contract storage items and IDs is persisted", zap.Int("batch", persistBatch), zap.Duration("took", time.Since(lastStart)), zap.Int("keys", persistedKeys))
			return nil
		}
		p = time.Now()
		fallthrough
	case newStorageItemsAdded:
		// Reset SYS-prefixed and IX-prefixed information.
		bc.log.Debug("trying to reset headers information")
		for i := height + 1; i <= hHeight; i++ {
			upperCache.PurgeHeader(bc.GetHeaderHash(i))
		}
		upperCache.DeleteHeaderHashes(height+1, headerBatchCount)
		upperCache.StoreAsCurrentBlock(b)
		upperCache.PutCurrentHeader(b.Hash(), height)
		v.StoragePrefix = statesync.TemporaryPrefix(v.StoragePrefix)
		upperCache.PutVersion(v)
		// It's important to manually change the cache's Version at this stage, so that native cache
		// can be properly initialized (with the correct contract storage data prefix) at the final
		// stage of the state reset. At the same time, DB's SYSVersion-prefixed data will be persisted
		// from upperCache to cache in a standard way (several lines below).
		cache.Version = v
		bc.persistent.Version = v

		upperCache.Store.Put(resetStageKey, []byte{stateResetBit | byte(headersReset)})
		bc.log.Info("headers information is reset", zap.Duration("took", time.Since(p)))

		persistStart := time.Now()
		_, uerr := upperCache.PersistSync()
		if uerr != nil {
			panic(uerr)
		}
		upperCache = cache.GetPrivate()
		persistCh <- func(persistedKeys int, err error) error {
			if err != nil {
				return fmt.Errorf("failed to persist headers changes to the DB: %w", err)
			}
			bc.log.Debug("headers information is persisted", zap.Duration("took", time.Since(persistStart)), zap.Int("keys", persistedKeys))
			return nil
		}
		p = time.Now()
		fallthrough
	case headersReset:
		// Reset MPT.
		bc.log.Debug("trying to reset state root information and NEP transfers")
		err = bc.stateRoot.ResetState(height, upperCache.Store)
		if err != nil {
			return fmt.Errorf("failed to rollback MPT state: %w", err)
		}

		// Reset transfers.
		err = bc.resetTransfers(upperCache, height)
		if err != nil {
			return fmt.Errorf("failed to strip transfer log / transfer info: %w", err)
		}

		upperCache.Store.Put(resetStageKey, []byte{stateResetBit | byte(transfersReset)})
		bc.log.Info("state root information and NEP transfers are reset", zap.Duration("took", time.Since(p)))

		persistStart := time.Now()
		_, uerr := upperCache.PersistSync()
		if uerr != nil {
			panic(uerr)
		}
		upperCache = cache.GetPrivate()
		persistCh <- func(persistedKeys int, err error) error {
			if err != nil {
				return fmt.Errorf("failed to persist contract storage items changes to the DB: %w", err)
			}

			bc.log.Debug("state root information and NEP transfers are persisted", zap.Duration("took", time.Since(persistStart)), zap.Int("keys", persistedKeys))
			return nil
		}
		p = time.Now()
		fallthrough
	case transfersReset:
		// there's nothing to do after that, so just continue with common operations
		// and remove state reset stage in the end.
	default:
		return fmt.Errorf("unknown state reset stage: %d", stage)
	}

	// Direct (cache-less) DB operation:  remove stale storage items.
	bc.log.Debug("trying to remove stale storage items")
	keys := 0
	err = bc.store.SeekGC(storage.SeekRange{
		Prefix: []byte{byte(statesync.TemporaryPrefix(v.StoragePrefix))},
	}, func(_, _ []byte) bool {
		keys++
		return false
	})
	if err != nil {
		return fmt.Errorf("faield to remove stale storage items from DB: %w", err)
	}
	bc.log.Info("stale storage items are reset", zap.Duration("took", time.Since(p)), zap.Int("keys", keys))
	p = time.Now()

	bc.log.Debug("trying to remove state reset point")
	upperCache.Store.Delete(resetStageKey)
	// Unlike the state jump, state sync point must be removed as we have complete state for this height.
	upperCache.Store.Delete([]byte{byte(storage.SYSStateSyncPoint)})
	bc.log.Info("state reset point is removed", zap.Duration("took", time.Since(p)))

	persistStart := time.Now()
	_, uerr := upperCache.PersistSync()
	if uerr != nil {
		panic(uerr)
	}
	persistCh <- func(persistedKeys int, err error) error {
		if err != nil {
			return fmt.Errorf("failed to persist state reset stage to DAO: %w", err)
		}
		bc.log.Info("state reset point information is persisted", zap.Duration("took", time.Since(persistStart)), zap.Int("keys", persistedKeys))
		return nil
	}
	p = time.Now()

	err = bc.resetRAMState(height, true)
	if err != nil {
		return fmt.Errorf("failed to update in-memory blockchain data: %w", err)
	}
	return nil
}


// GetMemPool returns the memory pool of the blockchain.
func (bc *Blockchain) GetMemPool() *mempool.Pool {
	return bc.memPool
}


// GetTokenLastUpdated returns a set of contract ids with the corresponding last updated
// block indexes. In case of an empty account, latest stored state synchronisation point
// is returned under Math.MinInt32 key.
func (bc *Blockchain) GetTokenLastUpdated(acc util.Uint160) (map[int32]uint32, error) {
	info, err := bc.dao.GetTokenTransferInfo(acc)
	if err != nil {
		return nil, err
	}
	if bc.config.P2PStateExchangeExtensions && bc.config.Ledger.RemoveUntraceableBlocks {
		if _, ok := info.LastUpdated[bc.contracts.NEO.ID]; !ok {
			nBalance, lub := bc.contracts.NEO.BalanceOf(bc.dao, acc)
			if nBalance.Sign() != 0 {
				info.LastUpdated[bc.contracts.NEO.ID] = lub
			}
		}
	}
	stateSyncPoint, err := bc.dao.GetStateSyncPoint()
	if err == nil {
		info.LastUpdated[math.MinInt32] = stateSyncPoint
	}
	return info.LastUpdated, nil
}


// CalculateClaimable calculates the amount of GAS generated by owning specified
// amount of NEO between specified blocks.
func (bc *Blockchain) CalculateClaimable(acc util.Uint160, endHeight uint32) (*big.Int, error) {
	nextBlock, err := bc.getFakeNextBlock(bc.BlockHeight() + 1)
	if err != nil {
		return nil, err
	}
	ic := bc.newInteropContext(trigger.Application, bc.dao, nextBlock, nil)
	return bc.contracts.NEO.CalculateBonus(ic, acc, endHeight)
}



// CalculateAttributesFee returns network fee for all transaction attributes that should be
// paid according to native Policy.
func (bc *Blockchain) CalculateAttributesFee(tx *transaction.Transaction) int64 {
	var feeSum int64
	for _, attr := range tx.Attributes {
		base := bc.contracts.Policy.GetAttributeFeeInternal(bc.dao, attr.Type)
		switch attr.Type {
		case transaction.ConflictsT:
			feeSum += base * int64(len(tx.Signers))
		case transaction.NotaryAssistedT:
			if bc.P2PSigExtensionsEnabled() {
				na := attr.Value.(*transaction.NotaryAssisted)
				feeSum += base * (int64(na.NKeys) + 1)
			}
		default:
			feeSum += base
		}
	}
	return feeSum
}


// P2PSigExtensionsEnabled defines whether P2P signature extensions are enabled.
func (bc *Blockchain) P2PSigExtensionsEnabled() bool {
	return bc.config.P2PSigExtensions
}

// IsTxStillRelevant is a callback for mempool transaction filtering after the
// new block addition. It returns false for transactions added by the new block
// (passed via txpool) and does witness reverification for non-standard
// contracts. It operates under the assumption that full transaction verification
// was already done so we don't need to check basic things like size, input/output
// correctness, presence in blocks before the new one, etc.
func (bc *Blockchain) IsTxStillRelevant(t *transaction.Transaction, txpool *mempool.Pool, isPartialTx bool) bool {
	var (
		recheckWitness bool
		curheight      = bc.BlockHeight()
	)

	if t.ValidUntilBlock <= curheight {
		return false
	}
	if txpool == nil {
		if bc.dao.HasTransaction(t.Hash(), t.Signers, curheight, bc.config.MaxTraceableBlocks) != nil {
			return false
		}
	} else if txpool.HasConflicts(t, bc) {
		return false
	}
	if err := bc.verifyTxAttributes(bc.dao, t, isPartialTx); err != nil {
		return false
	}
	for i := range t.Scripts {
		if !vm.IsStandardContract(t.Scripts[i].VerificationScript) {
			recheckWitness = true
			break
		}
	}
	if recheckWitness {
		return bc.verifyTxWitnesses(t, nil, isPartialTx) == nil
	}
	return true
}