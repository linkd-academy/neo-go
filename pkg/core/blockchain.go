package core

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/mempool"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/stateroot"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

// Tuning parameters.
const (
	version = "0.2.12"

	// DefaultInitialGAS is the default amount of GAS emitted to the standby validators
	// multisignature account during native GAS contract initialization.
	DefaultInitialGAS                      = 52000000_00000000
	defaultGCPeriod                        = 10000
	defaultMemPoolSize                     = 50000
	defaultP2PNotaryRequestPayloadPoolSize = 1000
	defaultMaxBlockSize                    = 262144
	defaultMaxBlockSystemFee               = 900000000000
	defaultMaxTraceableBlocks              = 2102400 // 1 year of 15s blocks
	defaultMaxTransactionsPerBlock         = 512
	defaultTimePerBlock                    = 15 * time.Second
	// HeaderVerificationGasLimit is the maximum amount of GAS for block header verification.
	HeaderVerificationGasLimit = 3_00000000 // 3 GAS
	defaultStateSyncInterval   = 40000
)

// stateChangeStage denotes the stage of state modification process.
type stateChangeStage byte

// A set of stages used to split state jump / state reset into atomic operations.
const (
	// none means that no state jump or state reset process was initiated yet.
	none stateChangeStage = 1 << iota
	// stateJumpStarted means that state jump was just initiated, but outdated storage items
	// were not yet removed.
	stateJumpStarted
	// newStorageItemsAdded means that contract storage items are up-to-date with the current
	// state.
	newStorageItemsAdded
	// staleBlocksRemoved means that state corresponding to the stale blocks (genesis block in
	// in case of state jump) was removed from the storage.
	staleBlocksRemoved
	// headersReset denotes stale SYS-prefixed and IX-prefixed information was removed from
	// the storage (applicable to state reset only).
	headersReset
	// transfersReset denotes NEP transfers were successfully updated (applicable to state reset only).
	transfersReset
	// stateResetBit represents a bit identifier for state reset process. If this bit is not set, then
	// it's an unfinished state jump.
	stateResetBit byte = 1 << 7
)

var (
	// ErrAlreadyExists is returned when trying to add some transaction
	// that already exists on chain.
	ErrAlreadyExists = errors.New("already exists in blockchain")
	// ErrAlreadyInPool is returned when trying to add some already existing
	// transaction into the mempool.
	ErrAlreadyInPool = errors.New("already exists in mempool")
	// ErrOOM is returned when adding transaction to the memory pool because
	// it reached its full capacity.
	ErrOOM = errors.New("no space left in the memory pool")
	// ErrPolicy is returned on attempt to add transaction that doesn't
	// comply with node's configured policy into the mempool.
	ErrPolicy = errors.New("not allowed by policy")
	// ErrInvalidBlockIndex is returned when trying to add block with index
	// other than expected height of the blockchain.
	ErrInvalidBlockIndex = errors.New("invalid block index")
	// ErrHasConflicts is returned when trying to add some transaction which
	// conflicts with other transaction in the chain or pool according to
	// Conflicts attribute.
	ErrHasConflicts = errors.New("has conflicts")
)
var (
	persistInterval = 1 * time.Second
)

// Blockchain represents the blockchain. It maintans internal state representing
// the state of the ledger that can be accessed in various ways and changed by
// adding new blocks or headers.
type Blockchain struct {
	HeaderHashes

	config config.Blockchain

	// The only way chain state changes is by adding blocks, so we can't
	// allow concurrent block additions. It differs from the next lock in
	// that it's only for AddBlock method itself, the chain state is
	// protected by the lock below, but holding it during all of AddBlock
	// is too expensive (because the state only changes when persisting
	// change cache).
	addLock sync.Mutex

	// This lock ensures blockchain immutability for operations that need
	// that while performing their tasks. It's mostly used as a read lock
	// with the only writer being the block addition logic.
	lock sync.RWMutex

	// Data access object for CRUD operations around storage. It's write-cached.
	dao *dao.Simple

	// persistent is the same DB as dao, but we never write to it, so all reads
	// are directly from underlying persistent store.
	persistent *dao.Simple

	// Underlying persistent store.
	store storage.Store

	// Current index/height of the highest block.
	// Read access should always be called by BlockHeight().
	// Write access should only happen in storeBlock().
	blockHeight uint32

	// Current top Block wrapped in an atomic.Value for safe access.
	topBlock atomic.Value

	// Current persisted block count.
	persistedHeight uint32

	// Stop synchronization mechanisms.
	stopCh      chan struct{}
	runToExitCh chan struct{}
	// isRunning denotes whether blockchain routines are currently running.
	isRunning atomic.Value

	memPool *mempool.Pool

	// postBlock is a set of callback methods which should be run under the Blockchain lock after new block is persisted.
	// Block's transactions are passed via mempool.
	postBlock []func(func(*transaction.Transaction, *mempool.Pool, bool) bool, *mempool.Pool, *block.Block)

	log *zap.Logger

	lastBatch *storage.MemBatch

	contracts native.Contracts

	extensible atomic.Value

	// knownValidatorsCount is the latest known validators count used
	// for defaultBlockWitness.
	knownValidatorsCount atomic.Value
	// defaultBlockWitness stores transaction.Witness with m out of n multisig,
	// where n = knownValidatorsCount.
	defaultBlockWitness atomic.Value

	stateRoot *stateroot.Module

	// Notification subsystem.
	events  chan bcEvent
	subCh   chan any
	unsubCh chan any
}

// StateRoot represents local state root module.
type StateRoot interface {
	CurrentLocalHeight() uint32
	CurrentLocalStateRoot() util.Uint256
	CurrentValidatedHeight() uint32
	FindStates(root util.Uint256, prefix, start []byte, maxNum int) ([]storage.KeyValue, error)
	SeekStates(root util.Uint256, prefix []byte, f func(k, v []byte) bool)
	GetState(root util.Uint256, key []byte) ([]byte, error)
	GetStateProof(root util.Uint256, key []byte) ([][]byte, error)
	GetStateRoot(height uint32) (*state.MPTRoot, error)
	GetLatestStateHeight(root util.Uint256) (uint32, error)
}

// bcEvent is an internal event generated by the Blockchain and then
// broadcasted to other parties. It joins the new block and associated
// invocation logs, all the other events visible from outside can be produced
// from this combination.
type bcEvent struct {
	block          *block.Block
	appExecResults []*state.AppExecResult
}

// transferData is used for transfer caching during storeBlock.
type transferData struct {
	Info  state.TokenTransferInfo
	Log11 state.TokenTransferLog
	Log17 state.TokenTransferLog
}

// NewBlockchain returns a new blockchain object the will use the
// given Store as its underlying storage. For it to work correctly you need
// to spawn a goroutine for its Run method after this initialization.
func NewBlockchain(s storage.Store, cfg config.Blockchain, log *zap.Logger) (*Blockchain, error) {
	if log == nil {
		return nil, errors.New("empty logger")
	}

	// Protocol configuration fixups/checks.
	if cfg.InitialGASSupply <= 0 {
		cfg.InitialGASSupply = fixedn.Fixed8(DefaultInitialGAS)
		log.Info("initial gas supply is not set or wrong, setting default value", zap.Stringer("InitialGASSupply", cfg.InitialGASSupply))
	}
	if cfg.MemPoolSize <= 0 {
		cfg.MemPoolSize = defaultMemPoolSize
		log.Info("mempool size is not set or wrong, setting default value", zap.Int("MemPoolSize", cfg.MemPoolSize))
	}
	if cfg.P2PSigExtensions && cfg.P2PNotaryRequestPayloadPoolSize <= 0 {
		cfg.P2PNotaryRequestPayloadPoolSize = defaultP2PNotaryRequestPayloadPoolSize
		log.Info("P2PNotaryRequestPayloadPool size is not set or wrong, setting default value", zap.Int("P2PNotaryRequestPayloadPoolSize", cfg.P2PNotaryRequestPayloadPoolSize))
	}
	if cfg.MaxBlockSize == 0 {
		cfg.MaxBlockSize = defaultMaxBlockSize
		log.Info("MaxBlockSize is not set or wrong, setting default value", zap.Uint32("MaxBlockSize", cfg.MaxBlockSize))
	}
	if cfg.MaxBlockSystemFee <= 0 {
		cfg.MaxBlockSystemFee = defaultMaxBlockSystemFee
		log.Info("MaxBlockSystemFee is not set or wrong, setting default value", zap.Int64("MaxBlockSystemFee", cfg.MaxBlockSystemFee))
	}
	if cfg.MaxTraceableBlocks == 0 {
		cfg.MaxTraceableBlocks = defaultMaxTraceableBlocks
		log.Info("MaxTraceableBlocks is not set or wrong, using default value", zap.Uint32("MaxTraceableBlocks", cfg.MaxTraceableBlocks))
	}
	if cfg.MaxTransactionsPerBlock == 0 {
		cfg.MaxTransactionsPerBlock = defaultMaxTransactionsPerBlock
		log.Info("MaxTransactionsPerBlock is not set or wrong, using default value",
			zap.Uint16("MaxTransactionsPerBlock", cfg.MaxTransactionsPerBlock))
	}
	if cfg.TimePerBlock <= 0 {
		cfg.TimePerBlock = defaultTimePerBlock
		log.Info("TimePerBlock is not set or wrong, using default value",
			zap.Duration("TimePerBlock", cfg.TimePerBlock))
	}
	if cfg.MaxValidUntilBlockIncrement == 0 {
		const timePerDay = 24 * time.Hour

		cfg.MaxValidUntilBlockIncrement = uint32(timePerDay / cfg.TimePerBlock)
		log.Info("MaxValidUntilBlockIncrement is not set or wrong, using default value",
			zap.Uint32("MaxValidUntilBlockIncrement", cfg.MaxValidUntilBlockIncrement))
	}
	if cfg.P2PStateExchangeExtensions {
		if !cfg.StateRootInHeader {
			return nil, errors.New("P2PStatesExchangeExtensions are enabled, but StateRootInHeader is off")
		}
		if cfg.KeepOnlyLatestState && !cfg.RemoveUntraceableBlocks {
			return nil, errors.New("P2PStateExchangeExtensions can be enabled either on MPT-complete node (KeepOnlyLatestState=false) or on light GC-enabled node (RemoveUntraceableBlocks=true)")
		}
		if cfg.StateSyncInterval <= 0 {
			cfg.StateSyncInterval = defaultStateSyncInterval
			log.Info("StateSyncInterval is not set or wrong, using default value",
				zap.Int("StateSyncInterval", cfg.StateSyncInterval))
		}
	}
	if cfg.Hardforks == nil {
		cfg.Hardforks = map[string]uint32{}
		for _, hf := range config.Hardforks {
			cfg.Hardforks[hf.String()] = 0
		}
		log.Info("Hardforks are not set, using default value")
	} else if len(cfg.Hardforks) != 0 {
		// Explicitly set the height of all old omitted hardforks to 0 for proper
		// IsHardforkEnabled behaviour.
		for _, hf := range config.Hardforks {
			if _, ok := cfg.Hardforks[hf.String()]; !ok {
				cfg.Hardforks[hf.String()] = 0
				continue
			}
			break
		}
	}

	// Local config consistency checks.
	if cfg.Ledger.RemoveUntraceableBlocks && cfg.Ledger.GarbageCollectionPeriod == 0 {
		cfg.Ledger.GarbageCollectionPeriod = defaultGCPeriod
		log.Info("GarbageCollectionPeriod is not set or wrong, using default value", zap.Uint32("GarbageCollectionPeriod", cfg.Ledger.GarbageCollectionPeriod))
	}
	bc := &Blockchain{
		config:      cfg,
		dao:         dao.NewSimple(s, cfg.StateRootInHeader),
		persistent:  dao.NewSimple(s, cfg.StateRootInHeader),
		store:       s,
		stopCh:      make(chan struct{}),
		runToExitCh: make(chan struct{}),
		memPool:     mempool.New(cfg.MemPoolSize, 0, false, updateMempoolMetrics),
		log:         log,
		events:      make(chan bcEvent),
		subCh:       make(chan any),
		unsubCh:     make(chan any),
		contracts:   *native.NewContracts(cfg.ProtocolConfiguration),
	}

	bc.stateRoot = stateroot.NewModule(cfg, bc.VerifyWitness, bc.log, bc.dao.Store)
	bc.contracts.Designate.StateRootService = bc.stateRoot

	if err := bc.init(); err != nil {
		return nil, err
	}

	bc.isRunning.Store(false)
	return bc, nil
}



// Run runs chain loop, it needs to be run as goroutine and executing it is
// critical for correct Blockchain operation.
func (bc *Blockchain) Run() {
	bc.isRunning.Store(true)
	persistTimer := time.NewTimer(persistInterval)
	defer func() {
		persistTimer.Stop()
		if _, err := bc.persist(true); err != nil {
			bc.log.Warn("failed to persist", zap.Error(err))
		}
		if err := bc.dao.Store.Close(); err != nil {
			bc.log.Warn("failed to close db", zap.Error(err))
		}
		bc.isRunning.Store(false)
		close(bc.runToExitCh)
	}()
	go bc.notificationDispatcher()
	var nextSync bool
	for {
		select {
		case <-bc.stopCh:
			return
		case <-persistTimer.C:
			var oldPersisted uint32
			var gcDur time.Duration

			if bc.config.Ledger.RemoveUntraceableBlocks {
				oldPersisted = atomic.LoadUint32(&bc.persistedHeight)
			}
			dur, err := bc.persist(nextSync)
			if err != nil {
				bc.log.Warn("failed to persist blockchain", zap.Error(err))
			}
			if bc.config.Ledger.RemoveUntraceableBlocks {
				gcDur = bc.tryRunGC(oldPersisted)
			}
			nextSync = dur > persistInterval*2
			interval := persistInterval - dur - gcDur
			interval = max(interval, time.Microsecond) // Reset doesn't work with zero or negative value.
			persistTimer.Reset(interval)
		}
	}
}

func (bc *Blockchain) tryRunGC(oldHeight uint32) time.Duration {
	var dur time.Duration

	newHeight := atomic.LoadUint32(&bc.persistedHeight)
	var tgtBlock = int64(newHeight)

	tgtBlock -= int64(bc.config.MaxTraceableBlocks)
	if bc.config.P2PStateExchangeExtensions {
		syncP := newHeight / uint32(bc.config.StateSyncInterval)
		syncP--
		syncP *= uint32(bc.config.StateSyncInterval)
		tgtBlock = min(tgtBlock, int64(syncP))
	}
	// Always round to the GCP.
	tgtBlock /= int64(bc.config.Ledger.GarbageCollectionPeriod)
	tgtBlock *= int64(bc.config.Ledger.GarbageCollectionPeriod)
	// Count periods.
	oldHeight /= bc.config.Ledger.GarbageCollectionPeriod
	newHeight /= bc.config.Ledger.GarbageCollectionPeriod
	if tgtBlock > int64(bc.config.Ledger.GarbageCollectionPeriod) && newHeight != oldHeight {
		tgtBlock /= int64(bc.config.Ledger.GarbageCollectionPeriod)
		tgtBlock *= int64(bc.config.Ledger.GarbageCollectionPeriod)
		dur = bc.stateRoot.GC(uint32(tgtBlock), bc.store)
		dur += bc.removeOldTransfers(uint32(tgtBlock))
	}
	return dur
}

// Close stops Blockchain's internal loop, syncs changes to persistent storage
// and closes it. The Blockchain is no longer functional after the call to Close.
func (bc *Blockchain) Close() {
	// If there is a block addition in progress, wait for it to finish and
	// don't allow new ones.
	bc.addLock.Lock()
	close(bc.stopCh)
	<-bc.runToExitCh
	bc.addLock.Unlock()
	_ = bc.log.Sync()
}




