package core

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/nspcc-dev/go-ordered-json"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/interop/contract"
	"github.com/nspcc-dev/neo-go/pkg/core/mempool"
	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/stateroot"
	"github.com/nspcc-dev/neo-go/pkg/core/statesync"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
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


// SetNotary sets notary module. It may safely be called on the running blockchain.
// To unregister Notary service use SetNotary(nil).
func (bc *Blockchain) SetNotary(mod native.NotaryService) {
	if mod != nil {
		keys, _, err := bc.GetDesignatedByRole(noderoles.P2PNotary)
		if err != nil {
			bc.log.Error("failed to get notary key list")
			return
		}
		mod.UpdateNotaryNodes(keys)
	}
	bc.contracts.Designate.NotaryService.Store(&mod)
}

func (bc *Blockchain) init() error {
	// If we could not find the version in the Store, we know that there is nothing stored.
	// restoreErrorbc.dao.Store.Restore("initial_state.gob")

	ver, err := bc.dao.GetVersion()
	if err != nil {
		bc.log.Info("no storage version found! creating genesis block")
		ver = dao.Version{
			StoragePrefix:              storage.STStorage,
			StateRootInHeader:          bc.config.StateRootInHeader,
			P2PSigExtensions:           bc.config.P2PSigExtensions,
			P2PStateExchangeExtensions: bc.config.P2PStateExchangeExtensions,
			KeepOnlyLatestState:        bc.config.Ledger.KeepOnlyLatestState,
			Magic:                      uint32(bc.config.Magic),
			Value:                      version,
		}
		bc.dao.PutVersion(ver)
		bc.dao.Version = ver
		bc.persistent.Version = ver
		genesisBlock, err := CreateGenesisBlock(bc.config.ProtocolConfiguration)
		if err != nil {
			return err
		}
		bc.HeaderHashes.initGenesis(bc.dao, genesisBlock.Hash())
		if err := bc.stateRoot.Init(0); err != nil {
			return fmt.Errorf("can't init MPT: %w", err)
		}
		return bc.storeBlock(genesisBlock, nil)
	}
	if ver.Value != version {
		return fmt.Errorf("storage version mismatch (expected=%s, actual=%s)", version, ver.Value)
	}
	if ver.StateRootInHeader != bc.config.StateRootInHeader {
		return fmt.Errorf("StateRootInHeader setting mismatch (config=%t, db=%t)",
			bc.config.StateRootInHeader, ver.StateRootInHeader)
	}
	if ver.P2PSigExtensions != bc.config.P2PSigExtensions {
		return fmt.Errorf("P2PSigExtensions setting mismatch (old=%t, new=%t)",
			ver.P2PSigExtensions, bc.config.P2PSigExtensions)
	}
	if ver.P2PStateExchangeExtensions != bc.config.P2PStateExchangeExtensions {
		return fmt.Errorf("P2PStateExchangeExtensions setting mismatch (old=%t, new=%t)",
			ver.P2PStateExchangeExtensions, bc.config.P2PStateExchangeExtensions)
	}
	if ver.KeepOnlyLatestState != bc.config.Ledger.KeepOnlyLatestState {
		return fmt.Errorf("KeepOnlyLatestState setting mismatch (old=%v, new=%v)",
			ver.KeepOnlyLatestState, bc.config.Ledger.KeepOnlyLatestState)
	}
	if ver.Magic != uint32(bc.config.Magic) {
		return fmt.Errorf("protocol configuration Magic mismatch (old=%v, new=%v)",
			ver.Magic, bc.config.Magic)
	}
	bc.dao.Version = ver
	bc.persistent.Version = ver

	// At this point there was no version found in the storage which
	// implies a creating fresh storage with the version specified
	// and the genesis block as first block.
	bc.log.Info("restoring blockchain", zap.String("version", version))

	err = bc.HeaderHashes.init(bc.dao)
	if err != nil {
		return err
	}

	// Check whether StateChangeState stage is in the storage and continue interrupted state jump / state reset if so.
	stateChStage, err := bc.dao.Store.Get([]byte{byte(storage.SYSStateChangeStage)})
	if err == nil {
		if len(stateChStage) != 1 {
			return fmt.Errorf("invalid state jump stage format")
		}
		// State jump / state reset wasn't finished yet, thus continue it.
		stateSyncPoint, err := bc.dao.GetStateSyncPoint()
		if err != nil {
			return fmt.Errorf("failed to get state sync point from the storage")
		}
		if (stateChStage[0] & stateResetBit) != 0 {
			return bc.resetStateInternal(stateSyncPoint, stateChangeStage(stateChStage[0]&(^stateResetBit)))
		}
		if !(bc.config.P2PStateExchangeExtensions && bc.config.Ledger.RemoveUntraceableBlocks) {
			return errors.New("state jump was not completed, but P2PStateExchangeExtensions are disabled or archival node capability is on. " +
				"To start an archival node drop the database manually and restart the node")
		}
		return bc.jumpToStateInternal(stateSyncPoint, stateChangeStage(stateChStage[0]))
	}

	bHeight, err := bc.dao.GetCurrentBlockHeight()
	if err != nil {
		return fmt.Errorf("failed to retrieve current block height: %w", err)
	}
	bc.blockHeight = bHeight
	bc.persistedHeight = bHeight

	bc.log.Debug("initializing caches", zap.Uint32("blockHeight", bHeight))
	if err = bc.stateRoot.Init(bHeight); err != nil {
		return fmt.Errorf("can't init MPT at height %d: %w", bHeight, err)
	}

	err = bc.initializeNativeCache(bc.blockHeight, bc.dao)
	if err != nil {
		return fmt.Errorf("can't init natives cache: %w", err)
	}

	// Check autogenerated native contracts' manifests and NEFs against the stored ones.
	// Need to be done after native Management cache initialization to be able to get
	// contract state from DAO via high-level bc API.
	var current = bc.getCurrentHF()
	for _, c := range bc.contracts.Contracts {
		md := c.Metadata()
		storedCS := bc.GetContractState(md.Hash)
		// Check that contract was deployed.
		if !bc.isHardforkEnabled(c.ActiveIn(), bHeight) {
			if storedCS != nil {
				return fmt.Errorf("native contract %s is already stored, but marked as inactive for height %d in config", md.Name, bHeight)
			}
			continue
		}
		if storedCS == nil {
			return fmt.Errorf("native contract %s is not stored, but should be active at height %d according to config", md.Name, bHeight)
		}
		storedCSBytes, err := stackitem.SerializeConvertible(storedCS)
		if err != nil {
			return fmt.Errorf("failed to check native %s state against autogenerated one: %w", md.Name, err)
		}
		hfMD := md.HFSpecificContractMD(&current)
		autogenCS := &state.Contract{
			ContractBase:  hfMD.ContractBase,
			UpdateCounter: storedCS.UpdateCounter, // it can be restored only from the DB, so use the stored value.
		}
		autogenCSBytes, err := stackitem.SerializeConvertible(autogenCS)
		if err != nil {
			return fmt.Errorf("failed to check native %s state against autogenerated one: %w", md.Name, err)
		}
		if !bytes.Equal(storedCSBytes, autogenCSBytes) {
			storedJ, _ := json.Marshal(storedCS)
			autogenJ, _ := json.Marshal(autogenCS)
			return fmt.Errorf("native %s: version mismatch for the latest hardfork %s (stored contract state differs from autogenerated one), "+
				"try to resynchronize the node from the genesis: %s vs %s", md.Name, current, string(storedJ), string(autogenJ))
		}
	}

	updateBlockHeightMetric(bHeight)
	updatePersistedHeightMetric(bHeight)
	updateHeaderHeightMetric(bc.HeaderHeight())
	err = bc.updateExtensibleWhitelist(bHeight)

	return err
}

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

func (bc *Blockchain) initializeNativeCache(blockHeight uint32, d *dao.Simple) error {
	for _, c := range bc.contracts.Contracts {
		// Check that contract was deployed.
		if !bc.isHardforkEnabled(c.ActiveIn(), blockHeight) {
			continue
		}
		err := c.InitializeCache(blockHeight, d)
		if err != nil {
			return fmt.Errorf("failed to initialize cache for %s: %w", c.Metadata().Name, err)
		}
	}
	return nil
}

// isHardforkEnabled returns true if the specified hardfork is enabled at the
// given height. nil hardfork is treated as always enabled.
func (bc *Blockchain) isHardforkEnabled(hf *config.Hardfork, blockHeight uint32) bool {
	hfs := bc.config.Hardforks
	if hf != nil {
		start, ok := hfs[hf.String()]
		if !ok || start < blockHeight {
			return false
		}
	}
	return true
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



// GetNEP17Contracts returns the list of deployed NEP-17 contracts.
func (bc *Blockchain) GetNEP17Contracts() []util.Uint160 {
	return bc.contracts.Management.GetNEP17Contracts(bc.dao)
}

// GetNEP11Contracts returns the list of deployed NEP-11 contracts.
func (bc *Blockchain) GetNEP11Contracts() []util.Uint160 {
	return bc.contracts.Management.GetNEP11Contracts(bc.dao)
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

// GetUtilityTokenBalance returns utility token (GAS) balance for the acc.
func (bc *Blockchain) GetUtilityTokenBalance(acc util.Uint160) *big.Int {
	bs := bc.contracts.GAS.BalanceOf(bc.dao, acc)
	if bs == nil {
		return big.NewInt(0)
	}
	return bs
}

// GetGoverningTokenBalance returns governing token (NEO) balance and the height
// of the last balance change for the account.
func (bc *Blockchain) GetGoverningTokenBalance(acc util.Uint160) (*big.Int, uint32) {
	return bc.contracts.NEO.BalanceOf(bc.dao, acc)
}

// GetNotaryBalance returns Notary deposit amount for the specified account.
func (bc *Blockchain) GetNotaryBalance(acc util.Uint160) *big.Int {
	return bc.contracts.Notary.BalanceOf(bc.dao, acc)
}

// GetNotaryServiceFeePerKey returns a NotaryAssisted transaction attribute fee
// per key which is a reward per notary request key for designated notary nodes.
func (bc *Blockchain) GetNotaryServiceFeePerKey() int64 {
	return bc.contracts.Policy.GetAttributeFeeInternal(bc.dao, transaction.NotaryAssistedT)
}

// GetNotaryContractScriptHash returns Notary native contract hash.
func (bc *Blockchain) GetNotaryContractScriptHash() util.Uint160 {
	if bc.P2PSigExtensionsEnabled() {
		return bc.contracts.Notary.Hash
	}
	return util.Uint160{}
}

// GetNotaryDepositExpiration returns Notary deposit expiration height for the specified account.
func (bc *Blockchain) GetNotaryDepositExpiration(acc util.Uint160) uint32 {
	return bc.contracts.Notary.ExpirationOf(bc.dao, acc)
}

// LastBatch returns last persisted storage batch.
func (bc *Blockchain) LastBatch() *storage.MemBatch {
	return bc.lastBatch
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



// GetAppExecResults returns application execution results with the specified trigger by the given
// tx hash or block hash.
func (bc *Blockchain) GetAppExecResults(hash util.Uint256, trig trigger.Type) ([]state.AppExecResult, error) {
	return bc.dao.GetAppExecResults(hash, trig)
}

// GetStorageItem returns an item from storage.
func (bc *Blockchain) GetStorageItem(id int32, key []byte) state.StorageItem {
	return bc.dao.GetStorageItem(id, key)
}

// SeekStorage performs seek operation over contract storage. Prefix is trimmed in the resulting pair's key.
func (bc *Blockchain) SeekStorage(id int32, prefix []byte, cont func(k, v []byte) bool) {
	bc.dao.Seek(id, storage.SeekRange{Prefix: prefix}, cont)
}


// GetContractState returns contract by its script hash.
func (bc *Blockchain) GetContractState(hash util.Uint160) *state.Contract {
	contract, err := native.GetContract(bc.dao, hash)
	if contract == nil && !errors.Is(err, storage.ErrKeyNotFound) {
		bc.log.Warn("failed to get contract state", zap.Error(err))
	}
	return contract
}

// GetContractScriptHash returns contract script hash by its ID.
func (bc *Blockchain) GetContractScriptHash(id int32) (util.Uint160, error) {
	return native.GetContractScriptHash(bc.dao, id)
}

// GetNativeContractScriptHash returns native contract script hash by its name.
func (bc *Blockchain) GetNativeContractScriptHash(name string) (util.Uint160, error) {
	c := bc.contracts.ByName(name)
	if c != nil {
		return c.Metadata().Hash, nil
	}
	return util.Uint160{}, errors.New("Unknown native contract")
}

// GetNatives returns list of native contracts.
func (bc *Blockchain) GetNatives() []state.Contract {
	res := make([]state.Contract, 0, len(bc.contracts.Contracts))
	current := bc.getCurrentHF()
	for _, c := range bc.contracts.Contracts {
		activeIn := c.ActiveIn()
		if !(activeIn == nil || activeIn.Cmp(current) <= 0) {
			continue
		}

		st := bc.GetContractState(c.Metadata().Hash)
		if st != nil { // Should never happen, but better safe than sorry.
			res = append(res, *st)
		}
	}
	return res
}

// GetConfig returns the config stored in the blockchain.
func (bc *Blockchain) GetConfig() config.Blockchain {
	return bc.config
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

// FeePerByte returns transaction network fee per byte.
func (bc *Blockchain) FeePerByte() int64 {
	return bc.contracts.Policy.GetFeePerByteInternal(bc.dao)
}

// GetMemPool returns the memory pool of the blockchain.
func (bc *Blockchain) GetMemPool() *mempool.Pool {
	return bc.memPool
}
// Various errors that could be returns upon header verification.
var (
	ErrHdrHashMismatch     = errors.New("previous header hash doesn't match")
	ErrHdrIndexMismatch    = errors.New("previous header index doesn't match")
	ErrHdrInvalidTimestamp = errors.New("block is not newer than the previous one")
	ErrHdrStateRootSetting = errors.New("state root setting mismatch")
	ErrHdrInvalidStateRoot = errors.New("state root for previous block is invalid")
)

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

func (bc *Blockchain) verifyTxAttributes(d *dao.Simple, tx *transaction.Transaction, isPartialTx bool) error {
	for i := range tx.Attributes {
		switch attrType := tx.Attributes[i].Type; attrType {
		case transaction.HighPriority:
			h := bc.contracts.NEO.GetCommitteeAddress(d)
			if !tx.HasSigner(h) {
				return fmt.Errorf("%w: high priority tx is not signed by committee", ErrInvalidAttribute)
			}
		case transaction.OracleResponseT:
			h, err := bc.contracts.Oracle.GetScriptHash(bc.dao)
			if err != nil || h.Equals(util.Uint160{}) {
				return fmt.Errorf("%w: %w", ErrInvalidAttribute, err)
			}
			hasOracle := false
			for i := range tx.Signers {
				if tx.Signers[i].Scopes != transaction.None {
					return fmt.Errorf("%w: oracle tx has invalid signer scope", ErrInvalidAttribute)
				}
				if tx.Signers[i].Account.Equals(h) {
					hasOracle = true
				}
			}
			if !hasOracle {
				return fmt.Errorf("%w: oracle tx is not signed by oracle nodes", ErrInvalidAttribute)
			}
			if !bytes.Equal(tx.Script, bc.contracts.Oracle.GetOracleResponseScript()) {
				return fmt.Errorf("%w: oracle tx has invalid script", ErrInvalidAttribute)
			}
			resp := tx.Attributes[i].Value.(*transaction.OracleResponse)
			req, err := bc.contracts.Oracle.GetRequestInternal(bc.dao, resp.ID)
			if err != nil {
				return fmt.Errorf("%w: oracle tx points to invalid request: %w", ErrInvalidAttribute, err)
			}
			if uint64(tx.NetworkFee+tx.SystemFee) < req.GasForResponse {
				return fmt.Errorf("%w: oracle tx has insufficient gas", ErrInvalidAttribute)
			}
		case transaction.NotValidBeforeT:
			nvb := tx.Attributes[i].Value.(*transaction.NotValidBefore).Height
			curHeight := bc.BlockHeight()
			if isPartialTx {
				maxNVBDelta, err := bc.GetMaxNotValidBeforeDelta()
				if err != nil {
					return fmt.Errorf("%w: failed to retrieve MaxNotValidBeforeDelta value from native Notary contract: %w", ErrInvalidAttribute, err)
				}
				if curHeight+maxNVBDelta < nvb {
					return fmt.Errorf("%w: NotValidBefore (%d) bigger than MaxNVBDelta (%d) allows at height %d", ErrInvalidAttribute, nvb, maxNVBDelta, curHeight)
				}
				if nvb+maxNVBDelta < tx.ValidUntilBlock {
					return fmt.Errorf("%w: NotValidBefore (%d) set more than MaxNVBDelta (%d) away from VUB (%d)", ErrInvalidAttribute, nvb, maxNVBDelta, tx.ValidUntilBlock)
				}
			} else {
				if curHeight < nvb {
					return fmt.Errorf("%w: transaction is not yet valid: NotValidBefore = %d, current height = %d", ErrInvalidAttribute, nvb, curHeight)
				}
			}
		case transaction.ConflictsT:
			conflicts := tx.Attributes[i].Value.(*transaction.Conflicts)
			// Only fully-qualified dao.ErrAlreadyExists error bothers us here, thus, we
			// can safely omit the signers, current index and MTB arguments to HasTransaction call to improve performance a bit.
			if err := bc.dao.HasTransaction(conflicts.Hash, nil, 0, 0); errors.Is(err, dao.ErrAlreadyExists) {
				return fmt.Errorf("%w: conflicting transaction %s is already on chain", ErrInvalidAttribute, conflicts.Hash.StringLE())
			}
		case transaction.NotaryAssistedT:
			if !bc.config.P2PSigExtensions {
				return fmt.Errorf("%w: NotaryAssisted attribute was found, but P2PSigExtensions are disabled", ErrInvalidAttribute)
			}
			if !tx.HasSigner(bc.contracts.Notary.Hash) {
				return fmt.Errorf("%w: NotaryAssisted attribute was found, but transaction is not signed by the Notary native contract", ErrInvalidAttribute)
			}
		default:
			if !bc.config.ReservedAttributes && attrType >= transaction.ReservedLowerBound && attrType <= transaction.ReservedUpperBound {
				return fmt.Errorf("%w: attribute of reserved type was found, but ReservedAttributes are disabled", ErrInvalidAttribute)
			}
		}
	}
	return nil
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

// VerifyTx verifies whether transaction is bonafide or not relative to the
// current blockchain state. Note that this verification is completely isolated
// from the main node's mempool.
func (bc *Blockchain) VerifyTx(t *transaction.Transaction) error {
	var mp = mempool.New(1, 0, false, nil)
	bc.lock.RLock()
	defer bc.lock.RUnlock()
	return bc.verifyAndPoolTx(t, mp, bc)
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

// GetCommittee returns the sorted list of public keys of nodes in committee.
func (bc *Blockchain) GetCommittee() (keys.PublicKeys, error) {
	pubs := bc.contracts.NEO.GetCommitteeMembers(bc.dao)
	slices.SortFunc(pubs, (*keys.PublicKey).Cmp)
	return pubs, nil
}

// ComputeNextBlockValidators returns current validators. Validators list
// returned from this method is updated once per CommitteeSize number of blocks.
// For the last block in the dBFT epoch this method returns the list of validators
// recalculated from the latest relevant information about NEO votes; in this case
// list of validators may differ from the one returned by GetNextBlockValidators.
// For the not-last block of dBFT epoch this method returns the same list as
// GetNextBlockValidators.
func (bc *Blockchain) ComputeNextBlockValidators() []*keys.PublicKey {
	return bc.contracts.NEO.ComputeNextBlockValidators(bc.dao)
}

// GetNextBlockValidators returns next block validators. Validators list returned
// from this method is the sorted top NumOfCNs number of public keys from the
// committee of the current dBFT round (that was calculated once for the
// CommitteeSize number of blocks), thus, validators list returned from this
// method is being updated once per (committee size) number of blocks, but not
// every block.
func (bc *Blockchain) GetNextBlockValidators() ([]*keys.PublicKey, error) {
	return bc.contracts.NEO.GetNextBlockValidatorsInternal(bc.dao), nil
}

// GetEnrollments returns all registered validators.
func (bc *Blockchain) GetEnrollments() ([]state.Validator, error) {
	return bc.contracts.NEO.GetCandidates(bc.dao)
}

// GetTestVM returns an interop context with VM set up for a test run.
func (bc *Blockchain) GetTestVM(t trigger.Type, tx *transaction.Transaction, b *block.Block) (*interop.Context, error) {
	if b == nil {
		var err error
		h := bc.BlockHeight() + 1
		b, err = bc.getFakeNextBlock(h)
		if err != nil {
			return nil, fmt.Errorf("failed to create fake block for height %d: %w", h, err)
		}
	}
	systemInterop := bc.newInteropContext(t, bc.dao, b, tx)
	_ = systemInterop.SpawnVM() // All the other code suppose that the VM is ready.
	return systemInterop, nil
}

// GetTestHistoricVM returns an interop context with VM set up for a test run.
func (bc *Blockchain) GetTestHistoricVM(t trigger.Type, tx *transaction.Transaction, nextBlockHeight uint32) (*interop.Context, error) {
	if bc.config.Ledger.KeepOnlyLatestState {
		return nil, errors.New("only latest state is supported")
	}
	b, err := bc.getFakeNextBlock(nextBlockHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to create fake block for height %d: %w", nextBlockHeight, err)
	}
	var mode = mpt.ModeAll
	if bc.config.Ledger.RemoveUntraceableBlocks {
		if b.Index < bc.BlockHeight()-bc.config.MaxTraceableBlocks {
			return nil, fmt.Errorf("state for height %d is outdated and removed from the storage", b.Index)
		}
		mode |= mpt.ModeGCFlag
	}
	if b.Index < 1 || b.Index > bc.BlockHeight()+1 {
		return nil, fmt.Errorf("unsupported historic chain's height: requested state for %d, chain height %d", b.Index, bc.blockHeight)
	}
	// Assuming that block N-th is processing during historic call, the historic invocation should be based on the storage state of height N-1.
	sr, err := bc.stateRoot.GetStateRoot(b.Index - 1)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve stateroot for height %d: %w", b.Index, err)
	}
	s := mpt.NewTrieStore(sr.Root, mode, storage.NewPrivateMemCachedStore(bc.dao.Store))
	dTrie := dao.NewSimple(s, bc.config.StateRootInHeader)
	dTrie.Version = bc.dao.Version
	// Initialize native cache before passing DAO to interop context constructor, because
	// the constructor will call BaseExecFee/StoragePrice policy methods on the passed DAO.
	err = bc.initializeNativeCache(b.Index, dTrie)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize native cache backed by historic DAO: %w", err)
	}
	systemInterop := bc.newInteropContext(t, dTrie, b, tx)
	_ = systemInterop.SpawnVM() // All the other code suppose that the VM is ready.
	return systemInterop, nil
}

// getFakeNextBlock returns fake block with the specified index and pre-filled Timestamp field.
func (bc *Blockchain) getFakeNextBlock(nextBlockHeight uint32) (*block.Block, error) {
	b := block.New(bc.config.StateRootInHeader)
	b.Index = nextBlockHeight
	hdr, err := bc.GetHeader(bc.GetHeaderHash(nextBlockHeight - 1))
	if err != nil {
		return nil, err
	}
	b.Timestamp = hdr.Timestamp + uint64(bc.config.TimePerBlock/time.Millisecond)
	return b, nil
}

// Various witness verification errors.
var (
	ErrWitnessHashMismatch         = errors.New("witness hash mismatch")
	ErrNativeContractWitness       = errors.New("native contract witness must have empty verification script")
	ErrVerificationFailed          = errors.New("signature check failed")
	ErrInvalidInvocationScript     = errors.New("invalid invocation script")
	ErrInvalidSignature            = fmt.Errorf("%w: invalid signature", ErrVerificationFailed)
	ErrInvalidVerificationScript   = errors.New("invalid verification script")
	ErrUnknownVerificationContract = errors.New("unknown verification contract")
	ErrInvalidVerificationContract = errors.New("verification contract is missing `verify` method or `verify` method has unexpected return value")
)

// InitVerificationContext initializes context for witness check.
func (bc *Blockchain) InitVerificationContext(ic *interop.Context, hash util.Uint160, witness *transaction.Witness) error {
	if len(witness.VerificationScript) != 0 {
		if witness.ScriptHash() != hash {
			return fmt.Errorf("%w: expected %s, got %s", ErrWitnessHashMismatch, hash.StringLE(), witness.ScriptHash().StringLE())
		}
		if bc.contracts.ByHash(hash) != nil {
			return ErrNativeContractWitness
		}
		err := vm.IsScriptCorrect(witness.VerificationScript, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidVerificationScript, err)
		}
		ic.VM.LoadScriptWithHash(witness.VerificationScript, hash, callflag.ReadOnly)
	} else {
		cs, err := ic.GetContract(hash)
		if err != nil {
			return ErrUnknownVerificationContract
		}
		md := cs.Manifest.ABI.GetMethod(manifest.MethodVerify, -1)
		if md == nil || md.ReturnType != smartcontract.BoolType {
			return ErrInvalidVerificationContract
		}
		verifyOffset := md.Offset
		initOffset := -1
		md = cs.Manifest.ABI.GetMethod(manifest.MethodInit, 0)
		if md != nil {
			initOffset = md.Offset
		}
		ic.Invocations[cs.Hash]++
		ic.VM.LoadNEFMethod(&cs.NEF, &cs.Manifest, util.Uint160{}, hash, callflag.ReadOnly,
			true, verifyOffset, initOffset, nil)
	}
	if len(witness.InvocationScript) != 0 {
		err := vm.IsScriptCorrect(witness.InvocationScript, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidInvocationScript, err)
		}
		ic.VM.LoadScript(witness.InvocationScript)
	}
	return nil
}

// VerifyWitness checks that w is a correct witness for c signed by h. It returns
// the amount of GAS consumed during verification and an error.
func (bc *Blockchain) VerifyWitness(h util.Uint160, c hash.Hashable, w *transaction.Witness, gas int64) (int64, error) {
	ic := bc.newInteropContext(trigger.Verification, bc.dao, nil, nil)
	ic.Container = c
	if tx, ok := c.(*transaction.Transaction); ok {
		ic.Tx = tx
	}
	return bc.verifyHashAgainstScript(h, w, ic, gas)
}

// verifyHashAgainstScript verifies given hash against the given witness and returns the amount of GAS consumed.
func (bc *Blockchain) verifyHashAgainstScript(hash util.Uint160, witness *transaction.Witness, interopCtx *interop.Context, gas int64) (int64, error) {
	gas = min(gas, bc.contracts.Policy.GetMaxVerificationGas(interopCtx.DAO))

	vm := interopCtx.SpawnVM()
	vm.GasLimit = gas
	if err := bc.InitVerificationContext(interopCtx, hash, witness); err != nil {
		return 0, err
	}
	err := interopCtx.Exec()
	if vm.HasFailed() {
		return 0, fmt.Errorf("%w: vm execution has failed: %w", ErrVerificationFailed, err)
	}
	estack := vm.Estack()
	if estack.Len() > 0 {
		resEl := estack.Pop()
		res, err := resEl.Item().TryBool()
		if err != nil {
			return 0, fmt.Errorf("%w: invalid return value", ErrVerificationFailed)
		}
		if vm.Estack().Len() != 0 {
			return 0, fmt.Errorf("%w: expected exactly one returned value", ErrVerificationFailed)
		}
		if !res {
			return vm.GasConsumed(), ErrInvalidSignature
		}
	} else {
		return 0, fmt.Errorf("%w: no result returned from the script", ErrVerificationFailed)
	}
	return vm.GasConsumed(), nil
}

// verifyTxWitnesses verifies the scripts (witnesses) that come with a given
// transaction. It can reorder them by ScriptHash, because that's required to
// match a slice of script hashes from the Blockchain. Block parameter
// is used for easy interop access and can be omitted for transactions that are
// not yet added into any block. verificationFee argument can be provided to
// restrict the maximum amount of GAS allowed to spend on transaction
// verification.
// Golang implementation of VerifyWitnesses method in C# (https://github.com/neo-project/neo/blob/master/neo/SmartContract/Helper.cs#L87).
func (bc *Blockchain) verifyTxWitnesses(t *transaction.Transaction, block *block.Block, isPartialTx bool, verificationFee ...int64) error {
	interopCtx := bc.newInteropContext(trigger.Verification, bc.dao, block, t)
	var gasLimit int64
	if len(verificationFee) == 0 {
		gasLimit = t.NetworkFee - int64(t.Size())*bc.FeePerByte() - bc.CalculateAttributesFee(t)
	} else {
		gasLimit = verificationFee[0]
	}
	for i := range t.Signers {
		gasConsumed, err := bc.verifyHashAgainstScript(t.Signers[i].Account, &t.Scripts[i], interopCtx, gasLimit)
		if err != nil &&
			!(i == 0 && isPartialTx && errors.Is(err, ErrInvalidSignature)) { // it's OK for partially-filled transaction with dummy first witness.
			return fmt.Errorf("witness #%d: %w", i, err)
		}
		gasLimit -= gasConsumed
	}

	return nil
}

// verifyHeaderWitnesses is a block-specific implementation of VerifyWitnesses logic.
func (bc *Blockchain) verifyHeaderWitnesses(currHeader, prevHeader *block.Header) error {
	hash := prevHeader.NextConsensus
	_, err := bc.VerifyWitness(hash, currHeader, &currHeader.Script, HeaderVerificationGasLimit)
	return err
}


func (bc *Blockchain) newInteropContext(trigger trigger.Type, d *dao.Simple, block *block.Block, tx *transaction.Transaction) *interop.Context {
	baseExecFee := int64(interop.DefaultBaseExecFee)
	if block == nil || block.Index != 0 {
		// Use provided dao instead of Blockchain's one to fetch possible ExecFeeFactor
		// changes that were not yet persisted to Blockchain's dao.
		baseExecFee = bc.contracts.Policy.GetExecFeeFactorInternal(d)
	}
	baseStorageFee := int64(native.DefaultStoragePrice)
	if block == nil || block.Index != 0 {
		// Use provided dao instead of Blockchain's one to fetch possible StoragePrice
		// changes that were not yet persisted to Blockchain's dao.
		baseStorageFee = bc.contracts.Policy.GetStoragePriceInternal(d)
	}
	ic := interop.NewContext(trigger, bc, d, baseExecFee, baseStorageFee, native.GetContract, bc.contracts.Contracts, contract.LoadToken, block, tx, bc.log)
	ic.Functions = systemInterops
	switch {
	case tx != nil:
		ic.Container = tx
	case block != nil:
		ic.Container = block
	}
	ic.InitNonceData()
	return ic
}

// P2PSigExtensionsEnabled defines whether P2P signature extensions are enabled.
func (bc *Blockchain) P2PSigExtensionsEnabled() bool {
	return bc.config.P2PSigExtensions
}



