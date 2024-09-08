package dao

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	iocore "io"
	"math/big"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/config/limits"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/encoding/bigint"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// HasTransaction errors.
var (
	// ErrInternalDBInconsistency is returned when the format of the retrieved DAO
	// record is unexpected.
	ErrInternalDBInconsistency = errors.New("internal DB inconsistency")
)

// conflictRecordValueLen is the length of value of transaction conflict record.
// It consists of 1-byte [storage.ExecTransaction] prefix and 4-bytes block index
// in the LE form.
const conflictRecordValueLen = 1 + 4

// Simple is memCached wrapper around DB, simple DAO implementation.
type Simple struct {
	Version Version
	Store   *storage.MemCachedStore

	nativeCacheLock sync.RWMutex
	nativeCache     map[int32]NativeContractCache
	// nativeCachePS is the backend store that provides functionality to store
	// and retrieve multi-tier native contract cache. The lowest Simple has its
	// nativeCachePS set to nil.
	nativeCachePS *Simple

	private bool
	serCtx  *stackitem.SerializationContext
	keyBuf  []byte
	dataBuf *io.BufBinWriter
}

// NativeContractCache is an interface representing cache for a native contract.
// Cache can be copied to create a wrapper around current DAO layer. Wrapped cache
// can be persisted to the underlying DAO native cache.
type NativeContractCache interface {
	// Copy returns a copy of native cache item that can safely be changed within
	// the subsequent DAO operations.
	Copy() NativeContractCache
}

// NewSimple creates a new simple dao using the provided backend store.
func NewSimple(backend storage.Store, stateRootInHeader bool) *Simple {
	st := storage.NewMemCachedStore(backend)
	return newSimple(st, stateRootInHeader)
}

func newSimple(st *storage.MemCachedStore, stateRootInHeader bool) *Simple {
	return &Simple{
		Version: Version{
			StoragePrefix:     storage.STStorage,
			StateRootInHeader: stateRootInHeader,
		},
		Store:       st,
		nativeCache: make(map[int32]NativeContractCache),
	}
}

// GetBatch returns the currently accumulated DB changeset.
func (dao *Simple) GetBatch() *storage.MemBatch {
	return dao.Store.GetBatch()
}

// GetWrapped returns a new DAO instance with another layer of wrapped
// MemCachedStore around the current DAO Store.
func (dao *Simple) GetWrapped() *Simple {
	d := NewSimple(dao.Store, dao.Version.StateRootInHeader)
	d.Version = dao.Version
	d.nativeCachePS = dao
	return d
}

// GetPrivate returns a new DAO instance with another layer of private
// MemCachedStore around the current DAO Store.
func (dao *Simple) GetPrivate() *Simple {
	d := &Simple{
		Version: dao.Version,
		keyBuf:  dao.keyBuf,
		dataBuf: dao.dataBuf,
		serCtx:  dao.serCtx,
	} // Inherit everything...
	d.Store = storage.NewPrivateMemCachedStore(dao.Store) // except storage, wrap another layer.
	d.private = true
	d.nativeCachePS = dao
	// Do not inherit cache from nativeCachePS; instead should create clear map:
	// GetRWCache and GetROCache will retrieve cache from the underlying
	// nativeCache if requested. The lowest underlying DAO MUST have its native
	// cache initialized before access it, otherwise GetROCache and GetRWCache
	// won't work properly.
	d.nativeCache = make(map[int32]NativeContractCache)
	return d
}

// GetAndDecode performs get operation and decoding with serializable structures.
func (dao *Simple) GetAndDecode(entity io.Serializable, key []byte) error {
	entityBytes, err := dao.Store.Get(key)
	if err != nil {
		return err
	}
	reader := io.NewBinReaderFromBuf(entityBytes)
	entity.DecodeBinary(reader)
	return reader.Err
}

// putWithBuffer performs put operation using buf as a pre-allocated buffer for serialization.
func (dao *Simple) putWithBuffer(entity io.Serializable, key []byte, buf *io.BufBinWriter) error {
	entity.EncodeBinary(buf.BinWriter)
	if buf.Err != nil {
		return buf.Err
	}
	dao.Store.Put(key, buf.Bytes())
	return nil
}

// -- start notification event.
func (dao *Simple) makeExecutableKey(hash util.Uint256) []byte {
	key := dao.getKeyBuf(1 + util.Uint256Size)
	key[0] = byte(storage.DataExecutable)
	copy(key[1:], hash.BytesBE())
	return key
}

// GetAppExecResults gets application execution results with the specified trigger from the
// given store.
func (dao *Simple) GetAppExecResults(hash util.Uint256, trig trigger.Type) ([]state.AppExecResult, error) {
	key := dao.makeExecutableKey(hash)
	bs, err := dao.Store.Get(key)
	if err != nil {
		return nil, err
	}
	if len(bs) == 0 {
		return nil, fmt.Errorf("%w: empty execution log", ErrInternalDBInconsistency)
	}
	switch bs[0] {
	case storage.ExecBlock:
		r := io.NewBinReaderFromBuf(bs)
		_ = r.ReadB()
		_, err = block.NewTrimmedFromReader(dao.Version.StateRootInHeader, r)
		if err != nil {
			return nil, err
		}
		result := make([]state.AppExecResult, 0, 2)
		for {
			aer := new(state.AppExecResult)
			aer.DecodeBinary(r)
			if r.Err != nil {
				if errors.Is(r.Err, iocore.EOF) {
					break
				}
				return nil, r.Err
			}
			if aer.Trigger&trig != 0 {
				result = append(result, *aer)
			}
		}
		return result, nil
	case storage.ExecTransaction:
		_, _, aer, err := decodeTxAndExecResult(bs)
		if err != nil {
			return nil, err
		}
		if aer.Trigger&trig != 0 {
			return []state.AppExecResult{*aer}, nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: unexpected executable prefix %d", ErrInternalDBInconsistency, bs[0])
	}
}

// -- end notification event.

// -- start storage item.

// GetStorageItem returns StorageItem if it exists in the given store.
func (dao *Simple) GetStorageItem(id int32, key []byte) state.StorageItem {
	b, err := dao.Store.Get(dao.makeStorageItemKey(id, key))
	if err != nil {
		return nil
	}
	return b
}

// PutStorageItem puts the given StorageItem for the given id with the given
// key into the given store.
func (dao *Simple) PutStorageItem(id int32, key []byte, si state.StorageItem) {
	stKey := dao.makeStorageItemKey(id, key)
	dao.Store.Put(stKey, si)
}

// PutBigInt serializaed and puts the given integer for the given id with the given
// key into the given store.
func (dao *Simple) PutBigInt(id int32, key []byte, n *big.Int) {
	var buf [bigint.MaxBytesLen]byte
	stData := bigint.ToPreallocatedBytes(n, buf[:])
	dao.PutStorageItem(id, key, stData)
}

// DeleteStorageItem drops a storage item for the given id with the
// given key from the store.
func (dao *Simple) DeleteStorageItem(id int32, key []byte) {
	stKey := dao.makeStorageItemKey(id, key)
	dao.Store.Delete(stKey)
}

// Seek executes f for all storage items matching the given `rng` (matching the given prefix and
// starting from the point specified). If the key or the value is to be used outside of f, they
// may not be copied. Seek continues iterating until false is returned from f. A requested prefix
// (if any non-empty) is trimmed before passing to f.
func (dao *Simple) Seek(id int32, rng storage.SeekRange, f func(k, v []byte) bool) {
	rng.Prefix = bytes.Clone(dao.makeStorageItemKey(id, rng.Prefix)) // f() can use dao too.
	dao.Store.Seek(rng, func(k, v []byte) bool {
		return f(k[len(rng.Prefix):], v)
	})
}

// SeekAsync sends all storage items matching the given `rng` (matching the given prefix and
// starting from the point specified) to a channel and returns the channel.
// Resulting keys and values may not be copied.
func (dao *Simple) SeekAsync(ctx context.Context, id int32, rng storage.SeekRange) chan storage.KeyValue {
	rng.Prefix = bytes.Clone(dao.makeStorageItemKey(id, rng.Prefix))
	return dao.Store.SeekAsync(ctx, rng, true)
}

// makeStorageItemKey returns the key used to store the StorageItem in the DB.
func (dao *Simple) makeStorageItemKey(id int32, key []byte) []byte {
	// 1 for prefix + 4 for Uint32 + len(key) for key
	buf := dao.getKeyBuf(5 + len(key))
	buf[0] = byte(dao.Version.StoragePrefix)
	binary.LittleEndian.PutUint32(buf[1:], uint32(id))
	copy(buf[5:], key)
	return buf
}

// -- end storage item.

// -- other.

// GetBlock returns Block by the given hash if it exists in the store.
func (dao *Simple) GetBlock(hash util.Uint256) (*block.Block, error) {
	return dao.getBlock(dao.makeExecutableKey(hash))
}

func (dao *Simple) getBlock(key []byte) (*block.Block, error) {
	b, err := dao.Store.Get(key)
	if err != nil {
		return nil, err
	}

	r := io.NewBinReaderFromBuf(b)
	if r.ReadB() != storage.ExecBlock {
		// It may be a transaction.
		return nil, storage.ErrKeyNotFound
	}
	block, err := block.NewTrimmedFromReader(dao.Version.StateRootInHeader, r)
	if err != nil {
		return nil, err
	}
	return block, nil
}

// Version represents the current dao version.
type Version struct {
	StoragePrefix              storage.KeyPrefix
	StateRootInHeader          bool
	P2PSigExtensions           bool
	P2PStateExchangeExtensions bool
	KeepOnlyLatestState        bool
	Magic                      uint32
	Value                      string
}

const (
	stateRootInHeaderBit = 1 << iota
	p2pSigExtensionsBit
	p2pStateExchangeExtensionsBit
	keepOnlyLatestStateBit
)

// FromBytes decodes v from a byte-slice.
func (v *Version) FromBytes(data []byte) error {
	if len(data) == 0 {
		return errors.New("missing version")
	}
	i := 0
	for i < len(data) && data[i] != '\x00' {
		i++
	}

	if i == len(data) {
		v.Value = string(data)
		return nil
	}

	if len(data) < i+3 {
		return errors.New("version is invalid")
	}

	v.Value = string(data[:i])
	v.StoragePrefix = storage.KeyPrefix(data[i+1])
	v.StateRootInHeader = data[i+2]&stateRootInHeaderBit != 0
	v.P2PSigExtensions = data[i+2]&p2pSigExtensionsBit != 0
	v.P2PStateExchangeExtensions = data[i+2]&p2pStateExchangeExtensionsBit != 0
	v.KeepOnlyLatestState = data[i+2]&keepOnlyLatestStateBit != 0

	m := i + 3
	if len(data) == m+4 {
		v.Magic = binary.LittleEndian.Uint32(data[m:])
	}
	return nil
}

// Bytes encodes v to a byte-slice.
func (v *Version) Bytes() []byte {
	var mask byte
	if v.StateRootInHeader {
		mask |= stateRootInHeaderBit
	}
	if v.P2PSigExtensions {
		mask |= p2pSigExtensionsBit
	}
	if v.P2PStateExchangeExtensions {
		mask |= p2pStateExchangeExtensionsBit
	}
	if v.KeepOnlyLatestState {
		mask |= keepOnlyLatestStateBit
	}
	res := append([]byte(v.Value), '\x00', byte(v.StoragePrefix), mask)
	res = binary.LittleEndian.AppendUint32(res, v.Magic)
	return res
}

func (dao *Simple) mkKeyPrefix(k storage.KeyPrefix) []byte {
	b := dao.getKeyBuf(1)
	b[0] = byte(k)
	return b
}

// GetVersion attempts to get the current version stored in the
// underlying store.
func (dao *Simple) GetVersion() (Version, error) {
	var version Version

	data, err := dao.Store.Get(dao.mkKeyPrefix(storage.SYSVersion))
	if err == nil {
		err = version.FromBytes(data)
	}
	return version, err
}

// GetCurrentBlockHeight returns the current block height found in the
// underlying store.
func (dao *Simple) GetCurrentBlockHeight() (uint32, error) {
	b, err := dao.Store.Get(dao.mkKeyPrefix(storage.SYSCurrentBlock))
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[32:36]), nil
}

// GetCurrentHeaderHeight returns the current header height and hash from
// the underlying store.
func (dao *Simple) GetCurrentHeaderHeight() (i uint32, h util.Uint256, err error) {
	var b []byte
	b, err = dao.Store.Get(dao.mkKeyPrefix(storage.SYSCurrentHeader))
	if err != nil {
		return
	}
	i = binary.LittleEndian.Uint32(b[32:36])
	h, err = util.Uint256DecodeBytesLE(b[:32])
	return
}

// GetStateSyncPoint returns current state synchronization point P.
func (dao *Simple) GetStateSyncPoint() (uint32, error) {
	b, err := dao.Store.Get(dao.mkKeyPrefix(storage.SYSStateSyncPoint))
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

// GetStateSyncCurrentBlockHeight returns the current block height stored during state
// synchronization process.
func (dao *Simple) GetStateSyncCurrentBlockHeight() (uint32, error) {
	b, err := dao.Store.Get(dao.mkKeyPrefix(storage.SYSStateSyncCurrentBlockHeight))
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

// GetHeaderHashes returns a page of header hashes retrieved from
// the given underlying store.
func (dao *Simple) GetHeaderHashes(height uint32) ([]util.Uint256, error) {
	var hashes []util.Uint256

	key := dao.mkHeaderHashKey(height)
	b, err := dao.Store.Get(key)
	if err != nil {
		return nil, err
	}

	br := io.NewBinReaderFromBuf(b)
	br.ReadArray(&hashes)
	if br.Err != nil {
		return nil, br.Err
	}
	return hashes, nil
}

// DeleteHeaderHashes removes batches of header hashes starting from the one that
// contains header with index `since` up to the most recent batch. It assumes that
// all stored batches contain `batchSize` hashes.
func (dao *Simple) DeleteHeaderHashes(since uint32, batchSize int) {
	dao.Store.Seek(storage.SeekRange{
		Prefix:    dao.mkKeyPrefix(storage.IXHeaderHashList),
		Backwards: true,
	}, func(k, _ []byte) bool {
		first := binary.BigEndian.Uint32(k[1:])
		if first >= since {
			dao.Store.Delete(k)
			return first != since
		}
		if first+uint32(batchSize)-1 >= since {
			dao.Store.Delete(k)
		}
		return false
	})
}

// GetTransaction returns Transaction and its height by the given hash
// if it exists in the store. It does not return conflict record stubs.
func (dao *Simple) GetTransaction(hash util.Uint256) (*transaction.Transaction, uint32, error) {
	key := dao.makeExecutableKey(hash)
	b, err := dao.Store.Get(key)
	if err != nil {
		return nil, 0, err
	}
	if len(b) < 1 {
		return nil, 0, errors.New("bad transaction bytes")
	}
	if b[0] != storage.ExecTransaction {
		// It may be a block.
		return nil, 0, storage.ErrKeyNotFound
	}
	if len(b) == conflictRecordValueLen {
		// It's a conflict record stub.
		return nil, 0, storage.ErrKeyNotFound
	}
	r := io.NewBinReaderFromBuf(b)
	_ = r.ReadB()

	var height = r.ReadU32LE()

	tx := &transaction.Transaction{}
	tx.DecodeBinary(r)
	if r.Err != nil {
		return nil, 0, r.Err
	}

	return tx, height, nil
}

// PutVersion stores the given version in the underlying store.
func (dao *Simple) PutVersion(v Version) {
	dao.Version = v
	dao.Store.Put(dao.mkKeyPrefix(storage.SYSVersion), v.Bytes())
}

// PutCurrentHeader stores the current header.
func (dao *Simple) PutCurrentHeader(h util.Uint256, index uint32) {
	buf := dao.getDataBuf()
	buf.WriteBytes(h.BytesLE())
	buf.WriteU32LE(index)
	dao.Store.Put(dao.mkKeyPrefix(storage.SYSCurrentHeader), buf.Bytes())
}

// PutStateSyncPoint stores the current state synchronization point P.
func (dao *Simple) PutStateSyncPoint(p uint32) {
	buf := dao.getDataBuf()
	buf.WriteU32LE(p)
	dao.Store.Put(dao.mkKeyPrefix(storage.SYSStateSyncPoint), buf.Bytes())
}

// PutStateSyncCurrentBlockHeight stores the current block height during state synchronization process.
func (dao *Simple) PutStateSyncCurrentBlockHeight(h uint32) {
	buf := dao.getDataBuf()
	buf.WriteU32LE(h)
	dao.Store.Put(dao.mkKeyPrefix(storage.SYSStateSyncCurrentBlockHeight), buf.Bytes())
}

func (dao *Simple) getKeyBuf(l int) []byte {
	if dao.private {
		if dao.keyBuf == nil {
			dao.keyBuf = make([]byte, 0, 1+4+limits.MaxStorageKeyLen) // Prefix, uint32, key.
		}
		return dao.keyBuf[:l] // Should have enough capacity.
	}
	return make([]byte, l)
}

func (dao *Simple) getDataBuf() *io.BufBinWriter {
	if dao.private {
		if dao.dataBuf == nil {
			dao.dataBuf = io.NewBufBinWriter()
		}
		dao.dataBuf.Reset()
		return dao.dataBuf
	}
	return io.NewBufBinWriter()
}

func (dao *Simple) GetItemCtx() *stackitem.SerializationContext {
	if dao.private {
		if dao.serCtx == nil {
			dao.serCtx = stackitem.NewSerializationContext()
		}
		return dao.serCtx
	}
	return stackitem.NewSerializationContext()
}

// Persist flushes all the changes made into the (supposedly) persistent
// underlying store. It doesn't block accesses to DAO from other threads.
func (dao *Simple) Persist() (int, error) {
	if dao.nativeCachePS != nil {
		dao.nativeCacheLock.Lock()
		dao.nativeCachePS.nativeCacheLock.Lock()
		defer func() {
			dao.nativeCachePS.nativeCacheLock.Unlock()
			dao.nativeCacheLock.Unlock()
		}()

		dao.persistNativeCache()
	}
	return dao.Store.Persist()
}

// PersistSync flushes all the changes made into the (supposedly) persistent
// underlying store. It's a synchronous version of Persist that doesn't allow
// other threads to work with DAO while flushing the Store.
func (dao *Simple) PersistSync() (int, error) {
	if dao.nativeCachePS != nil {
		dao.nativeCacheLock.Lock()
		dao.nativeCachePS.nativeCacheLock.Lock()
		defer func() {
			dao.nativeCachePS.nativeCacheLock.Unlock()
			dao.nativeCacheLock.Unlock()
		}()
		dao.persistNativeCache()
	}
	return dao.Store.PersistSync()
}

// persistNativeCache is internal unprotected method for native cache persisting.
// It does NO checks for nativeCachePS is not nil.
func (dao *Simple) persistNativeCache() {
	lower := dao.nativeCachePS
	for id, nativeCache := range dao.nativeCache {
		lower.nativeCache[id] = nativeCache
	}
	dao.nativeCache = nil
}

// GetROCache returns native contact cache. The cache CAN NOT be modified by
// the caller. It's the caller's duty to keep it unmodified.
func (dao *Simple) GetROCache(id int32) NativeContractCache {
	dao.nativeCacheLock.RLock()
	defer dao.nativeCacheLock.RUnlock()

	return dao.getCache(id, true)
}

// GetRWCache returns native contact cache. The cache CAN BE safely modified
// by the caller.
func (dao *Simple) GetRWCache(id int32) NativeContractCache {
	dao.nativeCacheLock.Lock()
	defer dao.nativeCacheLock.Unlock()

	return dao.getCache(id, false)
}

// getCache is an internal unlocked representation of GetROCache and GetRWCache.
func (dao *Simple) getCache(k int32, ro bool) NativeContractCache {
	if itm, ok := dao.nativeCache[k]; ok {
		// Don't need to create itm copy, because its value was already copied
		// the first time it was retrieved from lower ps.
		return itm
	}

	if dao.nativeCachePS != nil {
		if ro {
			return dao.nativeCachePS.GetROCache(k)
		}
		v := dao.nativeCachePS.GetRWCache(k)
		if v != nil {
			// Create a copy here in order not to modify the existing cache.
			cp := v.Copy()
			dao.nativeCache[k] = cp
			return cp
		}
	}
	return nil
}

// SetCache adds native contract cache to the cache map.
func (dao *Simple) SetCache(id int32, v NativeContractCache) {
	dao.nativeCacheLock.Lock()
	defer dao.nativeCacheLock.Unlock()

	dao.nativeCache[id] = v
}
