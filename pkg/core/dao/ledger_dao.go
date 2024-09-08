package dao

import (
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

func (dao *Simple) mkHeaderHashKey(h uint32) []byte {
	b := dao.getKeyBuf(1 + 4)
	b[0] = byte(storage.IXHeaderHashList)
	binary.BigEndian.PutUint32(b[1:], h)
	return b
}

// StoreHeaderHashes pushes a batch of header hashes into the store.
func (dao *Simple) StoreHeaderHashes(hashes []util.Uint256, height uint32) error {
	key := dao.mkHeaderHashKey(height)
	buf := dao.getDataBuf()
	buf.WriteArray(hashes)
	if buf.Err != nil {
		return buf.Err
	}
	dao.Store.Put(key, buf.Bytes())
	return nil
}

func isTraceableBlock(indexBytes []byte, height, maxTraceableBlocks uint32) bool {
	index := binary.LittleEndian.Uint32(indexBytes)
	return index <= height && index+maxTraceableBlocks > height
}

// StoreAsBlock stores given block as DataBlock. It can reuse given buffer for
// the purpose of value serialization.
func (dao *Simple) StoreAsBlock(block *block.Block, aer1 *state.AppExecResult, aer2 *state.AppExecResult) error {
	var (
		key = dao.makeExecutableKey(block.Hash())
		buf = dao.getDataBuf()
	)
	buf.WriteB(storage.ExecBlock)
	block.EncodeTrimmed(buf.BinWriter)
	if aer1 != nil {
		aer1.EncodeBinaryWithContext(buf.BinWriter, dao.GetItemCtx())
	}
	if aer2 != nil {
		aer2.EncodeBinaryWithContext(buf.BinWriter, dao.GetItemCtx())
	}
	if buf.Err != nil {
		return buf.Err
	}
	dao.Store.Put(key, buf.Bytes())
	return nil
}

// DeleteBlock removes the block from dao. It's not atomic, so make sure you're
// using private MemCached instance here.
func (dao *Simple) DeleteBlock(h util.Uint256) error {
	key := dao.makeExecutableKey(h)

	b, err := dao.getBlock(key)
	if err != nil {
		return err
	}
	err = dao.storeHeader(key, &b.Header)
	if err != nil {
		return err
	}

	for _, tx := range b.Transactions {
		copy(key[1:], tx.Hash().BytesBE())
		dao.Store.Delete(key)
		for _, attr := range tx.GetAttributes(transaction.ConflictsT) {
			hash := attr.Value.(*transaction.Conflicts).Hash
			copy(key[1:], hash.BytesBE())

			v, err := dao.Store.Get(key)
			if err != nil {
				return fmt.Errorf("failed to retrieve conflict record stub for %s (height %d, conflict %s): %w", tx.Hash().StringLE(), b.Index, hash.StringLE(), err)
			}
			// It might be a block since we allow transactions to have block hash in the Conflicts attribute.
			if v[0] != storage.ExecTransaction {
				continue
			}
			index := binary.LittleEndian.Uint32(v[1:])
			// We can check for `<=` here, but use equality comparison to be more precise
			// and do not touch earlier conflict records (if any). Their removal must be triggered
			// by the caller code.
			if index == b.Index {
				dao.Store.Delete(key)
			}

			for _, s := range tx.Signers {
				sKey := append(key, s.Account.BytesBE()...)
				v, err := dao.Store.Get(sKey)
				if err != nil {
					return fmt.Errorf("failed to retrieve conflict record for %s (height %d, conflict %s, signer %s): %w", tx.Hash().StringLE(), b.Index, hash.StringLE(), address.Uint160ToString(s.Account), err)
				}
				index = binary.LittleEndian.Uint32(v[1:])
				if index == b.Index {
					dao.Store.Delete(sKey)
				}
			}
		}
	}

	return nil
}

// PurgeHeader completely removes specified header from dao. It differs from
// DeleteBlock in that it removes header anyway and does nothing except removing
// header. It does no checks for header existence.
func (dao *Simple) PurgeHeader(h util.Uint256) {
	key := dao.makeExecutableKey(h)
	dao.Store.Delete(key)
}

// StoreHeader saves the block header into the store.
func (dao *Simple) StoreHeader(h *block.Header) error {
	return dao.storeHeader(dao.makeExecutableKey(h.Hash()), h)
}

func (dao *Simple) storeHeader(key []byte, h *block.Header) error {
	buf := dao.getDataBuf()
	buf.WriteB(storage.ExecBlock)
	h.EncodeBinary(buf.BinWriter)
	buf.BinWriter.WriteB(0)
	if buf.Err != nil {
		return buf.Err
	}
	dao.Store.Put(key, buf.Bytes())
	return nil
}

// StoreAsCurrentBlock stores the hash of the given block with prefix
// SYSCurrentBlock.
func (dao *Simple) StoreAsCurrentBlock(block *block.Block) {
	buf := dao.getDataBuf()
	h := block.Hash()
	h.EncodeBinary(buf.BinWriter)
	buf.WriteU32LE(block.Index)
	dao.Store.Put(dao.mkKeyPrefix(storage.SYSCurrentBlock), buf.Bytes())
}
