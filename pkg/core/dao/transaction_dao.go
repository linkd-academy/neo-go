package dao

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// ErrAlreadyExists is returned when the transaction exists in dao.
var ErrAlreadyExists = errors.New("transaction already exists")

// ErrHasConflicts is returned when the transaction is in the list of conflicting
// transactions which are already in dao.
var ErrHasConflicts = errors.New("transaction has conflicts")

// GetTxExecResult gets the application execution result of the specified transaction
// and returns the transaction itself, its height, and its AppExecResult.
func (dao *Simple) GetTxExecResult(hash util.Uint256) (uint32, *transaction.Transaction, *state.AppExecResult, error) {
	key := dao.makeExecutableKey(hash)
	bs, err := dao.Store.Get(key)
	if err != nil {
		return 0, nil, nil, err
	}
	if len(bs) == 0 {
		return 0, nil, nil, fmt.Errorf("%w: empty execution log", ErrInternalDBInconsistency)
	}
	if bs[0] != storage.ExecTransaction {
		return 0, nil, nil, storage.ErrKeyNotFound
	}
	return decodeTxAndExecResult(bs)
}

// decodeTxAndExecResult decodes transaction, its height, and execution result from
// the given executable bytes. It performs no executable prefix check.
func decodeTxAndExecResult(buf []byte) (uint32, *transaction.Transaction, *state.AppExecResult, error) {
	const conflictRecordValueLen = 1 + 4

	if len(buf) == conflictRecordValueLen { // conflict record stub.
		return 0, nil, nil, storage.ErrKeyNotFound
	}
	r := io.NewBinReaderFromBuf(buf)
	_ = r.ReadB()
	h := r.ReadU32LE()
	tx := &transaction.Transaction{}
	tx.DecodeBinary(r)
	if r.Err != nil {
		return 0, nil, nil, r.Err
	}
	aer := new(state.AppExecResult)
	aer.DecodeBinary(r)
	if r.Err != nil {
		return 0, nil, nil, r.Err
	}

	return h, tx, aer, nil
}

// HasTransaction returns nil if the given store does not contain the given
// Transaction hash. It returns an error in case the transaction is in chain
// or in the list of conflicting transactions. If non-zero signers are specified,
// then additional check against the conflicting transaction signers intersection
// is held. Do not omit signers in case if it's important to check the validity
// of a supposedly conflicting on-chain transaction. The retrieved conflict isn't
// checked against the maxTraceableBlocks setting if signers are omitted.
// HasTransaction does not consider the case of block executable.
func (dao *Simple) HasTransaction(hash util.Uint256, signers []transaction.Signer, currentIndex uint32, maxTraceableBlocks uint32) error {
	key := dao.makeExecutableKey(hash)
	bytes, err := dao.Store.Get(key)
	if err != nil {
		return nil
	}

	if len(bytes) < conflictRecordValueLen { // (storage.ExecTransaction + index) for conflict record
		return nil
	}
	if bytes[0] != storage.ExecTransaction {
		// It's a block, thus no conflict. This path is needed since there's a transaction accepted on mainnet
		// that conflicts with block. This transaction was declined by Go nodes, but accepted by C# nodes, and hence
		// we need to adjust Go behaviour post-factum. Ref. #3427 and 0x289c235dcdab8be7426d05f0fbb5e86c619f81481ea136493fa95deee5dbb7cc.
		return nil
	}
	if len(bytes) != conflictRecordValueLen {
		return ErrAlreadyExists // fully-qualified transaction
	}
	if len(signers) == 0 {
		return ErrHasConflicts
	}

	if !isTraceableBlock(bytes[1:], currentIndex, maxTraceableBlocks) {
		// The most fresh conflict record is already outdated.
		return nil
	}

	for _, s := range signers {
		v, err := dao.Store.Get(append(key, s.Account.BytesBE()...))
		if err == nil {
			if isTraceableBlock(v[1:], currentIndex, maxTraceableBlocks) {
				return ErrHasConflicts
			}
		}
	}

	return nil
}

// StoreAsTransaction stores the given TX as DataTransaction. It also stores conflict records
// (hashes of transactions the given tx has conflicts with) as DataTransaction with value containing
// only five bytes: 1-byte [storage.ExecTransaction] executable prefix + 4-bytes-LE block index. It can reuse the given
// buffer for the purpose of value serialization.
func (dao *Simple) StoreAsTransaction(tx *transaction.Transaction, index uint32, aer *state.AppExecResult) error {
	key := dao.makeExecutableKey(tx.Hash())
	buf := dao.getDataBuf()

	buf.WriteB(storage.ExecTransaction)
	buf.WriteU32LE(index)
	tx.EncodeBinary(buf.BinWriter)
	if aer != nil {
		aer.EncodeBinaryWithContext(buf.BinWriter, dao.GetItemCtx())
	}
	if buf.Err != nil {
		return buf.Err
	}
	val := buf.Bytes()
	dao.Store.Put(key, val)

	val = val[:conflictRecordValueLen] // storage.ExecTransaction (1 byte) + index (4 bytes)
	attrs := tx.GetAttributes(transaction.ConflictsT)
	for _, attr := range attrs {
		// Conflict record stub.
		hash := attr.Value.(*transaction.Conflicts).Hash
		copy(key[1:], hash.BytesBE())

		// A short path if there's a block with the matching hash. If it's there, then
		// don't store the conflict record stub and conflict signers since it's a
		// useless record, no transaction with the same hash is possible.
		exec, err := dao.Store.Get(key)
		if err == nil {
			if len(exec) > 0 && exec[0] != storage.ExecTransaction {
				continue
			}
		}

		dao.Store.Put(key, val)

		// Conflicting signers.
		sKey := make([]byte, len(key)+util.Uint160Size)
		copy(sKey, key)
		for _, s := range tx.Signers {
			copy(sKey[len(key):], s.Account.BytesBE())
			dao.Store.Put(sKey, val)
		}
	}
	return nil
}
