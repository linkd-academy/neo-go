package dao

import (
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

func (dao *Simple) mkUserTransactionKey(account util.Uint160) []byte {
	b := dao.getKeyBuf(1 + util.Uint160Size)
	b[0] = byte(storage.STUserTransactions)
	copy(b[1:], account.BytesBE())
	return b
}

func (dao *Simple) StoreUserTransaction(tx *transaction.Transaction, account util.Uint160) error {
	key := dao.mkUserTransactionKey(account)
	buf := dao.getDataBuf()
	buf.WriteB(storage.ExecTransaction)
	tx.EncodeBinary(buf.BinWriter)
	if buf.Err != nil {
		return buf.Err
	}

	dao.Store.Put(key, buf.Bytes())
	return nil
}
