package dao

import (
	"errors"
	"log" // Added for logging

	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// makeUserTransactionKey creates the user transaction key.
// Returns an error if txHash is empty.
func (dao *Simple) makeUserTransactionKey(account util.Uint160, txHash util.Uint256) ([]byte, error) {
	if txHash == (util.Uint256{}) {
		return nil, errors.New("txHash cannot be empty")
	}

	// Key with txHash
	b := dao.getKeyBuf(1 + util.Uint160Size + util.Uint256Size)
	b[0] = byte(storage.STUserTransactions)  // Prefix STUserTransactions
	copy(b[1:], account.BytesBE())           // Adds the hash160
	copy(b[1+util.Uint160Size:], txHash.BytesBE()) // Adds the transaction hash

	// Log the key being generated
	log.Printf("Generated key with prefix: %x, account hash160: %x, txHash: %x", b[0], account.BytesBE(), txHash.BytesBE())

	return b, nil
}

// PutTransactionForHash160 stores a transaction associated with a given hash160.
func (dao *Simple) PutTransactionForHash160(hash util.Uint160, tx *transaction.Transaction) error {
	txHash := tx.Hash()
	key, err := dao.makeUserTransactionKey(hash, txHash)
	if err != nil {
		return err
	}

	buf := dao.getDataBuf()

	tx.EncodeBinary(buf.BinWriter)
	if buf.Err != nil {
		return buf.Err
	}

	// Log the transaction and key being stored
	log.Printf("Storing transaction for hash160: %x with txHash: %x", hash.BytesBE(), txHash.BytesBE())
	log.Printf("Transaction data size: %d bytes", len(buf.Bytes()))

	dao.Store.Put(key, buf.Bytes())
	return nil
}

// GetTransactionsForHash160 returns all transactions associated with a given hash160.
func (dao *Simple) GetTransactionsForHash160(hash util.Uint160) ([]*transaction.Transaction, error) {
	// Define the search range with the prefix and hash160
	prefixKey := dao.makeUserTransactionKeyWithoutTxHash(hash) // Only use hash160 for search

	// Log the prefix used for search
	log.Printf("Searching transactions for hash160: %x with prefix: %x", hash.BytesBE(), prefixKey)

	rng := storage.SeekRange{
		Prefix: prefixKey,
	}

	var transactions []*transaction.Transaction

	// Iterate through the transactions associated with the hash160
	dao.Store.Seek(rng, func(k, v []byte) bool {
		tx := new(transaction.Transaction)
		reader := io.NewBinReaderFromBuf(v)
		tx.DecodeBinary(reader)
		if reader.Err != nil {
			log.Printf("Error decoding transaction: %v", reader.Err)
			return false
		}

		// Log each transaction found
		log.Printf("Found transaction with size: %d bytes", len(v))
		transactions = append(transactions, tx)
		return true
	})

	return transactions, nil
}

// makeUserTransactionKeyWithoutTxHash creates the key for searching without txHash
func (dao *Simple) makeUserTransactionKeyWithoutTxHash(account util.Uint160) []byte {
	b := dao.getKeyBuf(1 + util.Uint160Size)
	b[0] = byte(storage.STUserTransactions)  // Prefix STUserTransactions
	copy(b[1:], account.BytesBE())           // Adds the hash160

	// Log the key generated without txHash
	log.Printf("Generated key for search without txHash: prefix: %x, account hash160: %x", b[0], account.BytesBE())

	return b
}
