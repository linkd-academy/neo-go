package storage

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"sync"

	_ "github.com/ncruces/go-sqlite3/driver"

	_ "github.com/ncruces/go-sqlite3/embed"
)

// SQLiteStore is an SQLite implementation of a Store.
type SQLiteStore struct {
	mut sync.RWMutex
	db  *sql.DB
}

// NewSQLiteStore creates a new SQLiteStore object.
func NewSQLiteStore(dataSourceName string, reset bool) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", "file:"+dataSourceName)

	if err != nil {
		return nil, err
	}
	store := &SQLiteStore{db: db}
	if err := store.initDB(reset); err != nil {
		return nil, err
	}

	if _, err := os.Stat(dataSourceName); os.IsNotExist(err) {
		fmt.Println("Database file not found!")
	} else {
		fmt.Println("Database file found at:", dataSourceName)
	}

	return store, nil
}

// initDB initializes the database schema.
func (s *SQLiteStore) initDB(reset bool) error {
	if reset {
		_, err := s.db.Exec(`DROP TABLE IF EXISTS store;`)
		if err != nil {
			return err
		}
	}
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS store (
			key TEXT PRIMARY KEY,
			value BLOB
		);
	`)
	if err != nil {
		fmt.Println("Error creating table:", err)
	}
	return err
}

// Get implements the Store interface.
func (s *SQLiteStore) Get(key []byte) ([]byte, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	hexKey := hex.EncodeToString(key)

	var value []byte
	err := s.db.QueryRow("SELECT value FROM store WHERE key = ?", hexKey).Scan(&value)
	if err == sql.ErrNoRows {
		fmt.Println("Key not found:", hexKey)
		return nil, ErrKeyNotFound
	}
	if err != nil {
		fmt.Println("Error fetching key:", err)
	}
	return value, err
}

// PutChangeSet implements the Store interface. Never returns an error.
func (s *SQLiteStore) PutChangeSet(puts map[string][]byte, stores map[string][]byte) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO store (key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for k, v := range puts {
		hexKey := hex.EncodeToString([]byte(k))
		if _, err := stmt.Exec(hexKey, v); err != nil {
			return err
		}
	}
	for k, v := range stores {
		hexKey := hex.EncodeToString([]byte(k))
		if _, err := stmt.Exec(hexKey, v); err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

// Seek implements the Store interface.
func (s *SQLiteStore) Seek(rng SeekRange, f func(k, v []byte) bool) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	hexPrefix := hex.EncodeToString(rng.Prefix)
	rows, err := s.db.Query("SELECT key, value FROM store WHERE key LIKE ? ORDER BY key", hexPrefix+"%")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var hexKey string
		var value []byte
		if err := rows.Scan(&hexKey, &value); err != nil {
			return
		}
		key, err := hex.DecodeString(hexKey)
		if err != nil {
			return
		}
		if !f(key, value) {
			break
		}
	}
}

// SeekGC implements the Store interface.
func (s *SQLiteStore) SeekGC(rng SeekRange, keep func(k, v []byte) bool) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	hexPrefix := hex.EncodeToString(rng.Prefix)
	rows, err := s.db.Query("SELECT key, value FROM store WHERE key LIKE ? ORDER BY key", hexPrefix+"%")
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for rows.Next() {
		var hexKey string
		var value []byte
		if err := rows.Scan(&hexKey, &value); err != nil {
			return err
		}
		key, err := hex.DecodeString(hexKey)
		if err != nil {
			return err
		}
		if !keep(key, value) {
			fmt.Printf("Deleting key: %s\n", hexKey)
			if _, err := tx.Exec("DELETE FROM store WHERE key = ?", hexKey); err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

// Close implements Store interface and closes the database connection.
func (s *SQLiteStore) Close() error {
	s.mut.Lock()
	defer s.mut.Unlock()
	fmt.Println("Closing database connection.")
	return s.db.Close()
}
