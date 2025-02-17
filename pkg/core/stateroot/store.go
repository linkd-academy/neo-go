package stateroot

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

var (
	// ErrStateMismatch means that local state root doesn't match the one
	// signed by state validators.
	ErrStateMismatch = errors.New("stateroot mismatch")
)

const (
	prefixLocal     = 0x02
	prefixValidated = 0x03
	prefixRootIndex = 0x04
)

func (s *Module) addLocalStateRoot(store *storage.MemCachedStore, sr *state.MPTRoot) {
	key := makeStateRootKey(sr.Index)
	putStateRoot(store, key, sr)

	indexKey := makeRootIndexKey(sr.Root)
	heightBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(heightBytes, sr.Index)
	store.Put(indexKey, heightBytes)

	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, sr.Index)
	store.Put([]byte{byte(storage.DataMPTAux), prefixLocal}, data)
}

func putStateRoot(store *storage.MemCachedStore, key []byte, sr *state.MPTRoot) {
	w := io.NewBufBinWriter()
	sr.EncodeBinary(w.BinWriter)
	store.Put(key, w.Bytes())
}

func (s *Module) getStateRoot(key []byte) (*state.MPTRoot, error) {
	data, err := s.Store.Get(key)
	if err != nil {
		return nil, err
	}

	sr := &state.MPTRoot{}
	r := io.NewBinReaderFromBuf(data)
	sr.DecodeBinary(r)
	return sr, r.Err
}

func makeStateRootKey(index uint32) []byte {
	key := make([]byte, 5)
	key[0] = byte(storage.DataMPTAux)
	binary.BigEndian.PutUint32(key[1:], index)
	return key
}

func makeRootIndexKey(root util.Uint256) []byte {
	key := make([]byte, 1+util.Uint256Size)
	key[0] = prefixRootIndex
	copy(key[1:], root.BytesBE())
	return key
}

// AddStateRoot adds validated state root provided by network.
func (s *Module) AddStateRoot(sr *state.MPTRoot) error {
	if err := s.VerifyStateRoot(sr); err != nil {
		return err
	}
	key := makeStateRootKey(sr.Index)
	local, err := s.getStateRoot(key)
	if err != nil {
		return err
	}
	if !local.Root.Equals(sr.Root) {
		return fmt.Errorf("%w at block %d: %v vs %v", ErrStateMismatch, sr.Index, local.Root, sr.Root)
	}
	if len(local.Witness) != 0 {
		return nil
	}
	putStateRoot(s.Store, key, sr)

	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, sr.Index)
	s.Store.Put([]byte{byte(storage.DataMPTAux), prefixValidated}, data)
	s.validatedHeight.Store(sr.Index)
	if !s.srInHead {
		updateStateHeightMetric(sr.Index)
	}
	return nil
}

func (s *Module) MigrateStateRootIndices() error {
	var count uint32
	s.log.Info("starting state root indices migration")
	b := storage.NewMemCachedStore(s.Store)
	s.Store.Seek(storage.SeekRange{
		Prefix: []byte{byte(storage.DataMPTAux)},
	}, func(k, v []byte) bool {
		if len(k) != 5 || k[0] != byte(storage.DataMPTAux) {
			return true
		}
		sr := &state.MPTRoot{}
		r := io.NewBinReaderFromBuf(v)
		sr.DecodeBinary(r)
		if r.Err != nil {
			s.log.Error("failed to decode state root", zap.Error(r.Err), zap.Binary("key", k))
			return true
		}
		indexKey := makeRootIndexKey(sr.Root)
		heightBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(heightBytes, sr.Index)
		b.Put(indexKey, heightBytes)
		count++
		if count%1000 == 0 {
			s.log.Info("migration progress", zap.Uint32("processed", count))
		}
		return true
	})
	if count == 0 {
		s.log.Info("no state roots to migrate")
		return nil
	}
	s.log.Info("persisting migrated indices", zap.Uint32("total", count))
	_, err := b.Persist()
	if err != nil {
		return fmt.Errorf("failed to persist state root indices: %w", err)
	}
	s.log.Info("state root indices migration completed successfully", zap.Uint32("total_migrated", count))
	return nil
}

func (s *Module) GetLatestStateHeight(root util.Uint256) (uint32, error) {
	indexKey := makeRootIndexKey(root)
	data, err := s.Store.Get(indexKey)
	if err != nil {
		return 0, storage.ErrKeyNotFound
	}
	return binary.BigEndian.Uint32(data), nil
}
