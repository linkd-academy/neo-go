package dao

import (
	"encoding/binary"
	"errors"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// GetTokenTransferInfo retrieves NEP-17 transfer info from the cache.
func (dao *Simple) GetTokenTransferInfo(acc util.Uint160) (*state.TokenTransferInfo, error) {
	key := dao.makeTTIKey(acc)
	bs := state.NewTokenTransferInfo()
	err := dao.GetAndDecode(bs, key)
	if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
		return nil, err
	}
	return bs, nil
}

// PutTokenTransferInfo saves NEP-17 transfer info in the cache.
func (dao *Simple) PutTokenTransferInfo(acc util.Uint160, bs *state.TokenTransferInfo) error {
	return dao.putTokenTransferInfo(acc, bs, dao.getDataBuf())
}

func (dao *Simple) putTokenTransferInfo(acc util.Uint160, bs *state.TokenTransferInfo, buf *io.BufBinWriter) error {
	return dao.putWithBuffer(bs, dao.makeTTIKey(acc), buf)
}
// -- end NEP-17 transfer info.

// -- start transfer log.

func (dao *Simple) getTokenTransferLogKey(acc util.Uint160, newestTimestamp uint64, index uint32, isNEP11 bool) []byte {
	key := dao.getKeyBuf(1 + util.Uint160Size + 8 + 4)
	if isNEP11 {
		key[0] = byte(storage.STNEP11Transfers)
	} else {
		key[0] = byte(storage.STNEP17Transfers)
	}
	copy(key[1:], acc.BytesBE())
	binary.BigEndian.PutUint64(key[1+util.Uint160Size:], newestTimestamp)
	binary.BigEndian.PutUint32(key[1+util.Uint160Size+8:], index)
	return key
}

// SeekNEP17TransferLog executes f for each NEP-17 transfer in log starting from
// the transfer with the newest timestamp up to the oldest transfer. It continues
// iteration until false is returned from f. The last non-nil error is returned.
func (dao *Simple) SeekNEP17TransferLog(acc util.Uint160, newestTimestamp uint64, f func(*state.NEP17Transfer) (bool, error)) error {
	key := dao.getTokenTransferLogKey(acc, newestTimestamp, 0, false)
	prefixLen := 1 + util.Uint160Size
	var seekErr error
	dao.Store.Seek(storage.SeekRange{
		Prefix:    key[:prefixLen],
		Start:     key[prefixLen : prefixLen+8],
		Backwards: true,
	}, func(k, v []byte) bool {
		lg := &state.TokenTransferLog{Raw: v}
		cont, err := lg.ForEachNEP17(f)
		if err != nil {
			seekErr = err
		}
		return cont
	})
	return seekErr
}

// SeekNEP11TransferLog executes f for each NEP-11 transfer in log starting from
// the transfer with the newest timestamp up to the oldest transfer. It continues
// iteration until false is returned from f. The last non-nil error is returned.
func (dao *Simple) SeekNEP11TransferLog(acc util.Uint160, newestTimestamp uint64, f func(*state.NEP11Transfer) (bool, error)) error {
	key := dao.getTokenTransferLogKey(acc, newestTimestamp, 0, true)
	prefixLen := 1 + util.Uint160Size
	var seekErr error
	dao.Store.Seek(storage.SeekRange{
		Prefix:    key[:prefixLen],
		Start:     key[prefixLen : prefixLen+8],
		Backwards: true,
	}, func(k, v []byte) bool {
		lg := &state.TokenTransferLog{Raw: v}
		cont, err := lg.ForEachNEP11(f)
		if err != nil {
			seekErr = err
		}
		return cont
	})
	return seekErr
}

// GetTokenTransferLog retrieves transfer log from the cache.
func (dao *Simple) GetTokenTransferLog(acc util.Uint160, newestTimestamp uint64, index uint32, isNEP11 bool) (*state.TokenTransferLog, error) {
	key := dao.getTokenTransferLogKey(acc, newestTimestamp, index, isNEP11)
	value, err := dao.Store.Get(key)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return new(state.TokenTransferLog), nil
		}
		return nil, err
	}
	return &state.TokenTransferLog{Raw: value}, nil
}

// PutTokenTransferLog saves the given transfer log in the cache.
func (dao *Simple) PutTokenTransferLog(acc util.Uint160, start uint64, index uint32, isNEP11 bool, lg *state.TokenTransferLog) {
	key := dao.getTokenTransferLogKey(acc, start, index, isNEP11)
	dao.Store.Put(key, lg.Raw)
}

// -- start NEP-17 transfer info.

func (dao *Simple) makeTTIKey(acc util.Uint160) []byte {
	key := dao.getKeyBuf(1 + util.Uint160Size)
	key[0] = byte(storage.STTokenTransferInfo)
	copy(key[1:], acc.BytesBE())
	return key
}



// -- end transfer log.