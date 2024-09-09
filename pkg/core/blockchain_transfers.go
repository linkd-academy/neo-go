package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"

	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

// resetTransfers is a helper function that strips the top newest NEP17 and NEP11 transfer logs
// down to the given height (not including the height itself) and updates corresponding token
// transfer info.
func (bc *Blockchain) resetTransfers(cache *dao.Simple, height uint32) error {
	// Completely remove transfer info, updating it takes too much effort. We'll gather new
	// transfer info on-the-fly later.
	cache.Store.Seek(storage.SeekRange{
		Prefix: []byte{byte(storage.STTokenTransferInfo)},
	}, func(k, v []byte) bool {
		cache.Store.Delete(k)
		return true
	})

	// Look inside each transfer batch and iterate over the batch transfers, picking those that
	// not newer than the given height. Also, for each suitable transfer update transfer info
	// flushing changes after complete account's transfers processing.
	prefixes := []byte{byte(storage.STNEP11Transfers), byte(storage.STNEP17Transfers)}
	for i := range prefixes {
		var (
			acc             util.Uint160
			trInfo          *state.TokenTransferInfo
			removeFollowing bool
			seekErr         error
		)

		cache.Store.Seek(storage.SeekRange{
			Prefix:    prefixes[i : i+1],
			Backwards: false, // From oldest to newest batch.
		}, func(k, v []byte) bool {
			var batchAcc util.Uint160
			copy(batchAcc[:], k[1:])

			if batchAcc != acc { // Some new account we're iterating over.
				if trInfo != nil {
					seekErr = cache.PutTokenTransferInfo(acc, trInfo)
					if seekErr != nil {
						return false
					}
				}
				acc = batchAcc
				trInfo = nil
				removeFollowing = false
			} else if removeFollowing {
				cache.Store.Delete(bytes.Clone(k))
				return seekErr == nil
			}

			r := io.NewBinReaderFromBuf(v[1:])
			l := len(v)
			bytesRead := 1 // 1 is for batch size byte which is read by default.
			var (
				oldBatchSize = v[0]
				newBatchSize byte
			)
			for range v[0] { // From oldest to newest transfer of the batch.
				var t *state.NEP17Transfer
				if k[0] == byte(storage.STNEP11Transfers) {
					tr := new(state.NEP11Transfer)
					tr.DecodeBinary(r)
					t = &tr.NEP17Transfer
				} else {
					t = new(state.NEP17Transfer)
					t.DecodeBinary(r)
				}
				if r.Err != nil {
					seekErr = fmt.Errorf("failed to decode subsequent transfer: %w", r.Err)
					break
				}

				if t.Block > height {
					break
				}
				bytesRead = l - r.Len() // Including batch size byte.
				newBatchSize++
				if trInfo == nil {
					var err error
					trInfo, err = cache.GetTokenTransferInfo(batchAcc)
					if err != nil {
						seekErr = fmt.Errorf("failed to retrieve token transfer info for %s: %w", batchAcc.StringLE(), r.Err)
						return false
					}
				}
				appendTokenTransferInfo(trInfo, t.Asset, t.Block, t.Timestamp, k[0] == byte(storage.STNEP11Transfers), newBatchSize >= state.TokenTransferBatchSize)
			}
			if newBatchSize == oldBatchSize {
				// The batch is already in storage and doesn't need to be changed.
				return seekErr == nil
			}
			if newBatchSize > 0 {
				v[0] = newBatchSize
				cache.Store.Put(k, v[:bytesRead])
			} else {
				cache.Store.Delete(k)
				removeFollowing = true
			}
			return seekErr == nil
		})
		if seekErr != nil {
			return seekErr
		}
		if trInfo != nil {
			// Flush the last batch of transfer info changes.
			err := cache.PutTokenTransferInfo(acc, trInfo)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// appendTokenTransferInfo is a helper for resetTransfers that updates token transfer info
// wrt the given transfer that was added to the subsequent transfer batch.
func appendTokenTransferInfo(transferData *state.TokenTransferInfo,
	token int32, bIndex uint32, bTimestamp uint64, isNEP11 bool, lastTransferInBatch bool) {
	var (
		newBatch      *bool
		nextBatch     *uint32
		currTimestamp *uint64
	)
	if !isNEP11 {
		newBatch = &transferData.NewNEP17Batch
		nextBatch = &transferData.NextNEP17Batch
		currTimestamp = &transferData.NextNEP17NewestTimestamp
	} else {
		newBatch = &transferData.NewNEP11Batch
		nextBatch = &transferData.NextNEP11Batch
		currTimestamp = &transferData.NextNEP11NewestTimestamp
	}
	transferData.LastUpdated[token] = bIndex
	*newBatch = lastTransferInBatch
	if *newBatch {
		*nextBatch++
		*currTimestamp = bTimestamp
	}
}

func (bc *Blockchain) removeOldTransfers(index uint32) time.Duration {
	bc.log.Info("starting transfer data garbage collection", zap.Uint32("index", index))
	start := time.Now()
	h, err := bc.GetHeader(bc.GetHeaderHash(index))
	if err != nil {
		dur := time.Since(start)
		bc.log.Error("failed to find block header for transfer GC", zap.Duration("time", dur), zap.Error(err))
		return dur
	}
	var removed, kept int64
	var ts = h.Timestamp
	prefixes := []byte{byte(storage.STNEP11Transfers), byte(storage.STNEP17Transfers)}

	for i := range prefixes {
		var acc util.Uint160
		var canDrop bool

		err = bc.store.SeekGC(storage.SeekRange{
			Prefix:    prefixes[i : i+1],
			Backwards: true, // From new to old.
		}, func(k, v []byte) bool {
			// We don't look inside of the batches, it requires too much effort, instead
			// we drop batches that are confirmed to contain outdated entries.
			var batchAcc util.Uint160
			var batchTs = binary.BigEndian.Uint64(k[1+util.Uint160Size:])
			copy(batchAcc[:], k[1:])

			if batchAcc != acc { // Some new account we're iterating over.
				acc = batchAcc
			} else if canDrop { // We've seen this account and all entries in this batch are guaranteed to be outdated.
				removed++
				return false
			}
			// We don't know what's inside, so keep the current
			// batch anyway, but allow to drop older ones.
			canDrop = batchTs <= ts
			kept++
			return true
		})
		if err != nil {
			break
		}
	}
	dur := time.Since(start)
	if err != nil {
		bc.log.Error("failed to flush transfer data GC changeset", zap.Duration("time", dur), zap.Error(err))
	} else {
		bc.log.Info("finished transfer data garbage collection",
			zap.Int64("removed", removed),
			zap.Int64("kept", kept),
			zap.Duration("time", dur))
	}
	return dur
}

func (bc *Blockchain) processTokenTransfer(cache *dao.Simple, transCache map[util.Uint160]transferData,
	h util.Uint256, b *block.Block, sc util.Uint160, from util.Uint160, to util.Uint160,
	amount *big.Int, tokenID []byte) {
	var id int32
	nativeContract := bc.contracts.ByHash(sc)
	if nativeContract != nil {
		id = nativeContract.Metadata().ID
	} else {
		assetContract, err := native.GetContract(cache, sc)
		if err != nil {
			return
		}
		id = assetContract.ID
	}
	var transfer io.Serializable
	var nep17xfer *state.NEP17Transfer
	var isNEP11 = (tokenID != nil)
	if !isNEP11 {
		nep17xfer = &state.NEP17Transfer{
			Asset:        id,
			Amount:       amount,
			Block:        b.Index,
			Counterparty: to,
			Timestamp:    b.Timestamp,
			Tx:           h,
		}
		transfer = nep17xfer
	} else {
		nep11xfer := &state.NEP11Transfer{
			NEP17Transfer: state.NEP17Transfer{
				Asset:        id,
				Amount:       amount,
				Block:        b.Index,
				Counterparty: to,
				Timestamp:    b.Timestamp,
				Tx:           h,
			},
			ID: tokenID,
		}
		transfer = nep11xfer
		nep17xfer = &nep11xfer.NEP17Transfer
	}
	if !from.Equals(util.Uint160{}) {
		_ = nep17xfer.Amount.Neg(nep17xfer.Amount)
		err := appendTokenTransfer(cache, transCache, from, transfer, id, b.Index, b.Timestamp, isNEP11)
		_ = nep17xfer.Amount.Neg(nep17xfer.Amount)
		if err != nil {
			return
		}
	}
	if !to.Equals(util.Uint160{}) {
		nep17xfer.Counterparty = from
		_ = appendTokenTransfer(cache, transCache, to, transfer, id, b.Index, b.Timestamp, isNEP11) // Nothing useful we can do.
	}
}

func appendTokenTransfer(cache *dao.Simple, transCache map[util.Uint160]transferData, addr util.Uint160, transfer io.Serializable,
	token int32, bIndex uint32, bTimestamp uint64, isNEP11 bool) error {
	transferData, ok := transCache[addr]
	if !ok {
		balances, err := cache.GetTokenTransferInfo(addr)
		if err != nil {
			return err
		}
		if !balances.NewNEP11Batch {
			trLog, err := cache.GetTokenTransferLog(addr, balances.NextNEP11NewestTimestamp, balances.NextNEP11Batch, true)
			if err != nil {
				return err
			}
			transferData.Log11 = *trLog
		}
		if !balances.NewNEP17Batch {
			trLog, err := cache.GetTokenTransferLog(addr, balances.NextNEP17NewestTimestamp, balances.NextNEP17Batch, false)
			if err != nil {
				return err
			}
			transferData.Log17 = *trLog
		}
		transferData.Info = *balances
	}
	var (
		log           *state.TokenTransferLog
		nextBatch     uint32
		currTimestamp uint64
	)
	if !isNEP11 {
		log = &transferData.Log17
		nextBatch = transferData.Info.NextNEP17Batch
		currTimestamp = transferData.Info.NextNEP17NewestTimestamp
	} else {
		log = &transferData.Log11
		nextBatch = transferData.Info.NextNEP11Batch
		currTimestamp = transferData.Info.NextNEP11NewestTimestamp
	}
	err := log.Append(transfer)
	if err != nil {
		return err
	}
	newBatch := log.Size() >= state.TokenTransferBatchSize
	if newBatch {
		cache.PutTokenTransferLog(addr, currTimestamp, nextBatch, isNEP11, log)
		// Put makes a copy of it anyway.
		log.Reset()
	}
	appendTokenTransferInfo(&transferData.Info, token, bIndex, bTimestamp, isNEP11, newBatch)
	transCache[addr] = transferData
	return nil
}

// ForEachNEP17Transfer executes f for each NEP-17 transfer in log starting from
// the transfer with the newest timestamp up to the oldest transfer. It continues
// iteration until false is returned from f. The last non-nil error is returned.
func (bc *Blockchain) ForEachNEP17Transfer(acc util.Uint160, newestTimestamp uint64, f func(*state.NEP17Transfer) (bool, error)) error {
	return bc.dao.SeekNEP17TransferLog(acc, newestTimestamp, f)
}

// ForEachNEP11Transfer executes f for each NEP-11 transfer in log starting from
// the transfer with the newest timestamp up to the oldest transfer. It continues
// iteration until false is returned from f. The last non-nil error is returned.
func (bc *Blockchain) ForEachNEP11Transfer(acc util.Uint160, newestTimestamp uint64, f func(*state.NEP11Transfer) (bool, error)) error {
	return bc.dao.SeekNEP11TransferLog(acc, newestTimestamp, f)
}