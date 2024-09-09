package core

import (
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/interop/contract"
	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
)

// newInteropContext creates a new interop context for the blockchain.
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