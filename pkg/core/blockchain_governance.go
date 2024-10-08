package core

import (
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// GetBaseExecFee return execution price for `NOP`.
func (bc *Blockchain) GetBaseExecFee() int64 {
	if bc.BlockHeight() == 0 {
		return interop.DefaultBaseExecFee
	}
	return bc.contracts.Policy.GetExecFeeFactorInternal(bc.dao)
}

// GetMaxVerificationGAS returns maximum verification GAS Policy limit.
func (bc *Blockchain) GetMaxVerificationGAS() int64 {
	return bc.contracts.Policy.GetMaxVerificationGas(bc.dao)
}

// GetMaxNotValidBeforeDelta returns maximum NotValidBeforeDelta Notary limit.
func (bc *Blockchain) GetMaxNotValidBeforeDelta() (uint32, error) {
	if !bc.config.P2PSigExtensions {
		panic("disallowed call to Notary") // critical error, thus panic.
	}
	if !bc.isHardforkEnabled(bc.contracts.Notary.ActiveIn(), bc.BlockHeight()) {
		return 0, fmt.Errorf("native Notary is active starting from %s", bc.contracts.Notary.ActiveIn().String())
	}
	return bc.contracts.Notary.GetMaxNotValidBeforeDelta(bc.dao), nil
}

// GetStoragePrice returns current storage price.
func (bc *Blockchain) GetStoragePrice() int64 {
	if bc.BlockHeight() == 0 {
		return native.DefaultStoragePrice
	}
	return bc.contracts.Policy.GetStoragePriceInternal(bc.dao)
}


// GoverningTokenHash returns the governing token (NEO) native contract hash.
func (bc *Blockchain) GoverningTokenHash() util.Uint160 {
	return bc.contracts.NEO.Hash
}

// UtilityTokenHash returns the utility token (GAS) native contract hash.
func (bc *Blockchain) UtilityTokenHash() util.Uint160 {
	return bc.contracts.GAS.Hash
}

// ManagementContractHash returns management contract's hash.
func (bc *Blockchain) ManagementContractHash() util.Uint160 {
	return bc.contracts.Management.Hash
}


// ApplyPolicyToTxSet applies configured policies to given transaction set. It
// expects slice to be ordered by fee and returns a subslice of it.
func (bc *Blockchain) ApplyPolicyToTxSet(txes []*transaction.Transaction) []*transaction.Transaction {
	maxTx := bc.config.MaxTransactionsPerBlock
	if maxTx != 0 && len(txes) > int(maxTx) {
		txes = txes[:maxTx]
	}
	maxBlockSize := bc.config.MaxBlockSize
	maxBlockSysFee := bc.config.MaxBlockSystemFee
	oldVC := bc.knownValidatorsCount.Load()
	defaultWitness := bc.defaultBlockWitness.Load()
	curVC := bc.config.GetNumOfCNs(bc.BlockHeight() + 1)
	if oldVC == nil || oldVC != curVC {
		m := smartcontract.GetDefaultHonestNodeCount(curVC)
		verification, _ := smartcontract.CreateDefaultMultiSigRedeemScript(bc.contracts.NEO.GetNextBlockValidatorsInternal(bc.dao))
		defaultWitness = transaction.Witness{
			InvocationScript:   make([]byte, 66*m),
			VerificationScript: verification,
		}
		bc.knownValidatorsCount.Store(curVC)
		bc.defaultBlockWitness.Store(defaultWitness)
	}
	var (
		b           = &block.Block{Header: block.Header{Script: defaultWitness.(transaction.Witness)}}
		blockSize   = uint32(b.GetExpectedBlockSizeWithoutTransactions(len(txes)))
		blockSysFee int64
	)
	for i, tx := range txes {
		blockSize += uint32(tx.Size())
		blockSysFee += tx.SystemFee
		if blockSize > maxBlockSize || blockSysFee > maxBlockSysFee {
			txes = txes[:i]
			break
		}
	}
	return txes
}


// GetCommittee returns the sorted list of public keys of nodes in committee.
func (bc *Blockchain) GetCommittee() (keys.PublicKeys, error) {
	pubs := bc.contracts.NEO.GetCommitteeMembers(bc.dao)
	slices.SortFunc(pubs, (*keys.PublicKey).Cmp)
	return pubs, nil
}

// ComputeNextBlockValidators returns current validators. Validators list
// returned from this method is updated once per CommitteeSize number of blocks.
// For the last block in the dBFT epoch this method returns the list of validators
// recalculated from the latest relevant information about NEO votes; in this case
// list of validators may differ from the one returned by GetNextBlockValidators.
// For the not-last block of dBFT epoch this method returns the same list as
// GetNextBlockValidators.
func (bc *Blockchain) ComputeNextBlockValidators() []*keys.PublicKey {
	return bc.contracts.NEO.ComputeNextBlockValidators(bc.dao)
}

// GetNextBlockValidators returns next block validators. Validators list returned
// from this method is the sorted top NumOfCNs number of public keys from the
// committee of the current dBFT round (that was calculated once for the
// CommitteeSize number of blocks), thus, validators list returned from this
// method is being updated once per (committee size) number of blocks, but not
// every block.
func (bc *Blockchain) GetNextBlockValidators() ([]*keys.PublicKey, error) {
	return bc.contracts.NEO.GetNextBlockValidatorsInternal(bc.dao), nil
}

// GetEnrollments returns all registered validators.
func (bc *Blockchain) GetEnrollments() ([]state.Validator, error) {
	return bc.contracts.NEO.GetCandidates(bc.dao)
}


// FeePerByte returns transaction network fee per byte.
func (bc *Blockchain) FeePerByte() int64 {
	return bc.contracts.Policy.GetFeePerByteInternal(bc.dao)
}
