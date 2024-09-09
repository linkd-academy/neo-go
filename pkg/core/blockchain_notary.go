package core

import (
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// SetNotary sets notary module. It may safely be called on the running blockchain.
// To unregister Notary service use SetNotary(nil).
func (bc *Blockchain) SetNotary(mod native.NotaryService) {
	if mod != nil {
		keys, _, err := bc.GetDesignatedByRole(noderoles.P2PNotary)
		if err != nil {
			bc.log.Error("failed to get notary key list")
			return
		}
		mod.UpdateNotaryNodes(keys)
	}
	bc.contracts.Designate.NotaryService.Store(&mod)
}

// GetNotaryBalance returns Notary deposit amount for the specified account.
func (bc *Blockchain) GetNotaryBalance(acc util.Uint160) *big.Int {
	return bc.contracts.Notary.BalanceOf(bc.dao, acc)
}

// GetNotaryServiceFeePerKey returns a NotaryAssisted transaction attribute fee
// per key which is a reward per notary request key for designated notary nodes.
func (bc *Blockchain) GetNotaryServiceFeePerKey() int64 {
	return bc.contracts.Policy.GetAttributeFeeInternal(bc.dao, transaction.NotaryAssistedT)
}

// GetNotaryContractScriptHash returns Notary native contract hash.
func (bc *Blockchain) GetNotaryContractScriptHash() util.Uint160 {
	if bc.P2PSigExtensionsEnabled() {
		return bc.contracts.Notary.Hash
	}
	return util.Uint160{}
}

// GetNotaryDepositExpiration returns Notary deposit expiration height for the specified account.
func (bc *Blockchain) GetNotaryDepositExpiration(acc util.Uint160) uint32 {
	return bc.contracts.Notary.ExpirationOf(bc.dao, acc)
}

