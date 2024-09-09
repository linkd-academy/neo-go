package core

import (
	"errors"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

// GetContractState returns contract by its script hash.
func (bc *Blockchain) GetContractState(hash util.Uint160) *state.Contract {
	contract, err := native.GetContract(bc.dao, hash)
	if contract == nil && !errors.Is(err, storage.ErrKeyNotFound) {
		bc.log.Warn("failed to get contract state", zap.Error(err))
	}
	return contract
}

// GetContractScriptHash returns contract script hash by its ID.
func (bc *Blockchain) GetContractScriptHash(id int32) (util.Uint160, error) {
	return native.GetContractScriptHash(bc.dao, id)
}

// GetNativeContractScriptHash returns native contract script hash by its name.
func (bc *Blockchain) GetNativeContractScriptHash(name string) (util.Uint160, error) {
	c := bc.contracts.ByName(name)
	if c != nil {
		return c.Metadata().Hash, nil
	}
	return util.Uint160{}, errors.New("Unknown native contract")
}

// GetNatives returns list of native contracts.
func (bc *Blockchain) GetNatives() []state.Contract {
	res := make([]state.Contract, 0, len(bc.contracts.Contracts))
	current := bc.getCurrentHF()
	for _, c := range bc.contracts.Contracts {
		activeIn := c.ActiveIn()
		if !(activeIn == nil || activeIn.Cmp(current) <= 0) {
			continue
		}

		st := bc.GetContractState(c.Metadata().Hash)
		if st != nil { // Should never happen, but better safe than sorry.
			res = append(res, *st)
		}
	}
	return res
}


// GetUtilityTokenBalance returns utility token (GAS) balance for the acc.
func (bc *Blockchain) GetUtilityTokenBalance(acc util.Uint160) *big.Int {
	bs := bc.contracts.GAS.BalanceOf(bc.dao, acc)
	if bs == nil {
		return big.NewInt(0)
	}
	return bs
}

// GetGoverningTokenBalance returns governing token (NEO) balance and the height
// of the last balance change for the account.
func (bc *Blockchain) GetGoverningTokenBalance(acc util.Uint160) (*big.Int, uint32) {
	return bc.contracts.NEO.BalanceOf(bc.dao, acc)
}


// GetNEP17Contracts returns the list of deployed NEP-17 contracts.
func (bc *Blockchain) GetNEP17Contracts() []util.Uint160 {
	return bc.contracts.Management.GetNEP17Contracts(bc.dao)
}

// GetNEP11Contracts returns the list of deployed NEP-11 contracts.
func (bc *Blockchain) GetNEP11Contracts() []util.Uint160 {
	return bc.contracts.Management.GetNEP11Contracts(bc.dao)
}