package core

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/mempool"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
)

// Various witness verification errors.
var (
	ErrWitnessHashMismatch         = errors.New("witness hash mismatch")
	ErrNativeContractWitness       = errors.New("native contract witness must have empty verification script")
	ErrVerificationFailed          = errors.New("signature check failed")
	ErrInvalidInvocationScript     = errors.New("invalid invocation script")
	ErrInvalidSignature            = fmt.Errorf("%w: invalid signature", ErrVerificationFailed)
	ErrInvalidVerificationScript   = errors.New("invalid verification script")
	ErrUnknownVerificationContract = errors.New("unknown verification contract")
	ErrInvalidVerificationContract = errors.New("verification contract is missing `verify` method or `verify` method has unexpected return value")
)

// InitVerificationContext initializes context for witness check.
func (bc *Blockchain) InitVerificationContext(ic *interop.Context, hash util.Uint160, witness *transaction.Witness) error {
	if len(witness.VerificationScript) != 0 {
		if witness.ScriptHash() != hash {
			return fmt.Errorf("%w: expected %s, got %s", ErrWitnessHashMismatch, hash.StringLE(), witness.ScriptHash().StringLE())
		}
		if bc.contracts.ByHash(hash) != nil {
			return ErrNativeContractWitness
		}
		err := vm.IsScriptCorrect(witness.VerificationScript, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidVerificationScript, err)
		}
		ic.VM.LoadScriptWithHash(witness.VerificationScript, hash, callflag.ReadOnly)
	} else {
		cs, err := ic.GetContract(hash)
		if err != nil {
			return ErrUnknownVerificationContract
		}
		md := cs.Manifest.ABI.GetMethod(manifest.MethodVerify, -1)
		if md == nil || md.ReturnType != smartcontract.BoolType {
			return ErrInvalidVerificationContract
		}
		verifyOffset := md.Offset
		initOffset := -1
		md = cs.Manifest.ABI.GetMethod(manifest.MethodInit, 0)
		if md != nil {
			initOffset = md.Offset
		}
		ic.Invocations[cs.Hash]++
		ic.VM.LoadNEFMethod(&cs.NEF, &cs.Manifest, util.Uint160{}, hash, callflag.ReadOnly,
			true, verifyOffset, initOffset, nil)
	}
	if len(witness.InvocationScript) != 0 {
		err := vm.IsScriptCorrect(witness.InvocationScript, nil)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidInvocationScript, err)
		}
		ic.VM.LoadScript(witness.InvocationScript)
	}
	return nil
}

// VerifyWitness checks that w is a correct witness for c signed by h. It returns
// the amount of GAS consumed during verification and an error.
func (bc *Blockchain) VerifyWitness(h util.Uint160, c hash.Hashable, w *transaction.Witness, gas int64) (int64, error) {
	ic := bc.newInteropContext(trigger.Verification, bc.dao, nil, nil)
	ic.Container = c
	if tx, ok := c.(*transaction.Transaction); ok {
		ic.Tx = tx
	}
	return bc.verifyHashAgainstScript(h, w, ic, gas)
}

// verifyHashAgainstScript verifies given hash against the given witness and returns the amount of GAS consumed.
func (bc *Blockchain) verifyHashAgainstScript(hash util.Uint160, witness *transaction.Witness, interopCtx *interop.Context, gas int64) (int64, error) {
	gas = min(gas, bc.contracts.Policy.GetMaxVerificationGas(interopCtx.DAO))

	vm := interopCtx.SpawnVM()
	vm.GasLimit = gas
	if err := bc.InitVerificationContext(interopCtx, hash, witness); err != nil {
		return 0, err
	}
	err := interopCtx.Exec()
	if vm.HasFailed() {
		return 0, fmt.Errorf("%w: vm execution has failed: %w", ErrVerificationFailed, err)
	}
	estack := vm.Estack()
	if estack.Len() > 0 {
		resEl := estack.Pop()
		res, err := resEl.Item().TryBool()
		if err != nil {
			return 0, fmt.Errorf("%w: invalid return value", ErrVerificationFailed)
		}
		if vm.Estack().Len() != 0 {
			return 0, fmt.Errorf("%w: expected exactly one returned value", ErrVerificationFailed)
		}
		if !res {
			return vm.GasConsumed(), ErrInvalidSignature
		}
	} else {
		return 0, fmt.Errorf("%w: no result returned from the script", ErrVerificationFailed)
	}
	return vm.GasConsumed(), nil
}

// verifyTxWitnesses verifies the scripts (witnesses) that come with a given
// transaction. It can reorder them by ScriptHash, because that's required to
// match a slice of script hashes from the Blockchain. Block parameter
// is used for easy interop access and can be omitted for transactions that are
// not yet added into any block. verificationFee argument can be provided to
// restrict the maximum amount of GAS allowed to spend on transaction
// verification.
// Golang implementation of VerifyWitnesses method in C# (https://github.com/neo-project/neo/blob/master/neo/SmartContract/Helper.cs#L87).
func (bc *Blockchain) verifyTxWitnesses(t *transaction.Transaction, block *block.Block, isPartialTx bool, verificationFee ...int64) error {
	interopCtx := bc.newInteropContext(trigger.Verification, bc.dao, block, t)
	var gasLimit int64
	if len(verificationFee) == 0 {
		gasLimit = t.NetworkFee - int64(t.Size())*bc.FeePerByte() - bc.CalculateAttributesFee(t)
	} else {
		gasLimit = verificationFee[0]
	}
	for i := range t.Signers {
		gasConsumed, err := bc.verifyHashAgainstScript(t.Signers[i].Account, &t.Scripts[i], interopCtx, gasLimit)
		if err != nil &&
			!(i == 0 && isPartialTx && errors.Is(err, ErrInvalidSignature)) { // it's OK for partially-filled transaction with dummy first witness.
			return fmt.Errorf("witness #%d: %w", i, err)
		}
		gasLimit -= gasConsumed
	}

	return nil
}

// verifyHeaderWitnesses is a block-specific implementation of VerifyWitnesses logic.
func (bc *Blockchain) verifyHeaderWitnesses(currHeader, prevHeader *block.Header) error {
	hash := prevHeader.NextConsensus
	_, err := bc.VerifyWitness(hash, currHeader, &currHeader.Script, HeaderVerificationGasLimit)
	return err
}


func (bc *Blockchain) verifyTxAttributes(d *dao.Simple, tx *transaction.Transaction, isPartialTx bool) error {
	for i := range tx.Attributes {
		switch attrType := tx.Attributes[i].Type; attrType {
		case transaction.HighPriority:
			h := bc.contracts.NEO.GetCommitteeAddress(d)
			if !tx.HasSigner(h) {
				return fmt.Errorf("%w: high priority tx is not signed by committee", ErrInvalidAttribute)
			}
		case transaction.OracleResponseT:
			h, err := bc.contracts.Oracle.GetScriptHash(bc.dao)
			if err != nil || h.Equals(util.Uint160{}) {
				return fmt.Errorf("%w: %w", ErrInvalidAttribute, err)
			}
			hasOracle := false
			for i := range tx.Signers {
				if tx.Signers[i].Scopes != transaction.None {
					return fmt.Errorf("%w: oracle tx has invalid signer scope", ErrInvalidAttribute)
				}
				if tx.Signers[i].Account.Equals(h) {
					hasOracle = true
				}
			}
			if !hasOracle {
				return fmt.Errorf("%w: oracle tx is not signed by oracle nodes", ErrInvalidAttribute)
			}
			if !bytes.Equal(tx.Script, bc.contracts.Oracle.GetOracleResponseScript()) {
				return fmt.Errorf("%w: oracle tx has invalid script", ErrInvalidAttribute)
			}
			resp := tx.Attributes[i].Value.(*transaction.OracleResponse)
			req, err := bc.contracts.Oracle.GetRequestInternal(bc.dao, resp.ID)
			if err != nil {
				return fmt.Errorf("%w: oracle tx points to invalid request: %w", ErrInvalidAttribute, err)
			}
			if uint64(tx.NetworkFee+tx.SystemFee) < req.GasForResponse {
				return fmt.Errorf("%w: oracle tx has insufficient gas", ErrInvalidAttribute)
			}
		case transaction.NotValidBeforeT:
			nvb := tx.Attributes[i].Value.(*transaction.NotValidBefore).Height
			curHeight := bc.BlockHeight()
			if isPartialTx {
				maxNVBDelta, err := bc.GetMaxNotValidBeforeDelta()
				if err != nil {
					return fmt.Errorf("%w: failed to retrieve MaxNotValidBeforeDelta value from native Notary contract: %w", ErrInvalidAttribute, err)
				}
				if curHeight+maxNVBDelta < nvb {
					return fmt.Errorf("%w: NotValidBefore (%d) bigger than MaxNVBDelta (%d) allows at height %d", ErrInvalidAttribute, nvb, maxNVBDelta, curHeight)
				}
				if nvb+maxNVBDelta < tx.ValidUntilBlock {
					return fmt.Errorf("%w: NotValidBefore (%d) set more than MaxNVBDelta (%d) away from VUB (%d)", ErrInvalidAttribute, nvb, maxNVBDelta, tx.ValidUntilBlock)
				}
			} else {
				if curHeight < nvb {
					return fmt.Errorf("%w: transaction is not yet valid: NotValidBefore = %d, current height = %d", ErrInvalidAttribute, nvb, curHeight)
				}
			}
		case transaction.ConflictsT:
			conflicts := tx.Attributes[i].Value.(*transaction.Conflicts)
			// Only fully-qualified dao.ErrAlreadyExists error bothers us here, thus, we
			// can safely omit the signers, current index and MTB arguments to HasTransaction call to improve performance a bit.
			if err := bc.dao.HasTransaction(conflicts.Hash, nil, 0, 0); errors.Is(err, dao.ErrAlreadyExists) {
				return fmt.Errorf("%w: conflicting transaction %s is already on chain", ErrInvalidAttribute, conflicts.Hash.StringLE())
			}
		case transaction.NotaryAssistedT:
			if !bc.config.P2PSigExtensions {
				return fmt.Errorf("%w: NotaryAssisted attribute was found, but P2PSigExtensions are disabled", ErrInvalidAttribute)
			}
			if !tx.HasSigner(bc.contracts.Notary.Hash) {
				return fmt.Errorf("%w: NotaryAssisted attribute was found, but transaction is not signed by the Notary native contract", ErrInvalidAttribute)
			}
		default:
			if !bc.config.ReservedAttributes && attrType >= transaction.ReservedLowerBound && attrType <= transaction.ReservedUpperBound {
				return fmt.Errorf("%w: attribute of reserved type was found, but ReservedAttributes are disabled", ErrInvalidAttribute)
			}
		}
	}
	return nil
}


// VerifyTx verifies whether transaction is bonafide or not relative to the
// current blockchain state. Note that this verification is completely isolated
// from the main node's mempool.
func (bc *Blockchain) VerifyTx(t *transaction.Transaction) error {
	var mp = mempool.New(1, 0, false, nil)
	bc.lock.RLock()
	defer bc.lock.RUnlock()
	return bc.verifyAndPoolTx(t, mp, bc)
}

// Various errors that could be returned upon verification.
var (
	ErrTxExpired         = errors.New("transaction has expired")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrTxSmallNetworkFee = errors.New("too small network fee")
	ErrTxTooBig          = errors.New("too big transaction")
	ErrMemPoolConflict   = errors.New("invalid transaction due to conflicts with the memory pool")
	ErrInvalidScript     = errors.New("invalid script")
	ErrInvalidAttribute  = errors.New("invalid attribute")
)

// verifyAndPoolTx verifies whether a transaction is bonafide or not and tries
// to add it to the mempool given.
func (bc *Blockchain) verifyAndPoolTx(t *transaction.Transaction, pool *mempool.Pool, feer mempool.Feer, data ...any) error {
	// This code can technically be moved out of here, because it doesn't
	// really require a chain lock.
	err := vm.IsScriptCorrect(t.Script, nil)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidScript, err)
	}

	height := bc.BlockHeight()
	isPartialTx := data != nil
	if t.ValidUntilBlock <= height || !isPartialTx && t.ValidUntilBlock > height+bc.config.MaxValidUntilBlockIncrement {
		return fmt.Errorf("%w: ValidUntilBlock = %d, current height = %d", ErrTxExpired, t.ValidUntilBlock, height)
	}
	// Policying.
	if err := bc.contracts.Policy.CheckPolicy(bc.dao, t); err != nil {
		// Only one %w can be used.
		return fmt.Errorf("%w: %w", ErrPolicy, err)
	}
	if t.SystemFee > bc.config.MaxBlockSystemFee {
		return fmt.Errorf("%w: too big system fee (%d > MaxBlockSystemFee %d)", ErrPolicy, t.SystemFee, bc.config.MaxBlockSystemFee)
	}
	size := t.Size()
	if size > transaction.MaxTransactionSize {
		return fmt.Errorf("%w: (%d > MaxTransactionSize %d)", ErrTxTooBig, size, transaction.MaxTransactionSize)
	}
	needNetworkFee := int64(size)*bc.FeePerByte() + bc.CalculateAttributesFee(t)
	netFee := t.NetworkFee - needNetworkFee
	if netFee < 0 {
		return fmt.Errorf("%w: net fee is %v, need %v", ErrTxSmallNetworkFee, t.NetworkFee, needNetworkFee)
	}
	// check that current tx wasn't included in the conflicts attributes of some other transaction which is already in the chain
	if err := bc.dao.HasTransaction(t.Hash(), t.Signers, height, bc.config.MaxTraceableBlocks); err != nil {
		switch {
		case errors.Is(err, dao.ErrAlreadyExists):
			return ErrAlreadyExists
		case errors.Is(err, dao.ErrHasConflicts):
			return fmt.Errorf("blockchain: %w", ErrHasConflicts)
		default:
			return err
		}
	}
	err = bc.verifyTxWitnesses(t, nil, isPartialTx, netFee)
	if err != nil {
		return err
	}
	if err := bc.verifyTxAttributes(bc.dao, t, isPartialTx); err != nil {
		return err
	}
	err = pool.Add(t, feer, data...)
	if err != nil {
		switch {
		case errors.Is(err, mempool.ErrConflict):
			return ErrMemPoolConflict
		case errors.Is(err, mempool.ErrDup):
			return ErrAlreadyInPool
		case errors.Is(err, mempool.ErrInsufficientFunds):
			return ErrInsufficientFunds
		case errors.Is(err, mempool.ErrOOM):
			return ErrOOM
		case errors.Is(err, mempool.ErrConflictsAttribute):
			return fmt.Errorf("mempool: %w: %w", ErrHasConflicts, err)
		default:
			return err
		}
	}

	return nil
}



func (bc *Blockchain) verifyHeader(currHeader, prevHeader *block.Header) error {
	if bc.config.StateRootInHeader {
		if bc.stateRoot.CurrentLocalHeight() == prevHeader.Index {
			if sr := bc.stateRoot.CurrentLocalStateRoot(); currHeader.PrevStateRoot != sr {
				return fmt.Errorf("%w: %s != %s",
					ErrHdrInvalidStateRoot, currHeader.PrevStateRoot.StringLE(), sr.StringLE())
			}
		}
	}
	if prevHeader.Hash() != currHeader.PrevHash {
		return ErrHdrHashMismatch
	}
	if prevHeader.Index+1 != currHeader.Index {
		return ErrHdrIndexMismatch
	}
	if prevHeader.Timestamp >= currHeader.Timestamp {
		return ErrHdrInvalidTimestamp
	}
	return bc.verifyHeaderWitnesses(currHeader, prevHeader)
}
