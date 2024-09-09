package rpcsrv

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/interop/iterator"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv/params"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// invokeFunction implements the `invokeFunction` RPC call.
func (s *Server) invokeFunction(reqParams params.Params) (any, *neorpc.Error) {
	tx, verbose, respErr := s.getInvokeFunctionParams(reqParams)
	if respErr != nil {
		return nil, respErr
	}
	return s.runScriptInVM(trigger.Application, tx.Script, util.Uint160{}, tx, nil, verbose)
}

// invokeFunctionHistoric implements the `invokeFunctionHistoric` RPC call.
func (s *Server) invokeFunctionHistoric(reqParams params.Params) (any, *neorpc.Error) {
	nextH, respErr := s.getHistoricParams(reqParams)
	if respErr != nil {
		return nil, respErr
	}
	if len(reqParams) < 2 {
		return nil, neorpc.ErrInvalidParams
	}
	tx, verbose, respErr := s.getInvokeFunctionParams(reqParams[1:])
	if respErr != nil {
		return nil, respErr
	}
	return s.runScriptInVM(trigger.Application, tx.Script, util.Uint160{}, tx, &nextH, verbose)
}

func (s *Server) getInvokeFunctionParams(reqParams params.Params) (*transaction.Transaction, bool, *neorpc.Error) {
	if len(reqParams) < 2 {
		return nil, false, neorpc.ErrInvalidParams
	}
	scriptHash, responseErr := s.contractScriptHashFromParam(reqParams.Value(0))
	if responseErr != nil {
		return nil, false, responseErr
	}
	method, err := reqParams[1].GetString()
	if err != nil {
		return nil, false, neorpc.ErrInvalidParams
	}
	var invparams *params.Param
	if len(reqParams) > 2 {
		invparams = &reqParams[2]
	}
	tx := &transaction.Transaction{}
	if len(reqParams) > 3 {
		signers, _, err := reqParams[3].GetSignersWithWitnesses()
		if err != nil {
			return nil, false, neorpc.ErrInvalidParams
		}
		tx.Signers = signers
	}
	var verbose bool
	if len(reqParams) > 4 {
		verbose, err = reqParams[4].GetBoolean()
		if err != nil {
			return nil, false, neorpc.ErrInvalidParams
		}
	}
	if len(tx.Signers) == 0 {
		tx.Signers = []transaction.Signer{{Account: util.Uint160{}, Scopes: transaction.None}}
	}
	script, err := params.CreateFunctionInvocationScript(scriptHash, method, invparams)
	if err != nil {
		return nil, false, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("can't create invocation script: %s", err))
	}
	tx.Script = script
	return tx, verbose, nil
}

// invokescript implements the `invokescript` RPC call.
func (s *Server) invokescript(reqParams params.Params) (any, *neorpc.Error) {
	tx, verbose, respErr := s.getInvokeScriptParams(reqParams)
	if respErr != nil {
		return nil, respErr
	}
	return s.runScriptInVM(trigger.Application, tx.Script, util.Uint160{}, tx, nil, verbose)
}

// invokescripthistoric implements the `invokescripthistoric` RPC call.
func (s *Server) invokescripthistoric(reqParams params.Params) (any, *neorpc.Error) {
	nextH, respErr := s.getHistoricParams(reqParams)
	if respErr != nil {
		return nil, respErr
	}
	if len(reqParams) < 2 {
		return nil, neorpc.ErrInvalidParams
	}
	tx, verbose, respErr := s.getInvokeScriptParams(reqParams[1:])
	if respErr != nil {
		return nil, respErr
	}
	return s.runScriptInVM(trigger.Application, tx.Script, util.Uint160{}, tx, &nextH, verbose)
}

func (s *Server) getInvokeScriptParams(reqParams params.Params) (*transaction.Transaction, bool, *neorpc.Error) {
	script, err := reqParams.Value(0).GetBytesBase64()
	if err != nil {
		return nil, false, neorpc.ErrInvalidParams
	}

	tx := &transaction.Transaction{}
	if len(reqParams) > 1 {
		signers, witnesses, err := reqParams[1].GetSignersWithWitnesses()
		if err != nil {
			return nil, false, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
		}
		tx.Signers = signers
		tx.Scripts = witnesses
	}
	var verbose bool
	if len(reqParams) > 2 {
		verbose, err = reqParams[2].GetBoolean()
		if err != nil {
			return nil, false, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
		}
	}
	if len(tx.Signers) == 0 {
		tx.Signers = []transaction.Signer{{Account: util.Uint160{}, Scopes: transaction.None}}
	}
	tx.Script = script
	return tx, verbose, nil
}

// invokeContractVerify implements the `invokecontractverify` RPC call.
func (s *Server) invokeContractVerify(reqParams params.Params) (any, *neorpc.Error) {
	scriptHash, tx, invocationScript, respErr := s.getInvokeContractVerifyParams(reqParams)
	if respErr != nil {
		return nil, respErr
	}
	return s.runScriptInVM(trigger.Verification, invocationScript, scriptHash, tx, nil, false)
}

// invokeContractVerifyHistoric implements the `invokecontractverifyhistoric` RPC call.
func (s *Server) invokeContractVerifyHistoric(reqParams params.Params) (any, *neorpc.Error) {
	nextH, respErr := s.getHistoricParams(reqParams)
	if respErr != nil {
		return nil, respErr
	}
	if len(reqParams) < 2 {
		return nil, neorpc.ErrInvalidParams
	}
	scriptHash, tx, invocationScript, respErr := s.getInvokeContractVerifyParams(reqParams[1:])
	if respErr != nil {
		return nil, respErr
	}
	return s.runScriptInVM(trigger.Verification, invocationScript, scriptHash, tx, &nextH, false)
}

func (s *Server) getInvokeContractVerifyParams(reqParams params.Params) (util.Uint160, *transaction.Transaction, []byte, *neorpc.Error) {
	scriptHash, responseErr := s.contractScriptHashFromParam(reqParams.Value(0))
	if responseErr != nil {
		return util.Uint160{}, nil, nil, responseErr
	}

	bw := io.NewBufBinWriter()
	if len(reqParams) > 1 {
		args, err := reqParams[1].GetArray() // second `invokecontractverify` parameter is an array of arguments for `verify` method
		if err != nil {
			return util.Uint160{}, nil, nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
		}
		if len(args) > 0 {
			err := params.ExpandArrayIntoScript(bw.BinWriter, args)
			if err != nil {
				return util.Uint160{}, nil, nil, neorpc.NewInternalServerError(fmt.Sprintf("can't create witness invocation script: %s", err))
			}
		}
	}
	invocationScript := bw.Bytes()

	tx := &transaction.Transaction{Script: []byte{byte(opcode.RET)}} // need something in script
	if len(reqParams) > 2 {
		signers, witnesses, err := reqParams[2].GetSignersWithWitnesses()
		if err != nil {
			return util.Uint160{}, nil, nil, neorpc.ErrInvalidParams
		}
		tx.Signers = signers
		tx.Scripts = witnesses
	} else { // fill the only known signer - the contract with `verify` method
		tx.Signers = []transaction.Signer{{Account: scriptHash}}
		tx.Scripts = []transaction.Witness{{InvocationScript: invocationScript, VerificationScript: []byte{}}}
	}
	return scriptHash, tx, invocationScript, nil
}

// getHistoricParams checks that historic calls are supported and returns index of
// a fake next block to perform the historic call. It also checks that
// specified stateroot is stored at the specified height for further request
// handling consistency.
func (s *Server) getHistoricParams(reqParams params.Params) (uint32, *neorpc.Error) {
	if s.chain.GetConfig().Ledger.KeepOnlyLatestState {
		return 0, neorpc.WrapErrorWithData(neorpc.ErrUnsupportedState, fmt.Sprintf("only latest state is supported: %s", errKeepOnlyLatestState))
	}
	if len(reqParams) < 1 {
		return 0, neorpc.ErrInvalidParams
	}
	height, respErr := s.blockHeightFromParam(reqParams.Value(0))
	if respErr != nil {
		hash, err := reqParams.Value(0).GetUint256()
		if err != nil {
			return 0, neorpc.NewInvalidParamsError(fmt.Sprintf("invalid block hash or index or stateroot hash: %s", err))
		}
		b, err := s.chain.GetBlock(hash)
		if err != nil {
			stateH, err := s.chain.GetStateModule().GetLatestStateHeight(hash)
			if err != nil {
				return 0, neorpc.NewInvalidParamsError(fmt.Sprintf("unknown block or stateroot: %s", err))
			}
			height = stateH
		} else {
			height = b.Index
		}
	}
	return height + 1, nil
}


func (s *Server) prepareInvocationContext(t trigger.Type, script []byte, contractScriptHash util.Uint160, tx *transaction.Transaction, nextH *uint32, verbose bool) (*interop.Context, *neorpc.Error) {
	var (
		err error
		ic  *interop.Context
	)
	if nextH == nil {
		ic, err = s.chain.GetTestVM(t, tx, nil)
		if err != nil {
			return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to create test VM: %s", err))
		}
	} else {
		ic, err = s.chain.GetTestHistoricVM(t, tx, *nextH)
		if err != nil {
			return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to create historic VM: %s", err))
		}
	}
	if verbose {
		ic.VM.EnableInvocationTree()
	}
	ic.VM.GasLimit = int64(s.config.MaxGasInvoke)
	if t == trigger.Verification {
		// We need this special case because witnesses verification is not the simple System.Contract.Call,
		// and we need to define exactly the amount of gas consumed for a contract witness verification.
		ic.VM.GasLimit = min(ic.VM.GasLimit, s.chain.GetMaxVerificationGAS())

		err = s.chain.InitVerificationContext(ic, contractScriptHash, &transaction.Witness{InvocationScript: script, VerificationScript: []byte{}})
		if err != nil {
			switch {
			case errors.Is(err, core.ErrUnknownVerificationContract):
				return nil, neorpc.WrapErrorWithData(neorpc.ErrUnknownContract, err.Error())
			case errors.Is(err, core.ErrInvalidVerificationContract):
				return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidVerificationFunction, err.Error())
			default:
				return nil, neorpc.NewInternalServerError(fmt.Sprintf("can't prepare verification VM: %s", err))
			}
		}
	} else {
		ic.VM.LoadScriptWithFlags(script, callflag.All)
	}
	return ic, nil
}

// runScriptInVM runs the given script in a new test VM and returns the invocation
// result. The script is either a simple script in case of `application` trigger,
// witness invocation script in case of `verification` trigger (it pushes `verify`
// arguments on stack before verification). In case of contract verification
// contractScriptHash should be specified.
func (s *Server) runScriptInVM(t trigger.Type, script []byte, contractScriptHash util.Uint160, tx *transaction.Transaction, nextH *uint32, verbose bool) (*result.Invoke, *neorpc.Error) {
	ic, respErr := s.prepareInvocationContext(t, script, contractScriptHash, tx, nextH, verbose)
	if respErr != nil {
		return nil, respErr
	}
	err := ic.VM.Run()
	var faultException string
	if err != nil {
		faultException = err.Error()
	}
	items := ic.VM.Estack().ToArray()
	sess := s.postProcessExecStack(items)
	var id uuid.UUID

	if sess != nil {
		// nextH == nil only when we're not using MPT-backed storage, therefore
		// the second attempt won't stop here.
		if s.config.SessionBackedByMPT && nextH == nil {
			ic.Finalize()
			// Rerun with MPT-backed storage.
			return s.runScriptInVM(t, script, contractScriptHash, tx, &ic.Block.Index, verbose)
		}
		id = uuid.New()
		sessionID := id.String()
		sess.finalize = ic.Finalize
		sess.timer = time.AfterFunc(time.Second*time.Duration(s.config.SessionExpirationTime), func() {
			s.sessionsLock.Lock()
			defer s.sessionsLock.Unlock()
			if len(s.sessions) == 0 {
				return
			}
			sess, ok := s.sessions[sessionID]
			if !ok {
				return
			}
			sess.iteratorsLock.Lock()
			sess.finalize()
			delete(s.sessions, sessionID)
			sess.iteratorsLock.Unlock()
		})
		s.sessionsLock.Lock()
		if len(s.sessions) >= s.config.SessionPoolSize {
			ic.Finalize()
			s.sessionsLock.Unlock()
			return nil, neorpc.NewInternalServerError("max session capacity reached")
		}
		s.sessions[sessionID] = sess
		s.sessionsLock.Unlock()
	} else {
		ic.Finalize()
	}
	var diag *result.InvokeDiag
	tree := ic.VM.GetInvocationTree()
	if tree != nil {
		diag = &result.InvokeDiag{
			Invocations: tree.Calls,
			Changes:     storage.BatchToOperations(ic.DAO.GetBatch()),
		}
	}
	notifications := ic.Notifications
	if notifications == nil {
		notifications = make([]state.NotificationEvent, 0)
	}
	res := &result.Invoke{
		State:          ic.VM.State().String(),
		GasConsumed:    ic.VM.GasConsumed(),
		Script:         script,
		Stack:          items,
		FaultException: faultException,
		Notifications:  notifications,
		Diagnostics:    diag,
		Session:        id,
	}

	return res, nil
}

// postProcessExecStack changes iterator interop items according to the server configuration.
// It does modifications in-place, but it returns a session if any iterator was registered.
func (s *Server) postProcessExecStack(stack []stackitem.Item) *session {
	var sess session

	for i, v := range stack {
		var id uuid.UUID

		stack[i], id = s.registerOrDumpIterator(v)
		if id != (uuid.UUID{}) {
			sess.iteratorIdentifiers = append(sess.iteratorIdentifiers, &iteratorIdentifier{
				ID:   id.String(),
				Item: v,
			})
		}
	}
	if len(sess.iteratorIdentifiers) != 0 {
		return &sess
	}
	return nil
}

// registerOrDumpIterator changes iterator interop stack items into result.Iterator
// interop stack items and returns a uuid for it if sessions are enabled. All the other stack
// items are not changed.
func (s *Server) registerOrDumpIterator(item stackitem.Item) (stackitem.Item, uuid.UUID) {
	var iterID uuid.UUID

	if (item.Type() != stackitem.InteropT) || !iterator.IsIterator(item) {
		return item, iterID
	}
	var resIterator result.Iterator

	if s.config.SessionEnabled {
		iterID = uuid.New()
		resIterator.ID = &iterID
	} else {
		resIterator.Values, resIterator.Truncated = iterator.ValuesTruncated(item, s.config.MaxIteratorResultItems)
	}
	return stackitem.NewInterop(resIterator), iterID
}

func (s *Server) traverseIterator(reqParams params.Params) (any, *neorpc.Error) {
	if !s.config.SessionEnabled {
		return nil, neorpc.ErrSessionsDisabled
	}
	sID, err := reqParams.Value(0).GetUUID()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("invalid session ID: %s", err))
	}
	iID, err := reqParams.Value(1).GetUUID()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("invalid iterator ID: %s", err))
	}
	count, err := reqParams.Value(2).GetInt()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("invalid iterator items count: %s", err))
	}
	if err := checkInt32(count); err != nil {
		return nil, neorpc.NewInvalidParamsError("invalid iterator items count: not an int32")
	}
	if count > s.config.MaxIteratorResultItems {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("iterator items count (%d) is out of range (%d at max)", count, s.config.MaxIteratorResultItems))
	}

	s.sessionsLock.Lock()
	session, ok := s.sessions[sID.String()]
	if !ok {
		s.sessionsLock.Unlock()
		return nil, neorpc.ErrUnknownSession
	}
	session.iteratorsLock.Lock()
	// Perform `till` update only after session.iteratorsLock is taken in order to have more
	// precise session lifetime.
	session.timer.Reset(time.Second * time.Duration(s.config.SessionExpirationTime))
	s.sessionsLock.Unlock()

	var (
		iIDStr = iID.String()
		iVals  []stackitem.Item
		found  bool
	)
	for _, it := range session.iteratorIdentifiers {
		if iIDStr == it.ID {
			iVals = iterator.Values(it.Item, count)
			found = true
			break
		}
	}
	session.iteratorsLock.Unlock()
	if !found {
		return nil, neorpc.ErrUnknownIterator
	}

	result := make([]json.RawMessage, len(iVals))
	for j := range iVals {
		result[j], err = stackitem.ToJSONWithTypes(iVals[j])
		if err != nil {
			return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to marshal iterator value: %s", err))
		}
	}
	return result, nil
}

// calculateNetworkFee calculates network fee for the transaction.
// calculateNetworkFee calculates network fee for the transaction.
func (s *Server) calculateNetworkFee(reqParams params.Params) (any, *neorpc.Error) {
	if len(reqParams) < 1 {
		return 0, neorpc.ErrInvalidParams
	}
	byteTx, err := reqParams[0].GetBytesBase64()
	if err != nil {
		return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
	}
	tx, err := transaction.NewTransactionFromBytes(byteTx)
	if err != nil {
		return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
	}
	hashablePart, err := tx.EncodeHashableFields()
	if err != nil {
		return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("failed to compute tx size: %s", err))
	}
	size := len(hashablePart) + io.GetVarSize(len(tx.Signers))
	var (
		netFee int64
		// Verification GAS cost can't exceed chin policy, but RPC config can limit it further.
		gasLimit = min(s.chain.GetMaxVerificationGAS(), int64(s.config.MaxGasInvoke))
	)
	for i, signer := range tx.Signers {
		w := tx.Scripts[i]
		if len(w.InvocationScript) == 0 { // No invocation provided, try to infer one.
			var paramz []manifest.Parameter
			if len(w.VerificationScript) == 0 { // Contract-based verification
				cs := s.chain.GetContractState(signer.Account)
				if cs == nil {
					return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidVerificationFunction, fmt.Sprintf("signer %d has no verification script and no deployed contract", i))
				}
				md := cs.Manifest.ABI.GetMethod(manifest.MethodVerify, -1)
				if md == nil || md.ReturnType != smartcontract.BoolType {
					return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidVerificationFunction, fmt.Sprintf("signer %d has no verify method in deployed contract", i))
				}
				paramz = md.Parameters // Might as well have none params and it's OK.
			} else { // Regular signature verification.
				if vm.IsSignatureContract(w.VerificationScript) {
					paramz = []manifest.Parameter{{Type: smartcontract.SignatureType}}
				} else if nSigs, _, ok := vm.ParseMultiSigContract(w.VerificationScript); ok {
					paramz = make([]manifest.Parameter, nSigs)
					for j := range paramz {
						paramz[j] = manifest.Parameter{Type: smartcontract.SignatureType}
					}
				}
			}
			inv := io.NewBufBinWriter()
			for _, p := range paramz {
				p.Type.EncodeDefaultValue(inv.BinWriter)
			}
			if inv.Err != nil {
				return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to create dummy invocation script (signer %d): %s", i, inv.Err.Error()))
			}
			w.InvocationScript = inv.Bytes()
		}
		gasConsumed, err := s.chain.VerifyWitness(signer.Account, tx, &w, gasLimit)
		if err != nil && !errors.Is(err, core.ErrInvalidSignature) {
			return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidSignature, fmt.Sprintf("witness %d: %s", i, err))
		}
		gasLimit -= gasConsumed
		netFee += gasConsumed
		size += io.GetVarSize(w.VerificationScript) + io.GetVarSize(w.InvocationScript)
	}
	netFee += int64(size)*s.chain.FeePerByte() + s.chain.CalculateAttributesFee(tx)
	return result.NetworkFee{Value: netFee}, nil
}

func (s *Server) invokeReadOnly(bw *io.BufBinWriter, h util.Uint160, method string, params ...any) (stackitem.Item, func(), error) {
	r, f, err := s.invokeReadOnlyMulti(bw, h, []string{method}, [][]any{params})
	if err != nil {
		return nil, nil, err
	}
	return r[0], f, nil
}

func (s *Server) invokeReadOnlyMulti(bw *io.BufBinWriter, h util.Uint160, methods []string, params [][]any) ([]stackitem.Item, func(), error) {
	if bw == nil {
		bw = io.NewBufBinWriter()
	} else {
		bw.Reset()
	}
	if len(methods) != len(params) {
		return nil, nil, fmt.Errorf("asymmetric parameters")
	}
	for i := range methods {
		emit.AppCall(bw.BinWriter, h, methods[i], callflag.ReadStates|callflag.AllowCall, params[i]...)
		if bw.Err != nil {
			return nil, nil, fmt.Errorf("failed to create `%s` invocation script: %w", methods[i], bw.Err)
		}
	}
	script := bw.Bytes()
	tx := &transaction.Transaction{Script: script}
	ic, err := s.chain.GetTestVM(trigger.Application, tx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("faile to prepare test VM: %w", err)
	}
	ic.VM.GasLimit = core.HeaderVerificationGasLimit
	ic.VM.LoadScriptWithFlags(script, callflag.All)
	err = ic.VM.Run()
	if err != nil {
		ic.Finalize()
		return nil, nil, fmt.Errorf("failed to run %d methods of %s: %w", len(methods), h.StringLE(), err)
	}
	estack := ic.VM.Estack()
	if estack.Len() != len(methods) {
		ic.Finalize()
		return nil, nil, fmt.Errorf("invalid return values count: expected %d, got %d", len(methods), estack.Len())
	}
	return estack.ToArray(), ic.Finalize, nil
}

// getHash returns the hash of the contract by its ID using cache.
func (s *Server) getHash(contractID int32, cache map[int32]util.Uint160) (util.Uint160, error) {
	if d, ok := cache[contractID]; ok {
		return d, nil
	}
	h, err := s.chain.GetContractScriptHash(contractID)
	if err != nil {
		return util.Uint160{}, err
	}
	cache[contractID] = h
	return h, nil
}

func (s *Server) contractIDFromParam(param *params.Param, root ...util.Uint256) (int32, *neorpc.Error) {
	var result int32
	if param == nil {
		return 0, neorpc.ErrInvalidParams
	}
	if scriptHash, err := param.GetUint160FromHex(); err == nil {
		if len(root) == 0 {
			cs := s.chain.GetContractState(scriptHash)
			if cs == nil {
				return 0, neorpc.ErrUnknownContract
			}
			result = cs.ID
		} else {
			cs, respErr := s.getHistoricalContractState(root[0], scriptHash)
			if respErr != nil {
				return 0, respErr
			}
			result = cs.ID
		}
	} else {
		id, err := param.GetInt()
		if err != nil {
			return 0, neorpc.ErrInvalidParams
		}
		if err := checkInt32(id); err != nil {
			return 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
		}
		result = int32(id)
	}
	return result, nil
}

// getContractScriptHashFromParam returns the contract script hash by hex contract hash, address, id or native contract name.
func (s *Server) contractScriptHashFromParam(param *params.Param) (util.Uint160, *neorpc.Error) {
	var result util.Uint160
	if param == nil {
		return result, neorpc.ErrInvalidParams
	}
	nameOrHashOrIndex, err := param.GetString()
	if err != nil {
		return result, neorpc.ErrInvalidParams
	}
	result, err = param.GetUint160FromAddressOrHex()
	if err == nil {
		return result, nil
	}
	result, err = s.chain.GetNativeContractScriptHash(nameOrHashOrIndex)
	if err == nil {
		return result, nil
	}
	id, err := strconv.Atoi(nameOrHashOrIndex)
	if err != nil {
		return result, neorpc.NewInvalidParamsError(fmt.Sprintf("Invalid contract identifier (name/hash/index is expected) : %s", err.Error()))
	}
	if err := checkInt32(id); err != nil {
		return result, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
	}
	result, err = s.chain.GetContractScriptHash(int32(id))
	if err != nil {
		return result, neorpc.ErrUnknownContract
	}
	return result, nil
}

func (s *Server) getNEP11Transfers(ps params.Params) (any, *neorpc.Error) {
	return s.getTokenTransfers(ps, true)
}

func (s *Server) getNEP17Transfers(ps params.Params) (any, *neorpc.Error) {
	return s.getTokenTransfers(ps, false)
}