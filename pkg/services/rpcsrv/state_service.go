package rpcsrv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"

	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv/params"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)




func makeStorageKey(id int32, key []byte) []byte {
	skey := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(skey, uint32(id))
	copy(skey[4:], key)
	return skey
}

var errKeepOnlyLatestState = errors.New("'KeepOnlyLatestState' setting is enabled")

func (s *Server) getProof(ps params.Params) (any, *neorpc.Error) {
	if s.chain.GetConfig().Ledger.KeepOnlyLatestState {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrUnsupportedState, fmt.Sprintf("'getproof' is not supported: %s", errKeepOnlyLatestState))
	}
	root, err := ps.Value(0).GetUint256()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	sc, err := ps.Value(1).GetUint160FromHex()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	key, err := ps.Value(2).GetBytesBase64()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	cs, respErr := s.getHistoricalContractState(root, sc)
	if respErr != nil {
		return nil, respErr
	}
	skey := makeStorageKey(cs.ID, key)
	proof, err := s.chain.GetStateModule().GetStateProof(root, skey)
	if err != nil {
		if errors.Is(err, mpt.ErrNotFound) {
			return nil, neorpc.ErrUnknownStorageItem
		}
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to get proof: %s", err))
	}
	return &result.ProofWithKey{
		Key:   skey,
		Proof: proof,
	}, nil
}

func (s *Server) verifyProof(ps params.Params) (any, *neorpc.Error) {
	if s.chain.GetConfig().Ledger.KeepOnlyLatestState {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrUnsupportedState, fmt.Sprintf("'verifyproof' is not supported: %s", errKeepOnlyLatestState))
	}
	root, err := ps.Value(0).GetUint256()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	proofStr, err := ps.Value(1).GetString()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	var p result.ProofWithKey
	if err := p.FromString(proofStr); err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	vp := new(result.VerifyProof)
	val, ok := mpt.VerifyProof(root, p.Key, p.Proof)
	if !ok {
		return nil, neorpc.ErrInvalidProof
	}
	vp.Value = val
	return vp, nil
}

func (s *Server) getState(ps params.Params) (any, *neorpc.Error) {
	root, respErr := s.getStateRootFromParam(ps.Value(0))
	if respErr != nil {
		return nil, respErr
	}
	csHash, err := ps.Value(1).GetUint160FromHex()
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, "invalid contract hash")
	}
	key, err := ps.Value(2).GetBytesBase64()
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, "invalid key")
	}
	cs, respErr := s.getHistoricalContractState(root, csHash)
	if respErr != nil {
		return nil, respErr
	}
	sKey := makeStorageKey(cs.ID, key)
	res, err := s.chain.GetStateModule().GetState(root, sKey)
	if err != nil {
		if errors.Is(err, mpt.ErrNotFound) {
			return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("invalid key: %s", err.Error()))
		}
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Failed to get historical item state: %s", err.Error()))
	}
	return res, nil
}

func (s *Server) findStates(ps params.Params) (any, *neorpc.Error) {
	root, respErr := s.getStateRootFromParam(ps.Value(0))
	if respErr != nil {
		return nil, respErr
	}
	csHash, err := ps.Value(1).GetUint160FromHex()
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("invalid contract hash: %s", err))
	}
	prefix, err := ps.Value(2).GetBytesBase64()
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("invalid prefix: %s", err))
	}
	var (
		key   []byte
		count = s.config.MaxFindResultItems
	)
	if len(ps) > 3 {
		key, err = ps.Value(3).GetBytesBase64()
		if err != nil {
			return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("invalid key: %s", err))
		}
		if len(key) > 0 {
			if !bytes.HasPrefix(key, prefix) {
				return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, "key doesn't match prefix")
			}
			key = key[len(prefix):]
		} else {
			// empty ("") key shouldn't exclude item matching prefix from the result
			key = nil
		}
	}
	if len(ps) > 4 {
		count, err = ps.Value(4).GetInt()
		if err != nil {
			return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("invalid count: %s", err))
		}
		if count > s.config.MaxFindResultItems {
			count = s.config.MaxFindResultItems
		}
	}
	cs, respErr := s.getHistoricalContractState(root, csHash)
	if respErr != nil {
		return nil, respErr
	}
	pKey := makeStorageKey(cs.ID, prefix)
	kvs, err := s.chain.GetStateModule().FindStates(root, pKey, key, count+1) // +1 to define result truncation
	if err != nil && !errors.Is(err, mpt.ErrNotFound) {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to find state items: %s", err))
	}
	res := result.FindStates{}
	if len(kvs) == count+1 {
		res.Truncated = true
		kvs = kvs[:len(kvs)-1]
	}
	if len(kvs) > 0 {
		proof, err := s.chain.GetStateModule().GetStateProof(root, kvs[0].Key)
		if err != nil {
			return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to get first proof: %s", err))
		}
		res.FirstProof = &result.ProofWithKey{
			Key:   kvs[0].Key,
			Proof: proof,
		}
	}
	if len(kvs) > 1 {
		proof, err := s.chain.GetStateModule().GetStateProof(root, kvs[len(kvs)-1].Key)
		if err != nil {
			return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to get last proof: %s", err))
		}
		res.LastProof = &result.ProofWithKey{
			Key:   kvs[len(kvs)-1].Key,
			Proof: proof,
		}
	}
	res.Results = make([]result.KeyValue, len(kvs))
	for i, kv := range kvs {
		res.Results[i] = result.KeyValue{
			Key:   kv.Key[4:], // cut contract ID as it is done in C#
			Value: kv.Value,
		}
	}
	return res, nil
}

// getStateRootFromParam retrieves state root hash from the provided parameter
// (only util.Uint256 serialized representation is allowed) and checks whether
// MPT states are supported for the old stateroot.
func (s *Server) getStateRootFromParam(p *params.Param) (util.Uint256, *neorpc.Error) {
	root, err := p.GetUint256()
	if err != nil {
		return util.Uint256{}, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, "invalid stateroot")
	}
	if s.chain.GetConfig().Ledger.KeepOnlyLatestState {
		curr, err := s.chain.GetStateModule().GetStateRoot(s.chain.BlockHeight())
		if err != nil {
			return util.Uint256{}, neorpc.NewInternalServerError(fmt.Sprintf("failed to get current stateroot: %s", err))
		}
		if !curr.Root.Equals(root) {
			return util.Uint256{}, neorpc.WrapErrorWithData(neorpc.ErrUnsupportedState, fmt.Sprintf("state-based methods are not supported for old states: %s", errKeepOnlyLatestState))
		}
	}
	return root, nil
}

func (s *Server) findStorage(reqParams params.Params) (any, *neorpc.Error) {
	id, prefix, start, take, respErr := s.getFindStorageParams(reqParams)
	if respErr != nil {
		return nil, respErr
	}
	return s.findStorageInternal(id, prefix, start, take, s.chain)
}

func (s *Server) findStorageInternal(id int32, prefix []byte, start, take int, seeker ContractStorageSeeker) (any, *neorpc.Error) {
	var (
		i   int
		end = start + take
		// Result is an empty list if a contract state is not found as it is in C# implementation.
		res = &result.FindStorage{Results: make([]result.KeyValue, 0)}
	)
	seeker.SeekStorage(id, prefix, func(k, v []byte) bool {
		if i < start {
			i++
			return true
		}
		if i < end {
			res.Results = append(res.Results, result.KeyValue{
				Key:   bytes.Clone(append(prefix, k...)), // Don't strip prefix, as it is done in C#.
				Value: v,
			})
			i++
			return true
		}
		res.Truncated = true
		return false
	})
	res.Next = i
	return res, nil
}

func (s *Server) findStorageHistoric(reqParams params.Params) (any, *neorpc.Error) {
	root, respErr := s.getStateRootFromParam(reqParams.Value(0))
	if respErr != nil {
		return nil, respErr
	}
	if len(reqParams) < 2 {
		return nil, neorpc.ErrInvalidParams
	}
	id, prefix, start, take, respErr := s.getFindStorageParams(reqParams[1:], root)
	if respErr != nil {
		return nil, respErr
	}

	return s.findStorageInternal(id, prefix, start, take, mptStorageSeeker{
		root:   root,
		module: s.chain.GetStateModule(),
	})
}

// mptStorageSeeker is an auxiliary structure that implements ContractStorageSeeker interface.
type mptStorageSeeker struct {
	root   util.Uint256
	module core.StateRoot
}

func (s mptStorageSeeker) SeekStorage(id int32, prefix []byte, cont func(k, v []byte) bool) {
	key := makeStorageKey(id, prefix)
	s.module.SeekStates(s.root, key, cont)
}

func (s *Server) getFindStorageParams(reqParams params.Params, root ...util.Uint256) (int32, []byte, int, int, *neorpc.Error) {
	if len(reqParams) < 2 {
		return 0, nil, 0, 0, neorpc.ErrInvalidParams
	}
	id, respErr := s.contractIDFromParam(reqParams.Value(0), root...)
	if respErr != nil {
		return 0, nil, 0, 0, respErr
	}

	prefix, err := reqParams.Value(1).GetBytesBase64()
	if err != nil {
		return 0, nil, 0, 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("invalid prefix: %s", err))
	}

	var skip int
	if len(reqParams) > 2 {
		skip, err = reqParams.Value(2).GetInt()
		if err != nil {
			return 0, nil, 0, 0, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("invalid start: %s", err))
		}
	}
	return id, prefix, skip, s.config.MaxFindStorageResultItems, nil
}

func (s *Server) getHistoricalContractState(root util.Uint256, csHash util.Uint160) (*state.Contract, *neorpc.Error) {
	csKey := makeStorageKey(native.ManagementContractID, native.MakeContractKey(csHash))
	csBytes, err := s.chain.GetStateModule().GetState(root, csKey)
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrUnknownContract, fmt.Sprintf("Failed to get historical contract state: %s", err.Error()))
	}
	contract := new(state.Contract)
	err = stackitem.DeserializeConvertible(csBytes, contract)
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to deserialize historical contract state: %s", err))
	}
	return contract, nil
}

func (s *Server) getStateHeight(_ params.Params) (any, *neorpc.Error) {
	var height = s.chain.BlockHeight()
	var stateHeight = s.chain.GetStateModule().CurrentValidatedHeight()
	if s.chain.GetConfig().StateRootInHeader {
		stateHeight = height - 1
	}
	return &result.StateHeight{
		Local:     height,
		Validated: stateHeight,
	}, nil
}

func (s *Server) getStateRoot(ps params.Params) (any, *neorpc.Error) {
	p := ps.Value(0)
	if p == nil {
		return nil, neorpc.NewInvalidParamsError("missing stateroot identifier")
	}
	var rt *state.MPTRoot
	var h util.Uint256
	height, err := p.GetIntStrict()
	if err == nil {
		if err := checkUint32(height); err != nil {
			return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
		}
		rt, err = s.chain.GetStateModule().GetStateRoot(uint32(height))
	} else if h, err = p.GetUint256(); err == nil {
		var hdr *block.Header
		hdr, err = s.chain.GetHeader(h)
		if err == nil {
			rt, err = s.chain.GetStateModule().GetStateRoot(hdr.Index)
		}
	}
	if err != nil {
		return nil, neorpc.ErrUnknownStateRoot
	}
	return rt, nil
}

func (s *Server) getStorage(ps params.Params) (any, *neorpc.Error) {
	id, rErr := s.contractIDFromParam(ps.Value(0))
	if rErr != nil {
		return nil, rErr
	}

	key, err := ps.Value(1).GetBytesBase64()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	item := s.chain.GetStorageItem(id, key)
	if item == nil {
		return "", neorpc.ErrUnknownStorageItem
	}

	return []byte(item), nil
}

func (s *Server) getStorageHistoric(ps params.Params) (any, *neorpc.Error) {
	root, respErr := s.getStateRootFromParam(ps.Value(0))
	if respErr != nil {
		return nil, respErr
	}
	if len(ps) < 2 {
		return nil, neorpc.ErrInvalidParams
	}

	id, rErr := s.contractIDFromParam(ps.Value(1), root)
	if rErr != nil {
		return nil, rErr
	}
	key, err := ps.Value(2).GetBytesBase64()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	pKey := makeStorageKey(id, key)

	v, err := s.chain.GetStateModule().GetState(root, pKey)
	if err != nil && !errors.Is(err, mpt.ErrNotFound) {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to get state item: %s", err))
	}
	if v == nil {
		return "", neorpc.ErrUnknownStorageItem
	}

	return v, nil
}

// getContractState returns contract state (contract information, according to the contract script hash,
// contract id or native contract name).
func (s *Server) getContractState(reqParams params.Params) (any, *neorpc.Error) {
	scriptHash, err := s.contractScriptHashFromParam(reqParams.Value(0))
	if err != nil {
		return nil, err
	}
	cs := s.chain.GetContractState(scriptHash)
	if cs == nil {
		return nil, neorpc.ErrUnknownContract
	}
	return cs, nil
}