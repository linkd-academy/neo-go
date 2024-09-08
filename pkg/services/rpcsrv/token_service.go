package rpcsrv

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/config/limits"
	"github.com/nspcc-dev/neo-go/pkg/core/interop/iterator"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv/params"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest/standard"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)



func (s *Server) getNEP11Tokens(h util.Uint160, acc util.Uint160, bw *io.BufBinWriter) ([]stackitem.Item, string, int, error) {
	items, finalize, err := s.invokeReadOnlyMulti(bw, h, []string{"tokensOf", "symbol", "decimals"}, [][]any{{acc}, nil, nil})
	if err != nil {
		return nil, "", 0, err
	}
	defer finalize()
	if (items[0].Type() != stackitem.InteropT) || !iterator.IsIterator(items[0]) {
		return nil, "", 0, fmt.Errorf("invalid `tokensOf` result type %s", items[0].String())
	}
	vals := iterator.Values(items[0], s.config.MaxNEP11Tokens)
	sym, err := stackitem.ToString(items[1])
	if err != nil {
		return nil, "", 0, fmt.Errorf("`symbol` return value error: %w", err)
	}
	dec, err := items[2].TryInteger()
	if err != nil {
		return nil, "", 0, fmt.Errorf("`decimals` return value error: %w", err)
	}
	if !dec.IsInt64() || dec.Sign() == -1 || dec.Int64() > math.MaxInt32 {
		return nil, "", 0, errors.New("`decimals` returned a bad integer")
	}
	return vals, sym, int(dec.Int64()), nil
}

func (s *Server) getNEP11Balances(ps params.Params) (any, *neorpc.Error) {
	u, err := ps.Value(0).GetUint160FromAddressOrHex()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	bs := &result.NEP11Balances{
		Address:  address.Uint160ToString(u),
		Balances: []result.NEP11AssetBalance{},
	}
	lastUpdated, err := s.chain.GetTokenLastUpdated(u)
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Failed to get NEP-11 last updated block: %s", err.Error()))
	}
	var count int
	stateSyncPoint := lastUpdated[math.MinInt32]
	bw := io.NewBufBinWriter()
contract_loop:
	for _, h := range s.chain.GetNEP11Contracts() {
		toks, sym, dec, err := s.getNEP11Tokens(h, u, bw)
		if err != nil {
			continue
		}
		if len(toks) == 0 {
			continue
		}
		cs := s.chain.GetContractState(h)
		if cs == nil {
			continue
		}
		isDivisible := (standard.ComplyABI(&cs.Manifest, standard.Nep11Divisible) == nil)
		lub, ok := lastUpdated[cs.ID]
		if !ok {
			cfg := s.chain.GetConfig()
			if !cfg.P2PStateExchangeExtensions && cfg.Ledger.RemoveUntraceableBlocks {
				return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to get LastUpdatedBlock for balance of %s token: internal database inconsistency", cs.Hash.StringLE()))
			}
			lub = stateSyncPoint
		}
		bs.Balances = append(bs.Balances, result.NEP11AssetBalance{
			Asset:    h,
			Decimals: dec,
			Name:     cs.Manifest.Name,
			Symbol:   sym,
			Tokens:   make([]result.NEP11TokenBalance, 0, len(toks)),
		})
		curAsset := &bs.Balances[len(bs.Balances)-1]
		for i := range toks {
			id, err := toks[i].TryBytes()
			if err != nil || len(id) > limits.MaxStorageKeyLen {
				continue
			}
			var amount = "1"
			if isDivisible {
				balance, err := s.getNEP11DTokenBalance(h, u, id, bw)
				if err != nil {
					continue
				}
				if balance.Sign() == 0 {
					continue
				}
				amount = balance.String()
			}
			count++
			curAsset.Tokens = append(curAsset.Tokens, result.NEP11TokenBalance{
				ID:          hex.EncodeToString(id),
				Amount:      amount,
				LastUpdated: lub,
			})
			if count >= s.config.MaxNEP11Tokens {
				break contract_loop
			}
		}
	}
	return bs, nil
}

func (s *Server) invokeNEP11Properties(h util.Uint160, id []byte, bw *io.BufBinWriter) ([]stackitem.MapElement, error) {
	item, finalize, err := s.invokeReadOnly(bw, h, "properties", id)
	if err != nil {
		return nil, err
	}
	defer finalize()
	if item.Type() != stackitem.MapT {
		return nil, fmt.Errorf("invalid `properties` result type %s", item.String())
	}
	return item.Value().([]stackitem.MapElement), nil
}

func (s *Server) getNEP11Properties(ps params.Params) (any, *neorpc.Error) {
	asset, err := ps.Value(0).GetUint160FromAddressOrHex()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	token, err := ps.Value(1).GetBytesHex()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	props, err := s.invokeNEP11Properties(asset, token, nil)
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrExecutionFailed, fmt.Sprintf("Failed to get NEP-11 properties: %s", err.Error()))
	}
	res := make(map[string]any)
	for _, kv := range props {
		key, err := kv.Key.TryBytes()
		if err != nil {
			continue
		}
		var val any
		if result.KnownNEP11Properties[string(key)] || kv.Value.Type() != stackitem.AnyT {
			v, err := kv.Value.TryBytes()
			if err != nil {
				continue
			}
			if result.KnownNEP11Properties[string(key)] {
				val = string(v)
			} else {
				val = v
			}
		}
		res[string(key)] = val
	}
	return res, nil
}

func (s *Server) getNEP17Balances(ps params.Params) (any, *neorpc.Error) {
	u, err := ps.Value(0).GetUint160FromAddressOrHex()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	bs := &result.NEP17Balances{
		Address:  address.Uint160ToString(u),
		Balances: []result.NEP17Balance{},
	}
	lastUpdated, err := s.chain.GetTokenLastUpdated(u)
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Failed to get NEP-17 last updated block: %s", err.Error()))
	}
	stateSyncPoint := lastUpdated[math.MinInt32]
	bw := io.NewBufBinWriter()
	for _, h := range s.chain.GetNEP17Contracts() {
		balance, sym, dec, err := s.getNEP17TokenBalance(h, u, bw)
		if err != nil {
			continue
		}
		if balance.Sign() == 0 {
			continue
		}
		cs := s.chain.GetContractState(h)
		if cs == nil {
			continue
		}
		lub, ok := lastUpdated[cs.ID]
		if !ok {
			cfg := s.chain.GetConfig()
			if !cfg.P2PStateExchangeExtensions && cfg.Ledger.RemoveUntraceableBlocks {
				return nil, neorpc.NewInternalServerError(fmt.Sprintf("failed to get LastUpdatedBlock for balance of %s token: internal database inconsistency", cs.Hash.StringLE()))
			}
			lub = stateSyncPoint
		}
		bs.Balances = append(bs.Balances, result.NEP17Balance{
			Asset:       h,
			Amount:      balance.String(),
			Decimals:    dec,
			LastUpdated: lub,
			Name:        cs.Manifest.Name,
			Symbol:      sym,
		})
	}
	return bs, nil
}

func (s *Server) getTokenTransfers(ps params.Params, isNEP11 bool) (any, *neorpc.Error) {
	u, err := ps.Value(0).GetUint160FromAddressOrHex()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	start, end, limit, page, err := getTimestampsAndLimit(ps, 1)
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("malformed timestamps/limit: %s", err))
	}

	bs := &tokenTransfers{
		Address:  address.Uint160ToString(u),
		Received: []any{},
		Sent:     []any{},
	}
	cache := make(map[int32]util.Uint160)
	var resCount, frameCount int
	// handleTransfer returns items to be added into the received and sent arrays
	// along with a continue flag and error.
	var handleTransfer = func(tr *state.NEP17Transfer) (*result.NEP17Transfer, *result.NEP17Transfer, bool, error) {
		var received, sent *result.NEP17Transfer

		// Iterating from the newest to the oldest, not yet reached required
		// time frame, continue looping.
		if tr.Timestamp > end {
			return nil, nil, true, nil
		}
		// Iterating from the newest to the oldest, moved past required
		// time frame, stop looping.
		if tr.Timestamp < start {
			return nil, nil, false, nil
		}
		frameCount++
		// Using limits, not yet reached required page.
		if limit != 0 && page*limit >= frameCount {
			return nil, nil, true, nil
		}

		h, err := s.getHash(tr.Asset, cache)
		if err != nil {
			return nil, nil, false, err
		}

		transfer := result.NEP17Transfer{
			Timestamp: tr.Timestamp,
			Asset:     h,
			Index:     tr.Block,
			TxHash:    tr.Tx,
		}
		if !tr.Counterparty.Equals(util.Uint160{}) {
			transfer.Address = address.Uint160ToString(tr.Counterparty)
		}
		if tr.Amount.Sign() > 0 { // token was received
			transfer.Amount = tr.Amount.String()
			received = &result.NEP17Transfer{}
			*received = transfer // Make a copy, transfer is to be modified below.
		} else {
			transfer.Amount = new(big.Int).Neg(tr.Amount).String()
			sent = &result.NEP17Transfer{}
			*sent = transfer
		}

		resCount++
		// Check limits for continue flag.
		return received, sent, !(limit != 0 && resCount >= limit), nil
	}
	if !isNEP11 {
		err = s.chain.ForEachNEP17Transfer(u, end, func(tr *state.NEP17Transfer) (bool, error) {
			r, s, res, err := handleTransfer(tr)
			if err == nil {
				if r != nil {
					bs.Received = append(bs.Received, r)
				}
				if s != nil {
					bs.Sent = append(bs.Sent, s)
				}
			}
			return res, err
		})
	} else {
		err = s.chain.ForEachNEP11Transfer(u, end, func(tr *state.NEP11Transfer) (bool, error) {
			r, s, res, err := handleTransfer(&tr.NEP17Transfer)
			if err == nil {
				id := hex.EncodeToString(tr.ID)
				if r != nil {
					bs.Received = append(bs.Received, nep17TransferToNEP11(r, id))
				}
				if s != nil {
					bs.Sent = append(bs.Sent, nep17TransferToNEP11(s, id))
				}
			}
			return res, err
		})
	}
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("invalid transfer log: %s", err))
	}
	return bs, nil
}

// getUnclaimedGas returns unclaimed GAS amount of the specified address.
func (s *Server) getUnclaimedGas(ps params.Params) (any, *neorpc.Error) {
	u, err := ps.Value(0).GetUint160FromAddressOrHex()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	neo, _ := s.chain.GetGoverningTokenBalance(u)
	if neo.Sign() == 0 {
		return result.UnclaimedGas{
			Address: u,
		}, nil
	}
	gas, err := s.chain.CalculateClaimable(u, s.chain.BlockHeight()+1) // +1 as in C#, for the next block.
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Can't calculate claimable: %s", err.Error()))
	}
	return result.UnclaimedGas{
		Address:   u,
		Unclaimed: *gas,
	}, nil
}


func (s *Server) getNEP17TokenBalance(h util.Uint160, acc util.Uint160, bw *io.BufBinWriter) (*big.Int, string, int, error) {
	items, finalize, err := s.invokeReadOnlyMulti(bw, h, []string{"balanceOf", "symbol", "decimals"}, [][]any{{acc}, nil, nil})
	if err != nil {
		return nil, "", 0, err
	}
	finalize()
	res, err := items[0].TryInteger()
	if err != nil {
		return nil, "", 0, fmt.Errorf("unexpected `balanceOf` result type: %w", err)
	}
	sym, err := stackitem.ToString(items[1])
	if err != nil {
		return nil, "", 0, fmt.Errorf("`symbol` return value error: %w", err)
	}
	dec, err := items[2].TryInteger()
	if err != nil {
		return nil, "", 0, fmt.Errorf("`decimals` return value error: %w", err)
	}
	if !dec.IsInt64() || dec.Sign() == -1 || dec.Int64() > math.MaxInt32 {
		return nil, "", 0, errors.New("`decimals` returned a bad integer")
	}
	return res, sym, int(dec.Int64()), nil
}

func (s *Server) getNEP11DTokenBalance(h util.Uint160, acc util.Uint160, id []byte, bw *io.BufBinWriter) (*big.Int, error) {
	item, finalize, err := s.invokeReadOnly(bw, h, "balanceOf", acc, id)
	if err != nil {
		return nil, err
	}
	finalize()
	res, err := item.TryInteger()
	if err != nil {
		return nil, fmt.Errorf("unexpected `balanceOf` result type: %w", err)
	}
	return res, nil
}