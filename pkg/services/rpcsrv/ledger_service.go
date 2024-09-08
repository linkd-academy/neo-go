package rpcsrv

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv/params"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

func (s *Server) getBestBlockHash(_ params.Params) (any, *neorpc.Error) {
	return "0x" + s.chain.CurrentBlockHash().StringLE(), nil
}

func (s *Server) getBlockCount(_ params.Params) (any, *neorpc.Error) {
	return s.chain.BlockHeight() + 1, nil
}

func (s *Server) getBlockHeaderCount(_ params.Params) (any, *neorpc.Error) {
	return s.chain.HeaderHeight() + 1, nil
}

func (s *Server) getBlock(reqParams params.Params) (any, *neorpc.Error) {
	param := reqParams.Value(0)
	hash, respErr := s.blockHashFromParam(param)
	if respErr != nil {
		return nil, respErr
	}

	block, err := s.chain.GetBlock(hash)
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrUnknownBlock, err.Error())
	}

	if v, _ := reqParams.Value(1).GetBoolean(); v {
		res := result.Block{
			Block:         *block,
			BlockMetadata: s.fillBlockMetadata(block, &block.Header),
		}
		return res, nil
	}
	writer := io.NewBufBinWriter()
	block.EncodeBinary(writer.BinWriter)
	return writer.Bytes(), nil
}

func (s *Server) getBlockHash(reqParams params.Params) (any, *neorpc.Error) {
	num, err := s.blockHeightFromParam(reqParams.Value(0))
	if err != nil {
		return nil, err
	}

	return s.chain.GetHeaderHash(num), nil
}



func (s *Server) getrawtransaction(reqParams params.Params) (any, *neorpc.Error) {
	txHash, err := reqParams.Value(0).GetUint256()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	tx, height, err := s.chain.GetTransaction(txHash)
	if err != nil {
		return nil, neorpc.ErrUnknownTransaction
	}
	if v, _ := reqParams.Value(1).GetBoolean(); v {
		res := result.TransactionOutputRaw{
			Transaction: *tx,
		}
		if height == math.MaxUint32 { // Mempooled transaction.
			return res, nil
		}
		_header := s.chain.GetHeaderHash(height)
		header, err := s.chain.GetHeader(_header)
		if err != nil {
			return nil, neorpc.NewInternalServerError(fmt.Sprintf("Failed to get header for the transaction: %s", err.Error()))
		}
		aers, err := s.chain.GetAppExecResults(txHash, trigger.Application)
		if err != nil {
			return nil, neorpc.NewInternalServerError(fmt.Sprintf("Failed to get application log for the transaction: %s", err.Error()))
		}
		if len(aers) == 0 {
			return nil, neorpc.NewInternalServerError("Inconsistent application log: application log for the transaction is empty")
		}
		res.TransactionMetadata = result.TransactionMetadata{
			Blockhash:     header.Hash(),
			Confirmations: int(s.chain.BlockHeight() - header.Index + 1),
			Timestamp:     header.Timestamp,
			VMState:       aers[0].VMState.String(),
		}
		return res, nil
	}
	return tx.Bytes(), nil
}

func (s *Server) getTransactionHeight(ps params.Params) (any, *neorpc.Error) {
	h, err := ps.Value(0).GetUint256()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	_, height, err := s.chain.GetTransaction(h)
	if err != nil || height == math.MaxUint32 {
		return nil, neorpc.ErrUnknownTransaction
	}

	return height, nil
}

func (s *Server) getNativeContracts(_ params.Params) (any, *neorpc.Error) {
	return s.chain.GetNatives(), nil
}

// getBlockSysFee returns the system fees of the block, based on the specified index.
func (s *Server) getBlockSysFee(reqParams params.Params) (any, *neorpc.Error) {
	num, err := s.blockHeightFromParam(reqParams.Value(0))
	if err != nil {
		return 0, neorpc.WrapErrorWithData(err, fmt.Sprintf("invalid block height: %s", err.Data))
	}

	headerHash := s.chain.GetHeaderHash(num)
	block, errBlock := s.chain.GetBlock(headerHash)
	if errBlock != nil {
		return 0, neorpc.ErrUnknownBlock
	}

	var blockSysFee int64
	for _, tx := range block.Transactions {
		blockSysFee += tx.SystemFee
	}

	return blockSysFee, nil
}


// getBlockHeader returns the corresponding block header information according to the specified script hash.
func (s *Server) getBlockHeader(reqParams params.Params) (any, *neorpc.Error) {
	param := reqParams.Value(0)
	hash, respErr := s.blockHashFromParam(param)
	if respErr != nil {
		return nil, respErr
	}

	verbose, _ := reqParams.Value(1).GetBoolean()
	h, err := s.chain.GetHeader(hash)
	if err != nil {
		return nil, neorpc.ErrUnknownBlock
	}

	if verbose {
		res := result.Header{
			Header:        *h,
			BlockMetadata: s.fillBlockMetadata(h, h),
		}
		return res, nil
	}

	buf := io.NewBufBinWriter()
	h.EncodeBinary(buf.BinWriter)
	if buf.Err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("encoding error: %s", buf.Err))
	}
	return buf.Bytes(), nil
}

// getNextBlockValidators returns validators for the next block with voting status.
func (s *Server) getNextBlockValidators(_ params.Params) (any, *neorpc.Error) {
	var validators keys.PublicKeys

	validators, err := s.chain.GetNextBlockValidators()
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Can't get next block validators: %s", err.Error()))
	}
	enrollments, err := s.chain.GetEnrollments()
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Can't get enrollments: %s", err.Error()))
	}
	var res = make([]result.Validator, 0)
	for _, v := range enrollments {
		if !validators.Contains(v.Key) {
			continue
		}
		res = append(res, result.Validator{
			PublicKey: *v.Key,
			Votes:     v.Votes.Int64(),
		})
	}
	return res, nil
}

// getCommittee returns the current list of NEO committee members.
func (s *Server) getCommittee(_ params.Params) (any, *neorpc.Error) {
	keys, err := s.chain.GetCommittee()
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("can't get committee members: %s", err))
	}
	return keys, nil
}

func (s *Server) blockHeightFromParam(param *params.Param) (uint32, *neorpc.Error) {
	num, err := param.GetInt()
	if err != nil {
		return 0, neorpc.ErrInvalidParams
	}

	if num < 0 || int64(num) > int64(s.chain.BlockHeight()) {
		return 0, neorpc.WrapErrorWithData(neorpc.ErrUnknownHeight, fmt.Sprintf("param at index %d should be greater than or equal to 0 and less than or equal to current block height, got: %d", 0, num))
	}
	return uint32(num), nil
}

func (s *Server) blockHashFromParam(param *params.Param) (util.Uint256, *neorpc.Error) {
	var (
		hash util.Uint256
		err  error
	)
	if param == nil {
		return hash, neorpc.ErrInvalidParams
	}

	if hash, err = param.GetUint256(); err != nil {
		num, respErr := s.blockHeightFromParam(param)
		if respErr != nil {
			return hash, respErr
		}
		hash = s.chain.GetHeaderHash(num)
	}
	return hash, nil
}

func (s *Server) fillBlockMetadata(obj io.Serializable, h *block.Header) result.BlockMetadata {
	res := result.BlockMetadata{
		Size:          io.GetVarSize(obj), // obj can be a Block or a Header.
		Confirmations: s.chain.BlockHeight() - h.Index + 1,
	}

	hash := s.chain.GetHeaderHash(h.Index + 1)
	if !hash.Equals(util.Uint256{}) {
		res.NextBlockHash = &hash
	}
	return res
}

// getApplicationLog returns the contract log based on the specified txid or blockid.
func (s *Server) getApplicationLog(reqParams params.Params) (any, *neorpc.Error) {
	hash, err := reqParams.Value(0).GetUint256()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	trig := trigger.All
	if len(reqParams) > 1 {
		trigString, err := reqParams.Value(1).GetString()
		if err != nil {
			return nil, neorpc.ErrInvalidParams
		}
		trig, err = trigger.FromString(trigString)
		if err != nil {
			return nil, neorpc.ErrInvalidParams
		}
	}

	appExecResults, err := s.chain.GetAppExecResults(hash, trigger.All)
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrUnknownScriptContainer, fmt.Sprintf("failed to locate application log: %s", err))
	}
	return result.NewApplicationLog(hash, appExecResults, trig), nil
}


func getTimestampsAndLimit(ps params.Params, index int) (uint64, uint64, int, int, error) {
	var start, end uint64
	var limit, page int

	limit = maxTransfersLimit
	pStart, pEnd, pLimit, pPage := ps.Value(index), ps.Value(index+1), ps.Value(index+2), ps.Value(index+3)
	if pPage != nil {
		p, err := pPage.GetInt()
		if err != nil {
			return 0, 0, 0, 0, err
		}
		if p < 0 {
			return 0, 0, 0, 0, errors.New("can't use negative page")
		}
		page = p
	}
	if pLimit != nil {
		l, err := pLimit.GetInt()
		if err != nil {
			return 0, 0, 0, 0, err
		}
		if l <= 0 {
			return 0, 0, 0, 0, errors.New("can't use negative or zero limit")
		}
		if l > maxTransfersLimit {
			return 0, 0, 0, 0, errors.New("too big limit requested")
		}
		limit = l
	}
	if pEnd != nil {
		val, err := pEnd.GetInt()
		if err != nil {
			return 0, 0, 0, 0, err
		}
		end = uint64(val)
	} else {
		end = uint64(time.Now().Unix() * 1000)
	}
	if pStart != nil {
		val, err := pStart.GetInt()
		if err != nil {
			return 0, 0, 0, 0, err
		}
		start = uint64(val)
	} else {
		start = uint64(time.Now().Add(-time.Hour*24*7).Unix() * 1000)
	}
	return start, end, limit, page, nil
}


// getCandidates returns the current list of candidates with their active/inactive voting status.
func (s *Server) getCandidates(_ params.Params) (any, *neorpc.Error) {
	var validators keys.PublicKeys

	validators, err := s.chain.GetNextBlockValidators()
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Can't get next block validators: %s", err.Error()))
	}
	enrollments, err := s.chain.GetEnrollments()
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("Can't get enrollments: %s", err.Error()))
	}
	var res = make([]result.Candidate, 0)
	for _, v := range enrollments {
		res = append(res, result.Candidate{
			PublicKey: *v.Key,
			Votes:     v.Votes.Int64(),
			Active:    validators.Contains(v.Key),
		})
	}
	return res, nil
}


func (s *Server) getRawNotaryTransaction(reqParams params.Params) (any, *neorpc.Error) {
	if !s.chain.P2PSigExtensionsEnabled() {
		return nil, neorpc.NewInternalServerError("P2PSignatureExtensions are disabled")
	}

	txHash, err := reqParams.Value(0).GetUint256()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	nrp := s.coreServer.GetNotaryPool()
	// Try to find fallback transaction.
	tx, ok := nrp.TryGetValue(txHash)
	if !ok {
		// Try to find main transaction.
		nrp.IterateVerifiedTransactions(func(t *transaction.Transaction, data any) bool {
			if data != nil && data.(*payload.P2PNotaryRequest).MainTransaction.Hash().Equals(txHash) {
				tx = data.(*payload.P2PNotaryRequest).MainTransaction
				return false
			}
			return true
		})
		// The transaction was not found.
		if tx == nil {
			return nil, neorpc.ErrUnknownTransaction
		}
	}

	if v, _ := reqParams.Value(1).GetBoolean(); v {
		return tx, nil
	}
	return tx.Bytes(), nil
}