package rpcsrv

import (
	"crypto/elliptic"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/services/oracle/broadcaster"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv/params"
	"github.com/nspcc-dev/neo-go/pkg/util"
)


func (s *Server) getPeers(_ params.Params) (any, *neorpc.Error) {
	peers := result.NewGetPeers()
	peers.AddUnconnected(s.coreServer.UnconnectedPeers())
	peers.AddConnected(s.coreServer.ConnectedPeers())
	peers.AddBad(s.coreServer.BadPeers())
	return peers, nil
}


func (s *Server) getRawMempool(reqParams params.Params) (any, *neorpc.Error) {
	verbose, _ := reqParams.Value(0).GetBoolean()
	mp := s.chain.GetMemPool()
	txes := mp.GetVerifiedTransactions()
	hashList := make([]util.Uint256, len(txes))
	for i := range txes {
		hashList[i] = txes[i].Hash()
	}
	if !verbose {
		return hashList, nil
	}
	return result.RawMempool{
		Height:     s.chain.BlockHeight(),
		Verified:   hashList,
		Unverified: []util.Uint256{}, // avoid `null` result
	}, nil
}


func (s *Server) getConnectionCount(_ params.Params) (any, *neorpc.Error) {
	return s.coreServer.PeerCount(), nil
}

// Addresses returns the list of addresses RPC server is listening to in the form of
// address:port.
func (s *Server) Addresses() []string {
	res := make([]string, len(s.http))
	for i, srv := range s.http {
		res[i] = srv.Addr
	}
	return res
}


func (s *Server) getRawNotaryPool(_ params.Params) (any, *neorpc.Error) {
	if !s.chain.P2PSigExtensionsEnabled() {
		return nil, neorpc.NewInternalServerError("P2PSignatureExtensions are disabled")
	}
	nrp := s.coreServer.GetNotaryPool()
	res := &result.RawNotaryPool{Hashes: make(map[util.Uint256][]util.Uint256)}
	nrp.IterateVerifiedTransactions(func(tx *transaction.Transaction, data any) bool {
		if data != nil {
			d := data.(*payload.P2PNotaryRequest)
			mainHash := d.MainTransaction.Hash()
			fallbackHash := d.FallbackTransaction.Hash()
			res.Hashes[mainHash] = append(res.Hashes[mainHash], fallbackHash)
		}
		return true
	})
	return res, nil
}

func (s *Server) getVersion(_ params.Params) (any, *neorpc.Error) {
	port, err := s.coreServer.Port(nil) // any port will suite
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("cannot fetch tcp port: %s", err))
	}

	cfg := s.chain.GetConfig()
	hfs := make(map[config.Hardfork]uint32, len(cfg.Hardforks))
	for _, cfgHf := range config.Hardforks {
		height, ok := cfg.Hardforks[cfgHf.String()]
		if !ok {
			continue
		}
		hfs[cfgHf] = height
	}
	standbyCommittee, err := keys.NewPublicKeysFromStrings(cfg.StandbyCommittee)
	if err != nil {
		return nil, neorpc.NewInternalServerError(fmt.Sprintf("cannot fetch standbyCommittee: %s", err))
	}
	return &result.Version{
		TCPPort:   port,
		Nonce:     s.coreServer.ID(),
		UserAgent: s.coreServer.UserAgent,
		RPC: result.RPC{
			MaxIteratorResultItems: s.config.MaxIteratorResultItems,
			SessionEnabled:         s.config.SessionEnabled,
		},
		Protocol: result.Protocol{
			AddressVersion:              address.NEO3Prefix,
			Network:                     cfg.Magic,
			MillisecondsPerBlock:        int(cfg.TimePerBlock / time.Millisecond),
			MaxTraceableBlocks:          cfg.MaxTraceableBlocks,
			MaxValidUntilBlockIncrement: cfg.MaxValidUntilBlockIncrement,
			MaxTransactionsPerBlock:     cfg.MaxTransactionsPerBlock,
			MemoryPoolMaxTransactions:   cfg.MemPoolSize,
			ValidatorsCount:             byte(cfg.GetNumOfCNs(s.chain.BlockHeight())),
			InitialGasDistribution:      cfg.InitialGASSupply,
			Hardforks:                   hfs,
			StandbyCommittee:            standbyCommittee,
			SeedList:                    cfg.SeedList,

			CommitteeHistory:  cfg.CommitteeHistory,
			P2PSigExtensions:  cfg.P2PSigExtensions,
			StateRootInHeader: cfg.StateRootInHeader,
			ValidatorsHistory: cfg.ValidatorsHistory,
		},
	}, nil
}

// getRelayResult returns successful relay result or an error.
func getRelayResult(err error, hash util.Uint256) (any, *neorpc.Error) {
	switch {
	case err == nil:
		return result.RelayResult{
			Hash: hash,
		}, nil
	case errors.Is(err, core.ErrTxExpired):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrExpiredTransaction, err.Error())
	case errors.Is(err, core.ErrAlreadyExists) || errors.Is(err, core.ErrInvalidBlockIndex):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrAlreadyExists, err.Error())
	case errors.Is(err, core.ErrAlreadyInPool):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrAlreadyInPool, err.Error())
	case errors.Is(err, core.ErrOOM):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrMempoolCapReached, err.Error())
	case errors.Is(err, core.ErrPolicy):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrPolicyFailed, err.Error())
	case errors.Is(err, core.ErrInvalidScript):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidScript, err.Error())
	case errors.Is(err, core.ErrTxTooBig):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidSize, err.Error())
	case errors.Is(err, core.ErrTxSmallNetworkFee):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInsufficientNetworkFee, err.Error())
	case errors.Is(err, core.ErrInvalidAttribute):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidAttribute, err.Error())
	case errors.Is(err, core.ErrInsufficientFunds), errors.Is(err, core.ErrMemPoolConflict):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInsufficientFunds, err.Error())
	case errors.Is(err, core.ErrInvalidSignature):
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidSignature, err.Error())
	default:
		return nil, neorpc.WrapErrorWithData(neorpc.ErrVerificationFailed, err.Error())
	}
}

// submitBlock broadcasts a raw block over the Neo network.
func (s *Server) submitBlock(reqParams params.Params) (any, *neorpc.Error) {
	blockBytes, err := reqParams.Value(0).GetBytesBase64()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("missing parameter or not a base64: %s", err))
	}
	b := block.New(s.stateRootEnabled)
	r := io.NewBinReaderFromBuf(blockBytes)
	b.DecodeBinary(r)
	if r.Err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("can't decode block: %s", r.Err))
	}
	return getRelayResult(s.chain.AddBlock(b), b.Hash())
}

// submitNotaryRequest broadcasts P2PNotaryRequest over the Neo network.
func (s *Server) submitNotaryRequest(ps params.Params) (any, *neorpc.Error) {
	if !s.chain.P2PSigExtensionsEnabled() {
		return nil, neorpc.NewInternalServerError("P2PSignatureExtensions are disabled")
	}

	bytePayload, err := ps.Value(0).GetBytesBase64()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("not a base64: %s", err))
	}
	r, err := payload.NewP2PNotaryRequestFromBytes(bytePayload)
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("can't decode notary payload: %s", err))
	}
	return getRelayResult(s.coreServer.RelayP2PNotaryRequest(r), r.FallbackTransaction.Hash())
}

func (s *Server) submitOracleResponse(ps params.Params) (any, *neorpc.Error) {
	oraclePtr := s.oracle.Load()
	if oraclePtr == nil {
		return nil, neorpc.ErrOracleDisabled
	}
	oracle := oraclePtr.(OracleHandler)
	var pub *keys.PublicKey
	pubBytes, err := ps.Value(0).GetBytesBase64()
	if err == nil {
		pub, err = keys.NewPublicKeyFromBytes(pubBytes, elliptic.P256())
	}
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("public key is missing: %s", err))
	}
	reqID, err := ps.Value(1).GetInt()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("request ID is missing: %s", err))
	}
	txSig, err := ps.Value(2).GetBytesBase64()
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("tx signature is missing: %s", err))
	}
	msgSig, err := ps.Value(3).GetBytesBase64()
	if err != nil {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, fmt.Sprintf("msg signature is missing: %s", err))
	}
	data := broadcaster.GetMessage(pubBytes, uint64(reqID), txSig)
	if !pub.Verify(msgSig, hash.Sha256(data).BytesBE()) {
		return nil, neorpc.ErrInvalidSignature
	}
	oracle.AddResponse(pub, uint64(reqID), txSig)
	return json.RawMessage([]byte("{}")), nil
}

func (s *Server) sendrawtransaction(reqParams params.Params) (any, *neorpc.Error) {
	if len(reqParams) < 1 {
		return nil, neorpc.NewInvalidParamsError("not enough parameters")
	}
	byteTx, err := reqParams[0].GetBytesBase64()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("not a base64: %s", err))
	}
	tx, err := transaction.NewTransactionFromBytes(byteTx)
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("can't decode transaction: %s", err))
	}
	return getRelayResult(s.coreServer.RelayTxn(tx), tx.Hash())
}