package rpcsrv

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/mempool"
	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/network"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv/params"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"go.uber.org/zap"
)

type (
	// Ledger abstracts away the Blockchain as used by the RPC server.
	Ledger interface {
		AddBlock(block *block.Block) error
		BlockHeight() uint32
		CalculateAttributesFee(tx *transaction.Transaction) int64
		CalculateClaimable(h util.Uint160, endHeight uint32) (*big.Int, error)
		CurrentBlockHash() util.Uint256
		FeePerByte() int64
		ForEachNEP11Transfer(acc util.Uint160, newestTimestamp uint64, f func(*state.NEP11Transfer) (bool, error)) error
		ForEachNEP17Transfer(acc util.Uint160, newestTimestamp uint64, f func(*state.NEP17Transfer) (bool, error)) error
		GetAppExecResults(util.Uint256, trigger.Type) ([]state.AppExecResult, error)
		GetBaseExecFee() int64
		GetBlock(hash util.Uint256) (*block.Block, error)
		GetCommittee() (keys.PublicKeys, error)
		GetConfig() config.Blockchain
		GetContractScriptHash(id int32) (util.Uint160, error)
		GetContractState(hash util.Uint160) *state.Contract
		GetEnrollments() ([]state.Validator, error)
		GetGoverningTokenBalance(acc util.Uint160) (*big.Int, uint32)
		GetHeader(hash util.Uint256) (*block.Header, error)
		GetHeaderHash(uint32) util.Uint256
		GetMaxVerificationGAS() int64
		GetMemPool() *mempool.Pool
		GetNEP11Contracts() []util.Uint160
		GetNEP17Contracts() []util.Uint160
		GetNativeContractScriptHash(string) (util.Uint160, error)
		GetNatives() []state.Contract
		GetNextBlockValidators() ([]*keys.PublicKey, error)
		GetNotaryContractScriptHash() util.Uint160
		GetStateModule() core.StateRoot
		GetStorageItem(id int32, key []byte) state.StorageItem
		GetTestHistoricVM(t trigger.Type, tx *transaction.Transaction, nextBlockHeight uint32) (*interop.Context, error)
		GetTestVM(t trigger.Type, tx *transaction.Transaction, b *block.Block) (*interop.Context, error)
		GetTokenLastUpdated(acc util.Uint160) (map[int32]uint32, error)
		GetTransaction(util.Uint256) (*transaction.Transaction, uint32, error)
		HeaderHeight() uint32
		InitVerificationContext(ic *interop.Context, hash util.Uint160, witness *transaction.Witness) error
		P2PSigExtensionsEnabled() bool
		SubscribeForBlocks(ch chan *block.Block)
		SubscribeForHeadersOfAddedBlocks(ch chan *block.Header)
		SubscribeForExecutions(ch chan *state.AppExecResult)
		SubscribeForNotifications(ch chan *state.ContainedNotificationEvent)
		SubscribeForTransactions(ch chan *transaction.Transaction)
		UnsubscribeFromBlocks(ch chan *block.Block)
		UnsubscribeFromHeadersOfAddedBlocks(ch chan *block.Header)
		UnsubscribeFromExecutions(ch chan *state.AppExecResult)
		UnsubscribeFromNotifications(ch chan *state.ContainedNotificationEvent)
		UnsubscribeFromTransactions(ch chan *transaction.Transaction)
		VerifyTx(*transaction.Transaction) error
		VerifyWitness(util.Uint160, hash.Hashable, *transaction.Witness, int64) (int64, error)
		mempool.Feer // fee interface
		ContractStorageSeeker
	}

	// ContractStorageSeeker is the interface `findstorage*` handlers need to be able to
	// seek over contract storage. Prefix is trimmed in the resulting pair's key.
	ContractStorageSeeker interface {
		SeekStorage(id int32, prefix []byte, cont func(k, v []byte) bool)
	}

	// OracleHandler is the interface oracle service needs to provide for the Server.
	OracleHandler interface {
		AddResponse(pub *keys.PublicKey, reqID uint64, txSig []byte)
	}

	// Server represents the JSON-RPC 2.0 server.
	Server struct {
		http  []*http.Server
		https []*http.Server

		chain  Ledger
		config config.RPC
		// wsReadLimit represents web-socket message limit for a receiving side.
		wsReadLimit      int64
		upgrader         websocket.Upgrader
		network          netmode.Magic
		stateRootEnabled bool
		coreServer       *network.Server
		oracle           *atomic.Value
		log              *zap.Logger
		shutdown         chan struct{}
		started          atomic.Bool
		errChan          chan<- error

		sessionsLock sync.Mutex
		sessions     map[string]*session

		subsLock    sync.RWMutex
		subscribers map[*subscriber]bool

		subsCounterLock   sync.RWMutex
		blockSubs         int
		blockHeaderSubs   int
		executionSubs     int
		notificationSubs  int
		transactionSubs   int
		notaryRequestSubs int

		blockCh           chan *block.Block
		blockHeaderCh     chan *block.Header
		executionCh       chan *state.AppExecResult
		notificationCh    chan *state.ContainedNotificationEvent
		transactionCh     chan *transaction.Transaction
		notaryRequestCh   chan mempoolevent.Event
		subEventsToExitCh chan struct{}
	}

	// session holds a set of iterators got after invoke* call with corresponding
	// finalizer and session expiration timer.
	session struct {
		// iteratorsLock protects iteratorIdentifiers of the current session.
		iteratorsLock sync.Mutex
		// iteratorIdentifiers stores the set of Iterator stackitems got either from original invocation
		// or from historic MPT-based invocation. In the second case, iteratorIdentifiers are supposed
		// to be filled during the first `traverseiterator` call using corresponding params.
		iteratorIdentifiers []*iteratorIdentifier
		timer               *time.Timer
		finalize            func()
	}
	// iteratorIdentifier represents Iterator on the server side, holding iterator ID and Iterator stackitem.
	iteratorIdentifier struct {
		ID string
		// Item represents Iterator stackitem.
		Item stackitem.Item
	}
)

const (
	// Default maximum number of websocket clients per Server.
	defaultMaxWebSocketClients = 64

	// Maximum number of elements for get*transfers requests.
	maxTransfersLimit = 1000

	// defaultSessionPoolSize is the number of concurrently running iterator sessions.
	defaultSessionPoolSize = 20
)

var rpcHandlers = map[string]func(*Server, params.Params) (any, *neorpc.Error){
	"calculatenetworkfee":          (*Server).calculateNetworkFee,
	"findstates":                   (*Server).findStates,
	"findstorage":                  (*Server).findStorage,
	"findstoragehistoric":          (*Server).findStorageHistoric,
	"getapplicationlog":            (*Server).getApplicationLog,
	"getbestblockhash":             (*Server).getBestBlockHash,
	"getblock":                     (*Server).getBlock,
	"getblockcount":                (*Server).getBlockCount,
	"getblockhash":                 (*Server).getBlockHash,
	"getblockheader":               (*Server).getBlockHeader,
	"getblockheadercount":          (*Server).getBlockHeaderCount,
	"getblocksysfee":               (*Server).getBlockSysFee,
	"getcandidates":                (*Server).getCandidates,
	"getcommittee":                 (*Server).getCommittee,
	"getconnectioncount":           (*Server).getConnectionCount,
	"getcontractstate":             (*Server).getContractState,
	"getnativecontracts":           (*Server).getNativeContracts,
	"getnep11balances":             (*Server).getNEP11Balances,
	"getnep11properties":           (*Server).getNEP11Properties,
	"getnep11transfers":            (*Server).getNEP11Transfers,
	"getnep17balances":             (*Server).getNEP17Balances,
	"getnep17transfers":            (*Server).getNEP17Transfers,
	"getblocks":                    (*Server).getBlocks,
	"getpeers":                     (*Server).getPeers,
	"getproof":                     (*Server).getProof,
	"getrawmempool":                (*Server).getRawMempool,
	"getrawnotarypool":             (*Server).getRawNotaryPool,
	"getrawnotarytransaction":      (*Server).getRawNotaryTransaction,
	"getrawtransaction":            (*Server).getrawtransaction,
	"getstate":                     (*Server).getState,
	"getstateheight":               (*Server).getStateHeight,
	"getstateroot":                 (*Server).getStateRoot,
	"getstorage":                   (*Server).getStorage,
	"getstoragehistoric":           (*Server).getStorageHistoric,
	"gettransactionheight":         (*Server).getTransactionHeight,
	"getunclaimedgas":              (*Server).getUnclaimedGas,
	"getnextblockvalidators":       (*Server).getNextBlockValidators,
	"getversion":                   (*Server).getVersion,
	"invokefunction":               (*Server).invokeFunction,
	"invokefunctionhistoric":       (*Server).invokeFunctionHistoric,
	"invokescript":                 (*Server).invokescript,
	"invokescripthistoric":         (*Server).invokescripthistoric,
	"invokecontractverify":         (*Server).invokeContractVerify,
	"invokecontractverifyhistoric": (*Server).invokeContractVerifyHistoric,
	"sendrawtransaction":           (*Server).sendrawtransaction,
	"submitblock":                  (*Server).submitBlock,
	"submitnotaryrequest":          (*Server).submitNotaryRequest,
	"submitoracleresponse":         (*Server).submitOracleResponse,
	"terminatesession":             (*Server).terminateSession,
	"traverseiterator":             (*Server).traverseIterator,
	"validateaddress":              (*Server).validateAddress,
	"verifyproof":                  (*Server).verifyProof,
}

var rpcWsHandlers = map[string]func(*Server, params.Params, *subscriber) (any, *neorpc.Error){
	"subscribe":   (*Server).subscribe,
	"unsubscribe": (*Server).unsubscribe,
}

// New creates a new Server struct. Pay attention that orc is expected to be either
// untyped nil or non-nil structure implementing OracleHandler interface.
func New(chain Ledger, conf config.RPC, coreServer *network.Server,
	orc OracleHandler, log *zap.Logger, errChan chan<- error) Server {
	protoCfg := chain.GetConfig().ProtocolConfiguration
	if conf.SessionEnabled {
		if conf.SessionExpirationTime <= 0 {
			conf.SessionExpirationTime = int(protoCfg.TimePerBlock / time.Second)
			if conf.SessionExpirationTime < 5 {
				conf.SessionExpirationTime = 5
			}
			log.Info("SessionExpirationTime is not set or wrong, setting default value", zap.Int("SessionExpirationTime", conf.SessionExpirationTime))
		}
		if conf.SessionPoolSize <= 0 {
			conf.SessionPoolSize = defaultSessionPoolSize
			log.Info("SessionPoolSize is not set or wrong, setting default value", zap.Int("SessionPoolSize", defaultSessionPoolSize))
		}
	}
	if conf.MaxIteratorResultItems <= 0 {
		conf.MaxIteratorResultItems = config.DefaultMaxIteratorResultItems
		log.Info("MaxIteratorResultItems is not set or wrong, setting default value", zap.Int("MaxIteratorResultItems", config.DefaultMaxIteratorResultItems))
	}
	if conf.MaxFindResultItems <= 0 {
		conf.MaxFindResultItems = config.DefaultMaxFindResultItems
		log.Info("MaxFindResultItems is not set or wrong, setting default value", zap.Int("MaxFindResultItems", config.DefaultMaxFindResultItems))
	}
	if conf.MaxFindStorageResultItems <= 0 {
		conf.MaxFindStorageResultItems = config.DefaultMaxFindStorageResultItems
		log.Info("MaxFindStorageResultItems is not set or wrong, setting default value", zap.Int("MaxFindStorageResultItems", config.DefaultMaxFindStorageResultItems))
	}
	if conf.MaxNEP11Tokens <= 0 {
		conf.MaxNEP11Tokens = config.DefaultMaxNEP11Tokens
		log.Info("MaxNEP11Tokens is not set or wrong, setting default value", zap.Int("MaxNEP11Tokens", config.DefaultMaxNEP11Tokens))
	}
	if conf.MaxRequestBodyBytes <= 0 {
		conf.MaxRequestBodyBytes = config.DefaultMaxRequestBodyBytes
		log.Info("MaxRequestBodyBytes is not set or wong, setting default value", zap.Int("MaxRequestBodyBytes", config.DefaultMaxRequestBodyBytes))
	}
	if conf.MaxRequestHeaderBytes <= 0 {
		conf.MaxRequestHeaderBytes = config.DefaultMaxRequestHeaderBytes
		log.Info("MaxRequestHeaderBytes is not set or wong, setting default value", zap.Int("MaxRequestHeaderBytes", config.DefaultMaxRequestHeaderBytes))
	}
	if conf.MaxWebSocketClients == 0 {
		conf.MaxWebSocketClients = defaultMaxWebSocketClients
		log.Info("MaxWebSocketClients is not set or wrong, setting default value", zap.Int("MaxWebSocketClients", defaultMaxWebSocketClients))
	}
	var oracleWrapped = new(atomic.Value)
	if orc != nil {
		oracleWrapped.Store(orc)
	}
	var wsOriginChecker func(*http.Request) bool
	if conf.EnableCORSWorkaround {
		wsOriginChecker = func(_ *http.Request) bool { return true }
	}

	addrs := conf.Addresses
	httpServers := make([]*http.Server, len(addrs))
	for i, addr := range addrs {
		httpServers[i] = &http.Server{
			Addr:           addr,
			MaxHeaderBytes: conf.MaxRequestHeaderBytes,
		}
	}

	var tlsServers []*http.Server
	if cfg := conf.TLSConfig; cfg.Enabled {
		addrs := cfg.Addresses
		tlsServers = make([]*http.Server, len(addrs))
		for i, addr := range addrs {
			tlsServers[i] = &http.Server{
				Addr:           addr,
				MaxHeaderBytes: conf.MaxRequestHeaderBytes,
			}
		}
	}

	return Server{
		http:  httpServers,
		https: tlsServers,

		chain:            chain,
		config:           conf,
		wsReadLimit:      int64(protoCfg.MaxBlockSize*4)/3 + 1024, // Enough for Base64-encoded content of `submitblock` and `submitp2pnotaryrequest`.
		upgrader:         websocket.Upgrader{CheckOrigin: wsOriginChecker},
		network:          protoCfg.Magic,
		stateRootEnabled: protoCfg.StateRootInHeader,
		coreServer:       coreServer,
		log:              log,
		oracle:           oracleWrapped,
		shutdown:         make(chan struct{}),
		errChan:          errChan,

		sessions: make(map[string]*session),

		subscribers: make(map[*subscriber]bool),
		// These are NOT buffered to preserve original order of events.
		blockCh:           make(chan *block.Block),
		executionCh:       make(chan *state.AppExecResult),
		notificationCh:    make(chan *state.ContainedNotificationEvent),
		transactionCh:     make(chan *transaction.Transaction),
		notaryRequestCh:   make(chan mempoolevent.Event),
		blockHeaderCh:     make(chan *block.Header),
		subEventsToExitCh: make(chan struct{}),
	}
}



// Name returns service name.
func (s *Server) Name() string {
	return "rpc"
}

// Start creates a new JSON-RPC server listening on the configured port. It creates
// goroutines needed internally and it returns its errors via errChan passed to New().
// The Server only starts once, subsequent calls to Start are no-op.
func (s *Server) Start() {
	if !s.config.Enabled {
		s.log.Info("RPC server is not enabled")
		return
	}
	if !s.started.CompareAndSwap(false, true) {
		s.log.Info("RPC server already started")
		return
	}

	go s.handleSubEvents()

	for _, srv := range s.http {
		srv.Handler = http.HandlerFunc(s.handleHTTPRequest)
		s.log.Info("starting rpc-server", zap.String("endpoint", srv.Addr))

		ln, err := net.Listen("tcp", srv.Addr)
		if err != nil {
			s.errChan <- fmt.Errorf("failed to listen on %s: %w", srv.Addr, err)
			return
		}
		srv.Addr = ln.Addr().String() // set Addr to the actual address
		go func(server *http.Server) {
			err = server.Serve(ln)
			if !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("failed to start RPC server", zap.Error(err))
				s.errChan <- err
			}
		}(srv)
	}

	if cfg := s.config.TLSConfig; cfg.Enabled {
		for _, srv := range s.https {
			srv.Handler = http.HandlerFunc(s.handleHTTPRequest)
			s.log.Info("starting rpc-server (https)", zap.String("endpoint", srv.Addr))

			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				s.errChan <- err
				return
			}
			srv.Addr = ln.Addr().String()

			go func(srv *http.Server) {
				err = srv.ServeTLS(ln, cfg.CertFile, cfg.KeyFile)
				if !errors.Is(err, http.ErrServerClosed) {
					s.log.Error("failed to start TLS RPC server",
						zap.String("endpoint", srv.Addr), zap.Error(err))
					s.errChan <- err
				}
			}(srv)
		}
	}
}

// Shutdown stops the RPC server if it's running. It can only be called once,
// subsequent calls to Shutdown on the same instance are no-op. The instance
// that was stopped can not be started again by calling Start (use a new
// instance if needed).
func (s *Server) Shutdown() {
	if !s.started.CompareAndSwap(true, false) {
		return
	}
	// Signal to websocket writer routines and handleSubEvents.
	close(s.shutdown)

	if s.config.TLSConfig.Enabled {
		for _, srv := range s.https {
			s.log.Info("shutting down RPC server (https)", zap.String("endpoint", srv.Addr))
			err := srv.Shutdown(context.Background())
			if err != nil {
				s.log.Warn("error during RPC (https) server shutdown",
					zap.String("endpoint", srv.Addr), zap.Error(err))
			}
		}
	}

	for _, srv := range s.http {
		s.log.Info("shutting down RPC server", zap.String("endpoint", srv.Addr))
		err := srv.Shutdown(context.Background())
		if err != nil {
			s.log.Warn("error during RPC (http) server shutdown",
				zap.String("endpoint", srv.Addr), zap.Error(err))
		}
	}

	// Perform sessions finalisation.
	if s.config.SessionEnabled {
		s.sessionsLock.Lock()
		for _, session := range s.sessions {
			// Concurrent iterator traversal may still be in process, thus need to protect iteratorIdentifiers access.
			session.iteratorsLock.Lock()
			session.finalize()
			if !session.timer.Stop() {
				<-session.timer.C
			}
			session.iteratorsLock.Unlock()
		}
		s.sessions = nil
		s.sessionsLock.Unlock()
	}

	// Wait for handleSubEvents to finish.
	<-s.subEventsToExitCh
	_ = s.log.Sync()
}

func (s *Server) validateAddress(reqParams params.Params) (any, *neorpc.Error) {
	param, err := reqParams.Value(0).GetString()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}

	return result.ValidateAddress{
		Address: reqParams.Value(0),
		IsValid: validateAddress(param),
	}, nil
}

// SetOracleHandler allows to update oracle handler used by the Server.
func (s *Server) SetOracleHandler(orc OracleHandler) {
	s.oracle.Store(orc)
}

// validateAddress verifies that the address is a correct Neo address
// see https://docs.neo.org/en-us/node/cli/2.9.4/api/validateaddress.html
func validateAddress(addr any) bool {
	if addr, ok := addr.(string); ok {
		_, err := address.StringToUint160(addr)
		return err == nil
	}
	return false
}

func escapeForLog(in string) string {
	return strings.Map(func(c rune) rune {
		if !strconv.IsGraphic(c) {
			return -1
		}
		return c
	}, in)
}