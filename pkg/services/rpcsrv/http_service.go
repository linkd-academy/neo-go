package rpcsrv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/rpcevent"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv/params"
	"go.uber.org/zap"
)

const (
	wsPongLimit  = 60 * time.Second
	wsPingPeriod = wsPongLimit / 2
	wsWriteLimit = wsPingPeriod / 2
)


func (s *Server) handleWsWrites(ws *websocket.Conn, resChan <-chan abstractResult, subChan <-chan intEvent) {
	pingTicker := time.NewTicker(wsPingPeriod)
eventloop:
	for {
		select {
		case <-s.shutdown:
			break eventloop
		case event, ok := <-subChan:
			if !ok {
				break eventloop
			}
			if err := ws.SetWriteDeadline(time.Now().Add(wsWriteLimit)); err != nil {
				break eventloop
			}
			if err := ws.WritePreparedMessage(event.msg); err != nil {
				break eventloop
			}
		case res, ok := <-resChan:
			if !ok {
				break eventloop
			}
			if err := ws.SetWriteDeadline(time.Now().Add(wsWriteLimit)); err != nil {
				break eventloop
			}
			if err := ws.WriteJSON(res); err != nil {
				break eventloop
			}
		case <-pingTicker.C:
			if err := ws.SetWriteDeadline(time.Now().Add(wsWriteLimit)); err != nil {
				break eventloop
			}
			if err := ws.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				break eventloop
			}
		}
	}
	ws.Close()
	pingTicker.Stop()
drainloop:
	for {
		select {
		case _, ok := <-subChan:
			if !ok {
				break drainloop
			}
		default:
			break drainloop
		}
	}
}

func (s *Server) handleWsReads(ws *websocket.Conn, resChan chan<- abstractResult, subscr *subscriber) {
	ws.SetReadLimit(s.wsReadLimit)
	err := ws.SetReadDeadline(time.Now().Add(wsPongLimit))
	ws.SetPongHandler(func(string) error { return ws.SetReadDeadline(time.Now().Add(wsPongLimit)) })
requestloop:
	for err == nil {
		req := params.NewRequest()
		err := ws.ReadJSON(req)
		if err != nil {
			break
		}
		res := s.handleRequest(req, subscr)
		res.RunForErrors(func(jsonErr *neorpc.Error) {
			s.logRequestError(req, jsonErr)
		})
		select {
		case <-s.shutdown:
			break requestloop
		case resChan <- res:
		}
	}
	s.dropSubscriber(subscr)
	close(resChan)
	ws.Close()
}

func (s *Server) handleHTTPRequest(w http.ResponseWriter, httpRequest *http.Request) {
	// Restrict request body before further processing.
	httpRequest.Body = http.MaxBytesReader(w, httpRequest.Body, int64(s.config.MaxRequestBodyBytes))
	req := params.NewRequest()

	if httpRequest.URL.Path == "/ws" && httpRequest.Method == "GET" {
		// Technically there is a race between this check and
		// s.subscribers modification 20 lines below, but it's tiny
		// and not really critical to bother with it. Some additional
		// clients may sneak in, no big deal.
		s.subsLock.RLock()
		numOfSubs := len(s.subscribers)
		s.subsLock.RUnlock()
		if numOfSubs >= s.config.MaxWebSocketClients {
			s.writeHTTPErrorResponse(
				params.NewIn(),
				w,
				neorpc.NewInternalServerError("websocket users limit reached"),
			)
			return
		}
		ws, err := s.upgrader.Upgrade(w, httpRequest, nil)
		if err != nil {
			s.log.Info("websocket connection upgrade failed", zap.Error(err))
			return
		}
		resChan := make(chan abstractResult) // response.abstract or response.abstractBatch
		subChan := make(chan intEvent, notificationBufSize)
		subscr := &subscriber{writer: subChan}
		s.subsLock.Lock()
		s.subscribers[subscr] = true
		s.subsLock.Unlock()
		go s.handleWsWrites(ws, resChan, subChan)
		s.handleWsReads(ws, resChan, subscr)
		return
	}

	if httpRequest.Method == "OPTIONS" && s.config.EnableCORSWorkaround { // Preflight CORS.
		setCORSOriginHeaders(w.Header())
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST") // GET for websockets.
		w.Header().Set("Access-Control-Max-Age", "21600")           // 6 hours.
		return
	}

	if httpRequest.Method != "POST" {
		s.writeHTTPErrorResponse(
			params.NewIn(),
			w,
			neorpc.NewInvalidParamsError(fmt.Sprintf("invalid method '%s', please retry with 'POST'", httpRequest.Method)),
		)
		return
	}

	err := req.DecodeData(httpRequest.Body)
	if err != nil {
		s.writeHTTPErrorResponse(params.NewIn(), w, neorpc.NewParseError(err.Error()))
		return
	}

	resp := s.handleRequest(req, nil)
	s.writeHTTPServerResponse(req, w, resp)
}

// RegisterLocal performs local client registration.
func (s *Server) RegisterLocal(ctx context.Context, events chan<- neorpc.Notification) func(*neorpc.Request) (*neorpc.Response, error) {
	subChan := make(chan intEvent, notificationBufSize)
	subscr := &subscriber{writer: subChan}
	s.subsLock.Lock()
	s.subscribers[subscr] = true
	s.subsLock.Unlock()
	go s.handleLocalNotifications(ctx, events, subChan, subscr)
	return func(req *neorpc.Request) (*neorpc.Response, error) {
		return s.handleInternal(req, subscr)
	}
}

func (s *Server) handleRequest(req *params.Request, sub *subscriber) abstractResult {
	if req.In != nil {
		req.In.Method = escapeForLog(req.In.Method) // No valid method name will be changed by it.
		return s.handleIn(req.In, sub)
	}
	resp := make(abstractBatch, len(req.Batch))
	for i, in := range req.Batch {
		in.Method = escapeForLog(in.Method) // No valid method name will be changed by it.
		resp[i] = s.handleIn(&in, sub)
	}
	return resp
}

// handleInternal is an experimental interface to handle client requests directly.
func (s *Server) handleInternal(req *neorpc.Request, sub *subscriber) (*neorpc.Response, error) {
	var (
		res    any
		rpcRes = &neorpc.Response{
			HeaderAndError: neorpc.HeaderAndError{
				Header: neorpc.Header{
					JSONRPC: req.JSONRPC,
					ID:      json.RawMessage(strconv.FormatUint(req.ID, 10)),
				},
			},
		}
	)
	reqParams, err := params.FromAny(req.Params)
	if err != nil {
		return nil, err
	}

	s.log.Debug("processing local rpc request",
		zap.String("method", req.Method),
		zap.Stringer("params", reqParams))

	start := time.Now()
	defer func() { addReqTimeMetric(req.Method, time.Since(start)) }()

	rpcRes.Error = neorpc.NewMethodNotFoundError(fmt.Sprintf("method %q not supported", req.Method))
	handler, ok := rpcHandlers[req.Method]
	if ok {
		res, rpcRes.Error = handler(s, reqParams)
	} else if sub != nil {
		handler, ok := rpcWsHandlers[req.Method]
		if ok {
			res, rpcRes.Error = handler(s, reqParams, sub)
		}
	}
	if res != nil {
		b, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("response can't be JSONized: %w", err)
		}
		rpcRes.Result = json.RawMessage(b)
	}
	return rpcRes, nil
}

func (s *Server) handleIn(req *params.In, sub *subscriber) abstract {
	var res any
	var resErr *neorpc.Error
	if req.JSONRPC != neorpc.JSONRPCVersion {
		return s.packResponse(req, nil, neorpc.NewInvalidParamsError(fmt.Sprintf("problem parsing JSON: invalid version, expected 2.0 got '%s'", req.JSONRPC)))
	}

	reqParams := params.Params(req.RawParams)

	s.log.Debug("processing rpc request",
		zap.String("method", req.Method),
		zap.Stringer("params", reqParams))

	start := time.Now()
	defer func() { addReqTimeMetric(req.Method, time.Since(start)) }()

	resErr = neorpc.NewMethodNotFoundError(fmt.Sprintf("method %q not supported", req.Method))
	handler, ok := rpcHandlers[req.Method]
	if ok {
		res, resErr = handler(s, reqParams)
	} else if sub != nil {
		handler, ok := rpcWsHandlers[req.Method]
		if ok {
			res, resErr = handler(s, reqParams, sub)
		}
	}
	return s.packResponse(req, res, resErr)
}

func (s *Server) handleLocalNotifications(ctx context.Context, events chan<- neorpc.Notification, subChan <-chan intEvent, subscr *subscriber) {
eventloop:
	for {
		select {
		case <-s.shutdown:
			break eventloop
		case <-ctx.Done():
			break eventloop
		case ev := <-subChan:
			events <- *ev.ntf // Make a copy.
		}
	}
	close(events)
	s.dropSubscriber(subscr)
drainloop:
	for {
		select {
		case <-subChan:
		default:
			break drainloop
		}
	}
}


func (s *Server) dropSubscriber(subscr *subscriber) {
	s.subsLock.Lock()
	delete(s.subscribers, subscr)
	s.subsLock.Unlock()
	s.subsCounterLock.Lock()
	for _, e := range subscr.feeds {
		if e.event != neorpc.InvalidEventID {
			s.unsubscribeFromChannel(e.event)
		}
	}
	s.subsCounterLock.Unlock()
}


func setCORSOriginHeaders(h http.Header) {
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With")
}

func (s *Server) writeHTTPServerResponse(r *params.Request, w http.ResponseWriter, resp abstractResult) {
	// Errors can happen in many places and we can only catch ALL of them here.
	resp.RunForErrors(func(jsonErr *neorpc.Error) {
		s.logRequestError(r, jsonErr)
	})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if s.config.EnableCORSWorkaround {
		setCORSOriginHeaders(w.Header())
	}
	if r.In != nil {
		resp := resp.(abstract)
		if resp.Error != nil {
			w.WriteHeader(getHTTPCodeForError(resp.Error))
		}
	}

	encoder := json.NewEncoder(w)
	err := encoder.Encode(resp)

	if err != nil {
		switch {
		case r.In != nil:
			s.log.Error("Error encountered while encoding response",
				zap.String("err", err.Error()),
				zap.String("method", r.In.Method))
		case r.Batch != nil:
			s.log.Error("Error encountered while encoding batch response",
				zap.String("err", err.Error()))
		}
	}
}

// subscribe handles subscription requests from websocket clients.
func (s *Server) subscribe(reqParams params.Params, sub *subscriber) (any, *neorpc.Error) {
	streamName, err := reqParams.Value(0).GetString()
	if err != nil {
		return nil, neorpc.ErrInvalidParams
	}
	event, err := neorpc.GetEventIDFromString(streamName)
	if err != nil || event == neorpc.MissedEventID {
		return nil, neorpc.ErrInvalidParams
	}
	if event == neorpc.NotaryRequestEventID && !s.chain.P2PSigExtensionsEnabled() {
		return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, "P2PSigExtensions are disabled")
	}
	// Optional filter.
	var filter neorpc.SubscriptionFilter
	if p := reqParams.Value(1); p != nil {
		param := *p
		jd := json.NewDecoder(bytes.NewReader(param.RawMessage))
		jd.DisallowUnknownFields()
		switch event {
		case neorpc.BlockEventID, neorpc.HeaderOfAddedBlockEventID:
			flt := new(neorpc.BlockFilter)
			err = jd.Decode(flt)
			filter = *flt
		case neorpc.TransactionEventID:
			flt := new(neorpc.TxFilter)
			err = jd.Decode(flt)
			filter = *flt
		case neorpc.NotaryRequestEventID:
			flt := new(neorpc.NotaryRequestFilter)
			err = jd.Decode(flt)
			filter = *flt
		case neorpc.NotificationEventID:
			flt := new(neorpc.NotificationFilter)
			err = jd.Decode(flt)
			filter = *flt
		case neorpc.ExecutionEventID:
			flt := new(neorpc.ExecutionFilter)
			err = jd.Decode(flt)
			filter = *flt
		}
		if err != nil {
			return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
		}
	}
	if filter != nil {
		err = filter.IsValid()
		if err != nil {
			return nil, neorpc.WrapErrorWithData(neorpc.ErrInvalidParams, err.Error())
		}
	}

	s.subsLock.Lock()
	var id int
	for ; id < len(sub.feeds); id++ {
		if sub.feeds[id].event == neorpc.InvalidEventID {
			break
		}
	}
	if id == len(sub.feeds) {
		s.subsLock.Unlock()
		return nil, neorpc.NewInternalServerError("maximum number of subscriptions is reached")
	}
	sub.feeds[id].event = event
	sub.feeds[id].filter = filter
	s.subsLock.Unlock()

	s.subsCounterLock.Lock()
	select {
	case <-s.shutdown:
		s.subsCounterLock.Unlock()
		return nil, neorpc.NewInternalServerError("server is shutting down")
	default:
	}
	s.subscribeToChannel(event)
	s.subsCounterLock.Unlock()
	return strconv.FormatInt(int64(id), 10), nil
}

// subscribeToChannel subscribes RPC server to appropriate chain events if
// it's not yet subscribed for them. It's supposed to be called with s.subsCounterLock
// taken by the caller.
func (s *Server) subscribeToChannel(event neorpc.EventID) {
	switch event {
	case neorpc.BlockEventID:
		if s.blockSubs == 0 {
			s.chain.SubscribeForBlocks(s.blockCh)
		}
		s.blockSubs++
	case neorpc.TransactionEventID:
		if s.transactionSubs == 0 {
			s.chain.SubscribeForTransactions(s.transactionCh)
		}
		s.transactionSubs++
	case neorpc.NotificationEventID:
		if s.notificationSubs == 0 {
			s.chain.SubscribeForNotifications(s.notificationCh)
		}
		s.notificationSubs++
	case neorpc.ExecutionEventID:
		if s.executionSubs == 0 {
			s.chain.SubscribeForExecutions(s.executionCh)
		}
		s.executionSubs++
	case neorpc.NotaryRequestEventID:
		if s.notaryRequestSubs == 0 {
			s.coreServer.SubscribeForNotaryRequests(s.notaryRequestCh)
		}
		s.notaryRequestSubs++
	case neorpc.HeaderOfAddedBlockEventID:
		if s.blockHeaderSubs == 0 {
			s.chain.SubscribeForHeadersOfAddedBlocks(s.blockHeaderCh)
		}
		s.blockHeaderSubs++
	}
}

// unsubscribe handles unsubscription requests from websocket clients.
func (s *Server) unsubscribe(reqParams params.Params, sub *subscriber) (any, *neorpc.Error) {
	id, err := reqParams.Value(0).GetInt()
	if err != nil || id < 0 {
		return nil, neorpc.ErrInvalidParams
	}
	s.subsLock.Lock()
	if len(sub.feeds) <= id || sub.feeds[id].event == neorpc.InvalidEventID {
		s.subsLock.Unlock()
		return nil, neorpc.ErrInvalidParams
	}
	event := sub.feeds[id].event
	sub.feeds[id].event = neorpc.InvalidEventID
	sub.feeds[id].filter = nil
	s.subsLock.Unlock()

	s.subsCounterLock.Lock()
	s.unsubscribeFromChannel(event)
	s.subsCounterLock.Unlock()
	return true, nil
}

// unsubscribeFromChannel unsubscribes RPC server from appropriate chain events
// if there are no other subscribers for it. It must be called with s.subsConutersLock
// holding by the caller.
func (s *Server) unsubscribeFromChannel(event neorpc.EventID) {
	switch event {
	case neorpc.BlockEventID:
		s.blockSubs--
		if s.blockSubs == 0 {
			s.chain.UnsubscribeFromBlocks(s.blockCh)
		}
	case neorpc.TransactionEventID:
		s.transactionSubs--
		if s.transactionSubs == 0 {
			s.chain.UnsubscribeFromTransactions(s.transactionCh)
		}
	case neorpc.NotificationEventID:
		s.notificationSubs--
		if s.notificationSubs == 0 {
			s.chain.UnsubscribeFromNotifications(s.notificationCh)
		}
	case neorpc.ExecutionEventID:
		s.executionSubs--
		if s.executionSubs == 0 {
			s.chain.UnsubscribeFromExecutions(s.executionCh)
		}
	case neorpc.NotaryRequestEventID:
		s.notaryRequestSubs--
		if s.notaryRequestSubs == 0 {
			s.coreServer.UnsubscribeFromNotaryRequests(s.notaryRequestCh)
		}
	case neorpc.HeaderOfAddedBlockEventID:
		s.blockHeaderSubs--
		if s.blockHeaderSubs == 0 {
			s.chain.UnsubscribeFromHeadersOfAddedBlocks(s.blockHeaderCh)
		}
	}
}

// handleSubEvents processes Server subscriptions until Shutdown. Upon
// completion signals to subEventCh channel.
func (s *Server) handleSubEvents() {
	var overflowEvent = neorpc.Notification{
		JSONRPC: neorpc.JSONRPCVersion,
		Event:   neorpc.MissedEventID,
		Payload: make([]any, 0),
	}
	b, err := json.Marshal(overflowEvent)
	if err != nil {
		s.log.Error("fatal: failed to marshal overflow event", zap.Error(err))
		return
	}
	overflowMsg, err := websocket.NewPreparedMessage(websocket.TextMessage, b)
	if err != nil {
		s.log.Error("fatal: failed to prepare overflow message", zap.Error(err))
		return
	}
chloop:
	for {
		var resp = neorpc.Notification{
			JSONRPC: neorpc.JSONRPCVersion,
			Payload: make([]any, 1),
		}
		var msg *websocket.PreparedMessage
		select {
		case <-s.shutdown:
			break chloop
		case b := <-s.blockCh:
			resp.Event = neorpc.BlockEventID
			resp.Payload[0] = b
		case execution := <-s.executionCh:
			resp.Event = neorpc.ExecutionEventID
			resp.Payload[0] = execution
		case notification := <-s.notificationCh:
			resp.Event = neorpc.NotificationEventID
			resp.Payload[0] = notification
		case tx := <-s.transactionCh:
			resp.Event = neorpc.TransactionEventID
			resp.Payload[0] = tx
		case e := <-s.notaryRequestCh:
			resp.Event = neorpc.NotaryRequestEventID
			resp.Payload[0] = &result.NotaryRequestEvent{
				Type:          e.Type,
				NotaryRequest: e.Data.(*payload.P2PNotaryRequest),
			}
		case header := <-s.blockHeaderCh:
			resp.Event = neorpc.HeaderOfAddedBlockEventID
			resp.Payload[0] = header
		}
		s.subsLock.RLock()
	subloop:
		for sub := range s.subscribers {
			if sub.overflown.Load() {
				continue
			}
			for i := range sub.feeds {
				if rpcevent.Matches(sub.feeds[i], &resp) {
					if msg == nil {
						b, err = json.Marshal(resp)
						if err != nil {
							s.log.Error("failed to marshal notification",
								zap.Error(err),
								zap.Stringer("type", resp.Event))
							break subloop
						}
						msg, err = websocket.NewPreparedMessage(websocket.TextMessage, b)
						if err != nil {
							s.log.Error("failed to prepare notification message",
								zap.Error(err),
								zap.Stringer("type", resp.Event))
							break subloop
						}
					}
					select {
					case sub.writer <- intEvent{msg, &resp}:
					default:
						sub.overflown.Store(true)
						// MissedEvent is to be delivered eventually.
						go func(sub *subscriber) {
							sub.writer <- intEvent{overflowMsg, &overflowEvent}
							sub.overflown.Store(false)
						}(sub)
					}
					// The message is sent only once per subscriber.
					break
				}
			}
		}
		s.subsLock.RUnlock()
	}
	// It's important to do it with subsCounterLock held because no subscription routine
	// should be running concurrently to this one. And even if one is to run
	// after unlock, it'll see closed s.shutdown and won't subscribe.
	s.subsCounterLock.Lock()
	// There might be no subscription in reality, but it's not a problem as
	// core.Blockchain allows unsubscribing non-subscribed channels.
	s.chain.UnsubscribeFromBlocks(s.blockCh)
	s.chain.UnsubscribeFromTransactions(s.transactionCh)
	s.chain.UnsubscribeFromNotifications(s.notificationCh)
	s.chain.UnsubscribeFromExecutions(s.executionCh)
	s.chain.UnsubscribeFromHeadersOfAddedBlocks(s.blockHeaderCh)
	if s.chain.P2PSigExtensionsEnabled() {
		s.coreServer.UnsubscribeFromNotaryRequests(s.notaryRequestCh)
	}
	s.subsCounterLock.Unlock()
drainloop:
	for {
		select {
		case <-s.blockCh:
		case <-s.executionCh:
		case <-s.notificationCh:
		case <-s.transactionCh:
		case <-s.notaryRequestCh:
		case <-s.blockHeaderCh:
		default:
			break drainloop
		}
	}
	// It's not required closing these, but since they're drained already
	// this is safe.
	close(s.blockCh)
	close(s.transactionCh)
	close(s.notificationCh)
	close(s.executionCh)
	close(s.notaryRequestCh)
	close(s.blockHeaderCh)
	// notify Shutdown routine
	close(s.subEventsToExitCh)
}


func (s *Server) packResponse(r *params.In, result any, respErr *neorpc.Error) abstract {
	resp := abstract{
		Header: neorpc.Header{
			JSONRPC: r.JSONRPC,
			ID:      r.RawID,
		},
	}
	if respErr != nil {
		resp.Error = respErr
	} else {
		resp.Result = result
	}
	return resp
}

// logRequestError is a request error logger.
func (s *Server) logRequestError(r *params.Request, jsonErr *neorpc.Error) {
	logFields := []zap.Field{
		zap.Int64("code", jsonErr.Code),
	}
	if len(jsonErr.Data) != 0 {
		logFields = append(logFields, zap.String("cause", jsonErr.Data))
	}

	if r.In != nil {
		logFields = append(logFields, zap.String("method", r.In.Method))
		params := params.Params(r.In.RawParams)
		logFields = append(logFields, zap.Any("params", params))
	}

	logText := "Error encountered with rpc request"
	switch jsonErr.Code {
	case neorpc.InternalServerErrorCode:
		s.log.Error(logText, logFields...)
	default:
		s.log.Info(logText, logFields...)
	}
}

// writeHTTPErrorResponse writes an error response to the ResponseWriter.
func (s *Server) writeHTTPErrorResponse(r *params.In, w http.ResponseWriter, jsonErr *neorpc.Error) {
	resp := s.packResponse(r, nil, jsonErr)
	s.writeHTTPServerResponse(&params.Request{In: r}, w, resp)
}


func (s *Server) terminateSession(reqParams params.Params) (any, *neorpc.Error) {
	if !s.config.SessionEnabled {
		return nil, neorpc.ErrSessionsDisabled
	}
	sID, err := reqParams.Value(0).GetUUID()
	if err != nil {
		return nil, neorpc.NewInvalidParamsError(fmt.Sprintf("invalid session ID: %s", err))
	}
	strSID := sID.String()
	s.sessionsLock.Lock()
	defer s.sessionsLock.Unlock()
	session, ok := s.sessions[strSID]
	if !ok {
		return nil, neorpc.ErrUnknownSession
	}
	// Iterators access Seek channel under the hood; finalizer closes this channel, thus,
	// we need to perform finalisation under iteratorsLock.
	session.iteratorsLock.Lock()
	session.finalize()
	if !session.timer.Stop() {
		<-session.timer.C
	}
	delete(s.sessions, strSID)
	session.iteratorsLock.Unlock()
	return ok, nil
}