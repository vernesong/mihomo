package hysteria2_realm

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/metacubex/chi"
	"github.com/metacubex/http"
)

type punchEvent struct {
	Addresses []string `json:"addresses"`
	Nonce     string   `json:"nonce"`
	Obfs      string   `json:"obfs"`
}

const maxRequestBodyBytes = 4 << 10

var connectResponseTimeout = 10 * time.Second

func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

func (s *server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(limitBody)
	r.Post("/v1/{id}", s.register)
	r.Delete("/v1/{id}", s.deregister)
	r.Get("/v1/{id}/events", s.events)
	r.Post("/v1/{id}/heartbeat", s.heartbeat)
	r.Post("/v1/{id}/connect", s.connect)
	r.Post("/v1/{id}/connects/{nonce}", s.connectResponse)
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, errNotFound, "unknown path")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusMethodNotAllowed, errBadRequest, "method not allowed")
	})
	return r
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	s.routes().ServeHTTP(w, r)
}

func (s *server) checkRealmToken(r *http.Request) bool {
	expected := s.realmToken
	return expected != "" && bearer(r) == expected
}

func (s *server) requestIP(r *http.Request) string {
	return clientIP(r, s.proxyHeader)
}

func (s *server) realmID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := chi.URLParam(r, "id")
	if !s.realmIDPattern.MatchString(id) {
		writeErr(w, http.StatusBadRequest, errBadRequest, "invalid realm name")
		return "", false
	}
	return id, true
}

func (s *server) register(w http.ResponseWriter, r *http.Request) {
	id, ok := s.realmID(w, r)
	if !ok {
		return
	}
	remote := s.requestIP(r)
	if !s.checkRealmToken(r) {
		debugf("register unauthorized realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusUnauthorized, errInvalidToken, "invalid realm token")
		return
	}
	var req struct {
		Addresses []string `json:"addresses"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, "invalid json")
		return
	}
	if err := validateAddresses(req.Addresses); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, err.Error())
		return
	}

	ip := remote
	s.mu.Lock()
	if _, exists := s.realms[id]; exists {
		s.mu.Unlock()
		debugf("register conflict realm=%s remote=%s", id, ip)
		writeErr(w, http.StatusConflict, errRealmTaken, "realm already registered")
		return
	}
	if s.maxRealms > 0 && len(s.realms) >= s.maxRealms {
		s.mu.Unlock()
		debugf("register rejected (global limit) realm=%s remote=%s", id, ip)
		writeErr(w, http.StatusTooManyRequests, errRealmLimit, "global realm limit reached")
		return
	}
	if s.maxRealmsPerIP > 0 && s.ipCounts[ip] >= s.maxRealmsPerIP {
		s.mu.Unlock()
		debugf("register rejected (per-ip limit) realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusTooManyRequests, errIPLimit, "per-ip realm limit reached")
		return
	}
	sess := &session{
		id:        randToken(),
		realmID:   id,
		addresses: req.Addresses,
		expires:   time.Now().Add(sessionTTL),
		events:    make(chan sessionEvent, eventsBufferSize),
		done:      make(chan struct{}),
		clientIP:  ip,
		pending:   make(map[string]chan punchResponsePayload),
	}
	s.realms[id] = sess
	s.sessions[sess.id] = sess
	s.ipCounts[ip]++
	s.mu.Unlock()
	debugf("registered realm=%s session=%s addresses=%d remote=%s", id, sess.id, len(req.Addresses), ip)

	writeJSON(w, http.StatusOK, map[string]any{
		"session_id": sess.id,
		"ttl":        int(sessionTTL.Seconds()),
	})
}

func (s *server) deregister(w http.ResponseWriter, r *http.Request) {
	id, ok := s.realmID(w, r)
	if !ok {
		return
	}
	remote := s.requestIP(r)
	sess := s.getSessionByToken(bearer(r))
	if sess == nil || sess.realmID != id {
		debugf("deregister unauthorized realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusUnauthorized, errInvalidToken, "invalid session token")
		return
	}
	debugf("deregistered realm=%s session=%s remote=%s", id, sess.id, remote)
	s.removeSession(sess)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) heartbeat(w http.ResponseWriter, r *http.Request) {
	id, ok := s.realmID(w, r)
	if !ok {
		return
	}
	remote := s.requestIP(r)
	sess := s.getSessionByToken(bearer(r))
	if sess == nil || sess.realmID != id {
		debugf("heartbeat unauthorized realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusUnauthorized, errInvalidToken, "invalid session token")
		return
	}
	var req struct {
		Addresses []string `json:"addresses"`
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			writeErr(w, http.StatusBadRequest, errBadRequest, "invalid json")
			return
		}
	}
	if req.Addresses != nil {
		if err := validateAddresses(req.Addresses); err != nil {
			writeErr(w, http.StatusBadRequest, errBadRequest, err.Error())
			return
		}
	}
	s.mu.Lock()
	if sess.closed {
		s.mu.Unlock()
		debugf("heartbeat closed realm=%s session=%s remote=%s", id, sess.id, remote)
		writeErr(w, http.StatusUnauthorized, errInvalidToken, "invalid session token")
		return
	}
	sess.expires = time.Now().Add(sessionTTL)
	if req.Addresses != nil {
		sess.addresses = append([]string(nil), req.Addresses...)
	}
	s.mu.Unlock()
	debugf("heartbeat realm=%s session=%s addressesUpdated=%t remote=%s", id, sess.id, req.Addresses != nil, remote)
	if !s.sendEvent(sess, sessionEvent{kind: "heartbeat_ack", data: map[string]any{"ttl": int(sessionTTL.Seconds())}}) {
		debugf("heartbeat ack dropped realm=%s session=%s remote=%s", id, sess.id, remote)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ttl": int(sessionTTL.Seconds())})
}

func (s *server) events(w http.ResponseWriter, r *http.Request) {
	id, ok := s.realmID(w, r)
	if !ok {
		return
	}
	remote := s.requestIP(r)
	sess := s.getSessionByToken(bearer(r))
	if sess == nil || sess.realmID != id {
		debugf("events unauthorized realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusUnauthorized, errInvalidToken, "invalid session token")
		return
	}
	debugf("events connected realm=%s session=%s remote=%s", id, sess.id, remote)
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, errBadRequest, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			debugf("events disconnected realm=%s session=%s remote=%s", id, sess.id, remote)
			return
		case <-sess.done:
			debugf("events closed realm=%s session=%s remote=%s", id, sess.id, remote)
			return
		case ev := <-sess.events:
			data, _ := json.Marshal(ev.data)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.kind, data)
			flusher.Flush()
		}
	}
}

func (s *server) connect(w http.ResponseWriter, r *http.Request) {
	id, ok := s.realmID(w, r)
	if !ok {
		return
	}
	remote := s.requestIP(r)
	if !s.checkRealmToken(r) {
		debugf("connect unauthorized realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusUnauthorized, errInvalidToken, "invalid realm token")
		return
	}
	var req struct {
		Addresses []string `json:"addresses"`
		Nonce     string   `json:"nonce"`
		Obfs      string   `json:"obfs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, "invalid json")
		return
	}
	if err := validateAddresses(req.Addresses); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, err.Error())
		return
	}
	if err := validateHexField("nonce", req.Nonce, nonceHexLength); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, err.Error())
		return
	}
	if err := validateHexField("obfs", req.Obfs, obfsHexLength); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, err.Error())
		return
	}

	s.mu.Lock()
	sess := s.realms[id]
	if sess == nil || sess.closed || time.Now().After(sess.expires) {
		s.mu.Unlock()
		debugf("connect realm not found realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusNotFound, errRealmNotFound, "realm not registered")
		return
	}
	serverAddrs := append([]string(nil), sess.addresses...)
	s.mu.Unlock()

	// Register the pending entry BEFORE pushing the punch event,
	// so a fast Hysteria server can never deliver before we're ready.
	respCh, ok := s.registerPending(sess, req.Nonce)
	if !ok {
		debugf("connect rate limited (pending) realm=%s session=%s remote=%s", id, sess.id, remote)
		writeErr(w, http.StatusServiceUnavailable, errRateLimited, "too many in-flight connect attempts")
		return
	}
	defer s.cancelPending(sess, req.Nonce)

	if s.sendEvent(sess, sessionEvent{kind: "punch", data: punchEvent{Addresses: req.Addresses, Nonce: req.Nonce, Obfs: req.Obfs}}) {
		debugf("connect notified realm=%s session=%s clientAddresses=%d serverAddresses=%d remote=%s", id, sess.id, len(req.Addresses), len(serverAddrs), remote)
	} else {
		debugf("connect rate limited realm=%s session=%s remote=%s", id, sess.id, remote)
		writeErr(w, http.StatusServiceUnavailable, errRateLimited, "server event buffer full")
		return
	}

	// Now we wait for the fresh addresses to arrive.
	timer := time.NewTimer(connectResponseTimeout)
	defer timer.Stop()
	select {
	case payload, ok := <-respCh:
		if !ok {
			debugf("connect canceled realm=%s session=%s remote=%s", id, sess.id, remote)
			writeErr(w, http.StatusNotFound, errRealmNotFound, "realm not registered")
			return
		}
		if len(payload.addresses) > 0 {
			serverAddrs = payload.addresses
			debugf("connect fresh addresses realm=%s session=%s addresses=%d remote=%s", id, sess.id, len(serverAddrs), remote)
		}
	case <-timer.C:
		debugf("connect response timed out realm=%s session=%s remote=%s", id, sess.id, remote)
	case <-sess.done:
		debugf("connect canceled realm=%s session=%s remote=%s", id, sess.id, remote)
		writeErr(w, http.StatusNotFound, errRealmNotFound, "realm not registered")
		return
	case <-r.Context().Done():
		// Client gave up
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"addresses": serverAddrs,
		"nonce":     req.Nonce,
		"obfs":      req.Obfs,
	})
}

func (s *server) connectResponse(w http.ResponseWriter, r *http.Request) {
	id, ok := s.realmID(w, r)
	if !ok {
		return
	}
	remote := s.requestIP(r)
	nonce := chi.URLParam(r, "nonce")
	if err := validateHexField("nonce", nonce, nonceHexLength); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, err.Error())
		return
	}
	sess := s.getSessionByToken(bearer(r))
	if sess == nil || sess.realmID != id {
		debugf("connect-response unauthorized realm=%s remote=%s", id, remote)
		writeErr(w, http.StatusUnauthorized, errInvalidToken, "invalid session token")
		return
	}
	var req struct {
		Addresses []string `json:"addresses"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, "invalid json")
		return
	}
	if err := validateAddresses(req.Addresses); err != nil {
		writeErr(w, http.StatusBadRequest, errBadRequest, err.Error())
		return
	}
	if !s.deliverPending(sess, nonce, punchResponsePayload{addresses: req.Addresses}) {
		debugf("connect-response no pending realm=%s session=%s nonce=%s remote=%s", id, sess.id, nonce, remote)
		writeErr(w, http.StatusNotFound, errAttemptNotFound, "no pending attempt for nonce")
		return
	}
	debugf("connect-response delivered realm=%s session=%s addresses=%d remote=%s", id, sess.id, len(req.Addresses), remote)
	w.WriteHeader(http.StatusNoContent)
}
