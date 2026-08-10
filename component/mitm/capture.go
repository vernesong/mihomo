package mitm

import (
	"bytes"
	"encoding/base64"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
)

const (
	CaptureHistoryLimit = 256
	CaptureWarning      = "[MITM] HTTP body capture is enabled; this may cause excessive memory and CPU usage"
)

type CapturedBody struct {
	Size     int64  `json:"size"`
	Encoding string `json:"encoding"`
	Content  string `json:"content"`
	Complete bool   `json:"complete"`
}

type CapturedRequest struct {
	Method  string        `json:"method"`
	URL     string        `json:"url"`
	RawURL  string        `json:"raw_url"`
	Proto   string        `json:"proto"`
	Headers http.Header   `json:"headers"`
	Body    *CapturedBody `json:"body,omitempty"`
}

type CapturedResponse struct {
	StatusCode int           `json:"statusCode"`
	Status     string        `json:"status"`
	Proto      string        `json:"proto"`
	Headers    http.Header   `json:"headers"`
	Body       *CapturedBody `json:"body,omitempty"`
}

type CapturedSession struct {
	ConnectionID string            `json:"id"`
	RequestIndex uint64            `json:"requestIndex"`
	StartedAt    time.Time         `json:"startedAt"`
	CompletedAt  *time.Time        `json:"completedAt,omitempty"`
	Source       string            `json:"source"`
	Capture      bool              `json:"capture"`
	Request      CapturedRequest   `json:"request"`
	Response     *CapturedResponse `json:"response,omitempty"`
	Error        string            `json:"error,omitempty"`
}

type CaptureSnapshot struct {
	Capture  bool              `json:"capture"`
	Limit    int               `json:"limit"`
	Sessions []CapturedSession `json:"sessions"`
}

type CaptureEvent struct {
	Type         string            `json:"type"`
	Capture      bool              `json:"capture"`
	Limit        int               `json:"limit,omitempty"`
	Sessions     []CapturedSession `json:"sessions,omitempty"`
	Session      *CapturedSession  `json:"session,omitempty"`
	ConnectionID string            `json:"id,omitempty"`
	RequestIndex uint64            `json:"requestIndex,omitempty"`
}

type captureStore struct {
	enabled          atomic.Bool
	nextRequestIndex atomic.Uint64

	mutex       sync.RWMutex
	sessions    map[uint64]CapturedSession
	order       []uint64
	subscribers map[chan CaptureEvent]struct{}
}

type captureTransaction struct {
	store   *captureStore
	capture bool

	mutex   sync.Mutex
	session CapturedSession
}

var defaultCaptureStore = newCaptureStore()

func newCaptureStore() *captureStore {
	return &captureStore{
		sessions:    make(map[uint64]CapturedSession),
		subscribers: make(map[chan CaptureEvent]struct{}),
	}
}

func CaptureEnabled() bool {
	return defaultCaptureStore.enabled.Load()
}

func SetCaptureEnabled(enabled bool) {
	if defaultCaptureStore.enabled.Swap(enabled) == enabled {
		return
	}
	defaultCaptureStore.publish(CaptureEvent{Type: "capture"})
}

func CapturedSessionsSnapshot() CaptureSnapshot {
	return defaultCaptureStore.snapshot()
}

func ClearCapturedSessions() {
	defaultCaptureStore.clear()
}

func SubscribeCaptureEvents() (<-chan CaptureEvent, func()) {
	return defaultCaptureStore.subscribe()
}

func (s *captureStore) snapshot() CaptureSnapshot {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	sessions := make([]CapturedSession, 0, len(s.order))
	for _, requestIndex := range s.order {
		if session, found := s.sessions[requestIndex]; found {
			sessions = append(sessions, cloneCapturedSession(session))
		}
	}
	return CaptureSnapshot{
		Capture:  s.enabled.Load(),
		Limit:    CaptureHistoryLimit,
		Sessions: sessions,
	}
}

func (s *captureStore) add(session CapturedSession) {
	session = cloneCapturedSession(session)
	var removedRequestIndex uint64
	var removedID string
	s.mutex.Lock()
	if len(s.order) >= CaptureHistoryLimit {
		removedRequestIndex = s.order[0]
		removedID = s.sessions[removedRequestIndex].ConnectionID
		delete(s.sessions, removedRequestIndex)
		s.order = s.order[1:]
	}
	s.sessions[session.RequestIndex] = session
	s.order = append(s.order, session.RequestIndex)
	s.mutex.Unlock()

	if removedRequestIndex != 0 {
		s.publish(CaptureEvent{Type: "remove", ConnectionID: removedID, RequestIndex: removedRequestIndex})
	}
	s.publishSession(session)
}

func (s *captureStore) updateSnapshot(session CapturedSession) {
	// The transaction already cloned this immutable snapshot, so the store can
	// take ownership without cloning it a second time.
	s.mutex.Lock()
	if _, found := s.sessions[session.RequestIndex]; !found {
		s.mutex.Unlock()
		return
	}
	s.sessions[session.RequestIndex] = session
	s.mutex.Unlock()
	s.publishSession(session)
}

func (s *captureStore) clear() {
	s.mutex.Lock()
	s.sessions = make(map[uint64]CapturedSession)
	s.order = nil
	s.mutex.Unlock()
	s.publish(CaptureEvent{Type: "clear"})
}

func (s *captureStore) publishSession(session CapturedSession) {
	eventSession := cloneCapturedSession(session)
	s.publish(CaptureEvent{Type: "session", Session: &eventSession})
}

func (s *captureStore) publish(event CaptureEvent) {
	event.Capture = s.enabled.Load()
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for subscriber := range s.subscribers {
		select {
		case subscriber <- event:
		default:
			delete(s.subscribers, subscriber)
			close(subscriber)
		}
	}
}

func (s *captureStore) subscribe() (<-chan CaptureEvent, func()) {
	subscriber := make(chan CaptureEvent, 128)
	s.mutex.Lock()
	s.subscribers[subscriber] = struct{}{}
	s.mutex.Unlock()

	var once sync.Once
	return subscriber, func() {
		once.Do(func() {
			s.mutex.Lock()
			if _, found := s.subscribers[subscriber]; found {
				delete(s.subscribers, subscriber)
				close(subscriber)
			}
			s.mutex.Unlock()
		})
	}
}

func beginCaptureSession(request *http.Request, metadata *C.Metadata) *captureTransaction {
	capture := defaultCaptureStore.enabled.Load()
	source := ""
	if metadata != nil && metadata.SourceValid() {
		source = metadata.SourceDetail()
	}
	requestURL := fullRequestURL(request)
	transaction := &captureTransaction{
		store:   defaultCaptureStore,
		capture: capture,
		session: CapturedSession{
			RequestIndex: defaultCaptureStore.nextRequestIndex.Add(1),
			StartedAt:    time.Now(),
			Source:       source,
			Capture:      capture,
			Request: CapturedRequest{
				Method:  request.Method,
				URL:     requestURL,
				RawURL:  requestURL,
				Proto:   request.Proto,
				Headers: captureRequestHeaders(request),
			},
		},
	}

	if capture {
		if request.Body == nil || request.Body == http.NoBody {
			transaction.session.Request.Body = newCapturedBody(nil, true)
		} else {
			request.Body = newObservedBody(request.Body, true, request.ContentLength, transaction.finishRequestBody)
		}
	}
	transaction.store.add(transaction.session)
	return transaction
}

func (t *captureTransaction) setRequestURL(requestURL string) {
	if requestURL == "" {
		return
	}
	t.mutex.Lock()
	if t.session.Request.URL == requestURL {
		t.mutex.Unlock()
		return
	}
	t.session.Request.URL = requestURL
	snapshot := cloneCapturedSession(t.session)
	t.mutex.Unlock()
	t.store.updateSnapshot(snapshot)
}

func (t *captureTransaction) setConnectionID(id string) {
	if id == "" {
		return
	}
	t.mutex.Lock()
	if t.session.ConnectionID == id {
		t.mutex.Unlock()
		return
	}
	t.session.ConnectionID = id
	snapshot := cloneCapturedSession(t.session)
	t.mutex.Unlock()
	t.store.updateSnapshot(snapshot)
}

func (t *captureTransaction) observeResponse(response *http.Response) {
	if response == nil {
		return
	}

	t.mutex.Lock()
	t.session.Response = &CapturedResponse{
		StatusCode: response.StatusCode,
		Status:     response.Status,
		Proto:      response.Proto,
		Headers:    cloneHTTPHeader(response.Header),
	}
	if response.StatusCode == http.StatusSwitchingProtocols {
		now := time.Now()
		t.session.CompletedAt = &now
	}
	if t.capture && (response.Body == nil || response.Body == http.NoBody) {
		t.session.Response.Body = newCapturedBody(nil, true)
		now := time.Now()
		t.session.CompletedAt = &now
	}
	snapshot := cloneCapturedSession(t.session)
	t.mutex.Unlock()
	t.store.updateSnapshot(snapshot)

	if response.StatusCode == http.StatusSwitchingProtocols {
		return
	}
	if response.Body == nil || response.Body == http.NoBody {
		if !t.capture {
			t.finishResponseBody(nil, true)
		}
		return
	}
	response.Body = newObservedBody(response.Body, t.capture, response.ContentLength, t.finishResponseBody)
}

func (t *captureTransaction) setError(err error) {
	if err == nil {
		return
	}
	t.mutex.Lock()
	t.session.Error = err.Error()
	snapshot := cloneCapturedSession(t.session)
	t.mutex.Unlock()
	t.store.updateSnapshot(snapshot)
}

func (t *captureTransaction) finishRequestBody(data []byte, complete bool) {
	if !t.capture {
		return
	}
	t.mutex.Lock()
	t.session.Request.Body = newCapturedBody(data, complete)
	snapshot := cloneCapturedSession(t.session)
	t.mutex.Unlock()
	t.store.updateSnapshot(snapshot)
}

func (t *captureTransaction) finishResponseBody(data []byte, complete bool) {
	t.mutex.Lock()
	if t.session.Response != nil && t.capture {
		t.session.Response.Body = newCapturedBody(data, complete)
	}
	if t.session.CompletedAt == nil {
		now := time.Now()
		t.session.CompletedAt = &now
	}
	snapshot := cloneCapturedSession(t.session)
	t.mutex.Unlock()
	t.store.updateSnapshot(snapshot)
}

type observedBody struct {
	body     io.ReadCloser
	capture  bool
	expected int64
	onDone   func([]byte, bool)

	mutex  sync.Mutex
	buffer bytes.Buffer
	size   int64
	once   sync.Once
}

func newObservedBody(body io.ReadCloser, capture bool, expected int64, onDone func([]byte, bool)) io.ReadCloser {
	return &observedBody{
		body:     body,
		capture:  capture,
		expected: expected,
		onDone:   onDone,
	}
}

func (b *observedBody) Read(buffer []byte) (int, error) {
	read, err := b.body.Read(buffer)
	if read > 0 {
		b.mutex.Lock()
		b.size += int64(read)
		if b.capture {
			_, _ = b.buffer.Write(buffer[:read])
		}
		b.mutex.Unlock()
	}
	if err != nil {
		b.finish(err == io.EOF)
	}
	return read, err
}

func (b *observedBody) Close() error {
	err := b.body.Close()
	b.mutex.Lock()
	complete := b.expected >= 0 && b.size >= b.expected
	b.mutex.Unlock()
	b.finish(complete)
	return err
}

func (b *observedBody) finish(complete bool) {
	b.once.Do(func() {
		b.mutex.Lock()
		var data []byte
		if b.capture {
			data = append(data, b.buffer.Bytes()...)
			b.buffer.Reset()
		}
		b.mutex.Unlock()
		b.onDone(data, complete)
	})
}

func newCapturedBody(data []byte, complete bool) *CapturedBody {
	body := &CapturedBody{
		Size:     int64(len(data)),
		Encoding: "utf8",
		Content:  string(data),
		Complete: complete,
	}
	if !utf8.Valid(data) {
		body.Encoding = "base64"
		body.Content = base64.StdEncoding.EncodeToString(data)
	}
	return body
}

func cloneCapturedSession(session CapturedSession) CapturedSession {
	cloned := session
	cloned.Request.Headers = cloneHTTPHeader(session.Request.Headers)
	if session.Request.Body != nil {
		body := *session.Request.Body
		cloned.Request.Body = &body
	}
	if session.Response != nil {
		response := *session.Response
		response.Headers = cloneHTTPHeader(session.Response.Headers)
		if session.Response.Body != nil {
			body := *session.Response.Body
			response.Body = &body
		}
		cloned.Response = &response
	}
	if session.CompletedAt != nil {
		completedAt := *session.CompletedAt
		cloned.CompletedAt = &completedAt
	}
	return cloned
}

func cloneHTTPHeader(header http.Header) http.Header {
	if header == nil {
		return make(http.Header)
	}
	return header.Clone()
}

func captureRequestHeaders(request *http.Request) http.Header {
	header := cloneHTTPHeader(request.Header)
	// net/http promotes the incoming Host header to Request.Host.
	if request.Host != "" && header.Get("Host") == "" {
		header.Set("Host", request.Host)
	}
	return header
}
