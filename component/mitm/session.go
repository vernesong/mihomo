package mitm

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/metacubex/mihomo/common/utils"
	F "github.com/metacubex/mihomo/component/httpflow"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
)

type Handler interface {
	HandleRequest(*Session) (*http.Request, *http.Response)
	HandleResponse(*Session) *http.Response
	HandleError(*Session, error)
}

type NopHandler struct{}

type requestURLContextKey struct{}

func (NopHandler) HandleRequest(*Session) (*http.Request, *http.Response) {
	return nil, nil
}

func (NopHandler) HandleResponse(*Session) *http.Response {
	return nil
}

func (NopHandler) HandleError(*Session, error) {}

type Session struct {
	id       string
	request  *http.Request
	response *http.Response
	metadata *C.Metadata
	capture  *captureTransaction

	propsMutex sync.RWMutex
	props      map[string]any
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) TransactionID() string {
	return s.id
}

func (s *Session) Request() *http.Request {
	return s.request
}

func (s *Session) Response() *http.Response {
	return s.response
}

func (s *Session) Metadata() *C.Metadata {
	return s.metadata
}

func (s *Session) SetRequest(request *http.Request) {
	s.request = request
	requestURL := fullRequestURL(request)
	if s.metadata != nil {
		s.metadata.URL = requestURL
	}
	s.capture.setRequestURL(requestURL)
}

func (s *Session) SetResponse(response *http.Response) {
	s.response = response
}

func (s *Session) setConnectionID(id string) {
	if id == "" {
		return
	}
	if s.metadata != nil {
		s.metadata.UUID = id
	}
	s.capture.setConnectionID(id)
}

func (s *Session) recordResult(result F.Result) {
	s.recordActions(result.Actions)
}

func (s *Session) recordActions(actions []F.Action) {
	recorded := s.capture.recordActions(actions)
	for _, action := range recorded {
		logTransactionAction(s.id, action)
	}
}

func (s *Session) fail(stage string, source F.Source, err error) {
	s.capture.setError(stage, source, err)
	logTransactionFailure(s.id, stage, source, err)
}

func (s *Session) abort() {
	s.capture.setAborted()
}

func (s *Session) GetProperties(key string) (any, bool) {
	s.propsMutex.RLock()
	defer s.propsMutex.RUnlock()
	value, found := s.props[key]
	return value, found
}

func (s *Session) SetProperties(key string, value any) {
	s.propsMutex.Lock()
	defer s.propsMutex.Unlock()
	s.props[key] = value
}

func (s *Session) NewResponse(code int, body io.Reader) *http.Response {
	return NewResponse(code, body, s.request)
}

func (s *Session) NewErrorResponse(err error) *http.Response {
	response := NewResponse(http.StatusBadGateway, nil, s.request)
	response.Header.Set("Warning", fmt.Sprintf(`199 "mihomo" %q`, err.Error()))
	return response
}

func NewResponse(code int, body io.Reader, request *http.Request) *http.Response {
	var responseBody io.ReadCloser = http.NoBody
	contentLength := int64(0)
	if body != nil {
		if readCloser, ok := body.(io.ReadCloser); ok {
			responseBody = readCloser
		} else {
			responseBody = io.NopCloser(body)
		}
		contentLength = -1
	}

	response := &http.Response{
		StatusCode:    code,
		Status:        fmt.Sprintf("%d %s", code, http.StatusText(code)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          responseBody,
		ContentLength: contentLength,
		Request:       request,
	}
	if request != nil {
		response.Proto = request.Proto
		response.ProtoMajor = request.ProtoMajor
		response.ProtoMinor = request.ProtoMinor
		response.Close = request.Close
	}
	return response
}

func newSession(request *http.Request, metadata *C.Metadata) *Session {
	sessionMetadata := metadata.Clone()
	sessionMetadata.URL = fullRequestURL(request)
	transactionID := utils.NewUUIDV4().String()
	session := &Session{
		id:       transactionID,
		request:  request,
		metadata: sessionMetadata,
		props:    make(map[string]any),
	}
	session.capture = beginCaptureSession(request, sessionMetadata, transactionID)
	return session
}

// RequestURLFromContext returns the absolute URL associated with an intercepted HTTP dial.
func RequestURLFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	requestURL, _ := ctx.Value(requestURLContextKey{}).(string)
	return requestURL
}

func withRequestURL(ctx context.Context, requestURL string) context.Context {
	return context.WithValue(ctx, requestURLContextKey{}, requestURL)
}

func fullRequestURL(request *http.Request) string {
	if request == nil || request.URL == nil {
		return ""
	}
	requestURL := *request.URL
	if requestURL.Scheme == "" {
		requestURL.Scheme = "https"
	}
	if requestURL.Host == "" {
		requestURL.Host = request.Host
	}
	requestURL.Fragment = ""
	return requestURL.String()
}
