package tlsmirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/metacubex/http"

	"golang.org/x/crypto/hkdf"
)

const enrollmentControlConnectionPostfix = ".tlsmirror-controlconnection.v2fly.arpa"

var enrollmentBase32 = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv").WithPadding(base32.NoPadding)

type connectionLoopbackPrevention struct {
	Key string
}

type enrollmentBypassContextKey struct{}

type enrollmentConfirmationReq struct {
	serverIdentifier []byte
	clientRandom     []byte
	serverRandom     []byte
	clientIdentifier []byte
	replyAddressTag  []byte
}

func WithConnectionEnrollmentBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, enrollmentBypassContextKey{}, true)
}

func isConnectionEnrollmentBypassed(ctx context.Context) bool {
	bypassed, _ := ctx.Value(enrollmentBypassContextKey{}).(bool)
	return bypassed
}

func WithLoopbackProtection(ctx context.Context, enrollmentID []byte) context.Context {
	return context.WithValue(ctx, connectionLoopbackPrevention{Key: string(enrollmentID)}, true)
}

func WithSecondaryLoopbackProtection(ctx context.Context, enrollmentID []byte) context.Context {
	return context.WithValue(ctx, connectionLoopbackPrevention{Key: string(enrollmentID)}, false)
}

func IsLoopbackProtectionEnabled(ctx context.Context, enrollmentID []byte) bool {
	enabled, ok := ctx.Value(connectionLoopbackPrevention{Key: string(enrollmentID)}).(bool)
	return ok && enabled
}

func ServerIdentifierHost(primaryKey []byte) (string, error) {
	serverID, err := deriveEnrollmentServerIdentifier(primaryKey)
	if err != nil {
		return "", err
	}
	return enrollmentBase32.EncodeToString(serverID) + enrollmentControlConnectionPostfix, nil
}

func ServeEnrollmentControlConnection(ctx context.Context, conn net.Conn, primaryKey string) error {
	key, err := DecodePrimaryKey(primaryKey)
	if err != nil {
		_ = conn.Close()
		return err
	}
	processor := enrollmentProcessorFor(key)
	listener := newSingleConnListener(conn)
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read request body: "+err.Error(), http.StatusInternalServerError)
				return
			}
			req, err := unmarshalEnrollmentConfirmationReq(body)
			if err != nil {
				http.Error(w, "failed to unmarshal request: "+err.Error(), http.StatusBadRequest)
				return
			}
			enrolled, err := processor.verify(req)
			if err != nil {
				http.Error(w, "failed to verify connection enrollment: "+err.Error(), http.StatusInternalServerError)
				return
			}
			responseBody := marshalEnrollmentConfirmationResp(enrolled)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(responseBody)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(responseBody)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}),
		Protocols: new(http.Protocols),
	}
	server.Protocols.SetHTTP2(true)
	server.Protocols.SetUnencryptedHTTP2(true)
	go func() {
		<-ctx.Done()
		_ = server.Close()
		_ = listener.Close()
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (c *Conn) verifyConnectionEnrollment(ctx context.Context, cfg ClientConfig) error {
	if cfg.EnrollmentDialer == nil {
		return errors.New("tlsmirror: connection enrolment requires an enrollment dialer")
	}
	clientRandom, serverRandom, err := c.mirror.handshakeRandom()
	if err != nil {
		return err
	}
	serverID, err := deriveEnrollmentServerIdentifier(c.primaryKey)
	if err != nil {
		return err
	}
	host := enrollmentBase32.EncodeToString(serverID) + enrollmentControlConnectionPostfix
	requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	controlCtx := WithLoopbackProtection(requestCtx, serverID)
	if cfg.ConnectionEnrolment.PrimaryEgressOutbound == "" {
		controlCtx = WithSecondaryLoopbackProtection(requestCtx, serverID)
	}
	controlCtx = WithConnectionEnrollmentBypass(controlCtx)
	controlConn, err := cfg.EnrollmentDialer(controlCtx, "tcp", net.JoinHostPort(host, "80"))
	if err != nil {
		return err
	}
	defer controlConn.Close()

	reqBody := marshalEnrollmentConfirmationReq(enrollmentConfirmationReq{
		serverIdentifier: serverID,
		clientRandom:     clientRandom[:],
		serverRandom:     serverRandom[:],
	})
	transport, err := newTrafficHTTPTransport(requestCtx, controlConn, "h2")
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(requestCtx, http.MethodPost, "http://"+host, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	httpResp, err := transport.RoundTrip(httpReq)
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}
	if err != nil {
		return err
	}
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("tlsmirror: unexpected enrollment response status %d", httpResp.StatusCode)
	}
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	enrolled, err := unmarshalEnrollmentConfirmationResp(respBody)
	if err != nil {
		return err
	}
	if !enrolled {
		return errors.New("tlsmirror: connection enrollment failed")
	}
	return nil
}

func deriveEnrollmentServerIdentifier(primaryKey []byte) ([]byte, error) {
	return deriveSecondaryKey(primaryKey, ":connection-enrollment-server-identifier-av38NNGF-TJvRw7C3-p8KM8yKd")
}

func deriveSecondaryKey(primaryKey []byte, tag string) ([]byte, error) {
	if len(primaryKey) != 32 {
		return nil, errors.New("tlsmirror: invalid primary key size")
	}
	key := make([]byte, 16)
	if _, err := io.ReadFull(hkdf.Expand(sha256.New, primaryKey, []byte("v2ray-sv77RCEY-e8AhYsbD-BmFC7XRK:tlsmirror-secondary"+tag)), key); err != nil {
		return nil, err
	}
	return key, nil
}

func deriveEnrollmentRequestKey(primaryKey []byte, clientRandom, serverRandom [32]byte) ([]byte, error) {
	requestKey, _, err := deriveEncryptionKey(primaryKey, clientRandom, serverRandom, ":connection-enrollment-re78HQNM-CmpRnPbr-PNJVRMhu")
	return requestKey, err
}

var enrollmentProcessors sync.Map

type enrollmentProcessor struct {
	primaryKey []byte
	active     sync.Map
}

func enrollmentProcessorFor(primaryKey []byte) *enrollmentProcessor {
	key := string(primaryKey)
	if value, ok := enrollmentProcessors.Load(key); ok {
		return value.(*enrollmentProcessor)
	}
	processor := &enrollmentProcessor{primaryKey: append([]byte(nil), primaryKey...)}
	value, _ := enrollmentProcessors.LoadOrStore(key, processor)
	return value.(*enrollmentProcessor)
}

func (p *enrollmentProcessor) add(clientRandom, serverRandom [32]byte, conn *Conn) (func(), error) {
	requestKey, err := deriveEnrollmentRequestKey(p.primaryKey, clientRandom, serverRandom)
	if err != nil {
		return nil, err
	}
	if _, loaded := p.active.LoadOrStore(string(requestKey), conn); loaded {
		return nil, errors.New("tlsmirror: enrollment connection already exists")
	}
	return func() {
		p.active.Delete(string(requestKey))
	}, nil
}

func (p *enrollmentProcessor) verify(req enrollmentConfirmationReq) (bool, error) {
	if len(req.clientRandom) != 32 || len(req.serverRandom) != 32 {
		return false, errors.New("tlsmirror: enrollment request is missing handshake random")
	}
	var clientRandom, serverRandom [32]byte
	copy(clientRandom[:], req.clientRandom)
	copy(serverRandom[:], req.serverRandom)
	requestKey, err := deriveEnrollmentRequestKey(p.primaryKey, clientRandom, serverRandom)
	if err != nil {
		return false, err
	}
	_, ok := p.active.Load(string(requestKey))
	return ok, nil
}

func marshalEnrollmentConfirmationReq(req enrollmentConfirmationReq) []byte {
	var out []byte
	out = appendProtoBytes(out, 1, req.serverIdentifier)
	out = appendProtoBytes(out, 2, req.clientRandom)
	out = appendProtoBytes(out, 3, req.serverRandom)
	out = appendProtoBytes(out, 4, req.clientIdentifier)
	out = appendProtoBytes(out, 5, req.replyAddressTag)
	return out
}

func unmarshalEnrollmentConfirmationReq(data []byte) (enrollmentConfirmationReq, error) {
	var req enrollmentConfirmationReq
	for len(data) > 0 {
		key, n, err := consumeProtoVarint(data)
		if err != nil {
			return req, err
		}
		data = data[n:]
		field := int(key >> 3)
		wireType := key & 0x7
		if field == 0 {
			return req, errors.New("tlsmirror: invalid protobuf field number")
		}
		if wireType == 2 {
			size, n, err := consumeProtoVarint(data)
			if err != nil {
				return req, err
			}
			data = data[n:]
			if uint64(len(data)) < size {
				return req, io.ErrUnexpectedEOF
			}
			value := append([]byte(nil), data[:size]...)
			data = data[size:]
			switch field {
			case 1:
				req.serverIdentifier = value
			case 2:
				req.clientRandom = value
			case 3:
				req.serverRandom = value
			case 4:
				req.clientIdentifier = value
			case 5:
				req.replyAddressTag = value
			}
			continue
		}
		n, err = skipProtoValue(data, field, wireType)
		if err != nil {
			return req, err
		}
		data = data[n:]
	}
	return req, nil
}

func marshalEnrollmentConfirmationResp(enrolled bool) []byte {
	if !enrolled {
		return nil
	}
	out := appendProtoVarint(nil, 1<<3)
	return appendProtoVarint(out, 1)
}

func unmarshalEnrollmentConfirmationResp(data []byte) (bool, error) {
	var enrolled bool
	for len(data) > 0 {
		key, n, err := consumeProtoVarint(data)
		if err != nil {
			return false, err
		}
		data = data[n:]
		field := int(key >> 3)
		wireType := key & 0x7
		if field == 0 {
			return false, errors.New("tlsmirror: invalid protobuf field number")
		}
		if field == 1 && wireType == 0 {
			value, n, err := consumeProtoVarint(data)
			if err != nil {
				return false, err
			}
			data = data[n:]
			enrolled = value != 0
			continue
		}
		n, err = skipProtoValue(data, field, wireType)
		if err != nil {
			return false, err
		}
		data = data[n:]
	}
	return enrolled, nil
}

func appendProtoBytes(out []byte, field int, value []byte) []byte {
	if len(value) == 0 {
		return out
	}
	out = appendProtoVarint(out, uint64(field<<3|2))
	out = appendProtoVarint(out, uint64(len(value)))
	return append(out, value...)
}

func appendProtoVarint(out []byte, value uint64) []byte {
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

func consumeProtoVarint(data []byte) (uint64, int, error) {
	var value uint64
	for i, b := range data {
		if i == 10 {
			return 0, 0, errors.New("tlsmirror: invalid protobuf varint")
		}
		value |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return value, i + 1, nil
		}
	}
	return 0, 0, io.ErrUnexpectedEOF
}

func skipProtoValue(data []byte, startField int, wireType uint64) (int, error) {
	switch wireType {
	case 0:
		_, n, err := consumeProtoVarint(data)
		return n, err
	case 1:
		if len(data) < 8 {
			return 0, io.ErrUnexpectedEOF
		}
		return 8, nil
	case 2:
		size, n, err := consumeProtoVarint(data)
		if err != nil {
			return 0, err
		}
		data = data[n:]
		if uint64(len(data)) < size {
			return 0, io.ErrUnexpectedEOF
		}
		return n + int(size), nil
	case 3:
		consumed := 0
		for {
			key, n, err := consumeProtoVarint(data)
			if err != nil {
				return 0, err
			}
			data = data[n:]
			consumed += n
			nestedField := int(key >> 3)
			nestedWireType := key & 0x7
			if nestedField == 0 {
				return 0, errors.New("tlsmirror: invalid protobuf field number")
			}
			if nestedWireType == 4 {
				if nestedField != startField {
					return 0, errors.New("tlsmirror: mismatched protobuf end group")
				}
				return consumed, nil
			}
			n, err = skipProtoValue(data, nestedField, nestedWireType)
			if err != nil {
				return 0, err
			}
			data = data[n:]
			consumed += n
		}
	case 4:
		return 0, errors.New("tlsmirror: unexpected protobuf end group")
	case 5:
		if len(data) < 4 {
			return 0, io.ErrUnexpectedEOF
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("tlsmirror: unsupported enrollment protobuf wire type %d", wireType)
	}
}

type singleConnListener struct {
	mu   sync.Mutex
	conn net.Conn
	done chan struct{}
	once sync.Once
}

func newSingleConnListener(conn net.Conn) *singleConnListener {
	return &singleConnListener{
		conn: conn,
		done: make(chan struct{}),
	}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.conn != nil {
		conn := l.conn
		l.conn = nil
		l.mu.Unlock()
		return &notifyCloseConn{Conn: conn, closeNotify: l.Close}, nil
	}
	l.mu.Unlock()
	<-l.done
	return nil, net.ErrClosed
}

func (l *singleConnListener) Close() error {
	l.once.Do(func() {
		close(l.done)
	})
	return nil
}

func (l *singleConnListener) Addr() net.Addr {
	l.mu.Lock()
	conn := l.conn
	l.mu.Unlock()
	if conn != nil {
		return conn.LocalAddr()
	}
	return &net.TCPAddr{}
}

type notifyCloseConn struct {
	net.Conn
	closeNotify func() error
}

func (c *notifyCloseConn) Close() error {
	_ = c.closeNotify()
	return c.Conn.Close()
}
