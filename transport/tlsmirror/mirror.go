package tlsmirror

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

type messageHook func(*record) (drop bool, err error)

type writeTask struct {
	rec      *record
	raw      []byte
	fallback *bufio.Reader
}

type mirrorConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	clientConn net.Conn
	serverConn net.Conn

	onC2SMessage   messageHook
	onS2CMessage   messageHook
	onC2SMessageTx messageHook
	onS2CMessageTx messageHook

	c2sInsert chan writeTask
	s2cInsert chan writeTask
	c2sReady  chan struct{}
	s2cReady  chan struct{}
	onClose   func()

	randomMu          sync.RWMutex
	clientRandom      [32]byte
	serverRandom      [32]byte
	clientRandomReady bool
	serverRandomReady bool
	c2sReadyOnce      sync.Once
	s2cReadyOnce      sync.Once
	tls12Explicit     bool
	explicitReady     chan struct{}
	explicitSuites    map[uint16]struct{}
	c2sExplicitNonce  explicitNonceGenerator
	s2cExplicitNonce  explicitNonceGenerator
}

func newMirrorConn(ctx context.Context, clientConn, serverConn net.Conn, cfg Config, onC2S, onS2C, onC2STx, onS2CTx messageHook) *mirrorConn {
	mctx, cancel := context.WithCancel(ctx)
	explicitSuites := make(map[uint16]struct{}, len(cfg.ExplicitNonceCipherSuites))
	for _, suite := range cfg.ExplicitNonceCipherSuites {
		explicitSuites[suite] = struct{}{}
	}
	return &mirrorConn{
		ctx:            mctx,
		cancel:         cancel,
		clientConn:     clientConn,
		serverConn:     serverConn,
		onC2SMessage:   onC2S,
		onS2CMessage:   onS2C,
		onC2SMessageTx: onC2STx,
		onS2CMessageTx: onS2CTx,
		c2sInsert:      make(chan writeTask, 100),
		s2cInsert:      make(chan writeTask, 100),
		c2sReady:       make(chan struct{}),
		s2cReady:       make(chan struct{}),
		explicitReady:  make(chan struct{}),
		explicitSuites: explicitSuites,
	}
}

func (m *mirrorConn) start() {
	go m.c2sWorker()
	go m.s2cWorker()
	go func() {
		<-m.ctx.Done()
		if m.onClose != nil {
			m.onClose()
		}
		_ = m.clientConn.Close()
		_ = m.serverConn.Close()
	}()
}

func (m *mirrorConn) Close() error {
	m.cancel()
	return nil
}

func (m *mirrorConn) handshakeRandom() ([32]byte, [32]byte, error) {
	m.randomMu.RLock()
	defer m.randomMu.RUnlock()
	if !m.clientRandomReady || !m.serverRandomReady {
		return [32]byte{}, [32]byte{}, errors.New("tlsmirror: handshake random is not ready")
	}
	return m.clientRandom, m.serverRandom, nil
}

func (m *mirrorConn) explicitNonceOverhead() int {
	select {
	case <-m.explicitReady:
	case <-m.ctx.Done():
		return 0
	}
	if m.tls12Explicit {
		return 8
	}
	return 0
}

func (m *mirrorConn) waitC2SReady(ctx context.Context) error {
	select {
	case <-m.c2sReady:
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mirrorConn) waitS2CReady(ctx context.Context) error {
	select {
	case <-m.s2cReady:
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mirrorConn) InsertC2S(rec *record) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.c2sInsert <- writeTask{rec: duplicateRecord(rec)}:
		return nil
	}
}

func (m *mirrorConn) InsertS2C(rec *record) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.s2cInsert <- writeTask{rec: duplicateRecord(rec)}:
		return nil
	}
}

func (m *mirrorConn) c2sWorker() {
	serverWriter := bufio.NewWriterSize(m.serverConn, 65536)

	first, clientReader, firstRaw, err := m.captureFirstHandshakeRecord(m.clientConn, serverWriter)
	if err != nil {
		return
	}
	clientRandom, err := parseClientRandom(first.fragment)
	if err != nil {
		m.fallbackDirectCopy(serverWriter, clientReader, firstRaw)
		return
	}
	m.randomMu.Lock()
	m.clientRandom = clientRandom
	m.clientRandomReady = true
	m.randomMu.Unlock()
	if err := writeRawFlush(serverWriter, firstRaw); err != nil {
		m.cancel()
		return
	}

	go m.recordWriter(serverWriter, m.c2sInsert, m.onC2SMessageTx, true)
	explicitNonceSessionAndChangeCipherSpecWasLastMessage := false
	for m.ctx.Err() == nil {
		rec, raw, err := readRecord(clientReader)
		if err != nil {
			m.fallbackQueuedCopy(m.c2sInsert, clientReader, nil, raw)
			return
		}
		if rec.recordType == recordTypeHandshake && explicitNonceSessionAndChangeCipherSpecWasLastMessage && !hasZeroExplicitNonce(rec.fragment) {
			m.fallbackQueuedCopy(m.c2sInsert, clientReader, rec, nil)
			return
		}
		if rec.recordType == recordTypeChangeCipherSpec {
			select {
			case <-m.explicitReady:
			default:
				m.fallbackQueuedCopy(m.c2sInsert, clientReader, rec, nil)
				return
			}
		}
		if m.onC2SMessage != nil {
			drop, err := m.onC2SMessage(rec)
			if err != nil {
				m.fallbackQueuedCopy(m.c2sInsert, clientReader, rec, nil)
				return
			}
			if drop {
				continue
			}
		}
		if err := m.InsertC2S(rec); err != nil {
			m.cancel()
			return
		}
		if rec.recordType == recordTypeChangeCipherSpec && m.tls12Explicit {
			explicitNonceSessionAndChangeCipherSpecWasLastMessage = true
			continue
		}
		if rec.recordType == recordTypeApplicationData || rec.recordType == recordTypeHandshake && explicitNonceSessionAndChangeCipherSpecWasLastMessage {
			m.c2sReadyOnce.Do(func() {
				close(m.c2sReady)
			})
		}
		explicitNonceSessionAndChangeCipherSpecWasLastMessage = false
	}
}

func (m *mirrorConn) s2cWorker() {
	clientWriter := bufio.NewWriterSize(m.clientConn, 65536)

	first, serverReader, firstRaw, err := m.captureFirstHandshakeRecord(m.serverConn, clientWriter)
	if err != nil {
		return
	}
	serverRandom, cipherSuite, err := parseServerHello(first.fragment)
	if err != nil {
		m.fallbackDirectCopy(clientWriter, serverReader, firstRaw)
		return
	}
	m.randomMu.Lock()
	m.serverRandom = serverRandom
	m.serverRandomReady = true
	_, m.tls12Explicit = m.explicitSuites[cipherSuite]
	m.randomMu.Unlock()
	close(m.explicitReady)
	if m.onS2CMessage != nil {
		drop, err := m.onS2CMessage(first)
		if err != nil {
			m.fallbackDirectCopy(clientWriter, serverReader, firstRaw)
			return
		}
		_ = drop
	}
	if err := writeRawFlush(clientWriter, firstRaw); err != nil {
		m.cancel()
		return
	}

	go m.recordWriter(clientWriter, m.s2cInsert, m.onS2CMessageTx, false)
	explicitNonceSessionAndChangeCipherSpecWasLastMessage := false
	for m.ctx.Err() == nil {
		rec, raw, err := readRecord(serverReader)
		if err != nil {
			m.fallbackQueuedCopy(m.s2cInsert, serverReader, nil, raw)
			return
		}
		if rec.recordType == recordTypeHandshake && explicitNonceSessionAndChangeCipherSpecWasLastMessage && !hasZeroExplicitNonce(rec.fragment) {
			m.fallbackQueuedCopy(m.s2cInsert, serverReader, rec, nil)
			return
		}
		if m.onS2CMessage != nil {
			drop, err := m.onS2CMessage(rec)
			if err != nil {
				m.fallbackQueuedCopy(m.s2cInsert, serverReader, rec, nil)
				return
			}
			if drop {
				continue
			}
		}
		if err := m.InsertS2C(rec); err != nil {
			m.cancel()
			return
		}
		if rec.recordType == recordTypeChangeCipherSpec && m.tls12Explicit {
			explicitNonceSessionAndChangeCipherSpecWasLastMessage = true
			continue
		}
		if rec.recordType == recordTypeApplicationData || rec.recordType == recordTypeHandshake && explicitNonceSessionAndChangeCipherSpecWasLastMessage {
			m.s2cReadyOnce.Do(func() {
				close(m.s2cReady)
			})
		}
		explicitNonceSessionAndChangeCipherSpecWasLastMessage = false
	}
}

func (m *mirrorConn) fallbackDirectCopy(writer *bufio.Writer, src *bufio.Reader, raw []byte) {
	if err := writeRawFlush(writer, raw); err != nil {
		m.cancel()
		return
	}
	_ = copyFlush(writer, src)
	m.cancel()
}

func (m *mirrorConn) captureFirstHandshakeRecord(src net.Conn, dst *bufio.Writer) (*record, *bufio.Reader, []byte, error) {
	var readBuffer [65536]byte
	var copied int
	for m.ctx.Err() == nil {
		n, err := src.Read(readBuffer[copied:])
		if err != nil {
			m.cancel()
			return nil, nil, nil, err
		}
		buffer := readBuffer[:copied+n]
		rec, needMore, processed, err := peekFirstHandshakeRecord(buffer)
		if processed == 0 {
			if needMore == 0 {
				_, _ = dst.Write(buffer)
				_ = dst.Flush()
				_ = copyFlush(dst, src)
				m.cancel()
				return nil, nil, nil, err
			}
			if _, err := dst.Write(readBuffer[copied : copied+n]); err != nil {
				m.cancel()
				return nil, nil, nil, err
			}
			if err := dst.Flush(); err != nil {
				m.cancel()
				return nil, nil, nil, err
			}
			copied += n
			continue
		}
		raw := append([]byte(nil), readBuffer[copied:processed]...)
		rest := append([]byte(nil), buffer[processed:]...)
		return rec, bufio.NewReaderSize(io.MultiReader(bytes.NewReader(rest), src), 65536), raw, nil
	}
	return nil, nil, nil, m.ctx.Err()
}

func peekFirstHandshakeRecord(buffer []byte) (*record, int, int, error) {
	if len(buffer) < 5 {
		return nil, 5, 0, nil
	}
	if buffer[0] != recordTypeHandshake {
		return nil, 0, 0, errors.New("tlsmirror: unexpected first tls record type")
	}
	switch buffer[1] {
	case 0x01, 0x02:
	case 0x03:
		if buffer[2] > 0x03 {
			return nil, 0, 0, errors.New("tlsmirror: unexpected first tls record version")
		}
	default:
		return nil, 0, 0, errors.New("tlsmirror: unexpected first tls record version")
	}
	length := int(buffer[3])<<8 | int(buffer[4])
	if length > maxTLSRecordPayload {
		return nil, 0, 0, errors.New("tlsmirror: tls record is too large")
	}
	processed := 5 + length
	if len(buffer) < processed {
		return nil, processed, 0, nil
	}
	return &record{
		recordType: buffer[0],
		version:    [2]byte{buffer[1], buffer[2]},
		fragment:   append([]byte(nil), buffer[5:processed]...),
	}, 0, processed, nil
}

func (m *mirrorConn) fallbackQueuedCopy(ch chan<- writeTask, src *bufio.Reader, first *record, raw []byte) {
	var rec *record
	if first != nil {
		rec = duplicateRecord(first)
	}
	raw = append([]byte(nil), raw...)
	select {
	case <-m.ctx.Done():
	case ch <- writeTask{rec: rec, raw: raw, fallback: src}:
	}
}

func copyFlush(writer *bufio.Writer, src io.Reader) error {
	buf := make([]byte, 32*1024)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := writer.Write(buf[:n]); err != nil {
				return err
			}
			if err := writer.Flush(); err != nil {
				return err
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return nil
			}
			return readErr
		}
	}
}

func writeRawFlush(writer *bufio.Writer, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	if _, err := writer.Write(raw); err != nil {
		return err
	}
	return writer.Flush()
}

func (m *mirrorConn) recordWriter(writer *bufio.Writer, ch <-chan writeTask, hook messageHook, c2s bool) {
	for m.ctx.Err() == nil {
		select {
		case <-m.ctx.Done():
			return
		case task := <-ch:
			rec := task.rec
			if rec != nil {
				m.fillExplicitNonce(rec, c2s)
				if hook != nil {
					drop, err := hook(rec)
					if err != nil {
						m.cancel()
						return
					}
					if drop {
						continue
					}
				}
				if err := writeRecord(writer, rec); err != nil {
					m.cancel()
					return
				}
				if rec.recordType == recordTypeAlert {
					m.cancel()
					return
				}
			}
			if len(task.raw) > 0 {
				if _, err := writer.Write(task.raw); err != nil {
					m.cancel()
					return
				}
				if err := writer.Flush(); err != nil {
					m.cancel()
					return
				}
			}
			if task.fallback != nil {
				_ = copyFlush(writer, task.fallback)
				m.cancel()
				return
			}
		}
	}
}

func (m *mirrorConn) fillExplicitNonce(rec *record, c2s bool) {
	if !rec.inserted || len(rec.fragment) < 8 {
		return
	}
	select {
	case <-m.explicitReady:
	default:
		return
	}
	if !m.tls12Explicit {
		return
	}
	if rec.recordType != recordTypeApplicationData && rec.recordType != recordTypeAlert {
		return
	}
	// recordWriter owns each direction's insert queue, so the generators do not
	// need additional synchronization here.
	nonce := m.s2cExplicitNonce.Next()
	if c2s {
		nonce = m.c2sExplicitNonce.Next()
	}
	copy(rec.fragment[:8], nonce)
}
