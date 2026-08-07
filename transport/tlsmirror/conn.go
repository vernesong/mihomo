package tlsmirror

import (
	"bytes"
	"context"
	"crypto/cipher"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/net/deadline"
)

type Conn struct {
	ctx    context.Context
	cancel context.CancelFunc

	mirror *mirrorConn

	primaryKey []byte
	isServer   bool
	config     Config

	mu              sync.Mutex
	readMu          sync.Mutex
	writeMu         sync.Mutex
	encryptor       *encryptor
	decryptor       *decryptor
	protocolVersion [2]byte
	firstWrite      bool
	firstWriteDelay time.Duration
	watermarkTx     cipher.Stream
	watermarkRx     cipher.Stream

	readCh                 chan []byte
	readBuffer             *bytes.Buffer
	recallTrafficGenerator func()
	enrollmentMu           sync.Mutex
	enrollmentRemove       func()
	enrollmentRemoved      bool
	readDeadline           deadline.PipeDeadline
	writeDeadline          deadline.PipeDeadline
}

func newHiddenConn(ctx context.Context, mirror *mirrorConn, primaryKey []byte, isServer bool, cfg Config) (*Conn, error) {
	cctx, cancel := context.WithCancel(ctx)
	firstWriteDelay, err := cfg.DeferInstanceDerivedWrite.Duration()
	if err != nil {
		cancel()
		return nil, err
	}
	return &Conn{
		ctx:             cctx,
		cancel:          cancel,
		mirror:          mirror,
		primaryKey:      primaryKey,
		isServer:        isServer,
		config:          cfg,
		firstWrite:      true,
		firstWriteDelay: firstWriteDelay,
		readCh:          make(chan []byte, 32),
		readDeadline:    deadline.MakePipeDeadline(),
		writeDeadline:   deadline.MakePipeDeadline(),
	}, nil
}

func (c *Conn) ensureCryptoLocked(version [2]byte) error {
	if c.encryptor != nil && c.decryptor != nil {
		return nil
	}
	clientRandom, serverRandom, err := c.mirror.handshakeRandom()
	if err != nil {
		return err
	}

	encryptTag := ":c2s"
	decryptTag := ":s2c"
	if c.isServer {
		encryptTag = ":s2c"
		decryptTag = ":c2s"
	}

	encKey, encMask, err := deriveEncryptionKey(c.primaryKey, clientRandom, serverRandom, encryptTag)
	if err != nil {
		return err
	}
	decKey, decMask, err := deriveEncryptionKey(c.primaryKey, clientRandom, serverRandom, decryptTag)
	if err != nil {
		return err
	}
	c.encryptor, err = newEncryptor(encKey, encMask)
	if err != nil {
		return err
	}
	c.decryptor, err = newDecryptor(decKey, decMask)
	if err != nil {
		return err
	}
	c.protocolVersion = version
	if c.protocolVersion == [2]byte{} {
		c.protocolVersion = [2]byte{0x03, 0x03}
	}
	return nil
}

func (c *Conn) handleInboundRecord(rec *record) (bool, error) {
	if err := c.applySequenceWatermarkRx(rec); err != nil {
		return false, err
	}
	if rec.recordType != recordTypeApplicationData {
		return false, nil
	}
	c.mu.Lock()
	err := c.ensureCryptoLocked(rec.version)
	decryptor := c.decryptor
	c.mu.Unlock()
	if err != nil {
		return false, nil
	}
	overhead := c.mirror.explicitNonceOverhead()
	if len(rec.fragment) < overhead+decryptor.NonceSize() {
		return false, nil
	}
	payload, err := decryptor.Open(nil, rec.fragment[overhead:])
	if err != nil {
		return false, nil
	}
	c.initSequenceWatermarkRx()
	if c.config.TransportLayerPadding.Enabled {
		payload, _ = unpackPadding(payload)
		if payload == nil {
			return true, nil
		}
	}
	select {
	case <-c.ctx.Done():
		return true, c.ctx.Err()
	case c.readCh <- payload:
		return true, nil
	}
}

func (c *Conn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		if c.readBuffer != nil {
			n, _ := c.readBuffer.Read(b)
			if n > 0 {
				return n, nil
			}
			c.readBuffer = nil
		}
		select {
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		case <-c.mirror.ctx.Done():
			return 0, c.mirror.ctx.Err()
		case <-c.readDeadline.Wait():
			return 0, os.ErrDeadlineExceeded
		case data := <-c.readCh:
			c.readBuffer = bytes.NewBuffer(data)
		}
	}
}

func (c *Conn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	writeDeadline := c.writeDeadline.Wait()
	if err := c.waitWriteReady(writeDeadline); err != nil {
		return 0, err
	}
	if c.firstWrite {
		c.firstWrite = false
		if c.firstWriteDelay > 0 {
			timer := time.NewTimer(c.firstWriteDelay)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return 0, c.ctx.Err()
			case <-c.mirror.ctx.Done():
				timer.Stop()
				return 0, c.mirror.ctx.Err()
			case <-writeDeadline:
				timer.Stop()
				return 0, os.ErrDeadlineExceeded
			case <-timer.C:
			}
		}
	}

	payloadSize := len(b)
	c.mu.Lock()
	if err := c.ensureCryptoLocked(c.protocolVersion); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	encryptor := c.encryptor
	version := c.protocolVersion
	c.mu.Unlock()

	overhead := c.mirror.explicitNonceOverhead()
	maxPlaintext := maxTLSRecordPayload - overhead - encryptor.Overhead()
	if c.config.TransportLayerPadding.Enabled {
		maxPlaintext -= 4
	}
	if maxPlaintext <= 0 {
		return 0, errors.New("tlsmirror: invalid tls record overhead")
	}
	for written := 0; written < len(b); {
		end := written + maxPlaintext
		if end > len(b) {
			end = len(b)
		}
		plain := b[written:end]
		if c.config.TransportLayerPadding.Enabled {
			plain = packPadding(append([]byte(nil), plain...), 0)
		}
		fragment := make([]byte, overhead, overhead+len(plain)+encryptor.Overhead())
		fragment = encryptor.Seal(fragment, plain)
		rec := &record{
			recordType: recordTypeApplicationData,
			version:    version,
			fragment:   fragment,
			inserted:   true,
		}
		var err error
		if c.isServer {
			err = c.insertS2C(rec, writeDeadline)
		} else {
			err = c.insertC2S(rec, writeDeadline)
		}
		if err != nil {
			return written, err
		}
		written = end
	}
	return payloadSize, nil
}

func (c *Conn) waitWriteReady(deadline <-chan struct{}) error {
	ready := c.mirror.c2sReady
	if c.isServer {
		ready = c.mirror.s2cReady
	}
	select {
	case <-ready:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.mirror.ctx.Done():
		return c.mirror.ctx.Err()
	case <-deadline:
		return os.ErrDeadlineExceeded
	}
}

func (c *Conn) insertC2S(rec *record, deadline <-chan struct{}) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.mirror.ctx.Done():
		return c.mirror.ctx.Err()
	case <-deadline:
		return os.ErrDeadlineExceeded
	case c.mirror.c2sInsert <- writeTask{rec: duplicateRecord(rec)}:
		return nil
	}
}

func (c *Conn) insertS2C(rec *record, deadline <-chan struct{}) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.mirror.ctx.Done():
		return c.mirror.ctx.Err()
	case <-deadline:
		return os.ErrDeadlineExceeded
	case c.mirror.s2cInsert <- writeTask{rec: duplicateRecord(rec)}:
		return nil
	}
}

func (c *Conn) Close() error {
	if c.recallTrafficGenerator != nil {
		c.recallTrafficGenerator()
	}
	c.removeEnrollment()
	c.cancel()
	return c.mirror.Close()
}

func (c *Conn) removeEnrollment() {
	c.enrollmentMu.Lock()
	if c.enrollmentRemoved {
		c.enrollmentMu.Unlock()
		return
	}
	c.enrollmentRemoved = true
	remove := c.enrollmentRemove
	c.enrollmentRemove = nil
	c.enrollmentMu.Unlock()
	if remove != nil {
		remove()
	}
}

func (c *Conn) setEnrollmentRemove(remove func()) {
	c.enrollmentMu.Lock()
	if c.enrollmentRemoved {
		c.enrollmentMu.Unlock()
		if remove != nil {
			remove()
		}
		return
	}
	c.enrollmentRemove = remove
	c.enrollmentMu.Unlock()
}

func (c *Conn) addrConn() net.Conn {
	if c.isServer {
		return c.mirror.clientConn
	}
	return c.mirror.serverConn
}

func (c *Conn) LocalAddr() net.Addr {
	return c.addrConn().LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.addrConn().RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.Set(t)
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.Set(t)
	return nil
}

var _ net.Conn = (*Conn)(nil)

var errCarrierHandshake = errors.New("tlsmirror: carrier handshake failed")

func (c *Conn) applySequenceWatermarkRx(rec *record) error {
	if !c.config.SequenceWatermarkingEnabled || c.watermarkRx == nil {
		return nil
	}
	if rec.recordType != recordTypeApplicationData && rec.recordType != recordTypeAlert {
		return nil
	}
	if len(rec.fragment) < 16 {
		return nil
	}
	watermarkRegion := rec.fragment[len(rec.fragment)-16:]
	c.watermarkRx.XORKeyStream(watermarkRegion, watermarkRegion)
	return nil
}

func (c *Conn) handleOutboundRecordTx(rec *record) (bool, error) {
	if !c.config.SequenceWatermarkingEnabled {
		return false, nil
	}
	if c.watermarkTx != nil {
		if (rec.recordType == recordTypeApplicationData || rec.recordType == recordTypeAlert) && len(rec.fragment) >= 16 {
			watermarkRegion := rec.fragment[len(rec.fragment)-16:]
			c.watermarkTx.XORKeyStream(watermarkRegion, watermarkRegion)
		}
	}
	if rec.inserted && c.watermarkTx == nil {
		if err := c.initSequenceWatermarkTx(); err != nil {
			return true, nil
		}
	}
	return false, nil
}

func (c *Conn) initSequenceWatermarkTx() error {
	clientRandom, serverRandom, err := c.mirror.handshakeRandom()
	if err != nil {
		return err
	}
	tag := ":c2s"
	if c.isServer {
		tag = ":s2c"
	}
	c.watermarkTx, err = newSequenceWatermark(c.primaryKey, clientRandom, serverRandom, tag)
	return err
}

func (c *Conn) initSequenceWatermarkRx() {
	if !c.config.SequenceWatermarkingEnabled || c.watermarkRx != nil {
		return
	}
	clientRandom, serverRandom, err := c.mirror.handshakeRandom()
	if err != nil {
		return
	}
	tag := ":s2c"
	if c.isServer {
		tag = ":c2s"
	}
	c.watermarkRx, _ = newSequenceWatermark(c.primaryKey, clientRandom, serverRandom, tag)
}
