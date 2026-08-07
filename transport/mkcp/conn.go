package mkcp

import (
	"context"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var globalConv uint32 = uint32(uint16(rand.Int63() >> 47))

type connState int32

const (
	stateActive connState = iota
	stateReadyToClose
	statePeerClosed
	stateTerminating
	statePeerTerminating
	stateTerminated
)

type Conn struct {
	mu            sync.Mutex
	cfg           Config
	writer        packetWriter
	closer        io.Closer
	localAddr     net.Addr
	remoteAddr    net.Addr
	conv          uint16
	created       time.Time
	mss           int
	rto           uint32
	rttVariation  uint32
	srtt          uint32
	minRtt        uint32
	rtoUpdated    uint32
	readDeadline  time.Time
	writeDeadline time.Time

	state        connState
	stateBegin   uint32
	lastIncoming uint32
	lastPing     uint32

	readBuf       []byte
	recvCache     map[uint32]*dataSegment
	recvNext      uint32
	ackList       []ackItem
	ackDirty      bool
	sendWindow    []*dataSegment
	sendNext      uint32
	firstUnacked  uint32
	remoteRecvWin uint32

	totalInFlightSize      uint32
	controlWindow          uint32
	firstUnackedWasUpdated bool

	readNotify  chan struct{}
	writeNotify chan struct{}
	flushNotify chan struct{}
	done        chan struct{}
	once        sync.Once
}

type ackItem struct {
	number    uint32
	timestamp uint32
	nextFlush uint32
}

func Dial(ctx context.Context, raw net.Conn, cfg Config) (*Conn, error) {
	security, err := cfg.security()
	if err != nil {
		return nil, err
	}
	conv := uint16(atomic.AddUint32(&globalConv, 1))
	conn := newConn(raw.LocalAddr(), raw.RemoteAddr(), conv, packetWriter{security: security, header: cfg.packetHeader(), writer: raw}, raw, cfg)
	reader := packetReader{security: security, header: cfg.packetHeader()}
	go func() {
		cache := make(chan []byte, 1024)
		go func() {
			defer close(cache)
			buf := make([]byte, 64*1024)
			for {
				n, err := raw.Read(buf)
				if err != nil {
					return
				}
				payload := append([]byte(nil), buf[:n]...)
				select {
				case cache <- payload:
				default:
				}
			}
		}()

		for payload := range cache {
			conn.Input(reader.read(payload))
		}
	}()
	return conn, nil
}

func newConn(localAddr, remoteAddr net.Addr, conv uint16, writer packetWriter, closer io.Closer, cfg Config) *Conn {
	c := &Conn{
		cfg:           cfg,
		writer:        writer,
		closer:        closer,
		localAddr:     localAddr,
		remoteAddr:    remoteAddr,
		conv:          conv,
		created:       time.Now(),
		mss:           int(cfg.mtu()) - writer.overhead() - dataSegmentOverhead,
		rto:           100,
		minRtt:        cfg.tti(),
		state:         stateActive,
		recvCache:     make(map[uint32]*dataSegment),
		remoteRecvWin: 32,
		controlWindow: cfg.sendingInFlightSize(),
		readNotify:    make(chan struct{}),
		writeNotify:   make(chan struct{}),
		flushNotify:   make(chan struct{}, 1),
		done:          make(chan struct{}),
	}
	if c.mss < 576 {
		c.mss = 576
	}
	go c.flushLoop()
	return c
}

func (c *Conn) elapsed() uint32 {
	return uint32(time.Since(c.created) / time.Millisecond)
}

func (c *Conn) Read(b []byte) (int, error) {
	for {
		c.mu.Lock()
		if len(c.readBuf) > 0 {
			n := copy(b, c.readBuf)
			c.readBuf = c.readBuf[n:]
			c.mu.Unlock()
			c.wakeFlush()
			return n, nil
		}
		if c.state == stateReadyToClose || c.state == stateTerminating || c.state == stateTerminated {
			c.mu.Unlock()
			return 0, io.EOF
		}
		ch := c.readNotify
		deadline := c.readDeadline
		c.mu.Unlock()
		if err := waitNotify(ch, deadline); err != nil {
			return 0, err
		}
	}
}

func (c *Conn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	written := 0
	for written < len(b) {
		end := written + c.mss
		if end > len(b) {
			end = len(b)
		}
		payload := append([]byte(nil), b[written:end]...)
		for {
			c.mu.Lock()
			if c.state != stateActive {
				c.mu.Unlock()
				return written, io.ErrClosedPipe
			}
			if uint32(len(c.sendWindow)) <= c.cfg.sendingBufferSize() {
				seg := &dataSegment{number: c.sendNext, payload: payload}
				c.sendNext++
				c.sendWindow = append(c.sendWindow, seg)
				c.signalWriteLocked()
				c.mu.Unlock()
				c.wakeFlush()
				written = end
				break
			}
			ch := c.writeNotify
			deadline := c.writeDeadline
			c.mu.Unlock()
			if err := waitNotify(ch, deadline); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func waitNotify(ch <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		<-ch
		return nil
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return os.ErrDeadlineExceeded
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

func (c *Conn) Close() error {
	terminate := false
	c.mu.Lock()
	switch c.state {
	case stateReadyToClose, stateTerminating, stateTerminated:
		c.mu.Unlock()
		return net.ErrClosed
	case stateActive:
		c.setStateLocked(stateReadyToClose)
	case statePeerClosed:
		c.setStateLocked(stateTerminating)
	case statePeerTerminating:
		c.setStateLocked(stateTerminated)
		terminate = true
	}
	c.mu.Unlock()
	c.wakeFlush()
	if terminate {
		c.terminate()
	}
	return nil
}

func (c *Conn) terminate() {
	c.once.Do(func() {
		c.mu.Lock()
		c.setStateLocked(stateTerminated)
		c.signalReadLocked()
		c.signalWriteLocked()
		c.mu.Unlock()
		close(c.done)
		if c.closer != nil {
			_ = c.closer.Close()
		}
	})
}

func (c *Conn) LocalAddr() net.Addr  { return c.localAddr }
func (c *Conn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *Conn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	if c.state != stateActive {
		c.mu.Unlock()
		return net.ErrClosed
	}
	c.readDeadline = t
	c.signalReadLocked()
	c.mu.Unlock()
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	if c.state != stateActive {
		c.mu.Unlock()
		return net.ErrClosed
	}
	c.writeDeadline = t
	c.signalWriteLocked()
	c.mu.Unlock()
	return nil
}

func (c *Conn) Input(segments []segment) {
	if len(segments) == 0 {
		return
	}
	wakeFlush := false
	terminate := false
	c.mu.Lock()
	current := c.elapsed()
	defer func() {
		c.mu.Unlock()
		if wakeFlush {
			c.wakeFlush()
		}
		if terminate {
			c.terminate()
		}
	}()
	if c.state == stateTerminated {
		return
	}
	c.lastIncoming = current
	for _, seg := range segments {
		if seg.conversation() != c.conv {
			break
		}
		switch s := seg.(type) {
		case *dataSegment:
			c.handleOptionLocked(s.option)
			c.processSendingNextLocked(s.sendingNext)
			idx := s.number - c.recvNext
			if idx < c.cfg.receivingInFlightSize() {
				c.ackList = append(c.ackList, ackItem{number: s.number, timestamp: s.timestamp})
				c.ackDirty = true
				if _, ok := c.recvCache[s.number]; !ok {
					c.recvCache[s.number] = s
				}
			}
			c.deliverLocked()
			wakeFlush = true
		case *ackSegment:
			c.handleOptionLocked(s.option)
			if s.receivingWindow > c.remoteRecvWin {
				c.remoteRecvWin = s.receivingWindow
			}
			c.processReceivingNextLocked(s.receivingNext)
			if len(s.numberList) == 0 {
				continue
			}
			var maxack uint32
			maxackRemoved := false
			for _, number := range s.numberList {
				removed := c.processAckLocked(number)
				if maxack < number {
					maxack = number
					maxackRemoved = removed
				}
			}
			if maxackRemoved {
				c.handleFastAckLocked(maxack)
				if current-s.timestamp < 10000 {
					c.updateRTTLocked(current-s.timestamp, current)
				}
			}
			wakeFlush = true
		case *cmdOnlySegment:
			c.handleOptionLocked(s.option)
			if s.cmd == commandTerminate {
				switch c.state {
				case stateActive, statePeerClosed:
					c.setStateLocked(statePeerTerminating)
				case stateReadyToClose:
					c.setStateLocked(stateTerminating)
				case stateTerminating:
					c.setStateLocked(stateTerminated)
					terminate = true
				}
			}
			c.processReceivingNextLocked(s.receivingNext)
			c.processSendingNextLocked(s.sendingNext)
			c.updatePeerRTOLocked(s.peerRTO, current)
			if s.option == segmentOptionClose || s.cmd == commandTerminate {
				c.signalReadLocked()
				c.signalWriteLocked()
			}
		}
	}
}

func (c *Conn) handleOptionLocked(opt segmentOption) {
	if opt&segmentOptionClose == segmentOptionClose {
		switch c.state {
		case stateReadyToClose:
			c.setStateLocked(stateTerminating)
		case stateActive:
			c.setStateLocked(statePeerClosed)
		}
	}
}

func (c *Conn) deliverLocked() {
	delivered := false
	for {
		seg := c.recvCache[c.recvNext]
		if seg == nil {
			break
		}
		delete(c.recvCache, c.recvNext)
		c.recvNext++
		c.readBuf = append(c.readBuf, seg.payload...)
		delivered = true
	}
	if delivered {
		c.signalReadLocked()
	}
}

func (c *Conn) processSendingNextLocked(next uint32) {
	if len(c.ackList) > 0 {
		acks := c.ackList[:0]
		for _, ack := range c.ackList {
			if ack.number >= next {
				acks = append(acks, ack)
			}
		}
		if len(acks) != len(c.ackList) {
			c.ackDirty = true
		}
		c.ackList = acks
	}
}

func (c *Conn) processReceivingNextLocked(next uint32) {
	changed := false
	filtered := c.sendWindow[:0]
	for _, seg := range c.sendWindow {
		if seg.number < next {
			changed = true
			continue
		}
		filtered = append(filtered, seg)
	}
	c.sendWindow = filtered
	c.findFirstUnackedLocked()
	if changed {
		c.signalWriteLocked()
	}
}

func (c *Conn) processAckLocked(number uint32) bool {
	if number-c.firstUnacked > 0x7fffffff || number-c.sendNext < 0x7fffffff {
		return false
	}
	for i, seg := range c.sendWindow {
		if seg.number == number {
			if c.totalInFlightSize > 0 {
				c.totalInFlightSize--
			}
			copy(c.sendWindow[i:], c.sendWindow[i+1:])
			c.sendWindow[len(c.sendWindow)-1] = nil
			c.sendWindow = c.sendWindow[:len(c.sendWindow)-1]
			c.findFirstUnackedLocked()
			c.signalWriteLocked()
			return true
		}
	}
	return false
}

func (c *Conn) handleFastAckLocked(number uint32) {
	if len(c.sendWindow) == 0 {
		return
	}
	reduction := c.rto / 3
	if reduction == 0 {
		return
	}
	for _, seg := range c.sendWindow {
		if number == seg.number || number-seg.number > 0x7fffffff {
			return
		}
		if seg.transmit > 0 && seg.timeout > reduction {
			seg.timeout -= reduction
		}
	}
}

func (c *Conn) findFirstUnackedLocked() {
	first := c.firstUnacked
	if len(c.sendWindow) == 0 {
		c.firstUnacked = c.sendNext
	} else {
		c.firstUnacked = c.sendWindow[0].number
	}
	if first != c.firstUnacked {
		c.firstUnackedWasUpdated = true
	}
}

func (c *Conn) signalReadLocked() {
	close(c.readNotify)
	c.readNotify = make(chan struct{})
}

func (c *Conn) signalWriteLocked() {
	close(c.writeNotify)
	c.writeNotify = make(chan struct{})
}

func (c *Conn) wakeFlush() {
	select {
	case c.flushNotify <- struct{}{}:
	default:
	}
}

func (c *Conn) flushLoop() {
	ticker := time.NewTicker(time.Duration(c.cfg.tti()) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.flush()
		case <-c.flushNotify:
			c.flush()
		case <-c.done:
			return
		}
	}
}

func (c *Conn) flush() {
	var segments []segment
	var terminate bool

	c.mu.Lock()
	current := c.elapsed()
	if c.state == stateTerminated {
		c.mu.Unlock()
		return
	}

	if c.state == stateActive && current-c.lastIncoming >= 30000 {
		c.setStateLocked(stateReadyToClose)
	}
	if c.state == stateReadyToClose && len(c.sendWindow) == 0 {
		c.setStateLocked(stateTerminating)
	}
	if c.state == stateTerminating {
		segments = append(segments, c.pingSegmentLocked(commandTerminate, current))
		if current-c.stateBegin > 8000 {
			c.setStateLocked(stateTerminated)
			terminate = true
		}
		c.mu.Unlock()
		c.writeSegments(segments)
		if terminate {
			c.terminate()
		}
		return
	}
	if c.state == statePeerTerminating && current-c.stateBegin > 4000 {
		c.setStateLocked(stateTerminating)
	}
	if c.state == stateReadyToClose && current-c.stateBegin > 15000 {
		c.setStateLocked(stateTerminating)
	}

	segments = append(segments, c.flushAcksLocked(current)...)
	segments = append(segments, c.flushSendLocked(current)...)
	if current-c.lastPing >= 3000 {
		segments = append(segments, c.pingSegmentLocked(commandPing, current))
	}
	terminate = c.state == stateTerminated
	c.mu.Unlock()

	c.writeSegments(segments)
	if terminate {
		c.terminate()
	}
}

func (c *Conn) flushAcksLocked(current uint32) []segment {
	if len(c.ackList) == 0 {
		return nil
	}
	segments := make([]segment, 0, 1)
	candidates := make([]uint32, 0, 128)
	ack := c.newAckSegmentLocked()
	for i := range c.ackList {
		item := &c.ackList[i]
		if item.nextFlush > current {
			if len(candidates) < cap(candidates) {
				candidates = append(candidates, item.number)
			}
			continue
		}
		ack.numberList = append(ack.numberList, item.number)
		if item.timestamp-ack.timestamp < 0x7fffffff {
			ack.timestamp = item.timestamp
		}
		timeout := c.rto / 2
		if timeout < 20 {
			timeout = 20
		}
		item.nextFlush = current + timeout
		if len(ack.numberList) == 128 {
			segments = append(segments, ack)
			ack = c.newAckSegmentLocked()
			c.ackDirty = false
		}
	}
	if c.ackDirty || len(ack.numberList) > 0 {
		for _, number := range candidates {
			if len(ack.numberList) == 128 {
				break
			}
			ack.numberList = append(ack.numberList, number)
		}
		segments = append(segments, ack)
		c.ackDirty = false
	}
	return segments
}

func (c *Conn) flushSendLocked(current uint32) []segment {
	if len(c.sendWindow) == 0 {
		if c.firstUnackedWasUpdated {
			c.firstUnackedWasUpdated = false
			return []segment{c.pingSegmentLocked(commandPing, current)}
		}
		return nil
	}
	segments := make([]segment, 0, len(c.sendWindow))
	cwnd := c.cfg.sendingInFlightSize()
	if remoteWindow := c.remoteRecvWin - c.firstUnacked; cwnd > remoteWindow {
		cwnd = remoteWindow
	}
	if c.cfg.Congestion && cwnd > c.controlWindow {
		cwnd = c.controlWindow
	}
	cwnd *= 20

	var lost uint32
	var inFlight uint32
	for _, seg := range c.sendWindow {
		if current-seg.timeout >= 0x7fffffff {
			continue
		}
		if seg.transmit == 0 {
			c.totalInFlightSize++
		} else {
			lost++
		}
		seg.conv = c.conv
		seg.timestamp = current
		seg.sendingNext = c.firstUnacked
		seg.option = 0
		if c.state == stateReadyToClose {
			seg.option = segmentOptionClose
		}
		seg.transmit++
		timeout := c.rto
		if timeout < c.cfg.tti() {
			timeout = c.cfg.tti()
		}
		seg.timeout = current + timeout
		segments = append(segments, seg)
		inFlight++
		if inFlight >= cwnd {
			break
		}
	}
	if c.cfg.Congestion && inFlight > 0 && c.totalInFlightSize != 0 {
		rate := lost * 100 / c.totalInFlightSize
		c.onPacketLossLocked(rate)
	}
	c.firstUnackedWasUpdated = false
	return segments
}

func (c *Conn) writeSegments(segments []segment) {
	for _, seg := range segments {
		var err error
		for i := 0; i < 5; i++ {
			err = c.writer.writeSegment(seg)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_ = err
	}
}

func (c *Conn) newAckSegmentLocked() *ackSegment {
	ack := &ackSegment{
		conv:            c.conv,
		receivingNext:   c.recvNext,
		receivingWindow: c.recvNext + c.cfg.receivingInFlightSize(),
	}
	if c.state == stateReadyToClose {
		ack.option = segmentOptionClose
	}
	return ack
}

func (c *Conn) pingSegmentLocked(cmd command, current uint32) *cmdOnlySegment {
	seg := &cmdOnlySegment{
		conv:          c.conv,
		cmd:           cmd,
		receivingNext: c.recvNext,
		sendingNext:   c.firstUnacked,
		peerRTO:       c.rto,
	}
	if c.state == stateReadyToClose {
		seg.option = segmentOptionClose
	}
	c.lastPing = current
	return seg
}

func (c *Conn) setStateLocked(state connState) {
	if c.state == state {
		return
	}
	c.state = state
	c.stateBegin = c.elapsed()
	switch state {
	case stateReadyToClose:
		c.signalReadLocked()
		c.signalWriteLocked()
	case statePeerClosed:
		c.sendWindow = nil
		c.signalWriteLocked()
	case stateTerminating:
		c.sendWindow = nil
		c.signalReadLocked()
		c.signalWriteLocked()
	case statePeerTerminating:
		c.sendWindow = nil
		c.signalReadLocked()
		c.signalWriteLocked()
	case stateTerminated:
		c.sendWindow = nil
		c.signalReadLocked()
		c.signalWriteLocked()
	}
}

func (c *Conn) updateRTTLocked(rtt uint32, current uint32) {
	if rtt > 0x7fffffff {
		return
	}
	if c.srtt == 0 {
		c.srtt = rtt
		c.rttVariation = rtt / 2
	} else {
		delta := rtt - c.srtt
		if c.srtt > rtt {
			delta = c.srtt - rtt
		}
		c.rttVariation = (3*c.rttVariation + delta) / 4
		c.srtt = (7*c.srtt + rtt) / 8
		if c.srtt < c.minRtt {
			c.srtt = c.minRtt
		}
	}
	var rto uint32
	if c.minRtt < 4*c.rttVariation {
		rto = c.srtt + 4*c.rttVariation
	} else {
		rto = c.srtt + c.rttVariation
	}
	if rto > 10000 {
		rto = 10000
	}
	c.rto = rto * 5 / 4
	c.rtoUpdated = current
}

func (c *Conn) updatePeerRTOLocked(rto uint32, current uint32) {
	if rto == 0 || current-c.rtoUpdated < 3000 {
		return
	}
	c.rtoUpdated = current
	c.rto = rto
}

func (c *Conn) onPacketLossLocked(lossRate uint32) {
	if c.rto == 0 {
		return
	}
	if lossRate >= 15 {
		c.controlWindow = 3 * c.controlWindow / 4
	} else if lossRate <= 5 {
		c.controlWindow += c.controlWindow / 4
	}
	if c.controlWindow < 16 {
		c.controlWindow = 16
	}
	max := 2 * c.cfg.sendingInFlightSize()
	if c.controlWindow > max {
		c.controlWindow = max
	}
}
