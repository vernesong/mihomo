package xhttp

import (
	"errors"
	"io"
	"sync"
)

type Packet struct {
	Seq     uint64
	Payload []byte
	Reader  io.ReadCloser
}

type uploadQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	packets map[uint64][]byte
	nextSeq uint64
	buf     []byte
	closed  bool
	reader  io.ReadCloser
}

func NewUploadQueue() *uploadQueue {
	q := &uploadQueue{
		packets: make(map[uint64][]byte),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *uploadQueue) Push(p Packet) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return io.ErrClosedPipe
	}

	if q.reader != nil {
		return errors.New("uploadQueue.reader already exists")
	}

	if p.Reader != nil {
		q.reader = p.Reader
		q.cond.Broadcast()
		return nil
	}

	cp := make([]byte, len(p.Payload))
	copy(cp, p.Payload)
	q.packets[p.Seq] = cp
	q.cond.Broadcast()
	return nil
}

func (q *uploadQueue) Read(b []byte) (int, error) {
	q.mu.Lock()

	for {
		if len(q.buf) > 0 {
			n := copy(b, q.buf)
			q.buf = q.buf[n:]
			q.mu.Unlock()
			return n, nil
		}

		if payload, ok := q.packets[q.nextSeq]; ok {
			delete(q.packets, q.nextSeq)
			q.nextSeq++
			q.buf = payload
			continue
		}

		if reader := q.reader; reader != nil {
			q.mu.Unlock() // unlock before calling q.reader.Read
			return reader.Read(b)
		}

		if q.closed {
			q.mu.Unlock()
			return 0, io.EOF
		}

		q.cond.Wait()
	}
}

func (q *uploadQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var err error
	if q.reader != nil {
		err = q.reader.Close()
	}
	q.closed = true
	q.cond.Broadcast()
	return err
}
