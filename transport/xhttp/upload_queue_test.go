package xhttp

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUploadQueueMaxPackets(t *testing.T) {
	q := NewUploadQueue(2)
	ch := make(chan struct{})
	go func() {
		err := q.Push(Packet{Seq: 0, Payload: []byte{'0'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 1, Payload: []byte{'1'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 2, Payload: []byte{'2'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 4, Payload: []byte{'4'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 5, Payload: []byte{'5'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 6, Payload: []byte{'6'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 7, Payload: []byte{'7'}})
		assert.ErrorIs(t, err, io.ErrClosedPipe)
		close(ch)
	}()

	buf := make([]byte, 20)
	n, err := q.Read(buf)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte{'0'}, buf[:n])
	assert.NoError(t, err)

	n, err = q.Read(buf)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte{'1'}, buf[:n])

	n, err = q.Read(buf)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte{'2'}, buf[:n])

	n, err = q.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrQueueTooLarge)

	err = q.Close()
	assert.NoError(t, err)

	<-ch
}
