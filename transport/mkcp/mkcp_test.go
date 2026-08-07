package mkcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	ln, err := Listen(ctx, pc, Config{})
	require.NoError(t, err)
	defer ln.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		_, err = io.Copy(conn, conn)
		serverErr <- err
	}()

	raw, err := net.Dial("udp", ln.Addr().String())
	require.NoError(t, err)
	conn, err := Dial(ctx, raw, Config{})
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	payload := bytes.Repeat([]byte("x"), 64*1024)
	_, err = conn.Write(payload)
	require.NoError(t, err)
	got := make([]byte, len(payload))
	_, err = io.ReadFull(conn, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestFullDuplex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	ln, err := Listen(ctx, pc, Config{})
	require.NoError(t, err)
	defer ln.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
		payload := bytes.Repeat([]byte("s"), 128*1024)
		readErr := make(chan error, 1)
		go func() {
			got := make([]byte, 128*1024)
			_, err := io.ReadFull(conn, got)
			if err == nil {
				err = requirePayload(got, 'c')
			}
			readErr <- err
		}()
		_, err = conn.Write(payload)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- <-readErr
	}()

	raw, err := net.Dial("udp", ln.Addr().String())
	require.NoError(t, err)
	conn, err := Dial(ctx, raw, Config{})
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	readErr := make(chan error, 1)
	go func() {
		got := make([]byte, 128*1024)
		_, err := io.ReadFull(conn, got)
		if err == nil {
			err = requirePayload(got, 's')
		}
		readErr <- err
	}()
	_, err = conn.Write(bytes.Repeat([]byte("c"), 128*1024))
	require.NoError(t, err)
	require.NoError(t, <-readErr)
	require.NoError(t, <-serverErr)
}

func TestRoundTripHeaders(t *testing.T) {
	for _, header := range []string{"srtp", "utp", "wechat-video", "dtls", "wireguard"} {
		t.Run(header, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			pc, err := net.ListenPacket("udp", "127.0.0.1:0")
			require.NoError(t, err)
			ln, err := Listen(ctx, pc, Config{Header: header})
			require.NoError(t, err)
			defer ln.Close()

			serverErr := make(chan error, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					serverErr <- err
					return
				}
				defer conn.Close()
				require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
				_, err = io.Copy(conn, conn)
				serverErr <- err
			}()

			raw, err := net.Dial("udp", ln.Addr().String())
			require.NoError(t, err)
			conn, err := Dial(ctx, raw, Config{Header: header})
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

			payload := bytes.Repeat([]byte("h"), 32*1024)
			_, err = conn.Write(payload)
			require.NoError(t, err)
			got := make([]byte, len(payload))
			_, err = io.ReadFull(conn, got)
			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}

func requirePayload(payload []byte, want byte) error {
	for i, got := range payload {
		if got != want {
			return fmt.Errorf("unexpected byte at %d: got %q, want %q", i, got, want)
		}
	}
	return nil
}
