package server

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
)

func TestMaxHandshakePacketSizeRefusesAnOversizedHandshakeResponse(t *testing.T) {
	s := NewDefaultServer()
	s.SetMaxHandshakePacketSize(4096)

	peer, local := net.Pipe()
	defer peer.Close()
	defer local.Close()

	done := make(chan error, 1)
	go func() {
		_, err := s.NewCustomizedConn(local, NewInMemoryAuthenticationHandler(), &EmptyHandler{})
		done <- err
	}()

	// Read the greeting, then answer with a header announcing a 16 MB packet and nothing else.
	client := packet.NewConn(peer)
	_, err := client.ReadPacket()
	require.NoError(t, err)
	_, err = peer.Write([]byte{0xff, 0xff, 0xff, 0x01})
	require.NoError(t, err)

	// The server tells the client why it is refused, and net.Pipe has no buffer: take it.
	go func() { _, _ = io.Copy(io.Discard, peer) }()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "exceeds the limit of 4096 bytes")
	case <-time.After(5 * time.Second):
		t.Fatal("the server is still waiting for the payload of an oversized handshake response")
	}
}

func TestMaxHandshakePacketSizeEndsWithTheConnectionPhase(t *testing.T) {
	s := NewDefaultServer()
	s.SetMaxHandshakePacketSize(4096)
	authHandler := NewInMemoryAuthenticationHandler()
	require.NoError(t, authHandler.AddUser("user", "password"))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		c, err := s.NewCustomizedConn(raw, authHandler, &EmptyHandler{})
		if err != nil {
			return
		}
		for c.HandleCommand() == nil {
		}
	}()

	conn, err := client.Connect(listener.Addr().String(), "user", "password", "")
	require.NoError(t, err, "a normal login is far below the limit")
	defer conn.Close()

	// Well over the handshake limit. EmptyHandler refuses the query, which is an answer: the connection is still good.
	for range 2 {
		_, err = conn.Execute("SELECT '" + string(make([]byte, 64<<10)) + "'")
		var myError *mysql.MyError
		require.ErrorAs(t, err, &myError, "the limit applied to a command, not only to the connection phase")
	}
}
