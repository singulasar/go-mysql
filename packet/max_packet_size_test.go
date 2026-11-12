package packet

import (
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// readPacketAsync reads one packet in the background, so a read that blocks (waiting for payload that never comes) fails the
// test instead of hanging it.
func readPacketAsync(c *Conn) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := c.ReadPacket()
		done <- err
	}()
	return done
}

func waitForRead(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("ReadPacket is still waiting; the packet should have been refused from its header alone")
		return nil
	}
}

func TestMaxPacketSizeRefusesTheAnnouncedLengthBeforeReadingOrAllocatingAnything(t *testing.T) {
	peer, local := net.Pipe()
	defer peer.Close()
	defer local.Close()
	c := NewConn(local)
	c.SetMaxPacketSize(1024)

	// A header announcing a 16 MB packet, and nothing else.
	go func() { _, _ = peer.Write([]byte{0xff, 0xff, 0xff, 0x00}) }()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := waitForRead(t, readPacketAsync(c))
	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, mysql.ErrBadConn)
	require.ErrorContains(t, err, "exceeds the limit of 1024 bytes")
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4<<20), "a buffer for the announced length was allocated")
}

func TestMaxPacketSizeAllowsPacketsUpToTheLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		size    int
		limit   int
		allowed bool
	}{
		{"empty packet", 0, 10, true},
		{"just under", 9, 10, true},
		{"exactly the limit", 10, 10, true},
		{"one over", 11, 10, false},
		{"no limit by default", 1 << 20, 0, true},
		{"a negative limit is no limit", 1 << 20, -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, local := net.Pipe()
			defer peer.Close()
			defer local.Close()
			c := NewConn(local)
			c.SetMaxPacketSize(tc.limit)

			go func() {
				payload := make([]byte, tc.size)
				_, _ = peer.Write(append([]byte{byte(tc.size), byte(tc.size >> 8), byte(tc.size >> 16), 0}, payload...))
			}()

			err := waitForRead(t, readPacketAsync(c))
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, mysql.ErrBadConn)
			}
		})
	}
}

func TestMaxPacketSizeLimitsAPacketSplitIntoFragmentsAsAWhole(t *testing.T) {
	// A packet of 16 MB or more arrives as fragments of exactly MaxPayloadLen, ending with a shorter one. Each fragment alone is
	// within the limit; together they are not.
	for _, tc := range []struct {
		name     string
		lastSize int
		allowed  bool
	}{
		{"the whole packet is within the limit", 50, true},
		{"the whole packet is over the limit", 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, local := net.Pipe()
			defer peer.Close()
			defer local.Close()
			c := NewConn(local)
			c.SetMaxPacketSize(mysql.MaxPayloadLen + 100)

			go func() {
				first := append([]byte{0xff, 0xff, 0xff, 0x00}, make([]byte, mysql.MaxPayloadLen)...)
				if _, err := peer.Write(first); err != nil {
					return
				}
				second := append([]byte{byte(tc.lastSize), 0, 0, 0x01}, make([]byte, tc.lastSize)...)
				_, _ = peer.Write(second)
			}()

			err := waitForRead(t, readPacketAsync(c))
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, mysql.ErrBadConn)
				require.ErrorContains(t, err, "exceeds the limit")
			}
		})
	}
}

func TestMaxPacketSizeCanBeLiftedAgain(t *testing.T) {
	peer, local := net.Pipe()
	defer peer.Close()
	defer local.Close()
	c := NewConn(local)
	c.SetMaxPacketSize(10)
	c.SetMaxPacketSize(0)

	go func() {
		_, _ = peer.Write(append([]byte{100, 0, 0, 0}, make([]byte, 100)...))
	}()
	require.NoError(t, waitForRead(t, readPacketAsync(c)))
}
