package transport

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestWriteFrameTimesOutBlockedTCPWrite(t *testing.T) {
	t.Parallel()
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	err := writeFrame(client, []byte{1}, 10*time.Millisecond)
	if err == nil {
		t.Fatal("blocked frame write succeeded")
	}
	var networkError net.Error
	if !errors.As(err, &networkError) || !networkError.Timeout() {
		t.Fatalf("blocked frame write error = %v, want network timeout", err)
	}
}
