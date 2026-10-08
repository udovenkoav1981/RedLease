package client1of1forvalkey

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestValkeyEndToEnd(t *testing.T) {
	target := os.Getenv("REDLEASE_VALKEY_TEST_TARGET")
	if target == "" {
		t.Skip("REDLEASE_VALKEY_TEST_TARGET is not set")
	}
	client, err := New(Config{
		ClientID:        1,
		Target:          target,
		Logger:          testLogger,
		ResponseTimeout: 1000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := client.WaitReady(readyCtx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	key := uint64(time.Now().UnixNano())
	lease, err := client.Acquire(t.Context(), key, 1000)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.RemainingTTLms() == 0 {
		t.Fatal("Acquire returned an expired lease")
	}
	if _, err := client.Acquire(t.Context(), key, 1000); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("contending Acquire error = %v, want ErrNotAcquired", err)
	}
	if err := lease.Renew(t.Context(), 1500); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	lease.Release()
	lease.Release()
}

func TestValkeyScriptsReloadAfterFlush(t *testing.T) {
	target := os.Getenv("REDLEASE_VALKEY_TEST_TARGET")
	if target == "" {
		t.Skip("REDLEASE_VALKEY_TEST_TARGET is not set")
	}
	client, err := New(Config{
		ClientID:        2,
		Target:          target,
		Logger:          testLogger,
		ResponseTimeout: 1000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := client.WaitReady(readyCtx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	key := uint64(time.Now().UnixNano())
	lease, err := client.Acquire(t.Context(), key, 3000)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := client.redis.ScriptFlush(t.Context()).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if err := lease.Renew(t.Context(), 3000); !errors.Is(err, ErrNotRenewed) {
		t.Fatalf("first Renew after SCRIPT FLUSH = %v, want ErrNotRenewed", err)
	}
	if err := lease.Renew(t.Context(), 3000); err != nil {
		t.Fatalf("second Renew after SCRIPT FLUSH: %v", err)
	}
	lease.Release()
}
