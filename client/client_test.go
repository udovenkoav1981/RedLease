package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClientWaitReadyRequiresThreeStreams(t *testing.T) {
	client, factories := newClientWithScriptedReplicas(t)
	result := make(chan error, 1)
	go func() { result <- client.WaitReady(context.Background()) }()

	for index := range 2 {
		factories[index].results <- streamFactoryResult{stream: newReplicaFakeStream()}
	}
	select {
	case err := <-result:
		t.Fatalf("WaitReady returned with two streams: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	factories[2].results <- streamFactoryResult{stream: newReplicaFakeStream()}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady did not return after third stream")
	}
}

func TestClientWaitReadyHonorsContext(t *testing.T) {
	client, _ := newClientWithScriptedReplicas(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := client.WaitReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady error = %v, want context cancellation", err)
	}
}

func TestClientCloseUnblocksWaitReady(t *testing.T) {
	client, _ := newClientWithScriptedReplicasWithoutCleanup()
	result := make(chan error, 1)
	go func() { result <- client.WaitReady(context.Background()) }()

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("WaitReady error = %v, want ErrClientClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock WaitReady")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func newClientWithScriptedReplicas(t *testing.T) (*Client, [ServerCount]*scriptedStreamFactory) {
	t.Helper()
	client, factories := newClientWithScriptedReplicasWithoutCleanup()
	t.Cleanup(func() { _ = client.Close() })
	return client, factories
}

func newClientWithScriptedReplicasWithoutCleanup() (*Client, [ServerCount]*scriptedStreamFactory) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{ctx: ctx, cancel: cancel}
	var factories [ServerCount]*scriptedStreamFactory
	for index := range client.replicas {
		factory := newScriptedStreamFactory()
		factories[index] = factory
		client.replicas[index] = newReplicaConn(factory)
	}
	return client, factories
}
