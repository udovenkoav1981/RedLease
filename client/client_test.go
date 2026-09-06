package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClientWaitReadyRequiresConfiguredQuorum(t *testing.T) {
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

func TestClientWaitReadyUsesEverySupportedQuorum(t *testing.T) {
	tests := []struct {
		name       string
		quorum     Quorum
		quorumSize int
	}{
		{name: "one of one", quorum: Quorum1Of1, quorumSize: 1},
		{name: "two of three", quorum: Quorum2Of3, quorumSize: 2},
		{name: "three of five", quorum: Quorum3Of5, quorumSize: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, factories := newClientForQuorum(test.quorum)
			t.Cleanup(func() { _ = client.Close() })

			result := make(chan error, 1)
			go func() { result <- client.WaitReady(context.Background()) }()

			for index := range test.quorumSize - 1 {
				factories[index].results <- streamFactoryResult{stream: newReplicaFakeStream()}
			}
			select {
			case err := <-result:
				t.Fatalf("WaitReady returned below quorum: %v", err)
			case <-time.After(20 * time.Millisecond):
			}

			factories[test.quorumSize-1].results <- streamFactoryResult{stream: newReplicaFakeStream()}
			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("WaitReady: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("WaitReady did not return at quorum")
			}
		})
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

func newClientWithScriptedReplicas(t *testing.T) (*Client, [testServerCount]*scriptedStreamFactory) {
	t.Helper()
	client, factories := newClientWithScriptedReplicasWithoutCleanup()
	t.Cleanup(func() { _ = client.Close() })
	return client, factories
}

func newClientWithScriptedReplicasWithoutCleanup() (*Client, [testServerCount]*scriptedStreamFactory) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		quorum:   testQuorum,
		replicas: make([]*replicaConn, testServerCount),
		ctx:      ctx,
		cancel:   cancel,
	}
	var factories [testServerCount]*scriptedStreamFactory
	for index := range client.replicas {
		factory := newScriptedStreamFactory()
		factories[index] = factory
		client.replicas[index] = newReplicaConn(factory)
	}
	return client, factories
}

func newClientForQuorum(quorum Quorum) (*Client, []*scriptedStreamFactory) {
	serverCount, _, _ := quorum.parameters()
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		quorum:   quorum,
		replicas: make([]*replicaConn, serverCount),
		ctx:      ctx,
		cancel:   cancel,
	}
	factories := make([]*scriptedStreamFactory, serverCount)
	for index := range client.replicas {
		factory := newScriptedStreamFactory()
		factories[index] = factory
		client.replicas[index] = newReplicaConn(factory)
	}
	return client, factories
}
