package main

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseOptions(t *testing.T) {
	var output bytes.Buffer
	config, err := parseOptions([]string{
		"-client=client", "-quorum=3/5", "-clients=2, 4", "-leases-per-client=16,32",
		"-duration=200ms", "-hold=5ms", "-ttl-ms=500",
		"-targets=127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053,127.0.0.1:50054,127.0.0.1:50055",
	}, &output)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if config.clientKind != "client" || config.quorum != "3/5" ||
		len(config.clientCounts) != 2 || config.clientCounts[0] != 2 || config.clientCounts[1] != 4 ||
		len(config.leaseCounts) != 2 || config.leaseCounts[0] != 16 || config.leaseCounts[1] != 32 ||
		len(config.targets) != 5 || config.settleDuration() != 1600*time.Millisecond {
		t.Fatalf("unexpected config: %+v", config)
	}
	scenarios, err := config.scenarios()
	if err != nil || len(scenarios) != 1 || scenarios[0].serverCount != 5 {
		t.Fatalf("scenarios = %+v, %v; want one 3/5 scenario", scenarios, err)
	}
}

func TestDefaultHold(t *testing.T) {
	config, err := parseOptions([]string{"-targets=127.0.0.1:50051"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if config.hold != 10*time.Millisecond {
		t.Fatalf("default hold = %s, want 10ms", config.hold)
	}
}

func TestParseOptionsRejectsInvalidValues(t *testing.T) {
	for _, args := range [][]string{
		{"-targets=127.0.0.1:50051,127.0.0.1:50051"},
		{"-targets=127.0.0.1:50051,"},
		{"-client=client", "-quorum=3/5", "-targets=127.0.0.1:50051"},
		{"-client=both", "-quorum=3/5"},
		{"-client=client1of1", "-quorum=2/3"},
		{"-clients=2,0"},
		{"-leases-per-client="},
		{"-ttl-ms=100"},
		{"-ttl-ms=5001"},
		{"-ttl-ms=200", "-hold=100ms"},
		{"-duration=0"},
		{"-response-timeout-ms=0"},
		{"-settle=-1s"},
		{"-clients=129", "-leases-per-client=256"},
	} {
		args = append([]string{"-targets=127.0.0.1:50051"}, args...)
		if _, err := parseOptions(args, &bytes.Buffer{}); err == nil {
			t.Errorf("parseOptions(%v) succeeded, want error", args)
		}
	}
}

func TestLatencyHistogram(t *testing.T) {
	var histogram latencyHistogram
	for _, duration := range []time.Duration{
		10 * time.Microsecond,
		20 * time.Microsecond,
		30 * time.Microsecond,
		40 * time.Microsecond,
	} {
		histogram.observe(duration)
	}
	if p50, p95, p99 := histogram.percentile(4, 50), histogram.percentile(4, 95), histogram.percentile(4, 99); p50 > p95 || p95 > p99 {
		t.Fatalf("percentiles are not ordered: p50=%v p95=%v p99=%v", p50, p95, p99)
	}
}

func TestAdaptersReturnNilLeaseOnFailedAcquire(t *testing.T) {
	config := options{clientKind: "both", quorum: quorum1of1, ttlMS: 500, responseTimeout: 500}
	scenarios, err := config.scenarios()
	if err != nil {
		t.Fatalf("scenarios: %v", err)
	}
	logger := slog.New(slog.DiscardHandler)
	for _, selected := range scenarios {
		t.Run(selected.name, func(t *testing.T) {
			clients, createErr := newLoadClients(selected, 1, []string{"127.0.0.1:1"}, &config, logger)
			if createErr != nil {
				t.Fatalf("create client: %v", createErr)
			}
			t.Cleanup(func() {
				if err := closeLoadClients(clients); err != nil {
					t.Errorf("close client: %v", err)
				}
			})
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			lease, acquireErr := clients[0].Acquire(cancelled, 1, config.ttlMS)
			if acquireErr == nil || lease != nil {
				t.Fatalf("Acquire = %v, %v; want nil lease and an error", lease, acquireErr)
			}
		})
	}
}

type fakeLoadLease struct{ releases *atomic.Uint64 }

func (l fakeLoadLease) RemainingTTLms() uint64 { return 1 }
func (l fakeLoadLease) Release()               { l.releases.Add(1) }

type fakeLoadClient struct{ releases *atomic.Uint64 }

func (c fakeLoadClient) WaitReady(context.Context) error { return nil }
func (c fakeLoadClient) Close() error                    { return nil }
func (c fakeLoadClient) Acquire(context.Context, uint64, uint64) (loadLease, error) {
	return fakeLoadLease(c), nil
}

func TestMeasureWithoutServer(t *testing.T) {
	var releases atomic.Uint64
	config := options{duration: 30 * time.Millisecond, hold: time.Millisecond, ttlMS: 500}
	clients := []loadClient{fakeLoadClient{releases: &releases}, fakeLoadClient{releases: &releases}}
	result := measure(t.Context(), clients, 2, &config, 12345)
	if result.succeeded == 0 || result.succeeded != releases.Load() || result.failed != 0 {
		t.Fatalf("measure = %+v, releases = %d", result, releases.Load())
	}
	if len(result.csvRecord(scenario{name: clientName}, quorum1of1, 2, 2)) != 17 {
		t.Fatal("CSV record has an unexpected number of columns")
	}
}

func TestRunRequiresTargetsBeforeWritingCSV(t *testing.T) {
	var output bytes.Buffer
	err := run(nil, &output, &bytes.Buffer{})
	if err == nil || output.Len() != 0 {
		t.Fatalf("run = %v, output = %q; want an error without CSV", err, output.String())
	}
}
