package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	clientprometheus "github.com/prometheus/client_golang/prometheus"

	"github.com/udovenkoav1981/RedLease/server"
)

var testLogger = slog.New(slog.DiscardHandler)

func TestParseFlagsDefaults(t *testing.T) {
	config, err := parseFlags(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if config.listenAddress != defaultListenAddress {
		t.Errorf("listen address = %q, want %q", config.listenAddress, defaultListenAddress)
	}
	if config.metricsListenAddress != "" {
		t.Errorf("metrics listen address = %q, want disabled", config.metricsListenAddress)
	}
	if config.configuredMaxTTLMS != defaultConfiguredMaxTTL {
		t.Errorf("configured max TTL = %d, want %d", config.configuredMaxTTLMS, defaultConfiguredMaxTTL)
	}
	if config.maxKeys != server.DefaultMaxKeys {
		t.Errorf("max keys = %d, want %d", config.maxKeys, server.DefaultMaxKeys)
	}
	if config.shardCount != 0 {
		t.Errorf("shard count = %d, want 0", config.shardCount)
	}
	if config.etcdEndpoint != "" || config.etcdPrefix != "" || config.etcdAdvertiseAddress != "" {
		t.Errorf("etcd mode is enabled by default: %+v", config)
	}
}

func TestParseFlagsCustomValues(t *testing.T) {
	config, err := parseFlags([]string{
		"-listen", "127.0.0.1:15051",
		"-metrics-listen", "127.0.0.1:19090",
		"-configured-max-ttl-ms", "1234",
		"-max-keys", "123",
		"-shard-count", "8",
		"-etcd-endpoint", "http://127.0.0.1:2379",
		"-etcd-prefix", "test",
		"-etcd-advertise", "192.0.2.10:17379",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if config.listenAddress != "127.0.0.1:15051" ||
		config.metricsListenAddress != "127.0.0.1:19090" ||
		config.configuredMaxTTLMS != 1234 ||
		config.maxKeys != 123 ||
		config.shardCount != 8 ||
		config.etcdEndpoint != "http://127.0.0.1:2379" ||
		config.etcdPrefix != "test" ||
		config.etcdAdvertiseAddress != "192.0.2.10:17379" {
		t.Fatalf("unexpected config: %+v", config)
	}
}

func TestParseFlagsHelpDescribesLocalPlaintextLauncher(t *testing.T) {
	var output bytes.Buffer
	_, err := parseFlags([]string{"-help"}, &output)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseFlags help error = %v, want flag.ErrHelp", err)
	}
	for _, fragment := range []string{
		"Local test launcher",
		"plaintext TCP",
		"no TLS or authentication",
		"metrics-listen",
		"etcd-endpoint",
		"etcd-prefix",
		"etcd-advertise",
	} {
		if !strings.Contains(output.String(), fragment) {
			t.Errorf("help does not contain %q:\n%s", fragment, output.String())
		}
	}
}

func TestLauncherEtcdConfig(t *testing.T) {
	tests := []struct {
		name          string
		config        launcherConfig
		wantEnabled   bool
		wantAdvertise string
		wantErr       string
	}{
		{
			name:   "disabled",
			config: launcherConfig{},
		},
		{
			name: "listen address is advertised by default",
			config: launcherConfig{
				listenAddress: "192.0.2.10:17379",
				etcdEndpoint:  "http://127.0.0.1:2379",
				etcdPrefix:    "cluster-a",
			},
			wantEnabled:   true,
			wantAdvertise: "192.0.2.10:17379",
		},
		{
			name: "explicit advertise address",
			config: launcherConfig{
				listenAddress:        "0.0.0.0:17379",
				etcdEndpoint:         "http://127.0.0.1:2379",
				etcdPrefix:           "cluster-a",
				etcdAdvertiseAddress: "192.0.2.11:17379",
			},
			wantEnabled:   true,
			wantAdvertise: "192.0.2.11:17379",
		},
		{
			name: "prefix without endpoint",
			config: launcherConfig{
				etcdPrefix: "cluster-a",
			},
			wantErr: "-etcd-endpoint is required",
		},
		{
			name: "missing prefix",
			config: launcherConfig{
				listenAddress: "192.0.2.10:17379",
				etcdEndpoint:  "http://127.0.0.1:2379",
			},
			wantErr: "-etcd-prefix is required",
		},
		{
			name: "advertise hostname is rejected",
			config: launcherConfig{
				listenAddress: "redlease.example:17379",
				etcdEndpoint:  "http://127.0.0.1:2379",
				etcdPrefix:    "cluster-a",
			},
			wantErr: "is not an IP address",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := test.config.etcdConfig()
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("etcdConfig error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("etcdConfig: %v", err)
			}
			if config.enabled() != test.wantEnabled {
				t.Fatalf("etcdConfig enabled = %t, want %t", config.enabled(), test.wantEnabled)
			}
			if config.advertiseAddress != test.wantAdvertise {
				t.Fatalf("etcdConfig = %+v, want advertise %q", config, test.wantAdvertise)
			}
		})
	}
}

func TestEtcdSlotKeys(t *testing.T) {
	want := [etcdServerSlotCount]string{
		"cluster-a-redlease-server-1",
		"cluster-a-redlease-server-2",
		"cluster-a-redlease-server-3",
		"cluster-a-redlease-server-4",
		"cluster-a-redlease-server-5",
	}
	if got := etcdSlotKeys("cluster-a"); got != want {
		t.Fatalf("etcd slot keys = %v, want %v", got, want)
	}
}

func TestMetricsHandlerServesPrivateRegistry(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	leaseServer, err := server.New(listener, server.Config{
		MaxTTL:     1000,
		MaxKeys:    10,
		Logger:     testLogger,
		ShardCount: 1,
	})
	if err != nil {
		_ = listener.Close()
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() {
		if err := leaseServer.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})

	handler, err := newMetricsHandler(leaseServer)
	if err != nil {
		t.Fatalf("newMetricsHandler: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", http.NoBody)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", response.Code, http.StatusOK)
	}
	for _, metric := range []string{
		"redlease_server_state",
		"redlease_server_resident_keys",
		"redlease_server_acquires_total",
		"go_goroutines",
		"go_memstats_heap_alloc_bytes",
		"process_cpu_seconds_total",
		"process_resident_memory_bytes",
	} {
		if !bytes.Contains(response.Body.Bytes(), []byte(metric)) {
			t.Errorf("GET /metrics does not contain %q", metric)
		}
	}
}

func TestMetricsEndpointSwitchesServerCollector(t *testing.T) {
	registry, err := newMetricsRegistry()
	if err != nil {
		t.Fatalf("newMetricsRegistry: %v", err)
	}
	endpoint := &metricsEndpoint{registry: registry}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	leaseServer, err := server.New(listener, server.Config{
		MaxTTL:     1000,
		MaxKeys:    10,
		Logger:     testLogger,
		ShardCount: 1,
	})
	if err != nil {
		_ = listener.Close()
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() { _ = leaseServer.Close() })

	if err := endpoint.setServer(leaseServer); err != nil {
		t.Fatalf("setServer: %v", err)
	}
	if !registryContainsMetric(t, registry, "redlease_server_state") {
		t.Fatal("registry does not contain attached RedLease collector")
	}
	if err := endpoint.setServer(nil); err != nil {
		t.Fatalf("detach server: %v", err)
	}
	if registryContainsMetric(t, registry, "redlease_server_state") {
		t.Fatal("registry still contains detached RedLease collector")
	}
	if !registryContainsMetric(t, registry, "go_goroutines") {
		t.Fatal("detaching RedLease collector removed standard collectors")
	}
}

func registryContainsMetric(
	t *testing.T,
	registry *clientprometheus.Registry,
	want string,
) bool {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == want {
			return true
		}
	}
	return false
}

func TestServerConfigValidatesMaxTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttlMS   uint64
		wantTTL uint64
		wantErr bool
	}{
		{name: "one millisecond", ttlMS: 1, wantTTL: 1},
		{name: "protocol maximum", ttlMS: 5000, wantTTL: 5000},
		{name: "zero", ttlMS: 0, wantErr: true},
		{name: "over protocol maximum", ttlMS: 5001, wantErr: true},
		{name: "maximum uint64", ttlMS: math.MaxUint64, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := (&launcherConfig{configuredMaxTTLMS: tt.ttlMS}).serverConfig(testLogger)
			if (err != nil) != tt.wantErr {
				t.Fatalf("serverConfig error = %v, wantErr %t", err, tt.wantErr)
			}
			if !tt.wantErr && config.MaxTTL != tt.wantTTL {
				t.Fatalf("max TTL = %d, want %d", config.MaxTTL, tt.wantTTL)
			}
			if !tt.wantErr && config.MaxKeys != server.DefaultMaxKeys {
				t.Fatalf("max keys = %d, want %d", config.MaxKeys, server.DefaultMaxKeys)
			}
		})
	}
}

func TestServerConfigPassesImplementationTuning(t *testing.T) {
	config, err := (&launcherConfig{
		configuredMaxTTLMS: 1000,
		maxKeys:            123,
		shardCount:         8,
	}).serverConfig(testLogger)
	if err != nil {
		t.Fatalf("serverConfig: %v", err)
	}
	if config.MaxKeys != 123 || config.ShardCount != 8 {
		t.Fatalf("unexpected server config: %+v", config)
	}
}
