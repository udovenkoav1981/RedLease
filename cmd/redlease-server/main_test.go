package main

import (
	"bytes"
	"flag"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/udovenkoav1981/RedLease/server"
)

func TestParseFlagsDefaults(t *testing.T) {
	config, err := parseFlags(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if config.listenAddress != defaultListenAddress {
		t.Errorf("listen address = %q, want %q", config.listenAddress, defaultListenAddress)
	}
	if config.configuredMaxTTLMS != defaultConfiguredMaxTTL {
		t.Errorf("configured max TTL = %d, want %d", config.configuredMaxTTLMS, defaultConfiguredMaxTTL)
	}
	if config.shardCount != 0 || config.shardQueueDepth != 0 || config.maxInFlightPerStream != 0 {
		t.Errorf("implementation tuning defaults = (%d, %d, %d), want (0, 0, 0)",
			config.shardCount, config.shardQueueDepth, config.maxInFlightPerStream)
	}
}

func TestParseFlagsCustomValues(t *testing.T) {
	config, err := parseFlags([]string{
		"-listen", "127.0.0.1:15051",
		"-configured-max-ttl-ms", "1234",
		"-shard-count", "8",
		"-shard-queue-depth", "16",
		"-max-in-flight-per-stream", "32",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if config.listenAddress != "127.0.0.1:15051" ||
		config.configuredMaxTTLMS != 1234 ||
		config.shardCount != 8 ||
		config.shardQueueDepth != 16 ||
		config.maxInFlightPerStream != 32 {
		t.Fatalf("unexpected config: %+v", config)
	}
}

func TestParseFlagsHelpDescribesLocalPlaintextLauncher(t *testing.T) {
	var output bytes.Buffer
	_, err := parseFlags([]string{"-help"}, &output)
	if err != flag.ErrHelp {
		t.Fatalf("parseFlags help error = %v, want flag.ErrHelp", err)
	}
	for _, fragment := range []string{"Local test launcher", "plaintext gRPC", "no TLS or authentication"} {
		if !strings.Contains(output.String(), fragment) {
			t.Errorf("help does not contain %q:\n%s", fragment, output.String())
		}
	}
}

func TestServerConfigConvertsMillisecondsAfterBoundsCheck(t *testing.T) {
	tests := []struct {
		name    string
		ttlMS   uint64
		wantTTL time.Duration
		wantErr bool
	}{
		{name: "one millisecond", ttlMS: 1, wantTTL: time.Millisecond},
		{name: "protocol maximum", ttlMS: 5000, wantTTL: server.ProtocolMaxTTL},
		{name: "zero", ttlMS: 0, wantErr: true},
		{name: "over protocol maximum", ttlMS: 5001, wantErr: true},
		{name: "maximum uint64", ttlMS: math.MaxUint64, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := (launcherConfig{configuredMaxTTLMS: tt.ttlMS}).serverConfig()
			if (err != nil) != tt.wantErr {
				t.Fatalf("serverConfig error = %v, wantErr %t", err, tt.wantErr)
			}
			if !tt.wantErr && config.ConfiguredMaxTTL != tt.wantTTL {
				t.Fatalf("configured max TTL = %s, want %s", config.ConfiguredMaxTTL, tt.wantTTL)
			}
		})
	}
}

func TestServerConfigPassesImplementationTuning(t *testing.T) {
	config, err := (launcherConfig{
		configuredMaxTTLMS:   1000,
		shardCount:           8,
		shardQueueDepth:      16,
		maxInFlightPerStream: 32,
	}).serverConfig()
	if err != nil {
		t.Fatalf("serverConfig: %v", err)
	}
	if config.ShardCount != 8 || config.ShardQueueDepth != 16 || config.MaxInFlightPerStream != 32 {
		t.Fatalf("unexpected server config: %+v", config)
	}
}
