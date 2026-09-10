package client1of1

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var testLogger = slog.New(slog.DiscardHandler)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name: "valid",
			config: Config{
				Target: "test-server",
				Logger: testLogger,
			},
		},
		{
			name: "missing logger",
			config: Config{
				Target: "test-server",
			},
			want: "logger",
		},
		{
			name: "missing target",
			config: Config{
				Logger: testLogger,
			},
			want: "target",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.want == "" && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("Validate error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestNewAppliesResponseTimeout(t *testing.T) {
	tests := []struct {
		name    string
		value   uint32
		timeout time.Duration
	}{
		{name: "default", timeout: defaultResponseTimeout},
		{name: "configured", value: 1250, timeout: 1250 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := New(Config{
				ClientID:        7,
				Target:          "passthrough:///unavailable",
				DialOptions:     []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
				Logger:          testLogger,
				ResponseTimeout: test.value,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer client.Close()
			if client.responseTimeout != test.timeout {
				t.Fatalf("response timeout = %v, want %v", client.responseTimeout, test.timeout)
			}
		})
	}
}

func TestLeaseIDSequenceStartsAtOne(t *testing.T) {
	generator, err := newLeaseIDGeneratorFromReader(9, strings.NewReader("boot"))
	if err != nil {
		t.Fatalf("newLeaseIDGeneratorFromReader: %v", err)
	}
	first := generator.next()
	second := generator.next()
	if first.clientID != 9 || first.sequence != 1 || second.sequence != 2 {
		t.Fatalf("lease IDs = %+v, %+v", first, second)
	}
}
