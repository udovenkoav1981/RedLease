package client1of1forvalkey

import (
	"log/slog"
	"strings"
	"testing"
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
				Target: "127.0.0.1:6379",
				Logger: testLogger,
			},
		},
		{
			name: "missing logger",
			config: Config{
				Target: "127.0.0.1:6379",
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
