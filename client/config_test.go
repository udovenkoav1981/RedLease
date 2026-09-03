package client

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestConfigRequiresAllFiveTargets(t *testing.T) {
	config := validClientConfig()
	config.Servers[3].Target = ""

	err := config.Validate()
	if err == nil || !strings.Contains(err.Error(), "server 3 target") {
		t.Fatalf("Validate error = %v, want missing server 3 target", err)
	}
}

func TestConfigRejectsNegativeResponseTimeout(t *testing.T) {
	config := validClientConfig()
	config.ResponseTimeout = -time.Millisecond

	if err := config.Validate(); err == nil {
		t.Fatal("negative response timeout accepted")
	}
}

func TestResolveClientConfigAppliesDefaultsAndCopiesOptions(t *testing.T) {
	config := validClientConfig()
	config.Servers[0].DialOptions = []grpc.DialOption{grpc.WithNoProxy()}

	resolved, err := resolveClientConfig(config)
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	if resolved.responseTimeout != defaultResponseTimeout {
		t.Fatalf("response timeout = %v, want %v", resolved.responseTimeout, defaultResponseTimeout)
	}
	if len(resolved.servers[0].DialOptions) != 1 {
		t.Fatalf("resolved dial options = %d, want 1", len(resolved.servers[0].DialOptions))
	}

	config.Servers[0].DialOptions = nil
	if len(resolved.servers[0].DialOptions) != 1 {
		t.Fatal("resolved dial options alias input slice")
	}
}

func validClientConfig() Config {
	var config Config
	for index := range config.Servers {
		config.Servers[index].Target = "test-target"
	}
	return config
}
