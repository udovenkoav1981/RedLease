package client

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConfigRequiresAllFiveTargets(t *testing.T) {
	config := validClientConfig()
	config.Servers[3].Target = ""

	err := config.Validate()
	if err == nil || !strings.Contains(err.Error(), "server 3 target") {
		t.Fatalf("Validate error = %v, want missing server 3 target", err)
	}
}

func TestNewAppliesDefaultsAndCopiesOptions(t *testing.T) {
	config := connectableClientConfig()
	config.Servers[0].DialOptions = append(config.Servers[0].DialOptions, grpc.WithNoProxy())

	client, err := New(config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if client.clientID != config.ClientID {
		t.Fatalf("client ID = %d, want %d", client.clientID, config.ClientID)
	}
	if client.responseTimeout != defaultResponseTimeout {
		t.Fatalf("response timeout = %v, want %v", client.responseTimeout, defaultResponseTimeout)
	}
	if len(client.servers[0].DialOptions) != 2 {
		t.Fatalf("client dial options = %d, want 2", len(client.servers[0].DialOptions))
	}

	config.Servers[0].DialOptions = nil
	if len(client.servers[0].DialOptions) != 2 {
		t.Fatal("client dial options alias input slice")
	}
}

func TestNewConvertsResponseTimeoutFromMilliseconds(t *testing.T) {
	config := connectableClientConfig()
	config.ResponseTimeout = 1250

	client, err := New(config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if client.responseTimeout != 1250*time.Millisecond {
		t.Fatalf("response timeout = %v, want 1250ms", client.responseTimeout)
	}
}

func validClientConfig() Config {
	var config Config
	for index := range config.Servers {
		config.Servers[index].Target = "test-target"
	}
	return config
}

func connectableClientConfig() Config {
	config := validClientConfig()
	for index := range config.Servers {
		config.Servers[index].DialOptions = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	return config
}
