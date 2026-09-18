// Command redlease-load measures concurrent short-lived leases through the
// public RedLease clients against separately started gRPC lock-servers.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/udovenkoav1981/RedLease/client"
)

const (
	defaultClients          = "2,4,8,16,32"
	defaultLeases           = "16,32,64,128,256"
	clientName              = "client"
	client1of1Name          = "client1of1"
	quorum1of1              = "1/1"
	defaultTTLMS     uint64 = 1000
	maxProtocolTTLMS uint64 = 5000
	defaultTimeoutMS uint32 = 1000
	safetyMarginMS   uint64 = 100
	maxWorkers              = 32_768
)

type options struct {
	clientKind      string
	quorum          string
	targets         []string
	clientCounts    []int
	leaseCounts     []int
	duration        time.Duration
	hold            time.Duration
	ttlMS           uint64
	responseTimeout uint32
	settle          time.Duration
}

type scenario struct {
	name        string
	quorum      client.Quorum
	serverCount int
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, "redlease-load:", err)
		os.Exit(1)
	}
}

func run(args []string, output, flagOutput io.Writer) error {
	config, err := parseOptions(args, flagOutput)
	if err != nil {
		return err
	}
	scenarios, err := config.scenarios()
	if err != nil {
		return err
	}

	writer := csv.NewWriter(output)
	if err := writer.Write([]string{
		clientName, "quorum", "clients", "leases_per_client", "elapsed_s",
		"acquire_ok", "acquire_failed", "key_limit_failed", "expired_before_release",
		"acquires_per_s", "client_min_per_s", "client_max_per_s", "client_min_max_ratio",
		"acquire_avg_ms", "acquire_p50_ms", "acquire_p95_ms", "acquire_p99_ms",
	}); err != nil {
		return fmt.Errorf("write CSV header: %w", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush CSV header: %w", err)
	}

	logger := slog.New(slog.DiscardHandler)
	for _, clientCount := range config.clientCounts {
		for _, leaseCount := range config.leaseCounts {
			for _, selected := range scenarios {
				result, caseErr := runCase(context.Background(), selected, clientCount, leaseCount, &config, logger)
				if caseErr != nil {
					return fmt.Errorf("%s %s with %d clients and %d leases/client: %w",
						selected.name, config.quorum, clientCount, leaseCount, caseErr)
				}
				if err := writer.Write(result.csvRecord(selected, config.quorum, clientCount, leaseCount)); err != nil {
					return fmt.Errorf("write CSV result: %w", err)
				}
				writer.Flush()
				if err := writer.Error(); err != nil {
					return fmt.Errorf("flush CSV result: %w", err)
				}
			}
		}
	}
	return nil
}

func parseOptions(args []string, output io.Writer) (options, error) {
	flags := flag.NewFlagSet("redlease-load", flag.ContinueOnError)
	flags.SetOutput(output)
	clientKind := flags.String(clientName, "both", "client1of1, client, or both (both requires quorum 1/1)")
	quorum := flags.String("quorum", quorum1of1, "quorum for the generic client: 1/1, 2/3, or 3/5")
	targets := flags.String("targets", "", "comma-separated addresses of already running gRPC lock-servers")
	clients := flags.String("clients", defaultClients, "comma-separated client counts")
	leases := flags.String("leases-per-client", defaultLeases, "comma-separated concurrent lease counts per client")
	duration := flags.Duration("duration", 5*time.Second, "measurement time per matrix cell")
	hold := flags.Duration("hold", 10*time.Millisecond, "simulated work time while holding each lease")
	ttlMS := flags.Uint64("ttl-ms", defaultTTLMS, "requested lease TTL in milliseconds (101..5000)")
	responseTimeout := defaultTimeoutMS
	flags.Func("response-timeout-ms", "per-server response timeout in milliseconds", func(raw string) error {
		value, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return err
		}
		responseTimeout = uint32(value)
		return nil
	})
	settle := flags.Duration(
		"settle", 0, "pause after each measurement for pending Release and lease expiry (0: automatic)",
	)
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	clientCounts, err := parsePositiveList(*clients)
	if err != nil {
		return options{}, fmt.Errorf("clients: %w", err)
	}
	leaseCounts, err := parsePositiveList(*leases)
	if err != nil {
		return options{}, fmt.Errorf("leases-per-client: %w", err)
	}
	serverTargets, err := parseTargets(*targets)
	if err != nil {
		return options{}, fmt.Errorf("targets: %w", err)
	}
	result := options{
		clientKind:      *clientKind,
		quorum:          *quorum,
		targets:         serverTargets,
		clientCounts:    clientCounts,
		leaseCounts:     leaseCounts,
		duration:        *duration,
		hold:            *hold,
		ttlMS:           *ttlMS,
		responseTimeout: responseTimeout,
		settle:          *settle,
	}
	if result.duration <= 0 || result.hold < 0 {
		return options{}, errors.New("duration must be positive and hold must not be negative")
	}
	if result.ttlMS <= safetyMarginMS || result.ttlMS > maxProtocolTTLMS {
		return options{}, errors.New("ttl-ms must be between 101 and 5000")
	}
	if result.hold >= time.Duration(result.ttlMS-safetyMarginMS)*time.Millisecond {
		return options{}, errors.New("hold must be less than ttl-ms minus the 100 ms safety margin")
	}
	if result.responseTimeout == 0 {
		return options{}, errors.New("response-timeout-ms must be positive")
	}
	if result.settle < 0 {
		return options{}, errors.New("settle must not be negative")
	}
	for _, clientCount := range result.clientCounts {
		for _, leaseCount := range result.leaseCounts {
			if clientCount > maxWorkers/leaseCount {
				return options{}, fmt.Errorf("%d clients × %d leases exceeds %d concurrent workers",
					clientCount, leaseCount, maxWorkers)
			}
		}
	}
	scenarios, err := result.scenarios()
	if err != nil {
		return options{}, err
	}
	for _, selected := range scenarios {
		if len(result.targets) != selected.serverCount {
			return options{}, fmt.Errorf("%s quorum %s requires %d distinct targets, got %d",
				selected.name, result.quorum, selected.serverCount, len(result.targets))
		}
	}
	return result, nil
}

func parseTargets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("provide -targets with the addresses of running servers")
	}
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		target := strings.TrimSpace(part)
		if target == "" {
			return nil, errors.New("empty server address")
		}
		if _, duplicate := seen[target]; duplicate {
			return nil, fmt.Errorf("duplicate server address %q", target)
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets, nil
}

func (o *options) settleDuration() time.Duration {
	if o.settle > 0 {
		return o.settle
	}
	ttl := time.Duration(o.ttlMS) * time.Millisecond //nolint:gosec // ttlMS is validated as <= 5000.
	return ttl + time.Duration(o.responseTimeout)*time.Millisecond + time.Duration(safetyMarginMS)*time.Millisecond
}

func parsePositiveList(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid positive integer %q", part)
		}
		values = append(values, value)
	}
	return values, nil
}

func (o *options) scenarios() ([]scenario, error) {
	var generic scenario
	switch o.quorum {
	case quorum1of1:
		generic = scenario{name: clientName, quorum: client.Quorum1Of1, serverCount: 1}
	case "2/3":
		generic = scenario{name: clientName, quorum: client.Quorum2Of3, serverCount: 3}
	case "3/5":
		generic = scenario{name: clientName, quorum: client.Quorum3Of5, serverCount: 5}
	default:
		return nil, fmt.Errorf("unsupported quorum %q", o.quorum)
	}
	specialized := scenario{name: client1of1Name, quorum: client.Quorum1Of1, serverCount: 1}
	switch o.clientKind {
	case client1of1Name:
		if o.quorum != quorum1of1 {
			return nil, errors.New("client1of1 supports only quorum 1/1")
		}
		return []scenario{specialized}, nil
	case clientName:
		return []scenario{generic}, nil
	case "both":
		if o.quorum != quorum1of1 {
			return nil, errors.New("both requires quorum 1/1 for comparable results")
		}
		return []scenario{specialized, generic}, nil
	default:
		return nil, fmt.Errorf("unsupported client %q", o.clientKind)
	}
}
