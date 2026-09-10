package prometheus

import (
	"testing"

	clientprometheus "github.com/prometheus/client_golang/prometheus"

	redleaseserver "github.com/udovenkoav1981/RedLease/server"
)

type fixedMetricsSource struct {
	snapshot redleaseserver.MetricsSnapshot
}

func (s fixedMetricsSource) MetricsSnapshot() redleaseserver.MetricsSnapshot {
	return s.snapshot
}

func TestNewCollectorRejectsNilServer(t *testing.T) {
	t.Parallel()
	if _, err := NewCollector(nil); err == nil {
		t.Fatal("NewCollector(nil) succeeded")
	}
}

func TestCollectorExportsSnapshot(t *testing.T) {
	t.Parallel()
	collector := newCollector(fixedMetricsSource{snapshot: redleaseserver.MetricsSnapshot{
		State:                    "active",
		ResidentKeys:             7,
		QueuedOperations:         3,
		ActiveStreams:            2,
		AcquiresTotal:            11,
		RenewsTotal:              12,
		ReleasesTotal:            13,
		RestartQuarantineSkipped: true,
	}})
	registry := clientprometheus.NewRegistry()
	if err := registry.Register(collector); err != nil {
		t.Fatalf("Register: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	wantValues := map[string]float64{
		"redlease_server_resident_keys":              7,
		"redlease_server_queued_operations":          3,
		"redlease_server_active_streams":             2,
		"redlease_server_restart_quarantine_skipped": 1,
		"redlease_server_acquires_total":             11,
		"redlease_server_renews_total":               12,
		"redlease_server_releases_total":             13,
	}
	states := make(map[string]float64)
	for _, family := range families {
		name := family.GetName()
		if name == "redlease_server_state" {
			for _, metric := range family.GetMetric() {
				state := ""
				for _, label := range metric.GetLabel() {
					if label.GetName() == "state" {
						state = label.GetValue()
					}
				}
				states[state] = metric.GetGauge().GetValue()
			}
			continue
		}

		want, expected := wantValues[name]
		if !expected {
			continue
		}
		metrics := family.GetMetric()
		if len(metrics) != 1 {
			t.Fatalf("metric family %s contains %d metrics, want 1", name, len(metrics))
		}
		got := metrics[0].GetGauge().GetValue()
		if family.GetType().String() == "COUNTER" {
			got = metrics[0].GetCounter().GetValue()
		}
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
		delete(wantValues, name)
	}
	if len(wantValues) != 0 {
		t.Errorf("missing metric families: %v", wantValues)
	}
	for _, state := range serverStates {
		want := 0.0
		if state == "active" {
			want = 1
		}
		if got := states[state]; got != want {
			t.Errorf("state %q = %v, want %v", state, got, want)
		}
	}
}
