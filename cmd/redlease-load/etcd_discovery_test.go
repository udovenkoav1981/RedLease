package main

import (
	"slices"
	"testing"
)

func TestLoadEtcdSlotKeys(t *testing.T) {
	got := loadEtcdSlotKeys("cluster-a")
	want := [loadEtcdSlotCount]string{
		"cluster-a-redlease-server-1",
		"cluster-a-redlease-server-2",
		"cluster-a-redlease-server-3",
		"cluster-a-redlease-server-4",
		"cluster-a-redlease-server-5",
	}
	if got != want {
		t.Fatalf("loadEtcdSlotKeys = %v, want %v", got, want)
	}
}

func TestOrderedEtcdTargetsUsesRequiredKeyOrder(t *testing.T) {
	keys := []string{"slot-1", "slot-2", "slot-3"}
	slots := map[string]string{
		"slot-3": "127.0.0.1:17381",
		"slot-1": "127.0.0.1:17379",
		"slot-2": "127.0.0.1:17380",
		"slot-4": "127.0.0.1:17382",
	}
	targets, complete, err := orderedEtcdTargets(keys, slots)
	if err != nil || !complete {
		t.Fatalf("orderedEtcdTargets = %v, %t, %v; want complete targets", targets, complete, err)
	}
	want := []string{"127.0.0.1:17379", "127.0.0.1:17380", "127.0.0.1:17381"}
	if !slices.Equal(targets, want) {
		t.Fatalf("orderedEtcdTargets = %v, want %v", targets, want)
	}
}

func TestOrderedEtcdTargetsRejectsIncompleteOrDuplicateSlots(t *testing.T) {
	keys := []string{"slot-1", "slot-2"}
	if targets, complete, err := orderedEtcdTargets(keys, map[string]string{"slot-1": "server-1"}); err != nil || complete || targets != nil {
		t.Fatalf("incomplete slots = %v, %t, %v", targets, complete, err)
	}
	if _, _, err := orderedEtcdTargets(keys, map[string]string{
		"slot-1": "server-1",
		"slot-2": "server-1",
	}); err == nil {
		t.Fatal("duplicate server addresses accepted")
	}
}

func TestTargetStateForcedUpdateSignalsUnchangedAddresses(t *testing.T) {
	state := newTargetState()
	state.update([]string{"server-1"}, false)
	_, firstVersion, firstChanged := state.snapshot()
	if state.update([]string{"server-1"}, false) {
		t.Fatal("unchanged non-forced update reported a change")
	}
	select {
	case <-firstChanged:
		t.Fatal("unchanged non-forced update signaled watchers")
	default:
	}
	if !state.update([]string{"server-1"}, true) {
		t.Fatal("forced update did not report a change")
	}
	_, secondVersion, _ := state.snapshot()
	if secondVersion != firstVersion+1 {
		t.Fatalf("version = %d, want %d", secondVersion, firstVersion+1)
	}
	select {
	case <-firstChanged:
	default:
		t.Fatal("forced update did not signal watchers")
	}
}
