package client

import "testing"

func TestSystemWallClockRemovesMonotonicReading(t *testing.T) {
	got := (systemWallClock{}).now()
	if got != got.Round(0) {
		t.Fatal("system wall clock returned a time with a monotonic reading")
	}
}
