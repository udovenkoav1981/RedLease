package client

import "testing"

func TestWallNowRemovesMonotonicReading(t *testing.T) {
	got := wallNow()
	if got != got.Round(0) {
		t.Fatal("system wall clock returned a time with a monotonic reading")
	}
}
