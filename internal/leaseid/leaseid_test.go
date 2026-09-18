package leaseid

import (
	"testing"
)

func TestNewBootID(t *testing.T) {
	if _, err := NewBootID(); err != nil {
		t.Fatalf("new boot ID: %v", err)
	}
}
