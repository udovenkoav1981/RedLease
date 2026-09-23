// Package bootid generates random boot identifiers shared by RedLease clients.
package bootid

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

// NewBootID generates a cryptographically random boot ID for one client process.
func NewBootID() (uint32, error) {
	var bootIDBytes [4]byte
	if _, err := rand.Read(bootIDBytes[:]); err != nil {
		return 0, fmt.Errorf("generate boot ID: %w", err)
	}

	return binary.BigEndian.Uint32(bootIDBytes[:]), nil
}
