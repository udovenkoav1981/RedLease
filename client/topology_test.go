package client

// Most client tests exercise the largest supported topology. Tests for the
// mapping itself cover all supported quorum configurations.
const (
	testQuorum      Quorum = Quorum3Of5
	testServerCount        = 5
	testQuorumSize         = 3
)
