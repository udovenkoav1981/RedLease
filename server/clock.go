package server

import "time"

// Lease deadlines use wall time so time spent in OS/VM suspend is observable
// after resume.
func wallNow() time.Time {
	return time.Now().Round(0)
}

type dependencies struct {
	now func() time.Time
	// quarantineDelay is a test seam. Production leaves it zero and always
	// uses restartQuarantineTime.
	quarantineDelay time.Duration
	beforeApply     func(operation)
	afterReceive    func(serverPhase)
}
