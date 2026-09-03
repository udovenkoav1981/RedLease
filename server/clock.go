package server

import "time"

// Lease deadlines use wall time so time spent in OS/VM suspend is observable
// after resume.
type wallClock interface {
	Now() time.Time
}

type systemWallClock struct{}

func (systemWallClock) Now() time.Time {
	return time.Now().Round(0)
}

type dependencies struct {
	wall wallClock
	// quarantineDelay is a test seam. Production leaves it zero and always
	// uses restartQuarantineTime.
	quarantineDelay time.Duration
	beforeApply     func(operation)
	afterReceive    func(serverPhase)
}
