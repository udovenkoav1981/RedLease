package server

import "time"

type dependencies struct {
	now func() time.Time
	// quarantineDelay is a test seam. Production leaves it zero and always
	// uses restartQuarantineTime.
	quarantineDelay time.Duration
	beforeApply     func(operation)
	afterReceive    func(serverPhase)
}
