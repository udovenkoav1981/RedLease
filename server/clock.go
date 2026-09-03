package server

import "time"

// wallClock is deliberately separate from the timer used for quarantine.
// Lease deadlines use wall time so time spent in OS/VM suspend is observable
// after resume.
type wallClock interface {
	Now() time.Time
}

type systemWallClock struct{}

func (systemWallClock) Now() time.Time {
	return time.Now().Round(0)
}

// monotonicTimer is the small portion of time.Timer needed by the server. The
// production implementation uses Go's monotonic timer machinery; tests can
// advance quarantine independently of the wall clock.
type monotonicTimer interface {
	Chan() <-chan time.Time
	Stop() bool
}

type timerFactory interface {
	NewTimer(time.Duration) monotonicTimer
}

type systemTimerFactory struct{}

func (systemTimerFactory) NewTimer(d time.Duration) monotonicTimer {
	return &systemTimer{timer: time.NewTimer(d)}
}

type systemTimer struct {
	timer *time.Timer
}

func (t *systemTimer) Chan() <-chan time.Time { return t.timer.C }
func (t *systemTimer) Stop() bool             { return t.timer.Stop() }

type dependencies struct {
	wall         wallClock
	timers       timerFactory
	beforeApply  func(operation)
	afterReceive func(serverPhase)
}
