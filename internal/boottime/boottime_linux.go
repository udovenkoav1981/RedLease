//go:build linux

// Package boottime provides suspend-aware monotonic time on Linux.
package boottime

import "golang.org/x/sys/unix"

const (
	millisecondsPerSecond     = 1_000
	nanosecondsPerMillisecond = 1_000_000
)

// Now returns milliseconds elapsed on CLOCK_BOOTTIME.
func Now() uint64 {
	var value unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &value); err != nil {
		panic("boottime: clock_gettime: " + err.Error())
	}
	return uint64(value.Sec)*millisecondsPerSecond + uint64(value.Nsec)/nanosecondsPerMillisecond
}

// Remaining returns whole milliseconds until deadline.
func Remaining(deadline, now uint64) uint64 {
	if deadline <= now {
		return 0
	}
	return deadline - now
}
