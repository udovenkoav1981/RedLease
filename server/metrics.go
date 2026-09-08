package server

// MetricsSnapshot is a lock-free, point-in-time view of server metrics.
// Fields may have been observed at slightly different instants. State is one
// of "quarantine", "active", "failed", "closed" or "unknown".
type MetricsSnapshot struct {
	State                    string
	ResidentKeys             uint64
	QueuedOperations         uint64
	ActiveStreams            uint64
	AcquiresTotal            uint64
	RenewsTotal              uint64
	ReleasesTotal            uint64
	RestartQuarantineSkipped bool
}

// MetricsSnapshot returns the current server metrics without locking lease
// shards. ResidentKeys counts physically stored records, including records
// which have expired but have not yet been removed.
func (s *Server) MetricsSnapshot() MetricsSnapshot {
	var queuedOperations uint64
	for _, shard := range s.shards {
		queuedOperations += uint64(len(shard.jobs))
	}

	return MetricsSnapshot{
		State:                    metricsState(serverPhase(s.phase.Load())),
		ResidentKeys:             s.keys.Load(),
		QueuedOperations:         queuedOperations,
		ActiveStreams:            uint64(s.activeStreams.Load()),
		AcquiresTotal:            s.operationTotals[operationAcquire].Load(),
		RenewsTotal:              s.operationTotals[operationRenew].Load(),
		ReleasesTotal:            s.operationTotals[operationRelease].Load(),
		RestartQuarantineSkipped: s.config.SkipRestartQuarantine,
	}
}

func metricsState(phase serverPhase) string {
	switch phase {
	case phaseQuarantine:
		return "quarantine"
	case phaseActive:
		return "active"
	case phaseFailed:
		return "failed"
	case phaseClosed:
		return "closed"
	default:
		return "unknown"
	}
}
