package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	loadEtcdSlotCount        = 5
	loadEtcdOperationTimeout = 2 * time.Second
	loadEtcdRetryDelay       = time.Second
)

type targetState struct {
	mu      sync.Mutex
	targets []string
	version uint64
	changed chan struct{}
}

func newTargetState() *targetState {
	return &targetState{changed: make(chan struct{})}
}

func (s *targetState) snapshot() ([]string, uint64, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.targets), s.version, s.changed
}

func (s *targetState) update(targets []string, force bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !force && slices.Equal(s.targets, targets) {
		return false
	}
	s.targets = slices.Clone(targets)
	s.version++
	close(s.changed)
	s.changed = make(chan struct{})
	return true
}

func (s *targetState) waitReady(ctx context.Context) ([]string, uint64, error) {
	for {
		targets, version, changed := s.snapshot()
		if len(targets) != 0 {
			return targets, version, nil
		}
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-changed:
		}
	}
}

type etcdTargetDiscovery struct {
	client    *clientv3.Client
	keyPrefix string
	keys      []string
	targets   *targetState
	logger    *slog.Logger

	cancel    context.CancelFunc
	worker    sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func startEtcdTargetDiscovery(
	ctx context.Context,
	endpoint string,
	prefix string,
	requiredSlots int,
	logger *slog.Logger,
) (*etcdTargetDiscovery, error) {
	if requiredSlots < 1 || requiredSlots > loadEtcdSlotCount {
		return nil, fmt.Errorf("invalid required etcd server-slot count %d", requiredSlots)
	}
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: loadEtcdOperationTimeout,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	discoveryContext, cancel := context.WithCancel(ctx)
	allKeys := loadEtcdSlotKeys(prefix)
	discovery := &etcdTargetDiscovery{
		client:    etcdClient,
		keyPrefix: prefix + "-redlease-server-",
		keys:      slices.Clone(allKeys[:requiredSlots]),
		targets:   newTargetState(),
		logger:    logger,
		cancel:    cancel,
	}
	discovery.worker.Add(1)
	go discovery.run(discoveryContext)
	return discovery, nil
}

func loadEtcdSlotKeys(prefix string) [loadEtcdSlotCount]string {
	names := [loadEtcdSlotCount]string{
		"redlease-server-1",
		"redlease-server-2",
		"redlease-server-3",
		"redlease-server-4",
		"redlease-server-5",
	}
	var keys [loadEtcdSlotCount]string
	for index, name := range names {
		keys[index] = prefix + "-" + name
	}
	return keys
}

func (d *etcdTargetDiscovery) run(ctx context.Context) {
	defer d.worker.Done()
	for ctx.Err() == nil {
		slots, revision, err := d.readSnapshot(ctx)
		if err == nil {
			// A reconnect can hide a delete and recreate with the same value.
			// Conservatively rebuild clients after every fresh etcd snapshot.
			d.publish(slots, true)
			err = d.watch(ctx, revision+1, slots)
		}
		if ctx.Err() != nil {
			return
		}
		d.logger.Warn("etcd server discovery interrupted", slog.Any("error", err))
		if !waitLoadEtcdRetry(ctx) {
			return
		}
	}
}

func (d *etcdTargetDiscovery) readSnapshot(
	ctx context.Context,
) (map[string]string, int64, error) {
	readContext, cancel := context.WithTimeout(
		clientv3.WithRequireLeader(ctx),
		loadEtcdOperationTimeout,
	)
	response, err := d.client.Get(readContext, d.keyPrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, 0, fmt.Errorf("read etcd server slots: %w", err)
	}

	slots := make(map[string]string, len(d.keys))
	for _, keyValue := range response.Kvs {
		key := string(keyValue.GetKey())
		if d.requiredKey(key) {
			slots[key] = string(keyValue.GetValue())
		}
	}
	return slots, response.Header.GetRevision(), nil
}

func (d *etcdTargetDiscovery) watch(
	ctx context.Context,
	revision int64,
	slots map[string]string,
) error {
	watchContext, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()
	responses := d.client.Watch(
		watchContext,
		d.keyPrefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(revision),
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response, open := <-responses:
			if !open {
				return errors.New("etcd server-slot watch closed")
			}
			if err := response.Err(); err != nil {
				return fmt.Errorf("watch etcd server slots: %w", err)
			}
			requiredKeyChanged := false
			for _, event := range response.Events {
				key := string(event.GetKv().GetKey())
				if !d.requiredKey(key) {
					continue
				}
				requiredKeyChanged = true
				if event.Type == clientv3.EventTypeDelete {
					delete(slots, key)
				} else {
					slots[key] = string(event.GetKv().GetValue())
				}
			}
			if requiredKeyChanged {
				d.publish(slots, true)
			}
		}
	}
}

func (d *etcdTargetDiscovery) requiredKey(key string) bool {
	return slices.Contains(d.keys, key)
}

func (d *etcdTargetDiscovery) publish(slots map[string]string, force bool) {
	targets, complete, err := orderedEtcdTargets(d.keys, slots)
	if err != nil {
		if d.targets.update(nil, force) {
			d.logger.Error("invalid RedLease targets in etcd", slog.Any("error", err))
		}
		return
	}
	if !complete {
		if d.targets.update(nil, force) {
			d.logger.Warn("required RedLease server slots are not all present in etcd")
		}
		return
	}
	if d.targets.update(targets, force) {
		d.logger.Info("RedLease server targets discovered", slog.Any("targets", targets))
	}
}

func orderedEtcdTargets(keys []string, slots map[string]string) ([]string, bool, error) {
	targets := make([]string, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for index, key := range keys {
		target, exists := slots[key]
		if !exists {
			return nil, false, nil
		}
		if target == "" {
			return nil, false, fmt.Errorf("etcd key %q contains an empty address", key)
		}
		if _, duplicate := seen[target]; duplicate {
			return nil, false, fmt.Errorf("etcd keys contain duplicate address %q", target)
		}
		seen[target] = struct{}{}
		targets[index] = target
	}
	return targets, true, nil
}

func waitLoadEtcdRetry(ctx context.Context) bool {
	timer := time.NewTimer(loadEtcdRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (d *etcdTargetDiscovery) Close() error {
	d.closeOnce.Do(func() {
		d.cancel()
		clientErr := d.client.Close()
		d.worker.Wait()
		d.closeErr = clientErr
	})
	return d.closeErr
}
