package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	etcdLeaseTTLSeconds  = int64(3)
	etcdKeepAlivePeriod  = time.Second
	etcdOperationTimeout = 2 * time.Second
	etcdRetryDelay       = time.Second
	etcdServerSlotCount  = 5
	decimalBase          = 10
	tcpPortBits          = 16
)

var errEtcdSlotsOccupied = errors.New("all etcd server slots are occupied")

type launcherEtcdConfig struct {
	endpoint         string
	prefix           string
	advertiseAddress string
}

type etcdRegistration struct {
	client       *clientv3.Client
	leaseID      clientv3.LeaseID
	key          string
	cancel       context.CancelFunc
	parentCancel context.CancelFunc
	keepAliveErr chan error
	worker       sync.WaitGroup
	closeOnce    sync.Once
	closeErr     error
}

type etcdRegistrationResult struct {
	registration *etcdRegistration
	err          error
}

type etcdAcquisition struct {
	cancel context.CancelFunc
	done   chan etcdRegistrationResult
}

func (c *launcherConfig) etcdConfig() (launcherEtcdConfig, error) {
	if c.etcdEndpoint == "" {
		if c.etcdPrefix != "" || c.etcdAdvertiseAddress != "" {
			return launcherEtcdConfig{}, errors.New("-etcd-endpoint is required with other etcd options")
		}
		return launcherEtcdConfig{}, nil
	}
	if c.etcdPrefix == "" {
		return launcherEtcdConfig{}, errors.New("-etcd-prefix is required when etcd mode is enabled")
	}

	advertiseAddress := c.etcdAdvertiseAddress
	if advertiseAddress == "" {
		advertiseAddress = c.listenAddress
	}
	if err := validateEtcdAdvertiseAddress(advertiseAddress); err != nil {
		return launcherEtcdConfig{}, err
	}
	return launcherEtcdConfig{
		endpoint:         c.etcdEndpoint,
		prefix:           c.etcdPrefix,
		advertiseAddress: advertiseAddress,
	}, nil
}

func (c launcherEtcdConfig) enabled() bool {
	return c.endpoint != ""
}

func validateEtcdAdvertiseAddress(address string) error {
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid -etcd-advertise address %q: %w", address, err)
	}
	if net.ParseIP(host) == nil {
		return fmt.Errorf("-etcd-advertise host %q is not an IP address", host)
	}
	port, err := strconv.ParseUint(rawPort, decimalBase, tcpPortBits)
	if err != nil || port == 0 {
		return fmt.Errorf("invalid -etcd-advertise port %q", rawPort)
	}
	return nil
}

func acquireEtcdRegistration(
	ctx context.Context,
	config launcherEtcdConfig,
	logger *slog.Logger,
) (*etcdRegistration, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{config.endpoint},
		DialTimeout: etcdOperationTimeout,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}
	clientOwned := true
	defer func() {
		if clientOwned {
			_ = client.Close()
		}
	}()

	keys := etcdSlotKeys(config.prefix)
	for {
		registration, err := tryEtcdSlots(ctx, client, &keys, config.advertiseAddress)
		if err == nil {
			clientOwned = false
			logger.Info(
				"etcd server slot acquired",
				slog.String("key", registration.key),
				slog.String("value", config.advertiseAddress),
				slog.Int64("ttl_seconds", etcdLeaseTTLSeconds),
			)
			return registration, nil
		}
		if !errors.Is(err, errEtcdSlotsOccupied) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			logger.Warn("cannot acquire an etcd server slot", slog.Any("error", err))
			if err := waitEtcdRetry(ctx); err != nil {
				return nil, err
			}
			continue
		}

		logger.Info("all etcd server slots are occupied; waiting for a deletion")
		if err := waitForEtcdSlotDeletion(ctx, client, config.prefix, &keys); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			logger.Warn("etcd slot watch interrupted", slog.Any("error", err))
			if err := waitEtcdRetry(ctx); err != nil {
				return nil, err
			}
		}
	}
}

func startEtcdAcquisition(
	ctx context.Context,
	config launcherEtcdConfig,
	logger *slog.Logger,
) *etcdAcquisition {
	acquisitionContext, cancelAcquisition := context.WithCancel(ctx)
	acquisition := &etcdAcquisition{
		cancel: cancelAcquisition,
		done:   make(chan etcdRegistrationResult, 1),
	}
	go func() {
		registration, err := acquireEtcdRegistration(acquisitionContext, config, logger)
		acquisition.done <- etcdRegistrationResult{registration: registration, err: err}
	}()
	return acquisition
}

func (a *etcdAcquisition) Close() error {
	a.cancel()
	result := <-a.done
	if result.registration == nil {
		return nil
	}
	return result.registration.Close()
}

func etcdSlotKeys(prefix string) [etcdServerSlotCount]string {
	names := [etcdServerSlotCount]string{
		"redlease-server-1",
		"redlease-server-2",
		"redlease-server-3",
		"redlease-server-4",
		"redlease-server-5",
	}
	var keys [etcdServerSlotCount]string
	for index, name := range names {
		keys[index] = prefix + "-" + name
	}
	return keys
}

func tryEtcdSlots(
	ctx context.Context,
	client *clientv3.Client,
	keys *[etcdServerSlotCount]string,
	value string,
) (*etcdRegistration, error) {
	for _, key := range keys {
		registration, acquired, err := tryEtcdSlot(ctx, client, key, value)
		if err != nil {
			return nil, err
		}
		if acquired {
			return registration, nil
		}
	}
	return nil, errEtcdSlotsOccupied
}

func tryEtcdSlot(
	ctx context.Context,
	client *clientv3.Client,
	key string,
	value string,
) (*etcdRegistration, bool, error) {
	grantContext, cancelGrant := context.WithTimeout(
		clientv3.WithRequireLeader(ctx),
		etcdOperationTimeout,
	)
	grant, err := client.Grant(grantContext, etcdLeaseTTLSeconds)
	cancelGrant()
	if err != nil {
		return nil, false, fmt.Errorf("grant lease for %q: %w", key, err)
	}

	transactionContext, cancelTransaction := context.WithTimeout(
		clientv3.WithRequireLeader(ctx),
		etcdOperationTimeout,
	)
	transaction, err := client.Txn(transactionContext).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(grant.ID))).
		Commit()
	cancelTransaction()
	if err != nil {
		revokeEtcdLease(ctx, client, grant.ID)
		return nil, false, fmt.Errorf("put %q: %w", key, err)
	}
	if !transaction.Succeeded {
		revokeEtcdLease(ctx, client, grant.ID)
		return nil, false, nil
	}

	registrationContext, cancelRegistration := context.WithCancel(ctx)
	registration := &etcdRegistration{
		client:       client,
		leaseID:      grant.ID,
		key:          key,
		cancel:       cancelRegistration,
		keepAliveErr: make(chan error, 1),
	}
	registration.worker.Add(1)
	go registration.keepAlive(registrationContext)
	return registration, true, nil
}

func waitForEtcdSlotDeletion(
	ctx context.Context,
	client *clientv3.Client,
	prefix string,
	keys *[etcdServerSlotCount]string,
) error {
	keyPrefix := prefix + "-redlease-server-"
	snapshotContext, cancelSnapshot := context.WithTimeout(
		clientv3.WithRequireLeader(ctx),
		etcdOperationTimeout,
	)
	snapshot, err := client.Get(snapshotContext, keyPrefix, clientv3.WithPrefix())
	cancelSnapshot()
	if err != nil {
		return fmt.Errorf("read etcd server slots: %w", err)
	}

	slots := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		slots[key] = struct{}{}
	}
	present := make(map[string]struct{}, len(keys))
	for _, keyValue := range snapshot.Kvs {
		key := string(keyValue.GetKey())
		if _, isSlot := slots[key]; isSlot {
			present[key] = struct{}{}
		}
	}
	for _, key := range keys {
		if _, exists := present[key]; !exists {
			return nil
		}
	}

	watchContext, cancelWatch := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancelWatch()
	watch := client.Watch(
		watchContext,
		keyPrefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(snapshot.Header.GetRevision()+1),
		clientv3.WithFilterPut(),
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response, open := <-watch:
			if !open {
				return errors.New("etcd slot watch closed")
			}
			if err := response.Err(); err != nil {
				return fmt.Errorf("watch etcd server slots: %w", err)
			}
			for _, event := range response.Events {
				if _, isSlot := slots[string(event.GetKv().GetKey())]; isSlot {
					return nil
				}
			}
		}
	}
}

func waitEtcdRetry(ctx context.Context) error {
	timer := time.NewTimer(etcdRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *etcdRegistration) keepAlive(ctx context.Context) {
	defer r.worker.Done()
	ticker := time.NewTicker(etcdKeepAlivePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			keepAliveContext, cancelKeepAlive := context.WithTimeout(
				ctx,
				etcdKeepAlivePeriod,
			)
			response, err := r.client.KeepAliveOnce(keepAliveContext, r.leaseID)
			cancelKeepAlive()
			if err != nil {
				if ctx.Err() == nil {
					r.keepAliveErr <- fmt.Errorf("keep alive %q: %w", r.key, err)
				}
				return
			}
			if response == nil || response.TTL <= 0 {
				r.keepAliveErr <- fmt.Errorf("keep alive %q returned an expired lease", r.key)
				return
			}
		}
	}
}

func (r *etcdRegistration) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		if r.parentCancel != nil {
			r.parentCancel()
		}
		r.worker.Wait()
		revokeContext, cancelRevoke := context.WithTimeout(
			context.Background(),
			etcdOperationTimeout,
		)
		_, revokeErr := r.client.Revoke(revokeContext, r.leaseID)
		cancelRevoke()
		if revokeErr != nil {
			revokeErr = fmt.Errorf("revoke %q: %w", r.key, revokeErr)
		}
		r.closeErr = errors.Join(revokeErr, r.client.Close())
	})
	return r.closeErr
}

func revokeEtcdLease(ctx context.Context, client *clientv3.Client, leaseID clientv3.LeaseID) {
	revokeContext, cancelRevoke := context.WithTimeout(
		context.WithoutCancel(ctx),
		etcdOperationTimeout,
	)
	_, _ = client.Revoke(revokeContext, leaseID)
	cancelRevoke()
}
