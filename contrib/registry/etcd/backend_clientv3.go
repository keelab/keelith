package etcd

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type sdkBackend struct {
	client *clientv3.Client
}

func (backend *sdkBackend) Grant(
	ctx context.Context,
	ttl time.Duration,
) (LeaseID, error) {
	seconds := int64((ttl + time.Second - 1) / time.Second)
	response, err := backend.client.Grant(ctx, seconds)
	if err != nil {
		return 0, err
	}
	if response == nil || response.ID == clientv3.NoLease {
		return 0, errors.New("etcd registry: grant returned no lease")
	}
	return LeaseID(response.ID), nil
}

func (backend *sdkBackend) KeepAlive(
	ctx context.Context,
	lease LeaseID,
) (<-chan error, error) {
	responses, err := backend.client.KeepAlive(ctx, clientv3.LeaseID(lease))
	if err != nil {
		return nil, err
	}
	terminal := make(chan error, 1)
	go func() {
		defer close(terminal)
		for {
			select {
			case response, ok := <-responses:
				if !ok {
					if context.Cause(ctx) == nil {
						terminal <- ErrLeaseLost
					}
					return
				}
				if response == nil || response.TTL <= 0 {
					terminal <- ErrLeaseLost
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return terminal, nil
}

func (backend *sdkBackend) Put(
	ctx context.Context,
	key string,
	value []byte,
	lease LeaseID,
) (int64, error) {
	response, err := backend.client.Put(
		ctx,
		key,
		string(value),
		clientv3.WithLease(clientv3.LeaseID(lease)),
	)
	if err != nil {
		return 0, err
	}
	if response == nil || response.Header == nil {
		return 0, errors.New("etcd registry: put returned no revision")
	}
	return response.Header.Revision, nil
}

func (backend *sdkBackend) Delete(
	ctx context.Context,
	key string,
) (int64, error) {
	response, err := backend.client.Delete(ctx, key)
	if err != nil {
		return 0, err
	}
	if response == nil || response.Header == nil {
		return 0, errors.New("etcd registry: delete returned no revision")
	}
	return response.Header.Revision, nil
}

func (backend *sdkBackend) Revoke(
	ctx context.Context,
	lease LeaseID,
) error {
	_, err := backend.client.Revoke(ctx, clientv3.LeaseID(lease))
	return err
}

func (backend *sdkBackend) List(
	ctx context.Context,
	prefix string,
) ([]Entry, int64, error) {
	response, err := backend.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}
	if response == nil || response.Header == nil {
		return nil, 0, errors.New("etcd registry: list returned no revision")
	}
	entries := make([]Entry, 0, len(response.Kvs))
	for _, keyValue := range response.Kvs {
		entries = append(entries, Entry{
			Key:   string(keyValue.Key),
			Value: append([]byte(nil), keyValue.Value...),
		})
	}
	return entries, response.Header.Revision, nil
}

func (backend *sdkBackend) Watch(
	ctx context.Context,
	prefix string,
	fromRevision int64,
) <-chan Batch {
	source := backend.client.Watch(
		ctx,
		prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(fromRevision),
	)
	batches := make(chan Batch)
	go func() {
		defer close(batches)
		for response := range source {
			batch := Batch{Revision: response.Header.Revision}
			if err := response.Err(); err != nil {
				batch.Err = err
			} else {
				batch.Events = make([]Event, 0, len(response.Events))
				for _, backendEvent := range response.Events {
					event := Event{
						Key: string(backendEvent.Kv.Key),
					}
					switch backendEvent.Type {
					case clientv3.EventTypePut:
						event.Type = EventPut
						event.Value = append(
							[]byte(nil),
							backendEvent.Kv.Value...,
						)
					case clientv3.EventTypeDelete:
						event.Type = EventDelete
					default:
						batch.Err = fmt.Errorf(
							"%w: event type %d",
							ErrInvalidRecord,
							backendEvent.Type,
						)
					}
					batch.Events = append(batch.Events, event)
				}
			}
			select {
			case batches <- batch:
			case <-ctx.Done():
				return
			}
			if batch.Err != nil {
				return
			}
		}
	}()
	return batches
}

func (backend *sdkBackend) Close() error {
	return backend.client.Close()
}
