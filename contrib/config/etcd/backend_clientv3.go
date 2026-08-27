package etcd

import (
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type sdkBackend struct {
	client *clientv3.Client
}

func (backend *sdkBackend) Get(
	ctx context.Context,
	key string,
) (Value, error) {
	response, err := backend.client.Get(ctx, key)
	if err != nil {
		return Value{}, err
	}
	if response == nil || response.Header == nil {
		return Value{}, errors.New("etcd config: get returned no revision")
	}
	if len(response.Kvs) == 0 {
		return Value{Revision: response.Header.Revision}, nil
	}
	if len(response.Kvs) != 1 {
		return Value{}, fmt.Errorf(
			"%w: exact key returned %d values",
			ErrInvalidDocument,
			len(response.Kvs),
		)
	}
	return Value{
		Revision: response.Header.Revision,
		Found:    true,
		Content:  append([]byte(nil), response.Kvs[0].Value...),
	}, nil
}

func (backend *sdkBackend) Watch(
	ctx context.Context,
	key string,
	fromRevision int64,
) <-chan Update {
	source := backend.client.Watch(
		ctx,
		key,
		clientv3.WithRev(fromRevision),
	)
	updates := make(chan Update)
	go func() {
		defer close(updates)
		for response := range source {
			if err := response.Err(); err != nil {
				select {
				case updates <- Update{
					Revision: response.Header.Revision,
					Err:      err,
				}:
				case <-ctx.Done():
				}
				return
			}
			for _, event := range response.Events {
				update := Update{
					Revision: event.Kv.ModRevision,
				}
				if update.Revision == 0 {
					update.Revision = response.Header.Revision
				}
				switch event.Type {
				case clientv3.EventTypePut:
					update.Content = append(
						[]byte(nil),
						event.Kv.Value...,
					)
				case clientv3.EventTypeDelete:
					update.Deleted = true
				default:
					update.Err = fmt.Errorf(
						"%w: event type %d",
						ErrInvalidDocument,
						event.Type,
					)
				}
				select {
				case updates <- update:
				case <-ctx.Done():
					return
				}
				if update.Err != nil {
					return
				}
			}
		}
	}()
	return updates
}

func (backend *sdkBackend) Close() error {
	return backend.client.Close()
}
