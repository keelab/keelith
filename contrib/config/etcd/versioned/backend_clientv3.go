package etcdversioned

import (
	"context"
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type sdkBackend struct {
	client *clientv3.Client
}

func (backend *sdkBackend) Get(
	ctx context.Context,
	key string,
) (KV, bool, error) {
	response, err := backend.client.Get(ctx, key)
	if err != nil {
		return KV{}, false, err
	}
	if response == nil || response.Header == nil {
		return KV{}, false, errors.New("versioned etcd config: get returned no revision")
	}
	if len(response.Kvs) == 0 {
		return KV{}, false, nil
	}
	if len(response.Kvs) != 1 {
		return KV{}, false, fmt.Errorf("versioned etcd config: exact key returned %d values", len(response.Kvs))
	}
	return sdkKV(response.Kvs[0]), true, nil
}

func (backend *sdkBackend) Create(
	ctx context.Context,
	key string,
	value []byte,
) (KV, bool, error) {
	response, err := backend.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return KV{}, false, err
	}
	if response == nil || response.Header == nil {
		return KV{}, false, errors.New("versioned etcd config: create returned no revision")
	}
	if response.Succeeded {
		return KV{
			Key:         key,
			ModRevision: response.Header.Revision,
			Value:       append([]byte(nil), value...),
		}, true, nil
	}
	if len(response.Responses) != 1 {
		return KV{}, false, errors.New("versioned etcd config: create conflict returned no value")
	}
	rangeResponse := response.Responses[0].GetResponseRange()
	if rangeResponse == nil || len(rangeResponse.Kvs) != 1 {
		return KV{}, false, errors.New("versioned etcd config: create conflict value disappeared")
	}
	return sdkKV(rangeResponse.Kvs[0]), false, nil
}

func (backend *sdkBackend) CommitActivation(
	ctx context.Context,
	revisionKey string,
	activeKey string,
	expectedModRevision int64,
	activeValue []byte,
	historyKey string,
	historyValue []byte,
) (int64, bool, error) {
	response, err := backend.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Version(revisionKey), ">", 0),
			clientv3.Compare(clientv3.ModRevision(activeKey), "=", expectedModRevision),
			clientv3.Compare(clientv3.Version(historyKey), "=", 0),
		).
		Then(
			clientv3.OpPut(activeKey, string(activeValue)),
			clientv3.OpPut(historyKey, string(historyValue)),
		).
		Commit()
	if err != nil {
		return 0, false, err
	}
	if response == nil || response.Header == nil {
		return 0, false, errors.New("versioned etcd config: activation returned no revision")
	}
	return response.Header.Revision, response.Succeeded, nil
}

func (backend *sdkBackend) List(
	ctx context.Context,
	prefix string,
	limit int,
) ([]KV, error) {
	response, err := backend.client.Get(
		ctx,
		prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
		clientv3.WithLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	if response == nil || response.Header == nil {
		return nil, errors.New("versioned etcd config: history returned no revision")
	}
	values := make([]KV, len(response.Kvs))
	for index, value := range response.Kvs {
		values[index] = sdkKV(value)
	}
	return values, nil
}

func (backend *sdkBackend) Watch(
	ctx context.Context,
	key string,
	fromRevision int64,
) <-chan Event {
	source := backend.client.Watch(ctx, key, clientv3.WithRev(fromRevision))
	updates := make(chan Event)
	go func() {
		defer close(updates)
		for response := range source {
			if err := response.Err(); err != nil {
				sendEvent(ctx, updates, Event{ModRevision: response.Header.Revision, Err: err})
				return
			}
			for _, event := range response.Events {
				update := Event{ModRevision: event.Kv.ModRevision}
				if update.ModRevision == 0 {
					update.ModRevision = response.Header.Revision
				}
				switch event.Type {
				case clientv3.EventTypePut:
					update.Value = append([]byte(nil), event.Kv.Value...)
				case clientv3.EventTypeDelete:
					update.Deleted = true
				default:
					update.Err = fmt.Errorf("versioned etcd config: unsupported event type %d", event.Type)
				}
				if !sendEvent(ctx, updates, update) || update.Err != nil {
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

func sdkKV(value *mvccpb.KeyValue) KV {
	return KV{
		Key:         string(value.Key),
		ModRevision: value.ModRevision,
		Value:       append([]byte(nil), value.Value...),
	}
}

func sendEvent(ctx context.Context, target chan<- Event, event Event) bool {
	select {
	case target <- event:
		return true
	case <-ctx.Done():
		return false
	}
}
