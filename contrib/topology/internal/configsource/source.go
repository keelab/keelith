// Package configsource bridges Keelith config sources to topology control.
package configsource

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/keelab/keelith/config"
	"github.com/keelab/keelith/programmable/topology/control"
)

type source struct{ backend config.Source }

type shutdown interface {
	Shutdown(context.Context) error
}

// New adapts a complete config document source to topology candidates.
func New(backend config.Source) (control.Source, error) {
	if isNil(backend) {
		return nil, control.ErrInvalidCandidate
	}
	return &source{backend: backend}, nil
}

func (source *source) Load(ctx context.Context) (control.Candidate, error) {
	snapshot, err := source.backend.Load(ctx)
	if err != nil {
		return control.Candidate{}, err
	}
	return candidate(snapshot)
}

func (source *source) Watch(ctx context.Context) (control.Watcher, error) {
	backend, err := source.backend.Watch(ctx)
	if err != nil {
		return nil, err
	}
	return &watcher{backend: backend}, nil
}

// Shutdown releases clients and watchers owned by the config backend.
func (source *source) Shutdown(ctx context.Context) error {
	backend, ok := source.backend.(shutdown)
	if !ok {
		return nil
	}
	return backend.Shutdown(ctx)
}

type watcher struct{ backend config.Watcher }

func (watcher *watcher) Next(ctx context.Context) (control.Candidate, error) {
	snapshot, err := watcher.backend.Next(ctx)
	if err != nil {
		return control.Candidate{}, err
	}
	return candidate(snapshot)
}

func (watcher *watcher) Close() error { return watcher.backend.Close() }

func candidate(snapshot config.Snapshot) (control.Candidate, error) {
	payload, err := json.Marshal(snapshot.Values())
	if err != nil {
		return control.Candidate{}, control.ErrInvalidCandidate
	}
	return control.ParseDocument(payload)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
