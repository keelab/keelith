package operator

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	topologyv1alpha1 "github.com/keelab/operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

const (
	defaultControllerMinBackoff = 100 * time.Millisecond
	defaultControllerMaxBackoff = 5 * time.Second
)

// ErrorHandler receives reconciliation/watch classes for logging. It must not
// mutate the referenced Kubernetes object because none is exposed.
type ErrorHandler func(error)

// ControllerConfig runs a bounded List/Watch loop for one namespace.
type ControllerConfig struct {
	Dynamic    dynamic.Interface
	Reconciler *Reconciler
	Namespace  string
	MinBackoff time.Duration
	MaxBackoff time.Duration
	Ready      func()
	OnError    ErrorHandler
}

// RunController lists existing revisions, then watches from the returned
// resourceVersion. Every reconnect performs another List, closing handoff gaps.
func RunController(ctx context.Context, config ControllerConfig) error {
	if ctx == nil || config.Dynamic == nil || config.Reconciler == nil ||
		len(validation.IsDNS1123Label(config.Namespace)) != 0 {
		return ErrInvalidConfig
	}
	if config.MinBackoff == 0 {
		config.MinBackoff = defaultControllerMinBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaultControllerMaxBackoff
	}
	if config.MinBackoff <= 0 || config.MaxBackoff < config.MinBackoff ||
		config.MaxBackoff > time.Minute {
		return ErrInvalidConfig
	}
	resource := config.Dynamic.Resource(
		topologyv1alpha1.GroupVersionResource,
	).Namespace(config.Namespace)
	backoff := config.MinBackoff
	var readyOnce sync.Once
	for {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		list, err := resource.List(ctx, metav1.ListOptions{})
		if err != nil {
			reportControllerError(config.OnError, err)
			if err := controllerSleep(ctx, backoff); err != nil {
				return err
			}
			backoff = controllerNextBackoff(backoff, config.MaxBackoff)
			continue
		}
		sort.Slice(list.Items, func(first, second int) bool {
			return list.Items[first].GetName() < list.Items[second].GetName()
		})
		for index := range list.Items {
			_, reconcileErr := config.Reconciler.Reconcile(
				ctx,
				config.Namespace,
				list.Items[index].GetName(),
			)
			reportControllerError(config.OnError, reconcileErr)
		}
		readyOnce.Do(func() {
			if config.Ready != nil {
				config.Ready()
			}
		})
		timeoutSeconds := int64(300)
		watcher, err := resource.Watch(ctx, metav1.ListOptions{
			ResourceVersion: list.GetResourceVersion(),
			TimeoutSeconds:  &timeoutSeconds,
		})
		if err != nil {
			reportControllerError(config.OnError, err)
			if err := controllerSleep(ctx, backoff); err != nil {
				return err
			}
			backoff = controllerNextBackoff(backoff, config.MaxBackoff)
			continue
		}
		backoff = config.MinBackoff
		err = consumeWatch(ctx, watcher, config)
		watcher.Stop()
		if err != nil && !errors.Is(err, context.Canceled) {
			reportControllerError(config.OnError, err)
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if err := controllerSleep(ctx, backoff); err != nil {
			return err
		}
		backoff = controllerNextBackoff(backoff, config.MaxBackoff)
	}
}

func consumeWatch(
	ctx context.Context,
	watcher watch.Interface,
	config ControllerConfig,
) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case event, open := <-watcher.ResultChan():
			if !open {
				return errors.New("topology operator: watch closed")
			}
			if event.Type == watch.Error {
				return errors.New("topology operator: watch error")
			}
			object, ok := event.Object.(*unstructured.Unstructured)
			if !ok || object.GetName() == "" {
				reportControllerError(
					config.OnError,
					ErrInvalidRevision,
				)
				continue
			}
			_, err := config.Reconciler.Reconcile(
				ctx,
				config.Namespace,
				object.GetName(),
			)
			reportControllerError(config.OnError, err)
		}
	}
}

func reportControllerError(handler ErrorHandler, err error) {
	if handler != nil && err != nil {
		handler(err)
	}
}

func controllerSleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func controllerNextBackoff(current time.Duration, maximum time.Duration) time.Duration {
	if current >= maximum-current {
		return maximum
	}
	return min(current*2, maximum)
}
