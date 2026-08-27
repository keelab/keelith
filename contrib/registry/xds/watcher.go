package xds

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/keelab/keelith/registry"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const nackMessage = "rejected eds response"

type watcher struct {
	client   *Client
	resource Resource
	context  context.Context
	cancel   context.CancelFunc
	stream   discoveryv3.AggregatedDiscoveryService_StreamAggregatedResourcesClient
	updates  chan registry.Snapshot
	done     chan struct{}

	mu          sync.Mutex
	terminal    error
	lastVersion string
	once        sync.Once
}

type receivedResponse struct {
	response *discoveryv3.DiscoveryResponse
	err      error
}

// Watch opens one explicit SotW ads v3 eds subscription.
func (client *Client) Watch(
	ctx context.Context,
	service string,
) (registry.Watcher, error) {
	if client == nil || ctx == nil {
		return nil, fmt.Errorf(
			"%w: client or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil, ErrClosed
	}
	resource, exists := client.resources[service]
	client.mu.Unlock()
	if !exists {
		return nil, fmt.Errorf(
			"%w: logical service is not configured",
			ErrInvalidOption,
		)
	}

	watchContext, cancel := context.WithCancel(ctx)
	stream, err := client.ads.StreamAggregatedResources(
		watchContext,
		grpc.MaxCallRecvMsgSize(client.maxResponseBytes),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("xds registry: open ads stream: %w", err)
	}
	if stream == nil {
		cancel()
		return nil, fmt.Errorf(
			"%w: ads client returned nil stream",
			ErrInvalidOption,
		)
	}
	if err := stream.Send(&discoveryv3.DiscoveryRequest{
		Node: &corev3.Node{
			Id:      client.node.GetId(),
			Cluster: client.node.GetCluster(),
		},
		ResourceNames: []string{resource.Cluster},
		TypeUrl:       EndpointTypeurl,
	}); err != nil {
		cancel()
		return nil, fmt.Errorf("xds registry: send initial request: %w", err)
	}

	current := &watcher{
		client:   client,
		resource: resource,
		context:  watchContext,
		cancel:   cancel,
		stream:   stream,
		updates:  make(chan registry.Snapshot, 1),
		done:     make(chan struct{}),
		terminal: registry.ErrWatcherClosed,
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	client.watchers[current] = struct{}{}
	client.mu.Unlock()
	go current.run()
	return current, nil
}

func (watcher *watcher) Next(ctx context.Context) (registry.Snapshot, error) {
	if ctx == nil {
		return registry.Snapshot{}, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidOption,
		)
	}
	select {
	case snapshot := <-watcher.updates:
		return snapshot.Clone(), nil
	default:
	}
	select {
	case snapshot := <-watcher.updates:
		return snapshot.Clone(), nil
	case <-watcher.done:
		select {
		case snapshot := <-watcher.updates:
			return snapshot.Clone(), nil
		default:
			return registry.Snapshot{}, watcher.terminalError()
		}
	case <-ctx.Done():
		return registry.Snapshot{}, context.Cause(ctx)
	}
}

func (watcher *watcher) Close() error {
	watcher.finish(registry.ErrWatcherClosed)
	return nil
}

func (watcher *watcher) run() {
	received := make(chan receivedResponse, 1)
	go watcher.receive(received)
	var staleTimer *time.Timer
	var stale <-chan time.Time
	defer func() {
		stopTimer(staleTimer)
	}()
	for {
		select {
		case <-watcher.context.Done():
			watcher.finish(context.Cause(watcher.context))
			return
		case <-stale:
			staleTimer = nil
			stale = nil
			snapshot, err := watcher.client.newSnapshot(
				watcher.resource,
				"stale:"+watcher.lastVersion,
				nil,
			)
			if err != nil {
				watcher.finish(err)
				return
			}
			watcher.client.recordExpiration()
			watcher.publish(snapshot)
			continue
		case result := <-received:
			if result.err != nil {
				if cause := context.Cause(watcher.context); cause != nil {
					watcher.finish(cause)
				} else {
					watcher.finish(fmt.Errorf(
						"%w: receive: %w",
						ErrWatchClosed,
						result.err,
					))
				}
				return
			}
			response := result.response
			projected, projectErr := watcher.client.projectResponse(
				response,
				watcher.resource,
			)
			if projectErr == nil && watcher.client.admission != nil {
				_, projectErr = watcher.client.admission.Update(
					watcher.resource.Service,
					projected.snapshot.Revision(),
					projected.dropPolicy,
				)
				if projectErr != nil {
					projectErr = fmt.Errorf(
						"%w: publish admission policy: %w",
						ErrInvalidResponse,
						projectErr,
					)
				}
			}
			if projectErr != nil {
				watcher.client.recordResponse(false)
				if err := watcher.stream.Send(watcher.request(
					response,
					&statuspb.Status{
						Code:    int32(codes.InvalidArgument),
						Message: nackMessage,
					},
				)); err != nil {
					watcher.finish(fmt.Errorf("%w: send NACK: %w", ErrWatchClosed, err))
					return
				}
				continue
			}

			watcher.lastVersion = response.GetVersionInfo()
			watcher.client.recordResponse(true)
			watcher.publish(projected.snapshot)
			if err := watcher.stream.Send(watcher.request(response, nil)); err != nil {
				watcher.finish(fmt.Errorf("%w: send ACK: %w", ErrWatchClosed, err))
				return
			}
			staleTimer, stale = resetTimer(staleTimer, projected.staleAfter)
		}
	}
}

func (watcher *watcher) receive(output chan<- receivedResponse) {
	for {
		response, err := watcher.stream.Recv()
		select {
		case output <- receivedResponse{response: response, err: err}:
		case <-watcher.context.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func resetTimer(
	current *time.Timer,
	duration time.Duration,
) (*time.Timer, <-chan time.Time) {
	stopTimer(current)
	if duration <= 0 {
		return nil, nil
	}
	next := time.NewTimer(duration)
	return next, next.C
}

func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (watcher *watcher) request(
	response *discoveryv3.DiscoveryResponse,
	detail *statuspb.Status,
) *discoveryv3.DiscoveryRequest {
	return &discoveryv3.DiscoveryRequest{
		VersionInfo:   watcher.lastVersion,
		ResourceNames: []string{watcher.resource.Cluster},
		TypeUrl:       EndpointTypeurl,
		ResponseNonce: response.GetNonce(),
		ErrorDetail:   detail,
	}
}

func (watcher *watcher) publish(snapshot registry.Snapshot) {
	select {
	case <-watcher.done:
		return
	default:
	}
	select {
	case <-watcher.updates:
	default:
	}
	select {
	case watcher.updates <- snapshot.Clone():
	case <-watcher.done:
	default:
	}
}

func (watcher *watcher) finish(err error) {
	if err == nil {
		err = ErrWatchClosed
	}
	watcher.once.Do(func() {
		watcher.mu.Lock()
		watcher.terminal = err
		watcher.mu.Unlock()
		watcher.client.removeWatcher(watcher)
		close(watcher.done)
		watcher.cancel()
	})
}

func (watcher *watcher) terminalError() error {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if watcher.terminal == nil {
		return registry.ErrWatcherClosed
	}
	return watcher.terminal
}
