package kitex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/discovery"
	kitexerrors "github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/governance/failure"
	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/selector"
)

var (
	// ErrInvalidEndpoint reports a selected endpoint outside the Kitex
	// profile's canonical kitex://host:port contract.
	ErrInvalidEndpoint = errors.New(
		"kitex profile: invalid selected endpoint",
	)
	// ErrInvalidPickerResult reports an incomplete Picker result.
	ErrInvalidPickerResult = errors.New(
		"kitex profile: invalid picker result",
	)
)

type selectionContextKey struct{}

type selectionState struct {
	mu      sync.Mutex
	started time.Time
	done    selector.Done
	once    sync.Once
}

func withSelectionState(
	ctx context.Context,
	state *selectionState,
) context.Context {
	return context.WithValue(ctx, selectionContextKey{}, state)
}

func selectionStateFromContext(ctx context.Context) *selectionState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(selectionContextKey{}).(*selectionState)
	return state
}

func (state *selectionState) bind(done selector.Done) error {
	if state == nil {
		return fmt.Errorf("%w: state is nil", ErrInvalidPickerResult)
	}
	if done == nil {
		return fmt.Errorf("%w: completion callback is nil", ErrInvalidPickerResult)
	}
	state.mu.Lock()
	state.started = time.Now()
	state.done = done
	state.mu.Unlock()
	return nil
}

func (state *selectionState) finish(
	ctx context.Context,
	err error,
) {
	if state == nil {
		return
	}
	state.once.Do(func() {
		state.mu.Lock()
		started := state.started
		done := state.done
		state.mu.Unlock()
		if done == nil {
			return
		}
		feedbackErr := kitexFeedbackFailure(err)
		done(selector.Result{
			Latency:  time.Since(started),
			Error:    feedbackErr,
			Canceled: errors.Is(feedbackErr, context.Canceled),
			Retried:  operation.AttemptFromContext(ctx) > 1,
		})
	})
}

func selectClientNode(
	ctx context.Context,
	target operation.Operation,
	picker kclient.Picker,
) (context.Context, *selectionState, error) {
	state := selectionStateFromContext(ctx)
	if state == nil {
		state = &selectionState{}
		ctx = withSelectionState(ctx, state)
	}
	node, done, err := picker.Pick(ctx, target)
	if err != nil {
		return ctx, state, kitexDependencyFailure(err)
	}
	if err := state.bind(done); err != nil {
		return ctx, state, err
	}
	instance, err := selectedKitexInstance(node)
	if err != nil {
		return ctx, state, failure.MarkTransport(err)
	}
	rpc := rpcinfo.GetRPCInfo(ctx)
	if rpc == nil || rpc.To() == nil {
		return ctx, state, failure.MarkTransport(fmt.Errorf(
			"%w: destination RPCInfo is missing",
			ErrInvalidPickerResult,
		))
	}
	remote := remoteinfo.AsRemoteInfo(rpc.To())
	if remote == nil {
		return ctx, state, failure.MarkTransport(fmt.Errorf(
			"%w: destination %T is not mutable",
			ErrInvalidPickerResult,
			rpc.To(),
		))
	}
	remote.SetInstance(instance)
	return ctx, state, nil
}

func selectedKitexInstance(node selector.Node) (discovery.Instance, error) {
	endpoint, err := url.Parse(node.Endpoint())
	if err != nil ||
		endpoint.Scheme != "kitex" ||
		endpoint.Host == "" ||
		endpoint.User != nil ||
		endpoint.Path != "" ||
		endpoint.RawPath != "" ||
		endpoint.RawQuery != "" ||
		endpoint.Fragment != "" ||
		endpoint.Opaque != "" {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrInvalidEndpoint,
			node.Endpoint(),
		)
	}
	host, portText, err := net.SplitHostPort(endpoint.Host)
	if err != nil || strings.TrimSpace(host) == "" {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrInvalidEndpoint,
			node.Endpoint(),
		)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrInvalidEndpoint,
			node.Endpoint(),
		)
	}
	tags := map[string]string{
		"keelith.node.id":      node.ID(),
		"keelith.node.service": node.Service(),
	}
	return discovery.NewInstance(
		"tcp",
		endpoint.Host,
		discovery.DefaultWeight,
		tags,
	), nil
}

func kitexDependencyFailure(err error) error {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return failure.MarkTransport(err)
}

func kitexFeedbackFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, kitexerrors.ErrCanceledByBusiness) ||
		errors.Is(err, kitexerrors.ErrStreamingCanceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		kitexerrors.IsTimeoutError(err) ||
		errors.Is(err, kitexerrors.ErrStreamingTimeout) {
		return context.DeadlineExceeded
	}
	switch {
	case errors.Is(err, kitexerrors.ErrServiceDiscovery),
		errors.Is(err, kitexerrors.ErrGetConnection),
		errors.Is(err, kitexerrors.ErrLoadbalance),
		errors.Is(err, kitexerrors.ErrNoMoreInstance),
		errors.Is(err, kitexerrors.ErrRemoteOrNetwork),
		errors.Is(err, kitexerrors.ErrNoConnection),
		errors.Is(err, kitexerrors.ErrNoDestAddress),
		errors.Is(err, kitexerrors.ErrStreamingProtocol):
		return failure.MarkTransport(err)
	default:
		return err
	}
}

func isNilClientPicker(picker kclient.Picker) bool {
	if picker == nil {
		return true
	}
	value := reflect.ValueOf(picker)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
