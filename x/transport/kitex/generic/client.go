package genericrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	kclient "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/generic"
	"github.com/cloudwego/kitex/pkg/remote/trans/gonet"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/transport"
	"github.com/keelab/keelith/operation"
	kkitex "github.com/keelab/x/transport/kitex"
)

// State is the bounded lifecycle state of a generic client.
type State string

const (
	StateNew      State = "new"
	StateReady    State = "ready"
	StateStopping State = "stopping"
	StateStopped  State = "stopped"
	StateFailed   State = "failed"
)

// Description is a low-sensitive, value-free runtime snapshot.
type Description struct {
	State             State
	Ready             bool
	Encrypted         bool
	MethodCount       int
	Active            int
	Calls             uint64
	Failures          uint64
	Rejected          uint64
	RequestOversized  uint64
	ResponseOversized uint64
}

type callerCloser interface {
	kclient.Client
	Close() error
}

// Client owns one immutable Proto JSON descriptor and one managed Kitex
// client. It implements app.Component structurally.
type Client struct {
	name         string
	dependencies []string
	methods      []string
	operations   map[string]operation.Operation
	serviceInfo  *serviceinfo.ServiceInfo
	client       callerCloser

	maxRequestBytes  int
	maxResponseBytes int
	encrypted        bool

	mu        sync.Mutex
	state     State
	active    int
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	calls             atomic.Uint64
	failures          atomic.Uint64
	rejected          atomic.Uint64
	requestOversized  atomic.Uint64
	responseOversized atomic.Uint64
}

// NewProtoJSONClient validates and freezes a gateway/debug generic contract.
//
// Construction parses the in-memory descriptor but performs no network I/O.
// Start enables calls; Stop rejects new calls and closes Kitex resources after
// active calls finish.
func NewProtoJSONClient(
	ctx context.Context,
	config Config,
	suite *kkitex.ClientSuite,
) (*Client, error) {
	if suite == nil {
		return nil, invalidConfig("Kitex client suite is nil")
	}
	validated, err := validateConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	provider, err := newProtoProvider(
		ctx,
		validated.mainPath,
		validated.mainContent,
		snapshotStringMap(validated.includes),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: Proto descriptor provider cannot be created",
			ErrInvalidConfig,
		)
	}
	codec, err := newProtoJSONCodec(provider)
	if err != nil {
		_ = provider.Close()
		return nil, fmt.Errorf(
			"%w: Proto JSON codec cannot be created",
			ErrInvalidConfig,
		)
	}
	serviceInfo := generic.ServiceInfoWithGeneric(codec)
	raw, err := kkitex.NewManagedClient[callerCloser](
		suite,
		func(guarded kclient.Suite) (callerCloser, error) {
			options := []kclient.Option{
				kclient.WithDestService(validated.service),
				kclient.WithHostPorts(validated.address),
				kclient.WithTransportProtocol(transport.TTHeader),
				kclient.WithConnectTimeout(validated.connectTimeout),
				kclient.WithCloseCallbacks(codec.Close),
				kclient.WithSuite(guarded),
			}
			if validated.encrypted {
				options = append(
					options,
					kclient.WithTransHandlerFactory(
						gonet.NewCliTransHandlerFactory(),
					),
					kclient.WithDialer(tlsTransportDialer(validated)),
				)
			}
			base, createErr := kclient.NewClient(serviceInfo, options...)
			if createErr != nil {
				return nil, createErr
			}
			managed, ok := base.(callerCloser)
			if !ok {
				return nil, errors.New(
					"kitex generic: client lifecycle is unavailable",
				)
			}
			return managed, nil
		},
	)
	if err != nil {
		_ = codec.Close()
		return nil, fmt.Errorf("kitex generic: create managed client: %w", err)
	}
	return &Client{
		name:             validated.name,
		dependencies:     validated.dependencies,
		methods:          validated.methods,
		operations:       validated.operations,
		serviceInfo:      serviceInfo,
		client:           raw,
		maxRequestBytes:  validated.maxRequestBytes,
		maxResponseBytes: validated.maxResponseBytes,
		encrypted:        validated.encrypted,
		state:            StateNew,
		closeDone:        make(chan struct{}),
	}, nil
}

// Name returns the stable App component name.
func (client *Client) Name() string {
	if client == nil {
		return ""
	}
	return client.name
}

// Dependencies returns an independent component dependency list.
func (client *Client) Dependencies() []string {
	if client == nil {
		return nil
	}
	return append([]string(nil), client.dependencies...)
}

// Start enables calls without dialing the remote service.
func (client *Client) Start(ctx context.Context) error {
	if client == nil {
		return invalidConfig("client is nil")
	}
	if ctx == nil {
		return invalidConfig("start context is nil")
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	switch client.state {
	case StateNew:
		client.state = StateReady
		return nil
	case StateReady:
		return ErrAlreadyStarted
	default:
		return ErrClosed
	}
}

// Stop rejects new calls and waits for owned resources to close.
//
// If ctx expires, resource closure continues once active calls finish.
func (client *Client) Stop(ctx context.Context) error {
	if client == nil {
		return nil
	}
	if ctx == nil {
		return invalidConfig("stop context is nil")
	}
	shouldClose := false
	client.mu.Lock()
	switch client.state {
	case StateNew, StateReady:
		client.state = StateStopping
		shouldClose = client.active == 0
	case StateStopping:
	case StateStopped, StateFailed:
		err := client.closeErr
		client.mu.Unlock()
		return err
	default:
		client.mu.Unlock()
		return ErrClosed
	}
	done := client.closeDone
	client.mu.Unlock()
	if shouldClose {
		client.closeResources()
	}
	select {
	case <-done:
		client.mu.Lock()
		err := client.closeErr
		client.mu.Unlock()
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Invoke calls one allowlisted unary method with a JSON object and returns an
// independently owned JSON response.
func (client *Client) Invoke(
	ctx context.Context,
	method string,
	request []byte,
) ([]byte, error) {
	if client == nil {
		return nil, invalidConfig("client is nil")
	}
	if ctx == nil {
		client.rejected.Add(1)
		return nil, invalidConfig("invoke context is nil")
	}
	if cause := context.Cause(ctx); cause != nil {
		client.rejected.Add(1)
		return nil, cause
	}
	target, allowed := client.operations[method]
	if !allowed {
		client.rejected.Add(1)
		return nil, ErrMethodNotAllowed
	}
	if len(request) > client.maxRequestBytes {
		client.rejected.Add(1)
		client.requestOversized.Add(1)
		return nil, ErrRequestTooLarge
	}
	if !validJSONObject(request) {
		client.rejected.Add(1)
		return nil, ErrInvalidJSON
	}
	if err := client.beginCall(); err != nil {
		client.rejected.Add(1)
		return nil, err
	}
	defer client.finishCall()
	client.calls.Add(1)

	methodInfo := client.serviceInfo.MethodInfo(ctx, method)
	if methodInfo == nil {
		client.failures.Add(1)
		return nil, ErrInvalidResponse
	}
	arguments, ok := methodInfo.NewArgs().(*generic.Args)
	if !ok {
		client.failures.Add(1)
		return nil, ErrInvalidResponse
	}
	result, ok := methodInfo.NewResult().(*generic.Result)
	if !ok {
		client.failures.Add(1)
		return nil, ErrInvalidResponse
	}
	arguments.Method = method
	arguments.Request = string(request)
	callContext := operation.WithContext(ctx, target)
	if err := client.client.Call(
		callContext,
		method,
		arguments,
		result,
	); err != nil {
		client.failures.Add(1)
		return nil, fmt.Errorf("kitex generic: invoke failed: %w", err)
	}
	response, ok := result.GetSuccess().(string)
	if !ok {
		client.failures.Add(1)
		return nil, ErrInvalidResponse
	}
	if len(response) > client.maxResponseBytes {
		client.failures.Add(1)
		client.responseOversized.Add(1)
		return nil, ErrResponseTooLarge
	}
	payload := []byte(response)
	if !validJSONObject(payload) {
		client.failures.Add(1)
		return nil, ErrInvalidResponse
	}
	return payload, nil
}

// Description returns a low-sensitive lifecycle and counter snapshot.
func (client *Client) Description() Description {
	if client == nil {
		return Description{}
	}
	client.mu.Lock()
	state := client.state
	active := client.active
	client.mu.Unlock()
	return Description{
		State:             state,
		Ready:             state == StateReady,
		Encrypted:         client.encrypted,
		MethodCount:       len(client.methods),
		Active:            active,
		Calls:             client.calls.Load(),
		Failures:          client.failures.Load(),
		Rejected:          client.rejected.Load(),
		RequestOversized:  client.requestOversized.Load(),
		ResponseOversized: client.responseOversized.Load(),
	}
}

func (client *Client) beginCall() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	switch client.state {
	case StateReady:
		client.active++
		return nil
	case StateNew:
		return ErrNotReady
	default:
		return ErrClosed
	}
}

func (client *Client) finishCall() {
	shouldClose := false
	client.mu.Lock()
	client.active--
	if client.active == 0 && client.state == StateStopping {
		shouldClose = true
	}
	client.mu.Unlock()
	if shouldClose {
		client.closeResources()
	}
}

func (client *Client) closeResources() {
	client.closeOnce.Do(func() {
		err := client.client.Close()
		client.mu.Lock()
		client.closeErr = err
		if err != nil {
			client.state = StateFailed
		} else {
			client.state = StateStopped
		}
		client.mu.Unlock()
		close(client.closeDone)
	})
}

func validJSONObject(payload []byte) bool {
	trimmed := strings.TrimSpace(string(payload))
	return len(trimmed) >= 2 &&
		trimmed[0] == '{' &&
		trimmed[len(trimmed)-1] == '}' &&
		json.Valid([]byte(trimmed))
}
