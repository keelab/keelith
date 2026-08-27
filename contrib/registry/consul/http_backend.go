package consul

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/keelab/keelith/secret"
)

// TokenSource resolves a consul acl token at request time. Implementations
// should use Keelith SecretProvider rather than configuration plaintext.
type TokenSource func(context.Context) (string, error)

// ClientOption configures consul http authentication.
type ClientOption interface {
	applyClient(*httpOptions) error
}

type clientOptionFunc func(*httpOptions) error

func (function clientOptionFunc) applyClient(options *httpOptions) error {
	return function(options)
}

type httpOptions struct {
	tokenSource TokenSource
}

// WithTokenSource injects a request-scoped acl token resolver.
func WithTokenSource(source TokenSource) ClientOption {
	return clientOptionFunc(func(options *httpOptions) error {
		if source == nil {
			return fmt.Errorf("token source is nil")
		}
		options.tokenSource = source
		return nil
	})
}

// WithSecretToken resolves the acl token through a Keelith Secret Manager on
// every request so rotation does not require rebuilding the consul client.
func WithSecretToken(
	manager *secret.Manager,
	reference secret.Reference,
) ClientOption {
	return clientOptionFunc(func(options *httpOptions) error {
		if manager == nil || reference.Provider() == "" ||
			reference.Key() == "" {
			return fmt.Errorf("secret manager or reference is invalid")
		}
		options.tokenSource = func(ctx context.Context) (string, error) {
			value, err := manager.Resolve(ctx, reference)
			if err != nil {
				return "", err
			}
			token := value.Bytes()
			defer clear(token)
			return string(secret.TrimLineBreaks(token)), nil
		}
		return nil
	})
}

// New constructs a Client using consul's Agent and Health http APIs.
func New(
	httpClient *http.Client,
	options Options,
	optionList ...ClientOption,
) (*Client, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("%w: http client is nil", ErrInvalidOption)
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	settings := httpOptions{}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf(
				"%w: client option %d is nil",
				ErrInvalidOption,
				index,
			)
		}
		if err := option.applyClient(&settings); err != nil {
			return nil, fmt.Errorf(
				"%w: client option %d: %w",
				ErrInvalidOption,
				index,
				err,
			)
		}
	}
	backend := &httpBackend{
		client:           httpClient,
		address:          normalized.Address,
		maxResponseBytes: normalized.MaxResponseBytes,
		tokenSource:      settings.tokenSource,
	}
	return Wrap(backend, normalized)
}

type httpBackend struct {
	client           *http.Client
	address          string
	maxResponseBytes int64
	tokenSource      TokenSource
}

func (backend *httpBackend) Register(
	ctx context.Context,
	registration Registration,
) error {
	payload, err := json.Marshal(struct {
		ID      string            `json:"id"`
		Name    string            `json:"Name"`
		Address string            `json:"Address"`
		Port    int               `json:"Port"`
		Meta    map[string]string `json:"Meta,omitempty"`
		Check   struct {
			TTL                            string `json:"ttl"`
			DeregisterCriticalServiceAfter string `json:"DeregisterCriticalServiceAfter"`
		} `json:"Check"`
	}{
		ID:      registration.ID,
		Name:    registration.Service,
		Address: registration.Address,
		Port:    registration.Port,
		Meta:    registration.Meta,
		Check: struct {
			TTL                            string `json:"ttl"`
			DeregisterCriticalServiceAfter string `json:"DeregisterCriticalServiceAfter"`
		}{
			TTL: registration.TTL.String(),
			DeregisterCriticalServiceAfter: max(
				3*registration.TTL,
				time.Minute,
			).String(),
		},
	})
	if err != nil {
		return fmt.Errorf("encode registration: %w", err)
	}
	if int64(len(payload)) > backend.maxResponseBytes {
		return fmt.Errorf("%w: registration exceeds budget", ErrInvalidRecord)
	}
	return backend.do(
		ctx,
		http.MethodPut,
		"/v1/agent/service/register",
		nil,
		payload,
		nil,
	)
}

func (backend *httpBackend) Deregister(
	ctx context.Context,
	id string,
) error {
	return backend.do(
		ctx,
		http.MethodPut,
		"/v1/agent/service/deregister/"+url.PathEscape(id),
		nil,
		nil,
		nil,
	)
}

func (backend *httpBackend) Pass(
	ctx context.Context,
	id string,
) error {
	return backend.do(
		ctx,
		http.MethodPut,
		"/v1/agent/check/pass/service:"+url.PathEscape(id),
		nil,
		nil,
		nil,
	)
}

func (backend *httpBackend) List(
	ctx context.Context,
	service string,
	datacenter string,
	revision string,
	wait time.Duration,
) ([]BackendInstance, string, error) {
	query := make(url.Values)
	query.Set("passing", "true")
	if datacenter != "" {
		query.Set("dc", datacenter)
	}
	if revision != "" {
		if _, err := strconv.ParseUint(revision, 10, 64); err != nil {
			return nil, "", fmt.Errorf(
				"%w: invalid consul index",
				ErrInvalidRecord,
			)
		}
		query.Set("index", revision)
	}
	if wait > 0 {
		query.Set("wait", wait.String())
	}
	var response []struct {
		Node struct {
			Address string `json:"Address"`
		} `json:"Node"`
		Service struct {
			ID      string            `json:"id"`
			Service string            `json:"Service"`
			Address string            `json:"Address"`
			Port    int               `json:"Port"`
			Meta    map[string]string `json:"Meta"`
		} `json:"Service"`
	}
	headers := make(http.Header)
	if err := backend.do(
		ctx,
		http.MethodGet,
		"/v1/health/service/"+url.PathEscape(service),
		query,
		nil,
		func(source http.Header, body io.Reader) error {
			headers = source.Clone()
			decoder := json.NewDecoder(body)
			if err := decoder.Decode(&response); err != nil {
				return err
			}
			var trailing any
			if err := decoder.Decode(&trailing); err != io.EOF {
				if err == nil {
					return fmt.Errorf("unexpected trailing json value")
				}
				return err
			}
			return nil
		},
	); err != nil {
		return nil, "", err
	}
	index := strings.TrimSpace(headers.Get("X-consul-Index"))
	if _, err := strconv.ParseUint(index, 10, 64); err != nil {
		return nil, "", fmt.Errorf(
			"%w: missing or invalid X-consul-Index",
			ErrInvalidRecord,
		)
	}
	result := make([]BackendInstance, 0, len(response))
	for _, entry := range response {
		address := entry.Service.Address
		if address == "" {
			address = entry.Node.Address
		}
		result = append(result, BackendInstance{
			ID:      entry.Service.ID,
			Service: entry.Service.Service,
			Address: address,
			Port:    entry.Service.Port,
			Meta:    cloneMetadata(entry.Service.Meta),
		})
	}
	return result, index, nil
}

func (backend *httpBackend) Close() error {
	backend.client.CloseIdleConnections()
	return nil
}

func (backend *httpBackend) do(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	payload []byte,
	decode func(http.Header, io.Reader) error,
) error {
	target := backend.address + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(
		ctx,
		method,
		target,
		bytes.NewReader(payload),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if backend.tokenSource != nil {
		token, err := backend.tokenSource(ctx)
		if err != nil {
			return fmt.Errorf("resolve acl token: %w", err)
		}
		token = strings.TrimSpace(token)
		if token == "" || strings.ContainsAny(token, "\r\n\x00") {
			return fmt.Errorf("%w: acl token is malformed", ErrInvalidOption)
		}
		request.Header.Set("X-consul-Token", token)
	}
	response, err := backend.client.Do(request)
	if err != nil {
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(
			io.Discard,
			io.LimitReader(response.Body, 4*1024),
		)
		return fmt.Errorf(
			"consul http status %d",
			response.StatusCode,
		)
	}
	if decode == nil {
		body, err := io.ReadAll(
			io.LimitReader(response.Body, backend.maxResponseBytes+1),
		)
		if err != nil {
			return err
		}
		if int64(len(body)) > backend.maxResponseBytes {
			return fmt.Errorf("%w: response exceeds budget", ErrInvalidRecord)
		}
		return nil
	}
	body, err := io.ReadAll(
		io.LimitReader(response.Body, backend.maxResponseBytes+1),
	)
	if err != nil {
		return err
	}
	if int64(len(body)) > backend.maxResponseBytes {
		return fmt.Errorf("%w: response exceeds budget", ErrInvalidRecord)
	}
	return decode(response.Header, bytes.NewReader(body))
}
