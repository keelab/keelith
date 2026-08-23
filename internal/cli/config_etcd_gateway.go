package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/keelab/keelith/config/versioned"
	"gopkg.in/yaml.v3"
)

const (
	etcdGatewayRecordVersion = 1
	etcdGatewayMaxResponse   = 4 * 1024 * 1024
)

type etcdGatewayStore struct {
	client    *http.Client
	transport *http.Transport
	endpoints []*url.URL
	prefix    string
	token     string
	next      atomic.Uint64

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

type etcdGatewayRevisionEnvelope struct {
	Version  int           `json:"version"`
	Revision core.Revision `json:"revision"`
	Content  []byte        `json:"content"`
}

type etcdGatewayActivationEnvelope struct {
	Version    int             `json:"version"`
	Activation core.Activation `json:"activation"`
}

type etcdGatewayHeader struct {
	Revision etcdJSONInt64 `json:"revision"`
}

type etcdGatewayKV struct {
	Key         string        `json:"key"`
	Value       string        `json:"value"`
	Version     etcdJSONInt64 `json:"version"`
	ModRevision etcdJSONInt64 `json:"modRevision"`
}

type etcdGatewayRangeRequest struct {
	Key        string `json:"key"`
	RangeEnd   string `json:"range_end,omitempty"`
	Limit      string `json:"limit,omitempty"`
	SortOrder  string `json:"sort_order,omitempty"`
	SortTarget string `json:"sort_target,omitempty"`
}

type etcdGatewayRangeResponse struct {
	Header *etcdGatewayHeader `json:"header"`
	KVs    []etcdGatewayKV    `json:"kvs"`
	Count  etcdJSONInt64      `json:"count"`
}

type etcdGatewayCompare struct {
	Key         string `json:"key"`
	Target      string `json:"target"`
	Result      string `json:"result"`
	Version     string `json:"version,omitempty"`
	ModRevision string `json:"mod_revision,omitempty"`
}

type etcdGatewayPutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type etcdGatewayRequestOp struct {
	Range *etcdGatewayRangeRequest `json:"request_range,omitempty"`
	Put   *etcdGatewayPutRequest   `json:"request_put,omitempty"`
}

type etcdGatewayResponseOp struct {
	Range *etcdGatewayRangeResponse `json:"responseRange,omitempty"`
}

type etcdGatewayTxnRequest struct {
	Compare []etcdGatewayCompare   `json:"compare"`
	Success []etcdGatewayRequestOp `json:"success"`
	Failure []etcdGatewayRequestOp `json:"failure,omitempty"`
}

type etcdGatewayTxnResponse struct {
	Header    *etcdGatewayHeader      `json:"header"`
	Succeeded bool                    `json:"succeeded"`
	Responses []etcdGatewayResponseOp `json:"responses"`
}

type etcdGatewayAuthRequest struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

type etcdGatewayAuthResponse struct {
	Token string `json:"token"`
}

type etcdJSONInt64 int64

func (value *etcdJSONInt64) UnmarshalJSON(content []byte) error {
	if value == nil {
		return errors.New("etcd gateway: nil integer target")
	}
	text := strings.TrimSpace(string(content))
	if len(text) >= 2 && text[0] == '"' && text[len(text)-1] == '"' {
		var decoded string
		if err := json.Unmarshal(content, &decoded); err != nil {
			return err
		}
		text = decoded
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("etcd gateway: invalid integer: %w", err)
	}
	*value = etcdJSONInt64(parsed)
	return nil
}

func openConfigStore(
	ctx context.Context,
	options configConnectionOptions,
) (core.Store, error) {
	if ctx == nil {
		return nil, errors.New("etcd gateway: context is nil")
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := validateConfigConnection(options); err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	if strings.HasPrefix(options.endpoints[0], "https://") {
		loaded, err := loadConfigTLS(options)
		if err != nil {
			return nil, err
		}
		tlsConfig = loaded
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   options.dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSClientConfig:       tlsConfig,
		TLSHandshakeTimeout:   options.dialTimeout,
		ResponseHeaderTimeout: options.dialTimeout,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
	}
	store := &etcdGatewayStore{
		client:    &http.Client{Transport: transport},
		transport: transport,
		endpoints: make([]*url.URL, 0, len(options.endpoints)),
		prefix:    strings.TrimSuffix(strings.TrimSpace(options.prefix), "/"),
	}
	for _, endpoint := range options.endpoints {
		parsed, err := url.Parse(strings.TrimSuffix(strings.TrimSpace(endpoint), "/"))
		if err != nil {
			transport.CloseIdleConnections()
			return nil, fmt.Errorf("etcd gateway: parse endpoint: %w", err)
		}
		store.endpoints = append(store.endpoints, parsed)
	}
	if options.passwordEnv != "" {
		password, found := os.LookupEnv(options.passwordEnv)
		if !found || password == "" {
			_ = store.Close()
			return nil, fmt.Errorf(
				"password environment variable %q is empty",
				options.passwordEnv,
			)
		}
		var response etcdGatewayAuthResponse
		if err := store.request(
			ctx,
			"/v3/auth/authenticate",
			etcdGatewayAuthRequest{Name: options.username, Password: password},
			&response,
			true,
			false,
		); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("etcd gateway: authenticate: %w", err)
		}
		if response.Token == "" {
			_ = store.Close()
			return nil, errors.New("etcd gateway: authenticate returned no token")
		}
		store.token = response.Token
	}
	return store, nil
}

func (store *etcdGatewayStore) Stage(
	ctx context.Context,
	request core.StageRequest,
) (core.Revision, error) {
	if err := store.require(ctx); err != nil {
		return core.Revision{}, err
	}
	request = request.Clone()
	if err := request.Validate(); err != nil {
		return core.Revision{}, err
	}
	if len(request.Content) > maxConfigFileBytes {
		return core.Revision{}, fmt.Errorf("config candidate exceeds %d bytes", maxConfigFileBytes)
	}
	if err := validateEtcdGatewayDocument(request.Content, request.Format); err != nil {
		return core.Revision{}, err
	}
	revision := core.Revision{
		ID:        core.RevisionID(request.Content),
		Format:    request.Format,
		Size:      len(request.Content),
		CreatedAt: time.Now().UTC(),
		Actor:     strings.TrimSpace(request.Actor),
		Message:   strings.TrimSpace(request.Message),
	}
	if err := revision.Validate(); err != nil {
		return core.Revision{}, err
	}
	payload, err := json.Marshal(etcdGatewayRevisionEnvelope{
		Version: etcdGatewayRecordVersion, Revision: revision,
		Content: request.Content,
	})
	if err != nil {
		return core.Revision{}, fmt.Errorf("etcd gateway: encode revision: %w", err)
	}
	key := store.revisionKey(revision.ID)
	txn := etcdGatewayTxnRequest{
		Compare: []etcdGatewayCompare{{
			Key: encodeEtcdBytes([]byte(key)), Target: "VERSION",
			Result: "EQUAL", Version: "0",
		}},
		Success: []etcdGatewayRequestOp{{Put: &etcdGatewayPutRequest{
			Key: encodeEtcdBytes([]byte(key)), Value: encodeEtcdBytes(payload),
		}}},
		Failure: []etcdGatewayRequestOp{{Range: &etcdGatewayRangeRequest{
			Key: encodeEtcdBytes([]byte(key)), Limit: "2",
		}}},
	}
	var response etcdGatewayTxnResponse
	if err := store.request(ctx, "/v3/kv/txn", txn, &response, false, true); err != nil {
		return core.Revision{}, fmt.Errorf("etcd gateway: stage: %w", err)
	}
	if response.Header == nil {
		return core.Revision{}, errors.New("etcd gateway: stage returned no header")
	}
	if response.Succeeded {
		return revision, nil
	}
	if len(response.Responses) != 1 || response.Responses[0].Range == nil ||
		len(response.Responses[0].Range.KVs) != 1 {
		return core.Revision{}, fmt.Errorf(
			"etcd gateway: stage conflict value disappeared (responses=%d)",
			len(response.Responses),
		)
	}
	existing, _, err := store.decodeRevision(
		response.Responses[0].Range.KVs[0],
		revision.ID,
	)
	return existing, err
}

func (store *etcdGatewayStore) Revision(
	ctx context.Context,
	id string,
) (core.Revision, []byte, error) {
	if err := store.require(ctx); err != nil {
		return core.Revision{}, nil, err
	}
	if !core.ValidRevisionID(id) {
		return core.Revision{}, nil, core.ErrInvalidRequest
	}
	values, err := store.rangeValues(ctx, etcdGatewayRangeRequest{
		Key: encodeEtcdBytes([]byte(store.revisionKey(id))), Limit: "2",
	})
	if err != nil {
		return core.Revision{}, nil, fmt.Errorf("etcd gateway: read revision: %w", err)
	}
	if len(values) == 0 {
		return core.Revision{}, nil, core.ErrNotFound
	}
	if len(values) != 1 {
		return core.Revision{}, nil, errors.New("etcd gateway: exact revision returned multiple values")
	}
	return store.decodeRevision(values[0], id)
}

func (store *etcdGatewayStore) Active(ctx context.Context) (core.Activation, error) {
	if err := store.require(ctx); err != nil {
		return core.Activation{}, err
	}
	activation, _, err := store.active(ctx)
	return activation, err
}

func (store *etcdGatewayStore) Activate(
	ctx context.Context,
	request core.ActivateRequest,
) (core.Activation, error) {
	if err := store.require(ctx); err != nil {
		return core.Activation{}, err
	}
	request.Actor = strings.TrimSpace(request.Actor)
	request.Reason = strings.TrimSpace(request.Reason)
	if err := request.Validate(); err != nil {
		return core.Activation{}, err
	}
	if _, _, err := store.Revision(ctx, request.Revision); err != nil {
		return core.Activation{}, err
	}
	current, currentModRevision, err := store.active(ctx)
	if err != nil && !errors.Is(err, core.ErrNoActive) {
		return core.Activation{}, err
	}
	if errors.Is(err, core.ErrNoActive) {
		current = core.Activation{}
		currentModRevision = 0
	}
	if current.Generation != request.ExpectedGeneration {
		return core.Activation{}, fmt.Errorf(
			"%w: expected generation %d, current %d",
			core.ErrConflict,
			request.ExpectedGeneration,
			current.Generation,
		)
	}
	if current.Revision == request.Revision {
		return current, nil
	}
	if current.Generation == math.MaxUint64 {
		return core.Activation{}, fmt.Errorf("%w: generation exhausted", core.ErrConflict)
	}
	activation := core.Activation{
		Generation:  current.Generation + 1,
		Revision:    request.Revision,
		Previous:    current.Revision,
		ActivatedAt: time.Now().UTC(),
		Actor:       request.Actor,
		Reason:      request.Reason,
	}
	if err := activation.Validate(); err != nil {
		return core.Activation{}, err
	}
	payload, err := json.Marshal(etcdGatewayActivationEnvelope{
		Version: etcdGatewayRecordVersion, Activation: activation,
	})
	if err != nil {
		return core.Activation{}, fmt.Errorf("etcd gateway: encode activation: %w", err)
	}
	revisionKey := store.revisionKey(request.Revision)
	activeKey := store.activeKey()
	historyKey := store.historyKey(activation.Generation)
	txn := etcdGatewayTxnRequest{
		Compare: []etcdGatewayCompare{
			{Key: encodeEtcdBytes([]byte(revisionKey)), Target: "VERSION", Result: "GREATER", Version: "0"},
			{Key: encodeEtcdBytes([]byte(activeKey)), Target: "MOD", Result: "EQUAL", ModRevision: strconv.FormatInt(currentModRevision, 10)},
			{Key: encodeEtcdBytes([]byte(historyKey)), Target: "VERSION", Result: "EQUAL", Version: "0"},
		},
		Success: []etcdGatewayRequestOp{
			{Put: &etcdGatewayPutRequest{Key: encodeEtcdBytes([]byte(activeKey)), Value: encodeEtcdBytes(payload)}},
			{Put: &etcdGatewayPutRequest{Key: encodeEtcdBytes([]byte(historyKey)), Value: encodeEtcdBytes(payload)}},
		},
	}
	var response etcdGatewayTxnResponse
	if err := store.request(ctx, "/v3/kv/txn", txn, &response, false, true); err != nil {
		return core.Activation{}, fmt.Errorf("etcd gateway: activate: %w", err)
	}
	if response.Header == nil {
		return core.Activation{}, errors.New("etcd gateway: activation returned no header")
	}
	if !response.Succeeded {
		return core.Activation{}, core.ErrConflict
	}
	return activation, nil
}

func (store *etcdGatewayStore) History(
	ctx context.Context,
	limit int,
) ([]core.Activation, error) {
	if err := store.require(ctx); err != nil {
		return nil, err
	}
	normalized, err := core.NormalizeHistoryLimit(limit)
	if err != nil {
		return nil, err
	}
	prefix := []byte(store.historyPrefix())
	values, err := store.rangeValues(ctx, etcdGatewayRangeRequest{
		Key:        encodeEtcdBytes(prefix),
		RangeEnd:   encodeEtcdBytes(etcdPrefixEnd(prefix)),
		Limit:      strconv.Itoa(normalized),
		SortOrder:  "DESCEND",
		SortTarget: "KEY",
	})
	if err != nil {
		return nil, fmt.Errorf("etcd gateway: history: %w", err)
	}
	history := make([]core.Activation, len(values))
	for index, value := range values {
		key, decodeErr := decodeEtcdBytes(value.Key)
		if decodeErr != nil ||
			!validEtcdHistoryKey(string(key), store.historyPrefix()) {
			return nil, core.ErrTampered
		}
		activation, err := decodeEtcdGatewayActivation(value)
		if err != nil {
			return nil, err
		}
		history[index] = activation
	}
	return history, nil
}

func (store *etcdGatewayStore) Close() error {
	if store == nil {
		return nil
	}
	store.closeOnce.Do(func() {
		store.mu.Lock()
		store.closed = true
		store.mu.Unlock()
		store.transport.CloseIdleConnections()
	})
	return nil
}

func (store *etcdGatewayStore) active(
	ctx context.Context,
) (core.Activation, int64, error) {
	values, err := store.rangeValues(ctx, etcdGatewayRangeRequest{
		Key: encodeEtcdBytes([]byte(store.activeKey())), Limit: "2",
	})
	if err != nil {
		return core.Activation{}, 0, fmt.Errorf("etcd gateway: read active: %w", err)
	}
	if len(values) == 0 {
		return core.Activation{}, 0, core.ErrNoActive
	}
	if len(values) != 1 {
		return core.Activation{}, 0, errors.New("etcd gateway: exact active key returned multiple values")
	}
	key, decodeErr := decodeEtcdBytes(values[0].Key)
	if decodeErr != nil || string(key) != store.activeKey() {
		return core.Activation{}, 0, core.ErrTampered
	}
	activation, err := decodeEtcdGatewayActivation(values[0])
	return activation, int64(values[0].ModRevision), err
}

func (store *etcdGatewayStore) rangeValues(
	ctx context.Context,
	request etcdGatewayRangeRequest,
) ([]etcdGatewayKV, error) {
	var response etcdGatewayRangeResponse
	if err := store.request(ctx, "/v3/kv/range", request, &response, true, true); err != nil {
		return nil, err
	}
	if response.Header == nil {
		return nil, errors.New("etcd gateway: range returned no header")
	}
	return response.KVs, nil
}

func (store *etcdGatewayStore) decodeRevision(
	value etcdGatewayKV,
	expectedID string,
) (core.Revision, []byte, error) {
	key, keyErr := decodeEtcdBytes(value.Key)
	if keyErr != nil || string(key) != store.revisionKey(expectedID) {
		return core.Revision{}, nil, core.ErrTampered
	}
	payload, err := decodeEtcdBytes(value.Value)
	if err != nil {
		return core.Revision{}, nil, core.ErrTampered
	}
	var envelope etcdGatewayRevisionEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil ||
		envelope.Version != etcdGatewayRecordVersion {
		return core.Revision{}, nil, core.ErrTampered
	}
	if err := envelope.Revision.Validate(); err != nil ||
		envelope.Revision.ID != expectedID ||
		envelope.Revision.Size != len(envelope.Content) ||
		core.RevisionID(envelope.Content) != envelope.Revision.ID ||
		len(envelope.Content) > maxConfigFileBytes ||
		validateEtcdGatewayDocument(envelope.Content, envelope.Revision.Format) != nil {
		return core.Revision{}, nil, core.ErrTampered
	}
	return envelope.Revision, append([]byte(nil), envelope.Content...), nil
}

func decodeEtcdGatewayActivation(value etcdGatewayKV) (core.Activation, error) {
	payload, err := decodeEtcdBytes(value.Value)
	if err != nil {
		return core.Activation{}, core.ErrTampered
	}
	var envelope etcdGatewayActivationEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil ||
		envelope.Version != etcdGatewayRecordVersion ||
		envelope.Activation.Validate() != nil {
		return core.Activation{}, core.ErrTampered
	}
	return envelope.Activation, nil
}

func (store *etcdGatewayStore) request(
	ctx context.Context,
	path string,
	input any,
	output any,
	retryReads bool,
	useToken bool,
) error {
	content, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	start := int(store.next.Add(1)-1) % len(store.endpoints)
	attempts := 1
	if retryReads {
		attempts = len(store.endpoints)
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		endpoint := store.endpoints[(start+attempt)%len(store.endpoints)]
		target := *endpoint
		target.Path = path
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			target.String(),
			bytes.NewReader(content),
		)
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		if useToken && store.token != "" {
			request.Header.Set("Authorization", store.token)
		}
		response, err := store.client.Do(request)
		if err != nil {
			lastErr = err
			continue
		}
		decodeErr := decodeEtcdGatewayResponse(response, output)
		if decodeErr == nil {
			return nil
		}
		lastErr = decodeErr
		if response.StatusCode < 500 {
			break
		}
	}
	return lastErr
}

func decodeEtcdGatewayResponse(response *http.Response, output any) (err error) {
	if response == nil {
		return errors.New("etcd gateway: nil HTTP response")
	}
	defer func() {
		err = errors.Join(err, response.Body.Close())
	}()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
		return fmt.Errorf("etcd gateway: HTTP status %d", response.StatusCode)
	}
	limited := io.LimitReader(response.Body, etcdGatewayMaxResponse+1)
	content, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("etcd gateway: read response: %w", err)
	}
	if len(content) > etcdGatewayMaxResponse {
		return errors.New("etcd gateway: response exceeds configured budget")
	}
	if err := json.Unmarshal(content, output); err != nil {
		return fmt.Errorf("etcd gateway: decode response: %w", err)
	}
	return nil
}

func (store *etcdGatewayStore) require(ctx context.Context) error {
	if store == nil || ctx == nil || store.client == nil ||
		store.transport == nil || len(store.endpoints) == 0 {
		return errors.New("etcd gateway: invalid store")
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return core.ErrClosed
	}
	return nil
}

func validateEtcdGatewayDocument(content []byte, format core.Format) error {
	var document map[string]any
	switch format {
	case core.FormatJSON:
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.UseNumber()
		if err := decoder.Decode(&document); err != nil {
			return fmt.Errorf("config candidate JSON is invalid: %w", err)
		}
		if err := requireEtcdGatewayEOF(decoder.Decode(new(any))); err != nil {
			return fmt.Errorf("config candidate JSON is invalid: %w", err)
		}
	case core.FormatYAML:
		decoder := yaml.NewDecoder(bytes.NewReader(content))
		if err := decoder.Decode(&document); err != nil {
			return fmt.Errorf("config candidate YAML is invalid: %w", err)
		}
		var extra any
		if err := requireEtcdGatewayEOF(decoder.Decode(&extra)); err != nil {
			return fmt.Errorf("config candidate YAML is invalid: %w", err)
		}
	default:
		return core.ErrInvalidRequest
	}
	if document == nil {
		return errors.New("config candidate root must be an object")
	}
	return nil
}

func requireEtcdGatewayEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple documents are not allowed")
	}
	return err
}

func encodeEtcdBytes(value []byte) string {
	return base64.StdEncoding.EncodeToString(value)
}

func decodeEtcdBytes(value string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(value)
}

func etcdPrefixEnd(prefix []byte) []byte {
	result := append([]byte(nil), prefix...)
	for index := len(result) - 1; index >= 0; index-- {
		if result[index] < 0xff {
			result[index]++
			return result[:index+1]
		}
	}
	return []byte{0}
}

func validEtcdHistoryKey(key string, prefix string) bool {
	if !strings.HasPrefix(key, prefix) || len(key) != len(prefix)+20 {
		return false
	}
	for _, r := range key[len(prefix):] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (store *etcdGatewayStore) revisionKey(id string) string {
	return store.prefix + "/revisions/" + id
}

func (store *etcdGatewayStore) activeKey() string {
	return store.prefix + "/active"
}

func (store *etcdGatewayStore) historyPrefix() string {
	return store.prefix + "/history/"
}

func (store *etcdGatewayStore) historyKey(generation uint64) string {
	return fmt.Sprintf("%s%020d", store.historyPrefix(), generation)
}

var _ core.Store = (*etcdGatewayStore)(nil)
