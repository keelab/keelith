package kafka

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/keelab/keelith/metadata"
	"go.opentelemetry.io/otel/propagation"
)

const (
	defaultMaxPropagationHeaders = 64
	defaultMaxPropagationBytes   = 16 * 1024
	maxPropagationHeaders        = 256
	maxPropagationBytes          = 1024 * 1024
	maxPropagationHeaderKeyBytes = 128
)

// PropagationConfig controls bounded Kafka metadata and W3C context
// propagation. A zero MetadataPolicy denies automatic business metadata
// propagation. A nil Propagator disables trace/baggage propagation.
type PropagationConfig struct {
	MetadataPolicy metadata.Policy
	Propagator     propagation.TextMapPropagator
	MaxHeaders     int
	MaxBytes       int
}

type propagationSettings struct {
	metadataPolicy metadata.Policy
	propagator     propagation.TextMapPropagator
	maxHeaders     int
	maxBytes       int
}

func normalizePropagation(
	config PropagationConfig,
) (propagationSettings, error) {
	maxHeaders := config.MaxHeaders
	if maxHeaders == 0 {
		maxHeaders = defaultMaxPropagationHeaders
	}
	maxBytes := config.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxPropagationBytes
	}
	if maxHeaders < 1 || maxHeaders > maxPropagationHeaders {
		return propagationSettings{}, fmt.Errorf(
			"%w: propagation header count is outside supported bounds",
			ErrInvalidOption,
		)
	}
	if maxBytes < 256 || maxBytes > maxPropagationBytes {
		return propagationSettings{}, fmt.Errorf(
			"%w: propagation byte budget is outside supported bounds",
			ErrInvalidOption,
		)
	}
	if isNilPropagator(config.Propagator) {
		return propagationSettings{}, fmt.Errorf(
			"%w: propagator is typed nil",
			ErrInvalidOption,
		)
	}
	return propagationSettings{
		metadataPolicy: config.MetadataPolicy,
		propagator:     config.Propagator,
		maxHeaders:     maxHeaders,
		maxBytes:       maxBytes,
	}, nil
}

func isNilPropagator(value propagation.TextMapPropagator) bool {
	if value == nil {
		return false
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

type headerState struct {
	values     map[string][]string
	maxHeaders int
	maxBytes   int
}

func newHeaderState(
	headers []Header,
	settings propagationSettings,
) (*headerState, error) {
	state := &headerState{
		values:     make(map[string][]string),
		maxHeaders: settings.maxHeaders,
		maxBytes:   settings.maxBytes,
	}
	for _, header := range headers {
		key, err := normalizeHeaderKey(header.Key)
		if err != nil {
			return nil, err
		}
		state.values[key] = append(
			state.values[key],
			string(append([]byte(nil), header.Value...)),
		)
	}
	if err := state.validate(); err != nil {
		return nil, err
	}
	return state, nil
}

func normalizeHeaderKey(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	normalized := strings.ToLower(trimmed)
	if normalized == "" ||
		trimmed != value ||
		len(normalized) > maxPropagationHeaderKeyBytes {
		return "", fmt.Errorf("%w: invalid propagation header key", ErrInvalidOption)
	}
	for _, character := range normalized {
		valid := character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' ||
			character == '_' ||
			character == '.'
		if !valid {
			return "", fmt.Errorf(
				"%w: invalid propagation header key",
				ErrInvalidOption,
			)
		}
	}
	return normalized, nil
}

func (state *headerState) validate() error {
	count := 0
	size := 0
	for key, values := range state.values {
		for _, value := range values {
			count++
			if count > state.maxHeaders ||
				len(key) > state.maxBytes-size {
				return fmt.Errorf(
					"%w: propagation headers exceed configured budget",
					ErrInvalidOption,
				)
			}
			size += len(key)
			if len(value) > state.maxBytes-size {
				return fmt.Errorf(
					"%w: propagation headers exceed configured budget",
					ErrInvalidOption,
				)
			}
			size += len(value)
		}
	}
	return nil
}

func (state *headerState) headers() ([]Header, error) {
	if err := state.validate(); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(state.values))
	for key := range state.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Header, 0)
	for _, key := range keys {
		for _, value := range state.values[key] {
			result = append(result, Header{
				Key:   key,
				Value: []byte(value),
			})
		}
	}
	return result, nil
}

type metadataHeaderCarrier struct{ state *headerState }

func (carrier metadataHeaderCarrier) Values(key string) []string {
	normalized, err := normalizeHeaderKey(key)
	if err != nil || carrier.state == nil {
		return nil
	}
	return append([]string(nil), carrier.state.values[normalized]...)
}

func (carrier metadataHeaderCarrier) Set(key string, values []string) {
	normalized, err := normalizeHeaderKey(key)
	if err != nil || carrier.state == nil {
		return
	}
	carrier.state.values[normalized] = append([]string(nil), values...)
}

type traceHeaderCarrier struct{ state *headerState }

func (carrier traceHeaderCarrier) Get(key string) string {
	normalized, err := normalizeHeaderKey(key)
	if err != nil || carrier.state == nil {
		return ""
	}
	values := carrier.state.values[normalized]
	if len(values) != 1 ||
		!utf8.ValidString(values[0]) ||
		strings.ContainsAny(values[0], "\r\n\x00") {
		return ""
	}
	return values[0]
}

func (carrier traceHeaderCarrier) Set(key string, value string) {
	normalized, err := normalizeHeaderKey(key)
	if err != nil || carrier.state == nil {
		return
	}
	carrier.state.values[normalized] = []string{value}
}

func (carrier traceHeaderCarrier) Keys() []string {
	if carrier.state == nil {
		return nil
	}
	keys := make([]string, 0, len(carrier.state.values))
	for key := range carrier.state.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func prepareMessageHeaders(
	ctx context.Context,
	headers []Header,
	settings propagationSettings,
) ([]Header, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	state, err := newHeaderState(headers, settings)
	if err != nil {
		return nil, err
	}
	if outbound, ok := metadata.Outbound(ctx); ok {
		if err := settings.metadataPolicy.Inject(
			outbound,
			metadataHeaderCarrier{state: state},
		); err != nil {
			return nil, fmt.Errorf("kafka: inject metadata: %w", err)
		}
	}
	if settings.propagator != nil {
		settings.propagator.Inject(
			ctx,
			traceHeaderCarrier{state: state},
		)
	}
	return state.headers()
}

func extractMessageContext(
	ctx context.Context,
	headers []Header,
	settings propagationSettings,
) (context.Context, metadata.Metadata, error) {
	if ctx == nil {
		return nil, metadata.Metadata{}, fmt.Errorf(
			"%w: context is nil",
			ErrInvalidOption,
		)
	}
	state, err := newHeaderState(headers, settings)
	if err != nil {
		return ctx, metadata.Metadata{}, err
	}
	if settings.propagator != nil {
		ctx = settings.propagator.Extract(
			ctx,
			traceHeaderCarrier{state: state},
		)
	}
	inbound, err := settings.metadataPolicy.Extract(
		metadataHeaderCarrier{state: state},
	)
	if err != nil {
		return ctx, metadata.Metadata{}, fmt.Errorf(
			"kafka: extract metadata: %w",
			err,
		)
	}
	return ctx, inbound, nil
}
