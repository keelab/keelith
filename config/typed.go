package config

import (
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	durationType        = reflect.TypeOf(time.Duration(0))
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

// ComponentDescription is a value-free typed configuration diagnostic.
type ComponentDescription struct {
	Name            string
	Path            string
	Loaded          bool
	Revision        string
	PendingRevision string
	RestartRequired bool
	RestartFields   []string
	Failed          bool
}

// ComponentOption configures a typed Component.
type ComponentOption[T any] interface {
	applyComponent(*componentOptions[T]) error
}

type componentOptionFunc[T any] func(*componentOptions[T]) error

func (fn componentOptionFunc[T]) applyComponent(
	options *componentOptions[T],
) error {
	return fn(options)
}

type componentOptions[T any] struct {
	defaultValue *T
	validators   []func(T) error
	apply        func(context.Context, T) error
	reloadable   []string
}

// WithComponentDefault supplies a value when the configured subtree is absent.
func WithComponentDefault[T any](value T) ComponentOption[T] {
	return componentOptionFunc[T](func(options *componentOptions[T]) error {
		cloned := cloneTyped(value)
		options.defaultValue = &cloned
		return nil
	})
}

// WithComponentValidator validates decoded component configuration before the
// Manager publishes its containing Snapshot.
func WithComponentValidator[T any](
	validator func(T) error,
) ComponentOption[T] {
	return componentOptionFunc[T](func(options *componentOptions[T]) error {
		if validator == nil {
			return fmt.Errorf("component validator is nil")
		}
		options.validators = append(options.validators, validator)
		return nil
	})
}

// WithComponentApply installs the component-specific hot-apply callback.
//
// The callback runs before Current changes and receives an independent value.
func WithComponentApply[T any](
	apply func(context.Context, T) error,
) ComponentOption[T] {
	return componentOptionFunc[T](func(options *componentOptions[T]) error {
		if apply == nil {
			return fmt.Errorf("component apply callback is nil")
		}
		options.apply = apply
		return nil
	})
}

// WithReloadableFields marks dot-separated fields that may change without a
// component restart. A parent path makes its complete subtree reloadable.
//
// Struct fields may alternatively use `reload:"true"`.
func WithReloadableFields[T any](paths ...string) ComponentOption[T] {
	snapshot := append([]string(nil), paths...)
	return componentOptionFunc[T](func(options *componentOptions[T]) error {
		for _, path := range snapshot {
			normalized := strings.TrimSpace(path)
			if !validComponentPath(normalized) {
				return fmt.Errorf("reloadable field path %q is invalid", path)
			}
			options.reloadable = append(options.reloadable, normalized)
		}
		return nil
	})
}

type typedPublished[T any] struct {
	revision string
	value    T
}

// Component is an immutable typed view over one Snapshot subtree.
//
// It implements Binding and can be passed directly to WithBindings.
type Component[T any] struct {
	name       string
	path       string
	segments   []string
	valueType  reflect.Type
	defaultVal *T
	validators []func(T) error
	apply      func(context.Context, T) error
	reloadable map[string]struct{}

	current atomic.Pointer[typedPublished[T]]

	applyMu           sync.Mutex
	mu                sync.Mutex
	validatedRevision string
	validatedValue    *T
	pendingRevision   string
	restartFields     []string
	lastError         string
}

// NewComponent declares one strict typed component configuration.
func NewComponent[T any](
	name string,
	path string,
	optionList ...ComponentOption[T],
) (*Component[T], error) {
	name = strings.TrimSpace(name)
	path = strings.TrimSpace(path)
	if !validComponentName(name) {
		return nil, fmt.Errorf("%w: component name %q is invalid", ErrInvalidOption, name)
	}
	if !validComponentPath(path) {
		return nil, fmt.Errorf("%w: component path %q is invalid", ErrInvalidOption, path)
	}
	valueType := reflect.TypeOf((*T)(nil)).Elem()
	if valueType.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"%w: component type %s must be a struct",
			ErrInvalidOption,
			valueType,
		)
	}
	if err := validateTypedStruct(valueType, ""); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidOption, err)
	}
	settings := componentOptions[T]{}
	for index, option := range optionList {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOption, index)
		}
		if err := option.applyComponent(&settings); err != nil {
			return nil, fmt.Errorf("%w: option %d: %w", ErrInvalidOption, index, err)
		}
	}
	reloadable := make(map[string]struct{}, len(settings.reloadable))
	for _, field := range settings.reloadable {
		reloadable[field] = struct{}{}
	}
	return &Component[T]{
		name:       name,
		path:       path,
		segments:   strings.Split(path, "."),
		valueType:  valueType,
		defaultVal: settings.defaultValue,
		validators: append([]func(T) error(nil), settings.validators...),
		apply:      settings.apply,
		reloadable: reloadable,
	}, nil
}

// Name returns the unique Manager subscriber identity.
func (component *Component[T]) Name() string {
	if component == nil {
		return ""
	}
	return component.name
}

// BindApply attaches a runtime component after its initial configuration has
// been loaded. It is intended for the common sequence:
//
//	load typed config -> construct dependency -> bind hot updates
//
// A component accepts only one apply callback.
func (component *Component[T]) BindApply(apply func(context.Context, T) error) error {
	if component == nil || apply == nil {
		return fmt.Errorf(
			"%w: typed component or apply callback is nil",
			ErrInvalidOption,
		)
	}
	component.applyMu.Lock()
	defer component.applyMu.Unlock()
	if component.apply != nil {
		return fmt.Errorf(
			"%w: component %q already has an apply callback",
			ErrInvalidOption,
			component.name,
		)
	}
	component.apply = apply
	return nil
}

// Validate strictly decodes and validates a candidate Snapshot.
func (component *Component[T]) Validate(snapshot Snapshot) error {
	if component == nil {
		return fmt.Errorf("%w: typed component is nil", ErrInvalidOption)
	}
	value, err := component.decode(snapshot)
	component.mu.Lock()
	defer component.mu.Unlock()
	if err != nil {
		component.validatedRevision = ""
		component.validatedValue = nil
		component.lastError = err.Error()
		return err
	}
	cloned := cloneTyped(value)
	component.validatedRevision = snapshot.Revision()
	component.validatedValue = &cloned
	component.lastError = ""
	return nil
}

// Apply atomically installs a validated Snapshot or reports restart-required
// fields while retaining the last applied value.
func (component *Component[T]) Apply(ctx context.Context, snapshot Snapshot) error {
	if component == nil {
		return fmt.Errorf("%w: typed component is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: typed component context is nil", ErrInvalidOption)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	component.applyMu.Lock()
	defer component.applyMu.Unlock()

	component.mu.Lock()
	var next T
	if component.validatedRevision == snapshot.Revision() &&
		component.validatedValue != nil {
		next = cloneTyped(*component.validatedValue)
	} else {
		component.mu.Unlock()
		decoded, err := component.decode(snapshot)
		if err != nil {
			component.mu.Lock()
			component.lastError = err.Error()
			component.mu.Unlock()
			return err
		}
		next = decoded
		component.mu.Lock()
	}
	component.mu.Unlock()

	current := component.current.Load()
	if current != nil {
		changed := component.restartChanges(current.value, next)
		if len(changed) > 0 {
			component.mu.Lock()
			component.pendingRevision = snapshot.Revision()
			component.restartFields = changed
			err := fmt.Errorf("%w: component %q fields %s", ErrRestartRequired, component.name, strings.Join(changed, ", "))
			component.lastError = err.Error()
			component.mu.Unlock()
			return err
		}
	}
	if component.apply != nil {
		if err := component.apply(ctx, cloneTyped(next)); err != nil {
			wrapped := fmt.Errorf("%w: component %q apply: %w", ErrValidation, component.name, err)
			component.mu.Lock()
			component.lastError = wrapped.Error()
			component.mu.Unlock()
			return wrapped
		}
	}
	component.current.Store(&typedPublished[T]{
		revision: snapshot.Revision(),
		value:    cloneTyped(next),
	})
	component.mu.Lock()
	component.pendingRevision = ""
	component.restartFields = nil
	component.lastError = ""
	component.mu.Unlock()
	return nil
}

// Current returns an independent copy of the active typed configuration.
func (component *Component[T]) Current() (T, bool) {
	var zero T
	if component == nil {
		return zero, false
	}
	current := component.current.Load()
	if current == nil {
		return zero, false
	}
	return cloneTyped(current.value), true
}

// Description returns value-free load, revision, and restart diagnostics.
func (component *Component[T]) Description() ComponentDescription {
	if component == nil {
		return ComponentDescription{}
	}
	component.mu.Lock()
	defer component.mu.Unlock()
	description := ComponentDescription{
		Name:            component.name,
		Path:            component.path,
		PendingRevision: component.pendingRevision,
		RestartRequired: len(component.restartFields) > 0,
		RestartFields:   append([]string(nil), component.restartFields...),
		Failed:          component.lastError != "",
	}
	if current := component.current.Load(); current != nil {
		description.Loaded = true
		description.Revision = current.revision
	}
	return description
}

func (component *Component[T]) decode(snapshot Snapshot) (T, error) {
	var zero T
	source, exists := snapshot.Lookup(component.segments...)
	if !exists {
		if component.defaultVal == nil {
			return zero, fmt.Errorf("%w: component %q path %q is missing", ErrTypedDecode, component.name, component.path)
		}
		value := cloneTyped(*component.defaultVal)
		for _, validator := range component.validators {
			if err := validator(cloneTyped(value)); err != nil {
				return zero, fmt.Errorf("%w: component %q: %w", ErrValidation, component.name, err)
			}
		}
		return value, nil
	}
	target := reflect.New(component.valueType).Elem()
	if err := decodeTypedValue(source, target, component.path); err != nil {
		return zero, fmt.Errorf("%w: component %q: %w", ErrTypedDecode, component.name, err)
	}
	value := target.Interface().(T)
	for _, validator := range component.validators {
		if err := validator(cloneTyped(value)); err != nil {
			return zero, fmt.Errorf("%w: component %q: %w", ErrValidation, component.name, err)
		}
	}
	return value, nil
}

func (component *Component[T]) restartChanges(current, next T) []string {
	changed := make([]string, 0)
	collectRestartChanges(reflect.ValueOf(current), reflect.ValueOf(next), "", component.reloadable, &changed)
	sort.Strings(changed)
	return changed
}

func collectRestartChanges(current reflect.Value, next reflect.Value, path string, reloadable map[string]struct{}, changed *[]string) {
	if reflect.DeepEqual(current.Interface(), next.Interface()) {
		return
	}
	valueType := current.Type()
	if valueType.Kind() != reflect.Struct || isScalarStruct(valueType) {
		if !pathReloadable(path, reloadable) {
			*changed = append(*changed, path)
		}
		return
	}
	for index := range valueType.NumField() {
		field := valueType.Field(index)
		if field.PkgPath != "" || fieldName(field) == "" {
			continue
		}
		name := fieldName(field)
		fieldPath := name
		if path != "" {
			fieldPath = path + "." + name
		}
		if field.Tag.Get("reload") == "true" ||
			pathReloadable(fieldPath, reloadable) {
			continue
		}
		collectRestartChanges(current.Field(index), next.Field(index), fieldPath, reloadable, changed)
	}
}

func pathReloadable(path string, reloadable map[string]struct{}) bool {
	if path == "" {
		return false
	}
	for allowed := range reloadable {
		if path == allowed || strings.HasPrefix(path, allowed+".") {
			return true
		}
	}
	return false
}

func decodeTypedValue(source any, target reflect.Value, path string) error {
	if !target.CanSet() {
		return fmt.Errorf("%s is not settable", path)
	}
	if source == nil {
		switch target.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			target.SetZero()
			return nil
		default:
			return fmt.Errorf("%s cannot be null", path)
		}
	}
	if target.Kind() == reflect.Pointer {
		target.Set(reflect.New(target.Type().Elem()))
		return decodeTypedValue(source, target.Elem(), path)
	}
	if target.Type() == durationType {
		value, err := decodeDuration(source)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		target.SetInt(int64(value))
		return nil
	}
	if text, ok := source.(string); ok &&
		target.CanAddr() &&
		target.Addr().Type().Implements(textUnmarshalerType) {
		candidate := reflect.New(target.Type())
		unmarshaler := candidate.Interface().(encoding.TextUnmarshaler)
		if err := unmarshaler.UnmarshalText([]byte(text)); err != nil {
			return fmt.Errorf("%s: text decode: %w", path, err)
		}
		target.Set(candidate.Elem())
		return nil
	}

	switch target.Kind() {
	case reflect.Struct:
		values, ok := stringMap(source)
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		fields, err := typedFields(target.Type())
		if err != nil {
			return err
		}
		for key := range values {
			if _, known := fields[key]; !known {
				return fmt.Errorf("%w: %s.%s", ErrUnknownField, path, key)
			}
		}
		for name, field := range fields {
			value, exists := values[name]
			if !exists {
				continue
			}
			childPath := path + "." + name
			if err := decodeTypedValue(
				value,
				target.Field(field.index),
				childPath,
			); err != nil {
				return err
			}
		}
		return nil
	case reflect.Bool:
		value, err := decodeBool(source)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		target.SetBool(value)
		return nil
	case reflect.String:
		value, ok := source.(string)
		if !ok {
			return fmt.Errorf("%s must be a string", path)
		}
		target.SetString(value)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value, err := decodeInt(source, target.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		target.SetInt(value)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value, err := decodeUint(source, target.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		target.SetUint(value)
		return nil
	case reflect.Float32, reflect.Float64:
		value, err := decodeFloat(source, target.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		target.SetFloat(value)
		return nil
	case reflect.Slice:
		values, ok := anySlice(source)
		if !ok {
			return fmt.Errorf("%s must be an array", path)
		}
		result := reflect.MakeSlice(target.Type(), len(values), len(values))
		for index, value := range values {
			if err := decodeTypedValue(
				value,
				result.Index(index),
				fmt.Sprintf("%s[%d]", path, index),
			); err != nil {
				return err
			}
		}
		target.Set(result)
		return nil
	case reflect.Array:
		values, ok := anySlice(source)
		if !ok || len(values) != target.Len() {
			return fmt.Errorf("%s must be an array of length %d", path, target.Len())
		}
		for index, value := range values {
			if err := decodeTypedValue(
				value,
				target.Index(index),
				fmt.Sprintf("%s[%d]", path, index),
			); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		if target.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("%s map key must be string", path)
		}
		values, ok := stringMap(source)
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		result := reflect.MakeMapWithSize(target.Type(), len(values))
		for key, value := range values {
			element := reflect.New(target.Type().Elem()).Elem()
			if err := decodeTypedValue(
				value,
				element,
				path+"."+key,
			); err != nil {
				return err
			}
			result.SetMapIndex(reflect.ValueOf(key).Convert(target.Type().Key()), element)
		}
		target.Set(result)
		return nil
	case reflect.Interface:
		cloned, err := cloneValue(source, true)
		if err != nil {
			return err
		}
		target.Set(reflect.ValueOf(cloned))
		return nil
	default:
		return fmt.Errorf("%s has unsupported target type %s", path, target.Type())
	}
}

type typedField struct {
	index int
}

func typedFields(valueType reflect.Type) (map[string]typedField, error) {
	fields := make(map[string]typedField)
	for index := range valueType.NumField() {
		field := valueType.Field(index)
		if field.PkgPath != "" {
			continue
		}
		name := fieldName(field)
		if name == "" {
			continue
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, fmt.Errorf(
				"config field %q is duplicated in %s",
				name,
				valueType,
			)
		}
		fields[name] = typedField{index: index}
	}
	return fields, nil
}

func fieldName(field reflect.StructField) string {
	for _, key := range []string{"config", "json", "yaml"} {
		tag, exists := field.Tag.Lookup(key)
		if !exists {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "-" {
			return ""
		}
		if name != "" {
			return name
		}
	}
	if field.Anonymous {
		return lowerFirst(field.Type.Name())
	}
	return lowerFirst(field.Name)
}

func validateTypedStruct(valueType reflect.Type, path string) error {
	fields, err := typedFields(valueType)
	if err != nil {
		return err
	}
	for name, field := range fields {
		fieldType := valueType.Field(field.index).Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Struct && !isScalarStruct(fieldType) {
			child := name
			if path != "" {
				child = path + "." + name
			}
			if err := validateTypedStruct(fieldType, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func isScalarStruct(valueType reflect.Type) bool {
	return valueType == durationType ||
		reflect.PointerTo(valueType).Implements(textUnmarshalerType)
}

func decodeDuration(source any) (time.Duration, error) {
	if text, ok := source.(string); ok {
		value, err := time.ParseDuration(text)
		if err != nil {
			return 0, fmt.Errorf("duration is invalid")
		}
		return value, nil
	}
	value, err := decodeInt(source, 64)
	if err != nil {
		return 0, fmt.Errorf("duration must be a string or integer nanoseconds")
	}
	return time.Duration(value), nil
}

func decodeBool(source any) (bool, error) {
	switch value := source.(type) {
	case bool:
		return value, nil
	case string:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, fmt.Errorf("boolean is invalid")
		}
		return parsed, nil
	default:
		return false, fmt.Errorf("value must be a boolean")
	}
}

func decodeInt(source any, bits int) (int64, error) {
	text, ok := numericText(source)
	if !ok {
		return 0, fmt.Errorf("value must be an integer")
	}
	value, err := strconv.ParseInt(text, 10, bits)
	if err != nil {
		return 0, fmt.Errorf("integer is invalid or overflows")
	}
	return value, nil
}

func decodeUint(source any, bits int) (uint64, error) {
	text, ok := numericText(source)
	if !ok {
		return 0, fmt.Errorf("value must be an unsigned integer")
	}
	value, err := strconv.ParseUint(text, 10, bits)
	if err != nil {
		return 0, fmt.Errorf("unsigned integer is invalid or overflows")
	}
	return value, nil
}

func decodeFloat(source any, bits int) (float64, error) {
	var text string
	switch value := source.(type) {
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return 0, fmt.Errorf("number is invalid or non-finite")
		}
		text = strconv.FormatFloat(float64(value), 'g', -1, 32)
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, fmt.Errorf("number is invalid or non-finite")
		}
		text = strconv.FormatFloat(value, 'g', -1, 64)
	default:
		var ok bool
		text, ok = numericText(source)
		if !ok {
			return 0, fmt.Errorf("value must be numeric")
		}
	}
	value, err := strconv.ParseFloat(text, bits)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("number is invalid or non-finite")
	}
	return value, nil
}

func numericText(source any) (string, bool) {
	switch value := source.(type) {
	case string:
		return value, true
	case json.Number:
		return value.String(), true
	case int:
		return strconv.FormatInt(int64(value), 10), true
	case int8:
		return strconv.FormatInt(int64(value), 10), true
	case int16:
		return strconv.FormatInt(int64(value), 10), true
	case int32:
		return strconv.FormatInt(int64(value), 10), true
	case int64:
		return strconv.FormatInt(value, 10), true
	case uint:
		return strconv.FormatUint(uint64(value), 10), true
	case uint8:
		return strconv.FormatUint(uint64(value), 10), true
	case uint16:
		return strconv.FormatUint(uint64(value), 10), true
	case uint32:
		return strconv.FormatUint(uint64(value), 10), true
	case uint64:
		return strconv.FormatUint(value, 10), true
	case float32:
		if math.Trunc(float64(value)) != float64(value) {
			return "", false
		}
		return strconv.FormatFloat(float64(value), 'f', -1, 32), true
	case float64:
		if math.Trunc(value) != value || math.IsNaN(value) || math.IsInf(value, 0) {
			return "", false
		}
		return strconv.FormatFloat(value, 'f', -1, 64), true
	default:
		return "", false
	}
}

func stringMap(source any) (map[string]any, bool) {
	if values, ok := source.(map[string]any); ok {
		return values, true
	}
	reflected := reflect.ValueOf(source)
	if !reflected.IsValid() ||
		reflected.Kind() != reflect.Map ||
		reflected.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	result := make(map[string]any, reflected.Len())
	iterator := reflected.MapRange()
	for iterator.Next() {
		result[iterator.Key().String()] = iterator.Value().Interface()
	}
	return result, true
}

func anySlice(source any) ([]any, bool) {
	if values, ok := source.([]any); ok {
		return values, true
	}
	reflected := reflect.ValueOf(source)
	if !reflected.IsValid() ||
		reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, false
	}
	result := make([]any, reflected.Len())
	for index := range reflected.Len() {
		result[index] = reflected.Index(index).Interface()
	}
	return result, true
}

func cloneTyped[T any](value T) T {
	cloned := cloneTypedValue(reflect.ValueOf(value))
	if !cloned.IsValid() {
		var zero T
		return zero
	}
	return cloned.Interface().(T)
}

func cloneTypedValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return reflect.Value{}
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(cloneTypedValue(value.Elem()))
		return result
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneTypedValue(value.Elem())
		result := reflect.New(value.Type()).Elem()
		result.Set(cloned)
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			result.SetMapIndex(
				cloneTypedValue(iterator.Key()),
				cloneTypedValue(iterator.Value()),
			)
		}
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := range value.Len() {
			result.Index(index).Set(cloneTypedValue(value.Index(index)))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for index := range value.Len() {
			result.Index(index).Set(cloneTypedValue(value.Index(index)))
		}
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for index := range value.NumField() {
			field := value.Type().Field(index)
			if field.PkgPath == "" && result.Field(index).CanSet() {
				result.Field(index).Set(cloneTypedValue(value.Field(index)))
			}
		}
		return result
	default:
		return value
	}
}

func validComponentName(value string) bool {
	if value == "" ||
		len(value) > 256 ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validComponentPath(value string) bool {
	if value == "" || strings.HasPrefix(value, ".") || strings.HasSuffix(value, ".") {
		return false
	}
	for _, segment := range strings.Split(value, ".") {
		if segment == "" {
			return false
		}
		for _, r := range segment {
			if !unicode.IsLetter(r) &&
				!unicode.IsDigit(r) &&
				r != '_' &&
				r != '-' {
				return false
			}
		}
	}
	return true
}

func lowerFirst(value string) string {
	if value == "" {
		return value
	}
	characters := []rune(value)
	characters[0] = unicode.ToLower(characters[0])
	return string(characters)
}
