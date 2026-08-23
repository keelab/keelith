package di

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

type key struct {
	typeOf reflect.Type
	name   string
	group  string
}

func (value key) String() string {
	name := value.typeOf.String()
	if value.name != "" {
		name += "[" + value.name + "]"
	}
	if value.group != "" {
		name += "[group=" + value.group + "]"
	}
	return name
}

type provider struct {
	module      string
	constructor reflect.Value
	function    reflect.Type
	outputs     []key
	scope       Scope
	decorator   bool
	override    bool
	supplied    bool
	displayName string
	static      *staticProviderSpec
}

// Module is an immutable collection of providers and included modules.
type Module struct {
	name      string
	providers []provider
	includes  []Module
	exports   []key
}

// Name returns the stable module name.
func (module Module) Name() string { return module.name }

// Option contributes providers to a Module.
type Option interface {
	apply(*moduleBuilder) error
}

type optionFunc func(*moduleBuilder) error

func (f optionFunc) apply(builder *moduleBuilder) error { return f(builder) }

type moduleBuilder struct {
	name      string
	providers []provider
	includes  []Module
	exports   []key
}

// NewModule validates and creates an immutable module.
func NewModule(name string, options ...Option) (Module, error) {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(name) != name {
		return Module{}, fmt.Errorf("%w: invalid name %q", ErrInvalidModule, name)
	}
	builder := moduleBuilder{name: name}
	for index, option := range options {
		if option == nil {
			return Module{}, fmt.Errorf("%w: option %d is nil", ErrInvalidModule, index)
		}
		if err := option.apply(&builder); err != nil {
			return Module{}, fmt.Errorf("module %q: %w", name, err)
		}
	}
	return Module{
		name:      name,
		providers: append([]provider(nil), builder.providers...),
		includes:  append([]Module(nil), builder.includes...),
		exports:   append([]key(nil), builder.exports...),
	}, nil
}

// MustModule is NewModule for package-level declarations.
func MustModule(name string, options ...Option) Module {
	module, err := NewModule(name, options...)
	if err != nil {
		panic(err)
	}
	return module
}

// Include composes child modules into the parent module.
func Include(modules ...Module) Option {
	snapshot := append([]Module(nil), modules...)
	return optionFunc(func(builder *moduleBuilder) error {
		for index, module := range snapshot {
			if module.name == "" {
				return fmt.Errorf("%w: included module %d is invalid", ErrInvalidModule, index)
			}
			builder.includes = append(builder.includes, module)
		}
		return nil
	})
}

// Export exposes an unqualified binding from this module to its parent.
// The argument must be a pointer to the binding type. Exporting a pointer
// binding therefore uses a pointer-to-pointer, for example (**sql.DB)(nil).
func Export(typePointer any) Option { return ExportNamed(typePointer, "") }

// ExportNamed exposes a qualified binding from this module to its parent.
func ExportNamed(typePointer any, name string) Option {
	return optionFunc(func(builder *moduleBuilder) error {
		typeOf := reflect.TypeOf(typePointer)
		if typeOf == nil || typeOf.Kind() != reflect.Pointer {
			return fmt.Errorf("%w: Export requires a pointer to type", ErrInvalidModule)
		}
		if strings.TrimSpace(name) != name {
			return fmt.Errorf("%w: invalid export qualifier %q", ErrInvalidModule, name)
		}
		builder.exports = append(builder.exports, key{typeOf: typeOf.Elem(), name: name})
		return nil
	})
}

// Select includes exactly one configuration-selected module.
func Select(condition bool, selected Module, fallback Module) Option {
	if condition {
		return Include(selected)
	}
	return Include(fallback)
}

// ProviderOption configures one constructor binding.
type ProviderOption interface {
	applyProvider(*provider) error
}

type providerOptionFunc func(*provider) error

func (f providerOptionFunc) applyProvider(item *provider) error { return f(item) }

type staticProviderSpec struct {
	call   string
	output string
	inputs []string
}

// Static attaches explicit, code-generatable expressions to the same Provider
// used by runtime Build. Keelith never infers Go identifiers from reflection
// names. Input expressions must follow the constructor parameter order.
func Static(call string, output string, inputs ...string) ProviderOption {
	snapshot := append([]string(nil), inputs...)
	return providerOptionFunc(func(item *provider) error {
		if strings.TrimSpace(call) == "" || strings.TrimSpace(output) == "" {
			return fmt.Errorf("%w: static call or output is empty", ErrInvalidProvider)
		}
		if item.static != nil {
			return fmt.Errorf("%w: duplicate static provider metadata", ErrInvalidProvider)
		}
		item.static = &staticProviderSpec{call: call, output: output, inputs: snapshot}
		return nil
	})
}

// Named qualifies every ordinary result from a provider.
func Named(name string) ProviderOption {
	return providerOptionFunc(func(item *provider) error {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(name) != name {
			return fmt.Errorf("%w: invalid qualifier %q", ErrInvalidProvider, name)
		}
		for index := range item.outputs {
			item.outputs[index].name = name
		}
		return nil
	})
}

// Grouped contributes every ordinary result to a named value group.
func Grouped(name string) ProviderOption {
	return providerOptionFunc(func(item *provider) error {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(name) != name {
			return fmt.Errorf("%w: invalid group %q", ErrInvalidProvider, name)
		}
		for index := range item.outputs {
			item.outputs[index].group = name
		}
		return nil
	})
}

// Transient marks a provider as construction-per-edge.
func Transient() ProviderOption {
	return providerOptionFunc(func(item *provider) error {
		item.scope = TransientScope
		return nil
	})
}

// As exposes a single constructor result through an interface type. Pass a
// pointer to the interface, for example As((*io.Reader)(nil)).
func As(interfacePointer any) ProviderOption {
	return providerOptionFunc(func(item *provider) error {
		typeOf := reflect.TypeOf(interfacePointer)
		if typeOf == nil || typeOf.Kind() != reflect.Pointer || typeOf.Elem().Kind() != reflect.Interface {
			return fmt.Errorf("%w: As requires a pointer to interface", ErrInvalidProvider)
		}
		if len(item.outputs) != 1 {
			return fmt.Errorf("%w: As requires exactly one result", ErrInvalidProvider)
		}
		if !item.outputs[0].typeOf.AssignableTo(typeOf.Elem()) {
			return fmt.Errorf("%w: %s does not implement %s", ErrInvalidProvider, item.outputs[0].typeOf, typeOf.Elem())
		}
		item.outputs[0].typeOf = typeOf.Elem()
		return nil
	})
}

// Provide registers a constructor. Supported results are T, (T, error),
// (T, Cleanup, error), and an Out result object.
func Provide(constructor any, options ...ProviderOption) Option {
	return registerProvider(constructor, false, false, options...)
}

// Decorate registers a constructor whose first dependency and result share
// the decorated binding. Decorators run in declaration order.
func Decorate(constructor any, options ...ProviderOption) Option {
	return registerProvider(constructor, true, false, options...)
}

func registerProvider(constructor any, decorator bool, override bool, options ...ProviderOption) Option {
	return optionFunc(func(builder *moduleBuilder) error {
		item, err := inspectProvider(builder.name, constructor)
		if err != nil {
			return err
		}
		item.decorator = decorator
		item.override = override
		for index, option := range options {
			if option == nil {
				return fmt.Errorf("%w: provider option %d is nil", ErrInvalidProvider, index)
			}
			if err := option.applyProvider(&item); err != nil {
				return err
			}
		}
		builder.providers = append(builder.providers, item)
		return nil
	})
}

func inspectProvider(module string, constructor any) (provider, error) {
	value := reflect.ValueOf(constructor)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return provider{}, fmt.Errorf("%w: constructor must be a non-nil function", ErrInvalidProvider)
	}
	typeOf := value.Type()
	outputs, err := inspectOutputs(typeOf)
	if err != nil {
		return provider{}, err
	}
	name := runtime.FuncForPC(value.Pointer()).Name()
	return provider{
		module: module, constructor: value, function: typeOf, outputs: outputs,
		scope: ApplicationScope, displayName: name,
	}, nil
}

func inspectOutputs(function reflect.Type) ([]key, error) {
	count := function.NumOut()
	if count == 0 || count > 3 {
		return nil, fmt.Errorf("%w: constructor must return one value and optional Cleanup/error", ErrInvalidProvider)
	}
	errorType := reflect.TypeFor[error]()
	cleanupType := reflect.TypeFor[Cleanup]()
	valueCount := count
	if function.Out(count-1) == errorType {
		valueCount--
	}
	if valueCount > 0 && function.Out(valueCount-1) == cleanupType {
		valueCount--
	}
	if valueCount != 1 {
		return nil, fmt.Errorf("%w: constructor must return exactly one value or Out object", ErrInvalidProvider)
	}
	result := function.Out(0)
	if isOutStruct(result) {
		return outputKeys(result)
	}
	return []key{{typeOf: result}}, nil
}

func isOutStruct(typeOf reflect.Type) bool {
	return typeOf.Kind() == reflect.Struct && embedsMarker(typeOf, reflect.TypeOf(Out{}))
}

func embedsMarker(typeOf reflect.Type, marker reflect.Type) bool {
	for index := 0; index < typeOf.NumField(); index++ {
		field := typeOf.Field(index)
		if field.Anonymous && field.Type == marker {
			return true
		}
	}
	return false
}

func outputKeys(typeOf reflect.Type) ([]key, error) {
	keys := make([]key, 0, typeOf.NumField()-1)
	for index := 0; index < typeOf.NumField(); index++ {
		field := typeOf.Field(index)
		if field.Anonymous && field.Type == reflect.TypeOf(Out{}) {
			continue
		}
		if !field.IsExported() {
			return nil, fmt.Errorf("%w: Out field %s is not exported", ErrInvalidProvider, field.Name)
		}
		name, group, _, err := parseTag(field.Tag.Get("di"))
		if err != nil {
			return nil, err
		}
		keys = append(keys, key{typeOf: field.Type, name: name, group: group})
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: Out has no results", ErrInvalidProvider)
	}
	return keys, nil
}

func parseTag(tag string) (name string, group string, optional bool, err error) {
	if tag == "" {
		return "", "", false, nil
	}
	parts := strings.Split(tag, ",")
	name = strings.TrimSpace(parts[0])
	for _, raw := range parts[1:] {
		part := strings.TrimSpace(raw)
		switch {
		case part == "optional":
			optional = true
		case strings.HasPrefix(part, "group="):
			group = strings.TrimPrefix(part, "group=")
		case part != "":
			return "", "", false, fmt.Errorf("%w: unknown tag option %q", ErrInvalidProvider, part)
		}
	}
	return name, group, optional, nil
}
