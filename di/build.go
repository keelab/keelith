package di

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/keelab/keelith/app"
)

var (
	contextType   = reflect.TypeFor[context.Context]()
	errorType     = reflect.TypeFor[error]()
	cleanupType   = reflect.TypeFor[Cleanup]()
	componentType = reflect.TypeFor[app.Component]()
	inType        = reflect.TypeFor[In]()
)

type binding struct {
	provider   *provider
	output     int
	decorators []*provider
	result     instance
}

type instance struct {
	values []reflect.Value
	err    error
	done   bool
	mu     sync.Mutex
}

type builder struct {
	ctx          context.Context
	bindings     map[scopedKey]*binding
	groups       map[scopedKey][]*binding
	imports      map[scopedKey]scopedKey
	rootModules  []string
	instances    map[*provider]*instance
	visiting     []scopedKey
	cleanups     []Cleanup
	components   []app.Component
	descriptions map[*provider]*ProviderDescription
	lazyMu       sync.Mutex
}

type scopedKey struct {
	module string
	key    key
}

// Build validates module bindings, constructs root T, and returns the owned graph.
func Build[T any](ctx context.Context, modules ...Module) (*Graph, T, error) {
	var zero T
	if ctx == nil {
		return nil, zero, fmt.Errorf("%w: context is nil", ErrInvalidModule)
	}
	rootType := reflect.TypeFor[T]()
	builder, err := newBuilder(ctx, modules)
	if err != nil {
		return nil, zero, err
	}
	value, err := builder.resolveRoot(key{typeOf: rootType})
	if err != nil {
		rollbackErr := closeCleanups(context.WithoutCancel(ctx), builder.cleanups)
		return nil, zero, errors.Join(err, rollbackErr)
	}
	root, ok := value.Interface().(T)
	if !ok {
		rollbackErr := closeCleanups(context.WithoutCancel(ctx), builder.cleanups)
		return nil, zero, errors.Join(
			fmt.Errorf("%w: root %s has incompatible value %s", ErrProviderFailed, rootType, value.Type()),
			rollbackErr,
		)
	}
	graph := &Graph{
		description: builder.description(rootType),
		components:  append([]app.Component(nil), builder.components...),
		cleanups:    append([]Cleanup(nil), builder.cleanups...),
	}
	return graph, root, nil
}

func newBuilder(ctx context.Context, modules []Module) (*builder, error) {
	result := &builder{
		ctx: ctx, bindings: make(map[scopedKey]*binding), groups: make(map[scopedKey][]*binding),
		imports:   make(map[scopedKey]scopedKey),
		instances: make(map[*provider]*instance), descriptions: make(map[*provider]*ProviderDescription),
	}
	providers := make([]provider, 0)
	pendingOverrides := make([]scopedKey, 0)
	modulesByName := make(map[string]Module)
	var collect func(Module) error
	collect = func(module Module) error {
		if previous, exists := modulesByName[module.name]; exists {
			if !reflect.DeepEqual(previous, module) {
				return fmt.Errorf("%w: duplicate module name %q", ErrInvalidModule, module.name)
			}
			return nil
		}
		modulesByName[module.name] = module
		providers = append(providers, module.providers...)
		for _, child := range module.includes {
			if err := collect(child); err != nil {
				return err
			}
		}
		return nil
	}
	for index, module := range modules {
		if module.name == "" {
			return nil, fmt.Errorf("%w: module %d is invalid", ErrInvalidModule, index)
		}
		result.rootModules = append(result.rootModules, module.name)
		if err := collect(module); err != nil {
			return nil, err
		}
	}
	for index := range providers {
		item := &providers[index]
		if item.decorator {
			continue
		}
		for outputIndex, output := range item.outputs {
			binding := &binding{provider: item, output: outputIndex}
			scoped := scopedKey{module: item.module, key: output}
			if output.group != "" {
				groupKey := scopedKey{
					module: item.module,
					key:    key{typeOf: output.typeOf, group: output.group},
				}
				result.groups[groupKey] = append(result.groups[groupKey], binding)
				continue
			}
			if existing := result.bindings[scoped]; existing != nil {
				if !item.override {
					return nil, fmt.Errorf(
						"%w: %s from %s and %s",
						ErrDuplicateProvider,
						output,
						existing.provider.displayName,
						item.displayName,
					)
				}
				result.bindings[scoped] = binding
			} else {
				if item.override {
					pendingOverrides = append(pendingOverrides, scoped)
				}
				result.bindings[scoped] = binding
			}
		}
	}
	// Preallocate application instances before Build can publish a Lazy resolver.
	// Lazy resolution therefore never mutates the instances map concurrently.
	for index := range providers {
		if providers[index].scope == ApplicationScope {
			result.instances[&providers[index]] = &instance{}
		}
	}
	for index := range providers {
		item := &providers[index]
		if !item.decorator {
			continue
		}
		if len(item.outputs) != 1 {
			return nil, fmt.Errorf("%w: decorator %s must have one output", ErrInvalidProvider, item.displayName)
		}
		output := item.outputs[0]
		base := result.bindings[scopedKey{module: item.module, key: output}]
		if base == nil {
			return nil, fmt.Errorf("%w: decorator %s has no base binding for %s", ErrMissingProvider, item.displayName, output)
		}
		base.decorators = append(base.decorators, item)
	}
	for _, module := range modulesByName {
		for _, child := range module.includes {
			for _, exported := range child.exports {
				visible := scopedKey{module: module.name, key: exported}
				if previous, duplicate := result.imports[visible]; duplicate {
					return nil, fmt.Errorf(
						"%w: module %s imports %s from %s and %s",
						ErrDuplicateProvider,
						module.name,
						exported,
						previous.module,
						child.name,
					)
				}
				result.imports[visible] = scopedKey{module: child.name, key: exported}
			}
		}
	}
	for _, overridden := range pendingOverrides {
		importedKey, imported := result.imports[overridden]
		if !imported {
			return nil, fmt.Errorf(
				"%w: module %s %s has no binding to replace",
				ErrInvalidOverride,
				overridden.module,
				overridden.key,
			)
		}
		importedBinding := result.bindings[importedKey]
		if importedBinding != nil {
			result.bindings[overridden].decorators = append(
				result.bindings[overridden].decorators,
				importedBinding.decorators...,
			)
		}
	}
	for _, module := range modulesByName {
		for _, exported := range module.exports {
			if _, exists := result.lookup(module.name, exported); !exists {
				return nil, fmt.Errorf("%w: module %s exports missing %s", ErrMissingProvider, module.name, exported)
			}
		}
	}
	return result, nil
}

func (builder *builder) resolveRoot(required key) (reflect.Value, error) {
	var selected scopedKey
	for _, module := range builder.rootModules {
		candidate, exists := builder.lookup(module, required)
		if !exists {
			continue
		}
		if selected.module != "" {
			return reflect.Value{}, fmt.Errorf(
				"%w: root %s is visible from %s and %s",
				ErrDuplicateProvider,
				required,
				selected.module,
				module,
			)
		}
		selected = candidate
	}
	if selected.module == "" {
		return reflect.Value{}, fmt.Errorf("%w: root %s", ErrMissingProvider, required)
	}
	return builder.resolveScoped(selected, ApplicationScope)
}

func (builder *builder) lookup(module string, required key) (scopedKey, bool) {
	local := scopedKey{module: module, key: required}
	if builder.bindings[local] != nil {
		return local, true
	}
	imported, exists := builder.imports[local]
	if !exists {
		return scopedKey{}, false
	}
	for {
		if builder.bindings[imported] != nil {
			return imported, true
		}
		next, ok := builder.imports[imported]
		if !ok {
			return scopedKey{}, false
		}
		imported = next
	}
}

func (builder *builder) resolveFor(module string, required key, scope Scope) (reflect.Value, error) {
	resolved, exists := builder.lookup(module, required)
	if !exists {
		return reflect.Value{}, fmt.Errorf("%w: module %s requires %s", ErrMissingProvider, module, required)
	}
	return builder.resolveScoped(resolved, scope)
}

func (builder *builder) resolveScoped(required scopedKey, consumerScope Scope) (reflect.Value, error) {
	if required.key.group != "" {
		return reflect.Value{}, fmt.Errorf("%w: group %s must be injected into a slice", ErrInvalidProvider, required.key)
	}
	binding := builder.bindings[required]
	if binding == nil {
		return reflect.Value{}, fmt.Errorf("%w: %s/%s", ErrMissingProvider, required.module, required.key)
	}
	return builder.resolveBinding(required, binding, consumerScope)
}

func (builder *builder) resolveBinding(
	required scopedKey,
	binding *binding,
	consumerScope Scope,
) (reflect.Value, error) {
	if consumerScope == ApplicationScope && binding.provider.scope == TransientScope {
		return reflect.Value{}, fmt.Errorf(
			"%w: application provider captures transient %s/%s",
			ErrScopeViolation,
			required.module,
			required.key,
		)
	}
	if slices.Contains(builder.visiting, required) {
		cycle := append(append([]scopedKey(nil), builder.visiting...), required)
		parts := make([]string, len(cycle))
		for index := range cycle {
			parts[index] = cycle[index].module + "/" + cycle[index].key.String()
		}
		return reflect.Value{}, fmt.Errorf("%w: %s", ErrDependencyCycle, strings.Join(parts, " -> "))
	}
	builder.visiting = append(builder.visiting, required)
	defer func() { builder.visiting = builder.visiting[:len(builder.visiting)-1] }()
	if binding.provider.scope == ApplicationScope {
		binding.result.mu.Lock()
		defer binding.result.mu.Unlock()
		if binding.result.done {
			return binding.result.values[0], binding.result.err
		}
	}
	values, err := builder.construct(binding.provider)
	if err != nil {
		return reflect.Value{}, err
	}
	value := values[binding.output]
	for _, decorator := range binding.decorators {
		value, err = builder.constructDecorator(decorator, required.key, value)
		if err != nil {
			return reflect.Value{}, err
		}
	}
	if binding.provider.scope == ApplicationScope {
		binding.result.values = []reflect.Value{value}
		binding.result.err = err
		binding.result.done = true
	}
	return value, err
}

func (builder *builder) construct(item *provider) ([]reflect.Value, error) {
	if item.scope == ApplicationScope {
		cached := builder.instances[item]
		cached.mu.Lock()
		defer cached.mu.Unlock()
		if cached.done {
			return cached.values, cached.err
		}
		cached.values, cached.err = builder.invoke(item, nil)
		cached.done = true
		return cached.values, cached.err
	}
	return builder.invoke(item, nil)
}

func (builder *builder) constructDecorator(
	item *provider,
	decorated key,
	current reflect.Value,
) (reflect.Value, error) {
	values, err := builder.invoke(item, &decoratedValue{key: decorated, value: current})
	if err != nil {
		return reflect.Value{}, err
	}
	return values[0], nil
}

type decoratedValue struct {
	key   key
	value reflect.Value
}

func (builder *builder) invoke(item *provider, decorated *decoratedValue) (values []reflect.Value, resultErr error) {
	started := time.Now()
	cleanupAdded := false
	description := builder.providerDescription(item)
	defer func() {
		description.Duration += time.Since(started)
		description.Constructs++
		if resultErr != nil {
			description.State = "failed"
		}
	}()
	if item.supplied {
		description.Constructed = true
		description.State = "ready"
		builder.captureComponent(item.constructor)
		return []reflect.Value{item.constructor}, nil
	}
	inputs := make([]reflect.Value, item.function.NumIn())
	decoratedUsed := false
	for index := 0; index < item.function.NumIn(); index++ {
		parameter := item.function.In(index)
		if parameter == contextType {
			inputs[index] = reflect.ValueOf(builder.ctx)
			continue
		}
		if decorated != nil && !decoratedUsed && parameter == decorated.key.typeOf {
			inputs[index] = decorated.value
			decoratedUsed = true
			continue
		}
		value, err := builder.resolveParameter(item.module, parameter, item.scope)
		if err != nil {
			return nil, fmt.Errorf("provider %s parameter %d: %w", item.displayName, index, err)
		}
		inputs[index] = value
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = fmt.Errorf("%w: %s panicked: %v", ErrProviderFailed, item.displayName, recovered)
			values = nil
		}
	}()
	results := item.constructor.Call(inputs)
	if len(results) > 0 && results[len(results)-1].Type() == errorType {
		errValue := results[len(results)-1]
		results = results[:len(results)-1]
		if !errValue.IsNil() {
			return nil, fmt.Errorf("%w: %s: %w", ErrProviderFailed, item.displayName, errValue.Interface().(error))
		}
	}
	if len(results) > 1 && results[len(results)-1].Type() == cleanupType {
		cleanupValue := results[len(results)-1]
		results = results[:len(results)-1]
		if !cleanupValue.IsNil() {
			builder.cleanups = append(builder.cleanups, cleanupValue.Interface().(Cleanup))
			cleanupAdded = true
		}
	}
	values, err := flattenOutputs(item, results[0])
	if err != nil {
		return nil, err
	}
	if cleanupAdded {
		for _, value := range values {
			if value.IsValid() && value.Type().Implements(componentType) {
				return nil, fmt.Errorf("%w: %s returns both Cleanup and app.Component", ErrInvalidProvider, item.displayName)
			}
		}
	}
	for _, value := range values {
		builder.captureComponent(value)
	}
	description.Constructed = true
	description.State = "ready"
	return values, nil
}

func flattenOutputs(item *provider, value reflect.Value) ([]reflect.Value, error) {
	if !isOutStruct(value.Type()) {
		return []reflect.Value{value}, nil
	}
	result := make([]reflect.Value, 0, len(item.outputs))
	for index := 0; index < value.NumField(); index++ {
		fieldType := value.Type().Field(index)
		if fieldType.Anonymous && fieldType.Type == reflect.TypeFor[Out]() {
			continue
		}
		result = append(result, value.Field(index))
	}
	return result, nil
}

func (builder *builder) resolveParameter(module string, parameter reflect.Type, scope Scope) (reflect.Value, error) {
	if parameter.Kind() == reflect.Struct && embedsMarker(parameter, inType) {
		return builder.resolveIn(module, parameter, scope)
	}
	if parameter.Implements(reflect.TypeFor[lazyDescriptor]()) {
		return builder.lazyValue(module, parameter, scope)
	}
	return builder.resolveFor(module, key{typeOf: parameter}, scope)
}

func (builder *builder) resolveIn(module string, parameter reflect.Type, scope Scope) (reflect.Value, error) {
	result := reflect.New(parameter).Elem()
	for index := 0; index < parameter.NumField(); index++ {
		field := parameter.Field(index)
		if field.Anonymous && field.Type == inType {
			continue
		}
		if !field.IsExported() {
			return reflect.Value{}, fmt.Errorf("%w: In field %s is not exported", ErrInvalidProvider, field.Name)
		}
		name, group, optional, err := parseTag(field.Tag.Get("di"))
		if err != nil {
			return reflect.Value{}, err
		}
		if group != "" {
			if field.Type.Kind() != reflect.Slice {
				return reflect.Value{}, fmt.Errorf("%w: group field %s must be a slice", ErrInvalidProvider, field.Name)
			}
			bindings := builder.groups[scopedKey{module: module, key: key{typeOf: field.Type.Elem(), group: group}}]
			slice := reflect.MakeSlice(field.Type, 0, len(bindings))
			for _, binding := range bindings {
				value, buildErr := builder.resolveBinding(
					scopedKey{module: binding.provider.module, key: key{typeOf: field.Type.Elem(), group: group}}, binding, scope,
				)
				if buildErr != nil {
					return reflect.Value{}, buildErr
				}
				slice = reflect.Append(slice, value)
			}
			result.Field(index).Set(slice)
			continue
		}
		value, resolveErr := builder.resolveFor(module, key{typeOf: field.Type, name: name}, scope)
		if resolveErr != nil && optional && errors.Is(resolveErr, ErrMissingProvider) {
			continue
		}
		if resolveErr != nil {
			return reflect.Value{}, fmt.Errorf("field %s: %w", field.Name, resolveErr)
		}
		result.Field(index).Set(value)
	}
	return result, nil
}

func (builder *builder) lazyValue(module string, parameter reflect.Type, scope Scope) (reflect.Value, error) {
	descriptor := reflect.New(parameter).Elem().Interface().(lazyDescriptor)
	required := key{typeOf: descriptor.dependencyType()}
	resolved, exists := builder.lookup(module, required)
	if !exists {
		return reflect.Value{}, fmt.Errorf("%w: %s", ErrMissingProvider, required)
	}
	binding := builder.bindings[resolved]
	if providerOwnsLifecycle(binding.provider) {
		return reflect.Value{}, fmt.Errorf(
			"%w: lazy provider %s owns lifecycle",
			ErrInvalidProvider,
			binding.provider.displayName,
		)
	}
	for _, decorator := range binding.decorators {
		if providerOwnsLifecycle(decorator) {
			return reflect.Value{}, fmt.Errorf("%w: lazy decorator %s owns lifecycle", ErrInvalidProvider, decorator.displayName)
		}
	}
	result := reflect.New(parameter).Elem()
	resolverField := result.FieldByName("Resolver")
	resolver := reflect.MakeFunc(resolverField.Type(), func(_ []reflect.Value) []reflect.Value {
		builder.lazyMu.Lock()
		defer builder.lazyMu.Unlock()
		value, err := builder.resolveScoped(resolved, scope)
		errValue := reflect.Zero(errorType)
		if err != nil {
			errValue = reflect.ValueOf(err)
			value = reflect.Zero(required.typeOf)
		}
		return []reflect.Value{value, errValue}
	})
	resolverField.Set(resolver)
	return result, nil
}

func providerOwnsLifecycle(item *provider) bool {
	if item.function == nil {
		return item.constructor.IsValid() && item.constructor.Type().Implements(componentType)
	}
	for output := range item.function.Outs() {
		if output == cleanupType || output.Implements(componentType) {
			return true
		}
	}
	return false
}

func (builder *builder) captureComponent(value reflect.Value) {
	if value.IsValid() && value.Type().Implements(componentType) && (value.Kind() != reflect.Pointer || !value.IsNil()) {
		builder.components = append(builder.components, value.Interface().(app.Component))
	}
}

func (builder *builder) providerDescription(item *provider) *ProviderDescription {
	if existing := builder.descriptions[item]; existing != nil {
		return existing
	}
	scope := "application"
	if item.scope == TransientScope {
		scope = "transient"
	}
	description := &ProviderDescription{
		ID:        item.module + ":" + item.displayName,
		Module:    item.module,
		Type:      item.outputs[0].typeOf.String(),
		Name:      item.outputs[0].name,
		Group:     item.outputs[0].group,
		Scope:     scope,
		State:     "registered",
		Decorator: item.decorator,
		Override:  item.override,
	}
	if item.function != nil {
		for in := range item.function.Ins() {
			if in != contextType {
				description.Dependencies = append(description.Dependencies, in.String())
			}
		}
	}
	builder.descriptions[item] = description
	return description
}

func (builder *builder) description(root reflect.Type) Description {
	providers := make([]ProviderDescription, 0, len(builder.descriptions))
	for _, item := range builder.descriptions {
		providers = append(providers, *item)
	}
	sort.Slice(providers, func(i, j int) bool { return providers[i].ID < providers[j].ID })
	return describeEdges(Description{Root: root.String(), Providers: providers})
}

func describeEdges(description Description) Description {
	providersByType := make(map[string][]ProviderDescription)
	for _, item := range description.Providers {
		providersByType[item.Type] = append(providersByType[item.Type], item)
	}
	seen := make(map[string]struct{})
	for _, consumer := range description.Providers {
		for _, dependencyType := range consumer.Dependencies {
			for _, dependency := range providersByType[dependencyType] {
				identity := dependency.ID + "\x00" + consumer.ID + "\x00" + dependencyType
				if _, duplicate := seen[identity]; duplicate {
					continue
				}
				seen[identity] = struct{}{}
				description.Edges = append(description.Edges, EdgeDescription{
					From: dependency.ID,
					To:   consumer.ID,
					Type: dependencyType,
				})
			}
		}
	}
	sort.Slice(description.Edges, func(i, j int) bool {
		if description.Edges[i].From != description.Edges[j].From {
			return description.Edges[i].From < description.Edges[j].From
		}
		if description.Edges[i].To != description.Edges[j].To {
			return description.Edges[i].To < description.Edges[j].To
		}
		return description.Edges[i].Type < description.Edges[j].Type
	})
	return description
}

func closeCleanups(ctx context.Context, cleanups []Cleanup) error {
	var failures []error
	for _, cleanup := range slices.Backward(cleanups) {
		if err := cleanup(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrCleanupFailed, errors.Join(failures...))
}

// Close releases construction-owned resources in reverse order exactly once.
func (graph *Graph) Close(ctx context.Context) error {
	if graph == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrCleanupFailed)
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	if graph.closed {
		return nil
	}
	graph.closed = true
	return closeCleanups(ctx, graph.cleanups)
}
