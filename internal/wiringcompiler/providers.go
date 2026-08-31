package wiringcompiler

import (
	"context"
	"fmt"
	"go/types"
	"strings"

	"github.com/keelab/keelith/internal/scaffold"
	"golang.org/x/tools/go/packages"
)

// Provider describes a validated project constructor symbol.
type Provider struct {
	Spec             scaffold.ProviderSpec
	ImportPath       string
	PackageName      string
	Name             string
	Signature        *types.Signature
	InputTypes       []string
	OutputType       string
	OutputKey        string
	ReturnsError     bool
	ReturnsCleanup   bool
	Scope            string
	Outputs          []Output
	InputExpressions []string
}

// Output is one binding exposed by a provider. A provider returning di.Out
// exposes each exported field as an independent binding.
type Output struct {
	Field string
	Type  string
	Name  string
	Group string
	Key   string
}

// LoadProviders resolves project-declared constructors without invoking them.
// Go's type checker is the source of truth for signatures and package paths.
func LoadProviders(ctx context.Context, root, module string, specs []scaffold.ProviderSpec) ([]Provider, error) {
	if ctx == nil {
		return nil, fmt.Errorf("wiring compiler: context is nil")
	}
	if strings.TrimSpace(root) == "" || strings.TrimSpace(module) == "" {
		return nil, fmt.Errorf("wiring compiler: project root and module are required")
	}
	result := make([]Provider, 0, len(specs))
	for index, spec := range specs {
		provider, err := loadProvider(ctx, root, module, spec)
		if err != nil {
			return nil, fmt.Errorf("provider %d %q: %w", index, spec.Constructor, err)
		}
		result = append(result, provider)
	}
	return orderProviders(result)
}

func orderProviders(providers []Provider) ([]Provider, error) {
	byOutput := make(map[string]int, len(providers))
	groupTypes := make(map[string][]int)
	for i, provider := range providers {
		if provider.Scope == "" {
			providers[i].Scope = "application"
		}
		if provider.OutputKey == "" {
			provider.OutputKey = provider.OutputType
		}
		providers[i] = provider
		if previous, exists := byOutput[provider.OutputKey]; exists {
			return nil, fmt.Errorf("duplicate provider output %q from %s and %s", provider.OutputKey, providers[previous].Spec.Constructor, provider.Spec.Constructor)
		}
		byOutput[provider.OutputKey] = i
		if provider.Spec.Group != "" {
			groupKey := "[]" + provider.OutputType + "[group=" + provider.Spec.Group + "]"
			groupTypes[groupKey] = append(groupTypes[groupKey], i)
		}
	}
	indegree := make([]int, len(providers))
	dependents := make([][]int, len(providers))
	for i, provider := range providers {
		for _, input := range provider.InputTypes {
			if strings.HasPrefix(input, "[]") {
				grouped, exists := groupTypes[input]
				if !exists {
					return nil, fmt.Errorf("provider %s requires group %s, but no matching grouped provider exists", provider.Spec.Constructor, input)
				}
				if provider.Scope == "application" {
					for _, dependency := range grouped {
						if providers[dependency].Scope == "transient" {
							return nil, fmt.Errorf("provider %s (application scope) cannot depend on transient grouped provider %s", provider.Spec.Constructor, providers[dependency].Spec.Constructor)
						}
					}
				}
				continue
			}
			dependency, ok := byOutput[input]
			if !ok {
				return nil, fmt.Errorf("provider %s requires %s, but no provider supplies it", provider.Spec.Constructor, input)
			}
			if provider.Scope == "application" && providers[dependency].Scope == "transient" {
				return nil, fmt.Errorf("provider %s (application scope) cannot depend on transient provider %s", provider.Spec.Constructor, providers[dependency].Spec.Constructor)
			}
			indegree[i]++
			dependents[dependency] = append(dependents[dependency], i)
		}
	}
	queue := make([]int, 0, len(providers))
	for i, degree := range indegree {
		if degree == 0 {
			queue = append(queue, i)
		}
	}
	ordered := make([]Provider, 0, len(providers))
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		ordered = append(ordered, providers[i])
		for _, dependent := range dependents[i] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if len(ordered) != len(providers) {
		return nil, fmt.Errorf("provider dependency graph contains a cycle: %s", describeCycle(providers, indegree))
	}
	return ordered, nil
}

func describeCycle(providers []Provider, indegree []int) string {
	parts := make([]string, 0)
	for index, degree := range indegree {
		if degree > 0 {
			parts = append(parts, providers[index].Spec.Constructor)
		}
	}
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, " -> ")
}

func loadProvider(ctx context.Context, root, module string, spec scaffold.ProviderSpec) (Provider, error) {
	importPath, symbol, err := splitConstructor(spec.Constructor, module)
	if err != nil {
		return Provider{}, err
	}
	config := &packages.Config{
		Context: ctx,
		Dir:     root,
		Mode:    packages.NeedName | packages.NeedTypes | packages.NeedImports | packages.NeedDeps,
	}
	loaded, err := packages.Load(config, importPath)
	if err != nil {
		return Provider{}, fmt.Errorf("load package %q: %w", importPath, err)
	}
	if len(loaded) != 1 {
		return Provider{}, fmt.Errorf("load package %q: expected one package, got %d", importPath, len(loaded))
	}
	for _, packageErr := range loaded[0].Errors {
		return Provider{}, fmt.Errorf("type-check package %q: %w", importPath, packageErr)
	}
	object := loaded[0].Types.Scope().Lookup(symbol)
	if object == nil {
		return Provider{}, fmt.Errorf("symbol %q is not declared", symbol)
	}
	function, ok := object.(*types.Func)
	if !ok {
		return Provider{}, fmt.Errorf("symbol %q is %s, want function", symbol, object.Type())
	}
	signature, ok := function.Type().(*types.Signature)
	if !ok {
		return Provider{}, fmt.Errorf("symbol %q has invalid function signature", symbol)
	}
	if signature.Results().Len() == 0 || signature.Results().Len() > 3 {
		return Provider{}, fmt.Errorf("symbol %q must return T, (T, error), or (T, cleanup, error)", symbol)
	}
	qualifier := func(pkg *types.Package) string {
		if pkg == nil {
			return ""
		}
		return pkg.Name()
	}
	inputs := make([]string, signature.Params().Len())
	for i := range inputs {
		inputs[i] = types.TypeString(signature.Params().At(i).Type(), qualifier)
	}
	if len(spec.Inputs) > 0 {
		if len(spec.Inputs) != len(inputs) {
			return Provider{}, fmt.Errorf("symbol %q declares %d inputs, signature has %d parameters", symbol, len(spec.Inputs), len(inputs))
		}
		copy(inputs, spec.Inputs)
	}
	results := signature.Results()
	last := results.At(results.Len() - 1).Type()
	returnsError := types.AssignableTo(last, types.Universe.Lookup("error").Type())
	returnsCleanup := !returnsError && results.Len() == 2 && isCleanupType(last)
	if results.Len() == 2 && !returnsError {
		if !returnsCleanup {
			return Provider{}, fmt.Errorf("symbol %q has unsupported two-value return; second value must implement error or di.Cleanup", symbol)
		}
	}
	if returnsError && results.Len() == 1 {
		return Provider{}, fmt.Errorf("symbol %q returns error without a constructed value", symbol)
	}
	if returnsError && results.Len() > 2 {
		if !isCleanupType(results.At(1).Type()) {
			return Provider{}, fmt.Errorf("symbol %q has unsupported return signature; middle value must be di.Cleanup", symbol)
		}
	}
	resultType := results.At(0).Type()
	outputType := types.TypeString(resultType, qualifier)
	outputs, err := inspectOutputs(resultType, qualifier)
	if err != nil {
		return Provider{}, fmt.Errorf("symbol %q: %w", symbol, err)
	}
	if len(outputs) > 0 && strings.TrimSpace(spec.As) != "" {
		return Provider{}, fmt.Errorf("symbol %q returns di.Out and cannot use --as; qualify an Out field with its di tag", symbol)
	}
	if strings.TrimSpace(spec.As) != "" {
		projected, err := loadProjectedType(ctx, root, spec.As)
		if err != nil {
			return Provider{}, err
		}
		if !types.AssignableTo(results.At(0).Type(), projected) {
			return Provider{}, fmt.Errorf("symbol %q result %s does not implement %s", symbol, outputType, spec.As)
		}
		outputType = types.TypeString(projected, qualifier)
	}
	outputKey := outputType
	if spec.Name != "" {
		outputKey += "[" + spec.Name + "]"
	}
	if spec.Group != "" {
		outputKey += "[group=" + spec.Group + "]"
	}
	return Provider{
		Spec: spec, ImportPath: importPath, PackageName: loaded[0].Name,
		Name: symbol, Signature: signature, InputTypes: inputs,
		OutputType: outputType, OutputKey: outputKey, ReturnsError: returnsError,
		ReturnsCleanup: returnsCleanup || (returnsError && results.Len() == 3), Outputs: outputs,
	}, nil
}

func isCleanupType(typeOf types.Type) bool {
	named, ok := typeOf.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Name() == "Cleanup" && named.Obj().Pkg().Path() == "github.com/keelab/keelith/di"
}

func inspectOutputs(result types.Type, qualifier func(*types.Package) string) ([]Output, error) {
	structure, ok := result.Underlying().(*types.Struct)
	if !ok {
		return nil, nil
	}
	out := false
	for index := 0; index < structure.NumFields(); index++ {
		field := structure.Field(index)
		if field.Embedded() && isOutType(field.Type()) {
			out = true
			break
		}
	}
	if !out {
		return nil, nil
	}
	outputs := make([]Output, 0, structure.NumFields()-1)
	for index := 0; index < structure.NumFields(); index++ {
		field := structure.Field(index)
		if field.Embedded() && isOutType(field.Type()) {
			continue
		}
		if !field.Exported() {
			return nil, fmt.Errorf("di.Out field %s is not exported", field.Name())
		}
		name, group, err := parseBindingTag(structure.Tag(index))
		if err != nil {
			return nil, fmt.Errorf("di.Out field %s: %w", field.Name(), err)
		}
		fieldType := types.TypeString(field.Type(), qualifier)
		key := fieldType
		if name != "" {
			key += "[" + name + "]"
		}
		if group != "" {
			key += "[group=" + group + "]"
		}
		outputs = append(outputs, Output{Field: field.Name(), Type: fieldType, Name: name, Group: group, Key: key})
	}
	if len(outputs) == 0 {
		return nil, fmt.Errorf("di.Out has no results")
	}
	return outputs, nil
}

func isOutType(typeOf types.Type) bool {
	named, ok := typeOf.(*types.Named)
	if !ok {
		return false
	}
	object := named.Obj()
	return object.Name() == "Out" && object.Pkg() != nil && object.Pkg().Path() == "github.com/keelab/keelith/di"
}

func parseBindingTag(tag string) (string, string, error) {
	if tag == "" {
		return "", "", nil
	}
	parts := strings.Split(tag, ",")
	name := strings.TrimSpace(parts[0])
	if name != parts[0] {
		return "", "", fmt.Errorf("binding name is not normalized")
	}
	group := ""
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "group=") {
			group = strings.TrimPrefix(part, "group=")
			continue
		}
		if part != "" && part != "optional" {
			return "", "", fmt.Errorf("unknown tag option %q", part)
		}
	}
	return name, group, nil
}

func loadProjectedType(ctx context.Context, root, value string) (types.Type, error) {
	separator := strings.LastIndexByte(value, '.')
	if separator <= 0 || separator == len(value)-1 {
		return nil, fmt.Errorf("as type %q must be import/path.Type", value)
	}
	path, name := value[:separator], value[separator+1:]
	loaded, err := packages.Load(&packages.Config{Context: ctx, Dir: root, Mode: packages.NeedName | packages.NeedTypes | packages.NeedDeps}, path)
	if err != nil {
		return nil, fmt.Errorf("load as type package %q: %w", path, err)
	}
	if len(loaded) != 1 {
		return nil, fmt.Errorf("load as type package %q: expected one package, got %d", path, len(loaded))
	}
	if len(loaded[0].Errors) > 0 {
		return nil, fmt.Errorf("type-check as type package %q: %w", path, loaded[0].Errors[0])
	}
	object := loaded[0].Types.Scope().Lookup(name)
	if object == nil {
		return nil, fmt.Errorf("as type %q is not declared", value)
	}
	typeName, ok := object.(*types.TypeName)
	if !ok {
		return nil, fmt.Errorf("as type %q must be an interface", value)
	}
	if _, ok := typeName.Type().Underlying().(*types.Interface); !ok {
		return nil, fmt.Errorf("as type %q must be an interface", value)
	}
	return typeName.Type(), nil
}

func splitConstructor(value, module string) (string, string, error) {
	value = strings.TrimSpace(value)
	if value == "" || value != strings.TrimSpace(value) {
		return "", "", fmt.Errorf("constructor is empty or not normalized")
	}
	separator := strings.LastIndexByte(value, '.')
	if separator <= 0 || separator == len(value)-1 {
		return "", "", fmt.Errorf("constructor must be import/path.Symbol")
	}
	importPath := value[:separator]
	symbol := value[separator+1:]
	if strings.HasPrefix(importPath, "./") {
		importPath = strings.TrimSuffix(module, "/") + "/" + strings.TrimPrefix(importPath, "./")
	}
	if strings.ContainsAny(importPath, " \t\r\n") || strings.ContainsAny(symbol, " \t\r\n") {
		return "", "", fmt.Errorf("constructor contains whitespace")
	}
	return importPath, symbol, nil
}
