package di

import (
	"context"
	"fmt"
	"reflect"
)

// RootSet is an explicit multi-root object. Applications define a struct that
// embeds In and lists HTTP, worker, scheduler, or administrative roots.
type RootSet interface{ rootSet() }

// Roots marks a dependency input struct as a multi-process root set.
type Roots struct{ In }

func (Roots) rootSet() {}

// BuildRoots constructs multiple roots in one graph so application-scoped
// providers are shared exactly once across protocol and worker entrypoints.
func BuildRoots[T RootSet](ctx context.Context, modules ...Module) (*Graph, T, error) {
	var zero T
	rootModule, err := rootsModule[T](modules)
	if err != nil {
		return nil, zero, err
	}
	return Build[T](ctx, rootModule)
}

// ValidateRoots statically validates the same synthetic root module used by
// BuildRoots without constructing providers.
func ValidateRoots[T RootSet](modules ...Module) (Description, error) {
	rootModule, err := rootsModule[T](modules)
	if err != nil {
		return Description{}, err
	}
	return Validate[T](rootModule)
}

func rootsModule[T RootSet](modules []Module) (Module, error) {
	typeOf := reflect.TypeOf((*T)(nil)).Elem()
	if typeOf.Kind() != reflect.Struct || !embedsMarker(typeOf, reflect.TypeOf(Roots{})) {
		return Module{}, fmt.Errorf("%w: root set must embed di.Roots", ErrInvalidModule)
	}
	constructor := reflect.MakeFunc(reflect.FuncOf(rootFieldTypes(typeOf), []reflect.Type{typeOf}, false), func(values []reflect.Value) []reflect.Value {
		result := reflect.New(typeOf).Elem()
		valueIndex := 0
		for index := 0; index < typeOf.NumField(); index++ {
			if typeOf.Field(index).Anonymous && typeOf.Field(index).Type == reflect.TypeOf(Roots{}) {
				continue
			}
			result.Field(index).Set(values[valueIndex])
			valueIndex++
		}
		return []reflect.Value{result}
	}).Interface()
	return NewModule("keelith.roots", Include(modules...), Provide(constructor))
}

func rootFieldTypes(typeOf reflect.Type) []reflect.Type {
	result := make([]reflect.Type, 0, typeOf.NumField()-1)
	for index := 0; index < typeOf.NumField(); index++ {
		field := typeOf.Field(index)
		if field.Anonymous && field.Type == reflect.TypeOf(Roots{}) {
			continue
		}
		result = append(result, field.Type)
	}
	return result
}
