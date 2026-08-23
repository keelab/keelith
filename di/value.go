package di

import (
	"fmt"
	"reflect"
)

// Value registers an existing application-scoped value.
func Value(value any, options ...ProviderOption) Option {
	return valueOption(value, false, options...)
}

// OverrideValue explicitly replaces an existing binding.
func OverrideValue(value any, options ...ProviderOption) Option {
	return valueOption(value, true, options...)
}

func valueOption(value any, override bool, options ...ProviderOption) Option {
	return optionFunc(func(builder *moduleBuilder) error {
		reflected := reflect.ValueOf(value)
		if !reflected.IsValid() {
			return fmt.Errorf("%w: supplied value is nil interface", ErrInvalidProvider)
		}
		item := provider{
			module: builder.name, outputs: []key{{typeOf: reflected.Type()}},
			scope: ApplicationScope, supplied: true, override: override,
			displayName: "value(" + reflected.Type().String() + ")",
			constructor: reflected,
		}
		for index, option := range options {
			if option == nil {
				return fmt.Errorf("%w: value option %d is nil", ErrInvalidProvider, index)
			}
			if err := option.applyProvider(&item); err != nil {
				return err
			}
		}
		builder.providers = append(builder.providers, item)
		return nil
	})
}

// OverrideProvider explicitly replaces bindings produced by a constructor.
func OverrideProvider(constructor any, options ...ProviderOption) Option {
	return registerProvider(constructor, false, true, options...)
}
