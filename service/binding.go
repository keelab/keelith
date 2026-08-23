package service

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/keelab/keelith/middleware"
	transporthttp "github.com/keelab/keelith/transport/http"
	"google.golang.org/grpc"
)

var (
	// ErrInvalidBinding reports an incomplete generated service binding.
	ErrInvalidBinding = errors.New("service: invalid binding")
	// ErrDuplicateBinding reports duplicate service names in one Profile.
	ErrDuplicateBinding = errors.New("service: duplicate binding")
	// ErrInvalidGroup reports an incomplete immutable service Group.
	ErrInvalidGroup = errors.New("service: invalid group")
	// ErrDuplicateGroup reports duplicate Group names in one Profile.
	ErrDuplicateGroup = errors.New("service: duplicate group")
)

// HTTPRegistrar binds one service implementation to a Keelith HTTP Router.
type HTTPRegistrar func(*transporthttp.Router) error

// GRPCRegistrar binds one service implementation to a grpc-go registrar.
type GRPCRegistrar func(grpc.ServiceRegistrar) error

// BindingSpec describes generated transport registration for one service.
// Application code normally receives Binding values from generated BindXxx
// functions instead of constructing this type directly.
type BindingSpec struct {
	Name           string
	Implementation any
	RegisterHTTP   HTTPRegistrar
	RegisterGRPC   GRPCRegistrar
}

// BindingOption decorates one generated service binding.
type BindingOption interface {
	applyBinding(*bindingOptions) error
}

type bindingOptionFunc func(*bindingOptions) error

func (f bindingOptionFunc) applyBinding(options *bindingOptions) error {
	return f(options)
}

type bindingOptions struct {
	httpBundles []*middleware.Bundle
	grpcBundles []*middleware.Bundle
}

// WithHTTPBundle appends one or more reusable HTTP middleware chains to this service.
func WithHTTPBundle(bundles ...*middleware.Bundle) BindingOption {
	return bindingOptionFunc(func(options *bindingOptions) error {
		for index, bundle := range bundles {
			if bundle == nil {
				return fmt.Errorf("http middleware bundle %d is nil", index)
			}
		}
		options.httpBundles = append(options.httpBundles, bundles...)
		return nil
	})
}

// WithGRPCBundle appends one or more reusable gRPC middleware chains to this service.
func WithGRPCBundle(bundles ...*middleware.Bundle) BindingOption {
	return bindingOptionFunc(func(options *bindingOptions) error {
		for index, bundle := range bundles {
			if bundle == nil {
				return fmt.Errorf("grpc middleware bundle %d is nil", index)
			}
		}
		options.grpcBundles = append(options.grpcBundles, bundles...)
		return nil
	})
}

// Binding is an immutable generated service-to-transport association.
type Binding struct {
	name           string
	group          string
	implementation any
	registerHTTP   HTTPRegistrar
	registerGRPC   GRPCRegistrar
	httpBundle     *middleware.Bundle
	grpcBundle     *middleware.Bundle
	err            error
}

func (binding Binding) appendProfile(entries *profileEntries) error {
	entries.bindings = append(entries.bindings, binding)
	return nil
}

// NewBinding snapshots generated registration and application options.
// Invalid options are retained and reported by NewProfile or Validate so the
// generated BindXxx call remains a single composable expression.
func NewBinding(spec BindingSpec, optionList ...BindingOption) Binding {
	options := bindingOptions{}
	var failures []error
	for index, option := range optionList {
		if option == nil {
			failures = append(failures, fmt.Errorf("binding option %d is nil", index))
			continue
		}
		if err := option.applyBinding(&options); err != nil {
			failures = append(failures, fmt.Errorf("binding option %d: %w", index, err))
		}
	}
	name := strings.TrimSpace(spec.Name)
	httpBundle, httpErr := scopedBindingBundle(name, options.httpBundles)
	if httpErr != nil {
		failures = append(failures, fmt.Errorf("http middleware: %w", httpErr))
	}
	grpcBundle, grpcErr := scopedBindingBundle(name, options.grpcBundles)
	if grpcErr != nil {
		failures = append(failures, fmt.Errorf("grpc middleware: %w", grpcErr))
	}
	return Binding{
		name:           name,
		implementation: spec.Implementation,
		registerHTTP:   spec.RegisterHTTP,
		registerGRPC:   spec.RegisterGRPC,
		httpBundle:     httpBundle,
		grpcBundle:     grpcBundle,
		err:            errors.Join(failures...),
	}
}

func scopedBindingBundle(
	service string,
	bundles []*middleware.Bundle,
) (*middleware.Bundle, error) {
	if len(bundles) == 0 {
		return nil, nil
	}
	combined, err := middleware.CombineBundles(bundles...)
	if err != nil {
		return nil, err
	}
	return middleware.ScopeToService(service, combined)
}

// Name returns the stable fully-qualified service name.
func (binding Binding) Name() string { return binding.name }

// Validate verifies generated registration and application options.
func (binding Binding) Validate() error {
	if binding.err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBinding, binding.err)
	}
	if binding.name == "" || len(binding.name) > maxProfileNameBytes || strings.TrimSpace(binding.name) != binding.name {
		return fmt.Errorf("%w: service name is empty or not normalized", ErrInvalidBinding)
	}
	if isNilImplementation(binding.implementation) {
		return fmt.Errorf("%w: service %q implementation is nil", ErrInvalidBinding, binding.name)
	}
	if binding.registerHTTP == nil && binding.registerGRPC == nil {
		return fmt.Errorf("%w: service %q has no transport", ErrInvalidBinding, binding.name)
	}
	if binding.httpBundle != nil && binding.registerHTTP == nil {
		return fmt.Errorf("%w: service %q has http middleware without http transport", ErrInvalidBinding, binding.name)
	}
	if binding.grpcBundle != nil && binding.registerGRPC == nil {
		return fmt.Errorf("%w: service %q has grpc middleware without grpc transport", ErrInvalidBinding, binding.name)
	}
	return nil
}

func isNilImplementation(implementation any) bool {
	if implementation == nil {
		return true
	}
	value := reflect.ValueOf(implementation)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
