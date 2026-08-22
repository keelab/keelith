package ops

import (
	"context"
	"fmt"
	"net/http"

	"github.com/keelab/keelith/contract"
)

// ContractStatusProvider obtains a bounded static contract/dependency graph.
type ContractStatusProvider func(context.Context) (contract.Description, error)

// WithContractStatus exposes generated contract topology at
// GET /debug/contracts.
func WithContractStatus(provider ContractStatusProvider) Option {
	return optionFunc(func(options *options) error {
		if provider == nil {
			return fmt.Errorf("contract status provider is nil")
		}
		options.contractStatus = provider
		return nil
	})
}

// ContractCatalogStatus adapts an immutable generated-manifest Catalog.
func ContractCatalogStatus(catalog *contract.Catalog) ContractStatusProvider {
	if catalog == nil {
		return nil
	}
	return func(ctx context.Context) (contract.Description, error) {
		if ctx == nil {
			return contract.Description{}, fmt.Errorf(
				"ops: contract status context is nil",
			)
		}
		if cause := context.Cause(ctx); cause != nil {
			return contract.Description{}, cause
		}
		return catalog.Describe(), nil
	}
}

func contractStatusHandler(provider ContractStatusProvider) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		status, err := readContractStatus(request.Context(), provider)
		if err != nil || contract.ValidateDescription(status) != nil {
			writeDiagnosticError(
				writer,
				http.StatusServiceUnavailable,
				"unavailable",
			)
			return
		}
		writeDiagnosticJSON(writer, http.StatusOK, status)
	})
}

func readContractStatus(ctx context.Context, provider ContractStatusProvider) (status contract.Description, err error) {
	defer func() {
		if recover() != nil {
			status = contract.Description{}
			err = fmt.Errorf("ops: contract status provider panic")
		}
	}()
	return provider(ctx)
}
