package di

import "errors"

var (
	// ErrInvalidModule reports malformed module declarations.
	ErrInvalidModule = errors.New("di: invalid module")
	// ErrInvalidProvider reports unsupported constructor signatures.
	ErrInvalidProvider = errors.New("di: invalid provider")
	// ErrMissingProvider reports an unresolved required dependency.
	ErrMissingProvider = errors.New("di: missing provider")
	// ErrDuplicateProvider reports an ambiguous binding.
	ErrDuplicateProvider = errors.New("di: duplicate provider")
	// ErrDependencyCycle reports a constructor dependency cycle.
	ErrDependencyCycle = errors.New("di: dependency cycle")
	// ErrScopeViolation reports an application value capturing a transient value.
	ErrScopeViolation = errors.New("di: scope violation")
	// ErrProviderFailed reports a returned error or recovered constructor panic.
	ErrProviderFailed = errors.New("di: provider failed")
	// ErrCleanupFailed reports one or more cleanup failures.
	ErrCleanupFailed = errors.New("di: cleanup failed")
	// ErrInvalidOverride reports an override without an existing binding.
	ErrInvalidOverride = errors.New("di: invalid override")
	// ErrInvalidLazy reports a zero-value lazy dependency.
	ErrInvalidLazy = errors.New("di: invalid lazy dependency")
)
