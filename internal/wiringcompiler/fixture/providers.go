// Package fixture contains a deliberately small but non-trivial provider
// graph used by wiring compiler tests.
package fixture

import (
	"context"

	"github.com/keelab/keelith/di"
)

// Config is the fixture configuration.
type Config struct{}

// Database is the fixture database.
type Database struct{}

// RepositoryA is the first repository.
type RepositoryA struct{}

// RepositoryB is the second repository.
type RepositoryB struct{}

// ServiceA is the first service.
type ServiceA struct{}

// ServiceB is the second service.
type ServiceB struct{}

// Worker is the worker root value.
type Worker struct{}

// Resources exposes the worker through di.Out.
type Resources struct {
	di.Out
	Worker *Worker `di:"worker"`
}

// NewConfig constructs the fixture configuration.
func NewConfig() Config { return Config{} }

// NewDatabase constructs a cleanup-aware database.
func NewDatabase(Config) (*Database, di.Cleanup, error) {
	return &Database{}, func(context.Context) error { return nil }, nil
}

// NewRepositoryA constructs the first repository.
func NewRepositoryA(*Database) *RepositoryA { return &RepositoryA{} }

// NewRepositoryB constructs the second repository.
func NewRepositoryB(*Database) *RepositoryB { return &RepositoryB{} }

// NewServiceA constructs the first service.
func NewServiceA(*RepositoryA) *ServiceA { return &ServiceA{} }

// NewServiceB constructs the second service.
func NewServiceB(*RepositoryB) *ServiceB { return &ServiceB{} }

// NewWorker constructs the worker root value.
func NewWorker(*ServiceA, *ServiceB) *Worker { return &Worker{} }

// NewResources constructs the Out result.
func NewResources(*Worker) Resources { return Resources{Worker: &Worker{}} }
