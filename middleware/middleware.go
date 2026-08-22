// Package middleware defines Keelith's transport-neutral invocation model.
package middleware

import "context"

// Handler executes one transport-neutral request.
type Handler func(context.Context, any) (any, error)

// Middleware wraps a Handler.
type Middleware func(Handler) Handler
