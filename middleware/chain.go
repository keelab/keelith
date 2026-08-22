package middleware

// Chain composes middleware in declaration order.
//
// The first middleware is the outermost invocation. Chain deliberately does
// not recover panics; recovery is an explicit middleware.
func Chain(middlewares ...Middleware) Middleware {
	snapshot := append([]Middleware(nil), middlewares...)
	return func(final Handler) Handler {
		handler := final
		for index := len(snapshot) - 1; index >= 0; index-- {
			handler = snapshot[index](handler)
		}
		return handler
	}
}
