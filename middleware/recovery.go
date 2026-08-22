package middleware

import (
	"context"
	"fmt"
	"runtime/debug"

	kerrors "github.com/keelab/keelith/errors"
)

// PanicReport contains diagnostic panic data delivered only to an explicit
// reporter. It is never placed in a transport response.
type PanicReport struct {
	Type  string
	Stack []byte
}

// PanicReporter receives recovered panic diagnostics.
type PanicReporter func(context.Context, PanicReport)

// Recovery converts panics to a stable internal Error.
func Recovery(reporter PanicReporter) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, request any) (response any, err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					if reporter != nil {
						report(ctx, reporter, PanicReport{
							Type:  fmt.Sprintf("%T", recovered),
							Stack: append([]byte(nil), debug.Stack()...),
						})
					}
					response = nil
					err = kerrors.New(500, "INTERNAL", "internal server error")
				}
			}()
			return next(ctx, request)
		}
	}
}

func report(ctx context.Context, reporter PanicReporter, panicReport PanicReport) {
	defer func() { _ = recover() }()
	reporter(ctx, panicReport)
}
