package health

import (
	"context"
	"errors"
	"time"
)

type checkerOutput struct {
	phase           Phase
	startupComplete bool
	checkers        map[string]Checker
	checkTimeout    time.Duration
	cacheTTL        time.Duration
	generation      uint64
	cached          Report
	cacheHit        bool
}

type checkerOutcome struct {
	name   string
	result Result
}

func runCheckers(
	ctx context.Context,
	timeout time.Duration,
	names []string,
	checkers map[string]Checker,
	now time.Time,
) []CheckResult {
	if len(names) == 0 {
		return nil
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	outcomes := make(chan checkerOutcome, len(names))

	for _, name := range names {
		name := name
		checker := checkers[name]

		go func() {
			outcomes <- checkerOutcome{
				name:   name,
				result: runChecker(checkCtx, checker, now),
			}
		}()
	}

	pending := make(map[string]struct{}, len(names))

	for _, name := range names {
		pending[name] = struct{}{}
	}
	results := make([]CheckResult, 0, len(names))

	for len(pending) > 0 {
		select {
		case outcome := <-outcomes:
			if _, ok := pending[outcome.name]; !ok {
				continue
			}
			delete(pending, outcome.name)
			results = append(results, namedResult(outcome.name, outcome.result, now))
		case <-checkCtx.Done():
			reason := checkFailureReason(ctx, checkCtx)
			for _, name := range names {
				if _, ok := pending[name]; !ok {
					continue
				}
				results = append(results, namedResult(name, Fail(reason), now))
				delete(pending, name)
			}
		}
	}

	return results
}

func checkFailureReason(parent, bounded context.Context) string {
	cause := context.Cause(bounded)
	if errors.Is(cause, context.DeadlineExceeded) && context.Cause(parent) == nil {
		return "checker timed out"
	}
	if cause != nil {
		return cause.Error()
	}

	return "checker canceled"
}

func cloneReport(report Report) Report {
	cloned := report
	cloned.Checks = append([]CheckResult(nil), report.Checks...)
	return cloned
}
