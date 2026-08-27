package cron

import (
	"context"
	"fmt"
	"time"

	"github.com/keelab/keelith/operation"
	"github.com/keelab/keelith/worker"
)

type executionTask struct {
	ID          string
	scheduledAt time.Time
	handler     worker.JobHandler
	context     context.Context
	cancel      func()
	pulling     context.Context
	results     chan<- worker.Result
}

func (scheduler *Scheduler) execute(task executionTask) {
	result := worker.Nack(context.Canceled)
	defer func() {
		if recovered := recover(); recovered != nil {
			result = worker.Nack(fmt.Errorf(
				"%w: %v",
				ErrHandlerPanic,
				recovered,
			))
		}
		task.cancel()
		scheduler.finish(result)
		task.results <- result
		close(task.results)
	}()

	for attempt := 1; ; attempt++ {
		if cause := context.Cause(task.context); cause != nil {
			result = worker.Nack(cause)
			return
		}
		scheduler.recordAttempt()
		execution := worker.NewExecution(
			task.ID,
			task.scheduledAt,
			scheduler.config.payload,
			scheduler.config.metadata,
		)
		result = normalizeResult(task.handler(
			operation.WithAttempt(task.context, attempt),
			execution,
		))
		if result.Action() != worker.ActionRetry ||
			attempt > scheduler.config.maxRetries {
			return
		}
		if !scheduler.canRetry() {
			return
		}
		if !waitRetry(
			task.context,
			task.pulling,
			result.RetryAfter(),
		) {
			return
		}
		if !scheduler.canRetry() {
			return
		}
		scheduler.recordRetry()
	}
}

func (scheduler *Scheduler) canRetry() bool {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	return scheduler.state == StateRunning && scheduler.accepting
}

func (scheduler *Scheduler) recordAttempt() {
	scheduler.mu.Lock()
	scheduler.description.Attempts++
	scheduler.description.LastStartedAt =
		time.Now().In(scheduler.config.location)
	scheduler.mu.Unlock()
}

func (scheduler *Scheduler) recordRetry() {
	scheduler.mu.Lock()
	scheduler.description.Retries++
	scheduler.mu.Unlock()
}

func (scheduler *Scheduler) finish(result worker.Result) {
	scheduler.mu.Lock()
	scheduler.description.Completed++
	scheduler.description.LastCompletedAt =
		time.Now().In(scheduler.config.location)
	scheduler.description.LastAction = result.Action()
	scheduler.description.LastFailed = result.Cause() != nil
	if result.Cause() != nil {
		scheduler.description.Failures++
	}
	scheduler.active--
	if scheduler.active == 0 {
		close(scheduler.drained)
		if scheduler.state == StateClosed {
			scheduler.doneOnce.Do(func() { close(scheduler.done) })
		}
	}
	scheduler.mu.Unlock()
}

func normalizeResult(result worker.Result) worker.Result {
	switch result.Action() {
	case worker.ActionAck:
		if result.Cause() == nil && result.RetryAfter() == 0 {
			return result
		}
	case worker.ActionNack, worker.ActionDeadLetter:
		if result.Cause() != nil && result.RetryAfter() == 0 {
			return result
		}
	case worker.ActionRetry:
		if result.Cause() != nil && result.RetryAfter() >= 0 {
			return result
		}
	}
	return worker.Nack(worker.ErrInvalidResult)
}

func waitRetry(
	ctx context.Context,
	pulling context.Context,
	delay time.Duration,
) bool {
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return false
	case <-pulling.Done():
		return false
	case <-timer.C:
		return true
	}
}

func mergeContexts(
	caller context.Context,
	runtime context.Context,
) (context.Context, func()) {
	merged, cancel := context.WithCancelCause(caller)
	stop := context.AfterFunc(runtime, func() {
		cancel(context.Cause(runtime))
	})
	return merged, func() {
		stop()
		cancel(nil)
	}
}
