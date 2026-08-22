package outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/keelab/keelith/outbox"
)

func TestDispatcherPublishesReschedulesAndTerminatesDurableRows(t *testing.T) {
	repository := &fakeRepository{messages: []outbox.Message{
		message("success", 1),
		message("retry", 2),
		message("terminal", 3),
	}}
	publisher := &fakePublisher{
		failures: map[string]error{
			"retry":    errors.New("temporary"),
			"terminal": errors.New("permanent"),
		},
	}
	dispatcher := mustDispatcher(t, repository, publisher, 3)
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	eventually(t, func() bool {
		description := dispatcher.Description()
		return description.Published == 1 &&
			description.Rescheduled == 1 &&
			description.Terminal == 1
	})
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := dispatcher.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	completed, rescheduled := repository.results()
	if len(completed) != 1 || completed[0] != "success" {
		t.Fatalf("completed = %v", completed)
	}
	if len(rescheduled) != 2 ||
		rescheduled[0].reason != "publish_failed" ||
		rescheduled[0].terminal ||
		!rescheduled[1].terminal {
		t.Fatalf("rescheduled = %#v", rescheduled)
	}
	description := dispatcher.Description()
	if description.Running ||
		!description.StopRequested ||
		description.Claimed != 3 ||
		description.PublisherFailures != 2 {
		t.Fatalf("Description() = %#v", description)
	}
}

func TestDispatcherCurrentFailuresRecoverWithoutErasingHistory(t *testing.T) {
	repository := &fakeRepository{messages: []outbox.Message{
		message("temporary-failure", 1),
		message("recovery", 1),
	}}
	publisher := &fakePublisher{failures: map[string]error{
		"temporary-failure": errors.New("temporary"),
	}}
	dispatcher := mustDispatcher(t, repository, publisher, 3)
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	eventually(t, func() bool {
		description := dispatcher.Description()
		return description.PublisherFailures == 1 &&
			description.Published == 1
	})
	description := dispatcher.Description()
	if description.ConsecutivePublisherFailures != 0 ||
		description.PublisherFailures != 1 {
		t.Fatalf("Description() after publisher recovery = %#v", description)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestDispatcherRepositoryFailuresRecoverWithoutErasingHistory(t *testing.T) {
	base := &fakeRepository{messages: []outbox.Message{
		message("repository-recovery", 1),
	}}
	repository := &failOnceClaimRepository{Repository: base}
	dispatcher := mustDispatcher(t, repository, &fakePublisher{}, 3)
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	eventually(t, func() bool {
		return dispatcher.Description().Published == 1
	})
	description := dispatcher.Description()
	if description.RepositoryFailures != 1 ||
		description.ConsecutiveRepositoryFailures != 0 {
		t.Fatalf("Description() after repository recovery = %#v", description)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestStopDrainsInflightPublishWithoutCancelingIt(t *testing.T) {
	repository := &fakeRepository{messages: []outbox.Message{
		message("slow", 1),
	}}
	started := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan error, 1)
	publisher := publisherFunc(func(
		ctx context.Context,
		_ outbox.Message,
	) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			canceled <- context.Cause(ctx)
			return context.Cause(ctx)
		}
	})
	dispatcher := mustDispatcher(t, repository, publisher, 3)
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started
	stopped := make(chan error, 1)
	go func() {
		stopped <- dispatcher.Stop(context.Background())
	}()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned before publish completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case err := <-canceled:
		t.Fatalf("normal Stop canceled publish: %v", err)
	default:
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	completed, _ := repository.results()
	if len(completed) != 1 || completed[0] != "slow" {
		t.Fatalf("completed = %v", completed)
	}
}

func TestMessageCloneIsIndependentAndValidationIsBounded(t *testing.T) {
	message := outbox.Message{
		ID:          "event-1",
		Destination: "orders.events",
		Key:         []byte("key"),
		Payload:     []byte("payload"),
		Headers:     map[string][]byte{"traceparent": []byte("value")},
	}
	if err := message.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	cloned := message.Clone()
	cloned.Key[0] = 'X'
	cloned.Payload[0] = 'X'
	cloned.Headers["traceparent"][0] = 'X'
	if string(message.Key) != "key" ||
		string(message.Payload) != "payload" ||
		string(message.Headers["traceparent"]) != "value" {
		t.Fatalf("Clone() mutated source: %#v", message)
	}
	message.ID = ""
	if !errors.Is(message.Validate(), outbox.ErrInvalidOption) {
		t.Fatalf("invalid Message.Validate() error = %v", message.Validate())
	}
	message = outbox.Message{
		ID:          "event-2",
		Destination: "orders.events",
		Key:         make([]byte, 1024*1024+1),
	}
	if !errors.Is(message.Validate(), outbox.ErrInvalidOption) {
		t.Fatalf("oversized key validation error = %v", message.Validate())
	}
}

func TestDispatcherRejectsRepositoryBatchLargerThanClaimLimit(t *testing.T) {
	messages := make([]outbox.Message, 11)
	for index := range messages {
		messages[index] = message("event-"+string(rune('a'+index)), 1)
	}
	dispatcher := mustDispatcher(
		t,
		&fakeRepository{messages: messages},
		&fakePublisher{},
		3,
	)
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Wait(); err == nil {
		t.Fatal("Wait() error = nil, want oversized batch failure")
	}
	description := dispatcher.Description()
	if description.RepositoryFailures != 1 || description.Published != 0 {
		t.Fatalf("Description() = %#v", description)
	}
}

func TestDispatcherContainsPublisherAndClassifierPanics(t *testing.T) {
	repository := &fakeRepository{messages: []outbox.Message{
		message("panic", 1),
	}}
	dispatcher, err := outbox.New(outbox.Config{
		Name:       "orders-outbox",
		Owner:      "instance-a",
		Repository: repository,
		Publisher: publisherFunc(func(context.Context, outbox.Message) error {
			panic("publisher secret")
		}),
		PollInterval:   time.Millisecond,
		ErrorDelay:     time.Millisecond,
		LeaseTTL:       time.Second,
		PublishTimeout: 100 * time.Millisecond,
		BatchSize:      10,
		MaxAttempts:    3,
		RetryBase:      time.Millisecond,
		RetryMax:       time.Second,
		ClassifyFailure: func(error) string {
			panic("classifier secret")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return dispatcher.Description().Rescheduled == 1
	})
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	_, rescheduled := repository.results()
	if len(rescheduled) != 1 ||
		rescheduled[0].reason != "publish_failed" {
		t.Fatalf("rescheduled = %#v", rescheduled)
	}
}

func mustDispatcher(
	t *testing.T,
	repository outbox.Repository,
	publisher outbox.Publisher,
	maxAttempts int,
) *outbox.Dispatcher {
	t.Helper()
	dispatcher, err := outbox.New(outbox.Config{
		Name:           "orders-outbox",
		Owner:          "instance-a",
		Repository:     repository,
		Publisher:      publisher,
		PollInterval:   time.Millisecond,
		ErrorDelay:     time.Millisecond,
		LeaseTTL:       time.Second,
		PublishTimeout: 100 * time.Millisecond,
		BatchSize:      10,
		MaxAttempts:    maxAttempts,
		RetryBase:      time.Millisecond,
		RetryMax:       time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return dispatcher
}

func message(id string, attempts int) outbox.Message {
	return outbox.Message{
		ID:          id,
		Destination: "orders.events",
		Payload:     []byte(id),
		Headers:     map[string][]byte{},
		Attempts:    attempts,
	}
}

type reschedule struct {
	id       string
	terminal bool
	reason   string
	next     time.Time
}

type fakeRepository struct {
	mu sync.Mutex

	messages    []outbox.Message
	claimed     bool
	completed   []string
	rescheduled []reschedule
}

type failOnceClaimRepository struct {
	outbox.Repository

	mu     sync.Mutex
	failed bool
}

func (repository *failOnceClaimRepository) Claim(
	ctx context.Context,
	request outbox.ClaimRequest,
) ([]outbox.Message, error) {
	repository.mu.Lock()
	if !repository.failed {
		repository.failed = true
		repository.mu.Unlock()
		return nil, errors.New("temporary repository failure")
	}
	repository.mu.Unlock()
	return repository.Repository.Claim(ctx, request)
}

func (repository *fakeRepository) Claim(
	ctx context.Context,
	_ outbox.ClaimRequest,
) ([]outbox.Message, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.claimed {
		return nil, nil
	}
	repository.claimed = true
	result := make([]outbox.Message, len(repository.messages))
	for index, message := range repository.messages {
		result[index] = message.Clone()
	}
	return result, nil
}

func (repository *fakeRepository) Complete(
	_ context.Context,
	_ string,
	id string,
) error {
	repository.mu.Lock()
	repository.completed = append(repository.completed, id)
	repository.mu.Unlock()
	return nil
}

func (repository *fakeRepository) Reschedule(
	_ context.Context,
	_ string,
	id string,
	next time.Time,
	terminal bool,
	reason string,
) error {
	repository.mu.Lock()
	repository.rescheduled = append(repository.rescheduled, reschedule{
		id:       id,
		terminal: terminal,
		reason:   reason,
		next:     next,
	})
	repository.mu.Unlock()
	return nil
}

func (repository *fakeRepository) results() (
	[]string,
	[]reschedule,
) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return append([]string(nil), repository.completed...),
		append([]reschedule(nil), repository.rescheduled...)
}

type fakePublisher struct {
	failures map[string]error
}

func (publisher *fakePublisher) Publish(
	_ context.Context,
	message outbox.Message,
) error {
	return publisher.failures[message.ID]
}

type publisherFunc func(context.Context, outbox.Message) error

func (function publisherFunc) Publish(
	ctx context.Context,
	message outbox.Message,
) error {
	return function(ctx, message)
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition was not satisfied")
		case <-ticker.C:
		}
	}
}
