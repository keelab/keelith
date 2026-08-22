package outbox

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReplayRequestRequiresExactBoundedInput(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	valid := ReplayRequest{
		IDs:            []string{"event-1", "event-2"},
		ExpectedReason: "broker_rejected",
		AvailableAt:    now,
	}
	if err := valid.Validate(now); err != nil {
		t.Fatalf("valid request error = %v", err)
	}
	testCases := []ReplayRequest{
		{},
		{
			IDs:            []string{"event-1", "event-1"},
			ExpectedReason: "broker_rejected",
			AvailableAt:    now,
		},
		{
			IDs:            []string{"event-1"},
			ExpectedReason: "broker_rejected",
			AvailableAt:    now.Add(-time.Nanosecond),
		},
		{
			IDs:            []string{"event-1"},
			ExpectedReason: strings.Repeat("x", 65),
			AvailableAt:    now,
		},
	}
	for index, request := range testCases {
		if err := request.Validate(now); !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("request %d error = %v", index, err)
		}
	}
}

func TestRetentionRequestIsBounded(t *testing.T) {
	request := RetentionRequest{
		PublishedBefore: time.Now(),
		TerminalBefore:  time.Now(),
		Limit:           1000,
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid request error = %v", err)
	}
	request.Limit = 10_001
	if err := request.Validate(); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("unbounded request error = %v", err)
	}
}
