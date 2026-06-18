package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
	"github.com/sony/gobreaker"
)

func TestParsePositiveInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		fallback int
		want     int
	}{
		{name: "valid", raw: "7", fallback: 3, want: 7},
		{name: "zero uses fallback", raw: "0", fallback: 3, want: 3},
		{name: "negative uses fallback", raw: "-4", fallback: 3, want: 3},
		{name: "invalid uses fallback", raw: "abc", fallback: 3, want: 3},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := parsePositiveInt(tc.raw, tc.fallback)
			if got != tc.want {
				t.Fatalf("parsePositiveInt(%q, %d) = %d, want %d", tc.raw, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestClassifyHTTPStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		statusCode int
		wantClass  notification.FailureClass
		wantRetry  bool
	}{
		{statusCode: http.StatusTooManyRequests, wantClass: notification.FailureClassRateLimited, wantRetry: true},
		{statusCode: http.StatusUnauthorized, wantClass: notification.FailureClassUnauthorized, wantRetry: false},
		{statusCode: http.StatusBadRequest, wantClass: notification.FailureClassInvalidRequest, wantRetry: false},
		{statusCode: http.StatusBadGateway, wantClass: notification.FailureClassTransient, wantRetry: true},
		{statusCode: http.StatusNotFound, wantClass: notification.FailureClassPermanent, wantRetry: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(http.StatusText(tc.statusCode), func(t *testing.T) {
			t.Parallel()

			if got := classifyHTTPStatus(tc.statusCode); got != tc.wantClass {
				t.Fatalf("classifyHTTPStatus(%d) = %q, want %q", tc.statusCode, got, tc.wantClass)
			}
			if got := isRetryableHTTPStatus(tc.statusCode); got != tc.wantRetry {
				t.Fatalf("isRetryableHTTPStatus(%d) = %v, want %v", tc.statusCode, got, tc.wantRetry)
			}
		})
	}
}

func TestClassifyConnectorFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		err       error
		wantClass notification.FailureClass
		wantRetry bool
	}{
		{
			name: "structured retryable error",
			err: connectorSendError{
				StatusCode:     http.StatusBadGateway,
				Message:        "provider outage",
				Classification: notification.FailureClassTransient,
				Retryable:      true,
			},
			wantClass: notification.FailureClassTransient,
			wantRetry: true,
		},
		{
			name: "structured invalid request",
			err: connectorSendError{
				StatusCode:     http.StatusBadRequest,
				Message:        "invalid recipient",
				Classification: notification.FailureClassInvalidRequest,
			},
			wantClass: notification.FailureClassInvalidRequest,
			wantRetry: false,
		},
		{
			name:      "deadline exceeded",
			err:       context.DeadlineExceeded,
			wantClass: notification.FailureClassTransient,
			wantRetry: true,
		},
		{
			name:      "missing provider account on binding",
			err:       errors.New("provider binding binding-123 is missing provider_account_id"),
			wantClass: notification.FailureClassMisconfigured,
			wantRetry: false,
		},
		{
			name:      "connection refused text",
			err:       errors.New("dial tcp 127.0.0.1:65535: connect: connection refused"),
			wantClass: notification.FailureClassTransient,
			wantRetry: true,
		},
		{
			name:      "generic permanent error",
			err:       errors.New("unexpected permanent failure"),
			wantClass: notification.FailureClassPermanent,
			wantRetry: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotClass, gotRetry := classifyConnectorFailure(tc.err)
			if gotClass != tc.wantClass {
				t.Fatalf("classifyConnectorFailure(%v) class = %q, want %q", tc.err, gotClass, tc.wantClass)
			}
			if gotRetry != tc.wantRetry {
				t.Fatalf("classifyConnectorFailure(%v) retry = %v, want %v", tc.err, gotRetry, tc.wantRetry)
			}
		})
	}
}

func TestShouldSkipBindingForCircuit(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	if !shouldSkipBindingForCircuit(notification.ProviderBindingHealth{
		CircuitState:  notification.ProviderCircuitStateOpen,
		CooldownUntil: timePtr(now.Add(5 * time.Second)),
	}, now) {
		t.Fatal("expected open circuit with future cooldown to be skipped")
	}

	if shouldSkipBindingForCircuit(notification.ProviderBindingHealth{
		CircuitState:  notification.ProviderCircuitStateOpen,
		CooldownUntil: timePtr(now.Add(-5 * time.Second)),
	}, now) {
		t.Fatal("expected open circuit with expired cooldown not to be skipped")
	}

	if shouldSkipBindingForCircuit(notification.ProviderBindingHealth{
		CircuitState: notification.ProviderCircuitStateClosed,
	}, now) {
		t.Fatal("expected closed circuit not to be skipped")
	}
}

func TestProviderCircuitStateFromGoBreaker(t *testing.T) {
	t.Parallel()

	if got := providerCircuitStateFromGoBreaker(gobreaker.StateOpen); got != notification.ProviderCircuitStateOpen {
		t.Fatalf("providerCircuitStateFromGoBreaker(open) = %q, want %q", got, notification.ProviderCircuitStateOpen)
	}
	if got := providerCircuitStateFromGoBreaker(gobreaker.StateHalfOpen); got != notification.ProviderCircuitStateClosed {
		t.Fatalf("providerCircuitStateFromGoBreaker(half-open) = %q, want %q", got, notification.ProviderCircuitStateClosed)
	}
	if got := providerCircuitStateFromGoBreaker(gobreaker.StateClosed); got != notification.ProviderCircuitStateClosed {
		t.Fatalf("providerCircuitStateFromGoBreaker(closed) = %q, want %q", got, notification.ProviderCircuitStateClosed)
	}
}

func TestIsRetryableFailureClass(t *testing.T) {
	t.Parallel()

	if !isRetryableFailureClass(notification.FailureClassTransient) {
		t.Fatal("transient should be retryable")
	}
	if !isRetryableFailureClass(notification.FailureClassRateLimited) {
		t.Fatal("rate-limited should be retryable")
	}
	if isRetryableFailureClass(notification.FailureClassUnauthorized) {
		t.Fatal("unauthorized should not be retryable")
	}
}

func timePtr(value time.Time) *time.Time {
	return &value
}
