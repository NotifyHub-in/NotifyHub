package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
	"github.com/NotifyHub-in/NotifyHub/libs/core/id"
	"github.com/NotifyHub-in/NotifyHub/libs/storage/postgres"
)

const (
	maxWebhookDeliveryAttempts = 3
	webhookBackoff             = time.Second
	maxWebhookResponseBody     = 2048
)

type Notifier struct {
	client *http.Client
	store  *postgres.Store
}

type errorLogger interface {
	Error(msg string, args ...any)
}

func NewNotifier(store *postgres.Store) *Notifier {
	return &Notifier{
		client: &http.Client{Timeout: 5 * time.Second},
		store:  store,
	}
}

func (n *Notifier) NotifyRequestUpdatedAsync(logger errorLogger, requestID string, metadata map[string]interface{}) {
	metadata = cloneInterfaceMap(metadata)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := n.NotifyRequestUpdated(ctx, requestID, metadata); err != nil && logger != nil {
			logger.Error("notify lifecycle webhook failed", "error", err, "request_id", requestID)
		}
	}()
}

func (n *Notifier) NotifyChannelEventAsync(logger errorLogger, event notification.ChannelEvent, metadata map[string]any) {
	event = cloneChannelEvent(event)
	metadata = cloneAnyMap(metadata)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := n.NotifyChannelEvent(ctx, event, metadata); err != nil && logger != nil {
			logger.Error("notify inbound channel webhook failed", "error", err, "provider_key", event.ProviderKey, "event_type", event.EventType)
		}
	}()
}

func (n *Notifier) NotifyRequestUpdated(ctx context.Context, requestID string, metadata map[string]interface{}) error {
	record, err := n.store.GetNotificationRequest(ctx, requestID)
	if err != nil {
		return fmt.Errorf("load notification request: %w", err)
	}
	attempts, err := n.store.ListDeliveryAttempts(ctx, requestID)
	if err != nil {
		return fmt.Errorf("load delivery attempts: %w", err)
	}
	subscriptions, err := n.store.ListEnabledWebhookSubscriptions(ctx)
	if err != nil {
		return fmt.Errorf("load webhook subscriptions: %w", err)
	}

	if len(subscriptions) == 0 {
		return nil
	}

	event := notification.LifecycleWebhookEvent{
		EventType: "notification.request.updated",
		RequestID: requestID,
		Status:    record.Status,
		Request:   record,
		Attempts:  attempts,
		Metadata:  metadata,
		SentAt:    time.Now().UTC(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal lifecycle webhook event: %w", err)
	}

	var deliveryErrors []string
	for _, subscription := range subscriptions {
		if err := n.deliverToSubscription(ctx, subscription, requestID, event, payload); err != nil {
			deliveryErrors = append(deliveryErrors, err.Error())
		}
	}

	if len(deliveryErrors) > 0 {
		return fmt.Errorf("deliver lifecycle webhooks: %s", strings.Join(deliveryErrors, "; "))
	}
	return nil
}

func (n *Notifier) NotifyChannelEvent(ctx context.Context, event notification.ChannelEvent, metadata map[string]any) error {
	subscriptions, err := n.store.ListEnabledWebhookSubscriptions(ctx)
	if err != nil {
		return fmt.Errorf("load webhook subscriptions: %w", err)
	}
	if len(subscriptions) == 0 {
		return nil
	}

	payload, err := json.Marshal(notification.ChannelWebhookEvent{
		EventType: "notification.channel_event.received",
		Event:     event,
		Metadata:  metadata,
		SentAt:    time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal channel webhook event: %w", err)
	}

	var deliveryErrors []string
	for _, subscription := range subscriptions {
		if err := n.deliverWebhookPayload(ctx, subscription, payload); err != nil {
			deliveryErrors = append(deliveryErrors, err.Error())
		}
	}
	if len(deliveryErrors) > 0 {
		return fmt.Errorf("deliver channel webhooks: %s", strings.Join(deliveryErrors, "; "))
	}
	return nil
}

func (n *Notifier) deliverToSubscription(ctx context.Context, subscription notification.WebhookSubscription, requestID string, event notification.LifecycleWebhookEvent, payload []byte) error {
	var lastErr error

	for attemptNumber := 1; attemptNumber <= maxWebhookDeliveryAttempts; attemptNumber++ {
		attempt := notification.WebhookDeliveryAttempt{
			DeliveryID:     id.New(12),
			RequestID:      requestID,
			SubscriptionID: subscription.SubscriptionID,
			EventType:      event.EventType,
			TargetURL:      subscription.TargetURL,
			AttemptNumber:  attemptNumber,
			MaxAttempts:    maxWebhookDeliveryAttempts,
			Status:         notification.WebhookDeliveryAttemptPending,
		}
		if err := n.store.CreateWebhookDeliveryAttempt(ctx, attempt); err != nil {
			return fmt.Errorf("create webhook delivery attempt: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, subscription.TargetURL, bytes.NewReader(payload))
		if err != nil {
			attempt.Status = notification.WebhookDeliveryAttemptFailed
			attempt.ErrorMessage = err.Error()
			_ = n.store.UpdateWebhookDeliveryAttempt(ctx, attempt)
			return fmt.Errorf("build lifecycle webhook request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := n.client.Do(req)
		if err != nil {
			attempt.Status = notification.WebhookDeliveryAttemptFailed
			attempt.ErrorMessage = err.Error()
			_ = n.store.UpdateWebhookDeliveryAttempt(ctx, attempt)
			lastErr = fmt.Errorf("deliver lifecycle webhook to %s: %w", subscription.TargetURL, err)
		} else {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, maxWebhookResponseBody))
			resp.Body.Close()
			attempt.HTTPStatusCode = resp.StatusCode
			attempt.ResponseBody = string(body)
			if resp.StatusCode >= http.StatusBadRequest {
				attempt.Status = notification.WebhookDeliveryAttemptFailed
				attempt.ErrorMessage = fmt.Sprintf("target returned %d", resp.StatusCode)
				lastErr = fmt.Errorf("deliver lifecycle webhook to %s: target returned %d", subscription.TargetURL, resp.StatusCode)
			} else {
				attempt.Status = notification.WebhookDeliveryAttemptSucceeded
				if updateErr := n.store.UpdateWebhookDeliveryAttempt(ctx, attempt); updateErr != nil {
					return fmt.Errorf("update successful webhook delivery attempt: %w", updateErr)
				}
				return nil
			}
			if updateErr := n.store.UpdateWebhookDeliveryAttempt(ctx, attempt); updateErr != nil {
				return fmt.Errorf("update failed webhook delivery attempt: %w", updateErr)
			}
		}

		if attemptNumber < maxWebhookDeliveryAttempts {
			if err := sleepWithContext(ctx, webhookBackoff); err != nil {
				return err
			}
		}
	}

	return lastErr
}

func (n *Notifier) deliverWebhookPayload(ctx context.Context, subscription notification.WebhookSubscription, payload []byte) error {
	var lastErr error
	for attemptNumber := 1; attemptNumber <= maxWebhookDeliveryAttempts; attemptNumber++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, subscription.TargetURL, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("build webhook request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := n.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("deliver webhook to %s: %w", subscription.TargetURL, err)
		} else {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, maxWebhookResponseBody))
			resp.Body.Close()
			if resp.StatusCode >= http.StatusBadRequest {
				lastErr = fmt.Errorf("deliver webhook to %s: target returned %d (%s)", subscription.TargetURL, resp.StatusCode, trimResponse(string(body)))
			} else {
				return nil
			}
		}

		if attemptNumber < maxWebhookDeliveryAttempts {
			if err := sleepWithContext(ctx, webhookBackoff); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func trimResponse(body string) string {
	body = strings.TrimSpace(body)
	if len(body) > 200 {
		return body[:200]
	}
	return body
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func cloneInterfaceMap(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return nil
	}
	cloned := make(map[string]interface{}, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func cloneChannelEvent(event notification.ChannelEvent) notification.ChannelEvent {
	if event.Metadata == nil {
		return event
	}
	cloned := make(map[string]string, len(event.Metadata))
	for key, value := range event.Metadata {
		cloned[key] = value
	}
	event.Metadata = cloned
	return event
}
