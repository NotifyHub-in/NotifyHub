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

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/id"
	"github.com/your-org/notification-control-plane/libs/storage/postgres"
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

func NewNotifier(store *postgres.Store) *Notifier {
	return &Notifier{
		client: &http.Client{Timeout: 5 * time.Second},
		store:  store,
	}
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
