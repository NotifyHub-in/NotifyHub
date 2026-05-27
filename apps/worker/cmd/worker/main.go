package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/id"
	"github.com/your-org/notification-control-plane/libs/core/render"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/core/webhooks"
	kafkamq "github.com/your-org/notification-control-plane/libs/messaging/kafka"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
	"github.com/your-org/notification-control-plane/libs/observability/metrics"
	"github.com/your-org/notification-control-plane/libs/storage/postgres"
)

type connectorClient struct {
	baseURL string
	client  *http.Client
}

func (c connectorClient) send(ctx context.Context, req notification.ConnectorSendRequest) (notification.ConnectorSendResponse, error) {
	var response notification.ConnectorSendResponse

	payload, err := json.Marshal(req)
	if err != nil {
		return response, fmt.Errorf("marshal connector request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/send", bytes.NewReader(payload))
	if err != nil {
		return response, fmt.Errorf("build connector request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return response, fmt.Errorf("call connector: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(httpResp.Body)
		return response, fmt.Errorf("connector returned %d: %s", httpResp.StatusCode, string(body))
	}

	if err := json.NewDecoder(httpResp.Body).Decode(&response); err != nil {
		return response, fmt.Errorf("decode connector response: %w", err)
	}

	return response, nil
}

func main() {
	cfg, err := config.LoadHTTPServiceConfig("worker", 8081)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := metrics.NewRegistry(cfg.ServiceName)

	store, err := postgres.Open(ctx, config.MustGetEnv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	postgres.AttachMetrics(registry)
	notifier := webhooks.NewNotifier(store)

	brokers := config.MustGetEnv("KAFKA_BROKERS")
	topic := config.GetEnv("KAFKA_NOTIFICATION_TOPIC", "notification.requests")
	groupID := config.GetEnv("KAFKA_WORKER_GROUP_ID", "notification-worker")
	if err := kafkamq.EnsureTopic(ctx, brokers, topic, 1, 1); err != nil {
		panic(err)
	}

	consumer := kafkamq.NewConsumer(brokers, topic, groupID)
	defer consumer.Close()

	var status sync.Map
	status.Store("state", "starting")
	status.Store("last_heartbeat", "")
	status.Store("last_request_id", "")
	status.Store("last_error", "")

	go func() {
		err := consumer.Consume(ctx, func(messageCtx context.Context, payload []byte) error {
			var plan notification.DeliveryPlan
			if err := json.Unmarshal(payload, &plan); err != nil {
				status.Store("last_error", err.Error())
				logger.Error("unmarshal delivery plan failed", "error", err)
				return nil
			}

			status.Store("state", "processing")
			status.Store("last_request_id", plan.Request.RequestID)
			status.Store("last_heartbeat", time.Now().UTC().Format(time.RFC3339))

			if isExpired(plan.Request, time.Now().UTC()) {
				expiredChannels := plan.Request.Channels
				for _, channel := range expiredChannels {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   1,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptExpired,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  "notification request expired before delivery",
					}
					if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("create expired delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
						continue
					}
					recordDeliveryAttemptMetric(registry, attempt)
				}

				if err := store.UpdateNotificationRequestStatus(messageCtx, plan.Request.RequestID, notification.RequestStatusExpired); err != nil {
					status.Store("last_error", err.Error())
					logger.Error("update expired notification request status failed", "error", err, "request_id", plan.Request.RequestID)
					return nil
				}
				registry.IncCounter("notification_request_final_status_total", "Final notification request statuses produced by the worker.", map[string]string{
					"status": string(notification.RequestStatusExpired),
				})
				registry.ObserveHistogram("notification_end_to_end_seconds", "End-to-end notification latency in seconds from API acceptance to observed terminal or dispatched state.", map[string]string{
					"stage":        "worker",
					"final_status": string(notification.RequestStatusExpired),
				}, metrics.DefaultLatencyBuckets(), time.Since(plan.Request.RequestedAt).Seconds())
				if err := notifier.NotifyRequestUpdated(messageCtx, plan.Request.RequestID, map[string]interface{}{"source": "worker"}); err != nil {
					logger.Error("notify lifecycle webhook failed", "error", err, "request_id", plan.Request.RequestID)
				}

				status.Store("state", "idle")
				status.Store("last_heartbeat", time.Now().UTC().Format(time.RFC3339))
				status.Store("last_error", "")
				logger.Info("skipped expired delivery plan", "request_id", plan.Request.RequestID)
				return nil
			}

			effectiveRoute, err := resolveRoute(messageCtx, store, plan.Request)
			if err != nil {
				status.Store("last_error", err.Error())
				logger.Error("resolve routing policy channels failed", "error", err, "request_id", plan.Request.RequestID)
				_ = store.UpdateNotificationRequestStatus(messageCtx, plan.Request.RequestID, notification.RequestStatusFailed)
				return nil
			}

			if err := store.UpdateNotificationRequestStatus(messageCtx, plan.Request.RequestID, notification.RequestStatusProcessing); err != nil {
				status.Store("last_error", err.Error())
				logger.Error("mark notification request processing failed", "error", err, "request_id", plan.Request.RequestID)
				return nil
			}

			allowedChannels, suppressedChannels, err := resolvePreferredChannels(messageCtx, store, plan.Request, effectiveRoute.Channels)
			if err != nil {
				status.Store("last_error", err.Error())
				logger.Error("resolve preference policy channels failed", "error", err, "request_id", plan.Request.RequestID)
				_ = store.UpdateNotificationRequestStatus(messageCtx, plan.Request.RequestID, notification.RequestStatusFailed)
				return nil
			}

			attemptStats := deliveryStats{}
			for _, channel := range suppressedChannels {
				attempt := notification.DeliveryAttempt{
					AttemptID:     id.New(12),
					RequestID:     plan.Request.RequestID,
					Channel:       channel,
					ConnectorName: connectorName(channel),
					Status:        notification.DeliveryAttemptSuppressed,
					Destination:   destination(plan.Request, channel),
					ErrorMessage:  "suppressed by preference policy",
				}
				if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
					logger.Error("create suppressed delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					attemptStats.failed++
					continue
				}
				recordDeliveryAttemptMetric(registry, attempt)
				attemptStats.suppressed++
			}

			for _, channel := range allowedChannels {
				deliveryPolicy, err := resolveDeliveryPolicy(messageCtx, store, channel)
				if err != nil {
					status.Store("last_error", err.Error())
					logger.Error("load delivery policy failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   1,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed delivery policy attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.failed++
					continue
				}

				tmpl, err := store.GetTemplateByKeyAndChannel(messageCtx, plan.Request.TemplateKey, channel)
				if errors.Is(err, postgres.ErrNotFound) {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   deliveryPolicy.MaxAttempts,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptUnsupported,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  "no enabled template for template key and channel",
					}
					if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("create unsupported template delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.unsupported++
					continue
				}
				if err != nil {
					status.Store("last_error", err.Error())
					logger.Error("load template failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   deliveryPolicy.MaxAttempts,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed template delivery attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.failed++
					continue
				}

				subject, err := render.Subject(tmpl, plan.Request)
				if err != nil {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   deliveryPolicy.MaxAttempts,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed subject render attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.failed++
					continue
				}

				body, err := render.Body(tmpl, plan.Request)
				if err != nil {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   deliveryPolicy.MaxAttempts,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed body render attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.failed++
					continue
				}

				if !supportedChannel(channel) {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   1,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptUnsupported,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  "connector not implemented in worker happy path",
					}
					if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("create unsupported delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.unsupported++
					continue
				}

				bindings, err := store.ListProviderBindingsByChannel(messageCtx, channel, effectiveRoute.BindingSet)
				if errors.Is(err, postgres.ErrNotFound) {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   deliveryPolicy.MaxAttempts,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptUnsupported,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  "no enabled provider binding for channel",
					}
					if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("create unsupported delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.unsupported++
					continue
				}
				if err != nil {
					status.Store("last_error", err.Error())
					logger.Error("load provider bindings failed", "error", err, "channel", channel)
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						AttemptNumber: 1,
						MaxAttempts:   deliveryPolicy.MaxAttempts,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed delivery attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					recordDeliveryAttemptMetric(registry, attempt)
					attemptStats.failed++
					continue
				}

				sent := false
				totalMaxAttempts := deliveryPolicy.MaxAttempts * len(bindings)
				nextAttemptNumber := 1
				sendReq := notification.ConnectorSendRequest{
					RequestID:   plan.Request.RequestID,
					Channel:     channel,
					Destination: destination(plan.Request, channel),
					Subject:     subject,
					Body:        body,
					Metadata:    plan.Request.Metadata,
				}

				for bindingIndex, binding := range bindings {
					connector := connectorClient{
						baseURL: binding.EndpointURL,
						client:  &http.Client{Timeout: 10 * time.Second},
					}

					for bindingAttempt := 1; bindingAttempt <= deliveryPolicy.MaxAttempts; bindingAttempt++ {
						attempt := notification.DeliveryAttempt{
							AttemptID:     id.New(12),
							RequestID:     plan.Request.RequestID,
							AttemptNumber: nextAttemptNumber,
							MaxAttempts:   totalMaxAttempts,
							Channel:       channel,
							ConnectorName: binding.ConnectorName,
							Status:        notification.DeliveryAttemptPending,
							Destination:   destination(plan.Request, channel),
						}

						if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
							status.Store("last_error", err.Error())
							logger.Error("create delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel, "attempt_number", nextAttemptNumber, "connector", binding.ConnectorName)
							attemptStats.failed++
							break
						}

						sendResp, err := connector.send(messageCtx, sendReq)
						if err == nil {
							attempt.Status = notification.DeliveryAttemptAccepted
							attempt.ProviderMessageID = sendResp.ProviderMessageID
							if err := store.UpdateDeliveryAttempt(messageCtx, attempt); err != nil {
								logger.Error("update accepted delivery attempt failed", "error", err, "attempt_id", attempt.AttemptID)
								attemptStats.failed++
								break
							}
							recordDeliveryAttemptMetric(registry, attempt)
							attemptStats.accepted++
							sent = true
							break
						}

						status.Store("last_error", err.Error())
						logger.Error("connector send failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel, "attempt_number", nextAttemptNumber, "binding_attempt", bindingAttempt, "max_binding_attempts", deliveryPolicy.MaxAttempts, "connector", binding.ConnectorName, "binding_priority", binding.Priority)
						attempt.Status = notification.DeliveryAttemptFailed
						attempt.ErrorMessage = err.Error()
						if err := store.UpdateDeliveryAttempt(messageCtx, attempt); err != nil {
							logger.Error("update failed delivery attempt failed", "error", err, "attempt_id", attempt.AttemptID)
						}
						recordDeliveryAttemptMetric(registry, attempt)

						nextAttemptNumber++
						if bindingAttempt == deliveryPolicy.MaxAttempts {
							break
						}
						recordRetryMetric(registry, channel, binding.ConnectorName)

						if err := sleepWithContext(messageCtx, time.Duration(deliveryPolicy.BackoffSeconds)*time.Second); err != nil {
							logger.Error("retry backoff interrupted", "error", err, "request_id", plan.Request.RequestID, "channel", channel, "connector", binding.ConnectorName)
							attemptStats.failed++
							break
						}
					}

					if sent {
						break
					}
					if nextAttemptNumber > totalMaxAttempts {
						break
					}

					if bindingIndex+1 >= len(bindings) {
						break
					}
					recordFailoverMetric(registry, channel, binding.ConnectorName, bindings[bindingIndex+1].ConnectorName)
					logger.Info("provider binding exhausted, failing over", "request_id", plan.Request.RequestID, "channel", channel, "connector", binding.ConnectorName, "binding_priority", binding.Priority, "next_binding_index", bindingIndex+1)
				}

				if !sent {
					attemptStats.failed++
					continue
				}
			}

			finalStatus := attemptStats.finalStatus()
			if err := store.UpdateNotificationRequestStatus(messageCtx, plan.Request.RequestID, finalStatus); err != nil {
				status.Store("last_error", err.Error())
				logger.Error("update final notification request status failed", "error", err, "request_id", plan.Request.RequestID)
				return nil
			}
			registry.IncCounter("notification_request_final_status_total", "Final notification request statuses produced by the worker.", map[string]string{
				"status": string(finalStatus),
			})
			registry.ObserveHistogram("notification_end_to_end_seconds", "End-to-end notification latency in seconds from API acceptance to observed terminal or dispatched state.", map[string]string{
				"stage":        "worker",
				"final_status": string(finalStatus),
			}, metrics.DefaultLatencyBuckets(), time.Since(plan.Request.RequestedAt).Seconds())
			if err := notifier.NotifyRequestUpdated(messageCtx, plan.Request.RequestID, map[string]interface{}{"source": "worker"}); err != nil {
				logger.Error("notify lifecycle webhook failed", "error", err, "request_id", plan.Request.RequestID)
			}

			status.Store("state", "idle")
			status.Store("last_heartbeat", time.Now().UTC().Format(time.RFC3339))
			status.Store("last_error", "")
			logger.Info("processed delivery plan", "request_id", plan.Request.RequestID, "status", finalStatus)
			return nil
		})
		if err != nil {
			status.Store("state", "stopped")
			status.Store("last_error", err.Error())
			logger.Error("worker consume loop stopped", "error", err)
		}
	}()

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, _ *metrics.Registry) {
		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"service":         info.Name,
				"phase":           "lifecycle-webhooks",
				"state":           loadString(&status, "state"),
				"last_request_id": loadString(&status, "last_request_id"),
				"last_heartbeat":  loadString(&status, "last_heartbeat"),
				"last_error":      loadString(&status, "last_error"),
				"topic":           topic,
			})
		})
	})
	if err != nil {
		panic(err)
	}
}

func connectorName(channel notification.Channel) string {
	switch channel {
	case notification.ChannelWebhook:
		return "webhook"
	case notification.ChannelEmail:
		return "email"
	case notification.ChannelSMS:
		return "sms"
	default:
		return "unsupported"
	}
}

func supportedChannel(channel notification.Channel) bool {
	switch channel {
	case notification.ChannelWebhook, notification.ChannelEmail, notification.ChannelSMS:
		return true
	default:
		return false
	}
}

func destination(record notification.NotificationRecord, channel notification.Channel) string {
	switch channel {
	case notification.ChannelWebhook:
		return record.Recipient.Webhook
	case notification.ChannelEmail:
		return record.Recipient.Email
	case notification.ChannelSMS:
		return record.Recipient.Phone
	default:
		return record.Recipient.UserID
	}
}

func loadString(status *sync.Map, key string) string {
	value, _ := status.Load(key)
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func recordDeliveryAttemptMetric(registry *metrics.Registry, attempt notification.DeliveryAttempt) {
	if registry == nil {
		return
	}
	registry.IncCounter("delivery_attempts_total", "Delivery attempt outcomes recorded by the worker.", map[string]string{
		"channel":   string(attempt.Channel),
		"connector": attempt.ConnectorName,
		"status":    string(attempt.Status),
	})
}

func recordRetryMetric(registry *metrics.Registry, channel notification.Channel, connector string) {
	if registry == nil {
		return
	}
	registry.IncCounter("delivery_retries_total", "Delivery retry attempts scheduled by the worker.", map[string]string{
		"channel":   string(channel),
		"connector": connector,
	})
}

func recordFailoverMetric(registry *metrics.Registry, channel notification.Channel, fromConnector string, toConnector string) {
	if registry == nil {
		return
	}
	registry.IncCounter("provider_failovers_total", "Provider failover transitions executed by the worker.", map[string]string{
		"channel":        string(channel),
		"from_connector": fromConnector,
		"to_connector":   toConnector,
	})
}

func resolveRoute(ctx context.Context, store *postgres.Store, record notification.NotificationRecord) (resolvedRoute, error) {
	if len(record.Channels) > 0 {
		return resolvedRoute{
			Channels:   record.Channels,
			BindingSet: record.BindingSet,
		}, nil
	}

	policy, err := store.GetRoutingPolicyByEventName(ctx, record.EventName)
	if err == postgres.ErrNotFound {
		return resolvedRoute{}, fmt.Errorf("no channels requested and no routing policy for event %s", record.EventName)
	}
	if err != nil {
		return resolvedRoute{}, err
	}
	return resolvedRoute{
		Channels:   policy.Channels,
		BindingSet: policy.BindingSet,
	}, nil
}

func resolvePreferredChannels(ctx context.Context, store *postgres.Store, record notification.NotificationRecord, channels []notification.Channel) ([]notification.Channel, []notification.Channel, error) {
	if record.Recipient.UserID == "" {
		return channels, nil, nil
	}

	preferences, err := store.ListPreferencePolicies(ctx, record.Recipient.UserID)
	if err != nil {
		return nil, nil, err
	}
	if len(preferences) == 0 {
		return channels, nil, nil
	}

	disabled := make(map[notification.Channel]bool, len(preferences))
	for _, preference := range preferences {
		if !preference.IsEnabled {
			disabled[preference.Channel] = true
		}
	}

	allowed := make([]notification.Channel, 0, len(channels))
	suppressed := make([]notification.Channel, 0, len(channels))
	for _, channel := range channels {
		if disabled[channel] {
			suppressed = append(suppressed, channel)
			continue
		}
		allowed = append(allowed, channel)
	}

	return allowed, suppressed, nil
}

func resolveDeliveryPolicy(ctx context.Context, store *postgres.Store, channel notification.Channel) (notification.DeliveryPolicy, error) {
	policy, err := store.GetDeliveryPolicyByChannel(ctx, channel)
	if errors.Is(err, postgres.ErrNotFound) {
		return notification.DeliveryPolicy{
			Channel:        channel,
			MaxAttempts:    1,
			BackoffSeconds: 0,
			Enabled:        true,
		}, nil
	}
	if err != nil {
		return notification.DeliveryPolicy{}, err
	}
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	if policy.BackoffSeconds < 0 {
		policy.BackoffSeconds = 0
	}
	return policy, nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isExpired(record notification.NotificationRecord, now time.Time) bool {
	return record.ExpiresAt != nil && !record.ExpiresAt.After(now)
}

type deliveryStats struct {
	accepted    int
	failed      int
	suppressed  int
	unsupported int
}

type resolvedRoute struct {
	Channels   []notification.Channel
	BindingSet string
}

func (s deliveryStats) finalStatus() notification.RequestStatus {
	switch {
	case s.accepted > 0:
		return notification.RequestStatusDispatched
	case s.failed > 0:
		return notification.RequestStatusFailed
	case s.unsupported > 0:
		return notification.RequestStatusUnsupported
	case s.suppressed > 0:
		return notification.RequestStatusSuppressed
	default:
		return notification.RequestStatusUnsupported
	}
}
