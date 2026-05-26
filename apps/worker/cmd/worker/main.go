package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	kafkamq "github.com/your-org/notification-control-plane/libs/messaging/kafka"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
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

	store, err := postgres.Open(ctx, config.MustGetEnv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()

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

			effectiveChannels, err := resolveChannels(messageCtx, store, plan.Request)
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

			allowedChannels, suppressedChannels, err := resolvePreferredChannels(messageCtx, store, plan.Request, effectiveChannels)
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
				attemptStats.suppressed++
			}

			for _, channel := range allowedChannels {
				tmpl, err := store.GetTemplateByKeyAndChannel(messageCtx, plan.Request.TemplateKey, channel)
				if err == postgres.ErrNotFound {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptUnsupported,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  "no enabled template for template key and channel",
					}
					if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("create unsupported template delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					}
					attemptStats.unsupported++
					continue
				}
				if err != nil {
					status.Store("last_error", err.Error())
					logger.Error("load template failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed template delivery attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					attemptStats.failed++
					continue
				}

				subject, err := render.Subject(tmpl, plan.Request)
				if err != nil {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed subject render attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					attemptStats.failed++
					continue
				}

				body, err := render.Body(tmpl, plan.Request)
				if err != nil {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed body render attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					attemptStats.failed++
					continue
				}

				binding, err := store.GetProviderBindingByChannel(messageCtx, channel)
				if err == postgres.ErrNotFound {
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptUnsupported,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  "no enabled provider binding for channel",
					}
					if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("create unsupported delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					}
					attemptStats.unsupported++
					continue
				}
				if err != nil {
					status.Store("last_error", err.Error())
					logger.Error("load provider binding failed", "error", err, "channel", channel)
					attempt := notification.DeliveryAttempt{
						AttemptID:     id.New(12),
						RequestID:     plan.Request.RequestID,
						Channel:       channel,
						ConnectorName: connectorName(channel),
						Status:        notification.DeliveryAttemptFailed,
						Destination:   destination(plan.Request, channel),
						ErrorMessage:  err.Error(),
					}
					if createErr := store.CreateDeliveryAttempt(messageCtx, attempt); createErr != nil {
						logger.Error("create failed delivery attempt failed", "error", createErr, "request_id", plan.Request.RequestID, "channel", channel)
					}
					attemptStats.failed++
					continue
				}

				attempt := notification.DeliveryAttempt{
					AttemptID:     id.New(12),
					RequestID:     plan.Request.RequestID,
					Channel:       channel,
					ConnectorName: binding.ConnectorName,
					Status:        notification.DeliveryAttemptPending,
					Destination:   destination(plan.Request, channel),
				}

				if err := store.CreateDeliveryAttempt(messageCtx, attempt); err != nil {
					status.Store("last_error", err.Error())
					logger.Error("create delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					attemptStats.failed++
					continue
				}

				connector := connectorClient{
					baseURL: binding.EndpointURL,
					client:  &http.Client{Timeout: 10 * time.Second},
				}

				if !supportedChannel(channel) {
					attempt.Status = notification.DeliveryAttemptUnsupported
					attempt.ErrorMessage = "connector not implemented in worker happy path"
					if err := store.UpdateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("update unsupported delivery attempt failed", "error", err, "attempt_id", attempt.AttemptID)
					}
					attemptStats.unsupported++
					continue
				}

				sendReq := notification.ConnectorSendRequest{
					RequestID:   plan.Request.RequestID,
					Channel:     channel,
					Destination: destination(plan.Request, channel),
					Subject:     subject,
					Body:        body,
					Metadata:    plan.Request.Metadata,
				}

				sendResp, err := connector.send(messageCtx, sendReq)
				if err != nil {
					status.Store("last_error", err.Error())
					logger.Error("connector send failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
					attempt.Status = notification.DeliveryAttemptFailed
					attempt.ErrorMessage = err.Error()
					if err := store.UpdateDeliveryAttempt(messageCtx, attempt); err != nil {
						logger.Error("update failed delivery attempt failed", "error", err, "attempt_id", attempt.AttemptID)
					}
					attemptStats.failed++
					continue
				}

				attempt.Status = notification.DeliveryAttemptAccepted
				attempt.ProviderMessageID = sendResp.ProviderMessageID
				if err := store.UpdateDeliveryAttempt(messageCtx, attempt); err != nil {
					logger.Error("update accepted delivery attempt failed", "error", err, "attempt_id", attempt.AttemptID)
					attemptStats.failed++
					continue
				}
				attemptStats.accepted++
			}

			finalStatus := attemptStats.finalStatus()
			if err := store.UpdateNotificationRequestStatus(messageCtx, plan.Request.RequestID, finalStatus); err != nil {
				status.Store("last_error", err.Error())
				logger.Error("update final notification request status failed", "error", err, "request_id", plan.Request.RequestID)
				return nil
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

	err = app.RunHTTPService(cfg, logger, func(mux *http.ServeMux, info serviceinfo.Info) {
		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"service":         info.Name,
				"phase":           "templates",
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

func resolveChannels(ctx context.Context, store *postgres.Store, record notification.NotificationRecord) ([]notification.Channel, error) {
	if len(record.Channels) > 0 {
		return record.Channels, nil
	}

	policy, err := store.GetRoutingPolicyByEventName(ctx, record.EventName)
	if err == postgres.ErrNotFound {
		return nil, fmt.Errorf("no channels requested and no routing policy for event %s", record.EventName)
	}
	if err != nil {
		return nil, err
	}
	return policy.Channels, nil
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

type deliveryStats struct {
	accepted    int
	failed      int
	suppressed  int
	unsupported int
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
