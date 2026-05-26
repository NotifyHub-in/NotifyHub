package notification

import "time"

type Channel string

const (
	ChannelEmail    Channel = "email"
	ChannelSMS      Channel = "sms"
	ChannelPush     Channel = "push"
	ChannelWhatsApp Channel = "whatsapp"
	ChannelWebhook  Channel = "webhook"
)

type RequestStatus string

const (
	RequestStatusAccepted    RequestStatus = "accepted"
	RequestStatusProcessing  RequestStatus = "processing"
	RequestStatusDispatched  RequestStatus = "dispatched"
	RequestStatusFailed      RequestStatus = "failed"
	RequestStatusUnsupported RequestStatus = "unsupported"
)

type DeliveryAttemptStatus string

const (
	DeliveryAttemptPending     DeliveryAttemptStatus = "pending"
	DeliveryAttemptAccepted    DeliveryAttemptStatus = "accepted"
	DeliveryAttemptFailed      DeliveryAttemptStatus = "failed"
	DeliveryAttemptUnsupported DeliveryAttemptStatus = "unsupported"
)

type Recipient struct {
	UserID  string `json:"user_id,omitempty"`
	Email   string `json:"email,omitempty"`
	Phone   string `json:"phone,omitempty"`
	Topic   string `json:"topic,omitempty"`
	Webhook string `json:"webhook,omitempty"`
}

type NotificationRequest struct {
	RequestID      string            `json:"request_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key"`
	EventName      string            `json:"event_name"`
	TemplateKey    string            `json:"template_key"`
	Channels       []Channel         `json:"channels"`
	Recipient      Recipient         `json:"recipient"`
	Variables      map[string]string `json:"variables,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Priority       string            `json:"priority,omitempty"`
	RequestedAt    time.Time         `json:"requested_at,omitempty"`
}

type NotificationAccepted struct {
	RequestID  string        `json:"request_id"`
	Status     RequestStatus `json:"status"`
	AcceptedAt time.Time     `json:"accepted_at"`
}

type NotificationRecord struct {
	RequestID      string            `json:"request_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	EventName      string            `json:"event_name"`
	TemplateKey    string            `json:"template_key"`
	Channels       []Channel         `json:"channels"`
	Recipient      Recipient         `json:"recipient"`
	Variables      map[string]string `json:"variables,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Priority       string            `json:"priority,omitempty"`
	Status         RequestStatus     `json:"status"`
	RequestedAt    time.Time         `json:"requested_at"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

type DeliveryAttempt struct {
	AttemptID         string                `json:"attempt_id"`
	RequestID         string                `json:"request_id"`
	Channel           Channel               `json:"channel"`
	ConnectorName     string                `json:"connector_name"`
	Status            DeliveryAttemptStatus `json:"status"`
	ProviderMessageID string                `json:"provider_message_id,omitempty"`
	Destination       string                `json:"destination,omitempty"`
	ErrorMessage      string                `json:"error_message,omitempty"`
	CreatedAt         time.Time             `json:"created_at"`
	UpdatedAt         time.Time             `json:"updated_at"`
}

type DeliveryPlan struct {
	Request NotificationRecord `json:"request"`
}

type ConnectorSendRequest struct {
	RequestID   string            `json:"request_id"`
	Channel     Channel           `json:"channel"`
	Destination string            `json:"destination"`
	Subject     string            `json:"subject,omitempty"`
	Body        string            `json:"body"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ConnectorSendResponse struct {
	RequestID         string    `json:"request_id"`
	ProviderMessageID string    `json:"provider_message_id"`
	Status            string    `json:"status"`
	AcceptedAt        time.Time `json:"accepted_at"`
}

type ConnectorCapabilities struct {
	Name     string    `json:"name"`
	Channels []Channel `json:"channels"`
}
