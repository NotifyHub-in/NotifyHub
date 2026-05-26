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
	RequestID  string    `json:"request_id"`
	Status     string    `json:"status"`
	AcceptedAt time.Time `json:"accepted_at"`
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
