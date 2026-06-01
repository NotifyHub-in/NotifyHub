package notification

import (
	"time"
)

type GupshupWhatsAppCallbackRequest struct {
	ExternalID string `json:"externalId"`
	EventType  string `json:"eventType"`
	EventTS    int64  `json:"eventTs"`
	DestAddr   string `json:"destAddr"`
	SrcAddr    string `json:"srcAddr"`
	Cause      string `json:"cause"`
	ErrorCode  string `json:"errorCode"`
	Channel    string `json:"channel"`
}

type SmsCallbackRequest struct {
	Response []SmsCallbackResponse `json:"response"`
}

type SmsCallbackResponse struct {
	SrcAddr    string `json:"srcAddr"`
	Channel    string `json:"channel"`
	ExternalID string `json:"externalId"`
	Cause      string `json:"cause"`
	ErrorCode  string `json:"errorCode"`
	DestAddr   string `json:"destAddr"`
	EventType  string `json:"eventType"`
	EventTS    int64  `json:"eventTs"`
	NoOfFrags  int64  `json:"noOfFrags"`
}

type KarixWhatsAppCallbackRequest struct {
	Channel                string                         `json:"channel"`
	AppDetails             map[string]any                 `json:"appDetails"`
	Recipient              KarixWhatsAppCallbackRecipient `json:"recipient"`
	Sender                 map[string]any                 `json:"sender"`
	Events                 KarixWhatsAppCallbackEvents    `json:"events"`
	NotificationAttributes KarixWhatsAppCallbackStatus    `json:"notificationAttributes"`
}

type KarixWhatsAppCallbackRecipient struct {
	To            string         `json:"to"`
	RecipientType string         `json:"recipient_type"`
	Reference     map[string]any `json:"reference"`
}

type KarixWhatsAppCallbackEvents struct {
	EventType string `json:"eventType"`
	Timestamp int64  `json:"timestamp"`
	Date      string `json:"date"`
	MID       string `json:"mid"`
}

type KarixWhatsAppCallbackStatus struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
	Code   string `json:"code"`
}

type ProviderCallbackEnvelope struct {
	ProviderMessageID string            `json:"provider_message_id"`
	Status            string            `json:"status"`
	ErrorCode         string            `json:"error_code,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	ReceivedAt        time.Time         `json:"received_at,omitempty"`
}
