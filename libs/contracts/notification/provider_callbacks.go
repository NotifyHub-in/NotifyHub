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

type GupshupWhatsAppInboundMessage struct {
	App       string                        `json:"app"`
	Timestamp int64                         `json:"timestamp"`
	Version   int                           `json:"version"`
	Type      string                        `json:"type"`
	Payload   GupshupWhatsAppInboundPayload `json:"payload"`
}

type GupshupWhatsAppInboundPayload struct {
	ID      string                         `json:"id"`
	Source  string                         `json:"source"`
	Type    string                         `json:"type"`
	Payload GupshupWhatsAppInboundContent  `json:"payload"`
	Sender  GupshupWhatsAppInboundSender   `json:"sender"`
	Context *GupshupWhatsAppInboundContext `json:"context,omitempty"`
}

type GupshupWhatsAppInboundContent struct {
	Text    string `json:"text,omitempty"`
	Caption string `json:"caption,omitempty"`
	URL     string `json:"url,omitempty"`
	Name    string `json:"name,omitempty"`
	Type    string `json:"type,omitempty"`
}

type GupshupWhatsAppInboundSender struct {
	Phone       string `json:"phone"`
	Name        string `json:"name"`
	CountryCode string `json:"country_code"`
	DialCode    string `json:"dial_code"`
}

type GupshupWhatsAppInboundContext struct {
	ID   string `json:"id"`
	GsID string `json:"gsId"`
}

type MetaWhatsAppWebhookPayload struct {
	Object string                     `json:"object"`
	Entry  []MetaWhatsAppWebhookEntry `json:"entry"`
}

type MetaWhatsAppWebhookEntry struct {
	ID      string                      `json:"id"`
	Changes []MetaWhatsAppWebhookChange `json:"changes"`
}

type MetaWhatsAppWebhookChange struct {
	Field string                   `json:"field"`
	Value MetaWhatsAppWebhookValue `json:"value"`
}

type MetaWhatsAppWebhookValue struct {
	Messages []MetaWhatsAppMessage `json:"messages"`
	Contacts []MetaWhatsAppContact `json:"contacts"`
	Metadata MetaWhatsAppMetadata  `json:"metadata"`
}

type MetaWhatsAppMessage struct {
	ID        string                       `json:"id"`
	From      string                       `json:"from"`
	Timestamp string                       `json:"timestamp"`
	Type      string                       `json:"type"`
	Text      *MetaWhatsAppMessageText     `json:"text,omitempty"`
	Image     *MetaWhatsAppMessageMedia    `json:"image,omitempty"`
	Video     *MetaWhatsAppMessageMedia    `json:"video,omitempty"`
	Document  *MetaWhatsAppMessageMedia    `json:"document,omitempty"`
	Audio     *MetaWhatsAppMessageMedia    `json:"audio,omitempty"`
	Sticker   *MetaWhatsAppMessageMedia    `json:"sticker,omitempty"`
	Location  *MetaWhatsAppMessageLocation `json:"location,omitempty"`
	Contacts  []any                        `json:"contacts,omitempty"`
	Context   *MetaWhatsAppMessageContext  `json:"context,omitempty"`
}

type MetaWhatsAppMessageText struct {
	Body string `json:"body"`
}

type MetaWhatsAppMessageMedia struct {
	ID       string `json:"id,omitempty"`
	Caption  string `json:"caption,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
	Sha256   string `json:"sha256,omitempty"`
}

type MetaWhatsAppMessageLocation struct {
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
	Name      string  `json:"name,omitempty"`
	Address   string  `json:"address,omitempty"`
}

type MetaWhatsAppMessageContext struct {
	ID              string `json:"id,omitempty"`
	From            string `json:"from,omitempty"`
	ReferredProduct struct {
		CatalogID string `json:"catalog_id,omitempty"`
		ProductID string `json:"product_retailer_id,omitempty"`
	} `json:"referred_product,omitempty"`
}

type MetaWhatsAppContact struct {
	Profile struct {
		Name string `json:"name"`
	} `json:"profile"`
	WAID string `json:"wa_id"`
}

type MetaWhatsAppMetadata struct {
	DisplayPhoneNumber string `json:"display_phone_number"`
	PhoneNumberID      string `json:"phone_number_id"`
}
