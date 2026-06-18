package notification

import (
	"strings"
	"time"
)

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
	RequestStatusDelivered   RequestStatus = "delivered"
	RequestStatusExpired     RequestStatus = "expired"
	RequestStatusFailed      RequestStatus = "failed"
	RequestStatusSuppressed  RequestStatus = "suppressed"
	RequestStatusUnsupported RequestStatus = "unsupported"
)

type DeliveryAttemptStatus string

const (
	DeliveryAttemptPending     DeliveryAttemptStatus = "pending"
	DeliveryAttemptAccepted    DeliveryAttemptStatus = "accepted"
	DeliveryAttemptDelivered   DeliveryAttemptStatus = "delivered"
	DeliveryAttemptExpired     DeliveryAttemptStatus = "expired"
	DeliveryAttemptFailed      DeliveryAttemptStatus = "failed"
	DeliveryAttemptSuppressed  DeliveryAttemptStatus = "suppressed"
	DeliveryAttemptUnsupported DeliveryAttemptStatus = "unsupported"
)

type Recipient struct {
	UserID    string `json:"user_id,omitempty"`
	Email     string `json:"email,omitempty"`
	Phone     string `json:"phone,omitempty"`
	PushToken string `json:"push_token,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Webhook   string `json:"webhook,omitempty"`
}

type NotificationRequest struct {
	RequestID        string            `json:"request_id,omitempty"`
	IdempotencyKey   string            `json:"idempotency_key"`
	EventName        string            `json:"event_name"`
	TemplateKey      string            `json:"template_key"`
	LanguageCode     string            `json:"language_code,omitempty"`
	Channels         []Channel         `json:"channels"`
	BindingSet       string            `json:"binding_set,omitempty"`
	Recipient        Recipient         `json:"recipient"`
	Variables        map[string]string `json:"variables,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	Priority         string            `json:"priority,omitempty"`
	SourceClientID   string            `json:"source_client_id,omitempty"`
	SourceTenantID   string            `json:"source_tenant_id,omitempty"`
	SourceClientName string            `json:"source_client_name,omitempty"`
	RequestedAt      time.Time         `json:"requested_at,omitempty"`
	ExpiresAt        *time.Time        `json:"expires_at,omitempty"`
}

type NotificationAccepted struct {
	RequestID        string        `json:"request_id"`
	Status           RequestStatus `json:"status"`
	AcceptedAt       time.Time     `json:"accepted_at"`
	IdempotentReplay bool          `json:"idempotent_replay,omitempty"`
}

type NotificationRecord struct {
	RequestID        string            `json:"request_id"`
	IdempotencyKey   string            `json:"idempotency_key"`
	EventName        string            `json:"event_name"`
	TemplateKey      string            `json:"template_key"`
	LanguageCode     string            `json:"language_code,omitempty"`
	Channels         []Channel         `json:"channels"`
	BindingSet       string            `json:"binding_set,omitempty"`
	Recipient        Recipient         `json:"recipient"`
	Variables        map[string]string `json:"variables,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	Priority         string            `json:"priority,omitempty"`
	SourceClientID   string            `json:"source_client_id,omitempty"`
	SourceTenantID   string            `json:"source_tenant_id,omitempty"`
	SourceClientName string            `json:"source_client_name,omitempty"`
	Status           RequestStatus     `json:"status"`
	RequestedAt      time.Time         `json:"requested_at"`
	ExpiresAt        *time.Time        `json:"expires_at,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

type NotificationClient struct {
	ClientID        string    `json:"client_id"`
	TenantID        string    `json:"tenant_id"`
	ClientName      string    `json:"client_name"`
	Enabled         bool      `json:"enabled"`
	AllowedChannels []Channel `json:"allowed_channels,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type NotificationClientCreateRequest struct {
	TenantID        string    `json:"tenant_id"`
	ClientName      string    `json:"client_name"`
	AllowedChannels []Channel `json:"allowed_channels,omitempty"`
	Enabled         bool      `json:"enabled"`
}

type NotificationClientCreateResponse struct {
	Client NotificationClient `json:"client"`
	APIKey string             `json:"api_key"`
}

type NotificationClientPatchRequest struct {
	ClientName      *string    `json:"client_name,omitempty"`
	Enabled         *bool      `json:"enabled,omitempty"`
	AllowedChannels *[]Channel `json:"allowed_channels,omitempty"`
}

type DeliveryAttempt struct {
	AttemptID         string                `json:"attempt_id"`
	RequestID         string                `json:"request_id"`
	AttemptNumber     int                   `json:"attempt_number"`
	MaxAttempts       int                   `json:"max_attempts"`
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
	Retry   *RetryDirective    `json:"retry,omitempty"`
}

type RetryDirective struct {
	RetryID    string  `json:"retry_id"`
	Channel    Channel `json:"channel"`
	BindingSet string  `json:"binding_set,omitempty"`
}

type ProviderBinding struct {
	BindingID         string    `json:"binding_id"`
	Channel           Channel   `json:"channel"`
	BindingSet        string    `json:"binding_set,omitempty"`
	ConnectorName     string    `json:"connector_name"`
	EndpointURL       string    `json:"endpoint_url,omitempty"`
	ProviderAccountID string    `json:"provider_account_id,omitempty"`
	Enabled           bool      `json:"enabled"`
	Priority          int       `json:"priority"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type ProviderBindingUpsertRequest struct {
	Channel           Channel `json:"channel"`
	BindingSet        string  `json:"binding_set,omitempty"`
	ConnectorName     string  `json:"connector_name,omitempty"`
	EndpointURL       string  `json:"endpoint_url,omitempty"`
	ProviderAccountID string  `json:"provider_account_id,omitempty"`
	Enabled           bool    `json:"enabled"`
	Priority          int     `json:"priority"`
}

type RoutingPolicy struct {
	PolicyID   string    `json:"policy_id"`
	EventName  string    `json:"event_name"`
	Channels   []Channel `json:"channels"`
	BindingSet string    `json:"binding_set,omitempty"`
	Enabled    bool      `json:"enabled"`
	Priority   int       `json:"priority"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type RoutingPolicyUpsertRequest struct {
	EventName  string    `json:"event_name"`
	Channels   []Channel `json:"channels"`
	BindingSet string    `json:"binding_set,omitempty"`
	Enabled    bool      `json:"enabled"`
	Priority   int       `json:"priority"`
}

type PreferencePolicy struct {
	PolicyID  string    `json:"policy_id"`
	UserID    string    `json:"user_id"`
	Channel   Channel   `json:"channel"`
	IsEnabled bool      `json:"is_enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type PreferencePolicyUpsertRequest struct {
	UserID    string  `json:"user_id"`
	Channel   Channel `json:"channel"`
	IsEnabled bool    `json:"is_enabled"`
}

type Template struct {
	TemplateID      string            `json:"template_id"`
	TemplateKey     string            `json:"template_key"`
	Channel         Channel           `json:"channel"`
	LanguageCode    string            `json:"language_code,omitempty"`
	SubjectTemplate string            `json:"subject_template,omitempty"`
	BodyTemplate    string            `json:"body_template"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Enabled         bool              `json:"enabled"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

type TemplateUpsertRequest struct {
	TemplateKey     string            `json:"template_key"`
	Channel         Channel           `json:"channel"`
	LanguageCode    string            `json:"language_code,omitempty"`
	SubjectTemplate string            `json:"subject_template,omitempty"`
	BodyTemplate    string            `json:"body_template"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Enabled         bool              `json:"enabled"`
}

const DefaultLanguageCode = "en"

func NormalizeLanguageCode(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	code = strings.ReplaceAll(code, "_", "-")
	if code == "" {
		return DefaultLanguageCode
	}
	return code
}

type DeliveryPolicy struct {
	PolicyID       string    `json:"policy_id"`
	Channel        Channel   `json:"channel"`
	MaxAttempts    int       `json:"max_attempts"`
	BackoffSeconds int       `json:"backoff_seconds"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type DeliveryPolicyUpsertRequest struct {
	Channel        Channel `json:"channel"`
	MaxAttempts    int     `json:"max_attempts"`
	BackoffSeconds int     `json:"backoff_seconds"`
	Enabled        bool    `json:"enabled"`
}

type ScheduledRetry struct {
	RetryID                  string     `json:"retry_id"`
	RequestID                string     `json:"request_id"`
	Channel                  Channel    `json:"channel"`
	BindingSet               string     `json:"binding_set,omitempty"`
	AvailableAt              time.Time  `json:"available_at"`
	ClaimedAt                *time.Time `json:"claimed_at,omitempty"`
	LastError                string     `json:"last_error,omitempty"`
	TriggeredByAttemptNumber int        `json:"triggered_by_attempt_number"`
	CreatedAt                time.Time  `json:"created_at"`
	UpdatedAt                time.Time  `json:"updated_at"`
}

type DeadLetterNotification struct {
	DeadLetterID    string         `json:"dead_letter_id"`
	RequestID       string         `json:"request_id"`
	Channel         Channel        `json:"channel"`
	BindingSet      string         `json:"binding_set,omitempty"`
	ConnectorName   string         `json:"connector_name"`
	Reason          string         `json:"reason"`
	AttemptCount    int            `json:"attempt_count"`
	LastError       string         `json:"last_error,omitempty"`
	PayloadSnapshot map[string]any `json:"payload_snapshot,omitempty"`
	ReplayRequestID string         `json:"replay_request_id,omitempty"`
	ReplayedAt      *time.Time     `json:"replayed_at,omitempty"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

type ConnectorSendRequest struct {
	RequestID          string                     `json:"request_id"`
	Channel            Channel                    `json:"channel"`
	LanguageCode       string                     `json:"language_code,omitempty"`
	ProviderKey        string                     `json:"provider_key,omitempty"`
	ProviderAccountID  string                     `json:"provider_account_id,omitempty"`
	Destination        string                     `json:"destination"`
	Subject            string                     `json:"subject,omitempty"`
	Body               string                     `json:"body"`
	TemplateVariables  map[string]string          `json:"template_variables,omitempty"`
	Metadata           map[string]string          `json:"metadata,omitempty"`
	ProviderConfig     map[string]string          `json:"provider_config,omitempty"`
	ProviderSecretRefs map[string]SecretReference `json:"provider_secret_refs,omitempty"`
}

type FailureClass string

const (
	FailureClassTransient      FailureClass = "transient"
	FailureClassPermanent      FailureClass = "permanent"
	FailureClassRateLimited    FailureClass = "rate_limited"
	FailureClassMisconfigured  FailureClass = "misconfigured"
	FailureClassUnauthorized   FailureClass = "unauthorized"
	FailureClassInvalidRequest FailureClass = "invalid_request"
)

type ProviderCircuitState string

const (
	ProviderCircuitStateClosed ProviderCircuitState = "closed"
	ProviderCircuitStateOpen   ProviderCircuitState = "open"
)

type ConnectorErrorResponse struct {
	Error          string       `json:"error"`
	Code           string       `json:"code,omitempty"`
	Classification FailureClass `json:"classification,omitempty"`
	Retryable      bool         `json:"retryable,omitempty"`
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

type ProviderBindingHealth struct {
	BindingID           string               `json:"binding_id"`
	Channel             Channel              `json:"channel"`
	BindingSet          string               `json:"binding_set,omitempty"`
	ConnectorName       string               `json:"connector_name"`
	CircuitState        ProviderCircuitState `json:"circuit_state"`
	ConsecutiveFailures int                  `json:"consecutive_failures"`
	OpenedAt            *time.Time           `json:"opened_at,omitempty"`
	CooldownUntil       *time.Time           `json:"cooldown_until,omitempty"`
	LastFailureClass    FailureClass         `json:"last_failure_class,omitempty"`
	LastError           string               `json:"last_error,omitempty"`
	LastFailureAt       *time.Time           `json:"last_failure_at,omitempty"`
	CreatedAt           time.Time            `json:"created_at"`
	UpdatedAt           time.Time            `json:"updated_at"`
}

type ProviderCallback struct {
	ProviderMessageID string            `json:"provider_message_id"`
	Status            string            `json:"status"`
	ErrorCode         string            `json:"error_code,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

type WebhookSubscription struct {
	SubscriptionID string    `json:"subscription_id"`
	TargetURL      string    `json:"target_url"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type WebhookSubscriptionUpsertRequest struct {
	TargetURL string `json:"target_url"`
	Enabled   bool   `json:"enabled"`
}

type LifecycleWebhookEvent struct {
	EventType string                 `json:"event_type"`
	RequestID string                 `json:"request_id"`
	Status    RequestStatus          `json:"status"`
	Request   NotificationRecord     `json:"request"`
	Attempts  []DeliveryAttempt      `json:"attempts"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	SentAt    time.Time              `json:"sent_at"`
}

type WebhookDeliveryAttemptStatus string

const (
	WebhookDeliveryAttemptPending   WebhookDeliveryAttemptStatus = "pending"
	WebhookDeliveryAttemptSucceeded WebhookDeliveryAttemptStatus = "succeeded"
	WebhookDeliveryAttemptFailed    WebhookDeliveryAttemptStatus = "failed"
)

type WebhookDeliveryAttempt struct {
	DeliveryID     string                       `json:"delivery_id"`
	RequestID      string                       `json:"request_id"`
	SubscriptionID string                       `json:"subscription_id"`
	EventType      string                       `json:"event_type"`
	TargetURL      string                       `json:"target_url"`
	AttemptNumber  int                          `json:"attempt_number"`
	MaxAttempts    int                          `json:"max_attempts"`
	Status         WebhookDeliveryAttemptStatus `json:"status"`
	HTTPStatusCode int                          `json:"http_status_code,omitempty"`
	ErrorMessage   string                       `json:"error_message,omitempty"`
	ResponseBody   string                       `json:"response_body,omitempty"`
	CreatedAt      time.Time                    `json:"created_at"`
	UpdatedAt      time.Time                    `json:"updated_at"`
}

type ChannelEventDirection string

const (
	ChannelEventDirectionInbound  ChannelEventDirection = "inbound"
	ChannelEventDirectionOutbound ChannelEventDirection = "outbound"
)

type ChannelEventStatus string

const (
	ChannelEventStatusReceived  ChannelEventStatus = "received"
	ChannelEventStatusProcessed ChannelEventStatus = "processed"
	ChannelEventStatusFailed    ChannelEventStatus = "failed"
)

type ChannelEvent struct {
	EventID           string                `json:"event_id"`
	ProviderKey       string                `json:"provider_key"`
	ProviderAccountID string                `json:"provider_account_id,omitempty"`
	Channel           Channel               `json:"channel"`
	Direction         ChannelEventDirection `json:"direction"`
	EventType         string                `json:"event_type"`
	Status            ChannelEventStatus    `json:"status"`
	ExternalMessageID string                `json:"external_message_id,omitempty"`
	ReplyToMessageID  string                `json:"reply_to_message_id,omitempty"`
	ConversationID    string                `json:"conversation_id,omitempty"`
	FromAddress       string                `json:"from_address,omitempty"`
	ToAddress         string                `json:"to_address,omitempty"`
	Body              string                `json:"body,omitempty"`
	MediaType         string                `json:"media_type,omitempty"`
	MediaURL          string                `json:"media_url,omitempty"`
	MediaName         string                `json:"media_name,omitempty"`
	Payload           map[string]any        `json:"payload,omitempty"`
	Metadata          map[string]string     `json:"metadata,omitempty"`
	ReceivedAt        time.Time             `json:"received_at"`
	CreatedAt         time.Time             `json:"created_at"`
	UpdatedAt         time.Time             `json:"updated_at"`
}

type ChannelWebhookEvent struct {
	EventType string         `json:"event_type"`
	Event     ChannelEvent   `json:"event"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	SentAt    time.Time      `json:"sent_at"`
}
