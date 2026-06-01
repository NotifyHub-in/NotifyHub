package notification

import (
	"fmt"
	"strings"
	"time"
)

type MaterialType string

const (
	MaterialTypePlainString  MaterialType = "plain_string"
	MaterialTypeSecretString MaterialType = "secret_string"
	MaterialTypeSecretJSON   MaterialType = "secret_json"
	MaterialTypeSecretFile   MaterialType = "secret_file"
)

type ProviderDefinition struct {
	ProviderKey          string                  `json:"provider_key"`
	Channel              Channel                 `json:"channel"`
	ConnectorName        string                  `json:"connector_name"`
	AdapterKey           string                  `json:"adapter_key"`
	Description          string                  `json:"description,omitempty"`
	RequiredConfigSchema map[string]MaterialType `json:"required_config_schema"`
	ConfigVariants       []ProviderConfigVariant `json:"config_variants,omitempty"`
	CallbackMode         string                  `json:"callback_mode,omitempty"`
}

type ProviderConfigVariant struct {
	RequiredConfigSchema map[string]MaterialType `json:"required_config_schema"`
}

type SecretReference struct {
	Ref          string       `json:"ref"`
	MaterialType MaterialType `json:"material_type"`
	Version      string       `json:"version,omitempty"`
	Source       string       `json:"source,omitempty"`
}

type ProviderAccount struct {
	ProviderAccountID string                     `json:"provider_account_id"`
	TenantID          string                     `json:"tenant_id"`
	ProviderKey       string                     `json:"provider_key"`
	DisplayName       string                     `json:"display_name"`
	Channel           Channel                    `json:"channel"`
	Enabled           bool                       `json:"enabled"`
	Config            map[string]string          `json:"config,omitempty"`
	SecretRefs        map[string]SecretReference `json:"secret_refs,omitempty"`
	CreatedAt         time.Time                  `json:"created_at"`
	UpdatedAt         time.Time                  `json:"updated_at"`
}

type ProviderAccountUpsertRequest struct {
	TenantID    string                     `json:"tenant_id"`
	ProviderKey string                     `json:"provider_key"`
	DisplayName string                     `json:"display_name"`
	Channel     Channel                    `json:"channel"`
	Enabled     bool                       `json:"enabled"`
	Config      map[string]string          `json:"config,omitempty"`
	SecretRefs  map[string]SecretReference `json:"secret_refs,omitempty"`
}

type ProviderAccountPatchRequest struct {
	DisplayName *string                     `json:"display_name,omitempty"`
	Enabled     *bool                       `json:"enabled,omitempty"`
	Config      *map[string]string          `json:"config,omitempty"`
	SecretRefs  *map[string]SecretReference `json:"secret_refs,omitempty"`
}

type CallbackVerificationMode string

const (
	CallbackVerificationModeNone         CallbackVerificationMode = "none"
	CallbackVerificationModeSharedSecret CallbackVerificationMode = "shared_secret"
	CallbackVerificationModeHMACSHA256   CallbackVerificationMode = "hmac_sha256"
)

type CallbackRoute struct {
	RouteID               string                   `json:"route_id"`
	ProviderKey           string                   `json:"provider_key"`
	ProviderAccountID     string                   `json:"provider_account_id"`
	CallbackPath          string                   `json:"callback_path"`
	VerificationMode      CallbackVerificationMode `json:"verification_mode"`
	VerificationSecretRef SecretReference          `json:"verification_secret_ref"`
	Enabled               bool                     `json:"enabled"`
	CreatedAt             time.Time                `json:"created_at"`
	UpdatedAt             time.Time                `json:"updated_at"`
}

type CallbackRouteUpsertRequest struct {
	ProviderKey           string                   `json:"provider_key"`
	ProviderAccountID     string                   `json:"provider_account_id"`
	CallbackPath          string                   `json:"callback_path"`
	VerificationMode      CallbackVerificationMode `json:"verification_mode"`
	VerificationSecretRef SecretReference          `json:"verification_secret_ref"`
	Enabled               bool                     `json:"enabled"`
}

var providerDefinitions = []ProviderDefinition{
	{
		ProviderKey:   "twilio-sms",
		Channel:       ChannelSMS,
		ConnectorName: "connector-sms",
		AdapterKey:    "twilio",
		Description:   "Twilio SMS provider",
		RequiredConfigSchema: map[string]MaterialType{
			"from_number": MaterialTypePlainString,
			"account_sid": MaterialTypeSecretString,
			"auth_token":  MaterialTypeSecretString,
		},
		CallbackMode: "signature",
	},
	{
		ProviderKey:   "gupshup-sms",
		Channel:       ChannelSMS,
		ConnectorName: "connector-sms",
		AdapterKey:    "gupshup",
		Description:   "Gupshup SMS provider",
		RequiredConfigSchema: map[string]MaterialType{
			"api_key":   MaterialTypeSecretString,
			"sender_id": MaterialTypePlainString,
			"base_url":  MaterialTypePlainString,
		},
		ConfigVariants: []ProviderConfigVariant{
			{
				RequiredConfigSchema: map[string]MaterialType{
					"api_key":   MaterialTypeSecretString,
					"sender_id": MaterialTypePlainString,
					"base_url":  MaterialTypePlainString,
				},
			},
			{
				RequiredConfigSchema: map[string]MaterialType{
					"username":  MaterialTypeSecretString,
					"password":  MaterialTypeSecretString,
					"sender_id": MaterialTypePlainString,
					"base_url":  MaterialTypePlainString,
				},
			},
		},
		CallbackMode: "signature",
	},
	{
		ProviderKey:   "karix-sms",
		Channel:       ChannelSMS,
		ConnectorName: "connector-sms",
		AdapterKey:    "karix",
		Description:   "Karix SMS provider",
		RequiredConfigSchema: map[string]MaterialType{
			"api_key":   MaterialTypeSecretString,
			"sender_id": MaterialTypePlainString,
			"base_url":  MaterialTypePlainString,
		},
		ConfigVariants: []ProviderConfigVariant{
			{
				RequiredConfigSchema: map[string]MaterialType{
					"api_key":   MaterialTypeSecretString,
					"sender_id": MaterialTypePlainString,
					"base_url":  MaterialTypePlainString,
				},
			},
			{
				RequiredConfigSchema: map[string]MaterialType{
					"key":      MaterialTypeSecretString,
					"send":     MaterialTypePlainString,
					"ver":      MaterialTypePlainString,
					"base_url": MaterialTypePlainString,
				},
			},
		},
		CallbackMode: "signature",
	},
	{
		ProviderKey:   "sendgrid-email",
		Channel:       ChannelEmail,
		ConnectorName: "connector-email",
		AdapterKey:    "sendgrid",
		Description:   "SendGrid email provider",
		RequiredConfigSchema: map[string]MaterialType{
			"from_email": MaterialTypePlainString,
			"api_key":    MaterialTypeSecretString,
		},
		CallbackMode: "none",
	},
	{
		ProviderKey:   "smtp-email",
		Channel:       ChannelEmail,
		ConnectorName: "connector-email",
		AdapterKey:    "smtp",
		Description:   "SMTP email provider",
		RequiredConfigSchema: map[string]MaterialType{
			"host":       MaterialTypePlainString,
			"port":       MaterialTypePlainString,
			"user":       MaterialTypeSecretString,
			"password":   MaterialTypeSecretString,
			"from_email": MaterialTypePlainString,
		},
		CallbackMode: "none",
	},
	{
		ProviderKey:   "gupshup-whatsapp",
		Channel:       ChannelWhatsApp,
		ConnectorName: "connector-whatsapp",
		AdapterKey:    "gupshup",
		Description:   "Gupshup WhatsApp provider",
		RequiredConfigSchema: map[string]MaterialType{
			"username": MaterialTypePlainString,
			"password": MaterialTypeSecretString,
			"version":  MaterialTypePlainString,
			"base_url": MaterialTypePlainString,
		},
		CallbackMode: "provider_callback",
	},
	{
		ProviderKey:   "karix-whatsapp",
		Channel:       ChannelWhatsApp,
		ConnectorName: "connector-whatsapp",
		AdapterKey:    "karix",
		Description:   "Karix WhatsApp provider",
		RequiredConfigSchema: map[string]MaterialType{
			"key":      MaterialTypeSecretString,
			"sender":   MaterialTypePlainString,
			"version":  MaterialTypePlainString,
			"base_url": MaterialTypePlainString,
		},
		CallbackMode: "provider_callback",
	},
	{
		ProviderKey:   "fcm-push",
		Channel:       ChannelPush,
		ConnectorName: "connector-push",
		AdapterKey:    "fcm",
		Description:   "Firebase Cloud Messaging push provider",
		RequiredConfigSchema: map[string]MaterialType{
			"project_id":           MaterialTypePlainString,
			"service_account_json": MaterialTypeSecretJSON,
		},
		CallbackMode: "none",
	},
}

var providerDefinitionsByKey = func() map[string]ProviderDefinition {
	defs := make(map[string]ProviderDefinition, len(providerDefinitions))
	for _, def := range providerDefinitions {
		defs[def.ProviderKey] = def
	}
	return defs
}()

func ProviderDefinitions() []ProviderDefinition {
	out := make([]ProviderDefinition, len(providerDefinitions))
	copy(out, providerDefinitions)
	return out
}

func ProviderDefinitionByKey(providerKey string) (ProviderDefinition, bool) {
	def, ok := providerDefinitionsByKey[providerKey]
	return def, ok
}

func ValidateProviderAccount(account ProviderAccount) error {
	def, ok := ProviderDefinitionByKey(account.ProviderKey)
	if !ok {
		return fmt.Errorf("unknown provider_key %q", account.ProviderKey)
	}
	if account.Channel != def.Channel {
		return fmt.Errorf("provider_key %q belongs to channel %q, not %q", account.ProviderKey, def.Channel, account.Channel)
	}
	if account.ProviderAccountID == "" {
		return fmt.Errorf("provider_account_id is required")
	}
	if account.TenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if account.DisplayName == "" {
		return fmt.Errorf("display_name is required")
	}

	variants := def.configVariants()
	if len(variants) == 0 {
		variants = []map[string]MaterialType{def.RequiredConfigSchema}
	}

	var variantErrors []string
	for _, schema := range variants {
		if err := validateProviderConfigVariant(account, schema); err == nil {
			return nil
		} else {
			variantErrors = append(variantErrors, err.Error())
		}
	}

	return fmt.Errorf("provider_key %q configuration does not match any supported variant: %s", account.ProviderKey, strings.Join(variantErrors, "; "))
}

func (def ProviderDefinition) configVariants() []map[string]MaterialType {
	if len(def.ConfigVariants) == 0 {
		if len(def.RequiredConfigSchema) == 0 {
			return nil
		}
		return []map[string]MaterialType{def.RequiredConfigSchema}
	}

	variants := make([]map[string]MaterialType, 0, len(def.ConfigVariants)+1)
	if len(def.RequiredConfigSchema) > 0 {
		variants = append(variants, def.RequiredConfigSchema)
	}
	for _, variant := range def.ConfigVariants {
		variants = append(variants, variant.RequiredConfigSchema)
	}
	return variants
}

func validateProviderConfigVariant(account ProviderAccount, schema map[string]MaterialType) error {
	for fieldName, materialType := range schema {
		switch materialType {
		case MaterialTypePlainString:
			if value, ok := account.Config[fieldName]; !ok || value == "" {
				return fmt.Errorf("config field %q is required", fieldName)
			}
		default:
			ref, ok := account.SecretRefs[fieldName]
			if !ok {
				return fmt.Errorf("secret ref %q is required", fieldName)
			}
			if ref.Ref == "" {
				return fmt.Errorf("secret ref %q is empty", fieldName)
			}
			if ref.MaterialType != materialType {
				return fmt.Errorf("secret ref %q material type must be %q", fieldName, materialType)
			}
		}
	}

	return nil
}

func ValidateCallbackRoute(route CallbackRoute) error {
	if route.ProviderKey == "" {
		return fmt.Errorf("provider_key is required")
	}
	if route.ProviderAccountID == "" {
		return fmt.Errorf("provider_account_id is required")
	}
	if route.CallbackPath == "" {
		return fmt.Errorf("callback_path is required")
	}
	if route.VerificationMode == "" {
		route.VerificationMode = CallbackVerificationModeNone
	}
	switch route.VerificationMode {
	case CallbackVerificationModeNone:
		return nil
	case CallbackVerificationModeSharedSecret, CallbackVerificationModeHMACSHA256:
		if route.VerificationSecretRef.Ref == "" {
			return fmt.Errorf("verification_secret_ref.ref is required for verification mode %q", route.VerificationMode)
		}
		if route.VerificationSecretRef.MaterialType == "" {
			return fmt.Errorf("verification_secret_ref.material_type is required for verification mode %q", route.VerificationMode)
		}
		return nil
	default:
		return fmt.Errorf("unsupported verification_mode %q", route.VerificationMode)
	}
}
