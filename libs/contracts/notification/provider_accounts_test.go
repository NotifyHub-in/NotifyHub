package notification

import "testing"

func TestProviderDefinitionsIncludeManagedProviders(t *testing.T) {
	for _, providerKey := range []string{"twilio-sms", "gupshup-sms", "karix-sms", "sendgrid-email", "smtp-email", "gupshup-whatsapp", "karix-whatsapp", "fcm-push"} {
		if _, ok := ProviderDefinitionByKey(providerKey); !ok {
			t.Fatalf("expected provider definition %q to exist", providerKey)
		}
	}
}

func TestValidateProviderAccount(t *testing.T) {
	account := ProviderAccount{
		ProviderAccountID: "provacct_sms_twilio_prod",
		TenantID:          "tenant-a",
		ProviderKey:       "twilio-sms",
		DisplayName:       "Twilio Production",
		Channel:           ChannelSMS,
		Enabled:           true,
		Config: map[string]string{
			"from_number": "+14155550123",
		},
		SecretRefs: map[string]SecretReference{
			"account_sid": {
				Ref:          "secret://tenant/tenant-a/twilio/account-sid",
				MaterialType: MaterialTypeSecretString,
			},
			"auth_token": {
				Ref:          "secret://tenant/tenant-a/twilio/auth-token",
				MaterialType: MaterialTypeSecretString,
			},
		},
	}

	if err := ValidateProviderAccount(account); err != nil {
		t.Fatalf("ValidateProviderAccount returned error: %v", err)
	}

	account.SecretRefs["auth_token"] = SecretReference{
		Ref:          "secret://tenant/tenant-a/twilio/auth-token",
		MaterialType: MaterialTypeSecretJSON,
	}
	if err := ValidateProviderAccount(account); err == nil {
		t.Fatal("expected material type mismatch to fail validation")
	}
}

func TestValidateProviderAccountSupportsSMSVariants(t *testing.T) {
	cases := []struct {
		name    string
		account ProviderAccount
	}{
		{
			name: "gupshup production",
			account: ProviderAccount{
				ProviderAccountID: "provacct_sms_gupshup_prod",
				TenantID:          "tenant-a",
				ProviderKey:       "gupshup-sms",
				DisplayName:       "Gupshup Production",
				Channel:           ChannelSMS,
				Enabled:           true,
				Config: map[string]string{
					"base_url":  "https://api.gupshup.io",
					"sender_id": "EXAMPLE",
				},
				SecretRefs: map[string]SecretReference{
					"api_key": {
						Ref:          "secret://tenant/tenant-a/gupshup/api-key",
						MaterialType: MaterialTypeSecretString,
					},
				},
			},
		},
		{
			name: "gupshup legacy",
			account: ProviderAccount{
				ProviderAccountID: "provacct_sms_gupshup_legacy",
				TenantID:          "tenant-a",
				ProviderKey:       "gupshup-sms",
				DisplayName:       "Gupshup Legacy",
				Channel:           ChannelSMS,
				Enabled:           true,
				Config: map[string]string{
					"base_url":  "https://enterprise.smsgupshup.com/GatewayAPI/rest",
					"sender_id": "EXAMPLE",
				},
				SecretRefs: map[string]SecretReference{
					"username": {
						Ref:          "secret://tenant/tenant-a/gupshup/username",
						MaterialType: MaterialTypeSecretString,
					},
					"password": {
						Ref:          "secret://tenant/tenant-a/gupshup/password",
						MaterialType: MaterialTypeSecretString,
					},
				},
			},
		},
		{
			name: "karix production",
			account: ProviderAccount{
				ProviderAccountID: "provacct_sms_karix_prod",
				TenantID:          "tenant-a",
				ProviderKey:       "karix-sms",
				DisplayName:       "Karix Production",
				Channel:           ChannelSMS,
				Enabled:           true,
				Config: map[string]string{
					"base_url":  "https://api.karix.io",
					"sender_id": "EXAMPLE",
				},
				SecretRefs: map[string]SecretReference{
					"api_key": {
						Ref:          "secret://tenant/tenant-a/karix/api-key",
						MaterialType: MaterialTypeSecretString,
					},
				},
			},
		},
		{
			name: "karix legacy",
			account: ProviderAccount{
				ProviderAccountID: "provacct_sms_karix_legacy",
				TenantID:          "tenant-a",
				ProviderKey:       "karix-sms",
				DisplayName:       "Karix Legacy",
				Channel:           ChannelSMS,
				Enabled:           true,
				Config: map[string]string{
					"base_url": "https://japi.instaalerts.zone/httpapi/JsonReceiver",
					"send":     "EXAMPLE",
					"ver":      "1.0",
				},
				SecretRefs: map[string]SecretReference{
					"key": {
						Ref:          "secret://tenant/tenant-a/karix/key",
						MaterialType: MaterialTypeSecretString,
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateProviderAccount(tc.account); err != nil {
				t.Fatalf("ValidateProviderAccount returned error: %v", err)
			}
		})
	}
}
