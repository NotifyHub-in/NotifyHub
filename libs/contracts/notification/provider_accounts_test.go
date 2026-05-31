package notification

import "testing"

func TestProviderDefinitionsIncludeManagedProviders(t *testing.T) {
	for _, providerKey := range []string{"twilio-sms", "gupshup-sms", "karix-sms", "sendgrid-email", "fcm-push"} {
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
