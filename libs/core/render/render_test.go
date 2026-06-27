package render

import (
	"testing"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
)

func TestBodyRendersLegacyUppercaseTemplateVariablesCaseInsensitively(t *testing.T) {
	tmpl := notification.Template{
		BodyTemplate: "{{OTP}} is your verification code for {{VerifyEmailUrl}}.",
	}
	record := notification.NotificationRecord{
		Variables: map[string]string{
			"otp":              "123456",
			"verifyEmailUrl":   "https://example.com/verify",
			"ignored_variable": "kept",
		},
	}

	got, err := Body(tmpl, record)
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}

	want := "123456 is your verification code for https://example.com/verify."
	if got != want {
		t.Fatalf("Body() = %q, want %q", got, want)
	}
}

func TestBodyRendersDotPrefixedTemplateVariablesCaseInsensitively(t *testing.T) {
	tmpl := notification.Template{
		BodyTemplate: "{{.OTP}} is your verification code.",
	}
	record := notification.NotificationRecord{
		Variables: map[string]string{
			"otp": "123456",
		},
	}

	got, err := Body(tmpl, record)
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}

	if got != "123456 is your verification code." {
		t.Fatalf("Body() = %q, want %q", got, "123456 is your verification code.")
	}
}

func TestSubjectRendersLowercaseTemplateVariablesCaseInsensitively(t *testing.T) {
	tmpl := notification.Template{
		SubjectTemplate: "Hello {{recipient_name}}",
	}
	record := notification.NotificationRecord{
		Variables: map[string]string{
			"RECIPIENT_NAME": "Arun",
		},
	}

	got, err := Subject(tmpl, record)
	if err != nil {
		t.Fatalf("Subject() error = %v", err)
	}

	if got != "Hello Arun" {
		t.Fatalf("Subject() = %q, want %q", got, "Hello Arun")
	}
}
