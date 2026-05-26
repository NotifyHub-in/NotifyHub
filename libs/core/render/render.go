package render

import (
	"bytes"
	"fmt"
	"regexp"
	"text/template"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

const (
	MaxSubjectTemplateLength = 512
	MaxBodyTemplateLength    = 20000
	MaxRenderedOutputLength  = 65536
)

var legacyVariablePattern = regexp.MustCompile(`\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}`)

func Subject(tmpl notification.Template, record notification.NotificationRecord) (string, error) {
	return executeTemplate("subject", tmpl.SubjectTemplate, record.Variables)
}

func Body(tmpl notification.Template, record notification.NotificationRecord) (string, error) {
	return executeTemplate("body", tmpl.BodyTemplate, record.Variables)
}

func ValidateSubjectTemplate(source string) error {
	if len(source) > MaxSubjectTemplateLength {
		return fmt.Errorf("subject template exceeds %d characters", MaxSubjectTemplateLength)
	}
	return validateTemplate("subject", source)
}

func ValidateBodyTemplate(source string) error {
	if len(source) > MaxBodyTemplateLength {
		return fmt.Errorf("body template exceeds %d characters", MaxBodyTemplateLength)
	}
	return validateTemplate("body", source)
}

func validateTemplate(name, source string) error {
	if source == "" {
		return nil
	}

	_, err := template.New(name).Option("missingkey=error").Parse(normalizeTemplate(source))
	if err != nil {
		return fmt.Errorf("parse %s template: %w", name, err)
	}
	return nil
}

func executeTemplate(name, source string, variables map[string]string) (string, error) {
	if variables == nil {
		variables = map[string]string{}
	}

	parsed, err := template.New(name).Option("missingkey=error").Parse(normalizeTemplate(source))
	if err != nil {
		return "", fmt.Errorf("parse %s template: %w", name, err)
	}

	var output bytes.Buffer
	if err := parsed.Execute(&output, variables); err != nil {
		return "", fmt.Errorf("render %s template: %w", name, err)
	}
	if output.Len() > MaxRenderedOutputLength {
		return "", fmt.Errorf("%s template rendered output exceeds %d bytes", name, MaxRenderedOutputLength)
	}

	return output.String(), nil
}

func normalizeTemplate(source string) string {
	return legacyVariablePattern.ReplaceAllString(source, "{{ .$1 }}")
}
