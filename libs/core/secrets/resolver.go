package secrets

import (
	"fmt"
	"os"
	"strings"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

type Resolver struct{}

func NewResolver() *Resolver {
	return &Resolver{}
}

func (r *Resolver) Resolve(ref notification.SecretReference) (string, error) {
	return Resolve(ref)
}

func Resolve(ref notification.SecretReference) (string, error) {
	if ref.Ref == "" {
		return "", fmt.Errorf("secret reference is empty")
	}

	switch ref.MaterialType {
	case notification.MaterialTypePlainString:
		return ref.Ref, nil
	case notification.MaterialTypeSecretString, notification.MaterialTypeSecretJSON:
		return resolveSecretValue(ref)
	case notification.MaterialTypeSecretFile:
		return readSecretFile(ref.Ref)
	default:
		return "", fmt.Errorf("unsupported secret material type %q", ref.MaterialType)
	}
}

func resolveSecretValue(ref notification.SecretReference) (string, error) {
	if strings.HasPrefix(ref.Ref, "file://") {
		return readSecretFile(strings.TrimPrefix(ref.Ref, "file://"))
	}
	if ref.Source == "file" {
		return readSecretFile(ref.Ref)
	}

	if value, ok := os.LookupEnv(ref.Ref); ok && value != "" {
		return value, nil
	}
	return "", fmt.Errorf("secret reference %q not found", ref.Ref)
}

func readSecretFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
