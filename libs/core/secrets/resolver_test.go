package secrets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
)

func TestResolve(t *testing.T) {
	t.Run("plain string", func(t *testing.T) {
		got, err := Resolve(notification.SecretReference{
			Ref:          "literal-value",
			MaterialType: notification.MaterialTypePlainString,
		})
		if err != nil {
			t.Fatalf("Resolve returned error: %v", err)
		}
		if got != "literal-value" {
			t.Fatalf("Resolve returned %q, want %q", got, "literal-value")
		}
	})

	t.Run("env secret", func(t *testing.T) {
		t.Setenv("TEST_SHARED_SECRET", "super-secret")
		got, err := Resolve(notification.SecretReference{
			Ref:          "TEST_SHARED_SECRET",
			MaterialType: notification.MaterialTypeSecretString,
		})
		if err != nil {
			t.Fatalf("Resolve returned error: %v", err)
		}
		if got != "super-secret" {
			t.Fatalf("Resolve returned %q, want %q", got, "super-secret")
		}
	})

	t.Run("file secret", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "secret.txt")
		if err := os.WriteFile(path, []byte("file-secret"), 0o600); err != nil {
			t.Fatalf("write temp secret file: %v", err)
		}
		got, err := Resolve(notification.SecretReference{
			Ref:          path,
			MaterialType: notification.MaterialTypeSecretFile,
		})
		if err != nil {
			t.Fatalf("Resolve returned error: %v", err)
		}
		if got != "file-secret" {
			t.Fatalf("Resolve returned %q, want %q", got, "file-secret")
		}
	})

	t.Run("missing secret", func(t *testing.T) {
		if _, err := Resolve(notification.SecretReference{
			Ref:          "MISSING_SECRET",
			MaterialType: notification.MaterialTypeSecretString,
		}); err == nil {
			t.Fatal("expected missing secret to fail")
		}
	})

	t.Run("resolve config merges plain config and secret refs", func(t *testing.T) {
		t.Setenv("TEST_RESOLVE_CONFIG_SECRET", "merged-secret")
		got, err := ResolveConfig(
			map[string]string{"project_id": "demo-project"},
			map[string]notification.SecretReference{
				"service_account_json": {
					Ref:          "TEST_RESOLVE_CONFIG_SECRET",
					MaterialType: notification.MaterialTypeSecretString,
				},
			},
		)
		if err != nil {
			t.Fatalf("ResolveConfig returned error: %v", err)
		}
		if got["project_id"] != "demo-project" {
			t.Fatalf("ResolveConfig project_id = %q, want %q", got["project_id"], "demo-project")
		}
		if got["service_account_json"] != "merged-secret" {
			t.Fatalf("ResolveConfig service_account_json = %q, want %q", got["service_account_json"], "merged-secret")
		}
	})
}
