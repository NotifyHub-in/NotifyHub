package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Arunshaik2001/notification-control-plane/libs/contracts/notification"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/id"
)

func (s *Store) UpsertProviderAccount(ctx context.Context, account notification.ProviderAccount) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("upsert_provider_account", startedAt, err)
	}()

	if err := notification.ValidateProviderAccount(account); err != nil {
		return err
	}

	configJSON, err := marshalStringMap(account.Config)
	if err != nil {
		return fmt.Errorf("marshal provider account config: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin provider account transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	const upsertAccountQuery = `
		INSERT INTO provider_accounts (
			provider_account_id, tenant_id, provider_key, display_name, channel, enabled, config
		) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
		ON CONFLICT (provider_account_id)
		DO UPDATE SET
			tenant_id = EXCLUDED.tenant_id,
			provider_key = EXCLUDED.provider_key,
			display_name = EXCLUDED.display_name,
			channel = EXCLUDED.channel,
			enabled = EXCLUDED.enabled,
			config = EXCLUDED.config,
			updated_at = NOW()
	`

	if _, err = tx.ExecContext(ctx, upsertAccountQuery,
		account.ProviderAccountID,
		account.TenantID,
		account.ProviderKey,
		account.DisplayName,
		account.Channel,
		account.Enabled,
		string(configJSON),
	); err != nil {
		return fmt.Errorf("upsert provider account: %w", err)
	}

	if _, err = tx.ExecContext(ctx, `DELETE FROM provider_secret_refs WHERE provider_account_id = $1`, account.ProviderAccountID); err != nil {
		return fmt.Errorf("clear provider account secret refs: %w", err)
	}

	for secretName, ref := range account.SecretRefs {
		if secretName == "" {
			return fmt.Errorf("provider account secret refs contain an empty key")
		}
		const insertSecretRefQuery = `
			INSERT INTO provider_secret_refs (
				secret_ref_id, provider_account_id, secret_name, secret_ref, material_type, version, source
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`
		if _, err = tx.ExecContext(ctx, insertSecretRefQuery,
			id.New(12),
			account.ProviderAccountID,
			secretName,
			ref.Ref,
			ref.MaterialType,
			ref.Version,
			ref.Source,
		); err != nil {
			return fmt.Errorf("insert provider account secret ref %q: %w", secretName, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit provider account transaction: %w", err)
	}
	return nil
}

func (s *Store) GetProviderAccount(ctx context.Context, providerAccountID string) (account notification.ProviderAccount, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_provider_account", startedAt, err)
	}()

	const query = `
		SELECT provider_account_id, tenant_id, provider_key, display_name, channel, enabled, config, created_at, updated_at
		FROM provider_accounts
		WHERE provider_account_id = $1
		LIMIT 1
	`

	var configJSON []byte
	err = s.db.QueryRowContext(ctx, query, providerAccountID).Scan(
		&account.ProviderAccountID,
		&account.TenantID,
		&account.ProviderKey,
		&account.DisplayName,
		&account.Channel,
		&account.Enabled,
		&configJSON,
		&account.CreatedAt,
		&account.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.ProviderAccount{}, ErrNotFound
	}
	if err != nil {
		return notification.ProviderAccount{}, fmt.Errorf("query provider account: %w", err)
	}

	if len(configJSON) > 0 {
		if err := json.Unmarshal(configJSON, &account.Config); err != nil {
			return notification.ProviderAccount{}, fmt.Errorf("unmarshal provider account config: %w", err)
		}
	}
	account.SecretRefs, err = s.loadProviderAccountSecretRefs(ctx, account.ProviderAccountID)
	if err != nil {
		return notification.ProviderAccount{}, err
	}
	return account, nil
}

func (s *Store) ListProviderAccounts(ctx context.Context, tenantID string) (accounts []notification.ProviderAccount, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_provider_accounts", startedAt, err)
	}()

	query := `
		SELECT provider_account_id, tenant_id, provider_key, display_name, channel, enabled, config, created_at, updated_at
		FROM provider_accounts
	`
	args := []any{}
	if tenantID != "" {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY tenant_id ASC, provider_key ASC, provider_account_id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query provider accounts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			account    notification.ProviderAccount
			configJSON []byte
		)
		if err := rows.Scan(
			&account.ProviderAccountID,
			&account.TenantID,
			&account.ProviderKey,
			&account.DisplayName,
			&account.Channel,
			&account.Enabled,
			&configJSON,
			&account.CreatedAt,
			&account.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan provider account: %w", err)
		}
		if len(configJSON) > 0 {
			if err := json.Unmarshal(configJSON, &account.Config); err != nil {
				return nil, fmt.Errorf("unmarshal provider account config: %w", err)
			}
		}
		account.SecretRefs, err = s.loadProviderAccountSecretRefs(ctx, account.ProviderAccountID)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider accounts: %w", err)
	}
	return accounts, nil
}

func (s *Store) DisableProviderAccount(ctx context.Context, providerAccountID string) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("disable_provider_account", startedAt, err)
	}()

	const query = `
		UPDATE provider_accounts
		SET enabled = FALSE,
		    updated_at = NOW()
		WHERE provider_account_id = $1
	`

	result, err := s.db.ExecContext(ctx, query, providerAccountID)
	if err != nil {
		return fmt.Errorf("disable provider account: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) loadProviderAccountSecretRefs(ctx context.Context, providerAccountID string) (map[string]notification.SecretReference, error) {
	const query = `
		SELECT secret_name, secret_ref, material_type, version, source
		FROM provider_secret_refs
		WHERE provider_account_id = $1
		ORDER BY secret_name ASC
	`

	rows, err := s.db.QueryContext(ctx, query, providerAccountID)
	if err != nil {
		return nil, fmt.Errorf("query provider account secret refs: %w", err)
	}
	defer rows.Close()

	secretRefs := make(map[string]notification.SecretReference)
	for rows.Next() {
		var (
			secretName string
			ref        notification.SecretReference
		)
		if err := rows.Scan(&secretName, &ref.Ref, &ref.MaterialType, &ref.Version, &ref.Source); err != nil {
			return nil, fmt.Errorf("scan provider account secret ref: %w", err)
		}
		secretRefs[secretName] = ref
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider account secret refs: %w", err)
	}
	return secretRefs, nil
}
