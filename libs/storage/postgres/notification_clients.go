package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
	"github.com/jackc/pgx/v5/pgconn"
)

func (s *Store) CreateNotificationClient(ctx context.Context, client notification.NotificationClient, apiKeyHash string) (created notification.NotificationClient, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("create_notification_client", startedAt, err)
	}()

	allowedChannelsJSON, err := json.Marshal(client.AllowedChannels)
	if err != nil {
		return notification.NotificationClient{}, fmt.Errorf("marshal allowed channels: %w", err)
	}

	const query = `
		INSERT INTO notification_clients (
			client_id, tenant_id, client_name, api_key_hash, allowed_channels, enabled
		) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
	`

	_, err = s.db.ExecContext(ctx, query,
		client.ClientID,
		client.TenantID,
		client.ClientName,
		apiKeyHash,
		string(allowedChannelsJSON),
		client.Enabled,
	)
	if err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
			return notification.NotificationClient{}, ErrConflict
		}
		return notification.NotificationClient{}, fmt.Errorf("insert notification client: %w", err)
	}

	return s.GetNotificationClient(ctx, client.ClientID)
}

func (s *Store) GetNotificationClient(ctx context.Context, clientID string) (client notification.NotificationClient, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_notification_client", startedAt, err)
	}()

	const query = `
		SELECT client_id, tenant_id, client_name, allowed_channels, enabled, created_at, updated_at
		FROM notification_clients
		WHERE client_id = $1
		LIMIT 1
	`

	var allowedChannelsJSON []byte
	err = s.db.QueryRowContext(ctx, query, clientID).Scan(
		&client.ClientID,
		&client.TenantID,
		&client.ClientName,
		&allowedChannelsJSON,
		&client.Enabled,
		&client.CreatedAt,
		&client.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.NotificationClient{}, ErrNotFound
	}
	if err != nil {
		return notification.NotificationClient{}, fmt.Errorf("query notification client: %w", err)
	}
	if len(allowedChannelsJSON) > 0 {
		if err := json.Unmarshal(allowedChannelsJSON, &client.AllowedChannels); err != nil {
			return notification.NotificationClient{}, fmt.Errorf("unmarshal allowed channels: %w", err)
		}
	}
	return client, nil
}

func (s *Store) GetNotificationClientByAPIKeyHash(ctx context.Context, apiKeyHash string) (client notification.NotificationClient, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_notification_client_by_api_key_hash", startedAt, err)
	}()

	const query = `
		SELECT client_id, tenant_id, client_name, allowed_channels, enabled, created_at, updated_at
		FROM notification_clients
		WHERE api_key_hash = $1
		LIMIT 1
	`

	var allowedChannelsJSON []byte
	err = s.db.QueryRowContext(ctx, query, apiKeyHash).Scan(
		&client.ClientID,
		&client.TenantID,
		&client.ClientName,
		&allowedChannelsJSON,
		&client.Enabled,
		&client.CreatedAt,
		&client.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.NotificationClient{}, ErrNotFound
	}
	if err != nil {
		return notification.NotificationClient{}, fmt.Errorf("query notification client by api key hash: %w", err)
	}
	if len(allowedChannelsJSON) > 0 {
		if err := json.Unmarshal(allowedChannelsJSON, &client.AllowedChannels); err != nil {
			return notification.NotificationClient{}, fmt.Errorf("unmarshal allowed channels: %w", err)
		}
	}
	return client, nil
}

func (s *Store) ListNotificationClients(ctx context.Context, tenantID string) (clients []notification.NotificationClient, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_notification_clients", startedAt, err)
	}()

	const baseQuery = `
		SELECT client_id, tenant_id, client_name, allowed_channels, enabled, created_at, updated_at
		FROM notification_clients`

	query := baseQuery
	args := []any{}
	if tenantID != "" {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY tenant_id ASC, client_name ASC, client_id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query notification clients: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var client notification.NotificationClient
		var allowedChannelsJSON []byte
		if err := rows.Scan(
			&client.ClientID,
			&client.TenantID,
			&client.ClientName,
			&allowedChannelsJSON,
			&client.Enabled,
			&client.CreatedAt,
			&client.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan notification client: %w", err)
		}
		if len(allowedChannelsJSON) > 0 {
			if err := json.Unmarshal(allowedChannelsJSON, &client.AllowedChannels); err != nil {
				return nil, fmt.Errorf("unmarshal allowed channels: %w", err)
			}
		}
		clients = append(clients, client)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notification clients: %w", err)
	}
	return clients, nil
}

func (s *Store) UpdateNotificationClient(ctx context.Context, client notification.NotificationClient) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("update_notification_client", startedAt, err)
	}()

	allowedChannelsJSON, err := json.Marshal(client.AllowedChannels)
	if err != nil {
		return fmt.Errorf("marshal allowed channels: %w", err)
	}

	const query = `
		UPDATE notification_clients
		SET tenant_id = $2,
		    client_name = $3,
		    allowed_channels = $4::jsonb,
		    enabled = $5,
		    updated_at = NOW()
		WHERE client_id = $1
	`
	result, err := s.db.ExecContext(ctx, query, client.ClientID, client.TenantID, client.ClientName, string(allowedChannelsJSON), client.Enabled)
	if err != nil {
		return fmt.Errorf("update notification client: %w", err)
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
