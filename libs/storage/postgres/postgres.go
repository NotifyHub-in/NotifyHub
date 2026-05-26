package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

var ErrNotFound = errors.New("record not found")

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) CreateNotificationRequest(ctx context.Context, record notification.NotificationRecord) error {
	recipientJSON, err := json.Marshal(record.Recipient)
	if err != nil {
		return fmt.Errorf("marshal recipient: %w", err)
	}
	channelsJSON, err := json.Marshal(record.Channels)
	if err != nil {
		return fmt.Errorf("marshal channels: %w", err)
	}
	variablesJSON, err := json.Marshal(record.Variables)
	if err != nil {
		return fmt.Errorf("marshal variables: %w", err)
	}
	metadataJSON, err := json.Marshal(record.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	const query = `
		INSERT INTO notification_requests (
			request_id, idempotency_key, event_name, template_key, channels, recipient,
			variables, metadata, priority, status, requested_at
		) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, $11)
	`

	_, err = s.db.ExecContext(ctx, query,
		record.RequestID,
		record.IdempotencyKey,
		record.EventName,
		record.TemplateKey,
		string(channelsJSON),
		string(recipientJSON),
		string(variablesJSON),
		string(metadataJSON),
		record.Priority,
		record.Status,
		record.RequestedAt,
	)
	if err != nil {
		return fmt.Errorf("insert notification request: %w", err)
	}

	return nil
}

func (s *Store) GetNotificationRequest(ctx context.Context, requestID string) (notification.NotificationRecord, error) {
	const query = `
		SELECT request_id, idempotency_key, event_name, template_key, channels, recipient, variables,
		       metadata, priority, status, requested_at, created_at, updated_at
		FROM notification_requests
		WHERE request_id = $1
	`

	var (
		record        notification.NotificationRecord
		channelsJSON  []byte
		recipientJSON []byte
		variablesJSON []byte
		metadataJSON  []byte
	)

	err := s.db.QueryRowContext(ctx, query, requestID).Scan(
		&record.RequestID,
		&record.IdempotencyKey,
		&record.EventName,
		&record.TemplateKey,
		&channelsJSON,
		&recipientJSON,
		&variablesJSON,
		&metadataJSON,
		&record.Priority,
		&record.Status,
		&record.RequestedAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.NotificationRecord{}, ErrNotFound
	}
	if err != nil {
		return notification.NotificationRecord{}, fmt.Errorf("query notification request: %w", err)
	}

	if err := json.Unmarshal(channelsJSON, &record.Channels); err != nil {
		return notification.NotificationRecord{}, fmt.Errorf("unmarshal channels: %w", err)
	}
	if err := json.Unmarshal(recipientJSON, &record.Recipient); err != nil {
		return notification.NotificationRecord{}, fmt.Errorf("unmarshal recipient: %w", err)
	}
	if len(variablesJSON) > 0 {
		if err := json.Unmarshal(variablesJSON, &record.Variables); err != nil {
			return notification.NotificationRecord{}, fmt.Errorf("unmarshal variables: %w", err)
		}
	}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
			return notification.NotificationRecord{}, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return record, nil
}

func (s *Store) UpdateNotificationRequestStatus(ctx context.Context, requestID string, status notification.RequestStatus) error {
	const query = `UPDATE notification_requests SET status = $2, updated_at = NOW() WHERE request_id = $1`
	result, err := s.db.ExecContext(ctx, query, requestID, status)
	if err != nil {
		return fmt.Errorf("update notification request status: %w", err)
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

func (s *Store) CreateDeliveryAttempt(ctx context.Context, attempt notification.DeliveryAttempt) error {
	const query = `
		INSERT INTO delivery_attempts (
			attempt_id, request_id, channel, connector_name, status, provider_message_id, destination, error_message
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := s.db.ExecContext(ctx, query,
		attempt.AttemptID,
		attempt.RequestID,
		attempt.Channel,
		attempt.ConnectorName,
		attempt.Status,
		attempt.ProviderMessageID,
		attempt.Destination,
		attempt.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}
	return nil
}

func (s *Store) UpdateDeliveryAttempt(ctx context.Context, attempt notification.DeliveryAttempt) error {
	const query = `
		UPDATE delivery_attempts
		SET status = $2, provider_message_id = $3, destination = $4, error_message = $5, updated_at = NOW()
		WHERE attempt_id = $1
	`

	result, err := s.db.ExecContext(ctx, query,
		attempt.AttemptID,
		attempt.Status,
		attempt.ProviderMessageID,
		attempt.Destination,
		attempt.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("update delivery attempt: %w", err)
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

func (s *Store) ListDeliveryAttempts(ctx context.Context, requestID string) ([]notification.DeliveryAttempt, error) {
	const query = `
		SELECT attempt_id, request_id, channel, connector_name, status, provider_message_id,
		       destination, error_message, created_at, updated_at
		FROM delivery_attempts
		WHERE request_id = $1
		ORDER BY created_at ASC
	`

	rows, err := s.db.QueryContext(ctx, query, requestID)
	if err != nil {
		return nil, fmt.Errorf("query delivery attempts: %w", err)
	}
	defer rows.Close()

	var attempts []notification.DeliveryAttempt
	for rows.Next() {
		var attempt notification.DeliveryAttempt
		if err := rows.Scan(
			&attempt.AttemptID,
			&attempt.RequestID,
			&attempt.Channel,
			&attempt.ConnectorName,
			&attempt.Status,
			&attempt.ProviderMessageID,
			&attempt.Destination,
			&attempt.ErrorMessage,
			&attempt.CreatedAt,
			&attempt.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan delivery attempt: %w", err)
		}
		attempts = append(attempts, attempt)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery attempts: %w", err)
	}

	return attempts, nil
}
