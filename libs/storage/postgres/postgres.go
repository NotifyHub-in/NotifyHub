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

func (s *Store) UpsertProviderBinding(ctx context.Context, binding notification.ProviderBinding) error {
	const query = `
		INSERT INTO provider_bindings (
			binding_id, channel, connector_name, endpoint_url, enabled, priority
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (channel)
		DO UPDATE SET
			connector_name = EXCLUDED.connector_name,
			endpoint_url = EXCLUDED.endpoint_url,
			enabled = EXCLUDED.enabled,
			priority = EXCLUDED.priority,
			updated_at = NOW()
	`

	_, err := s.db.ExecContext(ctx, query,
		binding.BindingID,
		binding.Channel,
		binding.ConnectorName,
		binding.EndpointURL,
		binding.Enabled,
		binding.Priority,
	)
	if err != nil {
		return fmt.Errorf("upsert provider binding: %w", err)
	}
	return nil
}

func (s *Store) ListProviderBindings(ctx context.Context) ([]notification.ProviderBinding, error) {
	const query = `
		SELECT binding_id, channel, connector_name, endpoint_url, enabled, priority, created_at, updated_at
		FROM provider_bindings
		ORDER BY priority ASC, channel ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query provider bindings: %w", err)
	}
	defer rows.Close()

	var bindings []notification.ProviderBinding
	for rows.Next() {
		var binding notification.ProviderBinding
		if err := rows.Scan(
			&binding.BindingID,
			&binding.Channel,
			&binding.ConnectorName,
			&binding.EndpointURL,
			&binding.Enabled,
			&binding.Priority,
			&binding.CreatedAt,
			&binding.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan provider binding: %w", err)
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider bindings: %w", err)
	}
	return bindings, nil
}

func (s *Store) GetProviderBindingByChannel(ctx context.Context, channel notification.Channel) (notification.ProviderBinding, error) {
	const query = `
		SELECT binding_id, channel, connector_name, endpoint_url, enabled, priority, created_at, updated_at
		FROM provider_bindings
		WHERE channel = $1 AND enabled = TRUE
		ORDER BY priority ASC
		LIMIT 1
	`

	var binding notification.ProviderBinding
	err := s.db.QueryRowContext(ctx, query, channel).Scan(
		&binding.BindingID,
		&binding.Channel,
		&binding.ConnectorName,
		&binding.EndpointURL,
		&binding.Enabled,
		&binding.Priority,
		&binding.CreatedAt,
		&binding.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.ProviderBinding{}, ErrNotFound
	}
	if err != nil {
		return notification.ProviderBinding{}, fmt.Errorf("query provider binding: %w", err)
	}
	return binding, nil
}

func (s *Store) UpsertRoutingPolicy(ctx context.Context, policy notification.RoutingPolicy) error {
	channelsJSON, err := json.Marshal(policy.Channels)
	if err != nil {
		return fmt.Errorf("marshal routing policy channels: %w", err)
	}

	const query = `
		INSERT INTO routing_policies (
			policy_id, event_name, channels, enabled, priority
		) VALUES ($1, $2, $3::jsonb, $4, $5)
		ON CONFLICT (event_name)
		DO UPDATE SET
			channels = EXCLUDED.channels,
			enabled = EXCLUDED.enabled,
			priority = EXCLUDED.priority,
			updated_at = NOW()
	`

	_, err = s.db.ExecContext(ctx, query,
		policy.PolicyID,
		policy.EventName,
		string(channelsJSON),
		policy.Enabled,
		policy.Priority,
	)
	if err != nil {
		return fmt.Errorf("upsert routing policy: %w", err)
	}
	return nil
}

func (s *Store) ListRoutingPolicies(ctx context.Context) ([]notification.RoutingPolicy, error) {
	const query = `
		SELECT policy_id, event_name, channels, enabled, priority, created_at, updated_at
		FROM routing_policies
		ORDER BY priority ASC, event_name ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query routing policies: %w", err)
	}
	defer rows.Close()

	var policies []notification.RoutingPolicy
	for rows.Next() {
		var (
			policy       notification.RoutingPolicy
			channelsJSON []byte
		)
		if err := rows.Scan(
			&policy.PolicyID,
			&policy.EventName,
			&channelsJSON,
			&policy.Enabled,
			&policy.Priority,
			&policy.CreatedAt,
			&policy.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan routing policy: %w", err)
		}
		if err := json.Unmarshal(channelsJSON, &policy.Channels); err != nil {
			return nil, fmt.Errorf("unmarshal routing policy channels: %w", err)
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate routing policies: %w", err)
	}
	return policies, nil
}

func (s *Store) GetRoutingPolicyByEventName(ctx context.Context, eventName string) (notification.RoutingPolicy, error) {
	const query = `
		SELECT policy_id, event_name, channels, enabled, priority, created_at, updated_at
		FROM routing_policies
		WHERE event_name = $1 AND enabled = TRUE
		ORDER BY priority ASC
		LIMIT 1
	`

	var (
		policy       notification.RoutingPolicy
		channelsJSON []byte
	)
	err := s.db.QueryRowContext(ctx, query, eventName).Scan(
		&policy.PolicyID,
		&policy.EventName,
		&channelsJSON,
		&policy.Enabled,
		&policy.Priority,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.RoutingPolicy{}, ErrNotFound
	}
	if err != nil {
		return notification.RoutingPolicy{}, fmt.Errorf("query routing policy: %w", err)
	}
	if err := json.Unmarshal(channelsJSON, &policy.Channels); err != nil {
		return notification.RoutingPolicy{}, fmt.Errorf("unmarshal routing policy channels: %w", err)
	}
	return policy, nil
}

func (s *Store) UpsertPreferencePolicy(ctx context.Context, policy notification.PreferencePolicy) error {
	const query = `
		INSERT INTO preference_policies (
			policy_id, user_id, channel, is_enabled
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, channel)
		DO UPDATE SET
			is_enabled = EXCLUDED.is_enabled,
			updated_at = NOW()
	`

	_, err := s.db.ExecContext(ctx, query,
		policy.PolicyID,
		policy.UserID,
		policy.Channel,
		policy.IsEnabled,
	)
	if err != nil {
		return fmt.Errorf("upsert preference policy: %w", err)
	}
	return nil
}

func (s *Store) ListPreferencePolicies(ctx context.Context, userID string) ([]notification.PreferencePolicy, error) {
	query := `
		SELECT policy_id, user_id, channel, is_enabled, created_at, updated_at
		FROM preference_policies
	`
	args := []any{}
	if userID != "" {
		query += ` WHERE user_id = $1`
		args = append(args, userID)
	}
	query += ` ORDER BY user_id ASC, channel ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query preference policies: %w", err)
	}
	defer rows.Close()

	var policies []notification.PreferencePolicy
	for rows.Next() {
		var policy notification.PreferencePolicy
		if err := rows.Scan(
			&policy.PolicyID,
			&policy.UserID,
			&policy.Channel,
			&policy.IsEnabled,
			&policy.CreatedAt,
			&policy.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan preference policy: %w", err)
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate preference policies: %w", err)
	}
	return policies, nil
}

func (s *Store) GetPreferencePolicy(ctx context.Context, userID string, channel notification.Channel) (notification.PreferencePolicy, error) {
	const query = `
		SELECT policy_id, user_id, channel, is_enabled, created_at, updated_at
		FROM preference_policies
		WHERE user_id = $1 AND channel = $2
		LIMIT 1
	`

	var policy notification.PreferencePolicy
	err := s.db.QueryRowContext(ctx, query, userID, channel).Scan(
		&policy.PolicyID,
		&policy.UserID,
		&policy.Channel,
		&policy.IsEnabled,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.PreferencePolicy{}, ErrNotFound
	}
	if err != nil {
		return notification.PreferencePolicy{}, fmt.Errorf("query preference policy: %w", err)
	}
	return policy, nil
}

func (s *Store) UpsertTemplate(ctx context.Context, tmpl notification.Template) error {
	const query = `
		INSERT INTO templates (
			template_id, template_key, channel, subject_template, body_template, enabled
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (template_key, channel)
		DO UPDATE SET
			subject_template = EXCLUDED.subject_template,
			body_template = EXCLUDED.body_template,
			enabled = EXCLUDED.enabled,
			updated_at = NOW()
	`

	_, err := s.db.ExecContext(ctx, query,
		tmpl.TemplateID,
		tmpl.TemplateKey,
		tmpl.Channel,
		tmpl.SubjectTemplate,
		tmpl.BodyTemplate,
		tmpl.Enabled,
	)
	if err != nil {
		return fmt.Errorf("upsert template: %w", err)
	}
	return nil
}

func (s *Store) ListTemplates(ctx context.Context) ([]notification.Template, error) {
	const query = `
		SELECT template_id, template_key, channel, subject_template, body_template, enabled, created_at, updated_at
		FROM templates
		ORDER BY template_key ASC, channel ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query templates: %w", err)
	}
	defer rows.Close()

	var templates []notification.Template
	for rows.Next() {
		var tmpl notification.Template
		if err := rows.Scan(
			&tmpl.TemplateID,
			&tmpl.TemplateKey,
			&tmpl.Channel,
			&tmpl.SubjectTemplate,
			&tmpl.BodyTemplate,
			&tmpl.Enabled,
			&tmpl.CreatedAt,
			&tmpl.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		templates = append(templates, tmpl)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate templates: %w", err)
	}
	return templates, nil
}

func (s *Store) GetTemplateByKeyAndChannel(ctx context.Context, templateKey string, channel notification.Channel) (notification.Template, error) {
	const query = `
		SELECT template_id, template_key, channel, subject_template, body_template, enabled, created_at, updated_at
		FROM templates
		WHERE template_key = $1 AND channel = $2 AND enabled = TRUE
		LIMIT 1
	`

	var tmpl notification.Template
	err := s.db.QueryRowContext(ctx, query, templateKey, channel).Scan(
		&tmpl.TemplateID,
		&tmpl.TemplateKey,
		&tmpl.Channel,
		&tmpl.SubjectTemplate,
		&tmpl.BodyTemplate,
		&tmpl.Enabled,
		&tmpl.CreatedAt,
		&tmpl.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.Template{}, ErrNotFound
	}
	if err != nil {
		return notification.Template{}, fmt.Errorf("query template: %w", err)
	}
	return tmpl, nil
}
