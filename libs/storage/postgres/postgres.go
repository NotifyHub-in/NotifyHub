package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	obsmetrics "github.com/your-org/notification-control-plane/libs/observability/metrics"
)

var ErrNotFound = errors.New("record not found")
var ErrConflict = errors.New("record conflict")
var storeRegistry *obsmetrics.Registry

type Store struct {
	db *sql.DB
}

func AttachMetrics(registry *obsmetrics.Registry) {
	storeRegistry = registry
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

func observeDBOperation(operation string, startedAt time.Time, err error) {
	if storeRegistry == nil {
		return
	}
	labels := map[string]string{
		"service":   storeRegistry.Service(),
		"operation": operation,
		"status":    "ok",
	}
	if err != nil {
		labels["status"] = "error"
	}
	storeRegistry.ObserveHistogram("db_operation_duration_seconds", "Database operation duration in seconds.", labels, obsmetrics.DefaultLatencyBuckets(), time.Since(startedAt).Seconds())
	storeRegistry.IncCounter("db_operations_total", "Total database operations.", labels)
}

func (s *Store) CreateNotificationRequest(ctx context.Context, record notification.NotificationRecord) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("create_notification_request", startedAt, err)
	}()

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
			request_id, idempotency_key, event_name, template_key, channels, binding_set, recipient,
			variables, metadata, priority, status, requested_at, expires_at
		) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10, $11, $12, $13)
	`

	_, err = s.db.ExecContext(ctx, query,
		record.RequestID,
		record.IdempotencyKey,
		record.EventName,
		record.TemplateKey,
		string(channelsJSON),
		record.BindingSet,
		string(recipientJSON),
		string(variablesJSON),
		string(metadataJSON),
		record.Priority,
		record.Status,
		record.RequestedAt,
		record.ExpiresAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrConflict
		}
		return fmt.Errorf("insert notification request: %w", err)
	}

	return nil
}

func (s *Store) GetNotificationRequestByIdempotencyKey(ctx context.Context, idempotencyKey string) (record notification.NotificationRecord, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_notification_request_by_idempotency_key", startedAt, err)
	}()

	const query = `
		SELECT request_id, idempotency_key, event_name, template_key, channels, binding_set, recipient, variables,
		       metadata, priority, status, requested_at, expires_at, created_at, updated_at
		FROM notification_requests
		WHERE idempotency_key = $1
		LIMIT 1
	`

	var (
		channelsJSON  []byte
		recipientJSON []byte
		variablesJSON []byte
		metadataJSON  []byte
	)

	var expiresAt sql.NullTime

	err = s.db.QueryRowContext(ctx, query, idempotencyKey).Scan(
		&record.RequestID,
		&record.IdempotencyKey,
		&record.EventName,
		&record.TemplateKey,
		&channelsJSON,
		&record.BindingSet,
		&recipientJSON,
		&variablesJSON,
		&metadataJSON,
		&record.Priority,
		&record.Status,
		&record.RequestedAt,
		&expiresAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.NotificationRecord{}, ErrNotFound
	}
	if err != nil {
		return notification.NotificationRecord{}, fmt.Errorf("query notification request by idempotency key: %w", err)
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
	if expiresAt.Valid {
		record.ExpiresAt = &expiresAt.Time
	}

	return record, nil
}

func (s *Store) GetNotificationRequest(ctx context.Context, requestID string) (record notification.NotificationRecord, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_notification_request", startedAt, err)
	}()

	const query = `
		SELECT request_id, idempotency_key, event_name, template_key, channels, binding_set, recipient, variables,
		       metadata, priority, status, requested_at, expires_at, created_at, updated_at
		FROM notification_requests
		WHERE request_id = $1
	`

	var (
		channelsJSON  []byte
		recipientJSON []byte
		variablesJSON []byte
		metadataJSON  []byte
	)

	var expiresAt sql.NullTime

	err = s.db.QueryRowContext(ctx, query, requestID).Scan(
		&record.RequestID,
		&record.IdempotencyKey,
		&record.EventName,
		&record.TemplateKey,
		&channelsJSON,
		&record.BindingSet,
		&recipientJSON,
		&variablesJSON,
		&metadataJSON,
		&record.Priority,
		&record.Status,
		&record.RequestedAt,
		&expiresAt,
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
	if expiresAt.Valid {
		record.ExpiresAt = &expiresAt.Time
	}

	return record, nil
}

func (s *Store) UpdateNotificationRequestStatus(ctx context.Context, requestID string, status notification.RequestStatus) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("update_notification_request_status", startedAt, err)
	}()

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

func (s *Store) CreateDeliveryAttempt(ctx context.Context, attempt notification.DeliveryAttempt) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("create_delivery_attempt", startedAt, err)
	}()

	const query = `
		INSERT INTO delivery_attempts (
			attempt_id, request_id, attempt_number, max_attempts, channel, connector_name, status, provider_message_id, destination, error_message
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`

	_, err = s.db.ExecContext(ctx, query,
		attempt.AttemptID,
		attempt.RequestID,
		attempt.AttemptNumber,
		attempt.MaxAttempts,
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

func (s *Store) UpdateDeliveryAttempt(ctx context.Context, attempt notification.DeliveryAttempt) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("update_delivery_attempt", startedAt, err)
	}()

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

func (s *Store) ListDeliveryAttempts(ctx context.Context, requestID string) (attempts []notification.DeliveryAttempt, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_delivery_attempts", startedAt, err)
	}()

	const query = `
		SELECT attempt_id, request_id, channel, connector_name, status, provider_message_id,
		       destination, error_message, attempt_number, max_attempts, created_at, updated_at
		FROM delivery_attempts
		WHERE request_id = $1
		ORDER BY attempt_number ASC, created_at ASC
	`

	rows, err := s.db.QueryContext(ctx, query, requestID)
	if err != nil {
		return nil, fmt.Errorf("query delivery attempts: %w", err)
	}
	defer rows.Close()

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
			&attempt.AttemptNumber,
			&attempt.MaxAttempts,
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

func (s *Store) GetDeliveryAttemptByProviderMessageID(ctx context.Context, providerMessageID string) (attempt notification.DeliveryAttempt, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_delivery_attempt_by_provider_message_id", startedAt, err)
	}()

	const query = `
		SELECT attempt_id, request_id, channel, connector_name, status, provider_message_id,
		       destination, error_message, attempt_number, max_attempts, created_at, updated_at
		FROM delivery_attempts
		WHERE provider_message_id = $1
		ORDER BY updated_at DESC
		LIMIT 1
	`

	err = s.db.QueryRowContext(ctx, query, providerMessageID).Scan(
		&attempt.AttemptID,
		&attempt.RequestID,
		&attempt.Channel,
		&attempt.ConnectorName,
		&attempt.Status,
		&attempt.ProviderMessageID,
		&attempt.Destination,
		&attempt.ErrorMessage,
		&attempt.AttemptNumber,
		&attempt.MaxAttempts,
		&attempt.CreatedAt,
		&attempt.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.DeliveryAttempt{}, ErrNotFound
	}
	if err != nil {
		return notification.DeliveryAttempt{}, fmt.Errorf("query delivery attempt by provider message id: %w", err)
	}
	return attempt, nil
}

func (s *Store) UpsertProviderBinding(ctx context.Context, binding notification.ProviderBinding) error {
	configRefsJSON, err := marshalStringMap(binding.ConfigRefs)
	if err != nil {
		return fmt.Errorf("marshal provider binding config refs: %w", err)
	}

	const query = `
		INSERT INTO provider_bindings (
			binding_id, channel, binding_set, connector_name, endpoint_url, config_refs, enabled, priority
		) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
		ON CONFLICT (channel, binding_set, connector_name)
		DO UPDATE SET
			endpoint_url = EXCLUDED.endpoint_url,
			config_refs = EXCLUDED.config_refs,
			enabled = EXCLUDED.enabled,
			priority = EXCLUDED.priority,
			updated_at = NOW()
	`

	_, err = s.db.ExecContext(ctx, query,
		binding.BindingID,
		binding.Channel,
		binding.BindingSet,
		binding.ConnectorName,
		binding.EndpointURL,
		string(configRefsJSON),
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
		SELECT binding_id, channel, binding_set, connector_name, endpoint_url, config_refs, enabled, priority, created_at, updated_at
		FROM provider_bindings
		ORDER BY priority ASC, channel ASC, binding_set ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query provider bindings: %w", err)
	}
	defer rows.Close()

	var bindings []notification.ProviderBinding
	for rows.Next() {
		var (
			binding        notification.ProviderBinding
			configRefsJSON []byte
		)
		if err := rows.Scan(
			&binding.BindingID,
			&binding.Channel,
			&binding.BindingSet,
			&binding.ConnectorName,
			&binding.EndpointURL,
			&configRefsJSON,
			&binding.Enabled,
			&binding.Priority,
			&binding.CreatedAt,
			&binding.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan provider binding: %w", err)
		}
		binding.ConfigRefs, err = unmarshalStringMap(configRefsJSON)
		if err != nil {
			return nil, fmt.Errorf("unmarshal provider binding config refs: %w", err)
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider bindings: %w", err)
	}
	return bindings, nil
}

func (s *Store) ListProviderBindingsByChannel(ctx context.Context, channel notification.Channel, bindingSet string) (bindings []notification.ProviderBinding, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_provider_bindings_by_channel", startedAt, err)
	}()

	loadBindings := func(query string, args ...any) ([]notification.ProviderBinding, error) {
		rows, queryErr := s.db.QueryContext(ctx, query, args...)
		if queryErr != nil {
			return nil, fmt.Errorf("query provider bindings by channel: %w", queryErr)
		}
		defer rows.Close()

		var loaded []notification.ProviderBinding
		for rows.Next() {
			var (
				binding        notification.ProviderBinding
				configRefsJSON []byte
			)
			if err := rows.Scan(
				&binding.BindingID,
				&binding.Channel,
				&binding.BindingSet,
				&binding.ConnectorName,
				&binding.EndpointURL,
				&configRefsJSON,
				&binding.Enabled,
				&binding.Priority,
				&binding.CreatedAt,
				&binding.UpdatedAt,
			); err != nil {
				return nil, fmt.Errorf("scan provider binding by channel: %w", err)
			}
			binding.ConfigRefs, err = unmarshalStringMap(configRefsJSON)
			if err != nil {
				return nil, fmt.Errorf("unmarshal provider binding by channel config refs: %w", err)
			}
			loaded = append(loaded, binding)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate provider bindings by channel: %w", err)
		}
		return loaded, nil
	}

	baseQuery := `
		SELECT binding_id, channel, binding_set, connector_name, endpoint_url, config_refs, enabled, priority, created_at, updated_at
		FROM provider_bindings
		WHERE channel = $1 AND enabled = TRUE
	`
	if bindingSet != "" {
		bindings, err = loadBindings(baseQuery+` AND binding_set = $2 ORDER BY priority ASC, connector_name ASC`, channel, bindingSet)
		if err != nil {
			return nil, err
		}
		if len(bindings) == 0 {
			return nil, ErrNotFound
		}
		return bindings, nil
	}

	bindings, err = loadBindings(baseQuery+` AND binding_set = '' ORDER BY priority ASC, connector_name ASC`, channel)
	if err != nil {
		return nil, err
	}
	if len(bindings) > 0 {
		return bindings, nil
	}

	// Legacy fallback for deployments that have channel bindings but no explicit default set yet.
	bindings, err = loadBindings(baseQuery+` ORDER BY priority ASC, connector_name ASC`, channel)
	if err != nil {
		return nil, err
	}
	if len(bindings) == 0 {
		return nil, ErrNotFound
	}
	return bindings, nil
}

func marshalStringMap(values map[string]string) ([]byte, error) {
	if len(values) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(values)
}

func unmarshalStringMap(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return map[string]string{}, nil
	}

	values := make(map[string]string)
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	return values, nil
}

func (s *Store) GetProviderBindingByChannel(ctx context.Context, channel notification.Channel, bindingSet string) (notification.ProviderBinding, error) {
	bindings, err := s.ListProviderBindingsByChannel(ctx, channel, bindingSet)
	if err != nil {
		return notification.ProviderBinding{}, err
	}
	return bindings[0], nil
}

func (s *Store) UpsertProviderBindingHealth(ctx context.Context, health notification.ProviderBindingHealth) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("upsert_provider_binding_health", startedAt, err)
	}()

	const query = `
		INSERT INTO provider_binding_health (
			binding_id, channel, binding_set, connector_name, circuit_state, consecutive_failures,
			opened_at, cooldown_until, last_failure_class, last_error, last_failure_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (binding_id)
		DO UPDATE SET
			channel = EXCLUDED.channel,
			binding_set = EXCLUDED.binding_set,
			connector_name = EXCLUDED.connector_name,
			circuit_state = EXCLUDED.circuit_state,
			consecutive_failures = EXCLUDED.consecutive_failures,
			opened_at = EXCLUDED.opened_at,
			cooldown_until = EXCLUDED.cooldown_until,
			last_failure_class = EXCLUDED.last_failure_class,
			last_error = EXCLUDED.last_error,
			last_failure_at = EXCLUDED.last_failure_at,
			updated_at = NOW()
	`

	_, err = s.db.ExecContext(ctx, query,
		health.BindingID,
		health.Channel,
		health.BindingSet,
		health.ConnectorName,
		health.CircuitState,
		health.ConsecutiveFailures,
		health.OpenedAt,
		health.CooldownUntil,
		health.LastFailureClass,
		health.LastError,
		health.LastFailureAt,
	)
	if err != nil {
		return fmt.Errorf("upsert provider binding health: %w", err)
	}
	return nil
}

func (s *Store) GetProviderBindingHealth(ctx context.Context, bindingID string) (health notification.ProviderBindingHealth, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_provider_binding_health", startedAt, err)
	}()

	const query = `
		SELECT binding_id, channel, binding_set, connector_name, circuit_state, consecutive_failures,
		       opened_at, cooldown_until, last_failure_class, last_error, last_failure_at, created_at, updated_at
		FROM provider_binding_health
		WHERE binding_id = $1
		LIMIT 1
	`

	health, err = scanProviderBindingHealth(s.db.QueryRowContext(ctx, query, bindingID))
	if errors.Is(err, sql.ErrNoRows) {
		return notification.ProviderBindingHealth{}, ErrNotFound
	}
	if err != nil {
		return notification.ProviderBindingHealth{}, fmt.Errorf("query provider binding health: %w", err)
	}
	return health, nil
}

func (s *Store) ListProviderBindingHealth(ctx context.Context) (healthRecords []notification.ProviderBindingHealth, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_provider_binding_health", startedAt, err)
	}()

	const query = `
		SELECT binding_id, channel, binding_set, connector_name, circuit_state, consecutive_failures,
		       opened_at, cooldown_until, last_failure_class, last_error, last_failure_at, created_at, updated_at
		FROM provider_binding_health
		ORDER BY channel ASC, binding_set ASC, connector_name ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query provider binding health: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		record, scanErr := scanProviderBindingHealth(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan provider binding health: %w", scanErr)
		}
		healthRecords = append(healthRecords, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider binding health: %w", err)
	}
	return healthRecords, nil
}

func (s *Store) ResetProviderBindingHealth(ctx context.Context, bindingID string) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("reset_provider_binding_health", startedAt, err)
	}()

	const query = `
		UPDATE provider_binding_health
		SET circuit_state = $2,
		    consecutive_failures = 0,
		    opened_at = NULL,
		    cooldown_until = NULL,
		    last_failure_class = '',
		    last_error = '',
		    last_failure_at = NULL,
		    updated_at = NOW()
		WHERE binding_id = $1
	`

	result, err := s.db.ExecContext(ctx, query, bindingID, notification.ProviderCircuitStateClosed)
	if err != nil {
		return fmt.Errorf("reset provider binding health: %w", err)
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

type rowScanner interface {
	Scan(dest ...any) error
}

func scanProviderBindingHealth(scanner rowScanner) (notification.ProviderBindingHealth, error) {
	var (
		record          notification.ProviderBindingHealth
		openedAt        sql.NullTime
		cooldownUntil   sql.NullTime
		lastFailureAt   sql.NullTime
		lastFailureText string
	)

	err := scanner.Scan(
		&record.BindingID,
		&record.Channel,
		&record.BindingSet,
		&record.ConnectorName,
		&record.CircuitState,
		&record.ConsecutiveFailures,
		&openedAt,
		&cooldownUntil,
		&lastFailureText,
		&record.LastError,
		&lastFailureAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return notification.ProviderBindingHealth{}, err
	}
	if openedAt.Valid {
		record.OpenedAt = &openedAt.Time
	}
	if cooldownUntil.Valid {
		record.CooldownUntil = &cooldownUntil.Time
	}
	if lastFailureAt.Valid {
		record.LastFailureAt = &lastFailureAt.Time
	}
	record.LastFailureClass = notification.FailureClass(lastFailureText)
	if record.CircuitState == "" {
		record.CircuitState = notification.ProviderCircuitStateClosed
	}
	return record, nil
}

func (s *Store) UpsertRoutingPolicy(ctx context.Context, policy notification.RoutingPolicy) error {
	channelsJSON, err := json.Marshal(policy.Channels)
	if err != nil {
		return fmt.Errorf("marshal routing policy channels: %w", err)
	}

	const query = `
		INSERT INTO routing_policies (
			policy_id, event_name, channels, binding_set, enabled, priority
		) VALUES ($1, $2, $3::jsonb, $4, $5, $6)
		ON CONFLICT (event_name)
		DO UPDATE SET
			channels = EXCLUDED.channels,
			binding_set = EXCLUDED.binding_set,
			enabled = EXCLUDED.enabled,
			priority = EXCLUDED.priority,
			updated_at = NOW()
	`

	_, err = s.db.ExecContext(ctx, query,
		policy.PolicyID,
		policy.EventName,
		string(channelsJSON),
		policy.BindingSet,
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
		SELECT policy_id, event_name, channels, binding_set, enabled, priority, created_at, updated_at
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
			&policy.BindingSet,
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

func (s *Store) GetRoutingPolicyByEventName(ctx context.Context, eventName string) (policy notification.RoutingPolicy, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_routing_policy_by_event_name", startedAt, err)
	}()

	const query = `
		SELECT policy_id, event_name, channels, binding_set, enabled, priority, created_at, updated_at
		FROM routing_policies
		WHERE event_name = $1 AND enabled = TRUE
		ORDER BY priority ASC
		LIMIT 1
	`

	var channelsJSON []byte
	err = s.db.QueryRowContext(ctx, query, eventName).Scan(
		&policy.PolicyID,
		&policy.EventName,
		&channelsJSON,
		&policy.BindingSet,
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

func (s *Store) ListPreferencePolicies(ctx context.Context, userID string) (policies []notification.PreferencePolicy, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_preference_policies", startedAt, err)
	}()

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

func (s *Store) GetTemplateByKeyAndChannel(ctx context.Context, templateKey string, channel notification.Channel) (tmpl notification.Template, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_template_by_key_and_channel", startedAt, err)
	}()

	const query = `
		SELECT template_id, template_key, channel, subject_template, body_template, enabled, created_at, updated_at
		FROM templates
		WHERE template_key = $1 AND channel = $2 AND enabled = TRUE
		LIMIT 1
	`

	err = s.db.QueryRowContext(ctx, query, templateKey, channel).Scan(
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

func (s *Store) UpsertDeliveryPolicy(ctx context.Context, policy notification.DeliveryPolicy) error {
	const query = `
		INSERT INTO delivery_policies (
			policy_id, channel, max_attempts, backoff_seconds, enabled
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (channel)
		DO UPDATE SET
			max_attempts = EXCLUDED.max_attempts,
			backoff_seconds = EXCLUDED.backoff_seconds,
			enabled = EXCLUDED.enabled,
			updated_at = NOW()
	`

	_, err := s.db.ExecContext(ctx, query,
		policy.PolicyID,
		policy.Channel,
		policy.MaxAttempts,
		policy.BackoffSeconds,
		policy.Enabled,
	)
	if err != nil {
		return fmt.Errorf("upsert delivery policy: %w", err)
	}
	return nil
}

func (s *Store) ListDeliveryPolicies(ctx context.Context) ([]notification.DeliveryPolicy, error) {
	const query = `
		SELECT policy_id, channel, max_attempts, backoff_seconds, enabled, created_at, updated_at
		FROM delivery_policies
		ORDER BY channel ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query delivery policies: %w", err)
	}
	defer rows.Close()

	var policies []notification.DeliveryPolicy
	for rows.Next() {
		var policy notification.DeliveryPolicy
		if err := rows.Scan(
			&policy.PolicyID,
			&policy.Channel,
			&policy.MaxAttempts,
			&policy.BackoffSeconds,
			&policy.Enabled,
			&policy.CreatedAt,
			&policy.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan delivery policy: %w", err)
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery policies: %w", err)
	}
	return policies, nil
}

func (s *Store) GetDeliveryPolicyByChannel(ctx context.Context, channel notification.Channel) (policy notification.DeliveryPolicy, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_delivery_policy_by_channel", startedAt, err)
	}()

	const query = `
		SELECT policy_id, channel, max_attempts, backoff_seconds, enabled, created_at, updated_at
		FROM delivery_policies
		WHERE channel = $1 AND enabled = TRUE
		LIMIT 1
	`

	err = s.db.QueryRowContext(ctx, query, channel).Scan(
		&policy.PolicyID,
		&policy.Channel,
		&policy.MaxAttempts,
		&policy.BackoffSeconds,
		&policy.Enabled,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.DeliveryPolicy{}, ErrNotFound
	}
	if err != nil {
		return notification.DeliveryPolicy{}, fmt.Errorf("query delivery policy: %w", err)
	}
	return policy, nil
}

func (s *Store) UpsertWebhookSubscription(ctx context.Context, subscription notification.WebhookSubscription) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("upsert_webhook_subscription", startedAt, err)
	}()

	const query = `
		INSERT INTO webhook_subscriptions (
			subscription_id, target_url, enabled
		) VALUES ($1, $2, $3)
		ON CONFLICT (target_url)
		DO UPDATE SET
			enabled = EXCLUDED.enabled,
			updated_at = NOW()
	`

	_, err = s.db.ExecContext(ctx, query,
		subscription.SubscriptionID,
		subscription.TargetURL,
		subscription.Enabled,
	)
	if err != nil {
		return fmt.Errorf("upsert webhook subscription: %w", err)
	}
	return nil
}

func (s *Store) ListWebhookSubscriptions(ctx context.Context) (subscriptions []notification.WebhookSubscription, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_webhook_subscriptions", startedAt, err)
	}()

	const query = `
		SELECT subscription_id, target_url, enabled, created_at, updated_at
		FROM webhook_subscriptions
		ORDER BY created_at ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query webhook subscriptions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var subscription notification.WebhookSubscription
		if err := rows.Scan(
			&subscription.SubscriptionID,
			&subscription.TargetURL,
			&subscription.Enabled,
			&subscription.CreatedAt,
			&subscription.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan webhook subscription: %w", err)
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook subscriptions: %w", err)
	}
	return subscriptions, nil
}

func (s *Store) GetWebhookSubscriptionByID(ctx context.Context, subscriptionID string) (subscription notification.WebhookSubscription, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_webhook_subscription_by_id", startedAt, err)
	}()

	const query = `
		SELECT subscription_id, target_url, enabled, created_at, updated_at
		FROM webhook_subscriptions
		WHERE subscription_id = $1
		LIMIT 1
	`

	err = s.db.QueryRowContext(ctx, query, subscriptionID).Scan(
		&subscription.SubscriptionID,
		&subscription.TargetURL,
		&subscription.Enabled,
		&subscription.CreatedAt,
		&subscription.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.WebhookSubscription{}, ErrNotFound
	}
	if err != nil {
		return notification.WebhookSubscription{}, fmt.Errorf("query webhook subscription: %w", err)
	}
	return subscription, nil
}

func (s *Store) ListEnabledWebhookSubscriptions(ctx context.Context) ([]notification.WebhookSubscription, error) {
	const query = `
		SELECT subscription_id, target_url, enabled, created_at, updated_at
		FROM webhook_subscriptions
		WHERE enabled = TRUE
		ORDER BY created_at ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query enabled webhook subscriptions: %w", err)
	}
	defer rows.Close()

	var subscriptions []notification.WebhookSubscription
	for rows.Next() {
		var subscription notification.WebhookSubscription
		if err := rows.Scan(
			&subscription.SubscriptionID,
			&subscription.TargetURL,
			&subscription.Enabled,
			&subscription.CreatedAt,
			&subscription.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan enabled webhook subscription: %w", err)
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate enabled webhook subscriptions: %w", err)
	}
	return subscriptions, nil
}

func (s *Store) CreateWebhookDeliveryAttempt(ctx context.Context, attempt notification.WebhookDeliveryAttempt) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("create_webhook_delivery_attempt", startedAt, err)
	}()

	const query = `
		INSERT INTO webhook_delivery_attempts (
			delivery_id, request_id, subscription_id, event_type, target_url, attempt_number, max_attempts,
			status, http_status_code, error_message, response_body
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`

	_, err = s.db.ExecContext(ctx, query,
		attempt.DeliveryID,
		attempt.RequestID,
		attempt.SubscriptionID,
		attempt.EventType,
		attempt.TargetURL,
		attempt.AttemptNumber,
		attempt.MaxAttempts,
		attempt.Status,
		attempt.HTTPStatusCode,
		attempt.ErrorMessage,
		attempt.ResponseBody,
	)
	if err != nil {
		return fmt.Errorf("insert webhook delivery attempt: %w", err)
	}
	return nil
}

func (s *Store) UpdateWebhookDeliveryAttempt(ctx context.Context, attempt notification.WebhookDeliveryAttempt) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("update_webhook_delivery_attempt", startedAt, err)
	}()

	const query = `
		UPDATE webhook_delivery_attempts
		SET status = $2, http_status_code = $3, error_message = $4, response_body = $5, updated_at = NOW()
		WHERE delivery_id = $1
	`

	result, err := s.db.ExecContext(ctx, query,
		attempt.DeliveryID,
		attempt.Status,
		attempt.HTTPStatusCode,
		attempt.ErrorMessage,
		attempt.ResponseBody,
	)
	if err != nil {
		return fmt.Errorf("update webhook delivery attempt: %w", err)
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

func (s *Store) ListWebhookDeliveryAttempts(ctx context.Context, requestID string) (attempts []notification.WebhookDeliveryAttempt, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_webhook_delivery_attempts", startedAt, err)
	}()

	const query = `
		SELECT delivery_id, request_id, subscription_id, event_type, target_url, attempt_number, max_attempts,
		       status, http_status_code, error_message, response_body, created_at, updated_at
		FROM webhook_delivery_attempts
		WHERE request_id = $1
		ORDER BY created_at ASC, attempt_number ASC
	`

	rows, err := s.db.QueryContext(ctx, query, requestID)
	if err != nil {
		return nil, fmt.Errorf("query webhook delivery attempts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var attempt notification.WebhookDeliveryAttempt
		if err := rows.Scan(
			&attempt.DeliveryID,
			&attempt.RequestID,
			&attempt.SubscriptionID,
			&attempt.EventType,
			&attempt.TargetURL,
			&attempt.AttemptNumber,
			&attempt.MaxAttempts,
			&attempt.Status,
			&attempt.HTTPStatusCode,
			&attempt.ErrorMessage,
			&attempt.ResponseBody,
			&attempt.CreatedAt,
			&attempt.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan webhook delivery attempt: %w", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook delivery attempts: %w", err)
	}
	return attempts, nil
}

func (s *Store) CountDeliveryAttemptsByRequestAndChannel(ctx context.Context, requestID string, channel notification.Channel) (count int, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("count_delivery_attempts_by_request_and_channel", startedAt, err)
	}()

	const query = `
		SELECT COUNT(*)
		FROM delivery_attempts
		WHERE request_id = $1 AND channel = $2
	`
	if err = s.db.QueryRowContext(ctx, query, requestID, channel).Scan(&count); err != nil {
		return 0, fmt.Errorf("count delivery attempts by request and channel: %w", err)
	}
	return count, nil
}

func (s *Store) CreateScheduledRetry(ctx context.Context, retry notification.ScheduledRetry) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("create_scheduled_retry", startedAt, err)
	}()

	const query = `
		INSERT INTO scheduled_retries (
			retry_id, request_id, channel, binding_set, available_at, claimed_at, last_error, triggered_by_attempt_number
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = s.db.ExecContext(ctx, query,
		retry.RetryID,
		retry.RequestID,
		retry.Channel,
		retry.BindingSet,
		retry.AvailableAt,
		retry.ClaimedAt,
		retry.LastError,
		retry.TriggeredByAttemptNumber,
	)
	if err != nil {
		return fmt.Errorf("insert scheduled retry: %w", err)
	}
	return nil
}

func (s *Store) ListScheduledRetries(ctx context.Context, requestID string) (retries []notification.ScheduledRetry, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_scheduled_retries", startedAt, err)
	}()

	query := `
		SELECT retry_id, request_id, channel, binding_set, available_at, claimed_at, last_error,
		       triggered_by_attempt_number, created_at, updated_at
		FROM scheduled_retries
	`
	args := []any{}
	if requestID != "" {
		query += ` WHERE request_id = $1`
		args = append(args, requestID)
	}
	query += ` ORDER BY available_at ASC, created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query scheduled retries: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			retry     notification.ScheduledRetry
			claimedAt sql.NullTime
		)
		if err := rows.Scan(
			&retry.RetryID,
			&retry.RequestID,
			&retry.Channel,
			&retry.BindingSet,
			&retry.AvailableAt,
			&claimedAt,
			&retry.LastError,
			&retry.TriggeredByAttemptNumber,
			&retry.CreatedAt,
			&retry.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan scheduled retry: %w", err)
		}
		if claimedAt.Valid {
			retry.ClaimedAt = &claimedAt.Time
		}
		retries = append(retries, retry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled retries: %w", err)
	}
	return retries, nil
}

func (s *Store) ClaimNextScheduledRetry(ctx context.Context) (retry notification.ScheduledRetry, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("claim_next_scheduled_retry", startedAt, err)
	}()

	const query = `
		WITH candidate AS (
			SELECT retry_id
			FROM scheduled_retries
			WHERE available_at <= NOW()
			  AND (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL '5 minutes')
			ORDER BY available_at ASC, created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE scheduled_retries sr
		SET claimed_at = NOW(), updated_at = NOW()
		FROM candidate
		WHERE sr.retry_id = candidate.retry_id
		RETURNING sr.retry_id, sr.request_id, sr.channel, sr.binding_set, sr.available_at, sr.claimed_at,
		          sr.last_error, sr.triggered_by_attempt_number, sr.created_at, sr.updated_at
	`

	var claimedAt sql.NullTime
	err = s.db.QueryRowContext(ctx, query).Scan(
		&retry.RetryID,
		&retry.RequestID,
		&retry.Channel,
		&retry.BindingSet,
		&retry.AvailableAt,
		&claimedAt,
		&retry.LastError,
		&retry.TriggeredByAttemptNumber,
		&retry.CreatedAt,
		&retry.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.ScheduledRetry{}, ErrNotFound
	}
	if err != nil {
		return notification.ScheduledRetry{}, fmt.Errorf("claim next scheduled retry: %w", err)
	}
	if claimedAt.Valid {
		retry.ClaimedAt = &claimedAt.Time
	}
	return retry, nil
}

func (s *Store) ReleaseScheduledRetryClaim(ctx context.Context, retryID string) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("release_scheduled_retry_claim", startedAt, err)
	}()

	const query = `
		UPDATE scheduled_retries
		SET claimed_at = NULL, updated_at = NOW()
		WHERE retry_id = $1
	`
	result, err := s.db.ExecContext(ctx, query, retryID)
	if err != nil {
		return fmt.Errorf("release scheduled retry claim: %w", err)
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

func (s *Store) DeleteScheduledRetry(ctx context.Context, retryID string) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("delete_scheduled_retry", startedAt, err)
	}()

	const query = `DELETE FROM scheduled_retries WHERE retry_id = $1`
	result, err := s.db.ExecContext(ctx, query, retryID)
	if err != nil {
		return fmt.Errorf("delete scheduled retry: %w", err)
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

func (s *Store) CreateDeadLetterNotification(ctx context.Context, deadLetter notification.DeadLetterNotification) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("create_dead_letter_notification", startedAt, err)
	}()

	payloadJSON, err := json.Marshal(deadLetter.PayloadSnapshot)
	if err != nil {
		return fmt.Errorf("marshal dead letter payload snapshot: %w", err)
	}

	const query = `
		INSERT INTO dead_letter_notifications (
			dead_letter_id, request_id, channel, binding_set, connector_name, reason, attempt_count,
			last_error, payload_snapshot, replay_request_id, replayed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
	`
	_, err = s.db.ExecContext(ctx, query,
		deadLetter.DeadLetterID,
		deadLetter.RequestID,
		deadLetter.Channel,
		deadLetter.BindingSet,
		deadLetter.ConnectorName,
		deadLetter.Reason,
		deadLetter.AttemptCount,
		deadLetter.LastError,
		string(payloadJSON),
		deadLetter.ReplayRequestID,
		deadLetter.ReplayedAt,
	)
	if err != nil {
		return fmt.Errorf("insert dead letter notification: %w", err)
	}
	return nil
}

func (s *Store) ListDeadLetterNotifications(ctx context.Context, requestID string) (deadLetters []notification.DeadLetterNotification, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_dead_letter_notifications", startedAt, err)
	}()

	query := `
		SELECT dead_letter_id, request_id, channel, binding_set, connector_name, reason, attempt_count,
		       last_error, payload_snapshot, replay_request_id, replayed_at, created_at, updated_at
		FROM dead_letter_notifications
	`
	args := []any{}
	if requestID != "" {
		query += ` WHERE request_id = $1`
		args = append(args, requestID)
	}
	query += ` ORDER BY created_at DESC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query dead letter notifications: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			deadLetter      notification.DeadLetterNotification
			payloadSnapshot []byte
			replayedAt      sql.NullTime
		)
		if err := rows.Scan(
			&deadLetter.DeadLetterID,
			&deadLetter.RequestID,
			&deadLetter.Channel,
			&deadLetter.BindingSet,
			&deadLetter.ConnectorName,
			&deadLetter.Reason,
			&deadLetter.AttemptCount,
			&deadLetter.LastError,
			&payloadSnapshot,
			&deadLetter.ReplayRequestID,
			&replayedAt,
			&deadLetter.CreatedAt,
			&deadLetter.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan dead letter notification: %w", err)
		}
		if len(payloadSnapshot) > 0 {
			if err := json.Unmarshal(payloadSnapshot, &deadLetter.PayloadSnapshot); err != nil {
				return nil, fmt.Errorf("unmarshal dead letter payload snapshot: %w", err)
			}
		}
		if replayedAt.Valid {
			deadLetter.ReplayedAt = &replayedAt.Time
		}
		deadLetters = append(deadLetters, deadLetter)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letter notifications: %w", err)
	}
	return deadLetters, nil
}

func (s *Store) GetDeadLetterNotificationByID(ctx context.Context, deadLetterID string) (deadLetter notification.DeadLetterNotification, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_dead_letter_notification_by_id", startedAt, err)
	}()

	const query = `
		SELECT dead_letter_id, request_id, channel, binding_set, connector_name, reason, attempt_count,
		       last_error, payload_snapshot, replay_request_id, replayed_at, created_at, updated_at
		FROM dead_letter_notifications
		WHERE dead_letter_id = $1
		LIMIT 1
	`
	var (
		payloadSnapshot []byte
		replayedAt      sql.NullTime
	)
	err = s.db.QueryRowContext(ctx, query, deadLetterID).Scan(
		&deadLetter.DeadLetterID,
		&deadLetter.RequestID,
		&deadLetter.Channel,
		&deadLetter.BindingSet,
		&deadLetter.ConnectorName,
		&deadLetter.Reason,
		&deadLetter.AttemptCount,
		&deadLetter.LastError,
		&payloadSnapshot,
		&deadLetter.ReplayRequestID,
		&replayedAt,
		&deadLetter.CreatedAt,
		&deadLetter.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.DeadLetterNotification{}, ErrNotFound
	}
	if err != nil {
		return notification.DeadLetterNotification{}, fmt.Errorf("query dead letter notification: %w", err)
	}
	if len(payloadSnapshot) > 0 {
		if err := json.Unmarshal(payloadSnapshot, &deadLetter.PayloadSnapshot); err != nil {
			return notification.DeadLetterNotification{}, fmt.Errorf("unmarshal dead letter payload snapshot: %w", err)
		}
	}
	if replayedAt.Valid {
		deadLetter.ReplayedAt = &replayedAt.Time
	}
	return deadLetter, nil
}

func (s *Store) MarkDeadLetterReplayed(ctx context.Context, deadLetterID string, replayRequestID string, replayedAt time.Time) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("mark_dead_letter_replayed", startedAt, err)
	}()

	const query = `
		UPDATE dead_letter_notifications
		SET replay_request_id = $2, replayed_at = $3, updated_at = NOW()
		WHERE dead_letter_id = $1
	`
	result, err := s.db.ExecContext(ctx, query, deadLetterID, replayRequestID, replayedAt)
	if err != nil {
		return fmt.Errorf("mark dead letter replayed: %w", err)
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
