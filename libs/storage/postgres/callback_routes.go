package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
)

func (s *Store) UpsertCallbackRoute(ctx context.Context, route notification.CallbackRoute) (err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("upsert_callback_route", startedAt, err)
	}()

	if err := notification.ValidateCallbackRoute(route); err != nil {
		return err
	}

	const query = `
		INSERT INTO callback_routes (
			route_id, provider_key, provider_account_id, callback_path, verification_mode,
			verification_secret_ref, verification_secret_material_type, enabled
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (provider_key)
		DO UPDATE SET
			provider_account_id = EXCLUDED.provider_account_id,
			callback_path = EXCLUDED.callback_path,
			verification_mode = EXCLUDED.verification_mode,
			verification_secret_ref = EXCLUDED.verification_secret_ref,
			verification_secret_material_type = EXCLUDED.verification_secret_material_type,
			enabled = EXCLUDED.enabled,
			updated_at = NOW()
	`

	_, err = s.db.ExecContext(ctx, query,
		route.RouteID,
		route.ProviderKey,
		route.ProviderAccountID,
		route.CallbackPath,
		route.VerificationMode,
		route.VerificationSecretRef.Ref,
		route.VerificationSecretRef.MaterialType,
		route.Enabled,
	)
	if err != nil {
		return fmt.Errorf("upsert callback route: %w", err)
	}
	return nil
}

func (s *Store) GetCallbackRouteByProviderKey(ctx context.Context, providerKey string) (route notification.CallbackRoute, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("get_callback_route_by_provider_key", startedAt, err)
	}()

	const query = `
		SELECT route_id, provider_key, provider_account_id, callback_path, verification_mode,
		       verification_secret_ref, verification_secret_material_type, enabled, created_at, updated_at
		FROM callback_routes
		WHERE provider_key = $1
		LIMIT 1
	`

	err = s.db.QueryRowContext(ctx, query, providerKey).Scan(
		&route.RouteID,
		&route.ProviderKey,
		&route.ProviderAccountID,
		&route.CallbackPath,
		&route.VerificationMode,
		&route.VerificationSecretRef.Ref,
		&route.VerificationSecretRef.MaterialType,
		&route.Enabled,
		&route.CreatedAt,
		&route.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return notification.CallbackRoute{}, ErrNotFound
	}
	if err != nil {
		return notification.CallbackRoute{}, fmt.Errorf("query callback route: %w", err)
	}
	return route, nil
}

func (s *Store) ListCallbackRoutes(ctx context.Context) (routes []notification.CallbackRoute, err error) {
	startedAt := time.Now()
	defer func() {
		observeDBOperation("list_callback_routes", startedAt, err)
	}()

	const query = `
		SELECT route_id, provider_key, provider_account_id, callback_path, verification_mode,
		       verification_secret_ref, verification_secret_material_type, enabled, created_at, updated_at
		FROM callback_routes
		ORDER BY provider_key ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query callback routes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var route notification.CallbackRoute
		if err := rows.Scan(
			&route.RouteID,
			&route.ProviderKey,
			&route.ProviderAccountID,
			&route.CallbackPath,
			&route.VerificationMode,
			&route.VerificationSecretRef.Ref,
			&route.VerificationSecretRef.MaterialType,
			&route.Enabled,
			&route.CreatedAt,
			&route.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan callback route: %w", err)
		}
		routes = append(routes, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate callback routes: %w", err)
	}
	return routes, nil
}
