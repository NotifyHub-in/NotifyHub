CREATE TABLE IF NOT EXISTS callback_routes (
    route_id TEXT PRIMARY KEY,
    provider_key TEXT NOT NULL UNIQUE,
    provider_account_id TEXT NOT NULL REFERENCES provider_accounts(provider_account_id) ON DELETE CASCADE,
    callback_path TEXT NOT NULL,
    verification_mode TEXT NOT NULL DEFAULT 'none',
    verification_secret_ref TEXT NOT NULL DEFAULT '',
    verification_secret_material_type TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS callback_routes_provider_account_id_idx
    ON callback_routes(provider_account_id, enabled);
