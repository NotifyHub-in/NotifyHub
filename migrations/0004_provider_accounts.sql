CREATE TABLE IF NOT EXISTS provider_accounts (
    provider_account_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    channel TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS provider_accounts_tenant_id_idx
    ON provider_accounts (tenant_id, provider_key, channel, enabled);

CREATE TABLE IF NOT EXISTS provider_secret_refs (
    secret_ref_id TEXT PRIMARY KEY,
    provider_account_id TEXT NOT NULL REFERENCES provider_accounts(provider_account_id) ON DELETE CASCADE,
    secret_name TEXT NOT NULL,
    secret_ref TEXT NOT NULL,
    material_type TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT 'current',
    source TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider_account_id, secret_name)
);

CREATE INDEX IF NOT EXISTS provider_secret_refs_provider_account_id_idx
    ON provider_secret_refs (provider_account_id);
