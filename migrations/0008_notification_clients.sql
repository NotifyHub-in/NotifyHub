CREATE TABLE IF NOT EXISTS notification_clients (
    client_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    client_name TEXT NOT NULL,
    api_key_hash TEXT NOT NULL UNIQUE,
    allowed_channels JSONB NOT NULL DEFAULT '[]'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS notification_clients_tenant_id_idx
    ON notification_clients(tenant_id, enabled);

ALTER TABLE notification_requests
    ADD COLUMN IF NOT EXISTS source_client_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_tenant_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_client_name TEXT NOT NULL DEFAULT '';

