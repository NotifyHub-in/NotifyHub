CREATE TABLE IF NOT EXISTS provider_binding_health (
    binding_id TEXT PRIMARY KEY REFERENCES provider_bindings(binding_id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    binding_set TEXT NOT NULL DEFAULT '',
    connector_name TEXT NOT NULL,
    circuit_state TEXT NOT NULL DEFAULT 'closed',
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    opened_at TIMESTAMPTZ NULL,
    cooldown_until TIMESTAMPTZ NULL,
    last_failure_class TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    last_failure_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_provider_binding_health_channel
    ON provider_binding_health (channel, binding_set, circuit_state, cooldown_until);
