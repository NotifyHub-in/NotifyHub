CREATE TABLE IF NOT EXISTS channel_events (
    event_id TEXT PRIMARY KEY,
    provider_key TEXT NOT NULL,
    provider_account_id TEXT NOT NULL DEFAULT '',
    channel TEXT NOT NULL,
    direction TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT '',
    external_message_id TEXT NOT NULL DEFAULT '',
    reply_to_message_id TEXT NOT NULL DEFAULT '',
    conversation_id TEXT NOT NULL DEFAULT '',
    from_address TEXT NOT NULL DEFAULT '',
    to_address TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    media_type TEXT NOT NULL DEFAULT '',
    media_url TEXT NOT NULL DEFAULT '',
    media_name TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS channel_events_provider_key_idx ON channel_events(provider_key, received_at DESC);
CREATE INDEX IF NOT EXISTS channel_events_channel_idx ON channel_events(channel, received_at DESC);
CREATE INDEX IF NOT EXISTS channel_events_external_message_id_idx ON channel_events(external_message_id);
