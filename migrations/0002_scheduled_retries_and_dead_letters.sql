CREATE TABLE IF NOT EXISTS scheduled_retries (
    retry_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES notification_requests(request_id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    binding_set TEXT NOT NULL DEFAULT '',
    available_at TIMESTAMPTZ NOT NULL,
    claimed_at TIMESTAMPTZ NULL,
    last_error TEXT NOT NULL DEFAULT '',
    triggered_by_attempt_number INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scheduled_retries_request_id_idx ON scheduled_retries(request_id);
CREATE INDEX IF NOT EXISTS scheduled_retries_available_at_idx ON scheduled_retries(available_at);

CREATE TABLE IF NOT EXISTS dead_letter_notifications (
    dead_letter_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES notification_requests(request_id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    binding_set TEXT NOT NULL DEFAULT '',
    connector_name TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    payload_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    replay_request_id TEXT NOT NULL DEFAULT '',
    replayed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS dead_letter_notifications_request_id_idx ON dead_letter_notifications(request_id);
