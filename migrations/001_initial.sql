CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notification_requests (
    request_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    event_name TEXT NOT NULL,
    template_key TEXT NOT NULL,
    channels JSONB NOT NULL,
    recipient JSONB NOT NULL,
    variables JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES notification_requests(request_id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    channel TEXT NOT NULL,
    connector_name TEXT NOT NULL,
    status TEXT NOT NULL,
    provider_message_id TEXT NOT NULL DEFAULT '',
    destination TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS delivery_attempts_request_id_idx ON delivery_attempts(request_id);

ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS attempt_number INTEGER NOT NULL DEFAULT 1;
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS provider_bindings (
    binding_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL UNIQUE,
    connector_name TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO provider_bindings (binding_id, channel, connector_name, endpoint_url, enabled, priority)
VALUES
    ('binding-email-default', 'email', 'connector-email', 'http://connector-email:8091', TRUE, 100),
    ('binding-sms-default', 'sms', 'connector-sms', 'http://connector-sms:8092', TRUE, 100),
    ('binding-webhook-default', 'webhook', 'connector-webhook', 'http://connector-webhook:8093', TRUE, 100)
ON CONFLICT (channel) DO NOTHING;

CREATE TABLE IF NOT EXISTS routing_policies (
    policy_id TEXT PRIMARY KEY,
    event_name TEXT NOT NULL UNIQUE,
    channels JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO routing_policies (policy_id, event_name, channels, enabled, priority)
VALUES
    ('policy-order-delayed', 'order.delayed', '["email"]'::jsonb, TRUE, 100),
    ('policy-otp-requested', 'otp.requested', '["sms"]'::jsonb, TRUE, 100),
    ('policy-payment-failed', 'payment.failed', '["webhook"]'::jsonb, TRUE, 100)
ON CONFLICT (event_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS preference_policies (
    policy_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, channel)
);

CREATE INDEX IF NOT EXISTS preference_policies_user_id_idx ON preference_policies(user_id);

CREATE TABLE IF NOT EXISTS templates (
    template_id TEXT PRIMARY KEY,
    template_key TEXT NOT NULL,
    channel TEXT NOT NULL,
    subject_template TEXT NOT NULL DEFAULT '',
    body_template TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (template_key, channel)
);

INSERT INTO templates (template_id, template_key, channel, subject_template, body_template, enabled)
VALUES
    ('template-order-delayed-email', 'order-delayed-v1', 'email', 'Order {{order_id}} delayed', 'Your order {{order_id}} is delayed. We will send an update soon.', TRUE),
    ('template-otp-requested-sms', 'otp-requested-v1', 'sms', '', 'Your OTP is {{otp}}.', TRUE),
    ('template-payment-failed-webhook', 'payment-failed-v1', 'webhook', '', '{"event":"payment.failed","payment_id":"{{payment_id}}","reason":"{{reason}}"}', TRUE)
ON CONFLICT (template_key, channel) DO NOTHING;

CREATE TABLE IF NOT EXISTS delivery_policies (
    policy_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL UNIQUE,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    backoff_seconds INTEGER NOT NULL DEFAULT 1,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO delivery_policies (policy_id, channel, max_attempts, backoff_seconds, enabled)
VALUES
    ('delivery-policy-email', 'email', 3, 1, TRUE),
    ('delivery-policy-sms', 'sms', 3, 1, TRUE),
    ('delivery-policy-webhook', 'webhook', 3, 1, TRUE)
ON CONFLICT (channel) DO NOTHING;
