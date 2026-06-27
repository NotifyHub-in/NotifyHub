ALTER TABLE callback_routes
    DROP CONSTRAINT IF EXISTS callback_routes_provider_key_key;

ALTER TABLE callback_routes
    ADD CONSTRAINT callback_routes_provider_key_provider_account_id_key UNIQUE (provider_key, provider_account_id);

ALTER TABLE callback_routes
    ADD CONSTRAINT callback_routes_callback_path_key UNIQUE (callback_path);

CREATE INDEX IF NOT EXISTS callback_routes_provider_key_enabled_idx
    ON callback_routes(provider_key, enabled);
