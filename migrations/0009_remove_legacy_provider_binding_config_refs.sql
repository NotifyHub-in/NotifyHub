ALTER TABLE provider_bindings
    DROP COLUMN IF EXISTS config_refs;

DELETE FROM provider_bindings
WHERE provider_account_id IS NULL;

ALTER TABLE provider_bindings
    ALTER COLUMN provider_account_id SET NOT NULL;

ALTER TABLE provider_bindings
    DROP CONSTRAINT IF EXISTS provider_bindings_provider_account_id_fkey;

ALTER TABLE provider_bindings
    ADD CONSTRAINT provider_bindings_provider_account_id_fkey
        FOREIGN KEY (provider_account_id)
        REFERENCES provider_accounts(provider_account_id)
        ON DELETE CASCADE;
