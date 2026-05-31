ALTER TABLE provider_bindings
    ADD COLUMN IF NOT EXISTS provider_account_id TEXT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'provider_bindings'
          AND constraint_name = 'provider_bindings_provider_account_id_fkey'
    ) THEN
        ALTER TABLE provider_bindings
            ADD CONSTRAINT provider_bindings_provider_account_id_fkey
            FOREIGN KEY (provider_account_id)
            REFERENCES provider_accounts(provider_account_id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS provider_bindings_provider_account_id_idx
    ON provider_bindings(provider_account_id);
