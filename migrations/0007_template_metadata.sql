ALTER TABLE templates
    ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE templates
SET metadata = '{}'::jsonb
WHERE metadata IS NULL;
