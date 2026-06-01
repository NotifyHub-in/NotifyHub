ALTER TABLE notification_requests
    ADD COLUMN IF NOT EXISTS language_code TEXT NOT NULL DEFAULT 'en';

ALTER TABLE templates
    ADD COLUMN IF NOT EXISTS language_code TEXT NOT NULL DEFAULT 'en';

UPDATE templates
SET language_code = 'en'
WHERE language_code IS NULL OR language_code = '';

ALTER TABLE templates
    DROP CONSTRAINT IF EXISTS templates_template_key_channel_key;

ALTER TABLE templates
    ADD CONSTRAINT templates_template_key_channel_language_code_key UNIQUE (template_key, channel, language_code);
