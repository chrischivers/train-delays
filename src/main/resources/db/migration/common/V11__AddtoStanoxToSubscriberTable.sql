ALTER TABLE subscribers
ADD COLUMN to_stanox_code VARCHAR(10) NOT NULL;

ALTER TABLE subscribers
RENAME COLUMN stanox_code TO from_stanox_code;

ALTER TABLE subscribers
DROP CONSTRAINT  constraint_subscribers;

ALTER TABLE subscribers
ADD CONSTRAINT constraint_subscribers UNIQUE (user_id, schedule_train_id, service_code, from_stanox_code, to_stanox_code);