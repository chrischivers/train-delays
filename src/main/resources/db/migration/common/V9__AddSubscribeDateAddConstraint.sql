ALTER TABLE subscribers
ADD COLUMN subscribe_timestamp TIMESTAMP NOT NULL;

ALTER TABLE subscribers
ADD CONSTRAINT constraint_subscribers UNIQUE (user_id, schedule_train_id, service_code, stanox_code);