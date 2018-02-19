ALTER TABLE movement_log
ALTER COLUMN train_id TYPE VARCHAR(20);
ALTER TABLE movement_log
ALTER COLUMN service_code TYPE VARCHAR(20);

ALTER TABLE cancellation_log
ALTER COLUMN train_id TYPE VARCHAR(20);
ALTER TABLE cancellation_log
ALTER COLUMN service_code TYPE VARCHAR(20);