ALTER TABLE schedule
ADD COLUMN stp_indicator VARCHAR(10) NOT NULL,
ADD COLUMN train_category VARCHAR(10) NULL,
ADD COLUMN train_status VARCHAR(10) NULL;

ALTER TABLE schedule
DROP CONSTRAINT  unique_cons;

ALTER TABLE schedule
ADD CONSTRAINT unique_cons UNIQUE(schedule_train_id, service_code, stanox_code, stop_sequence, schedule_start, schedule_end, stp_indicator);

ALTER TABLE schedule
  ALTER COLUMN atoc_code DROP NOT NULL;