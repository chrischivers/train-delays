ALTER TABLE cancellation_log
ADD COLUMN origin_stanox_code VARCHAR(10) NOT NULL;

ALTER TABLE cancellation_log
ADD COLUMN origin_departure_timestamp BIGINT NOT NULL;

ALTER TABLE movement_log
ADD COLUMN origin_stanox_code VARCHAR(10) NOT NULL;

ALTER TABLE movement_log
ADD COLUMN origin_departure_timestamp BIGINT NOT NULL;