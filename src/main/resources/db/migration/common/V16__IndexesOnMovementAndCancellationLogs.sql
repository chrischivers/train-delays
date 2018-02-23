CREATE INDEX movement_log_lookup_ix ON movement_log (schedule_train_id, origin_departure_timestamp);
CREATE INDEX cancellation_log_lookup_ix ON cancellation_log (schedule_train_id, origin_departure_timestamp);

ALTER TABLE movement_log
ADD COLUMN origin_departure_date VARCHAR(50) NOT NULL,
ADD COLUMN origin_departure_time VARCHAR(50) NOT NULL,
ADD COLUMN planned_passenger_time VARCHAR(50) NOT NULL,
ADD COLUMN actual_time VARCHAR(50) NOT NULL;

ALTER TABLE cancellation_log
ADD COLUMN origin_departure_date VARCHAR(50) NOT NULL,
ADD COLUMN origin_departure_time VARCHAR(50) NOT NULL;