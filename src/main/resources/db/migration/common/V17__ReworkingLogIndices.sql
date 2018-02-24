DROP INDEX movement_log_lookup_ix;
DROP INDEX cancellation_log_lookup_ix;

CREATE INDEX movement_log_lookup_ix ON movement_log (schedule_train_id, stanox_code, origin_departure_timestamp);
CREATE INDEX cancellation_log_lookup_ix ON cancellation_log (schedule_train_id, stanox_code, origin_departure_timestamp);