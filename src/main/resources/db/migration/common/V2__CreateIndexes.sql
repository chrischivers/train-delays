CREATE INDEX subscribers_lookup_ix ON subscribers (schedule_train_id, service_code, stanox_code);
CREATE INDEX stanox_ix ON stanox (stanox_code);