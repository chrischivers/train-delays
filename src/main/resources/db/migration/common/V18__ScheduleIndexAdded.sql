DROP INDEX schedule_stanox_ix;
CREATE INDEX schedule_trainid_stanox ON schedule (schedule_train_id, stanox_code);