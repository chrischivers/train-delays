CREATE TABLE IF NOT EXISTS change_of_origin_log (
  id SERIAL PRIMARY KEY,
  train_id     VARCHAR(20)    NOT NULL,
  schedule_train_id VARCHAR(20) NOT NULL,
  service_code    VARCHAR(20)   NOT NULL,
  toc VARCHAR(20) NOT NULL,
  new_stanox_code VARCHAR(20) NOT NULL,
  origin_stanox_code VARCHAR(20) NOT NULL,
  origin_departure_timestamp BIGINT NOT NULL,
  origin_departure_date VARCHAR(50) NOT NULL,
  origin_departure_time VARCHAR(50) NOT NULL,
  reason_code VARCHAR(10) NULL
);

CREATE INDEX change_of_origin_log_lookup_ix ON change_of_origin_log (schedule_train_id, origin_stanox_code, origin_departure_timestamp);