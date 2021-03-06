CREATE TABLE IF NOT EXISTS schedule (
  id SERIAL PRIMARY KEY,
  schedule_train_id     VARCHAR(10)    NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
  atoc_code VARCHAR(10) NOT NULL,
  stop_sequence SMALLINT NOT NULL,
  stanox_code VARCHAR(10) NOT NULL,
  subsequent_stanox_codes VARCHAR(50)[] NOT NULL,
  subsequent_arrival_times VARCHAR(50)[] NOT NULL,
  monday   BOOLEAN NOT NULL,
  tuesday   BOOLEAN NOT NULL,
  wednesday   BOOLEAN NOT NULL,
  thursday   BOOLEAN NOT NULL,
  friday   BOOLEAN NOT NULL,
  saturday   BOOLEAN NOT NULL,
  sunday   BOOLEAN NOT NULL,
  schedule_start DATE NOT NULL,
  schedule_end DATE NOT NULL,
  location_type VARCHAR(5) NOT NULL,
  arrival_time TIME NULL,
  departure_time TIME NULL,
  CONSTRAINT unique_cons UNIQUE(schedule_train_id, service_code, stanox_code, stop_sequence, schedule_start, schedule_end)
);

CREATE TABLE IF NOT EXISTS stanox (
  stanox_code VARCHAR(10) NOT NULL,
  tiploc_code VARCHAR(10) NOT NULL,
  crs VARCHAR(10) NULL,
  description VARCHAR(50) NULL,
  PRIMARY KEY (stanox_code, tipLoc_code)
);

CREATE TABLE IF NOT EXISTS movement_log (
  id SERIAL PRIMARY KEY,
  train_id     VARCHAR(10)    NOT NULL,
  schedule_train_id VARCHAR(10) NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
  event_type VARCHAR(15) NOT NULL,
  toc VARCHAR(10) NOT NULL,
  stanox_code VARCHAR(10) NOT NULL,
  planned_passenger_timestamp BIGINT NOT NULL,
  actual_timestamp BIGINT NOT NULL,
  difference BIGINT NOT NULL,
  variation_status VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS cancellation_log (
  id SERIAL PRIMARY KEY,
  train_id     VARCHAR(10)    NOT NULL,
  schedule_train_id VARCHAR(10) NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
  toc VARCHAR(10) NOT NULL,
  stanox_code VARCHAR(10) NOT NULL,
  cancellation_type VARCHAR(10) NOT NULL,
  cancellation_reason_code VARCHAR(10) NULL
);

CREATE TABLE IF NOT EXISTS subscribers (
  id SERIAL PRIMARY KEY,
  user_id VARCHAR NOT NULL,
  email VARCHAR NOT NULL,
  schedule_train_id     VARCHAR(10)    NOT NULL,
  service_code   VARCHAR(10)   NOT NULL,
  stanox_code VARCHAR(10) NOT NULL
);
