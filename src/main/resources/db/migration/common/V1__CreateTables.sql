CREATE TABLE IF NOT EXISTS schedule (
  train_id     VARCHAR(10)    NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
  atoc_code VARCHAR(10) NOT NULL,
  stop_sequence SMALLINT NOT NULL,
  tiploc_code VARCHAR(10) NOT NULL,
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
  PRIMARY KEY (train_id, service_code, tiploc_code, stop_sequence, schedule_start, schedule_end)
);

CREATE TABLE IF NOT EXISTS tiploc (
  tiploc_code VARCHAR(10) NOT NULL,
  stanox VARCHAR(10) NOT NULL,
  description VARCHAR(50) NULL,
  PRIMARY KEY (tiploc_code)
);

CREATE TABLE IF NOT EXISTS movement_log (
  id SERIAL PRIMARY KEY,
  train_id     VARCHAR(10)    NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
  event_type VARCHAR(15) NOT NULL,
  toc VARCHAR(10) NOT NULL,
  stanox VARCHAR(10) NOT NULL,
  planned_passenger_timestamp BIGINT NOT NULL,
  actual_timestamp BIGINT NOT NULL,
  difference BIGINT NOT NULL,
  variation_status VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS subscribers (
  id SERIAL PRIMARY KEY,
  user_id VARCHAR NOT NULL,
  email VARCHAR NOT NULL,
  train_id     VARCHAR(10)    NOT NULL,
  service_code   VARCHAR(10)   NOT NULL,
  stanox VARCHAR(10) NOT NULL
);
