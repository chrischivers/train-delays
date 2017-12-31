CREATE TABLE IF NOT EXISTS schedule (
  train_id     VARCHAR(10)    NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
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
  tiploc_code VARCHAR(10) NOT NULL,
  arrival_time TIME NULL,
  departure_time TIME NULL,
  PRIMARY KEY (train_id, service_code, tiploc_code)
);

CREATE TABLE IF NOT EXISTS tiploc (
  tiploc_code VARCHAR(10) NOT NULL,
  stanox VARCHAR(10) NOT NULL,
  description VARCHAR(50) NULL,
  PRIMARY KEY (tiploc_code)
);
