CREATE TABLE IF NOT EXISTS association (
  id SERIAL PRIMARY KEY,
  main_schedule_train_id VARCHAR(10) NOT NULL,
  associated_schedule_train_id VARCHAR(10) NOT NULL,
  associated_start DATE NOT NULL,
  associated_end DATE NOT NULL,
  stp_indicator VARCHAR(10) NOT NULL,
  location VARCHAR(20) NOT NULL,
  monday   BOOLEAN NOT NULL,
  tuesday   BOOLEAN NOT NULL,
  wednesday   BOOLEAN NOT NULL,
  thursday   BOOLEAN NOT NULL,
  friday   BOOLEAN NOT NULL,
  saturday   BOOLEAN NOT NULL,
  sunday   BOOLEAN NOT NULL,
  days_run_pattern VARCHAR(10) NOT NULL,
  association_category VARCHAR(10) NOT NULL,
  CONSTRAINT unique_cons_association UNIQUE(main_schedule_train_id, associated_schedule_train_id, location, association_category, associated_start, associated_end)
);

CREATE INDEX association_idx ON association (main_schedule_train_id, associated_schedule_train_id, association_category, associated_start, associated_end);
