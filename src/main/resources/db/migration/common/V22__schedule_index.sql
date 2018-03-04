CREATE INDEX stanox_days_run_subsequent_codes_stp_indicator_idx ON schedule (stanox_code, days_run_pattern, subsequent_stanox_codes, stp_indicator);

ALTER TABLE association
  ALTER COLUMN association_category DROP NOT NULL;