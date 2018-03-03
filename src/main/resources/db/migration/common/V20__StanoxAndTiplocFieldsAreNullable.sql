ALTER TABLE stanox DROP CONSTRAINT stanox_pkey;

ALTER TABLE stanox
  ALTER COLUMN stanox_code DROP NOT NULL,
  ALTER COLUMN tiploc_code DROP NOT NULL;

ALTER TABLE stanox
  ADD COLUMN id SERIAL PRIMARY KEY;

ALTER TABLE stanox
  ADD CONSTRAINT tiploc_stanox_constraint UNIQUE (tiploc_code, stanox_code);

