CREATE TYPE daysRunPattern AS ENUM ('weekdays', 'saturdays', 'sundays');

ALTER TABLE schedule
ADD days_run_pattern daysRunPattern NULL;

UPDATE schedule
SET days_run_pattern = 'weekdays'
WHERE monday = true OR tuesday = true OR wednesday = true OR thursday = true OR friday = true;

UPDATE schedule
SET days_run_pattern = 'saturdays'
WHERE saturday = true AND days_run_pattern IS NULL;

UPDATE schedule
SET days_run_pattern = 'sundays'
WHERE sunday = true AND days_run_pattern IS NULL;

ALTER TABLE schedule
ALTER COLUMN days_run_pattern SET NOT NULL;