ALTER TABLE subscribers
ADD COLUMN email_verified BOOLEAN NULL AFTER `email`;

ALTER TABLE subscribers
ADD COLUMN name VARCHAR(100) NULL AFTER `email_verified`;

ALTER TABLE subscribers
ADD COLUMN first_name VARCHAR(100) NULL AFTER `name`;

ALTER TABLE subscribers
ADD COLUMN family_name VARCHAR(100) NULL AFTER `first_name`;

ALTER TABLE subscribers
ADD COLUMN locale VARCHAR(10) NULL AFTER `family_name`;