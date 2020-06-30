ALTER TABLE stores ADD COLUMN created timestamp default now();
ALTER TABLE stores ADD COLUMN last_updated timestamp default now();