ALTER TABLE feature_tables ADD COLUMN is_deleted boolean NOT NULL;

ALTER TABLE feature_tables ADD COLUMN metadata_hash bigint NOT NULL;