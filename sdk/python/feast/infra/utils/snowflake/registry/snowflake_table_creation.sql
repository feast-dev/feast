CREATE TABLE IF NOT EXISTS REGISTRY_PATH."PROJECTS" (
  project_id VARCHAR,
  project_name VARCHAR NOT NULL,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  project_proto BINARY NOT NULL,
  PRIMARY KEY (project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."DATA_SOURCES" (
  data_source_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  data_source_proto BINARY NOT NULL,
  PRIMARY KEY (data_source_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."ENTITIES" (
  entity_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  entity_proto BINARY NOT NULL,
  PRIMARY KEY (entity_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."FEAST_METADATA" (
  project_id VARCHAR,
  metadata_key VARCHAR,
  metadata_value VARCHAR NOT NULL,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  PRIMARY KEY (project_id, metadata_key)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."FEATURE_SERVICES" (
  feature_service_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  feature_service_proto BINARY NOT NULL,
  PRIMARY KEY (feature_service_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."FEATURE_VIEWS" (
  feature_view_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  feature_view_proto BINARY NOT NULL,
  materialized_intervals BINARY,
  user_metadata BINARY,
  PRIMARY KEY (feature_view_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."MANAGED_INFRA" (
  infra_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  infra_proto BINARY NOT NULL,
  PRIMARY KEY (infra_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."ON_DEMAND_FEATURE_VIEWS" (
  on_demand_feature_view_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  on_demand_feature_view_proto BINARY NOT NULL,
  user_metadata BINARY,
  PRIMARY KEY (on_demand_feature_view_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."SAVED_DATASETS" (
  saved_dataset_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  saved_dataset_proto BINARY NOT NULL,
  PRIMARY KEY (saved_dataset_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."STREAM_FEATURE_VIEWS" (
  stream_feature_view_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  stream_feature_view_proto BINARY NOT NULL,
  user_metadata BINARY,
  PRIMARY KEY (stream_feature_view_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."VALIDATION_REFERENCES" (
  validation_reference_name VARCHAR,
  project_id VARCHAR,
  last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
  validation_reference_proto BINARY NOT NULL,
  PRIMARY KEY (validation_reference_name, project_id)
);

CREATE TABLE IF NOT EXISTS REGISTRY_PATH."PERMISSIONS" (
    permission_name VARCHAR,
    project_id VARCHAR,
    last_updated_timestamp TIMESTAMP_LTZ NOT NULL,
    permission_proto BINARY NOT NULL,
    PRIMARY KEY (permission_name, project_id)
);
