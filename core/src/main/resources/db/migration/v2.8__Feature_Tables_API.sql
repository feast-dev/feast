-- Migration to Create Tables for Feature Tables API

-- Feature Sources SQL table used to Store Feature project
CREATE TABLE feature_sources (
    id bigint NOT NULL,
    type character varying(255) NOT NULL,
    field_mapping character varying(255),
    -- Options are stored as Protobuf encoded as JSON
    options text varying(255) NOT NULL,

    CONSTRAINT feature_sources_pkey PRIMARY KEY (id)
);

-- Feature Table SQL table used to store FeatureTables protobuf
CREATE TABLE feature_tables (
    id bigint NOT NULL,
    project_name character varying(255),
    name character varying(255) NOT NULL,
    created timestamp without time zone NOT NULL,
    last_updated timestamp without time zone NOT NULL,
    labels text,
    max_age bigint,
    stream_source_id bigint,
    batch_source_id bigint,

    CONSTRAINT feature_tables_pkey PRIMARY KEY (id),
    CONSTRAINT feature_tables_project_fkey FOREIGN KEY (project_name) 
        REFERENCES projects(name),
    CONSTRAINT feature_tables_stream_feature_source_fkey FOREIGN KEY (stream_source_id) 
        REFERENCES feature_sources(id),
    CONSTRAINT feature_tables_batch_feature_source_fkey FOREIGN KEY (batch_source_id) 
        REFERENCES feature_sources(id),
    -- Feature Tables must be unique within a project
    CONSTRAINT feature_tables_unique_project UNIQUE (name, project_name)
);

-- Join table between feature tables and entities V2
CREATE TABLE feature_tables_entities_v2 (
    feature_table_id bigint NOT NULL,
    entity_v2_id bigint NOT NULL,

    CONSTRAINT feature_tables_entities_v2_pkey PRIMARY KEY (feature_table_id, entity_v2_id),
    CONSTRAINT feature_tables_entities_v2_join_feature_tables_fkey 
        FOREIGN KEY (feature_table_id) REFERENCES feature_tables(id),
    CONSTRAINT feature_tables_entities_v2_join_entities_v2_fkey 
        FOREIGN KEY (entity_v2_id) REFERENCES entities_v2 (id)
);


-- Feature v2 SQL table used to store FeatureSpecV2 protobuf
CREATE TABLE features_v2 (
    id bigint NOT NULL,
    feature_table_id bigint NOT NULL,
    name character varying(255),
    type character varying(255),

    CONSTRAINT features_v2_pkey PRIMARY KEY (id),
    CONSTRAINT features_v2_feature_table_fkey FOREIGN KEY (feature_table_id) 
        REFERENCES feature_tables(id),
    -- Features should be unique within a feature set
    CONSTRAINT feature_v2_feature_table_unique UNIQUE (name, feature_table_id)
);

