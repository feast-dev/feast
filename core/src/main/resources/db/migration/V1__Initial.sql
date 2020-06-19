--
-- Dump of Feast Database (as of RELEASE 0.5)
-- Baseline dump for migrating to flyway
--


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET default_tablespace = '';


CREATE TABLE entities (
    id bigint NOT NULL,
    name character varying(255),
    type character varying(255),
    feature_set_id bigint
);


CREATE TABLE feature_sets (
    id bigint NOT NULL,
    created timestamp without time zone NOT NULL,
    last_updated timestamp without time zone NOT NULL,
    labels text,
    max_age bigint,
    name character varying(255) NOT NULL,
    status character varying(255),
    project_name character varying(255),
    source character varying(255)
);


CREATE TABLE features (
    id bigint NOT NULL,
    archived boolean NOT NULL,
    bool_domain bytea,
    domain character varying(255),
    float_domain bytea,
    group_presence bytea,
    image_domain bytea,
    int_domain bytea,
    labels text,
    mid_domain bytea,
    name character varying(255),
    natural_language_domain bytea,
    presence bytea,
    shape bytea,
    string_domain bytea,
    struct_domain bytea,
    time_domain bytea,
    time_of_day_domain bytea,
    type character varying(255),
    url_domain bytea,
    value_count bytea,
    feature_set_id bigint
);



CREATE SEQUENCE hibernate_sequence
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


CREATE TABLE jobs (
    id character varying(255) NOT NULL,
    created timestamp without time zone NOT NULL,
    last_updated timestamp without time zone NOT NULL,
    ext_id character varying(255),
    runner character varying(255),
    status character varying(16),
    source_id character varying(255),
    store_name character varying(255)
);


CREATE TABLE jobs_feature_sets (
    job_id character varying(255) NOT NULL,
    feature_sets_id bigint NOT NULL
);


CREATE TABLE projects (
    name character varying(255) NOT NULL,
    archived boolean NOT NULL
);


CREATE TABLE sources (
    id character varying(255) NOT NULL,
    bootstrap_servers character varying(255),
    is_default boolean,
    topics character varying(255),
    type character varying(255) NOT NULL
);


CREATE TABLE stores (
    name character varying(255) NOT NULL,
    config oid NOT NULL,
    subscriptions character varying(255),
    type character varying(255) NOT NULL
);


ALTER TABLE ONLY entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);


ALTER TABLE ONLY feature_sets
    ADD CONSTRAINT feature_sets_pkey PRIMARY KEY (id);


ALTER TABLE ONLY features
    ADD CONSTRAINT features_pkey PRIMARY KEY (id);


ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


ALTER TABLE ONLY projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (name);


ALTER TABLE ONLY sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (id);


ALTER TABLE ONLY stores
    ADD CONSTRAINT stores_pkey PRIMARY KEY (name);


ALTER TABLE ONLY entities
    ADD CONSTRAINT uk4hredqqfh86prhp1hf08nofvk UNIQUE (name, feature_set_id);


ALTER TABLE ONLY features
    ADD CONSTRAINT ukedouxmpcoev743cmstfwq25yp UNIQUE (name, feature_set_id);


ALTER TABLE ONLY feature_sets
    ADD CONSTRAINT ukoajkc7tn9nwhodjrbcjri5jix UNIQUE (name, project_name);


CREATE INDEX idx_jobs_feature_sets_feature_sets_id ON jobs_feature_sets USING btree (feature_sets_id);

CREATE INDEX idx_jobs_feature_sets_job_id ON jobs_feature_sets USING btree (job_id);


ALTER TABLE ONLY feature_sets
    ADD CONSTRAINT fk2di8f74x6wir076hrfbyi1qfh FOREIGN KEY (source) REFERENCES sources(id);


ALTER TABLE ONLY jobs_feature_sets
    ADD CONSTRAINT fk2qt5yj45cr02spdhp59h4wpeg FOREIGN KEY (job_id) REFERENCES jobs(id);


ALTER TABLE ONLY jobs
    ADD CONSTRAINT fk3dwuno3phk8j3iwdl4cckdqqd FOREIGN KEY (store_name) REFERENCES stores(name);


ALTER TABLE ONLY features
    ADD CONSTRAINT fkfxcpsscvj0g89o4p5dx4insb1 FOREIGN KEY (feature_set_id) REFERENCES feature_sets(id);


ALTER TABLE ONLY jobs
    ADD CONSTRAINT fkhkfwvhc2gei0wqw5h4mfvsy9f FOREIGN KEY (source_id) REFERENCES sources(id);


ALTER TABLE ONLY entities
    ADD CONSTRAINT fkhyblh5sfunv00a8ums8ms9otq FOREIGN KEY (feature_set_id) REFERENCES feature_sets(id);


ALTER TABLE ONLY feature_sets
    ADD CONSTRAINT fkiiqcdeuuq9mf0tmt7jtnln3oa FOREIGN KEY (project_name) REFERENCES projects(name);


ALTER TABLE ONLY jobs_feature_sets
    ADD CONSTRAINT fkroca9etjw89c48e8jays6jl4l FOREIGN KEY (feature_sets_id) REFERENCES feature_sets(id);
