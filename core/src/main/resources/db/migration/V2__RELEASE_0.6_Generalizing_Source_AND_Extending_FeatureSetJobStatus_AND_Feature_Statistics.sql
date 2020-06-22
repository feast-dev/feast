--- Feast Release 0.6

--- New fields from FeatureSetJobStatus (version & deliveryStatus)

ALTER TABLE jobs_feature_sets
    ADD column version int4 default 0;

ALTER TABLE jobs_feature_sets
    ADD column delivery_status varchar(255);

ALTER TABLE feature_sets
    ADD column version int4 default 0;

UPDATE feature_sets SET version = 1;


--- FeatureStatistics Creation

CREATE TABLE feature_statistics
(
    id                      integer          NOT NULL,
    average_length          real             NOT NULL,
    avg_bytes               real             NOT NULL,
    avg_num_values          real             NOT NULL,
    count                   bigint           NOT NULL,
    dataset_id              character varying(255),
    date                    timestamp without time zone,
    feature_type            character varying(255),
    max                     double precision NOT NULL,
    max_bytes               real             NOT NULL,
    max_num_values          bigint           NOT NULL,
    mean                    double precision NOT NULL,
    median                  double precision NOT NULL,
    min                     double precision NOT NULL,
    min_bytes               real             NOT NULL,
    min_num_values          bigint           NOT NULL,
    num_missing             bigint           NOT NULL,
    num_values_histogram    bytea,
    numeric_value_histogram bytea,
    numeric_value_quantiles bytea,
    rank_histogram          bytea,
    stdev                   double precision NOT NULL,
    top_values              bytea,
    total_num_values        bigint           NOT NULL,
    n_unique                bigint,
    zeroes                  bigint           NOT NULL,
    feature_id              bigint
);


ALTER TABLE ONLY feature_statistics
    ADD CONSTRAINT feature_statistics_pkey PRIMARY KEY (id);

CREATE INDEX idx_feature_statistics_dataset_id ON public.feature_statistics USING btree (dataset_id);

CREATE INDEX idx_feature_statistics_date ON public.feature_statistics USING btree (date);

CREATE INDEX idx_feature_statistics_feature ON public.feature_statistics USING btree (feature_id);

ALTER TABLE ONLY feature_statistics
    ADD CONSTRAINT feature_statistics_feature_fkey FOREIGN KEY (feature_id) REFERENCES public.features (id);


--- Releasing previous PK in Source

ALTER TABLE feature_sets
    DROP CONSTRAINT fk2di8f74x6wir076hrfbyi1qfh;
ALTER TABLE jobs
    DROP CONSTRAINT fkhkfwvhc2gei0wqw5h4mfvsy9f;
ALTER TABLE sources
    DROP CONSTRAINT sources_pkey;
ALTER TABLE sources
    ALTER COLUMN id DROP NOT NULL;

--- Migrating to auto-incremental Source primary key

ALTER TABLE sources
    ADD column pk SERIAL PRIMARY KEY;

-- ALTER TABLE sources
--     ALTER column type Type int4 USING ('{"KAFKA": 1}'::json ->> type)::INTEGER;
ALTER TABLE sources
    ADD column config varchar(255);


--- Update all related to Source tables

ALTER TABLE feature_sets
    ADD COLUMN source_id int4;

-- foreign key on source(id) -> source(pk)

ALTER TABLE feature_sets
    ADD CONSTRAINT feature_sets_to_sources_fkey
        FOREIGN KEY (source_id) REFERENCES sources (pk);


ALTER TABLE jobs
    ADD COLUMN source_type varchar(255); -- Enum SourceType
ALTER TABLE jobs
    ADD COLUMN source_config varchar(255);



--- Migrate Data
--- creating sources with identical primary keys as in feature_sets

SELECT feature_sets.id,
       sources.bootstrap_servers,
       sources.topics,
       sources.is_default,
       sources.type
INTO TEMP TABLE converted_sources
FROM feature_sets,
     sources
WHERE feature_sets.source = sources.id;

DELETE FROM sources;

INSERT INTO sources (pk, bootstrap_servers, topics, is_default, type)
    (SELECT * FROM converted_sources);

UPDATE feature_sets
SET source_id = id;
