-- Migrating to Entities as a higher-level concept

CREATE TABLE entities_v2(
    id bigint NOT NULL,
    created timestamp without time zone NOT NULL,
    last_updated timestamp without time zone NOT NULL,
    name character varying(255),
    project_name character varying(255),
    type character varying(255),
    description text,
    labels text
);

ALTER TABLE ONLY entities_v2
    ADD CONSTRAINT entities_v2_pkey PRIMARY KEY (id);

ALTER TABLE ONLY entities_v2
    ADD CONSTRAINT entities_v2_project_ukey UNIQUE (name, project_name);

ALTER TABLE ONLY entities_v2
    ADD CONSTRAINT entities_v2_project_fkey FOREIGN KEY (project_name) REFERENCES projects(name);