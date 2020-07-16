-- Migrating to Many2Many relationship between Job and Store

CREATE TABLE jobs_stores(
    job_id character varying(255) NOT NULL,
    store_name character varying(255) NOT NULL
);

ALTER TABLE ONLY jobs_stores
    ADD CONSTRAINT jobs_stores_job_fkey FOREIGN KEY (job_id) REFERENCES jobs(id);

ALTER TABLE ONLY jobs_stores
    ADD CONSTRAINT jobs_stores_store_fkey FOREIGN KEY (store_name) REFERENCES stores(name);


CREATE INDEX idx_jobs_stores_job_id ON jobs_stores USING btree (job_id);
CREATE INDEX idx_jobs_stores_store_name ON jobs_stores USING btree (store_name);
