ALTER TABLE jobs_feature_sets ADD CONSTRAINT jobs_feature_sets_pkey PRIMARY KEY (job_id, feature_sets_id);

ALTER TABLE jobs_stores ADD CONSTRAINT jobs_stores_pkey PRIMARY KEY (job_id, store_name);