CREATE KEYSPACE test with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE test.feature_store(
    entities text,
    feature text,
    value blob,
    PRIMARY KEY (entities, feature)
) WITH CLUSTERING ORDER BY (feature DESC);