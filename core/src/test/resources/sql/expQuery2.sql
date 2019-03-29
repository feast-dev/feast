SELECT
    project.dataset.myentity.id,
    project.dataset.myentity.event_timestamp ,
    myentity_feature1
FROM
    project.dataset.myentity
WHERE
    event_timestamp >= TIMESTAMP("2018-01-02")
    AND event_timestamp <= TIMESTAMP(DATETIME_ADD("2018-01-30", INTERVAL 1 DAY)) LIMIT 1000