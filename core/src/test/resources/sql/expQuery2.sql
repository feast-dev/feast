SELECT
    id,
    event_timestamp,
    feature1
FROM
    `project.dataset.myentity`
WHERE
    event_timestamp >= TIMESTAMP("2018-01-02")
    AND event_timestamp <= TIMESTAMP(DATETIME_ADD("2018-01-30", INTERVAL 1 DAY)) LIMIT 1000