WITH raw_project_dataset_myentity_second AS (SELECT
    ROW_NUMBER() OVER (PARTITION
BY
    id,
    event_timestamp
ORDER BY
    created_timestamp DESC) rownum,
    id,
    event_timestamp,
    created_timestamp ,
    FIRST_VALUE(feature1 IGNORE NULLS) OVER w AS myentity_second_feature1
FROM
    `project.dataset.myentity_second`
WHERE
    event_timestamp >= TIMESTAMP("2018-01-02")
    AND event_timestamp <= TIMESTAMP(DATETIME_ADD("2018-01-30", INTERVAL 1 DAY)) WINDOW w AS ( PARTITION
BY
    id,
    event_timestamp
ORDER BY
    event_timestamp DESC )), project_dataset_myentity_second AS ( SELECT
    *
FROM
    raw_project_dataset_myentity_second
WHERE
    rownum = 1 ) , raw_project_dataset_myentity_minute AS (SELECT
    ROW_NUMBER() OVER (PARTITION
BY
    id,
    event_timestamp
ORDER BY
    created_timestamp DESC) rownum,
    id,
    event_timestamp,
    created_timestamp ,
    FIRST_VALUE(feature1 IGNORE NULLS) OVER w AS myentity_minute_feature1
FROM
    `project.dataset.myentity_minute`
WHERE
    event_timestamp >= TIMESTAMP("2018-01-02")
    AND event_timestamp <= TIMESTAMP(DATETIME_ADD("2018-01-30", INTERVAL 1 DAY)) WINDOW w AS ( PARTITION
BY
    id,
    event_timestamp
ORDER BY
    event_timestamp DESC )), project_dataset_myentity_minute AS ( SELECT
    *
FROM
    raw_project_dataset_myentity_minute
WHERE
    rownum = 1 ) , raw_project_dataset_myentity_hour AS (SELECT
    ROW_NUMBER() OVER (PARTITION
BY
    id,
    event_timestamp
ORDER BY
    created_timestamp DESC) rownum,
    id,
    event_timestamp,
    created_timestamp ,
    FIRST_VALUE(feature1 IGNORE NULLS) OVER w AS myentity_hour_feature1
FROM
    `project.dataset.myentity_hour`
WHERE
    event_timestamp >= TIMESTAMP("2018-01-02")
    AND event_timestamp <= TIMESTAMP(DATETIME_ADD("2018-01-30", INTERVAL 1 DAY)) WINDOW w AS ( PARTITION
BY
    id,
    event_timestamp
ORDER BY
    event_timestamp DESC )), project_dataset_myentity_hour AS ( SELECT
    *
FROM
    raw_project_dataset_myentity_hour
WHERE
    rownum = 1 ) , raw_project_dataset_myentity_day AS (SELECT
    ROW_NUMBER() OVER (PARTITION
BY
    id,
    event_timestamp
ORDER BY
    created_timestamp DESC) rownum,
    id,
    event_timestamp,
    created_timestamp ,
    FIRST_VALUE(feature1 IGNORE NULLS) OVER w AS myentity_day_feature1
FROM
    `project.dataset.myentity_day`
WHERE
    event_timestamp >= TIMESTAMP("2018-01-02")
    AND event_timestamp <= TIMESTAMP(DATETIME_ADD("2018-01-30", INTERVAL 1 DAY)) WINDOW w AS ( PARTITION
BY
    id,
    event_timestamp
ORDER BY
    event_timestamp DESC )), project_dataset_myentity_day AS ( SELECT
    *
FROM
    raw_project_dataset_myentity_day
WHERE
    rownum = 1 ) , raw_project_dataset_myentity_none AS (SELECT
    ROW_NUMBER() OVER (PARTITION
BY
    id,
    event_timestamp
ORDER BY
    created_timestamp DESC) rownum,
    id,
    event_timestamp,
    created_timestamp ,
    FIRST_VALUE(feature1 IGNORE NULLS) OVER w AS myentity_none_feature1
FROM
    `project.dataset.myentity_none` WINDOW w AS ( PARTITION
BY
    id,
    event_timestamp
ORDER BY
    event_timestamp DESC )), project_dataset_myentity_none AS ( SELECT
    *
FROM
    raw_project_dataset_myentity_none
WHERE
    rownum = 1 ) SELECT
    project_dataset_myentity_second.id,
    project_dataset_myentity_second.event_timestamp ,
    project_dataset_myentity_second.myentity_second_feature1,
    project_dataset_myentity_minute.myentity_minute_feature1,
    project_dataset_myentity_hour.myentity_hour_feature1,
    project_dataset_myentity_day.myentity_day_feature1,
    project_dataset_myentity_none.myentity_none_feature1
FROM
    project_dataset_myentity_second LEFT
JOIN
    project_dataset_myentity_minute
        ON project_dataset_myentity_second.id = project_dataset_myentity_minute.id
        AND TIMESTAMP_TRUNC(project_dataset_myentity_second.event_timestamp,
    MINUTE) = project_dataset_myentity_minute.event_timestamp LEFT
JOIN
    project_dataset_myentity_hour
        ON project_dataset_myentity_second.id = project_dataset_myentity_hour.id
        AND TIMESTAMP_TRUNC(project_dataset_myentity_second.event_timestamp,
    HOUR) = project_dataset_myentity_hour.event_timestamp LEFT
JOIN
    project_dataset_myentity_day
        ON project_dataset_myentity_second.id = project_dataset_myentity_day.id
        AND TIMESTAMP_TRUNC(project_dataset_myentity_second.event_timestamp,
    DAY) = project_dataset_myentity_day.event_timestamp LEFT
JOIN
    project_dataset_myentity_none
        ON project_dataset_myentity_second.id = project_dataset_myentity_none.id LIMIT 1000
