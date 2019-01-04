WITH raw_project_dataset_myentity_day AS (SELECT
    ROW_NUMBER() OVER (PARTITION
BY
    id,
    event_timestamp
ORDER BY
    created_timestamp DESC) rownum,
    id,
    event_timestamp,
    created_timestamp ,
    FIRST_VALUE(feature1 IGNORE NULLS) OVER w AS myentity_day_feature1 ,
    FIRST_VALUE(feature2 IGNORE NULLS) OVER w AS myentity_day_feature2
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
    FIRST_VALUE(feature3 IGNORE NULLS) OVER w AS myentity_none_feature3
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
    project_dataset_myentity_day.id,
    project_dataset_myentity_day.event_timestamp ,
    project_dataset_myentity_day.myentity_day_feature1,
    project_dataset_myentity_day.myentity_day_feature2,
    project_dataset_myentity_none.myentity_none_feature3
FROM
    project_dataset_myentity_day FULL
JOIN
    project_dataset_myentity_none
        ON project_dataset_myentity_day.id = project_dataset_myentity_none.id LIMIT 100