
WITH driver_stats AS (
    SELECT
        driver_id,
        conv_rate,
        event_timestamp
    FROM danny_test_dataset.driver_stats_with_string
)
SELECT
    driver_id,
    SUM(conv_rate) OVER (ORDER BY event_timestamp ASC ROWS 5 PRECEDING) AS total_conv_rate_over_last_5,
    event_timestamp
FROM driver_stats