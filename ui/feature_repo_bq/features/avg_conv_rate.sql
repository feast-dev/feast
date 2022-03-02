
with driver_stats as (
    select
        driver_id,
        conv_rate,
        event_timestamp
    from danny_test_dataset.driver_stats_with_string
)
select
    driver_id,
    AVG(conv_rate) OVER (ORDER BY event_timestamp ASC ROWS 5 PRECEDING) as average_conv_rate_over_last_5,
    event_timestamp
from driver_stats