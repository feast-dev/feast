-- Driver statistics feature model
-- This model aggregates driver-level features for ML

SELECT
    driver_id,
    event_timestamp,
    conv_rate,
    acc_rate,
    avg_daily_trips
FROM {{ source('raw_data', 'driver_hourly_stats') }}
