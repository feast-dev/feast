-- Product recommendation features
-- Tagged with 'feast' for filtering tests

SELECT
    product_id,
    event_timestamp,
    view_count,
    purchase_count,
    rating_avg
FROM {{ source('raw_data', 'product_stats') }}
