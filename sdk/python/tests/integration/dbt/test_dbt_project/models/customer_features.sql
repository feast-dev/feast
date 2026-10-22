-- Customer statistics feature model
-- Features for customer behavior

SELECT
    customer_id,
    event_timestamp,
    total_orders,
    total_spent,
    avg_order_value
FROM {{ ref('customer_stats') }}
