CREATE TABLE dbo.orders
(
    [order_id] bigint,
    [driver_id] bigint,
    [customer_id] bigint,
    [order_is_success] int,
    [event_timestamp] datetime2(3)
)
WITH
(
DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
