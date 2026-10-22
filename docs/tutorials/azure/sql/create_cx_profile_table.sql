CREATE TABLE dbo.customer_profile
(
    [datetime] DATETIME2(0),
    [customer_id] bigint,
    [current_balance] float,
    [lifetime_trip_count] bigint,
    [avg_passenger_count] float,
    [created] datetime2(3)
)
WITH
(
DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
