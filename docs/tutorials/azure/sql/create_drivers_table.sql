CREATE TABLE dbo.driver_hourly
(
    [datetime] DATETIME2(0),
    [driver_id] bigint,
    [avg_daily_trips] float,
    [conv_rate] float,
    [acc_rate] float,
    [created] datetime2(3)
)
WITH
(
DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
