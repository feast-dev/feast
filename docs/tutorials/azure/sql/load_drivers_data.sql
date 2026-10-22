COPY INTO dbo.driver_hourly
FROM 'https://feastonazuredatasamples.blob.core.windows.net/feastdatasamples/driver_hourly.csv'
WITH
(
	FILE_TYPE = 'CSV'
	,FIRSTROW = 2
	,MAXERRORS = 0
)
