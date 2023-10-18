COPY INTO dbo.orders
FROM 'https://feastonazuredatasamples.blob.core.windows.net/feastdatasamples/orders.csv'
WITH
(
	FILE_TYPE = 'CSV'
	,FIRSTROW = 2
	,MAXERRORS = 0
)
