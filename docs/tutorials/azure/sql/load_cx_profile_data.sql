COPY INTO dbo.customer_profile
FROM 'https://feastonazuredatasamples.blob.core.windows.net/feastdatasamples/customer_profile.csv'
WITH
(
	FILE_TYPE = 'CSV'
	,FIRSTROW = 2
	,MAXERRORS = 0
)
