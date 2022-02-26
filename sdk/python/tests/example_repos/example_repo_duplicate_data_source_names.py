from feast import FileSource

driver_hourly_stats = FileSource(
    path="driver_stats.parquet",  # this parquet is not real and will not be read
)

driver_hourly_stats_clone = FileSource(
    path="driver_stats.parquet",  # this parquet is not real and will not be read
)
