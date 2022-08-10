
from feast import FeatureService
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination

from example import driver_hourly_stats, driver, driver_hourly_stats_view

fs = FeatureService(
    name="driver_activity", 
    features=[driver_hourly_stats_view],
    logging_config=LoggingConfig(
        sample_rate=1.0,
        destination=FileLoggingDestination(path="logs/"),
    )
)
