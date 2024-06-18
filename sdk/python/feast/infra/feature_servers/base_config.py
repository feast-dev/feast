from typing import Optional

from pydantic import StrictBool, StrictInt

from feast.repo_config import FeastConfigBaseModel


class FeatureLoggingConfig(FeastConfigBaseModel):
    enabled: StrictBool = False
    """Whether the feature server should log served features."""

    flush_interval_secs: StrictInt = 600
    """Interval of flushing logs to the destination in offline store."""

    write_to_disk_interval_secs: StrictInt = 30
    """Interval of dumping logs collected in memory to local disk."""

    queue_capacity: StrictInt = 100000
    """Log queue capacity. If number of produced logs is bigger than
    processing speed logs will be accumulated in the queue.
    After queue length will reach this number all new items will be rejected."""

    emit_timeout_micro_secs: StrictInt = 10000
    """Timeout for adding new log item to the queue."""


class BaseFeatureServerConfig(FeastConfigBaseModel):
    """Base Feature Server config that should be extended"""

    enabled: StrictBool = False
    """Whether the feature server should be launched."""

    feature_logging: Optional[FeatureLoggingConfig] = None
    """ Feature logging configuration """
