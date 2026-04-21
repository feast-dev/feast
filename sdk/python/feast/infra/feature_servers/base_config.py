# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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


class MetricsConfig(FeastConfigBaseModel):
    """Prometheus metrics configuration.

    Follows the same pattern as ``FeatureLoggingConfig``: a single
    ``enabled`` flag controls global on/off, and per-category booleans
    allow fine-grained suppression.  Can also be enabled at runtime via
    the ``feast serve --metrics`` CLI flag — either option is sufficient.
    """

    enabled: StrictBool = False
    """Whether Prometheus metrics collection and the metrics HTTP server
    (default port 8000) should be enabled."""

    resource: StrictBool = True
    """Emit CPU and memory usage gauges (feast_feature_server_cpu_usage,
    feast_feature_server_memory_usage)."""

    request: StrictBool = True
    """Emit per-endpoint request counters and latency histograms
    (feast_feature_server_request_total,
    feast_feature_server_request_latency_seconds)."""

    online_features: StrictBool = True
    """Emit online feature retrieval metrics
    (feast_online_features_request_total,
    feast_online_features_entity_count,
    feast_feature_server_online_store_read_duration_seconds,
    feast_feature_server_transformation_duration_seconds,
    feast_feature_server_write_transformation_duration_seconds).
    ODFV transformation metrics additionally require track_metrics=True
    on the OnDemandFeatureView definition."""

    push: StrictBool = True
    """Emit push/write request counters
    (feast_push_request_total)."""

    materialization: StrictBool = True
    """Emit materialization success/failure counters and duration histograms
    (feast_materialization_result_total,
    feast_materialization_duration_seconds)."""

    freshness: StrictBool = True
    """Emit per-feature-view freshness gauges
    (feast_feature_freshness_seconds)."""


class DqmInitialDistributionConfig(FeastConfigBaseModel):
    """Controls whether baseline distribution is computed on ``feast apply``."""

    enabled: StrictBool = True


class DqmDistributionConfig(FeastConfigBaseModel):
    initial: DqmInitialDistributionConfig = DqmInitialDistributionConfig()


class DqmConfig(FeastConfigBaseModel):
    """Data Quality Monitoring (DQM) configuration."""

    distribution: DqmDistributionConfig = DqmDistributionConfig()


class BaseFeatureServerConfig(FeastConfigBaseModel):
    """Base Feature Server config that should be extended"""

    enabled: StrictBool = False
    """Whether the feature server should be launched."""

    metrics: Optional[MetricsConfig] = None
    """Prometheus metrics configuration.  Set ``metrics.enabled: true`` or
    pass the ``feast serve --metrics`` CLI flag to activate."""

    feature_logging: Optional[FeatureLoggingConfig] = None
    """ Feature logging configuration """

    dqm: Optional[DqmConfig] = None
    """Data Quality Monitoring configuration."""

    offline_push_batching_enabled: Optional[StrictBool] = None
    """Whether to batch writes to the offline store via the `/push` endpoint."""

    offline_push_batching_batch_size: Optional[StrictInt] = None
    """The maximum batch size for offline writes via `/push`."""

    offline_push_batching_batch_interval_seconds: Optional[StrictInt] = None
    """The batch interval between offline writes via `/push`."""
