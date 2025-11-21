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

class OfflinePushBatchingConfig(FeastConfigBaseModel):
    """ Optional configuration for batching offline writes via the `/push` endpoint.

    Write attempts via `/push` to the offline store are buffered
    in memory and flushed either when either one of the batch size threshold or the batch interval duration is reached. """

    enabled: StrictBool = False
    """Whether or not to batch writes to offline store via push endpoint."""

    batch_size: StrictInt = 100
    """The maximum batch size for offline writes. """

    batch_interval_seconds: StrictInt = 1
    """The batch interval between offline writes. """

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

    offline_push_batching: Optional[OfflinePushBatchingConfig] = None
    """ Offline write batching configuration for the HTTP /push endpoint """
