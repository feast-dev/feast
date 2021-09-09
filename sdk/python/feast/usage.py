# Copyright 2019 The Feast Authors
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
import logging
import os
import sys
import uuid
from datetime import datetime
from functools import wraps
from os.path import expanduser, join
from pathlib import Path
from typing import List, Optional, Tuple

import requests

from feast.version import get_version

USAGE_ENDPOINT = "https://usage.feast.dev"
_logger = logging.getLogger(__name__)

# This mapping from (function_name, module_name) to (frequency) determines how often
# each invokation of the method will be logged. For example, if ("get_online_features",
# "feast.feature_store") maps to 10000, then 1 of every 10000 calls will be logged.
# If a method is not in this mapping, every invokation will be logged.
METHOD_TO_LOG_FREQUENCY = {
    ("get_online_features", "feast.feature_store"): 10000,
    ("online_read", "feast.infra.online_stores.datastore"): 10000,
    ("online_write_batch", "feast.infra.online_stores.datastore"): 10000,
    ("online_read", "feast.infra.online_stores.dynamodb"): 10000,
    ("online_write_batch", "feast.infra.online_stores.dynamodb"): 10000,
    ("online_read", "feast.infra.online_stores.redis"): 10000,
    ("online_write_batch", "feast.infra.online_stores.redis"): 10000,
    ("online_read", "feast.infra.online_stores.sqlite"): 10000,
    ("online_write_batch", "feast.infra.online_stores.sqlite"): 10000,
}

online_stores = [
    "feast.infra.online_stores.datastore",
    "feast.infra.online_stores.dynamodb",
    "feast.infra.online_stores.redis",
    "feast.infra.online_stores.sqlite",
]

offline_stores = [
    "feast.infra.offline_stores.redshift",
    "feast.infra.offline_stores.bigquery",
    "feast.infra.offline_stores.file",
]

providers = [
    "feast.infra.gcp",
    "feast.infra.aws",
    "feast.infra.local",
]

# This is a mapping from methods to upstream methods indicating that their logs should
# be tied together. For example, FeatureStore.apply calls GcpProvider.update_infra, so
# ("update_infra", "feast.infra.gcp") is mapped to ("apply", "feast.feature_store").
METHOD_TO_TOP_LOGGING_METHOD = {}

# Apply
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("update_infra", provider): ("apply", "feast.feature_store")
        for provider in providers
    }
)
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("update", online_store): ("apply", "feast.feature_store")
        for online_store in online_stores
    }
)

# Historical retrieval
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("get_historical_features", provider): (
            "get_historical_features",
            "feast.feature_store",
        )
        for provider in providers
    }
)
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("get_historical_features", offline_store): (
            "get_historical_features",
            "feast.feature_store",
        )
        for offline_store in offline_stores
    }
)

# Online retrieval
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("online_read", provider): ("get_online_features", "feast.feature_store")
        for provider in providers
    }
)
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("online_read", online_store): ("get_online_features", "feast.feature_store")
        for online_store in online_stores
    }
)

# Materialization
materialization_methods = ["materialize", "materialize_incremental"]
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("materialize_single_feature_view", provider): (method, "feast.feature_store")
        for provider in providers
        for method in materialization_methods
    }
)
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("pull_latest_from_table_or_query", offline_store): (
            method,
            "feast.feature_store",
        )
        for offline_store in offline_stores
        for method in materialization_methods
    }
)
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("online_write_batch", provider): (method, "feast.feature_store")
        for provider in providers
        for method in materialization_methods
    }
)
METHOD_TO_TOP_LOGGING_METHOD.update(
    {
        ("online_write_batch", online_store): (method, "feast.feature_store")
        for online_store in online_stores
        for method in materialization_methods
    }
)

# This is a list of all top-level methods for which there are downstream methods whose
# logs should be tied to the logs of these methods.
TOP_LOGGING_METHODS = [
    ("apply", "feast.feature_store"),
    ("get_historical_features", "feast.feature_store"),
    ("get_online_features", "feast.feature_store"),
    ("materialize", "feast.feature_store"),
    ("materialize_incremental", "feast.feature_store"),
]


class Usage:
    def __init__(self):
        self._usage_enabled: bool = False
        self.method_to_log_frequency = METHOD_TO_LOG_FREQUENCY
        self.method_to_uid = {}
        self.check_env_and_configure()

    def check_env_and_configure(self):
        usage_enabled = (
            os.getenv("FEAST_USAGE", default="True") == "True"
        )  # written this way to turn the env var string into a boolean

        # Check if it changed
        if usage_enabled != self._usage_enabled:
            self._usage_enabled = usage_enabled

            if self._usage_enabled:
                try:
                    feast_home_dir = join(expanduser("~"), ".feast")
                    Path(feast_home_dir).mkdir(exist_ok=True)
                    usage_filepath = join(feast_home_dir, "usage")

                    self._is_test = os.getenv("FEAST_IS_USAGE_TEST", "False") == "True"
                    self._usage_counter = {k: 0 for k in self.method_to_log_frequency}

                    if os.path.exists(usage_filepath):
                        with open(usage_filepath, "r") as f:
                            self._usage_id = f.read()
                    else:
                        self._usage_id = str(uuid.uuid4())

                        with open(usage_filepath, "w") as f:
                            f.write(self._usage_id)
                        print(
                            "Feast is an open source project that collects anonymized error reporting and usage statistics. To opt out or learn"
                            " more see https://docs.feast.dev/reference/usage"
                        )
                except Exception as e:
                    _logger.debug(f"Unable to configure usage {e}")

    @property
    def usage_id(self) -> Optional[str]:
        if os.getenv("FEAST_FORCE_USAGE_UUID"):
            return os.getenv("FEAST_FORCE_USAGE_UUID")
        return self._usage_id

    def set_log_frequency_for_method(
        self, function_name: str, module_name: str, log_frequency: int
    ):
        self.method_to_log_frequency[(function_name, module_name)] = log_frequency

    def register(self, function_name: str, module_name: str):
        method = (function_name, module_name)
        if method in TOP_LOGGING_METHODS:
            uid = str(uuid.uuid4())
            self.method_to_uid[method] = uid

    def log(self, function_name: str, module_name: str, execution_time: int):
        self.check_env_and_configure()
        if self._usage_enabled and self.usage_id:
            method = (function_name, module_name)
            if method in self._usage_counter:
                frequency = self.method_to_log_frequency[method]
                self._usage_counter[method] += 1
                # Begin logging on the second invokation of the method.
                if self._usage_counter[method] % frequency != 2:
                    return
                self._usage_counter[method] = 2  # avoid overflow
            json = {
                "function_name": function_name,
                "module_name": module_name,
                "usage_id": self.usage_id,
                "execution_time": execution_time,
                "timestamp": datetime.utcnow().isoformat(),
                "version": get_version(),
                "os": sys.platform,
                "is_test": self._is_test,
            }

            # Ensure that downstream methods are tied to upstream methods by logging
            # the same uid.
            if method in TOP_LOGGING_METHODS:
                if method in self.method_to_uid:
                    json["uid"] = self.method_to_uid[method]
            if method in METHOD_TO_TOP_LOGGING_METHOD:
                top_logging_method = METHOD_TO_TOP_LOGGING_METHOD[method]
                if top_logging_method in self.method_to_uid:
                    json["uid"] = self.method_to_uid[top_logging_method]

            try:
                requests.post(USAGE_ENDPOINT, json=json)
            except Exception as e:
                if self._is_test:
                    raise e
                else:
                    pass
            return

    def log_exception(self, error_type: str, traceback: List[Tuple[str, int, str]]):
        self.check_env_and_configure()
        if self._usage_enabled and self.usage_id:
            json = {
                "error_type": error_type,
                "traceback": traceback,
                "usage_id": self.usage_id,
                "version": get_version(),
                "os": sys.platform,
                "is_test": self._is_test,
            }
            try:
                requests.post(USAGE_ENDPOINT, json=json)
            except Exception as e:
                if self._is_test:
                    raise e
                else:
                    pass
            return


def log_exceptions(func):
    @wraps(func)
    def exception_logging_wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error_type = type(e).__name__
            trace_to_log = []
            tb = e.__traceback__
            while tb is not None:
                trace_to_log.append(
                    (
                        _trim_filename(tb.tb_frame.f_code.co_filename),
                        tb.tb_lineno,
                        tb.tb_frame.f_code.co_name,
                    )
                )
                tb = tb.tb_next
            usage.log_exception(error_type, trace_to_log)
            raise
        return result

    return exception_logging_wrapper


def log_exceptions_and_usage(func):
    @wraps(func)
    def exception_logging_wrapper(*args, **kwargs):
        try:
            start_time = datetime.now()
            usage.register(func.__name__, func.__module__)
            result = func(*args, **kwargs)
            end_time = datetime.now()
            time_diff = end_time - start_time
            execution_time = int(time_diff.total_seconds() * 1000)
            usage.log(func.__name__, func.__module__, execution_time)
        except Exception as e:
            error_type = type(e).__name__
            trace_to_log = []
            tb = e.__traceback__
            while tb is not None:
                trace_to_log.append(
                    (
                        _trim_filename(tb.tb_frame.f_code.co_filename),
                        tb.tb_lineno,
                        tb.tb_frame.f_code.co_name,
                    )
                )
                tb = tb.tb_next
            usage.log_exception(error_type, trace_to_log)
            raise
        return result

    return exception_logging_wrapper


def _trim_filename(filename: str) -> str:
    return filename.split("/")[-1]


# Single global usage object
usage = Usage()
