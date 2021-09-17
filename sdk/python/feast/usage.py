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
import concurrent.futures
import enum
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

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


@enum.unique
class UsageEvent(enum.Enum):
    """
    An event meant to be logged
    """

    UNKNOWN = 0
    APPLY_WITH_ODFV = 1
    GET_HISTORICAL_FEATURES_WITH_ODFV = 2
    GET_ONLINE_FEATURES_WITH_ODFV = 3

    def __str__(self):
        return self.name.lower()


class Usage:
    def __init__(self):
        self._usage_enabled: bool = False
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
                    self._usage_counter = {}

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

    def _send_usage_request(self, json):
        try:
            future = executor.submit(requests.post, USAGE_ENDPOINT, json=json)
            if self._is_test:
                concurrent.futures.wait([future])
        except Exception as e:
            if self._is_test:
                raise e
            else:
                pass

    def log_function(self, function_name: str):
        self.check_env_and_configure()
        if self._usage_enabled and self.usage_id:
            if (
                function_name == "get_online_features"
                and not self.should_log_for_get_online_features_event(function_name)
            ):
                return
            json = {
                "function_name": function_name,
                "usage_id": self.usage_id,
                "timestamp": datetime.utcnow().isoformat(),
                "version": get_version(),
                "os": sys.platform,
                "is_test": self._is_test,
            }
            self._send_usage_request(json)

    def should_log_for_get_online_features_event(self, event_name: str):
        if event_name not in self._usage_counter:
            self._usage_counter[event_name] = 0
        self._usage_counter[event_name] += 1
        if self._usage_counter[event_name] % 10000 != 2:
            return False
        self._usage_counter[event_name] = 2  # avoid overflow
        return True

    def log_event(self, event: UsageEvent):
        self.check_env_and_configure()
        if self._usage_enabled and self.usage_id:
            event_name = str(event)
            if (
                event == UsageEvent.GET_ONLINE_FEATURES_WITH_ODFV
                and not self.should_log_for_get_online_features_event(event_name)
            ):
                return
            json = {
                "event_name": event_name,
                "usage_id": self.usage_id,
                "timestamp": datetime.utcnow().isoformat(),
                "version": get_version(),
                "os": sys.platform,
                "is_test": self._is_test,
            }
            self._send_usage_request(json)

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
            result = func(*args, **kwargs)
            usage.log_function(func.__name__)
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


def log_event(event: UsageEvent):
    usage.log_event(event)


def _trim_filename(filename: str) -> str:
    return filename.split("/")[-1]


# Single global usage object
usage = Usage()
