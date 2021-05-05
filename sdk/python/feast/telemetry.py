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

import os
import sys
import uuid
from datetime import datetime
from functools import wraps
from os.path import expanduser, join
from pathlib import Path
from typing import List, Tuple

import requests

from feast.version import get_version

TELEMETRY_ENDPOINT = (
    "https://us-central1-kf-feast.cloudfunctions.net/bq_telemetry_logger"
)


class Telemetry:
    def __init__(self):
        feast_home_dir = join(expanduser("~"), ".feast")
        Path(feast_home_dir).mkdir(exist_ok=True)
        telemetry_filepath = join(feast_home_dir, "telemetry")
        self._telemetry_enabled = (
            os.getenv("FEAST_TELEMETRY", default="True") == "True"
        )  # written this way to turn the env var string into a boolean

        self._is_test = os.getenv("FEAST_IS_TELEMETRY_TEST", "False") == "True"

        if self._telemetry_enabled:
            self._telemetry_counter = {"get_online_features": 0}
            if os.path.exists(telemetry_filepath):
                with open(telemetry_filepath, "r") as f:
                    self._telemetry_id = f.read()
            else:
                self._telemetry_id = str(uuid.uuid4())

                with open(telemetry_filepath, "w") as f:
                    f.write(self._telemetry_id)
                print(
                    "Feast is an open source project that collects anonymized error reporting and usage statistics. To opt out or learn"
                    " more see https://docs.feast.dev/v/master/feast-on-kubernetes/advanced-1/telemetry"
                )

    @property
    def telemetry_id(self):
        if os.getenv("FEAST_FORCE_TELEMETRY_UUID"):
            return os.getenv("FEAST_FORCE_TELEMETRY_UUID")
        return self._telemetry_id

    def log(self, function_name: str):

        if self._telemetry_enabled and self.telemetry_id:
            if function_name == "get_online_features":
                if self._telemetry_counter["get_online_features"] % 10000 != 0:
                    self._telemetry_counter["get_online_features"] += 1
                    return

            json = {
                "function_name": function_name,
                "telemetry_id": self.telemetry_id,
                "timestamp": datetime.utcnow().isoformat(),
                "version": get_version(),
                "os": sys.platform,
                "is_test": self._is_test,
            }
            try:
                requests.post(TELEMETRY_ENDPOINT, json=json)
            except Exception as e:
                if self._is_test:
                    raise e
                else:
                    pass
            return

    def log_exception(self, error_type: str, traceback: List[Tuple[str, int, str]]):
        if self._telemetry_enabled and self.telemetry_id:
            json = {
                "error_type": error_type,
                "traceback": traceback,
                "telemetry_id": self.telemetry_id,
                "version": get_version(),
                "os": sys.platform,
                "is_test": self._is_test,
            }
            try:
                requests.post(TELEMETRY_ENDPOINT, json=json)
            except Exception as e:
                if self._is_test:
                    raise e
                else:
                    pass
            return


def public_method(func):
    @wraps(func)
    def public_method_wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error_type = type(e).__name__
            trace_to_log = []
            tb = e.__traceback__
            while tb is not None:
                trace_to_log.append((_trim_filename(tb.tb_frame.f_code.co_filename), tb.tb_lineno, tb.tb_frame.f_code.co_name))
                tb = tb.tb_next
            tele = Telemetry()
            tele.log_exception(error_type, trace_to_log)
            raise
        return result
    return public_method_wrapper

def _trim_filename(filename: str) -> str:
    return filename.split("/")[-1]
