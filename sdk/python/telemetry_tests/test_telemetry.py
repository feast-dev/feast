# Copyright 2020 The Feast Authors
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

from datetime import datetime
from feast.client import Client
from feast.entity import Entity
from feast.value_type import ValueType
from google.cloud import bigquery
import os
import pytest
from time import sleep

def test_telemetry_on():
    os.environ["FEAST_IS_TELEMETRY_TEST"] = 'True'
    test_client = Client(serving_url=None, core_url=None)
    test_client.set_project("project1")
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    timestamp = datetime.utcnow()
    try:
        test_client.apply(entity)
    except Exception:
        pass

    sleep(30)
    bq_client = bigquery.Client()
    query = f"select * from `kf-feast.feast_telemetry.cloudfunctions_googleapis_com_cloud_functions` where timestamp >= TIMESTAMP(\"{timestamp.date().isoformat()}\") and JSON_EXTRACT(textPayload, '$.is_test')='\"True\"' and JSON_EXTRACT(textPayload, '$.timestamp')>'\"{timestamp.isoformat()}\"'"
    query_job = bq_client.query(query)
    rows = query_job.result()
    assert(rows.total_rows == 1)

def test_telemetry_off():
    os.environ["FEAST_IS_TELEMETRY_TEST"] = 'True'
    test_client = Client(serving_url=None, core_url=None, telemetry=False)
    test_client.set_project("project1")
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    timestamp = datetime.utcnow()
    try:
        test_client.apply(entity)
    except Exception:
        pass

    sleep(30)
    bq_client = bigquery.Client()
    query = f"select * from `kf-feast.feast_telemetry.cloudfunctions_googleapis_com_cloud_functions` where timestamp >= TIMESTAMP(\"{timestamp.date().isoformat()}\") and JSON_EXTRACT(textPayload, '$.is_test')='\"True\"' and JSON_EXTRACT(textPayload, '$.timestamp')>'\"{timestamp.isoformat()}\"'"
    query_job = bq_client.query(query)
    rows = query_job.result()
    assert(rows.total_rows == 0)
