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
import tempfile
import uuid
from datetime import datetime

from tenacity import retry, wait_exponential, stop_after_attempt

from google.cloud import bigquery
import os
from time import sleep
from importlib import reload

from feast import Client, Entity, ValueType, FeatureStore, RepoConfig
from feast.repo_config import SqliteOnlineStoreConfig

TELEMETRY_BIGQUERY_TABLE = (
    "kf-feast.feast_telemetry.cloudfunctions_googleapis_com_cloud_functions"
)


def test_telemetry_on_v09(mocker):
    # Setup environment
    old_environ = dict(os.environ)
    os.environ["FEAST_IS_TELEMETRY_TEST"] = "True"
    test_telemetry_id = str(uuid.uuid4())
    os.environ["FEAST_FORCE_TELEMETRY_UUID"] = test_telemetry_id
    test_client = Client(serving_url=None, core_url=None, telemetry=True)
    test_client.set_project("project1")
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    mocker.patch.object(
        test_client, "_apply_entity", return_value=None,
    )

    test_client.apply(entity)

    os.environ.clear()
    os.environ.update(old_environ)

    ensure_bigquery_telemetry_id_with_retry(test_telemetry_id)


def test_telemetry_off_v09(mocker):
    old_environ = dict(os.environ)
    os.environ["FEAST_IS_TELEMETRY_TEST"] = "True"
    test_telemetry_id = str(uuid.uuid4())
    os.environ["FEAST_FORCE_TELEMETRY_UUID"] = test_telemetry_id
    os.environ["FEAST_TELEMETRY"] = "False"

    test_client = Client(serving_url=None, core_url=None, telemetry=False)
    test_client.set_project("project1")
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking"},
    )

    mocker.patch.object(
        test_client, "_apply_entity", return_value=None,
    )

    test_client.apply(entity)

    os.environ.clear()
    os.environ.update(old_environ)
    sleep(30)
    rows = read_bigquery_telemetry_id(test_telemetry_id)
    assert rows.total_rows == 0


def test_telemetry_on():
    old_environ = dict(os.environ)
    test_telemetry_id = str(uuid.uuid4())
    os.environ["FEAST_FORCE_TELEMETRY_UUID"] = test_telemetry_id
    os.environ["FEAST_IS_TELEMETRY_TEST"] = "True"
    os.environ["FEAST_TELEMETRY"] = "True"

    with tempfile.TemporaryDirectory() as temp_dir:
        test_feature_store = FeatureStore(
            config=RepoConfig(
                registry=os.path.join(temp_dir, "registry.db"),
                project="fake_project",
                provider="local",
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(temp_dir, "online.db")
                ),
            )
        )
        entity = Entity(
            name="driver_car_id",
            description="Car driver id",
            value_type=ValueType.STRING,
            labels={"team": "matchmaking"},
        )

        test_feature_store.apply([entity])

        os.environ.clear()
        os.environ.update(old_environ)
        ensure_bigquery_telemetry_id_with_retry(test_telemetry_id)


def test_telemetry_off():
    old_environ = dict(os.environ)
    test_telemetry_id = str(uuid.uuid4())
    os.environ["FEAST_IS_TELEMETRY_TEST"] = "True"
    os.environ["FEAST_TELEMETRY"] = "False"
    os.environ["FEAST_FORCE_TELEMETRY_UUID"] = test_telemetry_id

    with tempfile.TemporaryDirectory() as temp_dir:
        test_feature_store = FeatureStore(
            config=RepoConfig(
                registry=os.path.join(temp_dir, "registry.db"),
                project="fake_project",
                provider="local",
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(temp_dir, "online.db")
                ),
            )
        )
        entity = Entity(
            name="driver_car_id",
            description="Car driver id",
            value_type=ValueType.STRING,
            labels={"team": "matchmaking"},
        )
        test_feature_store.apply([entity])

        os.environ.clear()
        os.environ.update(old_environ)
        sleep(30)
        rows = read_bigquery_telemetry_id(test_telemetry_id)
        assert rows.total_rows == 0


def test_exception_telemetry_on():
    old_environ = dict(os.environ)
    test_telemetry_id = str(uuid.uuid4())
    os.environ["FEAST_FORCE_TELEMETRY_UUID"] = test_telemetry_id
    os.environ["FEAST_IS_TELEMETRY_TEST"] = "True"
    os.environ["FEAST_TELEMETRY"] = "True"

    try:
        test_feature_store = FeatureStore("/tmp/non_existent_directory")
    except:
        pass

    os.environ.clear()
    os.environ.update(old_environ)
    ensure_bigquery_telemetry_id_with_retry(test_telemetry_id)


def test_exception_telemetry_off():
    old_environ = dict(os.environ)
    test_telemetry_id = str(uuid.uuid4())
    os.environ["FEAST_IS_TELEMETRY_TEST"] = "True"
    os.environ["FEAST_TELEMETRY"] = "False"
    os.environ["FEAST_FORCE_TELEMETRY_UUID"] = test_telemetry_id

    try:
        test_feature_store = FeatureStore("/tmp/non_existent_directory")
    except:
        pass

    os.environ.clear()
    os.environ.update(old_environ)
    sleep(30)
    rows = read_bigquery_telemetry_id(test_telemetry_id)
    assert rows.total_rows == 0


@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
def ensure_bigquery_telemetry_id_with_retry(telemetry_id):
    rows = read_bigquery_telemetry_id(telemetry_id)
    if rows.total_rows != 1:
        raise Exception(f"Could not find telemetry id: {telemetry_id}")


def read_bigquery_telemetry_id(telemetry_id):
    bq_client = bigquery.Client()
    query = f"""
                SELECT
                  telemetry_id
                FROM (
                  SELECT
                    JSON_EXTRACT(textPayload, '$.telemetry_id') AS telemetry_id
                  FROM
                    `{TELEMETRY_BIGQUERY_TABLE}`
                  WHERE
                    timestamp >= TIMESTAMP(\"{datetime.utcnow().date().isoformat()}\"))
                WHERE
                  telemetry_id = '\"{telemetry_id}\"'
            """
    query_job = bq_client.query(query)
    return query_job.result()
