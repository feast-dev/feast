# Copyright 2024 The Feast Authors
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

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from feast.entity import Entity
from feast.infra.registry.snowflake import SnowflakeRegistry, SnowflakeRegistryConfig


@pytest.fixture
def registry():
    config = SnowflakeRegistryConfig(
        database="TEST_DB",
        schema="PUBLIC",
        account="test_account",
        user="test_user",
        password="test_password",
    )
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn.__exit__ = MagicMock(return_value=False)

    # execute_snowflake_statement during __init__ creates tables; return empty cursor
    mock_cursor = MagicMock()
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame()

    with patch(
        "feast.infra.registry.snowflake.GetSnowflakeConnection", return_value=mock_conn
    ), patch(
        "feast.infra.registry.snowflake.execute_snowflake_statement",
        return_value=mock_cursor,
    ):
        reg = SnowflakeRegistry(config, "test_project", None)
    return reg


def test_apply_object_update_includes_project_id_in_where_clause(registry):
    """
    Regression test for feast-dev/feast#6208.

    _apply_object UPDATE path was missing project_id in the WHERE clause,
    allowing a same-named object in one project to silently overwrite the
    same-named object in a different project when a Snowflake registry is shared.

    The SELECT path correctly scopes by project_id; the UPDATE must do the same.
    """
    entity = Entity(name="driver", join_keys=["driver_id"])
    project = "project_b"

    # Non-empty DataFrame tells _apply_object the row already exists → UPDATE path
    existing_row = pd.DataFrame({"project_id": [project]})

    captured_queries = []

    def capture_and_respond(conn, query):
        normalised = " ".join(query.split())
        captured_queries.append(normalised)
        cursor = MagicMock()
        cursor.fetch_pandas_all.return_value = (
            existing_row if "SELECT" in normalised else pd.DataFrame()
        )
        return cursor

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch(
        "feast.infra.registry.snowflake.GetSnowflakeConnection", return_value=mock_conn
    ), patch(
        "feast.infra.registry.snowflake.execute_snowflake_statement",
        side_effect=capture_and_respond,
    ), patch.object(
        registry, "_maybe_init_project_metadata"
    ), patch.object(
        registry, "_initialize_project_if_not_exists"
    ), patch.object(
        registry, "get_project", return_value=MagicMock()
    ), patch.object(
        registry, "apply_project"
    ), patch.object(
        registry, "_set_last_updated_metadata"
    ):
        registry._apply_object(
            "ENTITIES",
            project,
            "ENTITY_NAME",
            entity,
            "ENTITY_PROTO",
        )

    update_queries = [q for q in captured_queries if q.startswith("UPDATE")]
    assert len(update_queries) == 1, (
        f"Expected exactly 1 UPDATE query, got {len(update_queries)}: {update_queries}"
    )

    update_query = update_queries[0]
    assert f"project_id = '{project}'" in update_query, (
        f"feast#6208: UPDATE WHERE clause is missing project_id filter — "
        f"cross-project overwrites are possible in shared Snowflake registries.\n"
        f"Query was: {update_query}"
    )
