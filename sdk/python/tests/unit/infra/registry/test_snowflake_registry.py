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

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from feast import FeatureView, Field
from feast.entity import Entity
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.snowflake import SnowflakeRegistry, SnowflakeRegistryConfig
from feast.infra.utils.snowflake.snowflake_utils import GetSnowflakeConnection
from feast.types import Float32


@pytest.fixture
def registry():
    config = SnowflakeRegistryConfig(
        database="TEST_DB",
        schema="PUBLIC",
        account="test_account",
        user="test_user",
        password="test_password",  # pragma: allowlist secret
    )
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn.__exit__ = MagicMock(return_value=False)

    # execute_snowflake_statement during __init__ creates tables; return empty cursor
    mock_cursor = MagicMock()
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame()

    with (
        patch(
            "feast.infra.registry.snowflake.GetSnowflakeConnection",
            return_value=mock_conn,
        ),
        patch(
            "feast.infra.registry.snowflake.execute_snowflake_statement",
            return_value=mock_cursor,
        ),
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

    with (
        patch(
            "feast.infra.registry.snowflake.GetSnowflakeConnection",
            return_value=mock_conn,
        ),
        patch(
            "feast.infra.registry.snowflake.execute_snowflake_statement",
            side_effect=capture_and_respond,
        ),
        patch.object(registry, "_maybe_init_project_metadata"),
        patch.object(registry, "_initialize_project_if_not_exists"),
        patch.object(registry, "get_project", return_value=MagicMock()),
        patch.object(registry, "apply_project"),
        patch.object(registry, "_set_last_updated_metadata"),
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


def test_apply_object_does_not_overwrite_sibling_project(registry):
    """
    Cross-project isolation: applying an object in project_b must not overwrite
    the same-named object in project_a when sharing a Snowflake registry.

    Regression coverage for feast-dev/feast#6208: the UPDATE path must scope by
    project_id so writes in one project cannot bleed into a sibling project.
    """
    entity = Entity(name="driver", join_keys=["driver_id"])
    project_a, project_b = "project_a", "project_b"

    # Simulate both projects having a pre-existing "driver" entity row
    row_b = pd.DataFrame({"project_id": [project_b]})

    update_queries = []

    def simulated_snowflake(conn, query):
        normalised = " ".join(query.split())
        cursor = MagicMock()
        if "SELECT" in normalised:
            # Scope the response to the project in the query; no row for project_a
            if f"project_id = '{project_b}'" in normalised:
                cursor.fetch_pandas_all.return_value = row_b
            else:
                cursor.fetch_pandas_all.return_value = pd.DataFrame()
        elif "UPDATE" in normalised:
            update_queries.append(normalised)
            cursor.fetch_pandas_all.return_value = pd.DataFrame()
        else:
            cursor.fetch_pandas_all.return_value = pd.DataFrame()
        return cursor

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=MagicMock())
    mock_conn.__exit__ = MagicMock(return_value=False)

    with (
        patch(
            "feast.infra.registry.snowflake.GetSnowflakeConnection",
            return_value=mock_conn,
        ),
        patch(
            "feast.infra.registry.snowflake.execute_snowflake_statement",
            side_effect=simulated_snowflake,
        ),
        patch.object(registry, "_maybe_init_project_metadata"),
        patch.object(registry, "_initialize_project_if_not_exists"),
        patch.object(registry, "get_project", return_value=MagicMock()),
        patch.object(registry, "apply_project"),
        patch.object(registry, "_set_last_updated_metadata"),
    ):
        registry._apply_object(
            "ENTITIES", project_b, "ENTITY_NAME", entity, "ENTITY_PROTO"
        )

    assert len(update_queries) == 1, (
        f"Expected exactly 1 UPDATE, got {len(update_queries)}: {update_queries}"
    )
    update_query = update_queries[0]
    assert f"project_id = '{project_b}'" in update_query, (
        f"feast#6208: UPDATE is not scoped to {project_b!r} — cross-project overwrite possible.\n"
        f"Query: {update_query}"
    )
    assert f"project_id = '{project_a}'" not in update_query, (
        f"feast#6208: UPDATE WHERE clause references {project_a!r} — unintended cross-project write.\n"
        f"Query: {update_query}"
    )


class TestSyncFeastMetadataToProjectsTable:
    def _make_registry(self):
        """Create a SnowflakeRegistry with mocked __init__."""
        with patch.object(SnowflakeRegistry, "__init__", lambda self: None):
            registry = SnowflakeRegistry()
            registry.registry_config = MagicMock()
            registry.registry_path = "test_db.test_schema"
            registry.purge_feast_metadata = False
            return registry

    @patch(
        "feast.infra.registry.snowflake.GetSnowflakeConnection",
    )
    @patch("feast.infra.registry.snowflake.execute_snowflake_statement")
    def test_sync_with_feast_metadata_projects(self, mock_execute, mock_get_conn):
        registry = self._make_registry()

        metadata_df = pd.DataFrame({"PROJECT_ID": ["project_a", "project_b"]})
        projects_df = pd.DataFrame({"PROJECT_ID": ["project_a"]})

        mock_cursor = MagicMock()
        mock_cursor.fetch_pandas_all.side_effect = [metadata_df, projects_df]
        mock_execute.return_value = mock_cursor

        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(registry, "apply_project") as mock_apply:
            registry._sync_feast_metadata_to_projects_table()

            mock_apply.assert_called_once()
            applied_project = mock_apply.call_args[0][0]
            assert applied_project.name == "project_b"

    @patch(
        "feast.infra.registry.snowflake.GetSnowflakeConnection",
    )
    @patch("feast.infra.registry.snowflake.execute_snowflake_statement")
    def test_sync_with_no_feast_metadata(self, mock_execute, mock_get_conn):
        registry = self._make_registry()

        empty_df = pd.DataFrame({"PROJECT_ID": []})
        mock_cursor = MagicMock()
        mock_cursor.fetch_pandas_all.return_value = empty_df
        mock_execute.return_value = mock_cursor

        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(registry, "apply_project") as mock_apply:
            registry._sync_feast_metadata_to_projects_table()

            mock_apply.assert_not_called()

    @patch(
        "feast.infra.registry.snowflake.GetSnowflakeConnection",
    )
    @patch("feast.infra.registry.snowflake.execute_snowflake_statement")
    def test_sync_deduplicates_project_ids(self, mock_execute, mock_get_conn):
        """Sets should deduplicate project IDs; lists would not."""
        registry = self._make_registry()

        metadata_df = pd.DataFrame(
            {"PROJECT_ID": ["project_a", "project_a", "project_b"]}
        )
        projects_df = pd.DataFrame({"PROJECT_ID": []})

        mock_cursor = MagicMock()
        mock_cursor.fetch_pandas_all.side_effect = [metadata_df, projects_df]
        mock_execute.return_value = mock_cursor

        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(registry, "apply_project") as mock_apply:
            registry._sync_feast_metadata_to_projects_table()

            assert mock_apply.call_count == 2
            applied_names = {call[0][0].name for call in mock_apply.call_args_list}
            assert applied_names == {"project_a", "project_b"}


class _DictableConfig:
    """A config object that supports dict() conversion and attribute access."""

    def __init__(self, data):
        self._data = data
        for k, v in data.items():
            setattr(self, k, v)

    def __iter__(self):
        return iter(self._data)

    def keys(self):
        return self._data.keys()

    def __getitem__(self, key):
        return self._data[key]


class TestGetSnowflakeConnection:
    @patch("feast.infra.utils.snowflake.snowflake_utils.parse_private_key_path")
    @patch("feast.infra.utils.snowflake.snowflake_utils.snowflake.connector")
    @patch("feast.infra.utils.snowflake.snowflake_utils._cache", {})
    def test_private_key_kwargs_not_leaked_to_connect(
        self, mock_connector, mock_parse_key
    ):
        """private_key_passphrase and private_key_content must not be passed to connect()."""
        mock_parse_key.return_value = b"parsed_key_bytes"
        mock_conn = MagicMock()
        mock_connector.connect.return_value = mock_conn

        config = _DictableConfig(
            {
                "type": "snowflake.registry",
                "account": "test_account",
                "user": "test_user",
                "password": None,
                "role": "test_role",
                "warehouse": "test_wh",
                "database": "test_db",
                "schema_": "test_schema",
                "config_path": "",
                "private_key": "/path/to/key.p8",
                "private_key_passphrase": "my_secret",  # pragma: allowlist secret
                "private_key_content": None,
            }
        )

        with GetSnowflakeConnection(config):
            pass

        connect_kwargs = mock_connector.connect.call_args[1]
        assert "private_key_passphrase" not in connect_kwargs
        assert "private_key_content" not in connect_kwargs
        assert connect_kwargs["private_key"] == b"parsed_key_bytes"


def _make_feature_view(name: str, entity: Entity, source: FileSource) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
    )


@pytest.fixture
def mock_snowflake_registry():
    """Return a SnowflakeRegistry whose Snowflake connection is fully mocked."""
    config = MagicMock()
    config.path = "snowflake://account/db/schema"
    config.registry_type = "snowflake"
    config.cache_ttl_seconds = 0
    config.cache_mode = "sync"

    with patch(
        "feast.infra.registry.snowflake.SnowflakeRegistry.__init__",
        return_value=None,
    ):
        registry = SnowflakeRegistry.__new__(SnowflakeRegistry)
        registry.registry_config = config
        registry.registry_path = "db.schema"
        registry.cache_mode = "sync"
        # list_all_feature_views also aggregates label views; default to none so the
        # updated_since tests only need to stub the feature-view list methods they exercise.
        registry.list_label_views = MagicMock(return_value=[])
        yield registry


def _make_mock_fv(name: str, updated_at: datetime) -> MagicMock:
    fv = MagicMock()
    fv.name = name
    # Snowflake stores timestamps as offset-naive UTC after round-tripping through proto
    fv.last_updated_timestamp = updated_at.replace(tzinfo=None)
    fv.tags = {}
    return fv


def test_list_all_feature_views_updated_since_no_cache(mock_snowflake_registry):
    """updated_since filters Python-side when allow_cache=False."""
    old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    new_ts = datetime(2024, 6, 1, tzinfo=timezone.utc)

    old_fv = _make_mock_fv("old_view", old_ts)
    new_fv = _make_mock_fv("new_view", new_ts)

    mock_snowflake_registry.list_feature_views = MagicMock(
        return_value=[old_fv, new_fv]
    )
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    cutoff = datetime(2023, 1, 1, tzinfo=timezone.utc)
    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False, updated_since=cutoff
    )
    assert [fv.name for fv in result] == ["new_view"]


def test_list_all_feature_views_updated_since_no_filter(mock_snowflake_registry):
    """Without updated_since, all feature views are returned."""
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    fv1 = _make_mock_fv("view_a", ts)
    fv2 = _make_mock_fv("view_b", ts)

    mock_snowflake_registry.list_feature_views = MagicMock(return_value=[fv1, fv2])
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False
    )
    assert len(result) == 2


def test_list_all_feature_views_updated_since_future_returns_empty(
    mock_snowflake_registry,
):
    """A future cutoff returns no feature views."""
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    fv = _make_mock_fv("any_view", ts)

    mock_snowflake_registry.list_feature_views = MagicMock(return_value=[fv])
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False, updated_since=future
    )
    assert result == []


def test_list_all_feature_views_updated_since_non_utc_tz(mock_snowflake_registry):
    """A non-UTC tz-aware cutoff is converted to UTC before comparing against naive-UTC timestamps."""
    est = timezone(timedelta(hours=-5))
    # 2023-01-01 00:00 EST == 2023-01-01 05:00 UTC
    cutoff_est = datetime(2023, 1, 1, 0, 0, 0, tzinfo=est)

    # Feature view updated at 2023-01-01 03:00 UTC — after midnight EST but before 05:00 UTC
    ts_between = datetime(2023, 1, 1, 3, 0, 0, tzinfo=timezone.utc)
    # Feature view updated at 2023-01-01 06:00 UTC — after both midnight EST and 05:00 UTC
    ts_after = datetime(2023, 1, 1, 6, 0, 0, tzinfo=timezone.utc)

    fv_between = _make_mock_fv("view_between", ts_between)
    fv_after = _make_mock_fv("view_after", ts_after)

    mock_snowflake_registry.list_feature_views = MagicMock(
        return_value=[fv_between, fv_after]
    )
    mock_snowflake_registry.list_stream_feature_views = MagicMock(return_value=[])
    mock_snowflake_registry.list_on_demand_feature_views = MagicMock(return_value=[])

    result = mock_snowflake_registry.list_all_feature_views(
        "project", allow_cache=False, updated_since=cutoff_est
    )
    # Only view_after is at or after 05:00 UTC; view_between (03:00 UTC) should be excluded
    assert [fv.name for fv in result] == ["view_after"]
