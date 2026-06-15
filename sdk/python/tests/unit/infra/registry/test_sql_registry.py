# Copyright 2021 The Feast Authors
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

import sys
import tempfile
import types
from datetime import timedelta

import dill
import pytest

from feast import Field
from feast.data_source import PushSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig, feature_views
from feast.protos.feast.core.Transformation_pb2 import (
    FeatureTransformationV2,
    UserDefinedFunctionV2,
)
from feast.stream_feature_view import StreamFeatureView
from feast.types import Float32
from feast.value_type import ValueType


@pytest.fixture
def sqlite_registry():
    """Create a temporary SQLite registry for testing."""
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


@pytest.fixture
def shared_sqlite_db_path():
    """Return a shared SQLite DB path for cross-project tests."""
    fd, path = tempfile.mkstemp()
    yield path


def test_sql_registry(sqlite_registry):
    """
    Test the SQL registry
    """
    entity = Entity(
        name="test_entity",
        description="Test entity for testing",
        tags={"test": "transaction"},
    )
    sqlite_registry.apply_entity(entity, "test_project")
    retrieved_entity = sqlite_registry.get_entity("test_entity", "test_project")
    assert retrieved_entity.name == "test_entity"
    assert retrieved_entity.description == "Test entity for testing"

    sqlite_registry.set_project_metadata("test_project", "test_key", "test_value")
    value = sqlite_registry.get_project_metadata("test_project", "test_key")
    assert value == "test_value"

    sqlite_registry.delete_entity("test_entity", "test_project")
    with pytest.raises(Exception):
        sqlite_registry.get_entity("test_entity", "test_project")


def _build_feature_view(name: str, entity: Entity, source: FileSource) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
    )


def test_feature_view_name_conflict_between_stream_and_batch(sqlite_registry):
    entity = Entity(
        name="driver",
        value_type=ValueType.STRING,
        join_keys=["driver_id"],
    )
    sqlite_registry.apply_entity(entity, "test_project")

    file_source = FileSource(
        path="driver_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )

    batch_view = _build_feature_view("driver_activity", entity, file_source)
    sqlite_registry.apply_feature_view(batch_view, "test_project")

    push_source = PushSource(name="driver_push", batch_source=file_source)
    stream_view = StreamFeatureView(
        name="driver_activity",
        source=push_source,
        entities=[entity],
        schema=[Field(name="conv_rate", dtype=Float32)],
        timestamp_field="event_timestamp",
    )

    with pytest.raises(ConflictingFeatureViewNames):
        sqlite_registry.apply_feature_view(stream_view, "test_project")


def _serialize_udf_referencing_module(module_name: str) -> bytes:
    """Create a dill-serialized UDF that references a fake module.

    The function is defined inside a temporary module so that dill records
    a dependency on that module. After serialization, the module is removed
    from sys.modules so that deserializing the bytes will raise
    ModuleNotFoundError.
    """
    mod = types.ModuleType(module_name)
    mod.__package__ = module_name
    sys.modules[module_name] = mod
    exec("def _udf(x): return x", mod.__dict__)
    udf_bytes = dill.dumps(mod._udf)
    del sys.modules[module_name]
    return udf_bytes


def test_shared_registry_cross_project_udf_does_not_crash(shared_sqlite_db_path):
    """Initializing a SqlRegistry must not crash when another project in the
    same database has a feature view whose UDF references a module that is
    not installed in the current environment.

    Before the fix, proto() called from_proto() on every feature view across
    all projects, triggering dill.loads() which raised ModuleNotFoundError.
    """
    db_url = f"sqlite:///{shared_sqlite_db_path}"
    config = SqlRegistryConfig(
        registry_type="sql", path=db_url, purge_feast_metadata=False
    )

    registry_a = SqlRegistry(config, "project_a", None)

    entity = Entity(name="driver", join_keys=["driver_id"])
    registry_a.apply_entity(entity, "project_a")

    file_source = FileSource(
        path="driver_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    fv = FeatureView(
        name="driver_features",
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=file_source,
    )
    registry_a.apply_feature_view(fv, "project_a")

    # Inject a UDF body that references a non-existent module directly into
    # the DB, simulating a feature view from another project that uses a
    # module not available in this environment.
    fake_udf_bytes = _serialize_udf_referencing_module("nonexistent_project_module")

    fv_proto = fv.to_proto()
    fv_proto.spec.project = "project_a"
    fv_proto.spec.feature_transformation.CopyFrom(
        FeatureTransformationV2(
            user_defined_function=UserDefinedFunctionV2(
                name="fake_udf",
                body=fake_udf_bytes,
                body_text="def _udf(x): return x",
                mode="python",
            )
        )
    )
    with registry_a.write_engine.begin() as conn:
        from sqlalchemy import update

        stmt = (
            update(feature_views)
            .where(
                feature_views.c.feature_view_name == "driver_features",
                feature_views.c.project_id == "project_a",
            )
            .values(feature_view_proto=fv_proto.SerializeToString())
        )
        conn.execute(stmt)

    # Creating a new SqlRegistry for project_b against the same DB should
    # NOT crash even though project_a has a UDF referencing an unavailable
    # module. proto() should read raw protos without deserializing UDFs.
    registry_b = SqlRegistry(config, "project_b", None)

    entity_b = Entity(name="customer", join_keys=["customer_id"])
    registry_b.apply_entity(entity_b, "project_b")
    retrieved = registry_b.get_entity("customer", "project_b")
    assert retrieved.name == "customer"

    # Verify project_a's data is still accessible in the cached proto
    proto = registry_b.proto()
    project_names = [p.spec.name for p in proto.projects]
    assert "project_a" in project_names
    assert "project_b" in project_names

    fv_names = [fv.spec.name for fv in proto.feature_views]
    assert "driver_features" in fv_names

    registry_a.teardown()
