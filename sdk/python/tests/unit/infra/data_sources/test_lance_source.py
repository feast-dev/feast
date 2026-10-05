# Copyright 2026 The Feast Authors
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

"""Tests for the non-JVM Lance read path.

Every catalog-based test uses the ``dir`` Lance namespace implementation, which
is a local-filesystem namespace. It drives the identical ``namespace_client`` +
``table_id`` code path as a remote catalog, so no server is needed.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from feast.credentials import ConnectionRef
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.duckdb import (
    DuckDBOfflineStore,
    DuckDBOfflineStoreConfig,
    _read_data_source,
    _write_lance_data_source,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.table_format import LanceFormat, TableFormatType
from feast.types import Array, Float32, Int64, String

lance = pytest.importorskip("lance")
lance_namespace = pytest.importorskip("lance_namespace")

from feast.infra.data_sources.contrib.lance.lance_source import (  # noqa: E402
    LanceSource,
    validate_lance_source_schema,
    validate_lance_write_shape,
)

# --------------------------------------------------------------------- helpers


def _write(uri_or_ns, table, mode="create", table_id=None):
    if table_id is None:
        return lance.write_dataset(table, uri_or_ns, mode=mode)
    return lance.write_dataset(
        table, namespace_client=uri_or_ns, table_id=table_id, mode=mode
    )


@pytest.fixture
def dir_namespace(tmp_path):
    """A local-filesystem Lance namespace, standing in for a remote catalog.

    Returns a ``(client, properties)`` pair: the client is used to write the
    fixture data, and the properties are what a ``LanceSource`` is configured
    with so that it connects for itself.
    """
    root = tmp_path / "warehouse"
    root.mkdir()
    properties = {"root": str(root)}
    return lance_namespace.connect("dir", dict(properties)), properties


def _driver_stats(n_rows=3, base_hour=10, conv_rate_offset=0.0):
    return pa.table(
        {
            "driver_id": pa.array([1000 + i for i in range(n_rows)], pa.int64()),
            "conv_rate": pa.array(
                [0.1 * (i + 1) + conv_rate_offset for i in range(n_rows)], pa.float32()
            ),
            "event_timestamp": pa.array(
                [datetime(2026, 1, 1, base_hour, i) for i in range(n_rows)],
                pa.timestamp("us"),
            ),
        }
    )


def _feature_view(source, schema=None, name="driver_stats", ttl=timedelta(days=365)):
    fv = FeatureView(
        name=name,
        entities=[Entity(name="driver", join_keys=["driver_id"])],
        schema=schema
        if schema is not None
        else [Field(name="conv_rate", dtype=Float32)],
        source=source,
        ttl=ttl,
    )
    # ``feast apply`` infers these; set them directly so the test needs no registry.
    fv.entity_columns = [Field(name="driver_id", dtype=Int64)]
    return fv


def _repo_config(tmp_path):
    return RepoConfig(
        project="lance_test",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        offline_store=DuckDBOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


# ------------------------------------------------------- construction contract


def test_requires_an_address():
    with pytest.raises(ValueError, match="requires either uri .* or table"):
        LanceSource()


def test_rejects_both_uri_and_table():
    with pytest.raises(ValueError, match="either uri or table, not both"):
        LanceSource(uri="/tmp/x.lance", table="t", namespace_impl="dir")


def test_table_requires_a_namespace_impl():
    with pytest.raises(ValueError, match="requires namespace_impl"):
        LanceSource(table="t")


def test_rejects_a_non_lance_table_format():
    from feast.table_format import IcebergFormat

    with pytest.raises(ValueError, match="requires a LanceFormat"):
        LanceSource(uri="/tmp/x.lance", table_format=IcebergFormat())


def test_rejects_an_unimplemented_connection_ref():
    with pytest.raises(TypeError, match="unexpected keyword argument 'connection_ref'"):
        LanceSource(
            uri="s3://bucket/emb.lance",
            connection_ref=ConnectionRef(provider="env", name="LANCE"),
        )


def test_default_name_is_the_uri_for_a_path_based_source():
    assert LanceSource(uri="/tmp/x.lance").name == "/tmp/x.lance"


def test_default_name_is_the_fqn_for_a_catalog_based_source():
    source = LanceSource(
        table="emb",
        namespace_impl="dir",
        table_format=LanceFormat(catalog="cat", namespace="ns"),
    )
    assert source.name == "cat.ns.emb"


def test_table_id_splits_a_hierarchical_namespace():
    source = LanceSource(
        table="emb",
        namespace_impl="rest",
        table_format=LanceFormat(namespace="a.b"),
    )
    assert source.table_id == ["a", "b", "emb"]


def test_table_id_is_flat_without_a_namespace():
    source = LanceSource(table="emb", namespace_impl="dir")
    assert source.table_id == ["emb"]


def test_table_id_is_unavailable_for_a_path_based_source():
    with pytest.raises(ValueError, match="path-based and has no table_id"):
        _ = LanceSource(uri="/tmp/x.lance").table_id


# -------------------------------------------------------------- pin accessors


def test_pin_is_none_when_unpinned():
    source = LanceSource(uri="/tmp/x.lance")
    assert source.pin is None
    assert source.pin_description == "latest version"


def test_pin_is_the_int_version_when_pinned_by_version():
    source = LanceSource(uri="/tmp/x.lance", table_format=LanceFormat(version=2))
    assert source.pin == 2
    assert source.pin_description == "version 2"


def test_pin_is_the_tag_name_when_pinned_by_tag():
    # Lance accepts an int version or a tag name in the same ``version=`` arg.
    source = LanceSource(uri="/tmp/x.lance", table_format=LanceFormat(tag="candidate"))
    assert source.pin == "candidate"
    assert source.pin_description == "tag 'candidate'"


# ------------------------------------------------------------- proto roundtrip


def test_proto_roundtrip_path_based():
    source = LanceSource(
        uri="s3://bucket/emb.lance",
        table_format=LanceFormat(version=4),
        storage_options={"aws_region": "us-west-2"},
        timestamp_field="event_timestamp",
        created_timestamp_column="created_ts",
        field_mapping={"src": "dst"},
        description="d",
        tags={"t": "v"},
        owner="o",
    )
    restored = LanceSource.from_proto(source.to_proto())
    assert restored == source
    assert restored.uri == "s3://bucket/emb.lance"
    assert restored.storage_options == {"aws_region": "us-west-2"}
    assert restored.table_format.version == 4
    assert restored.table_format.format_type == TableFormatType.LANCE


def test_proto_roundtrip_catalog_based():
    source = LanceSource(
        table="emb",
        namespace_impl="dir",
        namespace_properties={"root": "/warehouse"},
        table_format=LanceFormat(catalog="cat", namespace="ns", tag="eval"),
        timestamp_field="event_timestamp",
    )
    restored = LanceSource.from_proto(source.to_proto())
    assert restored == source
    assert restored.table_id == ["ns", "emb"]
    assert restored.pin == "eval"
    assert restored.namespace_properties == {"root": "/warehouse"}


def test_proto_uses_custom_source_so_no_registry_edit_is_needed():
    proto = LanceSource(uri="/tmp/x.lance").to_proto()
    assert proto.type == DataSourceProto.CUSTOM_SOURCE
    assert proto.data_source_class_type.endswith("lance_source.LanceSource")


def test_generic_data_source_from_proto_dispatches_to_lance_source():
    from feast.data_source import DataSource

    source = LanceSource(uri="/tmp/x.lance", timestamp_field="event_timestamp")
    restored = DataSource.from_proto(source.to_proto())
    assert isinstance(restored, LanceSource)
    assert restored == source


# ----------------------------------------------------- layer 1: path-based read


def test_path_based_read(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    assert source.to_arrow().num_rows == 3


def test_path_based_read_projects_columns(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    assert source.to_arrow(columns=["driver_id"]).column_names == ["driver_id"]


def test_projection_validates_the_full_source_schema(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    feature_view = _feature_view(source)

    table = source.to_arrow(columns=["driver_id"], feature_view=feature_view)

    assert table.column_names == ["driver_id"]


def test_path_based_schema_inference(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    names_and_types = dict(source.get_table_column_names_and_types(MagicMock()))
    assert names_and_types["driver_id"] == "int64"
    assert names_and_types["conv_rate"] == "float"


def test_source_datatype_decoder_is_the_arrow_decoder():
    from feast.type_map import pa_to_feast_value_type
    from feast.value_type import ValueType

    decoder = LanceSource.source_datatype_to_feast_value_type()
    assert decoder is pa_to_feast_value_type
    assert decoder("int64") == ValueType.INT64


def test_validate_rejects_a_missing_timestamp_field(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    source = LanceSource(uri=uri, timestamp_field="nope")
    with pytest.raises(ValueError, match="Timestamp field 'nope' is not present"):
        source.validate(MagicMock())


def test_get_table_query_string_is_unsupported(tmp_path):
    with pytest.raises(NotImplementedError, match="read through PyArrow, not SQL"):
        LanceSource(uri="/tmp/x.lance").get_table_query_string()


# -------------------------------------------------- layer 2: catalog-based read


def test_catalog_based_read(dir_namespace):
    client, properties = dir_namespace
    _write(client, _driver_stats(), mode="create", table_id=["emb"])
    source = LanceSource(
        table="emb",
        namespace_impl="dir",
        namespace_properties=properties,
        timestamp_field="event_timestamp",
    )
    assert source.to_arrow().num_rows == 3


def test_catalog_based_read_without_a_server_uses_a_namespace_client(dir_namespace):
    client, properties = dir_namespace
    _write(client, _driver_stats(), mode="create", table_id=["emb"])
    source = LanceSource(
        table="emb",
        namespace_impl="dir",
        namespace_properties=properties,
    )
    # The source connects for itself, through the same namespace_client code
    # path a remote catalog would use.
    assert isinstance(source.get_namespace_client(), type(client))
    assert source.is_catalog_based is True


# ----------------------------------------------- the pin actually selects data


def _two_versions(dir_namespace):
    """Write v1, tag it, then append v2. Returns the dir-namespace properties."""
    client, properties = dir_namespace
    ds = _write(client, _driver_stats(n_rows=2), mode="create", table_id=["emb"])
    assert ds.version == 1
    ds.tags.create("eval_set", 1)
    ds2 = _write(
        client,
        _driver_stats(n_rows=2, base_hour=12, conv_rate_offset=10.0),
        mode="append",
        table_id=["emb"],
    )
    assert ds2.version == 2
    return properties


def test_a_tag_pin_changes_what_is_read(dir_namespace):
    """Write two versions, tag the first, show the pinned read sees the earlier data."""
    props = _two_versions(dir_namespace)

    def source(table_format):
        return LanceSource(
            table="emb",
            namespace_impl="dir",
            namespace_properties=props,
            table_format=table_format,
        )

    latest = source(LanceFormat()).to_arrow()
    pinned = source(LanceFormat(tag="eval_set")).to_arrow()

    # v1 had 2 rows; the append brought the latest to 4.
    assert latest.num_rows == 4
    assert pinned.num_rows == 2
    # And the pinned read sees the *original* values, not the appended ones.
    assert pinned.column("conv_rate").to_pylist() == pytest.approx([0.1, 0.2])
    assert max(latest.column("conv_rate").to_pylist()) == pytest.approx(10.2)


def test_a_version_pin_changes_what_is_read(dir_namespace):
    props = _two_versions(dir_namespace)
    pinned = LanceSource(
        table="emb",
        namespace_impl="dir",
        namespace_properties=props,
        table_format=LanceFormat(version=1),
    ).to_arrow()
    assert pinned.num_rows == 2


def test_a_tag_pin_and_the_version_it_names_read_identically(dir_namespace):
    props = _two_versions(dir_namespace)

    def read(table_format):
        return LanceSource(
            table="emb",
            namespace_impl="dir",
            namespace_properties=props,
            table_format=table_format,
        ).to_arrow()

    assert read(LanceFormat(tag="eval_set")) == read(LanceFormat(version=1))


def test_a_path_based_tag_pin_changes_what_is_read(tmp_path):
    uri = str(tmp_path / "emb.lance")
    ds = _write(uri, _driver_stats(n_rows=2))
    ds.tags.create("eval_set", 1)
    _write(uri, _driver_stats(n_rows=2, base_hour=12), mode="append")

    assert LanceSource(uri=uri).to_arrow().num_rows == 4
    pinned = LanceSource(uri=uri, table_format=LanceFormat(tag="eval_set")).to_arrow()
    assert pinned.num_rows == 2


def test_schema_inference_reflects_the_pin_not_the_latest_version(tmp_path):
    """``feast apply`` must infer the schema a pinned read will actually see."""
    uri = str(tmp_path / "emb.lance")
    ds = _write(uri, _driver_stats(n_rows=2))
    ds.tags.create("narrow", 1)
    wider = _driver_stats(n_rows=2).append_column("extra", pa.array([1, 2], pa.int64()))
    _write(uri, wider, mode="overwrite")

    latest = dict(LanceSource(uri=uri).get_table_column_names_and_types(MagicMock()))
    pinned = dict(
        LanceSource(
            uri=uri, table_format=LanceFormat(tag="narrow")
        ).get_table_column_names_and_types(MagicMock())
    )
    assert "extra" in latest
    assert "extra" not in pinned


# ----------------------------------- a pin selects data, never shape (contract)


def test_a_pin_missing_a_declared_column_fails_explicitly(tmp_path):
    uri = str(tmp_path / "emb.lance")
    ds = _write(uri, _driver_stats(n_rows=2))
    ds.tags.create("narrow", 1)
    wider = _driver_stats(n_rows=2).append_column(
        "acc_rate", pa.array([0.5, 0.6], pa.float32())
    )
    _write(uri, wider, mode="overwrite")

    # The declared schema asks for acc_rate, which only the latest version has.
    fv = _feature_view(
        LanceSource(
            uri=uri,
            table_format=LanceFormat(tag="narrow"),
            timestamp_field="event_timestamp",
        ),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
        ],
    )
    with pytest.raises(ValueError) as err:
        validate_lance_source_schema(fv)
    message = str(err.value)
    assert "acc_rate" in message
    assert "tag 'narrow'" in message
    assert "A pin selects data, not shape" in message


def test_an_unpinned_source_satisfying_the_declared_schema_validates(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    fv = _feature_view(LanceSource(uri=uri, timestamp_field="event_timestamp"))
    validate_lance_source_schema(fv)  # does not raise


def test_validation_honours_field_mapping(tmp_path):
    """A declared name that is mapped from a source column must not be reported missing."""
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    source = LanceSource(
        uri=uri,
        timestamp_field="event_timestamp",
        field_mapping={"conv_rate": "conversion_rate"},
    )
    fv = _feature_view(source, schema=[Field(name="conversion_rate", dtype=Float32)])
    validate_lance_source_schema(fv)  # does not raise


def test_validation_is_a_noop_for_a_non_lance_source(tmp_path):
    from feast.infra.offline_stores.file_source import FileSource

    fv = _feature_view(
        FileSource(path=str(tmp_path / "x.parquet"), timestamp_field="event_timestamp")
    )
    validate_lance_source_schema(fv)  # does not raise, and reads nothing


def _vector_table(width, declared_timestamp=True):
    cols = {
        "item_id": pa.array([1, 2], pa.int64()),
        "embedding": pa.FixedSizeListArray.from_arrays(
            pa.array([0.0] * (2 * width), pa.float32()), width
        ),
    }
    if declared_timestamp:
        cols["event_timestamp"] = pa.array(
            [datetime(2026, 1, 1, 10), datetime(2026, 1, 1, 11)], pa.timestamp("us")
        )
    return pa.table(cols)


def _vector_feature_view(source, declared_width):
    fv = FeatureView(
        name="item_emb",
        entities=[Entity(name="item", join_keys=["item_id"])],
        schema=[
            Field(
                name="embedding",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=declared_width,
            )
        ],
        source=source,
        ttl=timedelta(days=365),
    )
    fv.entity_columns = [Field(name="item_id", dtype=Int64)]
    return fv


def test_a_pin_with_a_different_vector_width_fails_explicitly(tmp_path):
    """The reused #6909 validator rejects a pinned width that is not the declared one."""
    uri = str(tmp_path / "emb.lance")
    ds = _write(uri, _vector_table(width=4))
    ds.tags.create("narrow", 1)
    _write(uri, _vector_table(width=8), mode="overwrite")

    pinned = LanceSource(
        uri=uri,
        table_format=LanceFormat(tag="narrow"),
        timestamp_field="event_timestamp",
    )
    # Declared width 8 matches the latest version but not the pinned one.
    with pytest.raises(ValueError, match="Vector length 4 does not match expected 8"):
        validate_lance_source_schema(_vector_feature_view(pinned, declared_width=8))

    # The same pin with the matching declared width is fine.
    validate_lance_source_schema(_vector_feature_view(pinned, declared_width=4))


def test_to_arrow_enforces_the_declared_schema_when_given_a_feature_view(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _vector_table(width=4))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    with pytest.raises(ValueError, match="Vector length 4 does not match expected 8"):
        source.to_arrow(feature_view=_vector_feature_view(source, declared_width=8))


# --------------------------------------------- DuckDB offline store integration


def test_duckdb_reader_dispatches_to_the_lance_reader(tmp_path):
    from feast.infra.offline_stores.duckdb import _read_data_source

    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats())
    table = _read_data_source(
        LanceSource(uri=uri, timestamp_field="event_timestamp"), str(tmp_path)
    )
    assert sorted(table.columns) == ["conv_rate", "driver_id", "event_timestamp"]
    assert table.to_pyarrow().num_rows == 3


def test_pull_latest_from_a_lance_source(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _driver_stats(n_rows=3))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    job = DuckDBOfflineStore.pull_latest_from_table_or_query(
        config=_repo_config(tmp_path),
        data_source=source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        start_date=datetime(2026, 1, 1),
        end_date=datetime(2026, 1, 2),
    )
    assert job.to_arrow().num_rows == 3


def _pit_fixture(tmp_path, namespace=None):
    """One driver with two feature rows, an hour apart.

    An entity query at 11:30 must see the 11:00 row, never the 12:00 one.
    """
    rows = pa.table(
        {
            "driver_id": pa.array([1000, 1000], pa.int64()),
            "conv_rate": pa.array([0.5, 0.9], pa.float32()),
            "event_timestamp": pa.array(
                [datetime(2026, 1, 1, 11), datetime(2026, 1, 1, 12)],
                pa.timestamp("us"),
            ),
        }
    )
    if namespace is not None:
        client, properties = namespace
        _write(client, rows, mode="create", table_id=["stats"])
        return LanceSource(
            table="stats",
            namespace_impl="dir",
            namespace_properties=properties,
            timestamp_field="event_timestamp",
        )
    uri = str(tmp_path / "stats.lance")
    _write(uri, rows)
    return LanceSource(uri=uri, timestamp_field="event_timestamp")


def _historical(tmp_path, source, entity_hour=11, minute=30):
    import pandas as pd

    entity_df = pd.DataFrame(
        {
            "driver_id": [1000],
            "event_timestamp": [datetime(2026, 1, 1, entity_hour, minute)],
        }
    )
    return DuckDBOfflineStore.get_historical_features(
        config=_repo_config(tmp_path),
        feature_views=[_feature_view(source)],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project="lance_test",
    ).to_df()


def test_get_historical_features_is_point_in_time_correct_path_based(tmp_path):
    df = _historical(tmp_path, _pit_fixture(tmp_path))
    assert len(df) == 1
    # 0.5 is the 11:00 row. 0.9 is the 12:00 row, which is in the future.
    assert df["conv_rate"].iloc[0] == pytest.approx(0.5)


def test_get_historical_features_is_point_in_time_correct_catalog_based(
    tmp_path, dir_namespace
):
    df = _historical(tmp_path, _pit_fixture(tmp_path, namespace=dir_namespace))
    assert len(df) == 1
    assert df["conv_rate"].iloc[0] == pytest.approx(0.5)


def test_get_historical_features_picks_the_later_row_for_a_later_entity_query(tmp_path):
    df = _historical(tmp_path, _pit_fixture(tmp_path), entity_hour=13)
    assert df["conv_rate"].iloc[0] == pytest.approx(0.9)


def test_get_historical_features_honours_a_tag_pin(tmp_path):
    """The motivating case: a tagged eval set retrieves the data it was built from."""
    uri = str(tmp_path / "stats.lance")
    v1 = pa.table(
        {
            "driver_id": pa.array([1000], pa.int64()),
            "conv_rate": pa.array([0.5], pa.float32()),
            "event_timestamp": pa.array([datetime(2026, 1, 1, 11)], pa.timestamp("us")),
        }
    )
    ds = _write(uri, v1)
    ds.tags.create("eval_set", 1)
    # A later, *corrected* value for the very same (entity, timestamp).
    v2 = pa.table(
        {
            "driver_id": pa.array([1000], pa.int64()),
            "conv_rate": pa.array([0.77], pa.float32()),
            "event_timestamp": pa.array([datetime(2026, 1, 1, 11)], pa.timestamp("us")),
        }
    )
    _write(uri, v2, mode="overwrite")

    latest = _historical(
        tmp_path, LanceSource(uri=uri, timestamp_field="event_timestamp")
    )
    pinned = _historical(
        tmp_path,
        LanceSource(
            uri=uri,
            table_format=LanceFormat(tag="eval_set"),
            timestamp_field="event_timestamp",
        ),
    )
    assert latest["conv_rate"].iloc[0] == pytest.approx(0.77)
    assert pinned["conv_rate"].iloc[0] == pytest.approx(0.5)


def test_get_historical_features_rejects_a_pin_that_breaks_the_declared_schema(
    tmp_path,
):
    uri = str(tmp_path / "stats.lance")
    ds = _write(uri, _driver_stats(n_rows=2))
    ds.tags.create("narrow", 1)
    _write(
        uri,
        _driver_stats(n_rows=2).append_column(
            "acc_rate", pa.array([0.5, 0.6], pa.float32())
        ),
        mode="overwrite",
    )
    source = LanceSource(
        uri=uri,
        table_format=LanceFormat(tag="narrow"),
        timestamp_field="event_timestamp",
    )
    fv = _feature_view(
        source,
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
        ],
    )
    import pandas as pd

    with pytest.raises(ValueError, match="A pin selects data, not shape"):
        DuckDBOfflineStore.get_historical_features(
            config=_repo_config(tmp_path),
            feature_views=[fv],
            feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
            entity_df=pd.DataFrame(
                {"driver_id": [1000], "event_timestamp": [datetime(2026, 1, 1, 13)]}
            ),
            registry=MagicMock(),
            project="lance_test",
        )


def test_a_string_column_survives_the_round_trip(tmp_path):
    """Lance keeps vectors and scalars in one dataset; check a non-numeric scalar."""
    uri = str(tmp_path / "emb.lance")
    _write(
        uri,
        pa.table(
            {
                "driver_id": pa.array([1000], pa.int64()),
                "city": pa.array(["cupertino"], pa.string()),
                "event_timestamp": pa.array(
                    [datetime(2026, 1, 1, 11)], pa.timestamp("us")
                ),
            }
        ),
    )
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    fv = _feature_view(source, schema=[Field(name="city", dtype=String)])
    validate_lance_source_schema(fv)
    assert source.to_arrow().column("city").to_pylist() == ["cupertino"]


# ------------------------------------------------------------- the write path


def _ibis_table(arrow_table):
    import ibis

    return ibis.memtable(arrow_table)


def test_get_existing_schema_is_none_for_an_absent_dataset(tmp_path):
    source = LanceSource(uri=str(tmp_path / "absent.lance"))
    assert source.get_existing_schema() is None


def test_get_write_target_omits_the_version(tmp_path):
    """``write_dataset`` has no ``version``; a write always makes a new one."""
    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats())
    source = LanceSource(uri=uri, table_format=LanceFormat(version=1))
    assert "version" not in source.get_write_target()


def test_write_creates_an_absent_path_based_dataset(tmp_path):
    uri = str(tmp_path / "new.lance")
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    _write_lance_data_source(_ibis_table(_driver_stats(n_rows=3)), source)

    assert lance.dataset(uri).count_rows() == 3
    assert lance.dataset(uri).version == 1


def test_write_appends_to_an_existing_path_based_dataset(tmp_path):
    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats(n_rows=3))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    _write_lance_data_source(_ibis_table(_driver_stats(n_rows=2, base_hour=12)), source)

    dataset = lance.dataset(uri)
    assert dataset.count_rows() == 5
    assert dataset.version == 2


def test_write_creates_an_absent_catalog_based_dataset(dir_namespace):
    _, properties = dir_namespace
    source = LanceSource(
        table="brand_new",
        namespace_impl="dir",
        namespace_properties=properties,
        timestamp_field="event_timestamp",
    )

    _write_lance_data_source(_ibis_table(_driver_stats(n_rows=4)), source)

    assert source.to_arrow().num_rows == 4


def test_write_appends_to_an_existing_catalog_based_dataset(dir_namespace):
    client, properties = dir_namespace
    _write(client, _driver_stats(n_rows=3), table_id=["driver"])
    source = LanceSource(
        table="driver",
        namespace_impl="dir",
        namespace_properties=properties,
        timestamp_field="event_timestamp",
    )

    _write_lance_data_source(_ibis_table(_driver_stats(n_rows=2, base_hour=12)), source)

    assert source.to_arrow().num_rows == 5


def test_write_through_a_version_pin_is_refused(tmp_path):
    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats())
    source = LanceSource(uri=uri, table_format=LanceFormat(version=1))

    with pytest.raises(ValueError, match="pinned to version 1"):
        _write_lance_data_source(_ibis_table(_driver_stats()), source)


def test_write_through_a_tag_pin_is_refused(tmp_path):
    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats())
    lance.dataset(uri).tags.create("prod", 1)
    source = LanceSource(uri=uri, table_format=LanceFormat(tag="prod"))

    with pytest.raises(ValueError, match="pinned to tag 'prod'"):
        _write_lance_data_source(_ibis_table(_driver_stats()), source)


def test_a_refused_pinned_write_leaves_the_dataset_untouched(tmp_path):
    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats(n_rows=3))
    source = LanceSource(uri=uri, table_format=LanceFormat(version=1))

    with pytest.raises(ValueError):
        _write_lance_data_source(_ibis_table(_driver_stats(n_rows=9)), source)

    assert lance.dataset(uri).version == 1
    assert lance.dataset(uri).count_rows() == 3


def test_an_append_is_invisible_to_an_earlier_pin(tmp_path):
    """Why a pinned write is refused: the pin would not show the write."""
    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats(n_rows=3))
    unpinned = LanceSource(uri=uri, timestamp_field="event_timestamp")
    pinned = LanceSource(
        uri=uri, table_format=LanceFormat(version=1), timestamp_field="event_timestamp"
    )

    _write_lance_data_source(
        _ibis_table(_driver_stats(n_rows=2, base_hour=12)), unpinned
    )

    assert unpinned.to_arrow().num_rows == 5
    assert pinned.to_arrow().num_rows == 3


def test_overwrite_without_allow_overwrite_is_refused(tmp_path):
    from feast.errors import SavedDatasetLocationAlreadyExists

    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats(n_rows=3))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    with pytest.raises(SavedDatasetLocationAlreadyExists):
        _write_lance_data_source(_ibis_table(_driver_stats()), source, mode="overwrite")
    assert lance.dataset(uri).count_rows() == 3


def test_overwrite_with_allow_overwrite_replaces_the_data(tmp_path):
    uri = str(tmp_path / "d.lance")
    _write(uri, _driver_stats(n_rows=5))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    _write_lance_data_source(
        _ibis_table(_driver_stats(n_rows=2)),
        source,
        mode="overwrite",
        allow_overwrite=True,
    )

    assert lance.dataset(uri).count_rows() == 2


def test_overwrite_changing_a_vector_width_is_refused(tmp_path):
    """Lance permits this silently; Feast must not.

    An ``overwrite`` that replaces a 8-wide embedding column with a 32-wide one
    succeeds in Lance and leaves a version history whose vectors cannot be
    compared with each other, invalidating every index built on the old width.
    """
    uri = str(tmp_path / "emb.lance")
    _write(uri, _vector_table(8))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    with pytest.raises(ValueError, match="would change the width"):
        _write_lance_data_source(
            _ibis_table(_vector_table(32)),
            source,
            mode="overwrite",
            allow_overwrite=True,
        )

    assert lance.dataset(uri).schema.field("embedding").type.list_size == 8
    assert lance.dataset(uri).version == 1


def test_overwrite_keeping_the_vector_width_is_allowed(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _vector_table(8))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    _write_lance_data_source(
        _ibis_table(_vector_table(8)), source, mode="overwrite", allow_overwrite=True
    )

    assert lance.dataset(uri).schema.field("embedding").type.list_size == 8
    assert lance.dataset(uri).version == 2


def test_a_non_vector_type_change_is_left_to_lance(tmp_path):
    """The width guard is narrow on purpose: only vectors are policed."""
    uri = str(tmp_path / "d.lance")
    _write(uri, pa.table({"a": pa.array([1], pa.int64())}))
    source = LanceSource(uri=uri)

    _write_lance_data_source(
        _ibis_table(pa.table({"a": pa.array(["x"], pa.string())})),
        source,
        mode="overwrite",
        allow_overwrite=True,
    )

    assert lance.dataset(uri).schema.field("a").type == pa.string()


def test_a_width_guard_ignores_a_column_absent_from_the_write(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _vector_table(8))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    validate_lance_write_shape(
        pa.table({"item_id": pa.array([1], pa.int64())}),
        source,
        lance.dataset(uri).schema,
    )


def test_write_then_read_round_trips_through_the_offline_store(tmp_path):
    uri = str(tmp_path / "d.lance")
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")

    _write_lance_data_source(_ibis_table(_driver_stats(n_rows=3)), source)
    read_back = _read_data_source(source, str(tmp_path))

    assert read_back.to_pyarrow().num_rows == 3


def test_offline_write_batch_rejects_a_width_other_than_the_declared_one(tmp_path):
    """The declared vector_length is enforced at the write, not at the next read.

    Checked at the store entry point because the writer callback is handed a
    DataSource and cannot see the declared schema.
    """
    uri = str(tmp_path / "emb.lance")
    _write(uri, _vector_table(8))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    fv = _vector_feature_view(source, declared_width=8)

    with pytest.raises(ValueError):
        DuckDBOfflineStore.offline_write_batch(
            _repo_config(tmp_path), fv, _vector_table(32), None
        )

    assert lance.dataset(uri).count_rows() == 2


def test_offline_write_batch_appends_a_declared_width_batch(tmp_path):
    uri = str(tmp_path / "emb.lance")
    _write(uri, _vector_table(8))
    source = LanceSource(uri=uri, timestamp_field="event_timestamp")
    fv = _vector_feature_view(source, declared_width=8)

    DuckDBOfflineStore.offline_write_batch(
        _repo_config(tmp_path), fv, _vector_table(8), None
    )

    assert lance.dataset(uri).count_rows() == 4
    assert lance.dataset(uri).version == 2
