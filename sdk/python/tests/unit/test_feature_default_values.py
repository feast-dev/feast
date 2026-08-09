from datetime import datetime

import pyarrow
import pytest

from feast import utils
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.offline_store import (
    RetrievalJob,
    _apply_default_values,
    to_sql_literal,
)
from feast.infra.offline_stores.offline_utils import build_final_output_expressions
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import NULL as NULL_PROTO
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Float64, Int64


def _feature_view(fields):
    return FeatureView(
        name="driver_stats",
        entities=[],
        schema=fields,
        source=FileSource(path="dummy.parquet", timestamp_field="event_timestamp"),
    )


def _populate(view, requested_features, read_rows, output_len=1):
    response = GetOnlineFeaturesResponse(results=[])
    utils._populate_response_from_feature_data(
        requested_features=requested_features,
        read_rows=read_rows,
        indexes=[[i] for i in range(len(read_rows))],
        online_features_response=response,
        full_feature_names=False,
        table=view,
        output_len=output_len,
    )
    return response


def test_online_missing_row_gets_default_and_keeps_not_found():
    view = _feature_view([Field(name="count", dtype=Int64, default_value=0)])

    response = _populate(view, ["count"], [(None, None)])

    vector = response.results[0]
    # WhichOneof, not just the value: an unset proto also reports int64_val == 0.
    assert vector.values[0].WhichOneof("val") == "int64_val"
    assert vector.values[0].int64_val == 0
    assert vector.statuses[0] == FieldStatus.NOT_FOUND


def test_online_present_value_is_untouched():
    view = _feature_view([Field(name="count", dtype=Int64, default_value=0)])
    row = (datetime(2025, 1, 1), {"count": ValueProto(int64_val=7)})

    response = _populate(view, ["count"], [row])

    vector = response.results[0]
    assert vector.values[0].int64_val == 7
    assert vector.statuses[0] == FieldStatus.PRESENT


def test_online_null_value_on_existing_row_gets_default():
    view = _feature_view([Field(name="count", dtype=Int64, default_value=0)])
    row = (datetime(2025, 1, 1), {"count": ValueProto()})

    response = _populate(view, ["count"], [row])

    assert response.results[0].values[0].WhichOneof("val") == "int64_val"
    assert response.results[0].values[0].int64_val == 0


def test_online_explicit_null_val_gets_default():
    """Remote and proto-JSON paths encode null as null_val, not as an unset Value."""
    view = _feature_view([Field(name="count", dtype=Int64, default_value=0)])
    row = (datetime(2025, 1, 1), {"count": ValueProto(null_val=NULL_PROTO)})

    response = _populate(view, ["count"], [row])

    assert response.results[0].values[0].WhichOneof("val") == "int64_val"
    assert response.results[0].values[0].int64_val == 0


def test_online_without_default_stays_null():
    view = _feature_view([Field(name="count", dtype=Int64)])

    response = _populate(view, ["count"], [(None, None)])

    vector = response.results[0]
    assert vector.values[0].WhichOneof("val") is None
    assert vector.statuses[0] == FieldStatus.NOT_FOUND


def test_online_defaults_only_fill_missing_positions():
    view = _feature_view([Field(name="count", dtype=Int64, default_value=0)])
    rows = [
        (datetime(2025, 1, 1), {"count": ValueProto(int64_val=7)}),
        (None, None),
    ]

    response = _populate(view, ["count"], rows, output_len=2)

    vector = response.results[0]
    assert [v.WhichOneof("val") for v in vector.values] == ["int64_val"] * 2
    assert [v.int64_val for v in vector.values] == [7, 0]
    assert list(vector.statuses) == [FieldStatus.PRESENT, FieldStatus.NOT_FOUND]


def test_apply_default_values_fills_only_nulls():
    table = pyarrow.table({"count": pyarrow.array([1, None, 3], type=pyarrow.int64())})

    filled = _apply_default_values(table, {"count": 0})

    assert filled.column("count").to_pylist() == [1, 0, 3]


def test_apply_default_values_ignores_unknown_and_undeclared_columns():
    table = pyarrow.table({"count": pyarrow.array([None], type=pyarrow.int64())})

    assert _apply_default_values(table, {}).column("count").to_pylist() == [None]
    assert _apply_default_values(table, {"absent": 0}).column("count").to_pylist() == [
        None
    ]


@pytest.mark.parametrize(
    "column,default_value,expected",
    [
        # The offline store picks the physical type, so it need not match the dtype.
        (pyarrow.array([None, None], type=pyarrow.null()), 0, [0, 0]),
        (pyarrow.array([None, 5], type=pyarrow.int32()), 2**40, [2**40, 5]),
    ],
)
def test_apply_default_values_handles_mismatched_column_types(
    column, default_value, expected
):
    table = pyarrow.table({"count": column})

    filled = _apply_default_values(table, {"count": default_value})

    assert filled.column("count").to_pylist() == expected


@pytest.mark.parametrize(
    "full_feature_names,expected_column",
    [(False, "count"), (True, "driver_stats__count")],
)
def test_default_values_by_column_respects_full_feature_names(
    full_feature_names, expected_column
):
    view = _feature_view(
        [
            Field(name="count", dtype=Int64, default_value=0),
            Field(name="rate", dtype=Float64),
        ]
    )

    defaults = utils.get_default_values_by_column(
        [(view, ["count", "rate"])], full_feature_names
    )

    assert defaults == {expected_column: 0}


def test_default_values_by_column_skips_unrequested_fields():
    view = _feature_view([Field(name="count", dtype=Int64, default_value=0)])

    assert utils.get_default_values_by_column([(view, [])], False) == {}


class _FakeRetrievalJob(RetrievalJob):
    """Returns a fixed table so to_arrow's post-processing can be exercised."""

    def __init__(self, table, on_demand_feature_views):
        self._table = table
        self._odfvs = on_demand_feature_views

    def _to_arrow_internal(self, timeout=None):
        return self._table

    @property
    def full_feature_names(self):
        return False

    @property
    def on_demand_feature_views(self):
        return self._odfvs

    @property
    def metadata(self):
        return None


def test_historical_defaults_are_applied_before_odfv_runs():
    """The ordering guarantee: a transformation must see the default, not the null."""
    seen = {}

    class _Projection:
        features: list = []

        def name_to_use(self):
            return "derived"

    class _RecordingODFV:
        name = "derived"
        projection = _Projection()

        def transform_arrow(self, table, full_feature_names):
            seen["count"] = table.column("count").to_pylist()
            counts = table.column("count").to_pylist()
            return pyarrow.table({"count_plus_10": [c + 10 for c in counts]})

    table = pyarrow.table({"count": pyarrow.array([None, 5], type=pyarrow.int64())})
    job = _FakeRetrievalJob(table, [_RecordingODFV()])
    job._feature_default_values = {"count": 0}

    result = job.to_arrow()

    assert seen["count"] == [0, 5]
    assert result.column("count_plus_10").to_pylist() == [10, 15]


def test_historical_without_defaults_is_unchanged():
    table = pyarrow.table({"count": pyarrow.array([None, 5], type=pyarrow.int64())})

    result = _FakeRetrievalJob(table, []).to_arrow()

    assert result.column("count").to_pylist() == [None, 5]


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, "0"),
        (-1, "-1"),
        (3.5, "3.5"),
        (True, "TRUE"),
        (False, "FALSE"),
        ("unknown", "'unknown'"),
        # No escaping is portable. BigQuery rejects the SQL-standard '' form, reading
        # 'O''Brien' as two adjacent literals, and backslashes are literal in Trino.
        # Anything needing an escape is filled in Python rather than pushed down.
        ("O'Brien", None),
        ('say "hi"', None),
        ("back\\slash", None),
        ("two\nlines", None),
        (float("nan"), None),
        ([1, 2], None),
        ({"a": 1}, None),
    ],
)
def test_to_sql_literal(value, expected):
    assert to_sql_literal(value) == expected


def test_build_final_output_expressions_coalesces_only_defaulted_columns():
    expressions = build_final_output_expressions(
        ["driver_id", "conv_rate"], {"conv_rate": "0.0"}, quote_char="`"
    )

    assert expressions == ["`driver_id`", "COALESCE(`conv_rate`, 0.0) AS `conv_rate`"]


def test_build_final_output_expressions_without_defaults_is_just_quoting():
    """The no-defaults query must stay exactly what it was before pushdown existed."""
    assert build_final_output_expressions(["a", "b"], {}, quote_char='"') == [
        '"a"',
        '"b"',
    ]
    assert build_final_output_expressions(["a", "b"], {}) == ["a", "b"]


def test_pushdown_store_skips_the_client_round_trip():
    table = pyarrow.table({"count": pyarrow.array([None], type=pyarrow.int64())})

    job = _FakeRetrievalJob(table, [])
    job._defaults_applied_in_query = True
    job._feature_default_values = {"count": 0}
    assert job._requires_python_post_processing is False

    # A default the query cannot express still has to be filled in Python.
    job._feature_default_values = {"count": [1, 2]}
    assert job._requires_python_post_processing is True


def test_requires_python_post_processing_tracks_defaults_and_odfvs():
    """Warehouse stores export server-side unless this says otherwise."""
    table = pyarrow.table({"count": pyarrow.array([None], type=pyarrow.int64())})

    plain = _FakeRetrievalJob(table, [])
    assert plain._requires_python_post_processing is False

    with_odfv = _FakeRetrievalJob(table, [object()])
    assert with_odfv._requires_python_post_processing is True

    with_default = _FakeRetrievalJob(table, [])
    with_default._feature_default_values = {"count": 0}
    assert with_default._requires_python_post_processing is True


def test_conflicting_defaults_for_one_output_column_raise():
    """Without full_feature_names two views can collide on a feature name."""
    a = _feature_view([Field(name="count", dtype=Int64, default_value=0)])
    b = _feature_view([Field(name="count", dtype=Int64, default_value=9)])

    with pytest.raises(ValueError, match="Conflicting default values"):
        utils.get_default_values_by_column([(a, ["count"]), (b, ["count"])], False)

    # The same default from both views is not a conflict.
    same = _feature_view([Field(name="count", dtype=Int64, default_value=0)])
    assert utils.get_default_values_by_column(
        [(a, ["count"]), (same, ["count"])], False
    )


def test_mutating_the_caller_s_default_does_not_drift_from_the_proto():
    original = [1, 2]
    field = Field(name="f", dtype=Array(Int64), default_value=original)

    original.append(3)

    assert field.default_value == [1, 2]
    assert Field.from_proto(field.to_proto()).default_value == [1, 2]


def test_query_context_can_be_subclassed_with_required_fields():
    """Spark subclasses FeatureViewQueryContext and adds its own fields."""
    from dataclasses import dataclass
    from typing import Optional

    from feast.infra.offline_stores.offline_utils import FeatureViewQueryContext

    @dataclass(frozen=True)
    class _SubContext(FeatureViewQueryContext):
        min_date_partition: Optional[str] = None

    assert _SubContext.__dataclass_fields__["min_date_partition"] is not None


def test_unrequested_feature_default_does_not_leak_into_the_query():
    """A default on a feature nobody asked for must not COALESCE a same-named column."""
    from feast.infra.offline_stores.offline_utils import FeatureViewQueryContext

    context = FeatureViewQueryContext(
        name="driver_stats",
        ttl=0,
        entities=["driver_id"],
        features=["conv_rate"],
        field_mapping={},
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        table_subquery="t",
        entity_selections=["driver_id AS driver_id"],
        min_event_timestamp=None,
        max_event_timestamp="2025-01-02T00:00:00",
        date_partition_column=None,
        timestamp_field_type=None,
        feature_defaults={"age": 0},
    )

    assert "age" not in context.feature_defaults or "conv_rate" in context.features


def test_long_string_default_is_not_pushed_down():
    assert to_sql_literal("x" * 100) is not None
    assert to_sql_literal("x" * 100_000) is None


@pytest.mark.parametrize(
    "dtype,value",
    [(Int64, 0), (Float64, 0.1), (Array(Int64), [1, 2])],
)
def test_field_equals_its_own_round_trip(dtype, value):
    """Otherwise every apply would register a schema change."""
    field = Field(name="f", dtype=dtype, default_value=value)

    assert Field.from_proto(field.to_proto()) == field


def test_odfv_output_default_is_applied_historically():
    """A transform returning null still yields the field's declared default."""

    class _Projection:
        def name_to_use(self):
            return "derived"

    class _NullReturningODFV:
        name = "derived"
        projection = _Projection()

        def transform_arrow(self, table, full_feature_names):
            return pyarrow.table(
                {"derived_count": pyarrow.array([None, 5], type=pyarrow.int64())}
            )

    odfv = _NullReturningODFV()
    odfv.projection.features = [
        Field(name="derived_count", dtype=Int64, default_value=-1)
    ]

    table = pyarrow.table({"count": pyarrow.array([1, 2], type=pyarrow.int64())})
    result = _FakeRetrievalJob(table, [odfv]).to_arrow()

    assert result.column("derived_count").to_pylist() == [-1, 5]


def test_odfv_output_without_default_stays_null():
    class _Projection:
        def name_to_use(self):
            return "derived"

    class _NullReturningODFV:
        name = "derived"
        projection = _Projection()

        def transform_arrow(self, table, full_feature_names):
            return pyarrow.table(
                {"derived_count": pyarrow.array([None, 5], type=pyarrow.int64())}
            )

    odfv = _NullReturningODFV()
    odfv.projection.features = [Field(name="derived_count", dtype=Int64)]

    table = pyarrow.table({"count": pyarrow.array([1, 2], type=pyarrow.int64())})
    result = _FakeRetrievalJob(table, [odfv]).to_arrow()

    assert result.column("derived_count").to_pylist() == [None, 5]


def test_odfv_field_default_survives_registry_round_trip():
    """Without this the ODFV output default silently vanishes on reload."""
    from feast.on_demand_feature_view import OnDemandFeatureView
    from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
        OnDemandFeatureView as OnDemandFeatureViewProto,
    )
    from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
        OnDemandFeatureViewSpec,
    )

    spec = OnDemandFeatureViewSpec(name="odfv", project="p")
    spec.features.append(Field(name="a", dtype=Int64, default_value=-1).to_proto())
    spec.features.append(Field(name="b", dtype=Int64).to_proto())

    parsed = OnDemandFeatureView._parse_features_from_proto(
        OnDemandFeatureViewProto(spec=spec)
    )

    assert parsed[0].default_value == -1
    assert parsed[1].default_value is None
