from datetime import datetime

import pandas as pd
import pyarrow
import pytest

from feast import utils
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.offline_store import (
    RetrievalJob,
    _apply_default_values,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import NULL as NULL_PROTO
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float64, Int64


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


def test_online_and_historical_agree_on_missing_value():
    field = Field(name="count", dtype=Int64, default_value=0)
    view = _feature_view([field])

    online = _populate(view, ["count"], [(None, None)]).results[0].values[0].int64_val

    historical_table = _apply_default_values(
        pyarrow.table({"count": pyarrow.array([None], type=pyarrow.int64())}),
        utils.get_default_values_by_column([(view, ["count"])], False),
    )
    historical = historical_table.column("count").to_pylist()[0]

    assert online == historical == 0


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


def test_historical_default_survives_to_df():
    table = pyarrow.table({"count": pyarrow.array([None, 5], type=pyarrow.int64())})
    job = _FakeRetrievalJob(table, [])
    job._feature_default_values = {"count": 0}

    assert job.to_df()["count"].tolist() == [0, 5]
    assert isinstance(job.to_df(), pd.DataFrame)
