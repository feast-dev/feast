from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from trino.exceptions import TrinoQueryError

from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    TrinoOfflineStoreConfig,
    TrinoRetrievalJob,
    _complex_column_depth,
    _stringify_complex,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import (
    QueryStatus,
    Trino,
)

COLUMNS = [
    {"name": "id", "type": "bigint"},
    {"name": "amount", "type": "decimal(10, 2)"},
    {"name": "attrs", "type": "map(varchar, varchar)"},
    {"name": "ts", "type": "timestamp(3)"},
    {"name": "ts_tz", "type": "timestamp(3) with time zone"},
]
T1 = datetime(2024, 1, 1, 12, 0, 0)
T1_TZ = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
PAGES = [
    [[1, Decimal("1.50"), {"a": "b"}, T1, T1_TZ], [2, None, None, T1, T1_TZ]],
    [[3, Decimal("2.00"), {"c": "d"}, T1, T1_TZ]],
]


@pytest.fixture(autouse=True)
def _no_signal_handlers():
    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino_queries.signal.signal"
    ):
        yield


def _job(pages=PAGES, batch_size=2, odfvs=None):
    cursor = MagicMock()
    cursor._query.columns = COLUMNS
    cursor.fetchmany.side_effect = [*pages, []]
    client = Trino(
        host="h",
        port=1,
        user="u",
        catalog="c",
        source=None,
        http_scheme="http",
        verify=False,
        extra_credential=None,
        auth=None,
    )
    client._cursor = cursor
    config = MagicMock()
    config.offline_store.streaming_batch_size = batch_size
    job = TrinoRetrievalJob(
        query="SELECT 1",
        client=client,
        config=config,
        full_feature_names=False,
        on_demand_feature_views=odfvs,
    )
    return job, cursor


def test_to_arrow_reader_pages_without_fetchall():
    job, cursor = _job()

    table = job.to_arrow_reader().read_all()

    cursor.fetchall.assert_not_called()
    cursor.fetchmany.assert_called_with(2)
    cursor.close.assert_called()
    assert table.num_rows == 3
    assert table.schema.field("amount").type == pa.decimal128(10, 2)
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("amount").to_pylist() == [
        Decimal("1.50"),
        None,
        Decimal("2.00"),
    ]
    assert table.column("attrs").to_pylist() == [str({"a": "b"}), None, str({"c": "d"})]
    assert table.column("ts").to_pylist() == [T1, T1, T1]
    assert table.schema.field("ts").type == pa.timestamp("us")
    assert table.schema.field("ts_tz").type == pa.timestamp("us", tz="UTC")
    assert table.column("ts_tz").null_count == 0


def test_to_arrow_reader_rejects_non_positive_batch_size():
    job, _ = _job()
    with pytest.raises(ValueError, match="positive"):
        job.to_arrow_reader(batch_size=0)


def test_to_arrow_reader_falls_back_to_materialized_path_with_odfvs():
    job, cursor = _job(odfvs=[MagicMock()])
    with patch.object(
        TrinoRetrievalJob, "to_arrow", return_value=pa.table({"id": [1]})
    ) as to_arrow:
        reader = job.to_arrow_reader()
    to_arrow.assert_called_once()
    cursor.fetchmany.assert_not_called()
    assert reader.read_all().num_rows == 1


def test_to_arrow_reader_closes_cursor_when_query_fails():
    job, cursor = _job()
    cursor.execute.side_effect = RuntimeError("syntax error")
    with pytest.raises(RuntimeError, match="syntax error"):
        job.to_arrow_reader()
    cursor.close.assert_called()


def test_to_arrow_reader_default_batch_size_reads_from_config():
    job, cursor = _job(batch_size=1)

    job.to_arrow_reader().read_all()

    cursor.fetchmany.assert_any_call(1)


def test_to_arrow_reader_explicit_batch_size_overrides_config():
    job, cursor = _job(batch_size=99)

    job.to_arrow_reader(batch_size=2).read_all()

    cursor.fetchmany.assert_any_call(2)


def test_to_arrow_reader_rejects_negative_batch_size():
    job, _ = _job()
    with pytest.raises(ValueError, match="positive"):
        job.to_arrow_reader(batch_size=-1)


def test_to_arrow_reader_records_success_metrics():
    job, _ = _job()
    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._emit_offline_store_request_metrics"
    ) as emit:
        job.to_arrow_reader().read_all()

    emit.assert_called_once()
    kwargs = emit.call_args.kwargs
    assert kwargs["method"] == "to_arrow_reader"
    assert kwargs["status_label"] == "success"
    assert kwargs["row_count"] == 3


def test_to_arrow_reader_records_elapsed_as_end_minus_start_on_success():
    """`time.monotonic() - start_wall` must stay subtraction: 10.0/24.0 make
    every other binary operator (add=34, mul=240, mod=4.0, div=2.4,
    floordiv=2) disagree with the correct 14.0, unlike a real (small)
    elapsed time which the plain "was called" checks above can't catch."""
    job, _ = _job()
    with (
        patch(
            "feast.infra.offline_stores.contrib.trino_offline_store.trino._emit_offline_store_request_metrics"
        ) as emit,
        patch(
            "feast.infra.offline_stores.contrib.trino_offline_store.trino.time.monotonic",
            side_effect=[10.0, 24.0],
        ),
    ):
        job.to_arrow_reader().read_all()

    assert emit.call_args.kwargs["elapsed"] == pytest.approx(14.0)


def test_to_arrow_reader_records_error_metrics_when_query_fails_to_start():
    job, cursor = _job()
    cursor.execute.side_effect = RuntimeError("boom")
    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._emit_offline_store_request_metrics"
    ) as emit:
        with pytest.raises(RuntimeError):
            job.to_arrow_reader()

    emit.assert_called_once()
    kwargs = emit.call_args.kwargs
    assert kwargs["status_label"] == "error"
    assert kwargs["row_count"] == 0


def test_to_arrow_reader_records_elapsed_when_query_fails_to_start():
    job, cursor = _job()
    cursor.execute.side_effect = RuntimeError("boom")
    with (
        patch(
            "feast.infra.offline_stores.contrib.trino_offline_store.trino._emit_offline_store_request_metrics"
        ) as emit,
        patch(
            "feast.infra.offline_stores.contrib.trino_offline_store.trino.time.monotonic",
            side_effect=[10.0, 24.0],
        ),
    ):
        with pytest.raises(RuntimeError):
            job.to_arrow_reader()

    assert emit.call_args.kwargs["elapsed"] == pytest.approx(14.0)


def test_to_arrow_reader_records_error_metrics_when_streaming_fails_midway():
    job, cursor = _job()
    cursor.fetchmany.side_effect = [PAGES[0], RuntimeError("stream broke")]
    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._emit_offline_store_request_metrics"
    ) as emit:
        with pytest.raises(RuntimeError, match="stream broke"):
            list(job.to_arrow_reader())

    emit.assert_called_once()
    kwargs = emit.call_args.kwargs
    assert kwargs["status_label"] == "error"
    assert kwargs["row_count"] == 2


def test_to_arrow_reader_drops_temp_table_after_streaming():
    job, cursor = _job()
    job._temp_table = "cat.ds.tmp_entities"
    with patch.object(job, "_drop_temp_table") as drop:
        job.to_arrow_reader().read_all()
    drop.assert_called_once()


def _trino_query_error() -> TrinoQueryError:
    return TrinoQueryError(
        error={
            "message": "boom",
            "errorCode": 1,
            "errorName": "GENERIC_ERROR",
            "errorType": "USER_ERROR",
        }
    )


class TestQueryStartAndIteratePages:
    """Direct tests for `Query.start()`/`Query.iterate_pages()`, independent of
    `TrinoRetrievalJob`, so the TrinoQueryError branch (never hit through the
    job-level tests, which only ever raise RuntimeError) is exercised."""

    def _query(self, pages):
        cursor = MagicMock()
        cursor._query.columns = COLUMNS
        cursor.fetchmany.side_effect = pages
        client = Trino(
            host="h",
            port=1,
            user="u",
            catalog="c",
            source=None,
            http_scheme="http",
            verify=False,
            extra_credential=None,
            auth=None,
        )
        client._cursor = cursor
        return client.create_query("SELECT 1"), cursor

    def test_start_returns_columns_and_sets_running_status(self):
        query, cursor = self._query(pages=[[]])
        columns = query.start()
        assert columns == COLUMNS
        assert query.status == QueryStatus.RUNNING
        cursor.execute.assert_called_once_with(operation="SELECT 1")

    def test_iterate_pages_completes_and_closes_cursor(self):
        query, cursor = self._query(pages=[PAGES[0], []])
        query.start()

        pages = list(query.iterate_pages(2))

        assert len(pages) == 1
        assert pages[0].data == PAGES[0]
        assert pages[0].columns == COLUMNS
        assert query.status == QueryStatus.COMPLETED
        assert query.execution_time is not None
        cursor.close.assert_called_once()

    def test_iterate_pages_yields_one_page_per_fetchmany_call(self):
        query, cursor = self._query(pages=[*PAGES, []])
        query.start()

        pages = list(query.iterate_pages(2))

        assert [p.data for p in pages] == PAGES
        assert cursor.fetchmany.call_count == 3  # two pages + the empty terminator

    def test_iterate_pages_sets_error_status_recloses_and_reraises(self):
        error = _trino_query_error()
        query, cursor = self._query(pages=[error])
        query.start()

        with pytest.raises(TrinoQueryError):
            list(query.iterate_pages(2))

        assert query.status == QueryStatus.ERROR
        cursor.close.assert_called_once()

    def test_iterate_pages_empty_result_terminates_immediately(self):
        """fetchmany's first call already returns [] -- the loop must end via
        a finite side_effect, not hang on a mutant that turns `break` into
        `continue` (StopIteration surfaces as a RuntimeError, killing it)."""
        query, cursor = self._query(pages=[[]])
        query.start()

        pages = list(query.iterate_pages(2))

        assert pages == []
        assert query.status == QueryStatus.COMPLETED
        cursor.close.assert_called_once()


class TestComplexColumnDepth:
    def test_map_is_depth_zero(self):
        assert _complex_column_depth("map(varchar, varchar)") == 0

    def test_row_is_depth_zero(self):
        assert _complex_column_depth("row(x bigint)") == 0

    def test_array_of_map_is_depth_one(self):
        assert _complex_column_depth("array(map(varchar, varchar))") == 1

    def test_array_of_array_of_map_is_depth_two(self):
        assert _complex_column_depth("array(array(map(varchar, varchar)))") == 2

    def test_array_of_array_of_array_of_map_is_depth_three(self):
        """Three levels of array-wrapping is needed to distinguish the correct
        trailing slice `[:-1]` from an off-by-one `[:-2]`/`[:~1]` mutant: at
        one or two levels both slices happen to leave the "map("/"row(" prefix
        intact, so the depth count matches by coincidence; at three levels the
        over-trimmed variant eats into the "map(" prefix itself and returns
        None instead of 3."""
        assert _complex_column_depth("array(array(array(map(a,b))))") == 3

    def test_varchar_is_not_complex(self):
        assert _complex_column_depth("varchar") is None

    def test_array_of_bigint_is_not_complex(self):
        assert _complex_column_depth("array(bigint)") is None


class TestStringifyComplex:
    def test_none_at_depth_zero(self):
        assert _stringify_complex(None, 0) is None

    def test_nan_at_depth_zero(self):
        assert _stringify_complex(float("nan"), 0) is None

    def test_dict_at_depth_zero(self):
        assert _stringify_complex({"a": "b"}, 0) == str({"a": "b"})

    def test_non_nan_float_at_depth_zero(self):
        assert _stringify_complex(1.5, 0) == "1.5"

    def test_list_at_depth_one(self):
        assert _stringify_complex([{"a": 1}, {"b": 2}], 1) == [
            str({"a": 1}),
            str({"b": 2}),
        ]

    def test_list_with_none_and_nan_at_depth_one(self):
        assert _stringify_complex([None, float("nan"), {"a": 1}], 1) == [
            None,
            None,
            str({"a": 1}),
        ]

    def test_negative_depth_raises_type_error(self):
        """`depth == 0` must stay a strict equality check, not `depth <= 0`:
        a real column's depth (from _complex_column_depth) is never negative,
        so calling with depth=-1 on a non-iterable value takes the recursive
        branch and fails to iterate -- under a `<= 0` mutant it would instead
        take the `str(value)` branch and return "1" without raising."""
        with pytest.raises(TypeError):
            _stringify_complex(1, -1)

    def test_depth_decrements_by_exactly_one_per_level(self):
        """`depth - 1` must stay subtraction: at depth=4, `>> 1` (=2),
        `% 1` (=0) and `^ 1` (=5) all disagree with the correct 3 at the
        first recursion step, unlike depth=1 where all four operations
        coincidentally give 0."""
        assert _stringify_complex([[[[1]]]], 4) == [[[["1"]]]]


class TestStreamingBatchSizeConfig:
    def _base_kwargs(self):
        return dict(
            host="h",
            port=1,
            catalog="c",
            user="u",
            connector={"type": "memory"},
            auth=None,
        )

    def test_default_is_200_000(self):
        config = TrinoOfflineStoreConfig(**self._base_kwargs())
        assert config.streaming_batch_size == 200_000

    def test_accepts_positive_value(self):
        config = TrinoOfflineStoreConfig(**self._base_kwargs(), streaming_batch_size=1)
        assert config.streaming_batch_size == 1

    def test_rejects_zero(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TrinoOfflineStoreConfig(**self._base_kwargs(), streaming_batch_size=0)
