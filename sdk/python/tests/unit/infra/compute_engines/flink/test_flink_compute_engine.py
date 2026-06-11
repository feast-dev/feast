from __future__ import annotations

import re
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Optional
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest
import toml  # type: ignore[import-untyped]

from feast import BatchFeatureView, Entity, Field, FileSource
from feast.aggregation import Aggregation
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.flink.compute import (
    FlinkComputeEngine,
    FlinkComputeEngineConfig,
)
from feast.infra.compute_engines.flink.nodes import (
    ENTITY_ROW_ID,
    ENTITY_TS_ALIAS,
    FlinkAggregationNode,
    FlinkDedupNode,
    FlinkFilterNode,
    FlinkJoinNode,
    FlinkOutputNode,
    FlinkSourceReadNode,
    FlinkTransformationNode,
    FlinkValidationNode,
    _subtract_flink_intervals,
)
from feast.infra.compute_engines.flink.utils import pandas_to_flink_table
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.types import Float32
from feast.value_type import ValueType


def test_flink_extra_does_not_downgrade_default_pyarrow_dependency() -> None:
    pyproject_path = Path(__file__).resolve().parents[7] / "pyproject.toml"
    pyproject = toml.loads(pyproject_path.read_text())

    dependencies = pyproject["project"]["dependencies"]
    assert "pyarrow>=21.0.0; extra != 'flink'" in dependencies
    assert "pyarrow>=16.1.0,<21.0.0; extra == 'flink'" in dependencies
    assert "pyarrow>=16.1.0" not in dependencies
    assert (
        "apache-flink>=2.2.1,<3"
        in pyproject["project"]["optional-dependencies"]["flink"]
    )


class FakeFlinkTable:
    def __init__(self, df: pd.DataFrame, fail_on_to_pandas: bool = False) -> None:
        self._df = df.copy()
        self.fail_on_to_pandas = fail_on_to_pandas

    def to_pandas(self) -> pd.DataFrame:
        if self.fail_on_to_pandas:
            raise AssertionError("FlinkOutputNode should not collect via to_pandas()")
        return self._df.copy()

    def get_schema(self) -> FakeFlinkSchema:
        return FakeFlinkSchema(list(self._df.columns))

    def execute(self) -> FakeTableResult:
        return FakeTableResult(self._df)


class FakeTableResult:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df.copy()

    def collect(self) -> FakeCloseableIterator:
        return FakeCloseableIterator(self._df)


class FakeCloseableIterator:
    def __init__(self, df: pd.DataFrame) -> None:
        self._rows = iter(df.itertuples(index=False, name=None))
        self.closed = False

    def __iter__(self) -> FakeCloseableIterator:
        return self

    def __next__(self) -> tuple[Any, ...]:
        return next(self._rows)

    def close(self) -> None:
        self.closed = True


class FakeTableEnvironment:
    def __init__(self) -> None:
        self.created_tables: List[pd.DataFrame] = []
        self.schemas: List[object] = []
        self.split_nums: List[int] = []
        self.views: dict[str, object] = {}
        self.dropped_views: List[str] = []
        self.queries: List[str] = []

    def from_pandas(
        self,
        df: pd.DataFrame,
        schema: object = None,
        splits_num: int = 1,
        split_num: Optional[int] = None,
    ) -> FakeFlinkTable:
        self.created_tables.append(df.copy())
        self.schemas.append(schema)
        self.split_nums.append(split_num if split_num is not None else splits_num)
        return FakeFlinkTable(df)

    def create_temporary_view(
        self, view_path: str, table_or_data_stream: object, *args: object
    ) -> None:
        self.views[view_path] = table_or_data_stream

    def drop_temporary_view(self, view_path: str) -> None:
        self.dropped_views.append(view_path)
        self.views.pop(view_path, None)

    def sql_query(self, query: str) -> Any:
        self.queries.append(query)
        return FakeFlinkTable(self._evaluate_sql(query))

    def _view_df(self, view_name: str) -> pd.DataFrame:
        table = self.views[view_name]
        if isinstance(table, FakeFlinkTable):
            return table.to_pandas()
        if isinstance(table, FakeNativeFlinkTable):
            return pd.DataFrame(columns=table.get_schema().get_field_names())
        raise TypeError(f"Unsupported fake Flink table type: {type(table)}")

    def _evaluate_sql(self, query: str) -> pd.DataFrame:
        if "ROW_NUMBER() OVER" in query:
            return self._evaluate_row_number_query(query)
        if " GROUP BY " in query:
            return self._evaluate_group_by_query(query)
        if " JOIN " in query:
            return self._evaluate_join_query(query)
        if " WHERE " in query:
            return self._evaluate_where_query(query)
        return self._evaluate_select_query(query)

    def _extract_views(self, query: str) -> List[str]:
        return re.findall(r"(?:FROM|JOIN)\s+`([^`]+)`", query)

    def _evaluate_select_query(self, query: str) -> pd.DataFrame:
        views = self._extract_views(query)
        if not views:
            if "FROM entities" in query:
                return self._view_df("entities")[["driver_id", "event_timestamp"]]
            raise ValueError(f"Could not infer source view from query: {query}")
        source_df = self._view_df(views[-1])
        select_clause = query.split(" FROM ", 1)[0].removeprefix("SELECT ")
        if select_clause == "*":
            return source_df
        result = pd.DataFrame()
        for column_expr in select_clause.split(", "):
            parts = re.findall(r"`([^`]+)`", column_expr)
            if not parts:
                continue
            source_column = parts[0]
            output_column = parts[-1]
            result[output_column] = source_df[source_column]
        return result

    def _evaluate_where_query(self, query: str) -> pd.DataFrame:
        view_name = self._extract_views(query)[0]
        df = self._view_df(view_name)
        if "`event_timestamp` <= `__entity_event_timestamp`" in query:
            df = df[df["event_timestamp"] <= df[ENTITY_TS_ALIAS]]
        if "conv_rate > 0.15" in query:
            df = df[df["conv_rate"] > 0.15]
        return df.reset_index(drop=True)

    def _evaluate_group_by_query(self, query: str) -> pd.DataFrame:
        view_name = self._extract_views(query)[0]
        df = self._view_df(view_name)
        return (
            df.groupby("driver_id")
            .agg(sum_conv_rate=pd.NamedAgg(column="conv_rate", aggfunc="sum"))
            .reset_index()
        )

    def _evaluate_row_number_query(self, query: str) -> pd.DataFrame:
        view_name = self._extract_views(query)[-1]
        df = self._view_df(view_name)
        if " - 1 AS " in query:
            df = df.copy()
            df[ENTITY_ROW_ID] = range(len(df))
            if " AS `__entity_event_timestamp`" in query:
                df = df.rename(columns={"event_timestamp": ENTITY_TS_ALIAS})
            return df.reset_index(drop=True)

        dedup_keys = [ENTITY_ROW_ID] if ENTITY_ROW_ID in df.columns else ["driver_id"]
        sort_keys = [
            column for column in ["event_timestamp", "created"] if column in df
        ]
        return (
            df.sort_values(by=sort_keys, ascending=False)
            .drop_duplicates(subset=dedup_keys)
            .reset_index(drop=True)
        )

    def _evaluate_join_query(self, query: str) -> pd.DataFrame:
        views = self._extract_views(query)
        if " AS e LEFT JOIN " in query:
            entity_df = self._view_df(views[-2])
            feature_df = self._view_df(views[-1])
            feature_columns = [
                column
                for column in feature_df.columns
                if column not in entity_df.columns and column != "driver_id"
            ]
            return entity_df.merge(
                feature_df[["driver_id", *feature_columns]],
                on="driver_id",
                how="left",
            )

        joined_df = self._view_df(views[0])
        for view_name in views[1:]:
            joined_df = joined_df.merge(
                self._view_df(view_name), on="driver_id", how="left"
            )
        return joined_df


class FakeFlinkSchema:
    def __init__(self, columns: List[str]) -> None:
        self._columns = columns

    def get_field_names(self) -> List[str]:
        return list(self._columns)


class FakeNativeFlinkTable:
    def __init__(self, columns: List[str]) -> None:
        self._columns = columns

    def get_schema(self) -> FakeFlinkSchema:
        return FakeFlinkSchema(self._columns)


class RecordingTableEnvironment(FakeTableEnvironment):
    def __init__(self) -> None:
        super().__init__()

    def create_temporary_view(
        self, view_path: str, table_or_data_stream: object, *args: object
    ) -> None:
        self.views[view_path] = table_or_data_stream

    def sql_query(self, query: str) -> FakeNativeFlinkTable:
        self.queries.append(query)
        return FakeNativeFlinkTable([])


class InputNode(DAGNode):
    def execute(self, context: ExecutionContext) -> DAGValue:
        return context.node_outputs[self.name]


class FakeRetrievalJob(RetrievalJob):
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self._table.to_pandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return self._table

    @property
    def full_feature_names(self) -> bool:
        return False

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return []

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return None

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        raise NotImplementedError

    def to_remote_storage(self) -> List[str]:
        raise NotImplementedError

    def to_sql(self) -> str:
        raise NotImplementedError


class FakeFlinkRetrievalJob:
    def __init__(self, df: pd.DataFrame) -> None:
        self._table = FakeFlinkTable(df)

    def to_flink_table(self, table_env: object) -> FakeFlinkTable:
        return self._table


def _repo_config(tmp_path: Path, batch_engine: dict[str, object]) -> RepoConfig:
    return RepoConfig(
        project="test_project",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        offline_store={"type": "file"},
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        batch_engine=batch_engine,
    )


def _driver() -> Entity:
    return Entity(name="driver_id", value_type=ValueType.INT64)


def _source() -> FileSource:
    return FileSource(
        name="driver_stats_source",
        path="unused.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )


def _feature_view(source: FileSource, **kwargs: Any) -> BatchFeatureView:
    return BatchFeatureView(
        name="driver_stats",
        entities=[_driver()],
        ttl=timedelta(days=2),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
        **kwargs,
    )


def _feature_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                datetime(2024, 1, 1, 9, 0, 0),
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 10, 0, 0),
            ],
            "created": [
                datetime(2024, 1, 1, 9, 1, 0),
                datetime(2024, 1, 1, 10, 1, 0),
                datetime(2024, 1, 1, 10, 1, 0),
            ],
            "conv_rate": [0.1, 0.2, 0.3],
        }
    )


def _offline_store(df: pd.DataFrame) -> MagicMock:
    store = MagicMock()
    store.pull_all_from_table_or_query.return_value = FakeFlinkRetrievalJob(df)
    store.pull_latest_from_table_or_query.return_value = FakeFlinkRetrievalJob(df)
    return store


def _registry(entity: Entity) -> MagicMock:
    registry = MagicMock()
    registry.get_entity.return_value = entity
    return registry


def _column_info() -> ColumnInfo:
    return ColumnInfo(
        join_keys=["driver_id"],
        feature_cols=["conv_rate"],
        ts_col="event_timestamp",
        created_ts_col="created",
    )


def _execution_context(
    tmp_path: Path, node_outputs: dict[str, DAGValue]
) -> ExecutionContext:
    return ExecutionContext(
        project="test_project",
        repo_config=_repo_config(tmp_path, {"type": "flink.engine"}),
        offline_store=MagicMock(),
        online_store=MagicMock(),
        entity_defs=[_driver()],
        node_outputs=node_outputs,
    )


def _flink_value(df: pd.DataFrame) -> DAGValue:
    return DAGValue(
        data=FakeFlinkTable(df),
        format=DAGFormat.FLINK,
        metadata={"columns": list(df.columns)},
    )


def test_pandas_to_flink_table_builds_typed_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSchemaBuilder:
        def __init__(self) -> None:
            self.columns: List[tuple[str, str]] = []

        def column(self, name: str, dtype: str) -> FakeSchemaBuilder:
            self.columns.append((name, dtype))
            return self

        def build(self) -> List[tuple[str, str]]:
            return self.columns

    class FakeSchema:
        @staticmethod
        def new_builder() -> FakeSchemaBuilder:
            return FakeSchemaBuilder()

    class FakeDataTypes:
        @staticmethod
        def BOOLEAN() -> str:
            return "BOOLEAN"

        @staticmethod
        def TINYINT() -> str:
            return "TINYINT"

        @staticmethod
        def SMALLINT() -> str:
            return "SMALLINT"

        @staticmethod
        def INT() -> str:
            return "INT"

        @staticmethod
        def BIGINT() -> str:
            return "BIGINT"

        @staticmethod
        def FLOAT() -> str:
            return "FLOAT"

        @staticmethod
        def DOUBLE() -> str:
            return "DOUBLE"

        @staticmethod
        def TIMESTAMP(precision: int) -> str:
            return f"TIMESTAMP({precision})"

        @staticmethod
        def TIMESTAMP_LTZ(precision: int) -> str:
            return f"TIMESTAMP_LTZ({precision})"

        @staticmethod
        def BYTES() -> str:
            return "BYTES"

        @staticmethod
        def DATE() -> str:
            return "DATE"

        @staticmethod
        def DECIMAL(precision: int, scale: int) -> str:
            return f"DECIMAL({precision},{scale})"

        @staticmethod
        def STRING() -> str:
            return "STRING"

    pyflink_module = types.ModuleType("pyflink")
    table_module = types.ModuleType("pyflink.table")
    setattr(table_module, "Schema", FakeSchema)
    setattr(table_module, "DataTypes", FakeDataTypes)
    monkeypatch.setitem(sys.modules, "pyflink", pyflink_module)
    monkeypatch.setitem(sys.modules, "pyflink.table", table_module)

    table_env = FakeTableEnvironment()
    df = pd.DataFrame(
        {
            "driver_id": pd.Series([1, 2], dtype="int64"),
            "conv_rate": pd.Series([0.1, 0.2], dtype="float64"),
            "event_timestamp": pd.to_datetime(
                ["2024-01-01 00:00:00", "2024-01-02 00:00:00"]
            ),
            "active": pd.Series([True, False], dtype="bool"),
        }
    )

    pandas_to_flink_table(table_env, df, split_num=4)

    assert table_env.schemas[-1] == [
        ("driver_id", "BIGINT"),
        ("conv_rate", "DOUBLE"),
        ("event_timestamp", "TIMESTAMP(3)"),
        ("active", "BOOLEAN"),
    ]
    assert table_env.schemas[-1] != list(df.columns)
    assert table_env.split_nums == [4]


def _native_flink_value(columns: List[str]) -> DAGValue:
    return DAGValue(
        data=FakeNativeFlinkTable(columns),
        format=DAGFormat.FLINK,
        metadata={"columns": columns},
    )


def test_repo_config_loads_flink_batch_engine_config(tmp_path: Path) -> None:
    config = _repo_config(
        tmp_path,
        {
            "type": "flink.engine",
            "execution_mode": "streaming",
            "parallelism": 3,
            "table_config": {"pipeline.name": "feast-flink-test"},
            "pandas_split_num": 2,
        },
    )

    assert isinstance(config.batch_engine, FlinkComputeEngineConfig)
    assert config.batch_engine.execution_mode == "streaming"
    assert config.batch_engine.parallelism == 3
    assert config.batch_engine.table_config == {"pipeline.name": "feast-flink-test"}
    assert config.batch_engine.pandas_split_num == 2


def test_flink_source_read_node_converts_arrow_retrieval_jobs(
    tmp_path: Path,
) -> None:
    offline_store = MagicMock()
    offline_store.pull_all_from_table_or_query.return_value = FakeRetrievalJob(
        pa.Table.from_pandas(_feature_data())
    )
    context = _execution_context(tmp_path, {})
    context.offline_store = offline_store
    node = FlinkSourceReadNode(
        "source",
        _source(),
        _column_info(),
        FakeTableEnvironment(),
        split_num=1,
    )

    result = node.execute(context)

    assert result.format == DAGFormat.FLINK
    assert result.metadata["columns"] == list(_feature_data().columns)
    assert result.data.to_pandas().equals(_feature_data())


def test_flink_historical_retrieval_executes_dag_with_transformation(
    tmp_path: Path,
) -> None:
    entity = _driver()
    source = _source()

    def double_conv_rate(table: FakeFlinkTable) -> FakeFlinkTable:
        df = table.to_pandas()
        df["conv_rate"] = df["conv_rate"] * 2
        return FakeFlinkTable(df)

    feature_view = _feature_view(
        source,
        mode="flink",
        udf=double_conv_rate,
        udf_string="double_conv_rate",
        online=False,
        offline=False,
    )
    config = _repo_config(
        tmp_path,
        {"type": "flink.engine", "pandas_split_num": 4},
    )
    table_env = FakeTableEnvironment()
    engine = FlinkComputeEngine(
        repo_config=config,
        offline_store=_offline_store(_feature_data()),
        online_store=MagicMock(),
        table_environment=table_env,
    )
    task = HistoricalRetrievalTask(
        project=config.project,
        entity_df=None,
        feature_view=feature_view,
        full_feature_name=False,
        registry=_registry(entity),
    )

    job = engine.get_historical_features(_registry(entity), task)
    result = job.to_df().sort_values("driver_id").reset_index(drop=True)

    assert job.error() is None
    assert result["driver_id"].tolist() == [1, 2]
    assert result["conv_rate"].tolist() == [0.4, 0.6]
    assert table_env.dropped_views
    assert table_env.views == {}


def test_flink_historical_retrieval_with_empty_entity_df_returns_empty_result(
    tmp_path: Path,
) -> None:
    entity = _driver()
    source = _source()
    feature_view = _feature_view(source, online=False, offline=False)
    config = _repo_config(tmp_path, {"type": "flink.engine", "pandas_split_num": 4})
    table_env = FakeTableEnvironment()
    engine = FlinkComputeEngine(
        repo_config=config,
        offline_store=_offline_store(_feature_data()),
        online_store=MagicMock(),
        table_environment=table_env,
    )
    task = HistoricalRetrievalTask(
        project=config.project,
        entity_df=pd.DataFrame(
            {
                "driver_id": pd.Series(dtype="int64"),
                "event_timestamp": pd.Series(dtype="datetime64[ns]"),
            }
        ),
        feature_view=feature_view,
        full_feature_name=False,
        registry=_registry(entity),
    )

    job = engine.get_historical_features(_registry(entity), task)
    result = job.to_df()

    assert job.error() is None
    assert result.empty
    assert "conv_rate" in result.columns
    assert table_env.created_tables[-1].empty


def test_flink_historical_retrieval_is_read_only_and_dedupes_per_entity_row(
    tmp_path: Path,
) -> None:
    entity = _driver()
    source = _source()
    feature_view = _feature_view(source, online=True, offline=True)
    config = _repo_config(tmp_path, {"type": "flink.engine", "pandas_split_num": 4})
    feature_data = pd.DataFrame(
        {
            "driver_id": [1, 1],
            "event_timestamp": [
                datetime(2024, 1, 1, 9, 0, 0),
                datetime(2024, 1, 1, 10, 0, 0),
            ],
            "created": [
                datetime(2024, 1, 1, 9, 1, 0),
                datetime(2024, 1, 1, 10, 1, 0),
            ],
            "conv_rate": [0.1, 0.2],
        }
    )
    offline_store = _offline_store(feature_data)
    online_store = MagicMock()
    table_env = FakeTableEnvironment()
    engine = FlinkComputeEngine(
        repo_config=config,
        offline_store=offline_store,
        online_store=online_store,
        table_environment=table_env,
    )
    task = HistoricalRetrievalTask(
        project=config.project,
        entity_df=pd.DataFrame(
            {
                "driver_id": [1, 1],
                "event_timestamp": [
                    datetime(2024, 1, 1, 9, 30, 0),
                    datetime(2024, 1, 1, 10, 30, 0),
                ],
            }
        ),
        feature_view=feature_view,
        full_feature_name=False,
        registry=_registry(entity),
    )

    result = engine.get_historical_features(_registry(entity), task).to_df()
    result = result.sort_values(ENTITY_TS_ALIAS).reset_index(drop=True)

    assert result["conv_rate"].tolist() == [0.1, 0.2]
    assert table_env.split_nums == [4]
    assert ENTITY_ROW_ID not in result.columns
    online_store.online_write_batch.assert_not_called()
    offline_store.offline_write_batch.assert_not_called()


def test_flink_historical_retrieval_supports_sql_entity_df(tmp_path: Path) -> None:
    entity = _driver()
    source = _source()
    feature_view = _feature_view(source, online=False, offline=False)
    config = _repo_config(tmp_path, {"type": "flink.engine"})
    table_env = FakeTableEnvironment()
    table_env.create_temporary_view(
        "entities",
        FakeFlinkTable(
            pd.DataFrame(
                {
                    "driver_id": [1, 1],
                    "event_timestamp": [
                        datetime(2024, 1, 1, 9, 30, 0),
                        datetime(2024, 1, 1, 10, 30, 0),
                    ],
                }
            )
        ),
    )
    engine = FlinkComputeEngine(
        repo_config=config,
        offline_store=_offline_store(_feature_data()),
        online_store=MagicMock(),
        table_environment=table_env,
    )
    task = HistoricalRetrievalTask(
        project=config.project,
        entity_df="SELECT driver_id, event_timestamp FROM entities",
        feature_view=feature_view,
        full_feature_name=False,
        registry=_registry(entity),
    )

    job = engine.get_historical_features(_registry(entity), task)
    result = job.to_df().sort_values(ENTITY_TS_ALIAS).reset_index(drop=True)

    assert job.error() is None
    assert result["conv_rate"].tolist() == [0.1, 0.2]
    assert any(
        "SELECT driver_id, event_timestamp FROM entities" in query
        for query in table_env.queries
    )
    assert set(table_env.views) == {"entities"}


def test_flink_materialize_writes_online_and_offline(tmp_path: Path) -> None:
    entity = _driver()
    source = _source()
    feature_view = _feature_view(source, online=True, offline=True)
    config = _repo_config(tmp_path, {"type": "flink.engine"})
    offline_store = _offline_store(_feature_data().head(1))
    online_store = MagicMock()
    table_env = FakeTableEnvironment()
    engine = FlinkComputeEngine(
        repo_config=config,
        offline_store=offline_store,
        online_store=online_store,
        table_environment=table_env,
    )
    task = MaterializationTask(
        project=config.project,
        feature_view=feature_view,
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 2),
    )

    jobs = engine.materialize(_registry(entity), task)

    assert len(jobs) == 1
    assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED
    assert jobs[0].error() is None
    online_store.online_write_batch.assert_called_once()
    offline_store.offline_write_batch.assert_called_once()
    assert table_env.dropped_views
    assert table_env.views == {}


def test_flink_output_node_streams_batches_without_full_pandas_collect(
    tmp_path: Path,
) -> None:
    source = _source()
    feature_view = _feature_view(source, online=True, offline=True)
    input_node = InputNode("input")
    node = FlinkOutputNode(
        "output",
        feature_view,
        FakeTableEnvironment(),
        split_num=1,
        write_output=True,
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {
            "input": DAGValue(
                data=FakeFlinkTable(_feature_data().head(2), fail_on_to_pandas=True),
                format=DAGFormat.FLINK,
                metadata={"columns": list(_feature_data().columns)},
            )
        },
    )
    context.repo_config.materialization_config.online_write_batch_size = 1
    context.online_store = MagicMock()
    context.offline_store = MagicMock()

    node.execute(context)

    assert context.online_store.online_write_batch.call_count == 2
    assert context.offline_store.offline_write_batch.call_count == 2


def test_flink_engine_reports_materialization_errors(tmp_path: Path) -> None:
    entity = _driver()
    source = _source()
    feature_view = _feature_view(source, online=False, offline=False)
    offline_store = MagicMock()
    offline_store.pull_all_from_table_or_query.side_effect = RuntimeError("boom")
    config = _repo_config(tmp_path, {"type": "flink.engine"})
    engine = FlinkComputeEngine(
        repo_config=config,
        offline_store=offline_store,
        online_store=MagicMock(),
        table_environment=FakeTableEnvironment(),
    )
    task = MaterializationTask(
        project=config.project,
        feature_view=feature_view,
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 2),
    )

    jobs = engine.materialize(_registry(entity), task)

    assert jobs[0].status() == MaterializationJobStatus.ERROR
    assert isinstance(jobs[0].error(), RuntimeError)


def test_flink_join_node_merges_input_tables(tmp_path: Path) -> None:
    left = InputNode("left")
    right = InputNode("right")
    node = FlinkJoinNode(
        "join",
        _column_info(),
        FakeTableEnvironment(),
        split_num=1,
        inputs=[left, right],
    )
    context = _execution_context(
        tmp_path,
        {
            "left": _flink_value(
                pd.DataFrame({"driver_id": [1, 2], "conv_rate": [0.1, 0.2]})
            ),
            "right": _flink_value(
                pd.DataFrame({"driver_id": [1, 2], "acc_rate": [0.3, 0.4]})
            ),
        },
    )

    result = node.execute(context).data.to_pandas().sort_values("driver_id")

    assert result["conv_rate"].tolist() == [0.1, 0.2]
    assert result["acc_rate"].tolist() == [0.3, 0.4]


def test_flink_join_node_uses_native_sql_when_available(tmp_path: Path) -> None:
    left = InputNode("left")
    right = InputNode("right")
    table_env = RecordingTableEnvironment()
    node = FlinkJoinNode(
        "join",
        _column_info(),
        table_env,
        split_num=1,
        inputs=[left, right],
    )
    context = _execution_context(
        tmp_path,
        {
            "left": _native_flink_value(["driver_id", "conv_rate"]),
            "right": _native_flink_value(["driver_id", "acc_rate"]),
        },
    )

    result = node.execute(context)

    assert result.format == DAGFormat.FLINK
    assert result.metadata["columns"] == ["driver_id", "conv_rate", "acc_rate"]
    assert any("JOIN" in query for query in table_env.queries)


def test_flink_filter_node_applies_filter_expression(tmp_path: Path) -> None:
    input_node = InputNode("input")
    node = FlinkFilterNode(
        "filter",
        _column_info(),
        FakeTableEnvironment(),
        split_num=1,
        filter_expr="conv_rate > 0.15",
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {
            "input": _flink_value(
                pd.DataFrame({"driver_id": [1, 2], "conv_rate": [0.1, 0.2]})
            )
        },
    )

    result = node.execute(context).data.to_pandas()

    assert result["driver_id"].tolist() == [2]


def test_flink_filter_node_uses_native_sql_when_available(tmp_path: Path) -> None:
    input_node = InputNode("input")
    table_env = RecordingTableEnvironment()
    node = FlinkFilterNode(
        "filter",
        _column_info(),
        table_env,
        split_num=1,
        filter_expr="conv_rate > 0.15",
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {"input": _native_flink_value(["driver_id", "conv_rate"])},
    )

    result = node.execute(context)

    assert result.format == DAGFormat.FLINK
    assert any(
        "WHERE" in query and "conv_rate > 0.15" in query for query in table_env.queries
    )


def test_flink_filter_node_renders_ttl_as_valid_flink_interval(
    tmp_path: Path,
) -> None:
    input_node = InputNode("input")
    table_env = RecordingTableEnvironment()
    node = FlinkFilterNode(
        "filter",
        _column_info(),
        table_env,
        split_num=1,
        ttl=timedelta(days=2, hours=3, minutes=4, seconds=5),
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {
            "input": _native_flink_value(
                ["driver_id", "conv_rate", "event_timestamp", ENTITY_TS_ALIAS]
            )
        },
    )

    node.execute(context)

    assert _subtract_flink_intervals(
        "`__entity_event_timestamp`", timedelta(days=2, hours=3, minutes=4, seconds=5)
    ) == (
        "`__entity_event_timestamp` - INTERVAL '2' DAY - INTERVAL '3' HOUR "
        "- INTERVAL '4' MINUTE - INTERVAL '5' SECOND"
    )
    assert any(
        "`event_timestamp` >= `__entity_event_timestamp` - INTERVAL '2' DAY "
        "- INTERVAL '3' HOUR - INTERVAL '4' MINUTE - INTERVAL '5' SECOND" in query
        for query in table_env.queries
    )
    assert all("+ INTERVAL" not in query for query in table_env.queries)


def test_flink_aggregation_node_groups_features(tmp_path: Path) -> None:
    input_node = InputNode("input")
    node = FlinkAggregationNode(
        "agg",
        ["driver_id"],
        aggregations=[Aggregation(column="conv_rate", function="sum")],
        table_env=FakeTableEnvironment(),
        split_num=1,
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {
            "input": _flink_value(
                pd.DataFrame({"driver_id": [1, 1, 2], "conv_rate": [0.1, 0.2, 0.3]})
            )
        },
    )

    result = node.execute(context).data.to_pandas().sort_values("driver_id")

    assert result["sum_conv_rate"].tolist() == pytest.approx([0.3, 0.3])


def test_flink_aggregation_node_uses_native_sql_when_available(tmp_path: Path) -> None:
    input_node = InputNode("input")
    table_env = RecordingTableEnvironment()
    node = FlinkAggregationNode(
        "agg",
        ["driver_id"],
        aggregations=[Aggregation(column="conv_rate", function="sum")],
        table_env=table_env,
        split_num=1,
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {"input": _native_flink_value(["driver_id", "conv_rate"])},
    )

    result = node.execute(context)

    assert result.format == DAGFormat.FLINK
    assert result.metadata["columns"] == ["driver_id", "sum_conv_rate"]
    assert any("GROUP BY" in query and "SUM" in query for query in table_env.queries)


def test_flink_dedup_node_uses_entity_row_id_for_historical_retrieval(
    tmp_path: Path,
) -> None:
    input_node = InputNode("input")
    node = FlinkDedupNode(
        "dedup",
        _column_info(),
        FakeTableEnvironment(),
        split_num=1,
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {
            "input": _flink_value(
                pd.DataFrame(
                    {
                        ENTITY_ROW_ID: [0, 0, 1],
                        "driver_id": [1, 1, 1],
                        "event_timestamp": [
                            datetime(2024, 1, 1, 9, 0, 0),
                            datetime(2024, 1, 1, 10, 0, 0),
                            datetime(2024, 1, 1, 10, 0, 0),
                        ],
                        "created": [
                            datetime(2024, 1, 1, 9, 1, 0),
                            datetime(2024, 1, 1, 10, 1, 0),
                            datetime(2024, 1, 1, 10, 1, 0),
                        ],
                        "conv_rate": [0.1, 0.2, 0.3],
                    }
                )
            )
        },
    )

    result = node.execute(context).data.to_pandas().sort_values(ENTITY_ROW_ID)

    assert result["conv_rate"].tolist() == [0.2, 0.3]


def test_flink_dedup_node_uses_native_row_number_when_available(
    tmp_path: Path,
) -> None:
    input_node = InputNode("input")
    table_env = RecordingTableEnvironment()
    node = FlinkDedupNode(
        "dedup",
        _column_info(),
        table_env,
        split_num=1,
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {
            "input": _native_flink_value(
                [ENTITY_ROW_ID, "driver_id", "event_timestamp", "created", "conv_rate"]
            )
        },
    )

    result = node.execute(context)

    assert result.format == DAGFormat.FLINK
    assert any("ROW_NUMBER() OVER" in query for query in table_env.queries)
    assert ENTITY_ROW_ID in result.metadata["columns"]


def test_flink_transformation_node_keeps_native_flink_table(tmp_path: Path) -> None:
    input_node = InputNode("input")
    native_result = FakeNativeFlinkTable(["driver_id", "conv_rate"])

    def native_udf(table: object) -> FakeNativeFlinkTable:
        return native_result

    node = FlinkTransformationNode(
        "transform",
        native_udf,
        RecordingTableEnvironment(),
        split_num=1,
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {"input": _native_flink_value(["driver_id", "conv_rate"])},
    )

    result = node.execute(context)

    assert result.data is native_result
    assert result.metadata["columns"] == ["driver_id", "conv_rate"]


def test_flink_validation_node_raises_for_missing_columns(tmp_path: Path) -> None:
    input_node = InputNode("input")
    node = FlinkValidationNode(
        "validate",
        expected_columns={"missing_feature": pa.float32()},
        json_columns=set(),
        table_env=FakeTableEnvironment(),
        split_num=1,
        inputs=[input_node],
    )
    context = _execution_context(
        tmp_path,
        {"input": _flink_value(pd.DataFrame({"driver_id": [1]}))},
    )

    with pytest.raises(ValueError, match="Missing expected columns"):
        node.execute(context)


@pytest.mark.integration
@pytest.mark.slow
def test_flink_compute_engine_executes_with_real_pyflink_when_installed(
    tmp_path: Path,
) -> None:
    pyflink_table = pytest.importorskip(
        "pyflink.table", reason="PyFlink is required for this runtime smoke test"
    )
    entity = _driver()
    source = _source()
    feature_view = _feature_view(source, online=True, offline=True)
    config = _repo_config(tmp_path, {"type": "flink.engine"})
    offline_store = _offline_store(_feature_data())
    online_store = MagicMock()
    table_env = pyflink_table.TableEnvironment.create(
        pyflink_table.EnvironmentSettings.new_instance().in_batch_mode().build()
    )
    engine = FlinkComputeEngine(
        repo_config=config,
        offline_store=offline_store,
        online_store=online_store,
        table_environment=table_env,
    )
    task = HistoricalRetrievalTask(
        project=config.project,
        entity_df=pd.DataFrame(
            {
                "driver_id": [1, 1, 2],
                "event_timestamp": [
                    datetime(2024, 1, 1, 9, 30, 0),
                    datetime(2024, 1, 1, 10, 30, 0),
                    datetime(2024, 1, 1, 10, 30, 0),
                ],
            }
        ),
        feature_view=feature_view,
        full_feature_name=False,
        registry=_registry(entity),
    )

    result = engine.get_historical_features(_registry(entity), task).to_df()
    result = result.sort_values(["driver_id", ENTITY_TS_ALIAS]).reset_index(drop=True)

    assert result["conv_rate"].tolist() == [0.1, 0.2, 0.3]
    assert ENTITY_ROW_ID not in result.columns
    online_store.online_write_batch.assert_not_called()

    materialization_task = MaterializationTask(
        project=config.project,
        feature_view=feature_view,
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 2),
    )

    jobs = engine.materialize(_registry(entity), materialization_task)

    assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED
    assert jobs[0].error() is None
    online_store.online_write_batch.assert_called_once()
    offline_store.offline_write_batch.assert_called_once()
