from abc import ABC, abstractmethod
from datetime import timedelta


class DataFrameBackend(ABC):
    """
    Abstract interface for DataFrame operations used by the LocalComputeEngine.

    This interface defines the contract for implementing pluggable DataFrame backends
    such as Pandas, Polars, or DuckDB. Each backend must support core table operations
    such as joins, filtering, aggregation, conversion to/from Arrow, and deduplication.

    The purpose of this abstraction is to allow seamless swapping of execution backends
    without changing DAGNode or ComputeEngine logic. All nodes operate on pyarrow.Table
    as the standard input/output format, while the backend defines how the computation
    is actually performed.

    Expected implementations include:
    - PandasBackend
    - PolarsBackend
    - DuckDBBackend (future)

    Methods
    -------
    from_arrow(table: pa.Table) -> Any
        Convert a pyarrow.Table to the backend-native DataFrame format.

    to_arrow(df: Any) -> pa.Table
        Convert a backend-native DataFrame to pyarrow.Table.

    join(left: Any, right: Any, on: List[str], how: str) -> Any
        Join two dataframes on specified keys with given join type.

    groupby_agg(df: Any, group_keys: List[str], agg_ops: Dict[str, Tuple[str, str]]) -> Any
        Group and aggregate the dataframe. `agg_ops` maps output column names
        to (aggregation function, source column name) pairs.

    filter(df: Any, expr: str) -> Any
        Apply a filter expression (string-based) to the DataFrame.

    to_timedelta_value(delta: timedelta) -> Any
        Convert a Python timedelta object to a backend-compatible value
        that can be subtracted from a timestamp column.

    drop_duplicates(df: Any, keys: List[str], sort_by: List[str], ascending: bool = False) -> Any
        Deduplicate the DataFrame by key columns, keeping the first row
        by descending or ascending sort order.

    rename_columns(df: Any, columns: Dict[str, str]) -> Any
        Rename columns in the DataFrame according to the provided mapping.
    """

    @abstractmethod
    def columns(self, df): ...

    @abstractmethod
    def from_arrow(self, table): ...

    @abstractmethod
    def join(self, left, right, on, how): ...

    @abstractmethod
    def groupby_agg(self, df, group_keys, agg_ops): ...

    @abstractmethod
    def filter(self, df, expr): ...

    @abstractmethod
    def to_arrow(self, df): ...

    @abstractmethod
    def to_timedelta_value(self, delta: timedelta): ...

    @abstractmethod
    def drop_duplicates(self, df, keys, sort_by, ascending: bool = False):
        pass

    @abstractmethod
    def rename_columns(self, df, columns: dict[str, str]): ...
