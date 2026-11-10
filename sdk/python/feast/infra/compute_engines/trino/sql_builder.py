from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence, Tuple

from feast.infra.compute_engines.trino.utils import quote_identifier
from feast.infra.compute_engines.utils import ENTITY_TS_ALIAS


@dataclass(frozen=True)
class TrinoQueryPlan:
    """Represents an immutable, compiled Trino SQL query plan built from DAG nodes.

    Maintains Common Table Expressions (CTEs), current projection columns,
    and metadata (join keys, timestamp column) to support in-cluster execution.
    Strictly immutable by design via copy-on-write transformations.
    """

    ctes: Tuple[Tuple[str, str], ...] = ()
    current_from: str = ""
    columns: Optional[Tuple[str, ...]] = None
    join_keys: Tuple[str, ...] = ()
    timestamp_col: Optional[str] = None
    created_timestamp_col: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ctes", tuple(self.ctes))
        if self.columns is not None and not isinstance(self.columns, tuple):
            object.__setattr__(self, "columns", tuple(self.columns))
        if not isinstance(self.join_keys, tuple):
            object.__setattr__(self, "join_keys", tuple(self.join_keys))
        if not isinstance(self.metadata, dict):
            object.__setattr__(self, "metadata", dict(self.metadata))

    def add_cte(
        self,
        name: str,
        query: str,
        columns: Optional[Sequence[str]] = None,
        timestamp_col: Optional[str] = None,
        created_timestamp_col: Optional[str] = None,
        join_keys: Optional[Sequence[str]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> TrinoQueryPlan:
        """Append a CTE step to the query plan immutably and update the current reference."""
        return TrinoQueryPlan(
            ctes=(*self.ctes, (name, query.strip())),
            current_from=name,
            columns=tuple(columns) if columns is not None else self.columns,
            join_keys=tuple(join_keys) if join_keys is not None else self.join_keys,
            timestamp_col=timestamp_col or self.timestamp_col,
            created_timestamp_col=created_timestamp_col or self.created_timestamp_col,
            metadata={**self.metadata, **(metadata or {})},
        )

    def to_sql(self) -> str:
        """Compile the CTE chain into a single executable ANSI SQL statement."""
        entity_ts_col = self.metadata.get("entity_ts_col")
        if self.columns:
            if (
                entity_ts_col
                and entity_ts_col != ENTITY_TS_ALIAS
                and ENTITY_TS_ALIAS in self.columns
            ):
                projected = [
                    f"{quote_identifier(c)} AS {quote_identifier(entity_ts_col)}"
                    if c == ENTITY_TS_ALIAS
                    else c
                    for c in self.columns
                    if c != entity_ts_col
                ]
                cols = ", ".join(projected)
            else:
                cols = ", ".join(self.columns)
        else:
            cols = "*"

        if not self.ctes:
            if self.current_from:
                return f"SELECT {cols} FROM {self.current_from}"
            return "SELECT 1"

        cte_parts = [f"{name} AS (\n{query}\n)" for name, query in self.ctes]
        with_clause = "WITH\n" + ",\n".join(cte_parts)
        return f"{with_clause}\nSELECT {cols}\nFROM {self.current_from}"

    def get_latest_cte_name(self) -> str:
        """Return the identifier of the latest CTE in the plan."""
        return self.current_from
