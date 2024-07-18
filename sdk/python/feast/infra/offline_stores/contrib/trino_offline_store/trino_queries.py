from __future__ import annotations

import signal
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import trino
from trino.dbapi import Cursor
from trino.exceptions import TrinoQueryError

from feast.infra.offline_stores.contrib.trino_offline_store.trino_type_map import (
    trino_to_pa_value_type,
)
from feast.utils import _utc_now


class QueryStatus(Enum):
    PENDING = 0
    RUNNING = 1
    ERROR = 2
    COMPLETED = 3
    CANCELLED = 4


class Trino:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        catalog: str,
        source: Optional[str],
        http_scheme: str,
        verify: bool,
        extra_credential: Optional[str],
        auth: Optional[trino.Authentication],
    ):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.source = source
        self.http_scheme = http_scheme
        self.verify = verify
        self.extra_credential = extra_credential
        self.auth = auth
        self._cursor: Optional[Cursor] = None

    def _get_cursor(self) -> Cursor:
        if self._cursor is None:
            headers = (
                {trino.constants.HEADER_EXTRA_CREDENTIAL: self.extra_credential}
                if self.extra_credential
                else {}
            )
            self._cursor = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                source=self.source,
                http_scheme=self.http_scheme,
                verify=self.verify,
                auth=self.auth,
                http_headers=headers,
            ).cursor()

        return self._cursor

    def create_query(self, query_text: str) -> Query:
        """
        Create a Query object without executing it.
        """
        return Query(query_text=query_text, cursor=self._get_cursor())

    def execute_query(self, query_text: str) -> Results:
        """
        Create a Query object and execute it.
        """
        query = Query(query_text=query_text, cursor=self._get_cursor())
        return query.execute()


class Query(object):
    def __init__(self, query_text: str, cursor: Cursor):
        self.query_text = query_text
        self.status = QueryStatus.PENDING
        self._cursor = cursor

        signal.signal(signal.SIGINT, self.cancel)
        signal.signal(signal.SIGTERM, self.cancel)

    def execute(self) -> Results:
        try:
            self.status = QueryStatus.RUNNING
            start_time = _utc_now()

            self._cursor.execute(operation=self.query_text)
            rows = self._cursor.fetchall()

            end_time = _utc_now()
            self.execution_time = end_time - start_time
            self.status = QueryStatus.COMPLETED

            return Results(data=rows, columns=self._cursor._query.columns)
        except TrinoQueryError as error:
            self.status = QueryStatus.ERROR
            raise error
        finally:
            self.close()

    def close(self):
        self._cursor.close()

    def cancel(self, *args):
        if self.status != QueryStatus.COMPLETED:
            self._cursor.cancel()
            self.status = QueryStatus.CANCELLED
        self.close()


@dataclass
class Results:
    """Class for keeping track of the results of a Trino query"""

    data: List[List[Any]]
    columns: List[Dict]

    @property
    def columns_names(self) -> List[str]:
        return [column["name"] for column in self.columns]

    @property
    def schema(self) -> Dict[str, str]:
        return {column["name"]: column["type"] for column in self.columns}

    @property
    def pyarrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field(column["name"], trino_to_pa_value_type(column["type"]))
                for column in self.columns
            ]
        )

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(data=self.data, columns=self.columns_names)
        for col_name, col_type in self.schema.items():
            if col_type.startswith("timestamp"):
                df[col_name] = pd.to_datetime(df[col_name])
        return df.fillna(np.nan)
