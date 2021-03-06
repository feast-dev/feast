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
from typing import Optional


class ParquetSource:
    """A Parquet based data source containing feature values."""

    def __init__(
        self,
        path: Optional[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
    ):
        """Initialize a ParquetSource from a Parquet file.

        Args:
            path: File path to parquet file. Must contain an event_timestamp column, entity columns and feature columns.
            event_timestamp_column: Event timestamp column used for point in time joins of feature values.
            created_timestamp_column (optional): Timestamp column when row was created, used for deduplicating rows.

        Examples:
            Examples should be written in doctest format, and should illustrate how
            to use the function.

            >>> ParquetSource("/data/my_features.parquet", event_timestamp_column="datetime")
        """
        self.path = path
        self.event_timestamp_column = event_timestamp_column
        self.created_timestamp_column = created_timestamp_column
        return
