# Copyright 2019 The Feast Authors
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
from typing import Dict, Optional


class BigQuerySource:
    """
    Represents a BigQuery table reference or BigQuery query that returns a set of features.
    """

    def __init__(
        self,
        event_timestamp_column: str,
        table_ref: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        query: Optional[str] = None,
    ):
        if (table_ref is None) == (query is None):
            raise ValueError("Exactly one of table_ref and query should be specified")
        if field_mapping is not None:
            for value in field_mapping.values():
                if list(field_mapping.values()).count(value) > 1:
                    raise ValueError(
                        f"Two fields cannot be mapped to the same name {value}"
                    )

            if event_timestamp_column in field_mapping.keys():
                raise ValueError(
                    f"The field {event_timestamp_column} is mapped to {field_mapping[event_timestamp_column]}. Please either remove this field mapping or use {field_mapping[event_timestamp_column]} as the event_timestamp_column."
                )

            if (
                created_timestamp_column is not None
                and created_timestamp_column in field_mapping.keys()
            ):
                raise ValueError(
                    f"The field {created_timestamp_column} is mapped to {field_mapping[created_timestamp_column]}. Please either remove this field mapping or use {field_mapping[created_timestamp_column]} as the _timestamp_column."
                )

        self.table_ref = table_ref
        self.event_timestamp_column = event_timestamp_column
        self.created_timestamp_column = created_timestamp_column
        self.field_mapping = field_mapping
        self.query = query
        return
