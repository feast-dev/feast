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
    def __init__(
        self,
        table_ref: Optional[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        field_mapping: Optional[Dict[str, str]],
        query: Optional[str],
    ):
        assert (table_ref is None) != (query is None), "Exactly one of table_ref and query should be specified"
        self.table_ref = table_ref
        self.event_timestamp_column = event_timestamp_column
        self.created_timestamp_column = created_timestamp_column
        self.field_mapping = field_mapping
        self.query = query
        return
