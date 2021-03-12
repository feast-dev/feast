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
from datetime import datetime
from typing import Dict, List

from feast.big_query_source import BigQuerySource
from feast.entity import Entity
from feast.feature import Feature


class FeatureView:
    """
    A FeatureView defines a logical grouping of servable features.
    """

    def __init__(
        self,
        name: str,
        entities: List[Entity],
        features: List[Feature],
        tags: Dict[str, str],
        ttl: str,
        online: bool,
        inputs: BigQuerySource,
        feature_start_time: datetime,
    ):
        cols = [entity.name for entity in entities] + [feat.name for feat in features]
        for col in cols:
            if inputs.field_mapping is not None and col in inputs.field_mapping.keys():
                raise ValueError(
                    f"The field {col} is mapped to {inputs.field_mapping[col]} for this data source. Please either remove this field mapping or use {inputs.field_mapping[col]} as the Entity or Feature name."
                )

        self.name = name
        self.entities = entities
        self.features = features
        self.tags = tags
        self.ttl = ttl
        self.online = online
        self.inputs = inputs
        self.feature_start_time = feature_start_time
        return
