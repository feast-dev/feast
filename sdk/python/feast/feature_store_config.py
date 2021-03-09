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
from typing import Optional

import yaml


class Config:
    """
    Configuration object that contains all possible configuration options for a FeatureStore.
    """

    def __init__(
        self,
        provider: Optional[str],
        online_store: Optional[str],
        metadata_store: Optional[str],
    ):
        self.provider = provider if (provider is not None) else "local"
        self.online_store = online_store if (online_store is not None) else "local"
        self.metadata_store = (
            metadata_store if (metadata_store is not None) else "./metadata_store"
        )

    @classmethod
    def from_path(cls, config_path: str):
        """
        Construct the configuration object from a filepath containing a yaml file.

        Example yaml file:

        provider: gcp
        online_store: firestore
        metadata_store: gs://my_bucket/metadata_store
        """
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
            return cls(
                provider=config_dict.get("provider"),
                online_store=config_dict.get("online_store"),
                metadata_store=config_dict.get("metadata_store"),
            )
