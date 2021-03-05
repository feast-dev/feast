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

from feast.feature_store_config import Config


class FeatureStore:
    """
    A FeatureStore object is used to define, create, and retrieve features.
    """

    def __init__(
        self, config_path: Optional[str], config: Optional[Config],
    ):
        if config_path is None or config is None:
            raise Exception("You cannot specify both config_path and config")
        if config is not None:
            self.config = config
        elif config_path is not None:
            self.config = Config.from_config_path(config_path)
        else:
            self.config = Config()
