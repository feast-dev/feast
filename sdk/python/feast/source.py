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


class Source:
    pass


class KafkaSource:
    def __init__(self, brokers: str, topics: str):
        if brokers and topics:
            self._brokers = brokers
            self._topics = topics
            self._source_type = "Kafka"
        else:
            raise ValueError("Missing configuration parameters in Kafka source")

    @property
    def brokers(self):
        return self._brokers

    @property
    def topics(self):
        return self._topics

    @property
    def source_type(self):
        return self._source_type
