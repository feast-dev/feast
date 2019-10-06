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
from feast.core.Source_pb2 import (
    Source as SourceProto,
    KafkaSourceConfig as KafkaSourceConfigProto,
    SourceType as SourceTypeProto,
)


class Source:
    def __eq__(self, other):
        return True

    @property
    def source_type(self) -> str:
        return "None"

    def to_proto(self):
        return None

    @classmethod
    def from_proto(cls, source_proto: SourceProto):
        if source_proto.type == SourceTypeProto.KAFKA:
            return KafkaSource(
                brokers=source_proto.kafka_source_config.bootstrap_servers,
                topic=source_proto.kafka_source_config.topic,
            )

        return cls()


class KafkaSource(Source):
    def __init__(self, brokers: str = "", topic: str = ""):
        self._source_type = "Kafka"
        self._brokers = brokers
        self._topic = topic

    def __eq__(self, other):
        if (
            self.brokers != other.brokers
            or self.topic != other.topic
            or self.source_type != other.source_type
        ):
            return False
        return True

    @property
    def brokers(self):
        return self._brokers

    @property
    def topic(self):
        return self._topic

    @property
    def source_type(self):
        return self._source_type

    def to_proto(self) -> SourceProto:
        return SourceProto(
            type=SourceTypeProto.KAFKA,
            kafka_source_config=KafkaSourceConfigProto(
                bootstrap_servers=self.brokers, topic=self.topic
            ),
        )
