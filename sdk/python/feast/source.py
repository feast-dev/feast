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
    """
    Source is the top level class that represents a data source for finding
    feature data. Source must be extended with specific implementations to
    be useful
    """

    def __eq__(self, other):
        return True

    @property
    def source_type(self) -> str:
        """
        The type of source. If not implemented, this will return "None"
        """
        return "None"

    def to_proto(self):
        """
        Converts this source object to its protobuf representation.
        """
        return None

    @classmethod
    def from_proto(cls, source_proto: SourceProto):
        """
        Creates a source from a protobuf representation. This will instantiate
        and return a specific source type, depending on the protobuf that is
        passed in.

        Args:
            source_proto: SourceProto python object

        Returns:
            Source object
        """
        if source_proto.type == SourceTypeProto.KAFKA:
            return KafkaSource(
                brokers=source_proto.kafka_source_config.bootstrap_servers,
                topic=source_proto.kafka_source_config.topic,
            )

        return cls()


class KafkaSource(Source):
    """
    Kafka feature set source type.
    """

    def __init__(self, brokers: str = "", topic: str = ""):
        """

        Args:
            brokers: Comma separated list of Kafka brokers/bootstrap server
                addresses, for example: my-host:9092,other-host:9092
            topic: Kafka topic to find feature rows for this feature set
        """
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
    def brokers(self) -> str:
        """
        Returns the list of broker addresses for this Kafka source
        """
        return self._brokers

    @property
    def topic(self) -> str:
        """
        Returns the topic for this feature set
        """
        return self._topic

    @property
    def source_type(self) -> str:
        """
        Returns the type of source. For a Kafka source this will always return
            "kafka"
        """
        return self._source_type

    def to_proto(self) -> SourceProto:
        """
        Converts this Source into its protobuf representation
        """
        return SourceProto(
            type=SourceTypeProto.KAFKA,
            kafka_source_config=KafkaSourceConfigProto(
                bootstrap_servers=self.brokers, topic=self.topic
            ),
        )
