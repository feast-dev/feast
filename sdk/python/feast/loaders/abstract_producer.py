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

from typing import Optional, Union

from tqdm import tqdm


class AbstractProducer:
    """
    Abstract class for Kafka producers
    """

    def __init__(self, brokers: str, row_count: int, disable_progress_bar: bool):
        self.brokers = brokers
        self.row_count = row_count

        # Progress bar will always display average rate
        self.pbar = tqdm(
            total=row_count, unit="rows", smoothing=0, disable=disable_progress_bar
        )

    def produce(self, topic: str, data: bytes):
        message = "{} should implement a produce method".format(self.__class__.__name__)
        raise NotImplementedError(message)

    def flush(self, timeout: int):
        message = "{} should implement a flush method".format(self.__class__.__name__)
        raise NotImplementedError(message)

    def _inc_pbar(self, meta):
        self.pbar.update(1)

    def _set_error(self, exception: str):
        raise Exception(exception)

    def print_results(self) -> None:
        """
        Print ingestion statistics.

        Returns:
            None: None
        """
        # Refresh and close tqdm progress bar
        self.pbar.refresh()

        self.pbar.close()

        print("Ingestion complete!")

        print(f"\nIngestion statistics:" f"\nSuccess: {self.pbar.n}/{self.row_count}")
        return None


class ConfluentProducer(AbstractProducer):
    """
    Concrete implementation of Confluent Kafka producer (confluent-kafka)
    """

    def __init__(self, brokers: str, row_count: int, disable_progress_bar: bool):
        from confluent_kafka import Producer

        self.producer = Producer({"bootstrap.servers": brokers})
        super().__init__(brokers, row_count, disable_progress_bar)

    def produce(self, topic: str, value: bytes) -> None:
        """
        Generic produce that implements confluent-kafka's produce method to
        push a byte encoded object into a Kafka topic.

        Args:
            topic (str): Kafka topic.
            value (bytes): Byte encoded object.

        Returns:
            None: None.
        """

        try:
            self.producer.produce(topic, value=value, callback=self._delivery_callback)
            # Serve delivery callback queue.
            # NOTE: Since produce() is an asynchronous API this poll() call
            #       will most likely not serve the delivery callback for the
            #       last produce()d message.
            self.producer.poll(0)
        except Exception as ex:
            self._set_error(str(ex))

        return None

    def flush(self, timeout: Optional[int]):
        """
        Generic flush that implements confluent-kafka's flush method.

        Args:
            timeout (Optional[int]): Timeout in seconds to wait for completion.

        Returns:
            int: Number of messages still in queue.
        """
        messages = self.producer.flush(timeout=timeout)
        if messages:
            raise Exception("Not all Kafka messages are successfully delivered.")
        return messages

    def _delivery_callback(self, err: str, msg) -> None:
        """
        Optional per-message delivery callback (triggered by poll() or flush())
        when a message has been successfully delivered or permanently failed
        delivery (after retries).

        Although the msg argument is not used, the current method signature is
        required as specified in the confluent-kafka documentation.

        Args:
            err (str): Error message.
            msg (): Kafka message.

        Returns:
            None
        """
        if err:
            self._set_error(err)
        else:
            self._inc_pbar(None)


class KafkaPythonProducer(AbstractProducer):
    """
    Concrete implementation of Python Kafka producer (kafka-python)
    """

    def __init__(self, brokers: str, row_count: int, disable_progress_bar: bool):
        from kafka import KafkaProducer

        self.producer = KafkaProducer(bootstrap_servers=[brokers])
        super().__init__(brokers, row_count, disable_progress_bar)

    def produce(self, topic: str, value: bytes):
        """
        Generic produce that implements kafka-python's send method to push a
        byte encoded object into a Kafka topic.

        Args:
            topic (str): Kafka topic.
            value (bytes): Byte encoded object.

        Returns:
            FutureRecordMetadata: resolves to RecordMetadata

        Raises:
            KafkaTimeoutError: if unable to fetch topic metadata, or unable
                to obtain memory buffer prior to configured max_block_ms
        """
        return (
            self.producer.send(topic, value=value)
            .add_callback(self._inc_pbar)
            .add_errback(self._set_error)
        )

    def flush(self, timeout: Optional[int]):
        """
        Generic flush that implements kafka-python's flush method.

        Args:
            timeout (Optional[int]): timeout in seconds to wait for completion.

        Returns:
            None

        Raises:
            KafkaTimeoutError: failure to flush buffered records within the
                provided timeout
        """
        messages = self.producer.flush(timeout=timeout)
        if messages:
            raise Exception("Not all Kafka messages are successfully delivered.")
        return messages


def get_producer(
    brokers: str, row_count: int, disable_progress_bar: bool
) -> Union[ConfluentProducer, KafkaPythonProducer]:
    """
    Simple context helper function that returns a AbstractProducer object when
    invoked.

    This helper function will try to import confluent-kafka as a producer first.

    This helper function will fallback to kafka-python if it fails to import
    confluent-kafka.

    Args:
        brokers (str): Kafka broker information with hostname and port.
        row_count (int): Number of rows in table

    Returns:
        Union[ConfluentProducer, KafkaPythonProducer]:
            Concrete implementation of a Kafka producer. Ig can be:
                * confluent-kafka producer
                * kafka-python producer
    """
    try:
        return ConfluentProducer(brokers, row_count, disable_progress_bar)
    except ImportError:
        print("Unable to import confluent-kafka, falling back to kafka-python")
        return KafkaPythonProducer(brokers, row_count, disable_progress_bar)
