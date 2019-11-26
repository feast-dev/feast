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

import logging
import multiprocessing
import os
import time
from itertools import repeat
from multiprocessing import Process, Queue, Pool
from typing import List
from typing import Tuple, Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
from feast.feature_set import FeatureSet
from feast.type_map import convert_df_to_feature_rows
from feast.types.FeatureRow_pb2 import FeatureRow
from kafka import KafkaProducer
from tqdm import tqdm

_logger = logging.getLogger(__name__)

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 300  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300
CPU_COUNT = os.cpu_count()  # type: int
KAFKA_CHUNK_PRODUCTION_TIMEOUT = 120  # type: int


def _kafka_feature_row_chunk_producer(
        feature_row_chunk_queue: Queue,
        chunk_count: int,
        brokers,
        topic,
        ctx: dict,
        pbar: tqdm,
):
    # Callback for failed production to Kafka
    def on_error(e):
        # Save last exception
        ctx["last_exception"] = e

        # Increment error count
        if "error_count" in ctx:
            ctx["error_count"] += 1
        else:
            ctx["error_count"] = 1

    # Callback for succeeded production to Kafka
    def on_success(meta):
        pbar.update()

    producer = KafkaProducer(bootstrap_servers=brokers)
    processed_chunks = 0

    while processed_chunks < chunk_count:
        if feature_row_chunk_queue.empty():
            time.sleep(0.1)
        else:
            feature_rows = feature_row_chunk_queue.get()
            for row in feature_rows:
                producer.send(topic, row.SerializeToString()).add_callback(
                    on_success
                ).add_errback(on_error)
            producer.flush(timeout=KAFKA_CHUNK_PRODUCTION_TIMEOUT)
            processed_chunks += 1
            pbar.refresh()
    # Using progress bar as counter is much faster than incrementing dict
    ctx["success_count"] = pbar.n
    pbar.close()


# TODO: This function is not in use.
def _encode_chunk(df: pd.DataFrame, feature_set: FeatureSet) \
        -> List[FeatureRow]:
    """
    Encode DataFrame chunk into feature rows chunk.

    Args:
        df (pd.DataFrame): DataFrame to encode.
        feature_set (FeatureSet): FeatureSet describing the DataFrame.

    Returns:
        List[FeatureRow]:
            List of FeatureRow objects.
    """
    return df.apply(convert_df_to_feature_rows(df, feature_set), axis=1,
                    raw=True)


# TODO: This function is not in use.
def encode_df_chunks(df: pd.DataFrame, feature_set: FeatureSet,
                     chunk_size: int = None) \
        -> Iterable[Tuple[int, List[FeatureRow]]]:
    """
    Generator function to encode chunks of DataFrame into a chunked list of
    FeatureRow objects.

    Args:
        df (pd.DataFrame):
            DataFrame to encode.
        feature_set (FeatureSet):
            FeatureSet describing the DataFrame.
        chunk_size (int):
            Size of DataFrame to encode.

    Returns:
        Iterable[Tuple[int, List[FeatureRow]]]:
            Iterable tuple containing the remaining rows in generator and a
            list of FeatureRow objects generated from encoding a chunk of
            DataFrame.
    """
    df = df.reset_index(drop=True)
    if chunk_size is None:
        # Encode the entire DataFrame
        yield 0, _encode_chunk(df, feature_set)
        return

    remaining_rows = len(df)
    total_rows = remaining_rows
    start_index = 0
    while start_index < total_rows:
        end_index = start_index + chunk_size
        chunk_buffer = _encode_chunk(df[start_index:end_index], feature_set)
        start_index += chunk_size
        remaining_rows = max(0, remaining_rows - chunk_size)
        yield remaining_rows, chunk_buffer


# TODO: This function is not in use.
def encode_pa_chunks(table: pa.lib.Table,
                     feature_set: FeatureSet,
                     chunk_size: int = None) \
        -> Iterable[Tuple[int, List[FeatureRow]]]:
    """
    Generator function to encode chunks of PyArrow table of type RecordBatch
    into a chunked list of FeatureRow objects.

    Args:
        table (pa.lib.Table):
            PyArrow table.
        feature_set (FeatureSet):
            FeatureSet describing the PyArrow table.
        chunk_size (int):
            Size of DataFrame to encode.

    Returns:
        Iterable[Tuple[int, List[FeatureRow]]]:
            Iterable tuple containing the remaining batches in generator and a
            list of FeatureRow objects generated from encoding a RecordBatch of
            PyArrow table.
    """
    if chunk_size is None:
        # Encode the entire table
        yield 0, _encode_chunk(table.to_pandas(), feature_set)
        return

    batches = table.to_batches(max_chunksize=chunk_size)
    remaining_batches = len(batches)
    for batch in batches:
        chunk_buffer = _encode_chunk(batch.to_pandas(), feature_set)
        remaining_batches -= 1
        yield remaining_batches, chunk_buffer


def ingest_kafka(
        feature_set: FeatureSet,
        dataframe: pd.DataFrame,
        max_workers: int,
        timeout: int = None,
        chunk_size: int = 5000,
        disable_pbar: bool = False,
):
    pbar = tqdm(unit="rows", total=dataframe.shape[0], disable=disable_pbar)

    # Validate feature set schema
    validate_dataframe(dataframe, feature_set)

    # Split dataframe into chunks
    num_chunks = max(dataframe.shape[0] / max(chunk_size, 100), 1)
    df_chunks = np.array_split(dataframe, num_chunks)

    # Create queue through which encoding and production will coordinate
    chunk_queue = Queue()

    # Create a context object to send and receive information across processes
    ctx = multiprocessing.Manager().dict(
        {"success_count": 0, "error_count": 0, "last_exception": ""}
    )

    # Create producer to push feature rows to Kafka
    ingestion_process = Process(
        target=_kafka_feature_row_chunk_producer,
        args=(
            chunk_queue,
            num_chunks,
            feature_set.get_kafka_source_brokers(),
            feature_set.get_kafka_source_topic(),
            ctx,
            pbar,
        ),
    )

    try:
        # Start ingestion process
        print(
            f"\nIngestion started for {feature_set.name}:{feature_set.version}")
        ingestion_process.start()

        # Create a pool of workers to convert df chunks into feature row chunks
        # and push them into the queue for ingestion to pick up
        with Pool(processes=max_workers) as pool:
            chunks_done = 0
            while chunks_done < num_chunks:
                chunks_to = min(chunks_done + max_workers, len(df_chunks))
                results = pool.starmap_async(
                    _encode_chunk,
                    zip(df_chunks[chunks_done:chunks_to], repeat(feature_set)),
                )

                # Push feature row encoded chunks onto queue
                for result in results.get():
                    chunk_queue.put(result)
                chunks_done += max_workers
    except Exception as ex:
        _logger.error(f"Exception occurred: {ex}")
    finally:
        # Wait for ingestion to complete, or time out
        ingestion_process.join(timeout=timeout)
        failed_message = (
            ""
            if ctx["error_count"] == 0
            else f"\nFail: {ctx['error_count']}/{dataframe.shape[0]}"
        )

        last_exception_message = (
            ""
            if ctx["last_exception"] == ""
            else f"\nLast exception:\n{ctx['last_exception']}"
        )
        print(
            f"\nIngestion statistics:"
            f"\nSuccess: {ctx['success_count']}/{dataframe.shape[0]}"
            f"{failed_message}"
            f"{last_exception_message}"
        )


def validate_dataframe(dataframe: pd.DataFrame, fs: FeatureSet) -> None:
    """
    Validate a DataFrame to check if all entity and feature names described
    in FeatureSet are present in the DataFrame columns.

    An error will be raised if there are no matching entity/feature names in
    FeatureSet and DataFrame.

    Args:
        dataframe (pd.DataFrame):
            Pandas DataFrame to be validated.
        fs (FeatureSet):
            FeatureSet that DataFrame should be validated against.

    Returns:
        None
    """
    if "datetime" not in dataframe.columns:
        raise ValueError(
            f'DataFrame does not contain entity "datetime" in columns '
            f'{dataframe.columns}'
        )

    for entity in fs.entities:
        if entity.name not in dataframe.columns:
            raise ValueError(
                f"DataFrame does not contain entity {entity.name} in columns "
                f"{dataframe.columns}"
            )

    for feature in fs.features:
        if feature.name not in dataframe.columns:
            raise ValueError(
                f"DataFrame does not contain feature {feature.name} in columns "
                f"{dataframe.columns}"
            )
