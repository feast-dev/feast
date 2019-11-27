import logging
import multiprocessing
import os
import time
from functools import partial
from itertools import repeat
from multiprocessing import Process, Queue, Pool
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
from feast.feature_set import FeatureSet
from feast.type_map import convert_df_to_feature_rows, \
    convert_dict_to_proto_values
from feast.types.FeatureRow_pb2 import FeatureRow
from kafka import KafkaProducer
from tqdm import tqdm
from feast.constants import DATETIME_COLUMN

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


def _encode_chunk(df: pd.DataFrame, feature_set: FeatureSet):
    # Encode DataFrame chunk into feature rows chunk
    return df.apply(convert_df_to_feature_rows(df, feature_set), axis=1, raw=True)


def _encode_pa_chunks(
        tbl: pa.lib.Table,
        fs: FeatureSet,
        max_workers: int,
        df_datetime_dtype: pd.DataFrame.dtypes,
        chunk_size: int = 5000
) -> Iterable[FeatureRow]:
    """
    Generator function to encode rows in PyArrow table to FeatureRows by
    breaking up the table into batches.

    Each batch will have its rows spread accross a pool of workers to be
    transformed into FeatureRow objects.

    :param tbl: PyArrow table to be processed.
    :type tbl: pa.lib.Table
    :param fs: FeatureSet describing PyArrow table.
    :type fs: FeatureSet
    :param max_workers: Maximum number of workers.
    :type max_workers: int
    :param df_datetime_dtype: Pandas dtype of datetime column.
    :type df_datetime_dtype: pd.DataFrame.dtypes
    :param chunk_size: Maximum size of each chunk when PyArrow table is batched.
    :type chunk_size: int
    :return: Iterable FeatureRow object.
    :rtype: Iterable[FeatureRow]
    """
    pool = Pool(max_workers)

    # Create a partial function with static non-iterable arguments
    func = partial(convert_dict_to_proto_values,
                   df_datetime_dtype=df_datetime_dtype,
                   feature_set=fs)

    for batch in tbl.to_batches(max_chunksize=chunk_size):
        m_df = batch.to_pandas()
        results = pool.map_async(func, m_df.to_dict("records"))
        yield from results.get()

    pool.close()
    pool.join()
    return


def ingest_table_to_kafka(
        feature_set: FeatureSet,
        tbl: pa.lib.Table,
        max_workers: int,
        chunk_size: int = 5000,
        disable_pbar: bool = False,
) -> None:
    """

    :param feature_set: FeatureSet describing PyArrow table.
    :type feature_set: FeatureSet
    :param tbl: PyArrow table to be processed.
    :type tbl: pa.lib.Table
    :param max_workers: Maximum number of workers.
    :type max_workers: int
    :param chunk_size: Maximum size of each chunk when PyArrow table is batched.
    :type chunk_size: int
    :param disable_pbar: Flag to indicate if tqdm progress bar should be
        disabled.
    :type disable_pbar: bool
    :return: None
    :rtype: None
    """
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

    pbar = tqdm(unit="rows", total=tbl.num_rows, disable=disable_pbar)

    # Use a small DataFrame to validate feature set schema
    ref_df = tbl.to_batches(max_chunksize=100)[0]
    df_datetime_dtype = ref_df[DATETIME_COLUMN].dtype

    # Validate feature set schema
    validate_dataframe(ref_df, feature_set)

    # Create a context object to track errors
    ctx = {"success_count": 0, "error_count": 0, "last_exception": ""}

    brokers = feature_set.get_kafka_source_brokers()
    topic = feature_set.get_kafka_source_topic()
    producer = KafkaProducer(bootstrap_servers=brokers)

    try:
        for row in _encode_pa_chunks(tbl=tbl,
                                     fs=feature_set,
                                     max_workers=max_workers,
                                     chunk_size=chunk_size,
                                     df_datetime_dtype=df_datetime_dtype):
            producer.send(topic, row.SerializeToString()).add_callback(
                on_success
            ).add_errback(on_error)

            producer.flush(timeout=KAFKA_CHUNK_PRODUCTION_TIMEOUT)
            pbar.refresh()

        # Using progress bar as counter is much faster than incrementing dict
        ctx["success_count"] = pbar.n
        pbar.close()
    except Exception as ex:
        _logger.error(f"Exception occurred: {ex}")
    finally:
        failed_message = (
            ""
            if ctx["error_count"] == 0
            else f"\nFail: {ctx['error_count']}/{tbl.num_rows}"
        )

        last_exception_message = (
            ""
            if ctx["last_exception"] == ""
            else f"\nLast exception:\n{ctx['last_exception']}"
        )
        print(
            f"\nIngestion statistics:"
            f"\nSuccess: {ctx['success_count']}/{tbl.num_rows}"
            f"{failed_message}"
            f"{last_exception_message}"
        )


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
        print(f"\nIngestion started for {feature_set.name}:{feature_set.version}")
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


def validate_dataframe(dataframe: pd.DataFrame, fs: FeatureSet):
    if "datetime" not in dataframe.columns:
        raise ValueError(
            f'Dataframe does not contain entity "datetime" in columns {dataframe.columns}'
        )

    for entity in fs.entities:
        if entity.name not in dataframe.columns:
            raise ValueError(
                f"Dataframe does not contain entity {entity.name} in columns {dataframe.columns}"
            )

    for feature in fs.features:
        if feature.name not in dataframe.columns:
            raise ValueError(
                f"Dataframe does not contain feature {feature.name} in columns {dataframe.columns}"
            )
