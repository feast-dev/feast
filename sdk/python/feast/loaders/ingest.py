import logging
import multiprocessing
import os
import time
from functools import partial
from multiprocessing import Process, Queue, Pool
from typing import Iterable
import pandas as pd
import pyarrow as pa
from feast.feature_set import FeatureSet
from feast.type_map import convert_dict_to_proto_values
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


def _kafka_feature_row_producer(
    feature_row_queue: Queue, row_count: int, brokers, topic, ctx: dict, pbar: tqdm
):
    """
    Pushes Feature Rows to Kafka. Reads rows from a queue. Function will run
    until total row_count is reached.

    Args:
        feature_row_queue: Queue containing feature rows.
        row_count: Total row count to process
        brokers: Broker to push to
        topic: Topic to push to
        ctx: Context dict used to communicate with primary process
        pbar: Progress bar object
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

    producer = KafkaProducer(bootstrap_servers=brokers)
    processed_rows = 0

    # Loop through feature rows until all rows are processed
    while processed_rows < row_count:
        # Wait if queue is empty
        if feature_row_queue.empty():
            time.sleep(1)
            producer.flush(timeout=KAFKA_CHUNK_PRODUCTION_TIMEOUT)
        else:
            while not feature_row_queue.empty():
                row = feature_row_queue.get()
                if row is not None:
                    # Push row to Kafka
                    producer.send(topic, row.SerializeToString()).add_callback(
                        on_success
                    ).add_errback(on_error)
                processed_rows += 1

                # Force an occasional flush
                if processed_rows % 10000 == 0:
                    producer.flush(timeout=KAFKA_CHUNK_PRODUCTION_TIMEOUT)
                del row
            pbar.refresh()

    # Ensure that all rows are pushed
    producer.flush(timeout=KAFKA_CHUNK_PRODUCTION_TIMEOUT)

    # Using progress bar as counter is much faster than incrementing dict
    ctx["success_count"] = pbar.n
    pbar.close()


def _encode_pa_chunks(
    tbl: pa.lib.Table,
    fs: FeatureSet,
    max_workers: int,
    df_datetime_dtype: pd.DataFrame.dtypes,
    chunk_size: int = 5000,
) -> Iterable[FeatureRow]:
    """
    Generator function to encode rows in PyArrow table to FeatureRows by
    breaking up the table into batches.

    Each batch will have its rows spread accross a pool of workers to be
    transformed into FeatureRow objects.

    Args:
        tbl: PyArrow table to be processed.
        fs: FeatureSet describing PyArrow table.
        max_workers: Maximum number of workers.
        df_datetime_dtype: Pandas dtype of datetime column.
        chunk_size: Maximum size of each chunk when PyArrow table is batched.

    Returns:
        Iterable FeatureRow object.
    """

    pool = Pool(max_workers)

    # Create a partial function with static non-iterable arguments
    func = partial(
        convert_dict_to_proto_values,
        df_datetime_dtype=df_datetime_dtype,
        feature_set=fs,
    )

    for batch in tbl.to_batches(max_chunksize=chunk_size):
        m_df = batch.to_pandas()
        results = pool.map_async(func, m_df.to_dict("records"))
        yield from results.get()

    pool.close()
    pool.join()
    return


def ingest_table_to_kafka(
    feature_set: FeatureSet,
    table: pa.lib.Table,
    max_workers: int,
    chunk_size: int = 5000,
    disable_pbar: bool = False,
    timeout: int = None,
) -> None:
    """
    Ingest a PyArrow Table to a Kafka topic based for a Feature Set

    Args:
        feature_set: FeatureSet describing PyArrow table.
        table: PyArrow table to be processed.
        max_workers: Maximum number of workers.
        chunk_size:  Maximum size of each chunk when PyArrow table is batched.
        disable_pbar: Flag to indicate if tqdm progress bar should be disabled.
        timeout: Maximum time before method times out
    """

    pbar = tqdm(unit="rows", total=table.num_rows, disable=disable_pbar)

    # Use a small DataFrame to validate feature set schema
    ref_df = table.to_batches(max_chunksize=100)[0].to_pandas()
    df_datetime_dtype = ref_df[DATETIME_COLUMN].dtype

    # Validate feature set schema
    _validate_dataframe(ref_df, feature_set)

    # Create queue through which encoding and production will coordinate
    row_queue = Queue()

    # Create a context object to send and receive information across processes
    ctx = multiprocessing.Manager().dict(
        {"success_count": 0, "error_count": 0, "last_exception": ""}
    )

    # Create producer to push feature rows to Kafka
    ingestion_process = Process(
        target=_kafka_feature_row_producer,
        args=(
            row_queue,
            table.num_rows,
            feature_set.get_kafka_source_brokers(),
            feature_set.get_kafka_source_topic(),
            ctx,
            pbar,
        ),
    )

    try:
        # Start ingestion process
        print(
            f"\n(ingest table to kafka) Ingestion started for {feature_set.name}:{feature_set.version}"
        )
        ingestion_process.start()

        # Iterate over chunks in the table and return feature rows
        for row in _encode_pa_chunks(
            tbl=table,
            fs=feature_set,
            max_workers=max_workers,
            chunk_size=chunk_size,
            df_datetime_dtype=df_datetime_dtype,
        ):
            # Push rows onto a queue for the production process to pick up
            row_queue.put(row)
            while row_queue.qsize() > chunk_size:
                time.sleep(0.1)
        row_queue.put(None)
    except Exception as ex:
        _logger.error(f"Exception occurred: {ex}")
    finally:
        # Wait for the Kafka production to complete
        ingestion_process.join(timeout=timeout)
        failed_message = (
            ""
            if ctx["error_count"] == 0
            else f"\nFail: {ctx['error_count']}/{table.num_rows}"
        )

        last_exception_message = (
            ""
            if ctx["last_exception"] == ""
            else f"\nLast exception:\n{ctx['last_exception']}"
        )
        print(
            f"\nIngestion statistics:"
            f"\nSuccess: {ctx['success_count']}/{table.num_rows}"
            f"{failed_message}"
            f"{last_exception_message}"
        )


def _validate_dataframe(dataframe: pd.DataFrame, feature_set: FeatureSet):
    """
    Validates a Pandas dataframe based on a feature set

    Args:
        dataframe:  Pandas dataframe
        feature_set: Feature Set instance
    """

    if "datetime" not in dataframe.columns:
        raise ValueError(
            f'Dataframe does not contain entity "datetime" in columns {dataframe.columns}'
        )

    for entity in feature_set.entities:
        if entity.name not in dataframe.columns:
            raise ValueError(
                f"Dataframe does not contain entity {entity.name} in columns {dataframe.columns}"
            )

    for feature in feature_set.features:
        if feature.name not in dataframe.columns:
            raise ValueError(
                f"Dataframe does not contain feature {feature.name} in columns {dataframe.columns}"
            )
