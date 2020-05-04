import logging
from functools import partial
from multiprocessing import Pool
from typing import Iterable, List

import pandas as pd
import pyarrow.parquet as pq

from feast.constants import DATETIME_COLUMN
from feast.feature_set import FeatureSet
from feast.type_map import (
    pa_column_to_proto_column,
    pa_column_to_timestamp_proto_column,
)
from feast.types import Field_pb2 as FieldProto
from feast.types.FeatureRow_pb2 import FeatureRow

_logger = logging.getLogger(__name__)

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 300  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300
KAFKA_CHUNK_PRODUCTION_TIMEOUT = 120  # type: int


def _encode_pa_tables(
    file: str, feature_set: str, fields: dict, ingestion_id: str, row_group_idx: int
) -> List[bytes]:
    """
    Helper function to encode a PyArrow table(s) read from parquet file(s) into
    FeatureRows.

    This function accepts a list of file directory pointing to many parquet
    files. All parquet files must have the same schema.

    Each parquet file will be read into as a table and encoded into FeatureRows
    using a pool of max_workers workers.

    Args:
        file (str):
            File directory of all the parquet file to encode.
            Parquet file must have more than one row group.

        feature_set (str):
            Feature set reference in the format f"{project}/{name}".

        fields (dict[str, enum.Enum.ValueType]):
            A mapping of field names to their value types.

        ingestion_id (str):
            UUID unique to this ingestion job.

        row_group_idx(int):
            Row group index to read and encode into byte like FeatureRow
            protobuf objects.

    Returns:
        List[bytes]:
            List of byte encoded FeatureRows from the parquet file.
    """
    pq_file = pq.ParquetFile(file)
    # Read parquet file as a PyArrow table
    table = pq_file.read_row_group(row_group_idx)

    # Add datetime column
    datetime_col = pa_column_to_timestamp_proto_column(table.column(DATETIME_COLUMN))

    # Preprocess the columns by converting all its values to Proto values
    proto_columns = {
        field_name: pa_column_to_proto_column(dtype, table.column(field_name))
        for field_name, dtype in fields.items()
    }

    # List to store result
    feature_rows = []

    # Loop optimization declaration(s)
    field = FieldProto.Field
    proto_items = proto_columns.items()
    append = feature_rows.append

    # Iterate through the rows
    for row_idx in range(table.num_rows):
        feature_row = FeatureRow(
            event_timestamp=datetime_col[row_idx],
            feature_set=feature_set,
            ingestion_id=ingestion_id,
        )
        # Loop optimization declaration
        ext = feature_row.fields.extend

        # Insert field from each column
        for k, v in proto_items:
            ext([field(name=k, value=v[row_idx])])

        # Append FeatureRow in byte string form
        append(feature_row.SerializeToString())

    return feature_rows


def get_feature_row_chunks(
    file: str,
    row_groups: List[int],
    fs: FeatureSet,
    ingestion_id: str,
    max_workers: int,
) -> Iterable[List[bytes]]:
    """
    Iterator function to encode a PyArrow table read from a parquet file to
    FeatureRow(s).

    Args:
        file (str):
            File directory of the parquet file. The parquet file must have more
            than one row group.

        row_groups (List[int]):
            Specific row group indexes to be read and transformed in the parquet
            file.

        fs (feast.feature_set.FeatureSet):
            FeatureSet describing parquet files.

        ingestion_id (str):
            UUID unique to this ingestion job.

        max_workers (int):
            Maximum number of workers to spawn.

    Returns:
        Iterable[List[bytes]]:
            Iterable list of byte encoded FeatureRow(s).
    """

    feature_set = f"{fs.project}/{fs.name}"

    field_map = {field.name: field.dtype for field in fs.fields.values()}

    pool = Pool(max_workers)
    func = partial(_encode_pa_tables, file, feature_set, field_map, ingestion_id)
    for chunk in pool.imap(func, row_groups):
        yield chunk
    return


def validate_dataframe(dataframe: pd.DataFrame, feature_set: FeatureSet):
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
