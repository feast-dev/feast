import contextlib
import os
import tempfile
import uuid
from typing import Iterator, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import retry, retry_if_exception_type, wait_exponential

from feast.errors import RedshiftCredentialsError, RedshiftQueryError
from feast.type_map import pa_to_redshift_value_type

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError, ConnectionClosedError
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))


def get_redshift_data_client(aws_region: str):
    """
    Get the Redshift Data API Service client for the given AWS region.
    """
    return boto3.client("redshift-data", config=Config(region_name=aws_region))


def get_s3_resource(aws_region: str):
    """
    Get the S3 resource for the given AWS region.
    """
    return boto3.resource("s3", config=Config(region_name=aws_region))


def get_bucket_and_key(s3_path: str) -> Tuple[str, str]:
    """
    Get the S3 bucket and key given the full path.

    For example get_bucket_and_key("s3://foo/bar/test.file") returns ("foo", "bar/test.file")

    If the s3_path doesn't start with "s3://", it throws ValueError.
    """
    assert s3_path.startswith("s3://")
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    return bucket, key


@retry(
    wait=wait_exponential(multiplier=1, max=4),
    retry=retry_if_exception_type(ConnectionClosedError),
)
def execute_redshift_statement_async(
    redshift_data_client, cluster_id: str, database: str, user: str, query: str
) -> dict:
    """Execute Redshift statement asynchronously. Does not wait for the query to finish.

    Raises RedshiftCredentialsError if the statement couldn't be executed due to the validation error.

    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        database: Redshift Database Name
        user: Redshift username
        query: The SQL query to execute

    Returns: JSON response

    """
    try:
        return redshift_data_client.execute_statement(
            ClusterIdentifier=cluster_id, Database=database, DbUser=user, Sql=query,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ValidationException":
            raise RedshiftCredentialsError() from e
        raise


class RedshiftStatementNotFinishedError(Exception):
    pass


@retry(
    wait=wait_exponential(multiplier=1, max=30),
    retry=retry_if_exception_type(RedshiftStatementNotFinishedError),
)
def wait_for_redshift_statement(redshift_data_client, statement: dict) -> None:
    """Waits for the Redshift statement to finish. Raises RedshiftQueryError if the statement didn't succeed.

    We use exponential backoff for checking the query state until it's not running. The backoff starts with
    0.1 seconds and doubles exponentially until reaching 30 seconds, at which point the backoff is fixed.

    Args:
        redshift_data_client:  Redshift Data API Service client
        statement:  The redshift statement to wait for (result of execute_redshift_statement)

    Returns: None

    """
    desc = redshift_data_client.describe_statement(Id=statement["Id"])
    if desc["Status"] in ("SUBMITTED", "STARTED", "PICKED"):
        raise RedshiftStatementNotFinishedError  # Retry
    if desc["Status"] != "FINISHED":
        raise RedshiftQueryError(desc)  # Don't retry. Raise exception.


def execute_redshift_statement(
    redshift_data_client, cluster_id: str, database: str, user: str, query: str
) -> str:
    """Execute Redshift statement synchronously. Waits for the query to finish.

    Raises RedshiftCredentialsError if the statement couldn't be executed due to the validation error.
    Raises RedshiftQueryError if the query runs but finishes with errors.


    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        database: Redshift Database Name
        user: Redshift username
        query: The SQL query to execute

    Returns: Statement ID

    """
    statement = execute_redshift_statement_async(
        redshift_data_client, cluster_id, database, user, query
    )
    wait_for_redshift_statement(redshift_data_client, statement)
    return statement["Id"]


def get_redshift_statement_result(redshift_data_client, statement_id: str) -> dict:
    """ Get the Redshift statement result """
    return redshift_data_client.get_statement_result(Id=statement_id)


def upload_df_to_s3(s3_resource, s3_path: str, df: pd.DataFrame,) -> None:
    """Uploads a Pandas DataFrame to S3 as a parquet file

    Args:
        s3_resource: S3 Resource object
        s3_path: S3 path where the Parquet file is temporarily uploaded
        df: The Pandas DataFrame to upload

    Returns: None

    """
    bucket, key = get_bucket_and_key(s3_path)

    # Drop the index so that we dont have unnecessary columns
    df.reset_index(drop=True, inplace=True)

    table = pa.Table.from_pandas(df)
    # Write the PyArrow Table on disk in Parquet format and upload it to S3
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = f"{temp_dir}/{uuid.uuid4()}.parquet"
        pq.write_table(table, file_path)
        s3_resource.Object(bucket, key).put(Body=open(file_path, "rb"))


def upload_df_to_redshift(
    redshift_data_client,
    cluster_id: str,
    database: str,
    user: str,
    s3_resource,
    s3_path: str,
    iam_role: str,
    table_name: str,
    df: pd.DataFrame,
) -> None:
    """Uploads a Pandas DataFrame to Redshift as a new table.

    The caller is responsible for deleting the table when no longer necessary.

    Here's how the upload process works:
        1. Pandas DataFrame is converted to PyArrow Table
        2. PyArrow Table is serialized into a Parquet format on local disk
        3. The Parquet file is uploaded to S3
        4. The S3 file is uploaded to Redshift as a new table through COPY command
        5. The local disk & s3 paths are cleaned up

    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        database: Redshift Database Name
        user: Redshift username
        s3_resource: S3 Resource object
        s3_path: S3 path where the Parquet file is temporarily uploaded
        iam_role: IAM Role for Redshift to assume during the COPY command.
                  The role must grant permission to read the S3 location.
        table_name: The name of the new Redshift table where we copy the dataframe
        df: The Pandas DataFrame to upload

    Returns: None

    """
    bucket, key = get_bucket_and_key(s3_path)

    # Drop the index so that we dont have unnecessary columns
    df.reset_index(drop=True, inplace=True)

    # Convert Pandas DataFrame into PyArrow table and compile the Redshift table schema.
    # Note, if the underlying data has missing values,
    # pandas will convert those values to np.nan if the dtypes are numerical (floats, ints, etc.) or boolean.
    # If the dtype is 'object', then missing values are inferred as python `None`s.
    # More details at:
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#values-considered-missing
    table = pa.Table.from_pandas(df)
    column_names, column_types = [], []
    for field in table.schema:
        column_names.append(field.name)
        column_types.append(pa_to_redshift_value_type(field.type))
    column_query_list = ", ".join(
        [
            f"{column_name} {column_type}"
            for column_name, column_type in zip(column_names, column_types)
        ]
    )

    # Write the PyArrow Table on disk in Parquet format and upload it to S3
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = f"{temp_dir}/{uuid.uuid4()}.parquet"
        pq.write_table(table, file_path)
        s3_resource.Object(bucket, key).put(Body=open(file_path, "rb"))

    # Create the table with the desired schema and
    # copy the Parquet file contents to the Redshift table
    create_and_copy_query = (
        f"CREATE TABLE {table_name}({column_query_list}); "
        + f"COPY {table_name} FROM '{s3_path}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET"
    )
    execute_redshift_statement(
        redshift_data_client, cluster_id, database, user, create_and_copy_query
    )

    # Clean up S3 temporary data
    s3_resource.Object(bucket, key).delete()


@contextlib.contextmanager
def temporarily_upload_df_to_redshift(
    redshift_data_client,
    cluster_id: str,
    database: str,
    user: str,
    s3_resource,
    s3_path: str,
    iam_role: str,
    table_name: str,
    df: pd.DataFrame,
) -> Iterator[None]:
    """Uploads a Pandas DataFrame to Redshift as a new table with cleanup logic.

    This is essentially the same as upload_df_to_redshift (check out its docstring for full details),
    but unlike it this method is a generator and should be used with `with` block. For example:

    >>> with temporarily_upload_df_to_redshift(...): # doctest: +SKIP
    >>>     # Use `table_name` table in Redshift here
    >>> # `table_name` will not exist at this point, since it's cleaned up by the `with` block

    """
    # Upload the dataframe to Redshift
    upload_df_to_redshift(
        redshift_data_client,
        cluster_id,
        database,
        user,
        s3_resource,
        s3_path,
        iam_role,
        table_name,
        df,
    )

    yield

    # Clean up the uploaded Redshift table
    execute_redshift_statement(
        redshift_data_client, cluster_id, database, user, f"DROP TABLE {table_name}",
    )


def download_s3_directory(s3_resource, bucket: str, key: str, local_dir: str):
    """ Download the S3 directory to a local disk """
    bucket_obj = s3_resource.Bucket(bucket)
    if key != "" and not key.endswith("/"):
        key = key + "/"
    for obj in bucket_obj.objects.filter(Prefix=key):
        local_file_path = local_dir + "/" + obj.key[len(key) :]
        local_file_dir = os.path.dirname(local_file_path)
        os.makedirs(local_file_dir, exist_ok=True)
        bucket_obj.download_file(obj.key, local_file_path)


def delete_s3_directory(s3_resource, bucket: str, key: str):
    """ Delete S3 directory recursively """
    bucket_obj = s3_resource.Bucket(bucket)
    if key != "" and not key.endswith("/"):
        key = key + "/"
    for obj in bucket_obj.objects.filter(Prefix=key):
        obj.delete()


def execute_redshift_query_and_unload_to_s3(
    redshift_data_client,
    cluster_id: str,
    database: str,
    user: str,
    s3_path: str,
    iam_role: str,
    query: str,
    drop_columns: Optional[List[str]] = None,
) -> None:
    """Unload Redshift Query results to S3

    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        database: Redshift Database Name
        user: Redshift username
        s3_path: S3 directory where the unloaded data is written
        iam_role: IAM Role for Redshift to assume during the UNLOAD command.
                  The role must grant permission to write to the S3 location.
        query: The SQL query to execute
        drop_columns: Optionally a list of columns to drop before unloading to S3.
                      This is a convenient field, since "SELECT ... EXCEPT col" isn't supported in Redshift.

    """
    # Run the query, unload the results to S3
    unique_table_name = "_" + str(uuid.uuid4()).replace("-", "")
    query = f"CREATE TEMPORARY TABLE {unique_table_name} AS ({query});\n"
    if drop_columns is not None:
        for column in drop_columns:
            query += f"ALTER TABLE {unique_table_name} DROP COLUMN {column};\n"
    query += f"UNLOAD ('SELECT * FROM {unique_table_name}') TO '{s3_path}/' IAM_ROLE '{iam_role}' PARQUET"
    execute_redshift_statement(redshift_data_client, cluster_id, database, user, query)


def unload_redshift_query_to_pa(
    redshift_data_client,
    cluster_id: str,
    database: str,
    user: str,
    s3_resource,
    s3_path: str,
    iam_role: str,
    query: str,
    drop_columns: Optional[List[str]] = None,
) -> pa.Table:
    """ Unload Redshift Query results to S3 and get the results in PyArrow Table format """
    bucket, key = get_bucket_and_key(s3_path)

    execute_redshift_query_and_unload_to_s3(
        redshift_data_client,
        cluster_id,
        database,
        user,
        s3_path,
        iam_role,
        query,
        drop_columns,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        download_s3_directory(s3_resource, bucket, key, temp_dir)
        delete_s3_directory(s3_resource, bucket, key)
        return pq.read_table(temp_dir)


def unload_redshift_query_to_df(
    redshift_data_client,
    cluster_id: str,
    database: str,
    user: str,
    s3_resource,
    s3_path: str,
    iam_role: str,
    query: str,
    drop_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """ Unload Redshift Query results to S3 and get the results in Pandas DataFrame format """
    table = unload_redshift_query_to_pa(
        redshift_data_client,
        cluster_id,
        database,
        user,
        s3_resource,
        s3_path,
        iam_role,
        query,
        drop_columns,
    )
    return table.to_pandas()
