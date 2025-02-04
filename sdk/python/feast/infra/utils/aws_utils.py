import asyncio
import contextlib
import itertools
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd
import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import (
    AsyncRetrying,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from feast.errors import (
    RedshiftCredentialsError,
    RedshiftQueryError,
    RedshiftTableNameTooLong,
)
from feast.type_map import pa_to_athena_value_type, pa_to_redshift_value_type
from feast.utils import get_user_agent

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError, ConnectionClosedError
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))

REDSHIFT_TABLE_NAME_MAX_LENGTH = 127


def get_redshift_data_client(aws_region: str):
    """
    Get the Redshift Data API Service client for the given AWS region.
    """
    return boto3.client(
        "redshift-data",
        config=Config(region_name=aws_region, user_agent=get_user_agent()),
    )


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
    stop=stop_after_attempt(5),
    reraise=True,
)
def execute_redshift_statement_async(
    redshift_data_client,
    cluster_id: Optional[str],
    workgroup: Optional[str],
    database: str,
    user: Optional[str],
    query: str,
) -> dict:
    """Execute Redshift statement asynchronously. Does not wait for the query to finish.

    Raises RedshiftCredentialsError if the statement couldn't be executed due to the validation error.

    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        workgroup: Redshift Serverless Workgroup
        database: Redshift Database Name
        user: Redshift username
        query: The SQL query to execute

    Returns: JSON response

    """
    try:
        rs_kwargs = {"Database": database, "Sql": query}

        # Standard Redshift requires a ClusterId as well as DbUser.  RS Serverless instead requires a WorkgroupName.
        if cluster_id and user:
            rs_kwargs["ClusterIdentifier"] = cluster_id
            rs_kwargs["DbUser"] = user
        elif workgroup:
            rs_kwargs["WorkgroupName"] = workgroup

        return redshift_data_client.execute_statement(**rs_kwargs)

    except ClientError as e:
        if e.response["Error"]["Code"] == "ValidationException":
            raise RedshiftCredentialsError() from e
        raise


class RedshiftStatementNotFinishedError(Exception):
    pass


@retry(
    wait=wait_exponential(multiplier=1, max=30),
    retry=retry_if_exception_type(RedshiftStatementNotFinishedError),
    reraise=True,
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
    redshift_data_client,
    cluster_id: Optional[str],
    workgroup: Optional[str],
    database: str,
    user: Optional[str],
    query: str,
) -> str:
    """Execute Redshift statement synchronously. Waits for the query to finish.

    Raises RedshiftCredentialsError if the statement couldn't be executed due to the validation error.
    Raises RedshiftQueryError if the query runs but finishes with errors.


    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        workgroup:  Redshift Serverless Workgroup
        database: Redshift Database Name
        user: Redshift username
        query: The SQL query to execute

    Returns: Statement ID

    """
    statement = execute_redshift_statement_async(
        redshift_data_client, cluster_id, workgroup, database, user, query
    )
    wait_for_redshift_statement(redshift_data_client, statement)
    return statement["Id"]


def get_redshift_statement_result(redshift_data_client, statement_id: str) -> dict:
    """Get the Redshift statement result"""
    return redshift_data_client.get_statement_result(Id=statement_id)


def upload_df_to_s3(
    s3_resource,
    s3_path: str,
    df: pd.DataFrame,
) -> None:
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
    cluster_id: Optional[str],
    workgroup: Optional[str],
    database: str,
    user: Optional[str],
    s3_resource,
    s3_path: str,
    iam_role: str,
    table_name: str,
    df: pd.DataFrame,
):
    """Uploads a Pandas DataFrame to Redshift as a new table.

    The caller is responsible for deleting the table when no longer necessary.

    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        workgroup: Redshift Serverless Workgroup
        database: Redshift Database Name
        user: Redshift username
        s3_resource: S3 Resource object
        s3_path: S3 path where the Parquet file is temporarily uploaded
        iam_role: IAM Role for Redshift to assume during the COPY command.
                  The role must grant permission to read the S3 location.
        table_name: The name of the new Redshift table where we copy the dataframe
        df: The Pandas DataFrame to upload

    Raises:
        RedshiftTableNameTooLong: The specified table name is too long.
    """

    # Drop the index so that we dont have unnecessary columns
    df.reset_index(drop=True, inplace=True)

    # Convert Pandas DataFrame into PyArrow table and compile the Redshift table schema.
    # Note, if the underlying data has missing values,
    # pandas will convert those values to np.nan if the dtypes are numerical (floats, ints, etc.) or boolean.
    # If the dtype is 'object', then missing values are inferred as python `None`s.
    # More details at:
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#values-considered-missing
    table = pa.Table.from_pandas(df)
    upload_arrow_table_to_redshift(
        table,
        redshift_data_client,
        cluster_id=cluster_id,
        workgroup=workgroup,
        database=database,
        user=user,
        s3_resource=s3_resource,
        iam_role=iam_role,
        s3_path=s3_path,
        table_name=table_name,
    )


def delete_redshift_table(
    redshift_data_client,
    cluster_id: str,
    workgroup: str,
    database: str,
    user: str,
    table_name: str,
):
    drop_query = f"DROP {table_name} IF EXISTS"
    execute_redshift_statement(
        redshift_data_client,
        cluster_id,
        workgroup,
        database,
        user,
        drop_query,
    )


def upload_arrow_table_to_redshift(
    table: Union[pyarrow.Table, Path],
    redshift_data_client,
    cluster_id: Optional[str],
    workgroup: Optional[str],
    database: str,
    user: Optional[str],
    s3_resource,
    iam_role: str,
    s3_path: str,
    table_name: str,
    schema: Optional[pyarrow.Schema] = None,
    fail_if_exists: bool = True,
):
    """Uploads an Arrow Table to Redshift to a new or existing table.

    Here's how the upload process works:
        1. PyArrow Table is serialized into a Parquet format on local disk
        2. The Parquet file is uploaded to S3
        3. The S3 file is uploaded to Redshift as a new table through COPY command
        4. The local disk & s3 paths are cleaned up

    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        workgroup: Redshift Serverless Workgroup
        database: Redshift Database Name
        user: Redshift username
        s3_resource: S3 Resource object
        s3_path: S3 path where the Parquet file is temporarily uploaded
        iam_role: IAM Role for Redshift to assume during the COPY command.
                  The role must grant permission to read the S3 location.
        table_name: The name of the new Redshift table where we copy the dataframe
        table: The Arrow Table or Path to parquet dataset to upload
        schema: (Optionally) client may provide arrow Schema which will be converted into redshift table schema
        fail_if_exists: fail if table with such name exists or append data to existing table

    Raises:
        RedshiftTableNameTooLong: The specified table name is too long.
    """
    if len(table_name) > REDSHIFT_TABLE_NAME_MAX_LENGTH:
        raise RedshiftTableNameTooLong(table_name)

    if isinstance(table, pyarrow.Table) and not schema:
        schema = table.schema

    if not schema:
        raise ValueError("Schema must be specified when data is passed as a Path")

    bucket, key = get_bucket_and_key(s3_path)

    column_query_list = ", ".join(
        [f"{field.name} {pa_to_redshift_value_type(field.type)}" for field in schema]
    )

    uploaded_files = []

    if isinstance(table, Path):
        for file in table.iterdir():
            file_key = os.path.join(key, file.name)
            with file.open("rb") as f:
                s3_resource.Object(bucket, file_key).put(Body=f)

            uploaded_files.append(file_key)
    else:
        # Write the PyArrow Table on disk in Parquet format and upload it to S3
        with tempfile.TemporaryFile(suffix=".parquet") as parquet_temp_file:
            # In Pyarrow v13.0, the parquet version was upgraded to v2.6 from v2.4.
            # Set the coerce_timestamps to "us"(microseconds) for backward compatibility.
            pq.write_table(
                table,
                parquet_temp_file,
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )
            parquet_temp_file.seek(0)
            s3_resource.Object(bucket, key).put(Body=parquet_temp_file)

        uploaded_files.append(key)

    copy_query = (
        f"COPY {table_name} FROM '{s3_path}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET"
    )
    create_query = (
        f"CREATE TABLE {'IF NOT EXISTS' if not fail_if_exists else ''}"
        f" {table_name}({column_query_list})"
    )

    try:
        execute_redshift_statement(
            redshift_data_client,
            cluster_id,
            workgroup,
            database,
            user,
            f"{create_query}; {copy_query};",
        )
    finally:
        # Clean up S3 temporary data
        for file_pah in uploaded_files:
            s3_resource.Object(bucket, file_pah).delete()


@contextlib.contextmanager
def temporarily_upload_df_to_redshift(
    redshift_data_client,
    cluster_id: str,
    workgroup: str,
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
        workgroup,
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
        redshift_data_client,
        cluster_id,
        workgroup,
        database,
        user,
        f"DROP TABLE {table_name}",
    )


@contextlib.contextmanager
def temporarily_upload_arrow_table_to_redshift(
    table: Union[pyarrow.Table, Path],
    redshift_data_client,
    cluster_id: str,
    workgroup: str,
    database: str,
    user: str,
    s3_resource,
    iam_role: str,
    s3_path: str,
    table_name: str,
    schema: Optional[pyarrow.Schema] = None,
    fail_if_exists: bool = True,
) -> Iterator[None]:
    """Uploads a Arrow Table to Redshift as a new table with cleanup logic.

    This is essentially the same as upload_arrow_table_to_redshift (check out its docstring for full details),
    but unlike it this method is a generator and should be used with `with` block. For example:

    >>> with temporarily_upload_arrow_table_to_redshift(...): # doctest: +SKIP
    >>>     # Use `table_name` table in Redshift here
    >>> # `table_name` will not exist at this point, since it's cleaned up by the `with` block

    """
    # Upload the dataframe to Redshift
    upload_arrow_table_to_redshift(
        table,
        redshift_data_client,
        cluster_id,
        workgroup,
        database,
        user,
        s3_resource,
        s3_path,
        iam_role,
        table_name,
        schema,
        fail_if_exists,
    )

    yield

    # Clean up the uploaded Redshift table
    execute_redshift_statement(
        redshift_data_client,
        cluster_id,
        workgroup,
        database,
        user,
        f"DROP TABLE {table_name}",
    )


def download_s3_directory(s3_resource, bucket: str, key: str, local_dir: str):
    """Download the S3 directory to a local disk"""
    bucket_obj = s3_resource.Bucket(bucket)
    if key != "" and not key.endswith("/"):
        key = key + "/"
    for obj in bucket_obj.objects.filter(Prefix=key):
        local_file_path = local_dir + "/" + obj.key[len(key) :]
        local_file_dir = os.path.dirname(local_file_path)
        os.makedirs(local_file_dir, exist_ok=True)
        bucket_obj.download_file(obj.key, local_file_path)


def delete_s3_directory(s3_resource, bucket: str, key: str):
    """Delete S3 directory recursively"""
    bucket_obj = s3_resource.Bucket(bucket)
    if key != "" and not key.endswith("/"):
        key = key + "/"
    for obj in bucket_obj.objects.filter(Prefix=key):
        obj.delete()


def execute_redshift_query_and_unload_to_s3(
    redshift_data_client,
    cluster_id: Optional[str],
    workgroup: Optional[str],
    database: str,
    user: Optional[str],
    s3_path: str,
    iam_role: str,
    query: str,
) -> None:
    """Unload Redshift Query results to S3

    Args:
        redshift_data_client: Redshift Data API Service client
        cluster_id: Redshift Cluster Identifier
        workgroup: Redshift Serverless workgroup name
        database: Redshift Database Name
        user: Redshift username
        s3_path: S3 directory where the unloaded data is written
        iam_role: IAM Role for Redshift to assume during the UNLOAD command.
                  The role must grant permission to write to the S3 location.
        query: The SQL query to execute

    """
    # Run the query, unload the results to S3
    unique_table_name = "_" + str(uuid.uuid4()).replace("-", "")
    query = f"CREATE TEMPORARY TABLE {unique_table_name} AS ({query});\n"
    query += f"UNLOAD ('SELECT * FROM {unique_table_name}') TO '{s3_path}/' IAM_ROLE '{iam_role}' FORMAT AS PARQUET"
    execute_redshift_statement(
        redshift_data_client, cluster_id, workgroup, database, user, query
    )


def unload_redshift_query_to_pa(
    redshift_data_client,
    cluster_id: str,
    workgroup: str,
    database: str,
    user: str,
    s3_resource,
    s3_path: str,
    iam_role: str,
    query: str,
) -> pa.Table:
    """Unload Redshift Query results to S3 and get the results in PyArrow Table format"""
    bucket, key = get_bucket_and_key(s3_path)

    execute_redshift_query_and_unload_to_s3(
        redshift_data_client,
        cluster_id,
        workgroup,
        database,
        user,
        s3_path,
        iam_role,
        query,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        download_s3_directory(s3_resource, bucket, key, temp_dir)
        delete_s3_directory(s3_resource, bucket, key)
        return pq.read_table(temp_dir)


def unload_redshift_query_to_df(
    redshift_data_client,
    cluster_id: str,
    workgroup: str,
    database: str,
    user: str,
    s3_resource,
    s3_path: str,
    iam_role: str,
    query: str,
) -> pd.DataFrame:
    """Unload Redshift Query results to S3 and get the results in Pandas DataFrame format"""
    table = unload_redshift_query_to_pa(
        redshift_data_client,
        cluster_id,
        workgroup,
        database,
        user,
        s3_resource,
        s3_path,
        iam_role,
        query,
    )
    return table.to_pandas()


def get_lambda_function(lambda_client, function_name: str) -> Optional[Dict]:
    """
    Get the AWS Lambda function by name or return None if it doesn't exist.
    Args:
        lambda_client: AWS Lambda client.
        function_name: Name of the AWS Lambda function.

    Returns: Either a dictionary containing the get_function API response, or None if it doesn't exist.

    """
    try:
        return lambda_client.get_function(FunctionName=function_name)["Configuration"]
    except ClientError as ce:
        # If the resource could not be found, return None.
        # Otherwise bubble up the exception (most likely permission errors)
        if ce.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        else:
            raise


def delete_lambda_function(lambda_client, function_name: str) -> Dict:
    """
    Delete the AWS Lambda function by name.
    Args:
        lambda_client: AWS Lambda client.
        function_name: Name of the AWS Lambda function.

    Returns: The delete_function API response dict

    """
    return lambda_client.delete_function(FunctionName=function_name)


@retry(
    wait=wait_exponential(multiplier=1, max=4),
    retry=retry_if_exception_type(ClientError),
    stop=stop_after_attempt(5),
    reraise=True,
)
def update_lambda_function_environment(
    lambda_client, function_name: str, environment: Dict[str, Any]
) -> None:
    """
    Update AWS Lambda function environment. The function is retried multiple times in case another action is
    currently being run on the lambda (e.g. it's being created or being updated in parallel).
    Args:
        lambda_client: AWS Lambda client.
        function_name: Name of the AWS Lambda function.
        environment: The desired lambda environment.

    """
    lambda_client.update_function_configuration(
        FunctionName=function_name, Environment=environment
    )


def get_first_api_gateway(api_gateway_client, api_gateway_name: str) -> Optional[Dict]:
    """
    Get the first API Gateway with the given name. Note, that API Gateways can have the same name.
    They are identified by AWS-generated ID, which is unique. Therefore this method lists all API
    Gateways and returns the first one with matching name. If no matching name is found, None is returned.
    Args:
        api_gateway_client: API Gateway V2 Client.
        api_gateway_name: Name of the API Gateway function.

    Returns: Either a dictionary containing the get_api response, or None if it doesn't exist

    """
    response = api_gateway_client.get_apis()
    apis = response.get("Items", [])

    # Limit the number of times we page through the API.
    for _ in range(10):
        # Try finding the match before getting the next batch of api gateways from AWS
        for api in apis:
            if api.get("Name") == api_gateway_name:
                return api

        # Break out of the loop if there's no next batch of api gateways
        next_token = response.get("NextToken")
        if not next_token:
            break

        # Get the next batch of api gateways using next_token
        response = api_gateway_client.get_apis(NextToken=next_token)
        apis = response.get("Items", [])

    # Return None if API Gateway with such name was not found
    return None


def delete_api_gateway(api_gateway_client, api_gateway_id: str) -> Dict:
    """
    Delete the API Gateway given ID.
    Args:
        api_gateway_client: API Gateway V2 Client.
        api_gateway_id: API Gateway ID to delete.

    Returns: The delete_api API response dict.

    """
    return api_gateway_client.delete_api(ApiId=api_gateway_id)


def get_account_id() -> str:
    """Get AWS Account ID"""
    return boto3.client("sts").get_caller_identity().get("Account")


def list_s3_files(aws_region: str, path: str) -> List[str]:
    s3 = boto3.client("s3", config=Config(region_name=aws_region))
    if path.startswith("s3://"):
        path = path[len("s3://") :]
    bucket, prefix = path.split("/", 1)
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = objects["Contents"]
    files = [f"s3://{bucket}/{content['Key']}" for content in contents]
    return files


# Athena utils


def get_athena_data_client(aws_region: str):
    """
    Get the athena Data API Service client for the given AWS region.
    """
    return boto3.client(
        "athena", config=Config(region_name=aws_region, user_agent=get_user_agent())
    )


@retry(
    wait=wait_exponential(multiplier=1, max=4),
    retry=retry_if_exception_type(ConnectionClosedError),
    stop=stop_after_attempt(5),
    reraise=True,
)
def execute_athena_query_async(
    athena_data_client, data_source: str, database: str, workgroup: str, query: str
) -> dict:
    """Execute Athena statement asynchronously. Does not wait for the query to finish.

    Raises AthenaCredentialsError if the statement couldn't be executed due to the validation error.

    Args:
        athena_data_client: Athena Data API Service client
        data_source: Athena Data Source
        database: Athena Database Name
        workgroup: Athena Workgroup Name
        query: The SQL query to execute

    Returns: JSON response

    """
    try:
        # return athena_data_client.execute_statement(
        return athena_data_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database, "Catalog": data_source},
            WorkGroup=workgroup,
        )

    except ClientError as e:
        raise AthenaQueryError(e)


class AthenaStatementNotFinishedError(Exception):
    pass


@retry(
    wait=wait_exponential(multiplier=1, max=30),
    retry=retry_if_exception_type(AthenaStatementNotFinishedError),
    reraise=True,
)
def wait_for_athena_execution(athena_data_client, execution: dict) -> None:
    """Waits for the Athena statement to finish. Raises AthenaQueryError if the statement didn't succeed.

    We use exponential backoff for checking the query state until it's not running. The backoff starts with
    0.1 seconds and doubles exponentially until reaching 30 seconds, at which point the backoff is fixed.

    Args:
        athena_data_client:  athena Service boto3 client
        execution:  The athena execution to wait for (result of execute_athena_statement)

    Returns: None

    """
    response = athena_data_client.get_query_execution(
        QueryExecutionId=execution["QueryExecutionId"]
    )
    if response["QueryExecution"]["Status"]["State"] in ("QUEUED", "RUNNING"):
        raise AthenaStatementNotFinishedError  # Retry
    if response["QueryExecution"]["Status"]["State"] != "SUCCEEDED":
        raise AthenaQueryError(response)  # Don't retry. Raise exception.


def drop_temp_table(
    athena_data_client, data_source: str, database: str, workgroup: str, temp_table: str
):
    query = f"DROP TABLE `{database}.{temp_table}`"
    execute_athena_query_async(
        athena_data_client, data_source, database, workgroup, query
    )


def execute_athena_query(
    athena_data_client,
    data_source: str,
    database: str,
    workgroup: str,
    query: str,
    temp_table: Optional[str] = None,
) -> str:
    """Execute athena statement synchronously. Waits for the query to finish.

    Raises athenaCredentialsError if the statement couldn't be executed due to the validation error.
    Raises athenaQueryError if the query runs but finishes with errors.


    Args:
        athena_data_client: Athena Data API Service client
        data_source: Athena Data Source Name
        database: Athena Database Name
        workgroup: Athena Workgroup Name
        query: The SQL query to execute
        temp_table: Temp table name to be deleted after query execution.

    Returns: Statement ID

    """

    execution = execute_athena_query_async(
        athena_data_client, data_source, database, workgroup, query
    )
    wait_for_athena_execution(athena_data_client, execution)
    if temp_table is not None:
        drop_temp_table(
            athena_data_client, data_source, database, workgroup, temp_table
        )

    return execution["QueryExecutionId"]


def get_athena_query_result(athena_data_client, query_execution_id: str) -> dict:
    """Get the athena query result"""
    response = athena_data_client.get_query_results(QueryExecutionId=query_execution_id)
    return response["ResultSet"]


class AthenaError(Exception):
    def __init__(self, details):
        super().__init__(f"Athena API failed. Details: {details}")


class AthenaQueryError(Exception):
    def __init__(self, details):
        super().__init__(f"Athena SQL Query failed to finish. Details: {details}")


class AthenaTableNameTooLong(Exception):
    def __init__(self, table_name: str):
        super().__init__(
            f"Athena table(Data catalog) names have a maximum length of 255 characters, but the table name {table_name} has length {len(table_name)} characters."
        )


def unload_athena_query_to_pa(
    athena_data_client,
    data_source: str,
    database: str,
    workgroup: str,
    s3_resource,
    s3_path: str,
    query: str,
    temp_table: str,
) -> pa.Table:
    """Unload Athena Query results to S3 and get the results in PyArrow Table format"""
    bucket, key = get_bucket_and_key(s3_path)

    execute_athena_query_and_unload_to_s3(
        athena_data_client, data_source, database, workgroup, query, temp_table
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        download_s3_directory(s3_resource, bucket, key, temp_dir)
        delete_s3_directory(s3_resource, bucket, key)
        return pq.read_table(temp_dir)


def unload_athena_query_to_df(
    athena_data_client,
    data_source: str,
    database: str,
    workgroup: str,
    s3_resource,
    s3_path: str,
    query: str,
    temp_table: str,
) -> pd.DataFrame:
    """Unload Athena Query results to S3 and get the results in Pandas DataFrame format"""
    table = unload_athena_query_to_pa(
        athena_data_client,
        data_source,
        database,
        workgroup,
        s3_resource,
        s3_path,
        query,
        temp_table,
    )
    return table.to_pandas()


def execute_athena_query_and_unload_to_s3(
    athena_data_client,
    data_source: str,
    database: str,
    workgroup: str,
    query: str,
    temp_table: str,
) -> None:
    """Unload Athena Query results to S3

    Args:
        athena_data_client: Athena Data API Service client
        data_source: Athena Data Source
        database: Athena Database Name
        workgroup: Athena Workgroup Name
        query: The SQL query to execute
        temp_table: temp table name to be deleted after query execution.

    """

    execute_athena_query(
        athena_data_client=athena_data_client,
        data_source=data_source,
        database=database,
        workgroup=workgroup,
        query=query,
        temp_table=temp_table,
    )


def upload_df_to_athena(
    athena_client,
    data_source: str,
    database: str,
    workgroup: str,
    s3_resource,
    s3_path: str,
    table_name: str,
    df: pd.DataFrame,
):
    """Uploads a Pandas DataFrame to S3(Athena) as a new table.

    The caller is responsible for deleting the table when no longer necessary.

    Args:
        athena_client: Athena API Service client
        data_source: Athena Data Source
        database: Athena Database Name
        workgroup: Athena Workgroup Name
        s3_resource: S3 Resource object
        s3_path: S3 path where the Parquet file is temporarily uploaded
        table_name: The name of the new Data Catalog table where we copy the dataframe
        df: The Pandas DataFrame to upload

    Raises:
        AthenaTableNameTooLong: The specified table name is too long.
    """

    # Drop the index so that we dont have unnecessary columns
    df.reset_index(drop=True, inplace=True)

    # Convert Pandas DataFrame into PyArrow table and compile the Athena table schema.
    # Note, if the underlying data has missing values,
    # pandas will convert those values to np.nan if the dtypes are numerical (floats, ints, etc.) or boolean.
    # If the dtype is 'object', then missing values are inferred as python `None`s.
    # More details at:
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#values-considered-missing
    table = pa.Table.from_pandas(df)
    upload_arrow_table_to_athena(
        table,
        athena_client,
        data_source=data_source,
        database=database,
        workgroup=workgroup,
        s3_resource=s3_resource,
        s3_path=s3_path,
        table_name=table_name,
    )


def upload_arrow_table_to_athena(
    table: Union[pyarrow.Table, Path],
    athena_client,
    data_source: str,
    database: str,
    workgroup: str,
    s3_resource,
    s3_path: str,
    table_name: str,
    schema: Optional[pyarrow.Schema] = None,
    fail_if_exists: bool = True,
):
    """Uploads an Arrow Table to S3(Athena).

    Here's how the upload process works:
        1. PyArrow Table is serialized into a Parquet format on local disk
        2. The Parquet file is uploaded to S3
        3. an Athena(data catalog) table is created. the S3 directory(in number 2) will be set as an external location.
        4. The local disk & s3 paths are cleaned up

    Args:
        table: The Arrow Table or Path to parquet dataset to upload
        athena_client: Athena API Service client
        data_source: Athena Data Source
        database: Athena Database Name
        workgroup: Athena Workgroup Name
        s3_resource: S3 Resource object
        s3_path: S3 path where the Parquet file is temporarily uploaded
        table_name: The name of the new Athena table where we copy the dataframe
        schema: (Optionally) client may provide arrow Schema which will be converted into Athena table schema
        fail_if_exists: fail if table with such name exists or append data to existing table

    Raises:
        AthenaTableNameTooLong: The specified table name is too long.
    """
    DATA_CATALOG_TABLE_NAME_MAX_LENGTH = 255

    if len(table_name) > DATA_CATALOG_TABLE_NAME_MAX_LENGTH:
        raise AthenaTableNameTooLong(table_name)

    if isinstance(table, pyarrow.Table) and not schema:
        schema = table.schema

    if not schema:
        raise ValueError("Schema must be specified when data is passed as a Path")

    bucket, key = get_bucket_and_key(s3_path)

    column_query_list = ", ".join(
        [f"`{field.name}` {pa_to_athena_value_type(field.type)}" for field in schema]
    )

    with tempfile.TemporaryFile(suffix=".parquet") as parquet_temp_file:
        pq.write_table(table, parquet_temp_file)
        parquet_temp_file.seek(0)
        s3_resource.Object(bucket, key).put(Body=parquet_temp_file)

    create_query = (
        f"CREATE EXTERNAL TABLE {database}.{table_name} {'IF NOT EXISTS' if not fail_if_exists else ''}"
        f"({column_query_list}) "
        f"STORED AS PARQUET "
        f"LOCATION '{s3_path[: s3_path.rfind('/')]}' "
        f"TBLPROPERTIES('parquet.compress' = 'SNAPPY') "
    )

    try:
        execute_athena_query(
            athena_data_client=athena_client,
            data_source=data_source,
            database=database,
            workgroup=workgroup,
            query=f"{create_query}",
        )
    finally:
        pass
        # Clean up S3 temporary data
        # for file_path in uploaded_files:
        #     s3_resource.Object(bucket, file_path).delete()


class DynamoUnprocessedWriteItems(Exception):
    pass


async def dynamo_write_items_async(
    dynamo_client, table_name: str, items: list[dict]
) -> None:
    """
    Writes in batches to a dynamo table asynchronously. Max size of each
    attempted batch is 25.
    Raises DynamoUnprocessedWriteItems if not all items can be written.

    Args:
        dynamo_client: async dynamodb client
        table_name: name of table being written to
        items: list of items to be written. see boto3 docs on format of the items.
    """
    DYNAMO_MAX_WRITE_BATCH_SIZE = 25

    async def _do_write(items):
        item_iter = iter(items)
        item_batches = []
        while True:
            item_batch = [
                item
                for item in itertools.islice(item_iter, DYNAMO_MAX_WRITE_BATCH_SIZE)
            ]
            if not item_batch:
                break

            item_batches.append(item_batch)

        return await asyncio.gather(
            *[
                dynamo_client.batch_write_item(
                    RequestItems={table_name: item_batch},
                )
                for item_batch in item_batches
            ]
        )

    put_items = [{"PutRequest": {"Item": item}} for item in items]

    retries = AsyncRetrying(
        retry=retry_if_exception_type(DynamoUnprocessedWriteItems),
        wait=wait_exponential(multiplier=1, max=4),
        reraise=True,
    )

    async for attempt in retries:
        with attempt:
            response_batches = await _do_write(put_items)

            put_items = []
            for response in response_batches:
                put_items.extend(response["UnprocessedItems"])

            if put_items:
                raise DynamoUnprocessedWriteItems()
