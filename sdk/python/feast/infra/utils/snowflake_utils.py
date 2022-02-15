import configparser
import os
import random
import string
from logging import getLogger
from tempfile import TemporaryDirectory
from typing import Dict, Iterator, List, Optional, Tuple, cast

import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from feast.errors import SnowflakeIncompleteConfig, SnowflakeQueryUnknownError

try:
    import snowflake.connector
    from snowflake.connector import ProgrammingError, SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("snowflake", str(e))


getLogger("snowflake.connector.cursor").disabled = True
getLogger("snowflake.connector.connection").disabled = True
getLogger("snowflake.connector.network").disabled = True
logger = getLogger(__name__)


def execute_snowflake_statement(conn: SnowflakeConnection, query) -> SnowflakeCursor:
    cursor = conn.cursor().execute(query)
    if cursor is None:
        raise SnowflakeQueryUnknownError(query)
    return cursor


def get_snowflake_conn(config, autocommit=True) -> SnowflakeConnection:
    if config.type == "snowflake.offline":
        config_header = "connections.feast_offline_store"

    config = dict(config)

    # read config file
    config_reader = configparser.ConfigParser()
    config_reader.read([config["config_path"]])
    if config_reader.has_section(config_header):
        kwargs = dict(config_reader[config_header])
    else:
        kwargs = {}

    kwargs.update((k, v) for k, v in config.items() if v is not None)

    try:
        conn = snowflake.connector.connect(
            account=kwargs["account"],
            user=kwargs["user"],
            password=kwargs["password"],
            role=f'''"{kwargs['role']}"''',
            warehouse=f'''"{kwargs['warehouse']}"''',
            database=f'''"{kwargs['database']}"''',
            schema=f'''"{kwargs['schema_']}"''',
            application="feast",
            autocommit=autocommit,
        )

        return conn
    except KeyError as e:
        raise SnowflakeIncompleteConfig(e)


# TO DO -- sfc-gh-madkins
# Remove dependency on write_pandas function by falling back to native snowflake python connector
# Current issue is datetime[ns] types are read incorrectly in Snowflake, need to coerce to datetime[ns, UTC]
def write_pandas(
    conn: SnowflakeConnection,
    df: pd.DataFrame,
    table_name: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    chunk_size: Optional[int] = None,
    compression: str = "gzip",
    on_error: str = "abort_statement",
    parallel: int = 4,
    quote_identifiers: bool = True,
    auto_create_table: bool = False,
    create_temp_table: bool = False,
):
    """Allows users to most efficiently write back a pandas DataFrame to Snowflake.

    It works by dumping the DataFrame into Parquet files, uploading them and finally copying their data into the table.

    Returns whether all files were ingested correctly, number of chunks uploaded, and number of rows ingested
    with all of the COPY INTO command's output for debugging purposes.

        Example usage:
            import pandas
            from snowflake.connector.pandas_tools import write_pandas

            df = pandas.DataFrame([('Mark', 10), ('Luke', 20)], columns=['name', 'balance'])
            success, nchunks, nrows, _ = write_pandas(cnx, df, 'customers')

    Args:
        conn: Connection to be used to communicate with Snowflake.
        df: Dataframe we'd like to write back.
        table_name: Table name where we want to insert into.
        database: Database schema and table is in, if not provided the default one will be used (Default value = None).
        schema: Schema table is in, if not provided the default one will be used (Default value = None).
        chunk_size: Number of elements to be inserted once, if not provided all elements will be dumped once
            (Default value = None).
        compression: The compression used on the Parquet files, can only be gzip, or snappy. Gzip gives supposedly a
            better compression, while snappy is faster. Use whichever is more appropriate (Default value = 'gzip').
        on_error: Action to take when COPY INTO statements fail, default follows documentation at:
            https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
            (Default value = 'abort_statement').
        parallel: Number of threads to be used when uploading chunks, default follows documentation at:
            https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters (Default value = 4).
        quote_identifiers: By default, identifiers, specifically database, schema, table and column names
            (from df.columns) will be quoted. If set to False, identifiers are passed on to Snowflake without quoting.
            I.e. identifiers will be coerced to uppercase by Snowflake.  (Default value = True)
        auto_create_table: When true, will automatically create a table with corresponding columns for each column in
            the passed in DataFrame. The table will not be created if it already exists
        create_temp_table: Will make the auto-created table as a temporary table
    """
    if database is not None and schema is None:
        raise ProgrammingError(
            "Schema has to be provided to write_pandas when a database is provided"
        )
    # This dictionary maps the compression algorithm to Snowflake put copy into command type
    # https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#type-parquet
    compression_map = {"gzip": "auto", "snappy": "snappy"}
    if compression not in compression_map.keys():
        raise ProgrammingError(
            "Invalid compression '{}', only acceptable values are: {}".format(
                compression, compression_map.keys()
            )
        )
    if quote_identifiers:
        location = (
            (('"' + database + '".') if database else "")
            + (('"' + schema + '".') if schema else "")
            + ('"' + table_name + '"')
        )
    else:
        location = (
            (database + "." if database else "")
            + (schema + "." if schema else "")
            + (table_name)
        )
    if chunk_size is None:
        chunk_size = len(df)
    cursor: SnowflakeCursor = conn.cursor()
    stage_name = create_temporary_sfc_stage(cursor)

    with TemporaryDirectory() as tmp_folder:
        for i, chunk in chunk_helper(df, chunk_size):
            chunk_path = os.path.join(tmp_folder, "file{}.txt".format(i))
            # Dump chunk into parquet file
            chunk.to_parquet(
                chunk_path,
                compression=compression,
                use_deprecated_int96_timestamps=True,
            )
            # Upload parquet file
            upload_sql = (
                "PUT /* Python:snowflake.connector.pandas_tools.write_pandas() */ "
                "'file://{path}' @\"{stage_name}\" PARALLEL={parallel}"
            ).format(
                path=chunk_path.replace("\\", "\\\\").replace("'", "\\'"),
                stage_name=stage_name,
                parallel=parallel,
            )
            logger.debug(f"uploading files with '{upload_sql}'")
            cursor.execute(upload_sql, _is_internal=True)
            # Remove chunk file
            os.remove(chunk_path)
    if quote_identifiers:
        columns = '"' + '","'.join(list(df.columns)) + '"'
    else:
        columns = ",".join(list(df.columns))

    if auto_create_table:
        file_format_name = create_file_format(compression, compression_map, cursor)
        infer_schema_sql = f"SELECT COLUMN_NAME, TYPE FROM table(infer_schema(location=>'@\"{stage_name}\"', file_format=>'{file_format_name}'))"
        logger.debug(f"inferring schema with '{infer_schema_sql}'")
        result_cursor = cursor.execute(infer_schema_sql, _is_internal=True)
        if result_cursor is None:
            raise SnowflakeQueryUnknownError(infer_schema_sql)
        result = cast(List[Tuple[str, str]], result_cursor.fetchall())
        column_type_mapping: Dict[str, str] = dict(result)
        # Infer schema can return the columns out of order depending on the chunking we do when uploading
        # so we have to iterate through the dataframe columns to make sure we create the table with its
        # columns in order
        quote = '"' if quote_identifiers else ""
        create_table_columns = ", ".join(
            [f"{quote}{c}{quote} {column_type_mapping[c]}" for c in df.columns]
        )
        create_table_sql = (
            f"CREATE {'TEMP ' if create_temp_table else ''}TABLE IF NOT EXISTS {location} "
            f"({create_table_columns})"
            f" /* Python:snowflake.connector.pandas_tools.write_pandas() */ "
        )
        logger.debug(f"auto creating table with '{create_table_sql}'")
        cursor.execute(create_table_sql, _is_internal=True)
        drop_file_format_sql = f"DROP FILE FORMAT IF EXISTS {file_format_name}"
        logger.debug(f"dropping file format with '{drop_file_format_sql}'")
        cursor.execute(drop_file_format_sql, _is_internal=True)

    # in Snowflake, all parquet data is stored in a single column, $1, so we must select columns explicitly
    # see (https://docs.snowflake.com/en/user-guide/script-data-load-transform-parquet.html)
    if quote_identifiers:
        parquet_columns = "$1:" + ",$1:".join(f'"{c}"' for c in df.columns)
    else:
        parquet_columns = "$1:" + ",$1:".join(df.columns)
    copy_into_sql = (
        "COPY INTO {location} /* Python:snowflake.connector.pandas_tools.write_pandas() */ "
        "({columns}) "
        'FROM (SELECT {parquet_columns} FROM @"{stage_name}") '
        "FILE_FORMAT=(TYPE=PARQUET COMPRESSION={compression}) "
        "PURGE=TRUE ON_ERROR={on_error}"
    ).format(
        location=location,
        columns=columns,
        parquet_columns=parquet_columns,
        stage_name=stage_name,
        compression=compression_map[compression],
        on_error=on_error,
    )
    logger.debug("copying into with '{}'".format(copy_into_sql))
    # Snowflake returns the original cursor if the query execution succeeded.
    result_cursor = cursor.execute(copy_into_sql, _is_internal=True)
    if result_cursor is None:
        raise SnowflakeQueryUnknownError(copy_into_sql)
    result_cursor.close()


@retry(
    wait=wait_exponential(multiplier=1, max=4),
    retry=retry_if_exception_type(ProgrammingError),
    stop=stop_after_attempt(5),
    reraise=True,
)
def create_file_format(
    compression: str, compression_map: Dict[str, str], cursor: SnowflakeCursor
) -> str:
    file_format_name = (
        '"' + "".join(random.choice(string.ascii_lowercase) for _ in range(5)) + '"'
    )
    file_format_sql = (
        f"CREATE FILE FORMAT {file_format_name} "
        f"/* Python:snowflake.connector.pandas_tools.write_pandas() */ "
        f"TYPE=PARQUET COMPRESSION={compression_map[compression]}"
    )
    logger.debug(f"creating file format with '{file_format_sql}'")
    cursor.execute(file_format_sql, _is_internal=True)
    return file_format_name


@retry(
    wait=wait_exponential(multiplier=1, max=4),
    retry=retry_if_exception_type(ProgrammingError),
    stop=stop_after_attempt(5),
    reraise=True,
)
def create_temporary_sfc_stage(cursor: SnowflakeCursor) -> str:
    stage_name = "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    create_stage_sql = (
        "create temporary stage /* Python:snowflake.connector.pandas_tools.write_pandas() */ "
        '"{stage_name}"'
    ).format(stage_name=stage_name)
    logger.debug(f"creating stage with '{create_stage_sql}'")
    result_cursor = cursor.execute(create_stage_sql, _is_internal=True)
    if result_cursor is None:
        raise SnowflakeQueryUnknownError(create_stage_sql)
    result_cursor.fetchall()
    return stage_name


def chunk_helper(lst: pd.DataFrame, n: int) -> Iterator[Tuple[int, pd.DataFrame]]:
    """Helper generator to chunk a sequence efficiently with current index like if enumerate was called on sequence."""
    for i in range(0, len(lst), n):
        yield int(i / n), lst[i : i + n]
