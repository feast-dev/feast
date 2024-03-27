import click
import snowflake.connector

from feast.file_utils import replace_str_in_file
from feast.infra.utils.snowflake.snowflake_utils import (
    execute_snowflake_statement,
    write_pandas,
)


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute()
    project_name = str(repo_path)[str(repo_path).rfind("/") + 1 :]
    repo_path = repo_path / "feature_repo"

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)
    driver_stats_path = data_path / "driver_stats.parquet"
    driver_df.to_parquet(path=str(driver_stats_path), allow_truncated_timestamps=True)

    snowflake_deployment_url = click.prompt(
        "Snowflake Deployment URL (exclude .snowflakecomputing.com):"
    )
    snowflake_user = click.prompt("Snowflake User Name:")
    snowflake_password = click.prompt("Snowflake Password:", hide_input=True)
    snowflake_role = click.prompt("Snowflake Role Name (Case Sensitive):")
    snowflake_warehouse = click.prompt("Snowflake Warehouse Name (Case Sensitive):")
    snowflake_database = click.prompt("Snowflake Database Name (Case Sensitive):")

    config_file = repo_path / "feature_store.yaml"
    for i in range(3):
        replace_str_in_file(
            config_file, "SNOWFLAKE_DEPLOYMENT_URL", snowflake_deployment_url
        )
        replace_str_in_file(config_file, "SNOWFLAKE_USER", snowflake_user)
        replace_str_in_file(config_file, "SNOWFLAKE_PASSWORD", snowflake_password)
        replace_str_in_file(config_file, "SNOWFLAKE_ROLE", snowflake_role)
        replace_str_in_file(config_file, "SNOWFLAKE_WAREHOUSE", snowflake_warehouse)
        replace_str_in_file(config_file, "SNOWFLAKE_DATABASE", snowflake_database)

    if click.confirm(
        f'Should I upload example data to Snowflake (overwriting "{project_name}_feast_driver_hourly_stats" table)?',
        default=True,
    ):
        snowflake_conn = snowflake.connector.connect(
            account=snowflake_deployment_url,
            user=snowflake_user,
            password=snowflake_password,
            role=snowflake_role,
            warehouse=snowflake_warehouse,
            application="feast",
        )

        with snowflake_conn as conn:
            execute_snowflake_statement(
                conn, f'CREATE DATABASE IF NOT EXISTS "{snowflake_database}"'
            )
            execute_snowflake_statement(conn, f'USE DATABASE "{snowflake_database}"')
            execute_snowflake_statement(conn, 'CREATE SCHEMA IF NOT EXISTS "PUBLIC"')
            execute_snowflake_statement(conn, 'USE SCHEMA "PUBLIC"')
            execute_snowflake_statement(
                conn, f'DROP TABLE IF EXISTS "{project_name}_feast_driver_hourly_stats"'
            )
            execute_snowflake_statement(
                conn,
                f'ALTER WAREHOUSE IF EXISTS "{snowflake_warehouse}" RESUME IF SUSPENDED',
            )

            write_pandas(
                conn,
                driver_df,
                f"{project_name}_feast_driver_hourly_stats",
                auto_create_table=True,
            )


if __name__ == "__main__":
    bootstrap()
