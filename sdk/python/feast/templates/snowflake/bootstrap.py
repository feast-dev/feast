import click
import snowflake.connector

from feast.infra.utils.snowflake_utils import write_pandas


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute()
    config_file = repo_path / "feature_store.yaml"

    project_name = str(repo_path)[str(repo_path).rfind("/") + 1 :]

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    repo_path = pathlib.Path(__file__).parent.absolute()
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

    if click.confirm(
        f'Should I upload example data to Snowflake (overwriting "{project_name}_feast_driver_hourly_stats" table)?',
        default=True,
    ):

        conn = snowflake.connector.connect(
            account=snowflake_deployment_url,
            user=snowflake_user,
            password=snowflake_password,
            role=snowflake_role,
            warehouse=snowflake_warehouse,
            application="feast",
        )

        cur = conn.cursor()
        cur.execute(f'CREATE DATABASE IF NOT EXISTS "{snowflake_database}"')
        cur.execute(f'USE DATABASE "{snowflake_database}"')
        cur.execute('CREATE SCHEMA IF NOT EXISTS "PUBLIC"')
        cur.execute('USE SCHEMA "PUBLIC"')
        cur.execute(f'DROP TABLE IF EXISTS "{project_name}_feast_driver_hourly_stats"')
        write_pandas(
            conn,
            driver_df,
            f"{project_name}_feast_driver_hourly_stats",
            auto_create_table=True,
        )
        conn.close()

    repo_path = pathlib.Path(__file__).parent.absolute()
    config_file = repo_path / "feature_store.yaml"

    replace_str_in_file(
        config_file, "SNOWFLAKE_DEPLOYMENT_URL", snowflake_deployment_url
    )
    replace_str_in_file(config_file, "SNOWFLAKE_USER", snowflake_user)
    replace_str_in_file(config_file, "SNOWFLAKE_PASSWORD", snowflake_password)
    replace_str_in_file(config_file, "SNOWFLAKE_ROLE", snowflake_role)
    replace_str_in_file(config_file, "SNOWFLAKE_WAREHOUSE", snowflake_warehouse)
    replace_str_in_file(config_file, "SNOWFLAKE_DATABASE", snowflake_database)


def replace_str_in_file(file_path, match_str, sub_str):
    with open(file_path, "r") as f:
        contents = f.read()
    contents = contents.replace(match_str, sub_str)
    with open(file_path, "wt") as f:
        f.write(contents)


if __name__ == "__main__":
    bootstrap()
