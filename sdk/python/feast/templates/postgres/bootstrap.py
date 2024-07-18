import click
import psycopg

from feast.file_utils import replace_str_in_file
from feast.infra.utils.postgres.connection_utils import df_to_postgres_table
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    config_file = repo_path / "feature_store.yaml"

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    postgres_host = click.prompt("Postgres host", default="localhost")
    postgres_port = click.prompt("Postgres port", default="5432")
    postgres_database = click.prompt("Postgres DB name", default="postgres")
    postgres_schema = click.prompt("Postgres schema", default="public")
    postgres_user = click.prompt("Postgres user")
    postgres_password = click.prompt("Postgres password", hide_input=True)

    if click.confirm(
        'Should I upload example data to Postgres (overwriting "feast_driver_hourly_stats" table)?',
        default=True,
    ):
        db_connection = psycopg.connect(
            conninfo=(
                f"postgresql://{postgres_user}"
                f":{postgres_password}"
                f"@{postgres_host}"
                f":{int(postgres_port)}"
                f"/{postgres_database}"
            ),
            options=f"-c search_path={postgres_schema}",
        )

        with db_connection as conn, conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS "feast_driver_hourly_stats"')

        df_to_postgres_table(
            config=PostgreSQLConfig(
                host=postgres_host,
                port=int(postgres_port),
                database=postgres_database,
                db_schema=postgres_schema,
                user=postgres_user,
                password=postgres_password,
            ),
            df=driver_df,
            table_name="feast_driver_hourly_stats",
        )

    replace_str_in_file(config_file, "DB_HOST", postgres_host)
    replace_str_in_file(config_file, "DB_PORT", postgres_port)
    replace_str_in_file(config_file, "DB_NAME", postgres_database)
    replace_str_in_file(config_file, "DB_SCHEMA", postgres_schema)
    replace_str_in_file(config_file, "DB_USERNAME", postgres_user)
    replace_str_in_file(config_file, "DB_PASSWORD", postgres_password)


if __name__ == "__main__":
    bootstrap()
