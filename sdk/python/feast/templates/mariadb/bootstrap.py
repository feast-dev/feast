import click
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from feast.file_utils import replace_str_in_file
from feast.infra.offline_stores.contrib.mariadb_offline_store.mariadb import (
    df_to_mariadb_table,
)


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    config_file = repo_path / "feature_store.yaml"

    print(repo_path)

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    mariadb_host = click.prompt("MariaDB host", default="localhost")
    mariadb_database = click.prompt("MariaDB name", default="mariadb")
    mariadb_user = click.prompt("MariaDB user")
    mariadb_password = click.prompt("MariaDB password", default="")
    mariadb_port = click.prompt("MariaDB port", default="3306")
    project_name = yaml.safe_load(open(config_file))["project"]

    print(project_name)

    connection_string = (
        "mysql+pymysql://"
        + mariadb_user
        + ":"
        + mariadb_password
        + "@"
        + mariadb_host
        + ":"
        + mariadb_port
        + "/"
        + mariadb_database
    )

    if click.confirm(
        'Should I upload example data to MariaDB (overwriting "feast_driver_hourly_stats" table)?',
        default=True,
    ):
        engine = create_engine(connection_string)

        session = sessionmaker(bind=engine)()

        session.execute(text("DROP TABLE IF EXISTS feast_driver_hourly_stats"))

        session.commit()

        df_to_mariadb_table(driver_df, "feast_driver_hourly_stats", engine)

    replace_str_in_file(config_file, "MARIADB_CONNECTION_STRING", connection_string)


if __name__ == "__main__":
    bootstrap()
