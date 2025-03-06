import click
from couchbase_columnar.cluster import Cluster
from couchbase_columnar.common.errors import InvalidCredentialError
from couchbase_columnar.credential import Credential
from couchbase_columnar.options import ClusterOptions, QueryOptions, TimeoutOptions

from feast.file_utils import replace_str_in_file
from feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase import (
    CouchbaseColumnarOfflineStoreConfig,
    df_to_columnar,
)


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    config_file = repo_path / "feature_store.yaml"

    if click.confirm("Configure Couchbase Online Store?", default=True):
        connection_string = click.prompt(
            "Couchbase Connection String", default="couchbase://127.0.0.1"
        )
        user = click.prompt("Couchbase Username", default="Administrator")
        password = click.prompt("Couchbase Password", hide_input=True)
        bucket_name = click.prompt("Couchbase Bucket Name", default="feast")
        kv_port = click.prompt("Couchbase KV Port", default=11210)

        replace_str_in_file(
            config_file, "COUCHBASE_CONNECTION_STRING", connection_string
        )
        replace_str_in_file(config_file, "COUCHBASE_USER", user)
        replace_str_in_file(config_file, "COUCHBASE_PASSWORD", password)
        replace_str_in_file(config_file, "COUCHBASE_BUCKET_NAME", bucket_name)
        replace_str_in_file(config_file, "COUCHBASE_KV_PORT", str(kv_port))

    if click.confirm(
        "Configure Couchbase Columnar Offline Store? (Note: requires Couchbase Capella Columnar)",
        default=True,
    ):
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

        columnar_connection_string = click.prompt("Columnar Connection String")
        columnar_user = click.prompt("Columnar Username")
        columnar_password = click.prompt("Columnar Password", hide_input=True)
        columnar_timeout = click.prompt("Couchbase Columnar Timeout", default=120)

        if click.confirm(
            'Should I upload example data to Couchbase Capella Columnar (overwriting "Default.Default.feast_driver_hourly_stats" table)?',
            default=True,
        ):
            cred = Credential.from_username_and_password(
                columnar_user, columnar_password
            )
            timeout_opts = TimeoutOptions(dispatch_timeout=timedelta(seconds=120))
            cluster = Cluster.create_instance(
                columnar_connection_string,
                cred,
                ClusterOptions(timeout_options=timeout_opts),
            )

            table_name = "Default.Default.feast_driver_hourly_stats"
            try:
                cluster.execute_query(
                    f"DROP COLLECTION {table_name} IF EXISTS",
                    QueryOptions(timeout=timedelta(seconds=columnar_timeout)),
                )
            except InvalidCredentialError:
                print("Error: Invalid Cluster Credentials.")
                return

            offline_store = CouchbaseColumnarOfflineStoreConfig(
                type="couchbase.offline",
                connection_string=columnar_connection_string,
                user=columnar_user,
                password=columnar_password,
                timeout=columnar_timeout,
            )

            df_to_columnar(
                df=driver_df, table_name=table_name, offline_store=offline_store
            )

        replace_str_in_file(
            config_file,
            "COUCHBASE_COLUMNAR_CONNECTION_STRING",
            columnar_connection_string,
        )
        replace_str_in_file(config_file, "COUCHBASE_COLUMNAR_USER", columnar_user)
        replace_str_in_file(
            config_file, "COUCHBASE_COLUMNAR_PASSWORD", columnar_password
        )
        replace_str_in_file(
            config_file, "COUCHBASE_COLUMNAR_TIMEOUT", str(columnar_timeout)
        )


if __name__ == "__main__":
    bootstrap()
