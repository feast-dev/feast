#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import pathlib
from datetime import datetime, timedelta

import click

from feast.file_utils import (
    remove_lines_from_file,
    replace_str_in_file,
    write_setting_or_remove,
)


def collect_hazelcast_online_store_settings():
    c_cluster_name = None
    c_members = None
    c_ca_path = None
    c_cert_path = None
    c_key_path = None
    c_discovery_token = None
    c_ttl_seconds = None

    cluster_type = click.prompt(
        "Would you like to connect a [L]ocal cluster or [V]iridian cluster?",
        type=click.Choice(["L", "V"]),
        show_choices=False,
        default="L",
    )
    is_viridian = cluster_type == "V"

    if is_viridian:
        c_cluster_name = click.prompt("Cluster ID: ")
        c_discovery_token = click.prompt("Discovery Token: ")
        c_ca_path = click.prompt("CA file path: ")
        c_cert_path = click.prompt("CERT file path: ")
        c_key_path = click.prompt("Key file path: ")
    else:
        c_cluster_name = click.prompt(
            "Cluster name: ",
            default="dev",
        )
        c_members = click.prompt(
            "Cluster members:",
            default="localhost:5701",
        )
        needs_ssl = click.confirm("Use TLS/SSL?", default=False)
        if needs_ssl:
            c_ca_path = click.prompt("CA file path: ")
            c_cert_path = click.prompt("CERT file path: ")
            c_key_path = click.prompt("Key file path: ")

    c_ttl_seconds = click.prompt(
        "Key TTL seconds: ",
        default=0,
    )
    return {
        "c_cluster_name": c_cluster_name,
        "c_members": c_members,
        "c_ca_path": c_ca_path,
        "c_cert_path": c_cert_path,
        "c_key_path": c_key_path,
        "c_discovery_token": c_discovery_token,
        "c_ttl_seconds": c_ttl_seconds,
    }


def apply_hazelcast_store_settings(config_file, settings):
    write_setting_or_remove(
        config_file,
        settings["c_cluster_name"],
        "cluster_name",
        "c_cluster_name",
    )
    #
    write_setting_or_remove(
        config_file,
        settings["c_discovery_token"],
        "discovery_token",
        "c_discovery_token",
    )
    #
    if settings["c_members"] is not None:
        settings["c_members"] = "[" + settings["c_members"] + "]"
    write_setting_or_remove(
        config_file,
        settings["c_members"],
        "cluster_members",
        "c_members",
    )
    #
    write_setting_or_remove(
        config_file,
        settings["c_ca_path"],
        "ssl_cafile_path",
        "c_ca_path",
    )
    #
    write_setting_or_remove(
        config_file,
        settings["c_cert_path"],
        "ssl_certfile_path",
        "c_cert_path",
    )
    #
    write_setting_or_remove(
        config_file,
        settings["c_key_path"],
        "ssl_keyfile_path",
        "c_key_path",
    )
    if settings["c_ca_path"] is None:
        remove_lines_from_file(
            config_file,
            "ssl_password: ${SSL_PASSWORD}",
            True,
        )
    #
    replace_str_in_file(
        config_file,
        "c_ttl_seconds",
        f"{settings['c_ttl_seconds']}",
    )


def bootstrap():
    """
    Bootstrap() will automatically be called
    from the init_repo() during `feast init`.
    """
    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    config_file = repo_path / "feature_store.yaml"

    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)
    #
    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(
        driver_entities,
        start_date,
        end_date,
    )
    #
    driver_stats_path = data_path / "driver_stats.parquet"
    driver_df.to_parquet(path=str(driver_stats_path), allow_truncated_timestamps=True)

    # example_repo.py
    example_py_file = repo_path / "example_repo.py"
    replace_str_in_file(
        example_py_file, "%PARQUET_PATH%", str(driver_stats_path.relative_to(repo_path))
    )

    # store config yaml, interact with user and then customize file:
    settings = collect_hazelcast_online_store_settings()
    apply_hazelcast_store_settings(config_file, settings)


if __name__ == "__main__":
    bootstrap()
