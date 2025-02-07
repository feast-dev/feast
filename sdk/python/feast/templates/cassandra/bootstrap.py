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

import os
import pathlib
import sys
from datetime import datetime, timedelta

import click

from feast.file_utils import (
    remove_lines_from_file,
    replace_str_in_file,
    write_setting_or_remove,
)


def collect_cassandra_store_settings():
    """
    Interactive CLI collection of settings for the feature store yaml.
    Returns a dict with all keys, possibly some are None.
    """

    db_type = click.prompt(
        "Regular [C]assandra or [A]stra DB?",
        type=click.Choice(["C", "A"]),
        show_choices=False,
        default="C",
    )
    is_astra = db_type == "A"

    if is_astra:
        c_secure_bundle_path = click.prompt(
            "Enter the full path to your Secure Connect Bundle"
        )
        c_hosts = None
        c_port = None
        c_username = click.prompt("Enter the Client ID from your Astra DB token")
        c_password = click.prompt(
            "Enter the Client Secret from your Astra DB token",
            hide_input=True,
        )
    else:
        # it's regular Cassandra
        c_secure_bundle_path = None
        hosts_string = click.prompt(
            ("Enter the seed hosts of your cluster (comma-separated IP addresses)"),
            default="127.0.0.1",
        )
        c_hosts = [
            haddr
            for haddr in (host.strip() for host in hosts_string.split(","))
            if haddr != ""
        ]
        if not c_hosts:
            print("*Error* : seed host list cannot be empty.")
            sys.exit(1)
        needs_port = click.confirm("Need to specify port?", default=False)
        if needs_port:
            c_port = click.prompt("    Port to use", default=9042, type=int)
        else:
            c_port = None
        use_auth = click.confirm(
            "Do you need username/password?",
            default=False,
        )
        if use_auth:
            c_username = click.prompt("    Database username")
            c_password = click.prompt("    Database password", hide_input=True)
        else:
            c_username = None
            c_password = None

    c_keyspace = click.prompt(
        "Specify the keyspace to use",
        default="feast_keyspace",
    )

    specify_protocol_version = click.confirm(
        "Specify protocol version?",
        default=False,
    )
    if specify_protocol_version:
        c_protocol_version = click.prompt(
            "    Protocol version",
            default={"A": 4, "C": 5}.get(db_type, 5),
            type=int,
        )
    else:
        c_protocol_version = None

    specify_lb = click.confirm("Specify load-balancing?", default=False)
    if specify_lb:
        c_local_dc = click.prompt(
            "    Local datacenter (for load-balancing)",
            default="datacenter1" if db_type == "C" else None,
        )
        c_load_balancing_policy = click.prompt(
            "    Load-balancing policy",
            type=click.Choice(
                [
                    "TokenAwarePolicy(DCAwareRoundRobinPolicy)",
                    "DCAwareRoundRobinPolicy",
                ]
            ),
            default="TokenAwarePolicy(DCAwareRoundRobinPolicy)",
        )
    else:
        c_local_dc = None
        c_load_balancing_policy = None

    specify_concurrency = click.confirm("Specify concurrency levels?", default=False)
    if specify_concurrency:
        c_r_concurrency = click.prompt(
            "    Read-concurrency level?", default=100, type=int
        )
        c_w_concurrency = click.prompt(
            "    Write-concurrency level?", default=100, type=int
        )
    else:
        c_r_concurrency = None
        c_w_concurrency = None

    return {
        "c_secure_bundle_path": c_secure_bundle_path,
        "c_hosts": c_hosts,
        "c_port": c_port,
        "c_username": c_username,
        "c_password": c_password,
        "c_keyspace": c_keyspace,
        "c_protocol_version": c_protocol_version,
        "c_local_dc": c_local_dc,
        "c_load_balancing_policy": c_load_balancing_policy,
        "c_r_concurrency": c_r_concurrency,
        "c_w_concurrency": c_w_concurrency,
    }


def apply_cassandra_store_settings(config_file, settings):
    """
    In-place replacements to `config_file` according to the settings
    to make the yaml a proper Cassandra/AstraDB feature-store yaml.
    `settings` must have all its keys, possibly the optional ones set to None:
        'c_secure_bundle_path'
        'c_hosts'
        'c_port'
        'c_username'
        'c_password'
        'c_keyspace'
        'c_protocol_version'
        'c_local_dc'
        'c_load_balancing_policy'
        'c_r_concurrency'
        'c_w_concurrency'
    """
    write_setting_or_remove(
        config_file,
        settings["c_secure_bundle_path"],
        "secure_bundle_path",
        "/path/to/secure/bundle.zip",
    )
    #
    if settings["c_hosts"]:
        replace_str_in_file(
            config_file,
            "        - 127.0.0.1",
            os.linesep.join(f"        - {c_host}" for c_host in settings["c_hosts"]),
        )
    else:
        remove_lines_from_file(config_file, "hosts:")
        remove_lines_from_file(config_file, "- 127.0.0.1")
    #
    write_setting_or_remove(
        config_file,
        settings["c_port"],
        "port",
        "9042",
    )
    #
    write_setting_or_remove(
        config_file,
        settings["c_username"],
        "username",
        "c_username",
    )
    #
    write_setting_or_remove(
        config_file,
        settings["c_password"],
        "password",
        "c_password",
    )
    #
    replace_str_in_file(
        config_file,
        "feast_keyspace",
        settings["c_keyspace"],
    )
    #
    write_setting_or_remove(
        config_file,
        settings["c_protocol_version"],
        "protocol_version",
        "c_protocol_version",
    )
    # it is assumed that if there's local_dc also there's l.b.p.
    if settings["c_local_dc"] is not None:
        replace_str_in_file(
            config_file,
            "c_local_dc",
            settings["c_local_dc"],
        )
        replace_str_in_file(
            config_file,
            "c_load_balancing_policy",
            settings["c_load_balancing_policy"],
        )
    else:
        remove_lines_from_file(config_file, "load_balancing:")
        remove_lines_from_file(config_file, "local_dc:")
        remove_lines_from_file(config_file, "load_balancing_policy:")

    write_setting_or_remove(
        config_file,
        settings["c_r_concurrency"],
        "read_concurrency",
        "c_r_concurrency",
    )
    write_setting_or_remove(
        config_file,
        settings["c_w_concurrency"],
        "write_concurrency",
        "c_w_concurrency",
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
    settings = collect_cassandra_store_settings()
    apply_cassandra_store_settings(config_file, settings)


if __name__ == "__main__":
    bootstrap()
