# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import sys
from typing import Dict

import click
import pkg_resources
import yaml

from feast.client import Client
from feast.config import Config
from feast.constants import ConfigOptions as opt
from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.job_service import start_job_service
from feast.loaders.yaml import yaml_loader

_logger = logging.getLogger(__name__)

_common_options = [
    click.option("--core-url", help="Set Feast core URL to connect to"),
    click.option("--serving-url", help="Set Feast serving URL to connect to"),
    click.option("--job-service-url", help="Set Feast job service URL to connect to"),
]
DATETIME_ISO = "%Y-%m-%dT%H:%M:%s"


def common_options(func):
    """
    Options that are available for most CLI commands
    """
    for option in reversed(_common_options):
        func = option(func)
    return func


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--client-only", "-c", is_flag=True, help="Print only the version of the CLI"
)
@common_options
def version(client_only: bool, **kwargs):
    """
    Displays version and connectivity information
    """

    try:
        feast_versions_dict = {
            "sdk": {"version": str(pkg_resources.get_distribution("feast"))}
        }

        if not client_only:
            feast_client = Client(**kwargs)
            feast_versions_dict.update(feast_client.version())

        print(json.dumps(feast_versions_dict))
    except Exception as e:
        _logger.error("Error initializing backend store")
        _logger.exception(e)
        sys.exit(1)


@cli.group()
def config():
    """
    View and edit Feast properties
    """
    pass


@config.command(name="list")
def config_list():
    """
    List Feast properties for the currently active configuration
    """
    try:
        print(Config())
    except Exception as e:
        _logger.error("Error occurred when reading Feast configuration file")
        _logger.exception(e)
        sys.exit(1)


@config.command(name="set")
@click.argument("prop")
@click.argument("value")
def config_set(prop, value):
    """
    Set a Feast properties for the currently active configuration
    """
    try:
        conf = Config()
        conf.set(option=prop.strip(), value=value.strip())
        conf.save()
    except Exception as e:
        _logger.error("Error in reading config file")
        _logger.exception(e)
        sys.exit(1)


@cli.group(name="entities")
def entity():
    """
    Create and manage entities
    """
    pass


@entity.command("apply")
@click.option(
    "--filename",
    "-f",
    help="Path to an entity configuration file that will be applied",
    type=click.Path(exists=True),
)
@click.option(
    "--project",
    "-p",
    help="Project that entity belongs to",
    type=click.STRING,
    default="default",
)
def entity_create(filename, project):
    """
    Create or update an entity
    """

    entities = [Entity.from_dict(entity_dict) for entity_dict in yaml_loader(filename)]
    feast_client = Client()  # type: Client
    feast_client.apply(entities, project)


@entity.command("describe")
@click.argument("name", type=click.STRING)
@click.option(
    "--project",
    "-p",
    help="Project that entity belongs to",
    type=click.STRING,
    default="default",
)
def entity_describe(name: str, project: str):
    """
    Describe an entity
    """
    feast_client = Client()  # type: Client
    entity = feast_client.get_entity(name=name, project=project)

    if not entity:
        print(f'Entity with name "{name}" could not be found')
        return

    print(
        yaml.dump(
            yaml.safe_load(str(entity)), default_flow_style=False, sort_keys=False
        )
    )


@entity.command(name="list")
@click.option(
    "--project",
    "-p",
    help="Project that entity belongs to",
    type=click.STRING,
    default="",
)
@click.option(
    "--labels",
    "-l",
    help="Labels to filter for entities",
    type=click.STRING,
    default="",
)
def entity_list(project: str, labels: str):
    """
    List all entities
    """
    feast_client = Client()  # type: Client

    labels_dict = _get_labels_dict(labels)

    table = []
    for entity in feast_client.list_entities(project=project, labels=labels_dict):
        table.append([entity.name, entity.description, entity.value_type])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "DESCRIPTION", "TYPE"], tablefmt="plain"))


@cli.group(name="feature-tables")
def feature_table():
    """
    Create and manage feature tables
    """
    pass


def _get_labels_dict(label_str: str) -> Dict[str, str]:
    """
    Converts CLI input labels string to dictionary format if provided string is valid.

    Args:
        label_str: A comma-separated string of key-value pairs

    Returns:
        Dict of key-value label pairs
    """
    labels_dict: Dict[str, str] = {}
    labels_kv = label_str.split(",")
    if label_str == "":
        return labels_dict
    if len(labels_kv) % 2 == 1:
        raise ValueError("Uneven key-value label pairs were entered")
    for k, v in zip(labels_kv[0::2], labels_kv[1::2]):
        labels_dict[k] = v
    return labels_dict


@feature_table.command("apply")
@click.option(
    "--filename",
    "-f",
    help="Path to a feature table configuration file that will be applied",
    type=click.Path(exists=True),
)
def feature_table_create(filename):
    """
    Create or update a feature table
    """

    feature_tables = [
        FeatureTable.from_dict(ft_dict) for ft_dict in yaml_loader(filename)
    ]
    feast_client = Client()  # type: Client
    feast_client.apply(feature_tables)


@feature_table.command("describe")
@click.argument("name", type=click.STRING)
@click.option(
    "--project",
    "-p",
    help="Project that feature table belongs to",
    type=click.STRING,
    default="default",
)
def feature_table_describe(name: str, project: str):
    """
    Describe a feature table
    """
    feast_client = Client()  # type: Client
    ft = feast_client.get_feature_table(name=name, project=project)

    if not ft:
        print(f'Feature table with name "{name}" could not be found')
        return

    print(yaml.dump(yaml.safe_load(str(ft)), default_flow_style=False, sort_keys=False))


@feature_table.command(name="list")
@click.option(
    "--project",
    "-p",
    help="Project that feature table belongs to",
    type=click.STRING,
    default="",
)
@click.option(
    "--labels",
    "-l",
    help="Labels to filter for feature tables",
    type=click.STRING,
    default="",
)
def feature_table_list(project: str, labels: str):
    """
    List all feature tables
    """
    feast_client = Client()  # type: Client

    labels_dict = _get_labels_dict(labels)

    table = []
    for ft in feast_client.list_feature_tables(project=project, labels=labels_dict):
        table.append([ft.name, ft.entities])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "ENTITIES"], tablefmt="plain"))


@cli.group(name="projects")
def project():
    """
    Create and manage projects
    """
    pass


@project.command(name="create")
@click.argument("name", type=click.STRING)
def project_create(name: str):
    """
    Create a project
    """
    feast_client = Client()  # type: Client
    feast_client.create_project(name)


@project.command(name="archive")
@click.argument("name", type=click.STRING)
def project_archive(name: str):
    """
    Archive a project
    """
    feast_client = Client()  # type: Client
    feast_client.archive_project(name)


@project.command(name="list")
def project_list():
    """
    List all projects
    """
    feast_client = Client()  # type: Client

    table = []
    for project in feast_client.list_projects():
        table.append([project])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME"], tablefmt="plain"))


@cli.group(name="jobs")
def job():
    """
    Create and manage jobs
    """
    pass


@job.command(name="sync-offline-to-online")
@click.option(
    "--feature-table",
    "-t",
    help="Feature table name of data to be synced",
    type=click.STRING,
    required=True,
)
@click.option("--start-time", "-s", help="Interval start", required=True)
@click.option("--end-time", "-e", help="Interval end", required=True)
def sync_offline_to_online(feature_table: str, start_time: str, end_time: str):
    """
    Sync offline store data to online store
    """
    from datetime import datetime

    client = Client()
    table = client.get_feature_table(feature_table)
    client.start_offline_to_online_ingestion(
        table,
        datetime.strptime(start_time, DATETIME_ISO),
        datetime.strptime(end_time, DATETIME_ISO),
    )


@job.command(name="start-stream-to-online")
@click.option(
    "--feature-table",
    "-t",
    help="Feature table name of job to be started",
    type=click.STRING,
    required=True,
)
@click.option(
    "--jar",
    "-j",
    help="The file path to the uber jar for offline to online ingestion spark job",
    default="",
)
def start_stream_to_online(feature_table: str, jar: str):
    """
    Start stream to online sync job
    """

    client = Client()
    table = client.get_feature_table(feature_table)
    client.start_stream_to_online_ingestion(table, [jar] if jar else [])


@job.command(name="stop-stream-to-online")
@click.option(
    "--feature-table",
    "-t",
    help="Feature table name of job to be stopped",
    type=click.STRING,
    required=True,
)
def stop_stream_to_online(feature_table: str):
    """
    Stop stream to online sync job
    """

    spark_launcher = Config().get(opt.SPARK_LAUNCHER)

    if spark_launcher == "emr":
        import feast.pyspark.aws.jobs

        feast.pyspark.aws.jobs.stop_stream_to_online(feature_table)
    else:
        raise NotImplementedError(
            f"Feast currently does not provide support for the specified spark launcher: {spark_launcher}"
        )


@job.command()
def list_jobs():
    """
    List jobs
    """
    from tabulate import tabulate

    spark_launcher = Config().get(opt.SPARK_LAUNCHER)

    if spark_launcher == "emr":
        import feast.pyspark.aws.jobs

        jobs = feast.pyspark.aws.jobs.list_jobs(None, None)
        print(
            tabulate(
                jobs, headers=feast.pyspark.aws.jobs.JobInfo._fields, tablefmt="plain"
            )
        )
    else:
        raise NotImplementedError(
            f"Feast currently does not provide support for the specified spark launcher: {spark_launcher}"
        )


@cli.command()
@click.option(
    "--features",
    "-f",
    help="Features in feature_table:feature format, comma separated",
    required=True,
)
@click.option(
    "--entity-df-path",
    "-e",
    help="Path to entity df in CSV format. It is assumed to have event_timestamp column and a header.",
    required=True,
)
@click.option(
    "--entity-df-dtype",
    "-d",
    help="Dtypes for entity df, in JSON format",
    required=False,
)
@click.option("--destination", "-d", help="Destination", default="")
def get_historical_features(
    features: str, entity_df_path: str, entity_df_dtype: str, destination: str
):
    """
    Get historical features. This CLI command is mostly for testing/easy demos; use the
    corresponding API method in production.

    The main reason why this command is unlikely to be more broadly useful is that we make quite a
    few assumptions about the entity dataframe, namely:
        * it has to have `event_timestamp` column
        * it has to parse cleanly by `pandas.read_csv()` with no extra tuning of data types
    """
    import pandas

    client = Client()

    if entity_df_dtype:
        dtype = json.loads(entity_df_dtype)
        entity_df = pandas.read_csv(
            entity_df_path, sep=None, engine="python", dtype=dtype
        )
    else:
        entity_df = pandas.read_csv(entity_df_path, sep=None, engine="python")

    entity_df["event_timestamp"] = pandas.to_datetime(entity_df["event_timestamp"])

    uploaded_df = client.stage_dataframe(entity_df, "event_timestamp")

    job = client.get_historical_features(features.split(","), uploaded_df,)
    print(job.get_output_file_uri())


@cli.command(name="server")
def server():
    """
    Start Feast Job Service
    """
    start_job_service()


if __name__ == "__main__":
    cli()
