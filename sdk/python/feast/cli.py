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

import click
import pkg_resources
import yaml

from feast.client import Client
from feast.config import Config
from feast.core.IngestionJob_pb2 import IngestionJobStatus
from feast.feature_set import FeatureSet, FeatureSetRef
from feast.loaders.yaml import yaml_loader

_logger = logging.getLogger(__name__)

_common_options = [
    click.option("--core-url", help="Set Feast core URL to connect to"),
    click.option("--serving-url", help="Set Feast serving URL to connect to"),
]


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


@cli.group(name="feature-sets")
def feature_set():
    """
    Create and manage feature sets
    """
    pass


@feature_set.command(name="list")
def feature_set_list():
    """
    List all feature sets
    """
    feast_client = Client()  # type: Client

    table = []
    for fs in feast_client.list_feature_sets(project="*", name="*"):
        table.append([fs.name, repr(fs)])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "REFERENCE"], tablefmt="plain"))


@feature_set.command("apply")
# TODO: add project option to overwrite project setting.
@click.option(
    "--filename",
    "-f",
    help="Path to a feature set configuration file that will be applied",
    type=click.Path(exists=True),
)
def feature_set_create(filename):
    """
    Create or update a feature set
    """

    feature_sets = [FeatureSet.from_dict(fs_dict) for fs_dict in yaml_loader(filename)]
    feast_client = Client()  # type: Client
    feast_client.apply(feature_sets)


@feature_set.command("describe")
@click.argument("name", type=click.STRING)
def feature_set_describe(name: str):
    """
    Describe a feature set
    """
    feast_client = Client()  # type: Client
    fs = feast_client.get_feature_set(name=name)
    if not fs:
        print(f'Feature set with name "{name}" could not be found')
        return

    print(yaml.dump(yaml.safe_load(str(fs)), default_flow_style=False, sort_keys=False))


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


@cli.group(name="ingest-jobs")
def ingest_job():
    """
    Manage ingestion jobs
    """
    pass


@ingest_job.command("list")
@click.option("--job-id", "-i", help="Show only ingestion jobs with the given job id")
@click.option(
    "--feature-set-ref",
    "-f",
    help="Show only ingestion job targeting the feature set with the given reference",
)
@click.option(
    "--store-name",
    "-s",
    help="List only ingestion job that ingest into feast store with given name",
)
# TODO: types
def ingest_job_list(job_id, feature_set_ref, store_name):
    """
    List ingestion jobs
    """
    # parse feature set reference
    if feature_set_ref is not None:
        feature_set_ref = FeatureSetRef.from_str(feature_set_ref)

    # pull & render ingestion jobs as a table
    feast_client = Client()
    table = []
    for ingest_job in feast_client.list_ingest_jobs(
        job_id=job_id, feature_set_ref=feature_set_ref, store_name=store_name
    ):
        table.append([ingest_job.id, IngestionJobStatus.Name(ingest_job.status)])

    from tabulate import tabulate

    print(tabulate(table, headers=["ID", "STATUS"], tablefmt="plain"))


@ingest_job.command("describe")
@click.argument("job_id")
def ingest_job_describe(job_id: str):
    """
    Describe the ingestion job with the given id.
    """
    # find ingestion job for id
    feast_client = Client()
    jobs = feast_client.list_ingest_jobs(job_id=job_id)
    if len(jobs) < 1:
        print(f"Ingestion Job with id {job_id} could not be found")
        sys.exit(1)
    job = jobs[0]

    # pretty render ingestion job as yaml
    print(
        yaml.dump(yaml.safe_load(str(job)), default_flow_style=False, sort_keys=False)
    )


@ingest_job.command("stop")
@click.option(
    "--wait", "-w", is_flag=True, help="Wait for the ingestion job to fully stop."
)
@click.option(
    "--timeout",
    "-t",
    default=600,
    help="Timeout in seconds to wait for the job to stop.",
)
@click.argument("job_id")
def ingest_job_stop(wait: bool, timeout: int, job_id: str):
    """
    Stop ingestion job for id.
    """
    # find ingestion job for id
    feast_client = Client()
    jobs = feast_client.list_ingest_jobs(job_id=job_id)
    if len(jobs) < 1:
        print(f"Ingestion Job with id {job_id} could not be found")
        sys.exit(1)
    job = jobs[0]

    feast_client.stop_ingest_job(job)

    # wait for ingestion job to stop
    if wait:
        job.wait(IngestionJobStatus.ABORTED, timeout=timeout)


@ingest_job.command("restart")
@click.argument("job_id")
def ingest_job_restart(job_id: str):
    """
    Restart job for id.
    Waits for the job to fully restart.
    """
    # find ingestion job for id
    feast_client = Client()
    jobs = feast_client.list_ingest_jobs(job_id=job_id)
    if len(jobs) < 1:
        print(f"Ingestion Job with id {job_id} could not be found")
        sys.exit(1)
    job = jobs[0]

    feast_client.restart_ingest_job(job)


@cli.command()
@click.option(
    "--name", "-n", help="Feature set name to ingest data into", required=True
)
@click.option(
    "--filename",
    "-f",
    help="Path to file to be ingested",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "--file-type",
    "-t",
    type=click.Choice(["CSV"], case_sensitive=False),
    help="Type of file to ingest. Defaults to CSV.",
)
def ingest(name, filename, file_type):
    """
    Ingest feature data into a feature set
    """

    feast_client = Client()  # type: Client
    feature_set = feast_client.get_feature_set(name=name)
    feature_set.ingest_file(file_path=filename)


if __name__ == "__main__":
    cli()
