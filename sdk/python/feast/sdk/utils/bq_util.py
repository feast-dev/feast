# Copyright 2018 The Feast Authors
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

import os
import tempfile
import time
from datetime import datetime
import pytz
import fastavro
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.client import Client as BQClient
from google.cloud.bigquery.job import ExtractJobConfig, DestinationFormat
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound
from google.cloud.storage import Client as GCSClient
from google.cloud import storage

from feast.sdk.utils.gs_utils import is_gs_path, split_gs_path, gcs_to_df


def head(client, table, max_rows=10):
    """Get the head of the table. Retrieves rows from the given table at 
        minimum cost
    
    Args:
        client (google.cloud.bigquery.client.Client): bigquery client
        table (google.cloud.bigquery.table.Table): bigquery table to get the 
            head of
        max_rows (int, optional): Defaults to 10. maximum number of rows to 
            retrieve
    
    Returns:
        pandas.DataFrame: dataframe containing the head of rows
    """

    rows = client.list_rows(table, max_results=max_rows)
    rows = [x for x in rows]
    return pd.DataFrame(
        data=[list(x.values()) for x in rows], columns=list(rows[0].keys())
    )


def get_table_name(feature_id, storage_spec):
    """
    Get fully qualified BigQuery table name from a feature ID and its
    storage spec
    Args:
        feature_id(str): ID of a feature
        storage_spec(feast.specs.StorageSpec_pb2.StorageSpec): storage spec of
        the feature

    Returns:
         str: fully qualified table name of the feature.

    """
    if "bigquery" != storage_spec.type:
        raise ValueError("storage spec is not BigQuery storage spec")

    try:
        project = storage_spec.options["project"]
        dataset = storage_spec.options["dataset"]
    except KeyError:
        raise ValueError("storage spec has empty project or dataset option")

    table_name = "_".join(feature_id.split(".")[:2])
    return ".".join([project, dataset, table_name])


def get_default_templocation(bigquery_client, project=None):
    if project is None:
        project = bigquery_client.project
    assert isinstance(project, str)
    assert len(project) > 0
    storage_client = storage.Client()
    default_bucket_name = f"feast-templocation-{project}"
    try:
        storage_client.get_bucket(default_bucket_name)
    except NotFound:
        print(
            f'Default bucket "{default_bucket_name}" not found. Attempting to create it.'
        )
        storage_client.create_bucket(bucket_name=default_bucket_name, project=project)
    return f"gs://{default_bucket_name}"


def query_to_dataframe(
    query: str,
    bigquery_client: bigquery.Client = None,
    storage_client: storage.Client = None,
    project: str = None,
    templocation: str = None,
) -> pd.DataFrame:
    """
    Run a query job on BigQuery and return the result in Pandas DataFrame format

    Args:
        query: BigQuery query e.g. "SELECT * FROM dataset.table"
        bigquery_client:
        storage_client:
        project: Google Cloud project id
        templocation: Google Cloud Storage location to store intermediate files, must start with "gs://"

    Returns: Pandas DataFrame of the query result

    """
    if isinstance(templocation, str) and not templocation.startswith("gs://"):
        raise RuntimeError('templocation must start with "gs://"')

    if bigquery_client is None:
        bigquery_client = bigquery.Client(project=project)

    if project is None:
        project = bigquery_client.project

    query_job = bigquery_client.query(query, project=project)
    query_job_state = ""

    while not query_job.done():
        if query_job.state != query_job_state:
            print(f"Query status: {query_job.state}")
            query_job_state = query_job.state
        time.sleep(5)

    if query_job.state != query_job_state:
        print(f"Query status: {query_job.state}")

    if query_job.exception():
        raise query_job.exception()

    if not templocation:
        templocation = get_default_templocation(bigquery_client, project=project)

    if templocation.endswith("/"):
        templocation += templocation[:-1]

    destination_uri = (
        f"{templocation}/bq-{datetime.now(pytz.utc).strftime('%Y%m%dT%H%M%SZ')}.avro"
    )
    extract_job_config = bigquery.job.ExtractJobConfig(destination_format="AVRO")
    extract_job = bigquery_client.extract_table(
        query_job.destination, destination_uri, job_config=extract_job_config
    )

    while not extract_job.done():
        time.sleep(5)

    if extract_job.exception():
        raise extract_job.exception()

    if not storage_client:
        storage_client = storage.Client(project=project)

    print("Reading query result into DataFrame")

    bucket_name, blob_name = (
        destination_uri.split("/")[2],
        "/".join(destination_uri.split("/")[3:]),
    )
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    downloaded_avro_filename = tempfile.NamedTemporaryFile().name
    blob.download_to_filename(downloaded_avro_filename)

    with open(downloaded_avro_filename, "rb") as avro_file:
        avro_reader = fastavro.reader(avro_file)
        df = pd.DataFrame.from_records(avro_reader)

    return df


class TableDownloader:
    def __init__(self):
        self._bq = None
        self._gcs = None

    @property
    def gcs(self):
        if self._gcs is None:
            self._gcs = GCSClient()
        return self._gcs

    @property
    def bq(self):
        if self._bq is None:
            self._bq = BQClient()
        return self._bq

    def download_table_as_file(self, table_id, dest, staging_location, file_type):
        """
        Download a bigquery table as file
        Args:
            table_id (str): fully qualified BigQuery table id
            dest (str): destination filename
            staging_location (str): url to staging_location (currently
                support a folder in GCS)
            file_type (feast.sdk.resources.feature_set.FileType): (default:
                FileType.CSV) exported file format
        Returns: (str) path to the downloaded file

        """
        if not is_gs_path(staging_location):
            raise ValueError("staging_uri must be a directory in GCS")

        temp_file_name = "temp_{}".format(int(round(time.time() * 1000)))
        staging_file_path = os.path.join(staging_location, temp_file_name)

        job_config = ExtractJobConfig()
        job_config.destination_format = file_type
        src_table = Table.from_string(table_id)
        job = self.bq.extract_table(src_table, staging_file_path, job_config=job_config)

        # await completion
        job.result()

        bucket_name, blob_name = split_gs_path(staging_file_path)
        bucket = self.gcs.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(dest)
        return dest

    def download_table_as_df(self, table_id, staging_location):
        """
        Download a BigQuery table as Pandas Dataframe
        Args:
            table_id (src) : fully qualified BigQuery table id
            staging_location: url to staging_location (currently
                support a folder in GCS)

        Returns: pandas.DataFrame: dataframe of the training dataset

        """
        if not is_gs_path(staging_location):
            raise ValueError("staging_uri must be a directory in GCS")

        temp_file_name = "temp_{}".format(int(round(time.time() * 1000)))
        staging_file_path = os.path.join(staging_location, temp_file_name)

        job_config = ExtractJobConfig()
        job_config.destination_format = DestinationFormat.CSV
        job = self.bq.extract_table(
            Table.from_string(table_id), staging_file_path, job_config=job_config
        )

        # await completion
        job.result()
        return gcs_to_df(staging_file_path)
