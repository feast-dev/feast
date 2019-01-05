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
import time

import pandas as pd
from google.cloud.bigquery.client import Client as BQClient
from google.cloud.bigquery.job import ExtractJobConfig, DestinationFormat
from google.cloud.bigquery.table import Table
from google.cloud.storage import Client as GCSClient

from feast.sdk.utils.gs_utils import is_gs_path, split_gs_path, gs_to_df


def head(client, table, max_rows=10):
    '''Get the head of the table. Retrieves rows from the given table at 
        minimum cost
    
    Args:
        client (google.cloud.bigquery.client.Client): bigquery client
        table (google.cloud.bigquery.table.Table): bigquery table to get the 
            head of
        max_rows (int, optional): Defaults to 10. maximum number of rows to 
            retrieve
    
    Returns:
        pandas.DataFrame: dataframe containing the head of rows
    '''
    
    rows = client.list_rows(table, max_results=max_rows)
    rows = [x for x in rows]
    return pd.DataFrame(
        data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))


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


class TableDownloader:
    def __init__(self):
        self._bq = BQClient()
        self._gcs = GCSClient()

    def download_table_as_file(self, table_id, dest, staging_location,
                               file_type):
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

        temp_file_name = 'temp_{}'.format(
            int(round(time.time() * 1000)))
        staging_file_path = os.path.join(staging_location, temp_file_name)

        job_config = ExtractJobConfig()
        job_config.destination_format = file_type
        src_table = Table.from_string(table_id)
        job = self._bq.extract_table(src_table, staging_file_path,
                                            job_config=job_config)

        # await completion
        job.result()

        bucket_name, blob_name = split_gs_path(staging_file_path)
        bucket = self._gcs.get_bucket(bucket_name)
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

        temp_file_name = 'temp_{}'.format(
            int(round(time.time() * 1000)))
        staging_file_path = os.path.join(staging_location, temp_file_name)

        job_config = ExtractJobConfig()
        job_config.destination_format = DestinationFormat.CSV
        job = self._bq.extract_table(Table.from_string(table_id),
                                     staging_file_path,
                                     job_config=job_config)

        # await completion
        job.result()
        return gs_to_df(staging_file_path)