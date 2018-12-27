import time
import os
from feast.sdk.utils.format import make_feature_id
from feast.sdk.utils.gs_utils import is_gs_path, split_gs_path, gs_to_df
from google.cloud.bigquery.client import Client as BQClient
from google.cloud.bigquery.job import ExtractJobConfig
from google.cloud.bigquery.table import Table
from google.cloud.storage import Client as GCSClient


class FeatureSet:
    """
    Represent a collection of features having same entity and granularity.
    """

    def __init__(self, entity, granularity, features):
        self._features = [make_feature_id(entity, granularity, feature)
                          for feature in features]

    @property
    def features(self):
        """
        Return list of feature ID of this feature set
        Returns: list of feature ID in this feature set

        """
        return self._features


class FileType(object):
    """
    File type for downloading training dataset as file
    """
    CSV = "CSV"
    """CSV file format"""

    JSON = "NEWLINE_DELIMITED_JSON"
    """Newline delimited JSON file format"""

    AVRO = "AVRO"
    """Avro file format"""


class DatasetInfo:
    def __init__(self, table):
        """
            Create instance of DatasetInfo with a BigQuery table as its
            backing store.
        Args:
            table: (google.cloud.bigquery.table.Table) backing table
        """
        if not isinstance(table, Table):
            raise TypeError("table must be a BigQuery table type")

        self._table = table
        self._bq_client = BQClient()
        self._gcs_client = GCSClient()

    def download(self, filename, staging_uri, type=FileType.CSV):
        """
        Download the training dataset as file
        Args:
            filename (str): destination filename
            staging_uri (str): url to staging_location (currently
                support a folder in GCS)
            type (feast.sdk.resources.feature_set.FileType): (default:
            FileType.CSV) exported file format

        """
        if not is_gs_path(staging_uri):
            raise ValueError("staging_uri must be a directory in GCS")

        temp_file_name = 'temp_{}'.format(
            int(round(time.time() * 1000)))
        staging_file_path = os.path.join(staging_uri, temp_file_name)

        job_config = ExtractJobConfig()
        job_config.destination_format = type
        job = self._bq_client.extract_table(self._table, staging_file_path,
                                            job_config=job_config)

        # await completion
        job.result()

        bucket_name, blob_name = split_gs_path(staging_file_path)
        bucket = self._gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(filename)

    def download_to_df(self, staging_uri):
        """
        Download the training dataset as Pandas Dataframe
        Args:
            staging_uri: url to staging_location (currently
                support a folder in GCS)

        Returns: pandas.DataFrame: dataframe of the training dataset

        """
        if not is_gs_path(staging_uri):
            raise ValueError("staging_uri must be a directory in GCS")

        temp_file_name = 'temp_{}'.format(
            int(round(time.time() * 1000)))
        staging_file_path = os.path.join(staging_uri, temp_file_name)

        job_config = ExtractJobConfig()
        job_config.destination_format = FileType.CSV
        job = self._bq_client.extract_table(self._table, staging_file_path,
                                            job_config=job_config)

        # await completion
        job.result()
        return gs_to_df(staging_file_path)
