#
# Copyright 2020 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import shutil
from abc import ABC, ABCMeta, abstractmethod
from tempfile import TemporaryDirectory, TemporaryFile
from typing import List
from typing.io import IO
from urllib.parse import ParseResult

GS = "gs"
S3 = "s3"
LOCAL_FILE = "file"
ABFSS = "abfss"


class AbstractStagingClient(ABC):
    """
    Client used to stage files in order to upload or download datasets into a historical store.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def download_file(self, uri: ParseResult) -> IO[bytes]:
        """
        Downloads a file from an object store and returns a TemporaryFile object
        """
        pass

    def download_directory(self, uri: ParseResult) -> TemporaryDirectory:
        """
        Downloads a directory from an object store and returns a TemporaryDirectory object
        """
        raise NotImplementedError

    @abstractmethod
    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in an object store.
        """
        pass

    @abstractmethod
    def upload_file(self, local_path: str, uri: ParseResult, remote_path: str):
        """
        Uploads a file to an object store.
        """
        pass


class GCSClient(AbstractStagingClient):
    """
    Implementation of AbstractStagingClient for google cloud storage
    """

    def __init__(self):
        try:
            from google.cloud import storage
        except ImportError:
            raise ImportError(
                "Install package google-cloud-storage==1.20.* for gcs staging support"
                "run ```pip install google-cloud-storage==1.20.*```"
            )
        self.gcs_client = storage.Client(project=None)

    def download_file(self, uri: ParseResult) -> IO[bytes]:
        """
        Downloads a file from google cloud storage and returns a TemporaryFile object

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of the file ex: urlparse("gs://bucket/file.avro")

        Returns:
             TemporaryFile object
        """
        url = uri.geturl()
        file_obj = TemporaryFile()
        self.gcs_client.download_blob_to_file(url, file_obj)
        return file_obj

    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in google cloud storage if path has wildcard(*) character.

        Args:
            uri (ParseResult): hostname will be mapped to bucket name.  path will be mapped to object location in google cloud storage.

        Returns:
            List[str]: A list containing the full path to the file(s) in the
                    remote staging location.
        """
        bucket = uri.hostname
        path = uri.path
        gs_bucket = self.gcs_client.get_bucket(bucket)

        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            blob_list = gs_bucket.list_blobs(
                prefix=path.strip("/").split("*")[0], delimiter="/"
            )
            # File path should not be in path (file path must be longer than path)
            return [
                f"{GS}://{bucket}/{file}"
                for file in [x.name for x in blob_list]
                if re.match(regex, file) and file not in path
            ]
        else:
            return [f"{GS}://{bucket}/{path.lstrip('/')}"]

    def upload_file(self, local_path: str, uri: ParseResult, remote_path: str):
        """
        Uploads file to google cloud storage.

        Args:
            local_path (str): Path to the local file that needs to be uploaded/staged
            uri (ParseResult): gs uri
            remote_path (str): relative path to the folder to which the files need to be uploaded
        """
        gs_bucket = self.gcs_client.get_bucket(uri.hostname)
        blob = gs_bucket.blob(remote_path)
        blob.upload_from_filename(local_path)


class S3Client(AbstractStagingClient):
    """
    Implementation of AbstractStagingClient for Aws S3 storage
    """

    def __init__(self):
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "Install package boto3 for s3 staging support"
                "run ```pip install boto3```"
            )
        self.s3_client = boto3.client("s3")

    def download_file(self, uri: ParseResult) -> IO[bytes]:
        """
        Downloads a file from AWS s3 storage and returns a TemporaryFile object

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of the file ex: urlparse("s3://bucket/file.avro")
        Returns:
            TemporaryFile object
        """
        url = uri.path.lstrip("/")
        bucket = uri.hostname
        file_obj = TemporaryFile()
        self.s3_client.download_fileobj(bucket, url, file_obj)
        return file_obj

    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in s3 if path has wildcard(*) character.

        Args:
            uri (ParseResult): hostname will be mapped to s3 bucket name.  path will be mapped to object location in s3.


        Returns:
            List[str]: A list containing the full path to the file(s) in the
                    remote staging location.
        """

        bucket = uri.hostname
        path = uri.path

        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            blob_list = self.s3_client.list_objects(
                Bucket=bucket, Prefix=path.strip("/").split("*")[0], Delimiter="/"
            )
            # File path should not be in path (file path must be longer than path)
            return [
                f"{S3}://{bucket}/{file}"
                for file in [x["Key"] for x in blob_list["Contents"]]
                if re.match(regex, file) and file not in path
            ]
        else:
            return [f"{S3}://{bucket}/{path.lstrip('/')}"]

    def upload_file(self, local_path: str, uri: ParseResult, remote_path: str):
        """
        Uploads file to s3.

        Args:
            local_path (str): Path to the local file that needs to be uploaded/staged
            uri (ParseResult): s3 uri
            remote_path (str): relative path to the folder to which the files need to be uploaded
        """
        with open(local_path, "rb") as file:
            self.s3_client.upload_fileobj(file, uri.hostname, remote_path)


class LocalFSClient(AbstractStagingClient):
    """
    Implementation of AbstractStagingClient for local file
    Note: The is used for E2E tests.
    """

    def __init__(self):
        pass

    def download_file(self, uri: ParseResult) -> IO[bytes]:
        """
        Reads a local file from the disk

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of the file ex: urlparse("file://folder/file.avro")
        Returns:
            TemporaryFile object
        """
        url = uri.path
        file_obj = open(url, "rb")
        return file_obj

    def download_directory(self, uri: ParseResult) -> TemporaryDirectory:
        dir_obj = TemporaryDirectory()
        os.rmdir(dir_obj.name)
        dest = dir_obj
        shutil.copytree(uri.path, dest.name)
        return dest

    def list_files(self, uri: ParseResult) -> List[str]:
        raise NotImplementedError("list files not implemented for Local file")

    def upload_file(self, local_path: str, uri: ParseResult, remote_path: str):
        pass  # For test cases


def get_staging_client(scheme):
    """
    Initialization of a specific client object(GCSClient, S3Client, AzureClient etc.)

    Args:
        scheme (str): uri scheme: s3, gs, abfss or file

    Returns:
        An object of concrete implementation of AbstractStagingClient
    """
    try:
        return storage_clients[scheme]()
    except ValueError:
        raise Exception(
            f"Could not identify file scheme {scheme}. Only gs://, file://, s3://, and abfss:// are supported"
        )


class AzureClient(AbstractStagingClient):
    """
    Implementation of AbstractStagingClient for Azure data lake storage Gen2
    """

    def __init__(self):
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            raise ImportError(
                "Install package azure-storage-file-datalake for Azure Blob staging support"
                "run ```pip install azure-storage-file-datalake```"
            )
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "Install package azure-identity for Azure Blob staging support"
                "run ```pip install azure-identity```"
            )
        self.credential = DefaultAzureCredential(
            exclude_interactive_browser_credential=False
        )
        self.protocol = "https"
        self.dls_client = DataLakeServiceClient

    def download_file(self, uri: ParseResult) -> IO[bytes]:
        """
        Downloads a file from Azure Blob storage and returns a TemporaryFile object

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of the file ex: urlparse("abfss://filesystem@account.dfs.core.windows.net/file.avro")
        Returns:
            TemporaryFile object
        """
        client = self.dls_client(
            f"{self.protocol}://{uri.hostname}", credential=self.credential
        )

        file_obj = TemporaryFile()
        client.get_file_client(uri.username, uri.path).download_file().readinto(
            file_obj
        )
        file_obj.flush()
        return file_obj

    def download_directory(self, uri: ParseResult) -> TemporaryDirectory:
        client = self.dls_client(
            f"{self.protocol}://{uri.hostname}", credential=self.credential
        )

        uri_path = uri.path.lstrip("/")
        fs_client = client.get_file_system_client(uri.username)
        blob_list = fs_client.get_paths(uri.path)
        dir_obj = TemporaryDirectory()
        for path_properties in blob_list:
            blob = path_properties.name
            blob_rel_path = os.path.relpath(blob, uri_path)
            local_path = f"{dir_obj.name}/{blob_rel_path}"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as file_obj:
                client.get_file_client(uri.username, blob).download_file().readinto(
                    file_obj
                )
        return dir_obj

    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in Azure store if path has wildcard(*) character.

        Args:
            uri (ParseResult): path will be mapped to object location in azure blob storage.

        Returns:
            List[str]: A list containing the full path to the file(s) in the
                    remote staging location.
        """
        path = uri.path

        client = self.dls_client(
            f"{self.protocol}://{uri.hostname}", credential=self.credential
        )

        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            directory = path.split("*")[0]
            fs_client = client.get_file_system_client(uri.username)
            blob_list = fs_client.get_paths(directory)
            # File path should not be in path (file path must be longer than path)
            return [
                f"{ABFSS}://{uri.username}@{uri.hostname}/{file}"
                for file in [x.name for x in blob_list]
                if re.match(regex, file) and file not in path
            ]
        else:
            return [f"{ABFSS}://{uri.username}@{uri.hostname}/{path.lstrip('/')}"]

    def upload_file(self, local_path: str, uri: ParseResult, remote_path: str):
        """
        Uploads file to Azure Blob.

        Args:
            local_path (str): Path to the local file that needs to be uploaded/staged
            uri (ParseResult): Azure blob storage uri
            remote_path (str): relative path to the folder to which the files need to be uploaded
        """
        client = self.dls_client(
            f"{self.protocol}://{uri.hostname}", credential=self.credential
        )
        fs_client = client.get_file_system_client(uri.username)

        with open(local_path, "rb") as file:
            file_contents = file.read()
            file_client = fs_client.get_file_client(remote_path)
            file_client.upload_data(file_contents, overwrite=True)


storage_clients = {
    GS: GCSClient,
    S3: S3Client,
    LOCAL_FILE: LocalFSClient,
    ABFSS: AzureClient,
}
