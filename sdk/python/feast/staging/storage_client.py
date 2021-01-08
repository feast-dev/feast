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
import hashlib
import os
import re
import shutil
from abc import ABC, ABCMeta, abstractmethod
from tempfile import TemporaryFile
from typing import List, Optional, Tuple
from typing.io import IO
from urllib.parse import ParseResult, urlparse

from google.auth.exceptions import DefaultCredentialsError

from feast.config import Config
from feast.constants import ConfigOptions as opt

GS = "gs"
S3 = "s3"
S3A = "s3a"
AZURE_SCHEME = "wasbs"
LOCAL_FILE = "file"


def _hash_fileobj(fileobj: IO[bytes]) -> str:
    """ Compute sha256 hash of a file. File pointer will be reset to 0 on return. """
    fileobj.seek(0)
    h = hashlib.sha256()
    for block in iter(lambda: fileobj.read(2 ** 20), b""):
        h.update(block)
    fileobj.seek(0)
    return h.hexdigest()


def _gen_remote_uri(
    fileobj: IO[bytes],
    remote_uri: Optional[ParseResult],
    remote_path_prefix: Optional[str],
    remote_path_suffix: Optional[str],
    sha256sum: Optional[str],
) -> ParseResult:
    if remote_uri is None:
        assert remote_path_prefix is not None and remote_path_suffix is not None

        if sha256sum is None:
            sha256sum = _hash_fileobj(fileobj)

        return urlparse(
            os.path.join(remote_path_prefix, f"{sha256sum}{remote_path_suffix}")
        )
    else:
        return remote_uri


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

    @abstractmethod
    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in an object store.
        """
        pass

    @abstractmethod
    def upload_fileobj(
        self,
        fileobj: IO[bytes],
        local_path: str,
        *,
        remote_uri: Optional[ParseResult] = None,
        remote_path_prefix: Optional[str] = None,
        remote_path_suffix: Optional[str] = None,
    ) -> ParseResult:
        """
        Uploads a file to an object store. You can either specify the destination object URI,
        or destination suffix+prefix. In the latter case, this interface will work as a
        content-addressable storage and the remote path will be computed using sha256 of the
        uploaded content as `$remote_path_prefix/$sha256$remote_path_suffix`

        Args:
            fileobj (IO[bytes]): file-like object containing the data to be uploaded. It needs to
                supports seek() operation in addition to read/write.
            local_path (str): a file name associated with fileobj. This param is only used for
                diagnostic messages. If `fileobj` is a local file, pass its filename here.
            remote_uri (ParseResult or None): destination object URI to upload to
            remote_path_prefix (str or None): destination path prefix to upload to when using
                content-addressable storage mode
            remote_path_suffix (str or None): destination path suffix to upload to when using
                content-addressable storage mode

        Returns:
            ParseResult: the URI to the uploaded file. It would be the same as `remote_uri` if
            `remote_uri` was passed in. Otherwise it will be the path computed from
            `remote_path_prefix` and `remote_path_suffix`.
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
        try:
            self.gcs_client = storage.Client(project=None)
        except DefaultCredentialsError:
            self.gcs_client = storage.Client.create_anonymous_client()

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
        file_obj.seek(0)
        return file_obj

    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in google cloud storage if path has wildcard(*) character.

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of this location

        Returns:
            List[str]: A list containing the full path to the file(s) in the
                    remote staging location.
        """

        bucket, path = self._uri_to_bucket_key(uri)
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
            return [f"{GS}://{bucket}/{path}"]

    def _uri_to_bucket_key(self, remote_path: ParseResult) -> Tuple[str, str]:
        assert remote_path.hostname is not None
        return remote_path.hostname, remote_path.path.lstrip("/")

    def upload_fileobj(
        self,
        fileobj: IO[bytes],
        local_path: str,
        *,
        remote_uri: Optional[ParseResult] = None,
        remote_path_prefix: Optional[str] = None,
        remote_path_suffix: Optional[str] = None,
    ) -> ParseResult:
        remote_uri = _gen_remote_uri(
            fileobj, remote_uri, remote_path_prefix, remote_path_suffix, None
        )
        bucket, key = self._uri_to_bucket_key(remote_uri)
        gs_bucket = self.gcs_client.get_bucket(bucket)
        blob = gs_bucket.blob(key)
        blob.upload_from_file(fileobj)
        return remote_uri


class S3Client(AbstractStagingClient):
    """
       Implementation of AbstractStagingClient for Aws S3 storage
    """

    def __init__(self, endpoint_url: str = None, url_scheme="s3"):
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "Install package boto3 for s3 staging support"
                "run ```pip install boto3```"
            )
        self.s3_client = boto3.client("s3", endpoint_url=endpoint_url)
        self.url_scheme = url_scheme

    def download_file(self, uri: ParseResult) -> IO[bytes]:
        """
        Downloads a file from AWS s3 storage and returns a TemporaryFile object

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of the file ex: urlparse("s3://bucket/file.avro")
        Returns:
            TemporaryFile object
        """
        bucket, url = self._uri_to_bucket_key(uri)
        file_obj = TemporaryFile()
        self.s3_client.download_fileobj(bucket, url, file_obj)
        return file_obj

    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in s3 if path has wildcard(*) character.

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of this location

        Returns:
            List[str]: A list containing the full path to the file(s) in the
                    remote staging location.
        """

        bucket, path = self._uri_to_bucket_key(uri)
        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            blob_list = self.s3_client.list_objects(
                Bucket=bucket, Prefix=path.strip("/").split("*")[0], Delimiter="/"
            )
            # File path should not be in path (file path must be longer than path)
            return [
                f"{self.url_scheme}://{bucket}/{file}"
                for file in [x["Key"] for x in blob_list["Contents"]]
                if re.match(regex, file) and file not in path
            ]
        else:
            return [f"{self.url_scheme}://{bucket}/{path}"]

    def _uri_to_bucket_key(self, remote_path: ParseResult) -> Tuple[str, str]:
        assert remote_path.hostname is not None
        return remote_path.hostname, remote_path.path.lstrip("/")

    def upload_fileobj(
        self,
        fileobj: IO[bytes],
        local_path: str,
        *,
        remote_uri: Optional[ParseResult] = None,
        remote_path_prefix: Optional[str] = None,
        remote_path_suffix: Optional[str] = None,
    ) -> ParseResult:
        sha256sum = _hash_fileobj(fileobj)
        remote_uri = _gen_remote_uri(
            fileobj, remote_uri, remote_path_prefix, remote_path_suffix, sha256sum
        )

        import botocore

        bucket, key = self._uri_to_bucket_key(remote_uri)

        try:
            head_response = self.s3_client.head_object(Bucket=bucket, Key=key)
            if head_response["Metadata"]["sha256sum"] == sha256sum:
                # File already exists
                return remote_uri
            else:
                print(f"Uploading {local_path} to {remote_uri}")
                self.s3_client.upload_fileobj(
                    fileobj,
                    bucket,
                    key,
                    ExtraArgs={"Metadata": {"sha256sum": sha256sum}},
                )
                return remote_uri
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

            self.s3_client.upload_fileobj(
                fileobj, bucket, key, ExtraArgs={"Metadata": {"sha256sum": sha256sum}},
            )
            return remote_uri


class AzureBlobClient(AbstractStagingClient):
    """
       Implementation of AbstractStagingClient for Azure Blob storage
    """

    def __init__(self, account_name: str, account_access_key: str):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise ImportError(
                "Install package azure-storage-blob for azure blob staging support"
                "run ```pip install azure-storage-blob```"
            )
        self.account_name = account_name
        account_url = f"https://{account_name}.blob.core.windows.net"
        self.blob_service_client = BlobServiceClient(
            account_url=account_url, credential=account_access_key
        )

    def download_file(self, uri: ParseResult) -> IO[bytes]:
        """
        Downloads a file from Azure blob storage and returns a TemporaryFile object

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of the file ex: urlparse("wasbs://bucket@account_name.blob.core.windows.net/file.avro")

        Returns:
             TemporaryFile object
        """
        bucket, path = self._uri_to_bucket_key(uri)
        container_client = self.blob_service_client.get_container_client(bucket)
        return container_client.download_blob(path).readall()

    def list_files(self, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in azure blob storage if path has wildcard(*) character.

        Args:
            uri (urllib.parse.ParseResult): Parsed uri of this location

        Returns:
            List[str]: A list containing the full path to the file(s) in the
                    remote staging location.
        """

        bucket, path = self._uri_to_bucket_key(uri)
        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            container_client = self.blob_service_client.get_container_client(bucket)
            blob_list = container_client.list_blobs(
                name_starts_with=path.strip("/").split("*")[0]
            )
            # File path should not be in path (file path must be longer than path)
            return [
                f"wasbs://{bucket}@{self.account_name}.blob.core.windows.net/{file}"
                for file in [x.name for x in blob_list]
                if re.match(regex, file) and file not in path
            ]
        else:
            return [
                f"wasbs://{bucket}@{self.account_name}.blob.core.windows.net/{path}"
            ]

    def _uri_to_bucket_key(self, uri: ParseResult) -> Tuple[str, str]:
        assert uri.hostname == f"{self.account_name}.blob.core.windows.net"
        assert uri.username
        bucket = uri.username
        key = uri.path.lstrip("/")
        return bucket, key

    def upload_fileobj(
        self,
        fileobj: IO[bytes],
        local_path: str,
        *,
        remote_uri: Optional[ParseResult] = None,
        remote_path_prefix: Optional[str] = None,
        remote_path_suffix: Optional[str] = None,
    ) -> ParseResult:
        remote_uri = _gen_remote_uri(
            fileobj, remote_uri, remote_path_prefix, remote_path_suffix, None
        )
        bucket, key = self._uri_to_bucket_key(remote_uri)
        container_client = self.blob_service_client.get_container_client(bucket)
        container_client.upload_blob(name=key, data=fileobj, overwrite=True)
        return remote_uri


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
            uri (urllib.parse.ParseResult): Parsed uri of the file ex: urlparse("file:///folder/file.avro")
        Returns:
            TemporaryFile object
        """
        url = uri.path
        file_obj = open(url, "rb")
        return file_obj

    def list_files(self, uri: ParseResult) -> List[str]:
        raise NotImplementedError("list files not implemented for Local file")

    def _uri_to_path(self, uri: ParseResult) -> str:
        return uri.path

    def upload_fileobj(
        self,
        fileobj: IO[bytes],
        local_path: str,
        *,
        remote_uri: Optional[ParseResult] = None,
        remote_path_prefix: Optional[str] = None,
        remote_path_suffix: Optional[str] = None,
    ) -> ParseResult:

        remote_uri = _gen_remote_uri(
            fileobj, remote_uri, remote_path_prefix, remote_path_suffix, None
        )
        remote_file_path = self._uri_to_path(remote_uri)
        os.makedirs(os.path.dirname(remote_file_path), exist_ok=True)
        with open(remote_file_path, "wb") as fdest:
            shutil.copyfileobj(fileobj, fdest)
        return remote_uri


def _s3_client(config: Config = None):
    if config is None:
        endpoint_url = None
    else:
        endpoint_url = config.get(opt.S3_ENDPOINT_URL, None)
    return S3Client(endpoint_url=endpoint_url)


def _s3a_client(config: Config = None):
    if config is None:
        endpoint_url = None
    else:
        endpoint_url = config.get(opt.S3_ENDPOINT_URL, None)
    return S3Client(endpoint_url=endpoint_url, url_scheme="s3a")


def _gcs_client(config: Config = None):
    return GCSClient()


def _azure_blob_client(config: Config = None):
    if config is None:
        raise Exception("Azure blob client requires config")
    account_name = config.get(opt.AZURE_BLOB_ACCOUNT_NAME, None)
    account_access_key = config.get(opt.AZURE_BLOB_ACCOUNT_ACCESS_KEY, None)
    if account_name is None or account_access_key is None:
        raise Exception(
            f"Azure blob client requires {opt.AZURE_BLOB_ACCOUNT_NAME} and {opt.AZURE_BLOB_ACCOUNT_ACCESS_KEY} set in config"
        )
    return AzureBlobClient(account_name, account_access_key)


def _local_fs_client(config: Config = None):
    return LocalFSClient()


storage_clients = {
    GS: _gcs_client,
    S3: _s3_client,
    S3A: _s3a_client,
    AZURE_SCHEME: _azure_blob_client,
    LOCAL_FILE: _local_fs_client,
}


def get_staging_client(scheme, config: Config = None) -> AbstractStagingClient:
    """
    Initialization of a specific client object(GCSClient, S3Client etc.)

    Args:
        scheme (str): uri scheme: s3, gs or file
        config (Config): additional configuration

    Returns:
        An object of concrete implementation of AbstractStagingClient
    """
    try:
        return storage_clients[scheme](config)
    except ValueError:
        raise Exception(
            f"Could not identify file scheme {scheme}. Only gs://, file://, s3:// and wasbs:// (for Azure) are supported"
        )
