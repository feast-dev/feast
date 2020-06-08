import re
from abc import ABC, ABCMeta, abstractmethod
from enum import Enum
from tempfile import TemporaryFile
from typing import List
from urllib.parse import ParseResult, urlparse

import boto3
from google.cloud import storage


class PROTOCOL(Enum):
    """
    Currently supported protocols enum class.
    """

    GS = "gs"
    S3 = "s3"
    WASB = "wasb"
    WASBS = "wasbs"
    LOCAL_FILE = "file"


class StagingStrategy:
    """
    Class for handling different staging protocols currently s3, gs and local file are supported.
    """

    def __init__(self):
        self._protocol_dict = dict()

    def execute_file_download(self, file_uri: ParseResult) -> TemporaryFile:
        """
        Downloads a file from the uri location and returns a TemporaryFile object

        :param file_uri: Parsed uri of the file ex: urlparse("gs://bucket/file.avro")
        :return: TemporaryFile object
        """
        protocol = self._get_staging_protocol(file_uri.scheme)
        return protocol.download_file(file_uri)

    def execute_get_source_files(self, source: str) -> List[str]:
        """
        Lists all the files under a directory if path has wildcard(*) character.

        :param source: File path with the protocol ex: gs://bucket/* or gs://bucket/file.avro
        :return: List[str]
                    Returns a list containing the full path to the file(s) in the
                    remote staging location.
        """
        uri = urlparse(source)
        if "*" in uri.path:
            protocol = self._get_staging_protocol(uri.scheme)
            return protocol.list_files(bucket=uri.hostname, uri=uri)
        elif PROTOCOL(uri.scheme) in [
            PROTOCOL.S3,
            PROTOCOL.GS,
            PROTOCOL.WASB,
            PROTOCOL.WASBS,
        ]:
            return [source]
        else:
            raise Exception(
                f"Could not identify file protocol {uri.scheme}. Only gs:// and file:// and s3:// and wasb:// and wasbs:// supported"
            )

    def execute_file_upload(
        self, scheme: str, local_path: str, bucket: str, remote_path: str
    ):
        """
        Uploads file to a cloud storage, currently s3 and gs are supported

        :param scheme: uri scheme: s3 or gs
        :param local_path: Path to the local file that needs to be uploaded/staged
        :param bucket: s3 or gs Bucket name
        :param remote_path: relative path to the folder to which the files need to be uploaded
        :return: None
        """
        protocol = self._get_staging_protocol(scheme)
        return protocol.upload_file(local_path, bucket, remote_path)

    def _get_staging_protocol(self, protocol):
        """
        Lazy initialization of a specific protocol object(GCSProtocol, S3Protocol etc.) w
        hen the 1st instance is encountered.

        :param protocol: uri scheme: s3, gs or file
        :return: returns a concrete implementation of AbstractStagingProtocol
        """
        if protocol in self._protocol_dict:
            return self._protocol_dict[protocol]
        else:
            if PROTOCOL(protocol) == PROTOCOL.GS:
                self._protocol_dict[protocol] = GCSProtocol()
            elif PROTOCOL(protocol) == PROTOCOL.S3:
                self._protocol_dict[protocol] = S3Protocol()
            elif PROTOCOL(protocol) == PROTOCOL.WASB:
                self._protocol_dict[protocol] = AzureBlobProtocol("http")
            elif PROTOCOL(protocol) == PROTOCOL.WASBS:
                self._protocol_dict[protocol] = AzureBlobProtocol("https")
            elif PROTOCOL(protocol) == PROTOCOL.LOCAL_FILE:
                self._protocol_dict[protocol] = LocalFSProtocol()
            else:
                raise Exception(
                    f"Could not identify file protocol {protocol}. Only gs:// and file:// and s3:// and wasb:// and wasbs:// supported"
                )
            return self._protocol_dict[protocol]


class AbstractStagingProtocol(ABC):
    """
    Abstract class for staging protocol, any new protocol should implement this class.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def download_file(self, uri: ParseResult) -> TemporaryFile:
        pass

    @abstractmethod
    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
        pass

    @abstractmethod
    def upload_file(self, local_path: str, bucket: str, remote_path: str):
        pass


class GCSProtocol(AbstractStagingProtocol):
    """
    Implementation of AbstractStagingProtocol for google cloud storage
    """

    def __init__(self):
        self.gcs_client = storage.Client(project=None)

    def download_file(self, uri: ParseResult) -> TemporaryFile:
        """
        Downloads a file from google cloud storage and returns a TemporaryFile object

        :param uri: Parsed uri of the file ex: urlparse("gs://bucket/file.avro")
        :return: TemporaryFile object
        """
        url = uri.geturl()
        file_obj = TemporaryFile()
        self.gcs_client.download_blob_to_file(url, file_obj)
        return file_obj

    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in google cloud storage if path has wildcard(*) character.

        :param bucket: google cloud storage bucket name
        :param uri: parsed uri of location in google cloud storage. ex: urlparse("gs://bucket/file.avro")
        :return: List[str]
                    Returns a list containing the full path to the file(s) in the
                    remote staging location.
        """

        bucket = self.gcs_client.get_bucket(bucket)
        path = uri.path

        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            blob_list = bucket.list_blobs(
                prefix=path.strip("/").split("*")[0], delimiter="/"
            )
            # File path should not be in path (file path must be longer than path)
            return [
                f"{uri.scheme}://{uri.hostname}/{file}"
                for file in [x.name for x in blob_list]
                if re.match(regex, file) and file not in path
            ]
        else:
            raise Exception(f"{path} is not a wildcard path")

    def upload_file(self, local_path: str, bucket: str, remote_path: str):
        """
        Uploads file to google cloud storage.

        :param local_path: Path to the local file that needs to be uploaded/staged
        :param bucket: s3 gs Bucket name
        :param remote_path: relative path to the folder to which the files need to be uploaded
        :return: None
        """
        bucket = self.gcs_client.get_bucket(bucket)
        blob = bucket.blob(remote_path)
        blob.upload_from_filename(local_path)


class S3Protocol(AbstractStagingProtocol):
    """
       Implementation of AbstractStagingProtocol for Aws S3 storage
    """

    def __init__(self):
        self.s3_client = boto3.client("s3")

    def download_file(self, uri: ParseResult) -> TemporaryFile:
        """
        Downloads a file from AWS s3 storage and returns a TemporaryFile object

        :param uri: Parsed uri of the file ex: urlparse("s3://bucket/file.avro")
        :return: TemporaryFile object
        """
        url = uri.path[1:]  # removing leading / from the path
        bucket = uri.hostname
        file_obj = TemporaryFile()
        self.s3_client.download_fileobj(bucket, url, file_obj)
        return file_obj

    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in s3 if path has wildcard(*) character.

        :param bucket: s3 bucket name
        :param uri: parsed uri of location in s3. ex: urlparse("s3://bucket/file.avro")
        :return: List[str]
                    Returns a list containing the full path to the file(s) in the
                    remote staging location.
        """
        path = uri.path

        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            blob_list = self.s3_client.list_objects(
                Bucket=bucket, Prefix=path.strip("/").split("*")[0], Delimiter="/"
            )
            # File path should not be in path (file path must be longer than path)
            return [
                f"{uri.scheme}://{uri.hostname}/{file}"
                for file in [x["Key"] for x in blob_list["Contents"]]
                if re.match(regex, file) and file not in path
            ]
        else:
            raise Exception(f"{path} is not a wildcard path")

    def upload_file(self, local_path: str, bucket: str, remote_path: str):
        """
        Uploads file to s3.

        :param local_path: Path to the local file that needs to be uploaded/staged
        :param bucket: s3 Bucket name
        :param remote_path: relative path to the folder to which the files need to be uploaded
        :return: None
        """
        with open(local_path, "rb") as file:
            self.s3_client.upload_fileobj(file, bucket, remote_path)


class AzureBlobProtocol(AbstractStagingProtocol):
    """
       Implementation of AbstractStagingProtocol for Azure storage
    """

    def __init__(self, protocol: str):
        from azure.common.credentials import get_azure_cli_credentials

        credential, subscription = get_azure_cli_credentials()
        self.protocol = protocol
        self.credential = credential

    def download_file(self, uri: ParseResult) -> TemporaryFile:
        """
        Downloads a file from Azure Blob storage and returns a TemporaryFile object

        :param uri: Parsed uri of the file ex: urlparse("wasb://account/container/file.avro")
        :return: TemporaryFile object
        """
        from azure.storage.blob import BlobClient

        file_obj = TemporaryFile()
        client = BlobClient.from_blob_url(
            f"{self.protocol}://{uri.hostname}{uri.path}", credential=self.credential
        )

        client.download_blob().readinto(file_obj)
        file_obj.flush()
        return file_obj

    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
        """
        Lists all the files under a directory in Azure store if path has wildcard(*) character.

        :param bucket: Azure store bucket name
        :param uri: parsed uri of location in Azure storage. ex: urlparse("wasb://account/container/file.avro")
        :return: List[str]
                    Returns a list containing the full path to the file(s) in the
                    remote staging location.
        """
        from azure.storage.blob import ContainerClient

        path_items = uri.path.split("/")
        container = path_items[1]
        path = "/".join(path_items[2:])

        client = ContainerClient.from_container_url(
            f"{self.protocol}://{uri.hostname}/{container}", credential=self.credential
        )

        if "*" in path:
            regex = re.compile(path.replace("*", ".*?").strip("/"))
            blob_list = client.list_blobs(name_starts_with=path.split("*")[0])
            # File path should not be in path (file path must be longer than path)
            return [
                f"{uri.scheme}://{uri.hostname}/{container}/{file}"
                for file in [x.name for x in blob_list]
                if re.match(regex, file) and file not in path
            ]
        else:
            raise Exception(f"{path} is not a wildcard path")

    def upload_file(self, local_path: str, bucket: str, remote_path: str):
        """
        Uploads file to Azure Blob.

        :param local_path: Path to the local file that needs to be uploaded/staged
        :param bucket: Azure Blob account name
        :param remote_path: relative path to the folder to which the files need to be uploaded
        :return: None
        """
        from azure.storage.blob import ContainerClient

        path_items = remote_path.split("/")
        container = path_items[0]
        path = "/".join(path_items[1:])
        client = ContainerClient(
            f"{self.protocol}://{bucket}", container, credential=self.credential
        )

        with open(local_path, "rb") as file:
            client.upload_blob(name=path, data=file)


class LocalFSProtocol(AbstractStagingProtocol):
    """
       Implementation of AbstractStagingProtocol for local file
       Note: The is used for E2E tests.
    """

    def __init__(self):
        pass

    def download_file(self, uri: ParseResult) -> TemporaryFile:
        """
        Reads a local file from the disk

        :param uri: Parsed uri of the file ex: urlparse("file://folder/file.avro")
        :return: TemporaryFile object
        """
        url = uri.path
        file_obj = open(url, "rb")
        return file_obj

    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
        raise NotImplementedError("list files not implemented for Local file")

    def upload_file(self, local_path: str, bucket: str, remote_path: str):
        pass  # For test cases
