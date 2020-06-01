import re
from abc import ABC, ABCMeta, abstractmethod
from enum import Enum
from tempfile import TemporaryFile
from typing import List
from urllib.parse import ParseResult, urlparse

import boto3
from google.cloud import storage


class PROTOCOL(Enum):
    GS = "gs"
    S3 = "s3"
    LOCAL_FILE = "file"


class StagingStrategy:
    def __init__(self):
        self._protocol_dict = dict()

    def execute_file_download(self, file_uri: ParseResult) -> TemporaryFile:
        protocol = self._get_staging_protocol(file_uri.scheme)
        return protocol.download_file(file_uri)

    def execute_get_source_files(self, source: str) -> List[str]:
        uri = urlparse(source)
        if "*" in uri.path:
            protocol = self._get_staging_protocol(uri.scheme)
            return protocol.list_files(bucket=uri.hostname, uri=uri)
        elif PROTOCOL(uri.scheme) in [PROTOCOL.S3, PROTOCOL.GS]:
            return [source]
        else:
            raise Exception(
                f"Could not identify file protocol {uri.scheme}. Only gs:// and file:// and s3:// supported"
            )

    def execute_file_upload(
        self, scheme: str, local_path: str, bucket: str, remote_path: str
    ):
        protocol = self._get_staging_protocol(scheme)
        return protocol.upload_file(local_path, bucket, remote_path)

    def _get_staging_protocol(self, protocol):
        if protocol in self._protocol_dict:
            return self._protocol_dict[protocol]
        else:
            if PROTOCOL(protocol) == PROTOCOL.GS:
                self._protocol_dict[protocol] = GCSProtocol()
            elif PROTOCOL(protocol) == PROTOCOL.S3:
                self._protocol_dict[protocol] = S3Protocol()
            elif PROTOCOL(protocol) == PROTOCOL.LOCAL_FILE:
                self._protocol_dict[protocol] = LocalFSProtocol()
            else:
                raise Exception(
                    f"Could not identify file protocol {protocol}. Only gs:// and file:// and s3:// supported"
                )
            return self._protocol_dict[protocol]


class AbstractStagingProtocol(ABC):

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
    def __init__(self):
        self.gcs_client = storage.Client(project=None)

    def download_file(self, uri: ParseResult) -> TemporaryFile:
        url = uri.geturl()
        file_obj = TemporaryFile()
        self.gcs_client.download_blob_to_file(url, file_obj)
        return file_obj

    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
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
        bucket = self.gcs_client.get_bucket(bucket)
        blob = bucket.blob(remote_path)
        blob.upload_from_filename(local_path)


class S3Protocol(AbstractStagingProtocol):
    def __init__(self):
        self.s3_client = boto3.client("s3")

    def download_file(self, uri: ParseResult) -> TemporaryFile:
        url = uri.path[1:]  # removing leading / from the path
        bucket = uri.hostname
        file_obj = TemporaryFile()
        self.s3_client.download_fileobj(bucket, url, file_obj)
        return file_obj

    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
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
        with open(local_path, "rb") as file:
            self.s3_client.upload_fileobj(file, bucket, remote_path)


class LocalFSProtocol(AbstractStagingProtocol):
    def __init__(self):
        pass

    def download_file(self, file_uri: ParseResult) -> TemporaryFile:
        url = file_uri.path
        file_obj = open(url, "rb")
        return file_obj

    def list_files(self, bucket: str, uri: ParseResult) -> List[str]:
        raise NotImplementedError("list file not implemented for Local file")

    def upload_file(self, local_path: str, bucket: str, remote_path: str):
        pass  # For test cases
