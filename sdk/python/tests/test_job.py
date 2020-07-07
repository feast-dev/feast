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
#

import boto3
import grpc
import pandas as pd
import pandavro
import pytest
from unittest.mock import patch, Mock
from moto import mock_s3
from pandas.testing import assert_frame_equal
from pytest import fixture, raises
from azure.storage.filedatalake import PathProperties

import feast.serving.ServingService_pb2_grpc as Serving
from feast.job import JobProto, RetrievalJob
from feast.serving.ServingService_pb2 import DataFormat, GetJobResponse
from feast.serving.ServingService_pb2 import Job as BatchRetrievalJob
from feast.serving.ServingService_pb2 import JobStatus, JobType

BUCKET = "test_bucket"

ACCOUNT = "testabfssaccount.dfs.core.windows.net"
FILESYSTEM = "testfilesystem"

TEST_DATA_FRAME = pd.DataFrame(
    {
        "driver": [1001, 1002, 1003],
        "transaction": [1001, 1002, 1003],
        "driver_id": [1001, 1002, 1003],
    }
)


def create_path_properties(name):
    pp = PathProperties()
    pp.name = name
    return pp


class TestRetrievalJob:
    @fixture
    def retrieve_job(self):

        serving_service_stub = Serving.ServingServiceStub(grpc.insecure_channel(""))
        job_proto = JobProto(
            id="123",
            type=JobType.JOB_TYPE_DOWNLOAD,
            status=JobStatus.JOB_STATUS_RUNNING,
        )
        return RetrievalJob(job_proto, serving_service_stub)

    @fixture
    def avro_data_path(self, tmpdir):
        avro_file = tmpdir.join("file.avro")
        pandavro.to_avro(file_path_or_buffer=str(avro_file), df=TEST_DATA_FRAME)
        return avro_file

    @pytest.mark.parametrize(
        "avro_file_or_empty",
        [("file.avro"), ("")],
        ids=["with_file", "with_directory"],
    )
    def test_to_dataframe_local_file_staging_should_pass(
        self, retrieve_job, avro_data_path, mocker, tmpdir, avro_file_or_empty
    ):
        mocker.patch.object(
            retrieve_job.serving_stub,
            "GetJob",
            return_value=GetJobResponse(
                job=BatchRetrievalJob(
                    id="123",
                    type=JobType.JOB_TYPE_DOWNLOAD,
                    status=JobStatus.JOB_STATUS_DONE,
                    file_uris=[f"file://{tmpdir}/{avro_file_or_empty}"],
                    data_format=DataFormat.DATA_FORMAT_AVRO,
                )
            ),
        )
        retrived_df = retrieve_job.to_dataframe()
        assert_frame_equal(TEST_DATA_FRAME, retrived_df, check_like=True)

    @mock_s3
    def test_to_dataframe_s3_file_staging_should_pass(
        self, retrieve_job, avro_data_path, mocker
    ):
        s3_client = boto3.client("s3")
        target = "test_proj/test_features.avro"
        s3_client.create_bucket(Bucket=BUCKET)
        with open(avro_data_path, "rb") as data:
            s3_client.upload_fileobj(data, BUCKET, target)

        mocker.patch.object(
            retrieve_job.serving_stub,
            "GetJob",
            return_value=GetJobResponse(
                job=BatchRetrievalJob(
                    id="123",
                    type=JobType.JOB_TYPE_DOWNLOAD,
                    status=JobStatus.JOB_STATUS_DONE,
                    file_uris=[f"s3://{BUCKET}/{target}"],
                    data_format=DataFormat.DATA_FORMAT_AVRO,
                )
            ),
        )
        retrived_df = retrieve_job.to_dataframe()
        assert_frame_equal(TEST_DATA_FRAME, retrived_df, check_like=True)

    @pytest.mark.parametrize(
        "job_proto,exception",
        [
            (
                GetJobResponse(
                    job=BatchRetrievalJob(
                        id="123",
                        type=JobType.JOB_TYPE_DOWNLOAD,
                        status=JobStatus.JOB_STATUS_DONE,
                        data_format=DataFormat.DATA_FORMAT_AVRO,
                        error="Testing job failure",
                    )
                ),
                Exception,
            ),
            (
                GetJobResponse(
                    job=BatchRetrievalJob(
                        id="123",
                        type=JobType.JOB_TYPE_DOWNLOAD,
                        status=JobStatus.JOB_STATUS_DONE,
                        data_format=DataFormat.DATA_FORMAT_INVALID,
                    )
                ),
                Exception,
            ),
        ],
        ids=["when_retrieve_job_fails", "when_data_format_is_not_avro"],
    )
    def test_to_dataframe_s3_file_staging_should_raise(
        self, retrieve_job, mocker, job_proto, exception
    ):
        mocker.patch.object(
            retrieve_job.serving_stub, "GetJob", return_value=job_proto,
        )
        with raises(exception):
            retrieve_job.to_dataframe()

    @patch("azure.storage.filedatalake.DataLakeServiceClient")
    def test_to_dataframe_azure_file_staging_should_pass(
        self, client, retrieve_job, avro_data_path, mocker
    ):
        file_client = Mock()
        client().get_file_client.return_value = file_client

        fs_client = Mock()
        client().get_file_system_client.return_value = fs_client
        fs_client.get_paths.return_value = [
            create_path_properties("test_proj2/file1.avro"),
            create_path_properties("test_proj2/file2.tmp"),
        ]

        target_file = "test_proj/test_features.avro"
        target_dir = "test_proj2"

        mocker.patch.object(
            retrieve_job.serving_stub,
            "GetJob",
            return_value=GetJobResponse(
                job=BatchRetrievalJob(
                    id="123",
                    type=JobType.JOB_TYPE_DOWNLOAD,
                    status=JobStatus.JOB_STATUS_DONE,
                    file_uris=[
                        f"abfss://{FILESYSTEM}@{ACCOUNT}/{target_file}",
                        f"abfss://{FILESYSTEM}@{ACCOUNT}/{target_dir}/",
                    ],
                    data_format=DataFormat.DATA_FORMAT_AVRO,
                )
            ),
        )

        def read_into(dest):
            with open(avro_data_path, "rb") as data:
                dest.write(data.read(-1))

        file_client.download_file().readinto.side_effect = read_into
        retrived_df = retrieve_job.to_dataframe()

        client().get_file_system_client.assert_called_once_with(FILESYSTEM)
        fs_client.get_paths.assert_called_once_with(f"/{target_dir}/")

        client().get_file_client.assert_any_call(
            FILESYSTEM, "/test_proj/test_features.avro"
        )
        client().get_file_client.assert_any_call(FILESYSTEM, "test_proj2/file1.avro")
        client().get_file_client.assert_any_call(FILESYSTEM, "test_proj2/file2.tmp")
        assert_frame_equal(
            pd.concat([TEST_DATA_FRAME, TEST_DATA_FRAME], ignore_index=True),
            retrived_df,
            check_like=True,
        )
